"""FastAPI application exposing the GTFS version publication API.

Guarantees provided to readers:

* Every request resolves versions and reads rows inside a single SQLite read
  transaction, so it observes one committed version — never a partially
  published one, and never a mix of two.
* A publication becomes visible all at once when its SQLite transaction
  commits; the previous version remains intact and queryable by version id.
* Uploaded bytes are staged and validated before the artifact is renamed into
  place; a failed registration rolls the artifact back, leaving no residue and
  no damage to existing versions.
* Downloads are served from the same open descriptor whose SHA-256 was checked
  against the registered digest, so a missing or tampered artifact is refused
  rather than served.

Arrival back-fill batches (``POST /api/v1/arrivals``) follow the same
transactional discipline: each batch validates and folds the terminal
revision rules inside one ``BEGIN IMMEDIATE`` write transaction, so a batch is
either fully applied or not applied at all, and observations are bound to the
exact published version they were checked against.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal, Optional

from fastapi import FastAPI, File, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

from .gtfs_parse import FeedRejected, validate_archive
from .store import (
    StorageError,
    Store,
    VersionInfo,
    iter_fd,
)

logger = logging.getLogger("gtfs.publisher")

DEFAULT_MAX_UPLOAD_BYTES = 256 * 1024 * 1024
UPLOAD_CHUNK = 1 * 1024 * 1024
MAX_ARRIVAL_EVENTS = 1000


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


class InvalidArrivalTime(ValueError):
    """Raised when a submitted timestamp is not an offset-aware ISO 8601 time."""


def parse_aware_datetime(raw: object) -> datetime:
    """Parse an ISO 8601 string that must carry an explicit UTC offset.

    Naive timestamps (no offset) are refused because every submitted time must
    be unambiguous; ``Z`` and offsets such as ``+08:00`` are both accepted.
    Fractional seconds are accepted only when they are exactly zero, because
    scheduled GTFS times are whole-second and the reported deviation would
    otherwise contain sub-second noise.
    """
    if not isinstance(raw, str):
        raise InvalidArrivalTime("timestamp must be an ISO 8601 string")
    text = raw.strip()
    if not text:
        raise InvalidArrivalTime("timestamp must not be empty")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise InvalidArrivalTime(
            f"{raw!r} is not a valid ISO 8601 timestamp with a UTC offset"
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidArrivalTime(
            f"{raw!r} has no UTC offset; every timestamp must carry one "
            "(e.g. 2026-11-15T06:00:00+08:00)"
        )
    if parsed.microsecond != 0:
        raise InvalidArrivalTime(
            f"{raw!r} has fractional seconds; whole-second timestamps are required"
        )
    return parsed


def parse_service_day(raw: object) -> datetime:
    """Parse the service-day midnight (must be 00:00:00 in its own offset)."""
    parsed = parse_aware_datetime(raw)
    if (parsed.hour, parsed.minute, parsed.second, parsed.microsecond) != (
        0,
        0,
        0,
        0,
    ):
        raise InvalidArrivalTime(
            f"{raw!r} is not a service-day midnight (expected 00:00:00 with a "
            "UTC offset, e.g. 2026-11-15T00:00:00+08:00)"
        )
    return parsed


def utc_iso(instant: datetime) -> str:
    """Canonical UTC representation used as the storage key."""
    return instant.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    get_store()  # opens/creates the DB and runs startup recovery
    yield


app = FastAPI(
    title="GTFS 时刻表发布服务",
    description=(
        "上传 GTFS ZIP 包（stops.txt、trips.txt、stop_times.txt）发布为新版本；"
        "旧版本在新版本发布后仍可按版号查询与下载。\n\n"
        "发布时校验：站点/班次 ID 唯一、停站必须引用存在的站点和班次、"
        "每站必须有到站时刻、同班次站序不得重复、时刻随站序不得倒退；"
        "超过 24 点的 GTFS 时刻（如 25:10:00）是合法的。"
        "校验失败会返回文件与行号，且不会改变任何已发布版本。\n\n"
        "另提供车载终端实际到站批量补传（POST /api/v1/arrivals）："
        "提交版号（current 接收时固定为具体版）、班次、服务日零点、各站序实际到站与修订号，"
        "按该版停站表返回逐站早晚秒数；高修订号覆盖、低号不回退、同号同内容幂等、"
        "同号异内容整批拒收，观测按具体版本隔离。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class VersionSummary(BaseModel):
    version_id: int = Field(description="版本号，发布后单调递增")
    created_at: str = Field(description="发布时间（UTC, ISO 8601）")
    upload_filename: str = Field(description="上传时的原始文件名")
    sha256: str = Field(description="ZIP 包 SHA-256 摘要")
    size_bytes: int = Field(description="ZIP 包字节数")
    stops_count: int
    trips_count: int
    stop_times_count: int


class VersionList(BaseModel):
    current_version: Optional[int] = Field(description="当前版本号，尚无版本时为 null")
    versions: List[VersionSummary]


class PublicationResult(BaseModel):
    version: VersionSummary
    previous_version: Optional[int] = Field(
        description="发布前查询的当前版本号（旧版仍可按版号访问）"
    )


class StopArrival(BaseModel):
    stop_sequence: int
    stop_id: str
    stop_name: str
    arrival_time: str = Field(description="GTFS 原始到站时刻，可超过 24:00:00")
    arrival_seconds: int = Field(description="自服务日起点的秒数")


class TripStops(BaseModel):
    version_id: int
    trip_id: str
    route_id: Optional[str]
    stops: List[StopArrival]


class ValidationFailure(BaseModel):
    error: str
    message: str
    errors: List[dict]


class ErrorResponse(BaseModel):
    error: str
    message: str


# ---------------------------------------------------------------------------
# Arrival event schemas
# ---------------------------------------------------------------------------


class ArrivalEventIn(BaseModel):
    stop_sequence: int = Field(
        ge=0,
        description="该版停站表中的站序 stop_sequence（如 10/20/30 可不连续）",
    )
    actual_arrival: str = Field(
        description="实际到站时刻，ISO 8601 且必须带时区偏移，如 2026-11-15T06:02:30+08:00"
    )
    revision: int = Field(
        ge=0,
        description="终端修订号（非负整数）；高号覆盖旧观测，低号迟到不回退，"
        "同号同内容幂等、同号异内容整批拒收",
    )

    @field_validator("actual_arrival")
    @classmethod
    def _validate_actual_arrival(cls, value: str) -> str:
        parse_aware_datetime(value)
        return value


class ArrivalBatchIn(BaseModel):
    version: str = Field(
        default="current",
        description="版号；可为具体版号或 current，接收时固定解析为具体版号",
    )
    trip_id: str = Field(min_length=1, description="班次 ID（该版 trips.txt 中必须存在）")
    service_day: str = Field(
        description="服务日零点，ISO 8601 带时区偏移且时间部分为 00:00:00，"
        "如 2026-11-15T00:00:00+08:00"
    )
    events: List[ArrivalEventIn] = Field(
        min_length=1,
        max_length=MAX_ARRIVAL_EVENTS,
        description="各站序的实际到站与终端修订号；同批重复站序按相同修订规则处理",
    )

    @field_validator("service_day")
    @classmethod
    def _validate_service_day(cls, value: str) -> str:
        parse_service_day(value)
        return value

    @field_validator("version", mode="before")
    @classmethod
    def _coerce_version(cls, value: object) -> str:
        # Accept a JSON number (e.g. ``1``) as well as "1"/"current"; the
        # storage layer resolves and pins the concrete version id anyway.
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
        return value


class ArrivalEventResult(BaseModel):
    stop_sequence: int
    revision: int
    actual_arrival: str = Field(description="提交时的原始带偏移时刻字符串")
    actual_arrival_utc: str = Field(description="归一化为 UTC 的实际到站时刻")
    deviation_seconds: int = Field(
        description="实际相对计划的早晚秒数：正数为晚点、负数为早点、0 为准点"
    )
    status: Literal["stored", "duplicate", "ignored"] = Field(
        description="stored=新写入或高修订号覆盖；duplicate=同号同内容幂等重送；"
        "ignored=低修订号迟到，未回退已有观测"
    )


class ArrivalObservationOut(BaseModel):
    stop_sequence: int
    actual_arrival: str
    actual_arrival_utc: str
    actual_seconds: int = Field(
        description="实际到站相对服务日零点的秒数（按提交时刻的偏移换算）"
    )
    revision: int
    deviation_seconds: int
    recorded_at: Optional[str] = Field(
        default=None, description="该观测最后一次被写入的时间（UTC, ISO 8601）"
    )


class PlannedStopWithDeviation(BaseModel):
    stop_sequence: int
    stop_id: str
    stop_name: str
    arrival_time: str = Field(description="GTFS 原始计划到站时刻，可超过 24:00:00")
    arrival_seconds: int = Field(description="计划时刻自服务日起点的秒数")
    observation: Optional[ArrivalObservationOut] = Field(
        default=None, description="该站序已提交的最新观测；从未补传时为 null"
    )


class ArrivalBatchResult(BaseModel):
    version_id: int = Field(description="接收时固定解析出的具体版号")
    trip_id: str
    service_day: str = Field(description="服务日零点（归一化为 UTC 的存储键）")
    service_day_submitted: str = Field(description="提交时原始的服务日零点字符串")
    recorded_at: str
    stored_count: int
    duplicate_count: int
    ignored_count: int
    results: List[ArrivalEventResult] = Field(description="逐条事件的处理结果")
    stops: List[PlannedStopWithDeviation] = Field(
        description="该版停站表全部站序及本服务日的最终已提交观测与偏差"
    )


class ArrivalView(BaseModel):
    version_id: int
    trip_id: str
    route_id: Optional[str]
    service_day: str
    stops: List[PlannedStopWithDeviation]


# ---------------------------------------------------------------------------
# Application state / wiring
# ---------------------------------------------------------------------------


def _data_dir() -> Path:
    return Path(os.environ.get("GTFS_DATA_DIR", "/data"))


_store: Optional[Store] = None


def get_store() -> Store:
    global _store
    if _store is None:
        store = Store(_data_dir())
        store.init()
        _store = store
    return _store


def set_store(store: Store) -> None:
    """Used by tests to inject an isolated store."""
    global _store
    _store = store


def max_upload_bytes() -> int:
    raw = os.environ.get("GTFS_MAX_UPLOAD_BYTES")
    if raw is None:
        return DEFAULT_MAX_UPLOAD_BYTES
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_MAX_UPLOAD_BYTES
    return value if value > 0 else DEFAULT_MAX_UPLOAD_BYTES


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@app.exception_handler(StorageError)
async def _storage_error_handler(request: Request, exc: StorageError) -> JSONResponse:
    headers = {}
    if exc.status_code == 410:
        headers["Cache-Control"] = "no-store"
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "message": exc.message},
        headers=headers,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _summary(info: VersionInfo) -> VersionSummary:
    return VersionSummary(
        version_id=info.version_id,
        created_at=info.created_at,
        upload_filename=info.upload_filename,
        sha256=info.sha256,
        size_bytes=info.size_bytes,
        stops_count=info.stops_count,
        trips_count=info.trips_count,
        stop_times_count=info.stop_times_count,
    )


def _safe_upload_name(raw: Optional[str]) -> str:
    name = (raw or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
    if not name:
        name = "feed.zip"
    # Keep the persisted name printable and bounded; it is never used as a path.
    name = "".join(ch for ch in name if ch >= " " and ch != "\x7f")
    return name[:200]


def _invalid_upload(message: str) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "error": "validation_failed",
            "message": message,
            "errors": [
                {
                    "file": "",
                    "line": 0,
                    "code": "invalid_archive",
                    "message": message,
                }
            ],
        },
    )


# ---------------------------------------------------------------------------
# Publication
# ---------------------------------------------------------------------------


@app.post(
    "/api/v1/versions",
    response_model=PublicationResult,
    status_code=201,
    summary="发布新的 GTFS 版本",
    responses={
        422: {
            "model": ValidationFailure,
            "description": "GTFS 校验失败，errors 中包含文件与行号",
        },
        413: {"model": ErrorResponse, "description": "上传包超过大小限制"},
    },
)
async def publish_version(
    file: UploadFile = File(..., description="包含 stops.txt、trips.txt、stop_times.txt 的 ZIP"),
) -> JSONResponse:
    store = get_store()
    upload_name = _safe_upload_name(file.filename)
    limit = max_upload_bytes()

    # Stage the upload under a unique name; nothing is registered yet.
    fd, staging_name = tempfile.mkstemp(prefix="upload-", suffix=".zip", dir=store.staging_dir)
    staging_path = Path(staging_name)
    sha256 = hashlib.sha256()
    size = 0
    try:
        try:
            with os.fdopen(fd, "wb") as out:
                while True:
                    chunk = await file.read(UPLOAD_CHUNK)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > limit:
                        return JSONResponse(
                            status_code=413,
                            content={
                                "error": "upload_too_large",
                                "message": f"upload exceeds the {limit} byte limit",
                            },
                        )
                    sha256.update(chunk)
                    out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
        finally:
            await file.close()

        if size == 0:
            return _invalid_upload("uploaded file is empty")

        # Validation and registration are blocking (file IO, SQLite write lock),
        # so they run off the event loop; queries stay responsive meanwhile.
        try:
            info, previous = await run_in_threadpool(
                _validate_and_publish,
                store,
                staging_path,
                sha256.hexdigest(),
                size,
                upload_name,
            )
        except FeedRejected as rejected:
            return JSONResponse(
                status_code=422,
                content={
                    "error": "validation_failed",
                    "message": f"{len(rejected.errors)} validation error(s); "
                    "no version was published",
                    "errors": rejected.errors,
                },
            )
        except zipfile.BadZipFile:
            return _invalid_upload("uploaded file is not a valid ZIP archive")
        except Exception:
            logger.exception("publication failed")
            # Registration failed; publish() already removed the artifact.
            return JSONResponse(
                status_code=500,
                content={
                    "error": "publication_failed",
                    "message": "the feed could not be registered; the previous "
                    "version is unchanged",
                },
            )
        staging_path = None  # ownership moved into the store
        return JSONResponse(
            status_code=201,
            content={
                "version": _summary(info).model_dump(),
                "previous_version": previous,
            },
        )
    finally:
        if staging_path is not None:
            try:
                staging_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass


def _validate_and_publish(
    store: Store,
    staging_path: Path,
    sha256: str,
    size: int,
    upload_name: str,
):
    """Blocking part of the publish path, executed in a worker thread."""
    with zipfile.ZipFile(staging_path, "r") as zf:
        feed = validate_archive(zf, upload_name)
    return store.publish(
        staging_path=staging_path,
        sha256=sha256,
        size_bytes=size,
        upload_filename=upload_name,
        feed=feed,
    )


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/versions",
    response_model=VersionList,
    summary="列出版本（新版本在前）",
)
def list_versions() -> VersionList:
    store = get_store()
    versions, current = store.list_versions()
    return VersionList(
        current_version=current, versions=[_summary(v) for v in versions]
    )


@app.get(
    "/api/v1/versions/{version}",
    response_model=VersionSummary,
    summary="查看某个版本的信息",
    responses={404: {"model": ErrorResponse}},
)
def get_version(version: str) -> VersionSummary:
    info, _current = get_store().get_version(version)
    return _summary(info)


@app.get(
    "/api/v1/versions/{version}/trips/{trip_id}/stops",
    response_model=TripStops,
    summary="按版本号或 current 查询某班次的完整停站",
    responses={404: {"model": ErrorResponse}},
)
def trip_stops_by_version(version: str, trip_id: str) -> TripStops:
    info, route_id, stops = get_store().trip_stops(version, trip_id)
    return TripStops(
        version_id=info.version_id, trip_id=trip_id, route_id=route_id, stops=stops
    )


@app.get(
    "/api/v1/trips/{trip_id}/stops",
    response_model=TripStops,
    summary="查询班次停站（version 省略时使用当前版本）",
    responses={404: {"model": ErrorResponse}},
)
def trip_stops_current(
    trip_id: str,
    version: str = Query(
        "current",
        description="版本号或 current（默认）",
        examples=["current", "1"],
    ),
) -> TripStops:
    info, route_id, stops = get_store().trip_stops(version, trip_id)
    return TripStops(
        version_id=info.version_id, trip_id=trip_id, route_id=route_id, stops=stops
    )


# ---------------------------------------------------------------------------
# Arrival event back-fill
# ---------------------------------------------------------------------------


def _deviation_seconds(
    actual_seconds: int, planned_seconds: int
) -> int:
    """Actual minus planned, both measured from the same service-day midnight.

    Planned GTFS times may run past 24:00:00; the service-day origin makes the
    comparison correct without any special case.  Positive means the bus
    arrived late, negative means early, zero means on time.
    """
    return actual_seconds - planned_seconds


def _planned_stop_views(
    stops: List[dict],
    observations: dict,
    planned_seconds: Optional[dict] = None,
) -> List[PlannedStopWithDeviation]:
    """Merge the version's planned stops with committed observations."""
    views: List[PlannedStopWithDeviation] = []
    for stop in stops:
        sequence = stop["stop_sequence"]
        observation = observations.get(sequence)
        observation_out = None
        if observation is not None:
            planned = (
                planned_seconds[sequence]
                if planned_seconds is not None
                else stop["arrival_seconds"]
            )
            observation_out = ArrivalObservationOut(
                stop_sequence=sequence,
                actual_arrival=observation["actual_arrival"],
                actual_arrival_utc=observation["actual_arrival_utc"],
                actual_seconds=observation["actual_seconds"],
                revision=observation["revision"],
                deviation_seconds=_deviation_seconds(
                    observation["actual_seconds"], planned
                ),
                recorded_at=observation.get("recorded_at"),
            )
        views.append(
            PlannedStopWithDeviation(
                stop_sequence=sequence,
                stop_id=stop["stop_id"],
                stop_name=stop["stop_name"],
                arrival_time=stop["arrival_time"],
                arrival_seconds=stop["arrival_seconds"],
                observation=observation_out,
            )
        )
    return views


def _query_service_day(raw: str) -> str:
    """Validate the service_day query parameter and return its UTC key."""
    try:
        return utc_iso(parse_service_day(raw))
    except InvalidArrivalTime as exc:
        raise StorageError("invalid_service_day", str(exc), 422)


@app.post(
    "/api/v1/arrivals",
    response_model=ArrivalBatchResult,
    summary="批量写入车载终端补传的实际到站事件",
    responses={
        404: {"model": ErrorResponse, "description": "未知版本或班次（整批不入库）"},
        409: {
            "model": ErrorResponse,
            "description": "同修订号异内容冲突，整批拒收、无任何写入",
        },
        422: {
            "model": ErrorResponse,
            "description": "未知站序、无效时间或请求体不合法，整批不入库",
        },
    },
)
async def submit_arrivals(batch: ArrivalBatchIn) -> JSONResponse:
    store = get_store()

    # Parse every timestamp up front; one invalid value rejects the batch
    # before the storage layer is touched, so nothing can be partially stored.
    service_day = parse_service_day(batch.service_day)
    service_day_utc = utc_iso(service_day)
    normalized_events: List[dict] = []
    for event in batch.events:
        try:
            actual = parse_aware_datetime(event.actual_arrival)
        except InvalidArrivalTime:
            raise StorageError(
                "invalid_actual_arrival",
                f"stop_sequence {event.stop_sequence}: "
                f"{event.actual_arrival!r} is not an ISO 8601 timestamp with "
                "a UTC offset; the whole batch was rejected",
                422,
            )
        actual_utc = actual.astimezone(timezone.utc)
        # Wall-clock seconds from the service-day midnight in the submitted
        # offset, matching how GTFS arrival_seconds are counted from that
        # midnight.  Both instants carry the same offset convention, so the
        # subtraction yields the local service-day offset (may exceed 24h).
        actual_seconds = int((actual - service_day).total_seconds())
        normalized_events.append(
            {
                "stop_sequence": event.stop_sequence,
                "actual_arrival": event.actual_arrival,
                "actual_arrival_utc": utc_iso(actual_utc),
                "actual_seconds": actual_seconds,
                "revision": event.revision,
            }
        )

    outcome = await run_in_threadpool(
        store.submit_arrivals,
        batch.version,
        batch.trip_id,
        service_day_utc,
        batch.service_day,
        normalized_events,
    )

    # Everything in the response comes from the same write-transaction
    # snapshot the batch committed in, so it can never disagree with the
    # stored state.
    stop_views = _planned_stop_views(
        outcome["planned_stops"],
        outcome["observations"],
        outcome["planned_seconds"],
    )

    result_views: List[ArrivalEventResult] = []
    counts = {"stored": 0, "duplicate": 0, "ignored": 0}
    for result, event in zip(outcome["results"], normalized_events):
        counts[result["status"]] += 1
        result_views.append(
            ArrivalEventResult(
                stop_sequence=result["stop_sequence"],
                revision=result["revision"],
                actual_arrival=result["actual_arrival"],
                actual_arrival_utc=event["actual_arrival_utc"],
                deviation_seconds=_deviation_seconds(
                    event["actual_seconds"],
                    outcome["planned_seconds"][result["stop_sequence"]],
                ),
                status=result["status"],
            )
        )

    return JSONResponse(
        status_code=200,
        content=ArrivalBatchResult(
            version_id=outcome["version_id"],
            trip_id=outcome["trip_id"],
            service_day=outcome["service_day"],
            service_day_submitted=outcome["service_day_submitted"],
            recorded_at=outcome["recorded_at"],
            stored_count=counts["stored"],
            duplicate_count=counts["duplicate"],
            ignored_count=counts["ignored"],
            results=result_views,
            stops=stop_views,
        ).model_dump(),
    )


def _arrival_view(version: str, trip_id: str, service_day_raw: str) -> ArrivalView:
    service_day_utc = _query_service_day(service_day_raw)
    data = get_store().get_arrivals(version, trip_id, service_day_utc)
    return ArrivalView(
        version_id=data["version_id"],
        trip_id=data["trip_id"],
        route_id=data["route_id"],
        service_day=data["service_day"],
        stops=_planned_stop_views(data["stops"], data["observations"]),
    )


@app.get(
    "/api/v1/versions/{version}/trips/{trip_id}/arrivals",
    response_model=ArrivalView,
    summary="按版本号或 current 查询某班次某服务日的到站偏差",
    responses={
        404: {"model": ErrorResponse},
        422: {"model": ErrorResponse, "description": "service_day 不合法"},
    },
)
def arrivals_by_version(
    version: str,
    trip_id: str,
    service_day: str = Query(
        ...,
        description="服务日零点，ISO 8601 带时区偏移，如 2026-11-15T00:00:00+08:00",
    ),
) -> ArrivalView:
    return _arrival_view(version, trip_id, service_day)


@app.get(
    "/api/v1/trips/{trip_id}/arrivals",
    response_model=ArrivalView,
    summary="查询班次到站偏差（version 省略时使用当前版本）",
    responses={
        404: {"model": ErrorResponse},
        422: {"model": ErrorResponse, "description": "service_day 不合法"},
    },
)
def arrivals_current(
    trip_id: str,
    service_day: str = Query(
        ...,
        description="服务日零点，ISO 8601 带时区偏移，如 2026-11-15T00:00:00+08:00",
    ),
    version: str = Query("current", description="版本号或 current（默认）"),
) -> ArrivalView:
    return _arrival_view(version, trip_id, service_day)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def _download_headers(info: VersionInfo) -> dict:
    return {
        "Content-Disposition": f'attachment; filename="gtfs-version-{info.version_id}.zip"',
        "Content-Length": str(info.size_bytes),
        "Content-Type": "application/zip",
        "ETag": f'"{info.sha256}"',
        "X-GTFS-Version": str(info.version_id),
        "X-Content-SHA256": info.sha256,
        "Cache-Control": "no-store",
    }


@app.get(
    "/api/v1/versions/{version}/download",
    summary="下载该版本原始 ZIP（校验摘要后返回）",
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse, "description": "摘要不符"},
        410: {"model": ErrorResponse, "description": "文件缺失"},
    },
)
def download_version(version: str):
    store = get_store()
    info, _current = store.get_version(version)
    # Verify the whole artifact before any bytes go on the wire; the returned
    # descriptor is the one that gets streamed, so the bytes cannot change
    # between verification and transmission.  StorageError subclasses
    # (missing -> 410, digest mismatch -> 409) are handled by the global handler.
    fd = store.open_verified_artifact(info)

    filename = f"gtfs-version-{info.version_id}.zip"
    headers = _download_headers(info)
    headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return StreamingResponse(
        iter_fd(fd, info.size_bytes),
        media_type="application/zip",
        headers=headers,
    )


@app.get("/healthz", summary="健康检查")
def healthz() -> dict:
    store = get_store()
    try:
        versions, current = store.list_versions()
    except Exception as exc:  # pragma: no cover - defensive
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(exc)},
        )
    return {"status": "ok", "versions": len(versions), "current_version": current}
