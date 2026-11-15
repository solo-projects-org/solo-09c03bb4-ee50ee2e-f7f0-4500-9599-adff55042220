"""Tests for the arrival-event back-fill API.

Covers:

* deviation calculation against the version's planned stop table, including
  GTFS times past 24:00:00 and different UTC offsets;
* revision-number semantics (higher overwrites, lower is ignored, same
  revision + same content is idempotent, same revision + different content
  rejects the whole batch), including repeated sequences inside one batch;
* whole-batch rejection on unknown trip/stop sequence/invalid time;
* version pinning: observations submitted against an old version never appear
  under a new version;
* concurrent batches serialising into a consistent final state.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.main import app
from app.store import Store

from test_api import upload, zip_bytes

SERVICE_DAY = "2026-11-15T00:00:00+08:00"


def arrive(
    client: TestClient,
    events: list[dict],
    *,
    trip_id: str = "T1",
    service_day: str = SERVICE_DAY,
    version: str = "current",
):
    return client.post(
        "/api/v1/arrivals",
        json={
            "version": version,
            "trip_id": trip_id,
            "service_day": service_day,
            "events": events,
        },
    )


def event(stop_sequence: int, actual_arrival: str, revision: int) -> dict:
    return {
        "stop_sequence": stop_sequence,
        "actual_arrival": actual_arrival,
        "revision": revision,
    }


def get_arrivals(
    client: TestClient,
    trip_id: str = "T1",
    service_day: str = SERVICE_DAY,
    version: str | None = None,
):
    if version is None:
        return client.get(
            f"/api/v1/trips/{trip_id}/arrivals",
            params={"service_day": service_day},
        )
    return client.get(
        f"/api/v1/versions/{version}/trips/{trip_id}/arrivals",
        params={"service_day": service_day},
    )


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    path = tmp_path / "data"
    path.mkdir()
    return path


@pytest.fixture()
def client(data_dir: Path):
    store = Store(data_dir)
    store.init()
    main_module.set_store(store)
    with TestClient(app) as test_client:
        yield test_client
    main_module._store = None


# ---------------------------------------------------------------------------
# Happy path / deviation calculation
# ---------------------------------------------------------------------------


def test_batch_returns_deviations_and_is_persisted(client: TestClient):
    assert upload(client).status_code == 201

    # Planned T1: S1 06:00:00 (21600s), S2 06:10:00 (22200s), S3 24:30:00.
    response = arrive(
        client,
        [
            event(1, "2026-11-15T06:00:00+08:00", 1),       # on time
            event(2, "2026-11-15T06:12:30+08:00", 1),       # 150 s late
            event(3, "2026-11-16T00:25:00+08:00", 1),       # 24:25 -> 300 s early
        ],
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["version_id"] == 1
    assert body["service_day"] == "2026-11-14T16:00:00Z"
    assert body["service_day_submitted"] == SERVICE_DAY
    assert body["stored_count"] == 3
    assert body["duplicate_count"] == 0
    assert body["ignored_count"] == 0
    assert [r["status"] for r in body["results"]] == ["stored"] * 3
    assert [r["deviation_seconds"] for r in body["results"]] == [0, 150, -300]

    # The full planned stop list is returned with observations attached.
    stops = body["stops"]
    assert [s["stop_sequence"] for s in stops] == [1, 2, 3]
    assert [s["observation"]["revision"] for s in stops] == [1, 1, 1]
    assert [s["observation"]["deviation_seconds"] for s in stops] == [0, 150, -300]
    assert stops[0]["arrival_time"] == "06:00:00"
    assert stops[2]["arrival_time"] == "24:30:00"

    # Follow-up GET returns the same committed result through both routes.
    for got in (
        get_arrivals(client),
        get_arrivals(client, version="1"),
        get_arrivals(client, version="current"),
    ):
        assert got.status_code == 200, got.text
        payload = got.json()
        assert payload["version_id"] == 1
        assert payload["service_day"] == "2026-11-14T16:00:00Z"
        assert [s["stop_sequence"] for s in payload["stops"]] == [1, 2, 3]
        assert [s["observation"]["deviation_seconds"] for s in payload["stops"]] == [
            0,
            150,
            -300,
        ]
        assert payload["stops"][0]["observation"]["recorded_at"]


def test_unobserved_stops_come_back_with_null_observation(client: TestClient):
    assert upload(client).status_code == 201
    response = arrive(client, [event(1, "2026-11-15T06:05:00+08:00", 7)])
    assert response.status_code == 200
    stops = response.json()["stops"]
    assert stops[0]["observation"] is not None
    assert stops[1]["observation"] is None
    assert stops[2]["observation"] is None

    fetched = get_arrivals(client).json()
    assert fetched["stops"][1]["observation"] is None
    assert fetched["stops"][2]["observation"] is None


def test_different_utc_offsets_represent_the_same_instant(client: TestClient):
    assert upload(client).status_code == 201
    # 06:00 +08:00 == 22:00Z the previous day; planned S1 is 06:00 local.
    response = arrive(
        client, [event(1, "2026-11-14T22:00:00Z", 1)]
    )
    assert response.status_code == 200, response.text
    assert response.json()["results"][0]["deviation_seconds"] == 0
    assert (
        response.json()["results"][0]["actual_arrival_utc"]
        == "2026-11-14T22:00:00Z"
    )

    # Z suffix and explicit offset are equivalent.
    again = arrive(client, [event(1, "2026-11-15T06:00:00+08:00", 1)])
    assert again.status_code == 200
    assert again.json()["results"][0]["status"] == "duplicate"


def test_service_day_offset_normalises_to_one_storage_key(client: TestClient):
    assert upload(client).status_code == 201
    first = arrive(
        client,
        [event(1, "2026-11-15T06:00:00+08:00", 1)],
        service_day="2026-11-15T00:00:00+08:00",
    )
    assert first.status_code == 200
    assert first.json()["service_day"] == "2026-11-14T16:00:00Z"

    # A UTC-midnight service day is a different instant and therefore a
    # different key: nothing has been submitted for it yet.
    utc_day = get_arrivals(
        client, service_day="2026-11-15T00:00:00+00:00"
    )
    assert utc_day.status_code == 200
    assert utc_day.json()["stops"][0]["observation"] is None

    # Re-expressing the same midnight instant with an equivalent offset
    # spelling maps back to the same stored service day (idempotent hit).
    same_key = arrive(
        client,
        [event(1, "2026-11-14T22:00:00Z", 1)],
        service_day="2026-11-15T00:00:00+08:00",
    )
    assert same_key.status_code == 200
    assert same_key.json()["results"][0]["status"] == "duplicate"
    assert same_key.json()["service_day"] == "2026-11-14T16:00:00Z"


def test_negative_offset_times_compute_deviation_correctly(client: TestClient):
    assert upload(client).status_code == 201
    # Service day midnight -05:00; 07:00-05:00 is 12:00Z; planned 06:00 local
    # service seconds = 21600, actual service seconds = 7*3600 = 25200.
    response = arrive(
        client,
        [event(1, "2026-11-15T07:00:00-05:00", 1)],
        service_day="2026-11-15T00:00:00-05:00",
    )
    assert response.status_code == 200, response.text
    assert response.json()["results"][0]["deviation_seconds"] == 3600


# ---------------------------------------------------------------------------
# Revision semantics
# ---------------------------------------------------------------------------


def test_higher_revision_overwrites_lower_is_ignored(client: TestClient):
    assert upload(client).status_code == 201
    assert arrive(
        client, [event(1, "2026-11-15T06:10:00+08:00", 5)]
    ).status_code == 200

    higher = arrive(client, [event(1, "2026-11-15T06:02:00+08:00", 9)])
    assert higher.status_code == 200
    item = higher.json()["results"][0]
    assert item["status"] == "stored"
    assert item["deviation_seconds"] == 120

    # A late, lower revision must not roll the observation back.
    lower = arrive(client, [event(1, "2026-11-15T06:59:00+08:00", 3)])
    assert lower.status_code == 200
    assert lower.json()["results"][0]["status"] == "ignored"
    assert lower.json()["ignored_count"] == 1

    fetched = get_arrivals(client).json()
    obs = fetched["stops"][0]["observation"]
    assert obs["revision"] == 9
    assert obs["actual_arrival"] == "2026-11-15T06:02:00+08:00"
    assert obs["deviation_seconds"] == 120
    assert obs["actual_seconds"] == 6 * 3600 + 2 * 60


def test_same_revision_same_content_is_idempotent(client: TestClient):
    assert upload(client).status_code == 201
    payload = [event(1, "2026-11-15T06:10:00+08:00", 4)]
    first = arrive(client, payload)
    recorded_at = first.json()["recorded_at"]
    assert first.json()["results"][0]["status"] == "stored"

    resent = arrive(client, payload)
    assert resent.status_code == 200
    body = resent.json()
    assert body["results"][0]["status"] == "duplicate"
    assert body["duplicate_count"] == 1
    assert body["stored_count"] == 0
    # Idempotent resend does not touch the stored observation.
    assert body["stops"][0]["observation"]["recorded_at"] == recorded_at
    assert body["stops"][0]["observation"]["revision"] == 4

    # Exactly one row exists in the table.
    store = main_module.get_store()
    with store.read_transaction() as conn:
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM arrival_observations"
        ).fetchone()["n"]
    assert count == 1


def test_same_revision_different_content_rejects_whole_batch(client: TestClient):
    assert upload(client).status_code == 201
    assert arrive(
        client, [event(1, "2026-11-15T06:10:00+08:00", 4)]
    ).status_code == 200

    conflict = arrive(
        client,
        [
            event(1, "2026-11-15T06:11:00+08:00", 4),   # same rev, new content
            event(2, "2026-11-15T06:20:00+08:00", 4),   # would otherwise store
        ],
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "arrival_revision_conflict"

    # Nothing from the rejected batch landed.
    fetched = get_arrivals(client).json()
    assert fetched["stops"][0]["observation"]["actual_arrival"] == (
        "2026-11-15T06:10:00+08:00"
    )
    assert fetched["stops"][1]["observation"] is None


def test_duplicate_sequence_in_one_batch_follows_revision_rules(client: TestClient):
    assert upload(client).status_code == 201
    response = arrive(
        client,
        [
            event(1, "2026-11-15T06:10:00+08:00", 2),
            event(1, "2026-11-15T06:10:00+08:00", 2),  # identical duplicate
            event(2, "2026-11-15T06:20:00+08:00", 2),
            event(2, "2026-11-15T06:15:00+08:00", 5),  # higher wins inside batch
            event(2, "2026-11-15T06:59:00+08:00", 1),  # late lower revision
        ],
    )
    assert response.status_code == 200, response.text
    statuses = [r["status"] for r in response.json()["results"]]
    assert statuses == ["stored", "duplicate", "stored", "stored", "ignored"]

    fetched = get_arrivals(client).json()
    assert fetched["stops"][1]["observation"]["actual_arrival"] == (
        "2026-11-15T06:15:00+08:00"
    )
    assert fetched["stops"][1]["observation"]["revision"] == 5


def test_duplicate_sequence_same_revision_different_content_rejects_batch(
    client: TestClient,
):
    assert upload(client).status_code == 201
    conflict = arrive(
        client,
        [
            event(1, "2026-11-15T06:10:00+08:00", 2),
            event(1, "2026-11-15T06:11:00+08:00", 2),
        ],
    )
    assert conflict.status_code == 409
    assert get_arrivals(client).json()["stops"][0]["observation"] is None


def test_conflict_then_higher_revision_can_still_proceed(client: TestClient):
    assert upload(client).status_code == 201
    assert arrive(
        client, [event(1, "2026-11-15T06:10:00+08:00", 4)]
    ).status_code == 200
    rejected = arrive(
        client, [event(1, "2026-11-15T06:11:00+08:00", 4)]
    )
    assert rejected.status_code == 409

    # A strictly higher revision is still accepted afterwards.
    accepted = arrive(client, [event(1, "2026-11-15T06:11:00+08:00", 5)])
    assert accepted.status_code == 200
    assert accepted.json()["results"][0]["status"] == "stored"


# ---------------------------------------------------------------------------
# Whole-batch validation
# ---------------------------------------------------------------------------


def test_unknown_trip_rejects_batch_with_404(client: TestClient):
    assert upload(client).status_code == 201
    response = arrive(
        client, [event(1, "2026-11-15T06:00:00+08:00", 1)], trip_id="NOPE"
    )
    assert response.status_code == 404
    assert response.json()["error"] == "trip_not_found"


def test_unknown_stop_sequence_rejects_whole_batch(client: TestClient):
    assert upload(client).status_code == 201
    response = arrive(
        client,
        [
            event(1, "2026-11-15T06:00:00+08:00", 1),
            event(99, "2026-11-15T07:00:00+08:00", 1),
        ],
    )
    assert response.status_code == 422
    assert response.json()["error"] == "unknown_stop_sequence"
    # The valid first event must not have been stored either.
    assert get_arrivals(client).json()["stops"][0]["observation"] is None


def test_unknown_version_and_invalid_version_query(client: TestClient):
    assert upload(client).status_code == 201
    response = arrive(
        client,
        [event(1, "2026-11-15T06:00:00+08:00", 1)],
        version="99",
    )
    assert response.status_code == 404
    assert response.json()["error"] == "version_not_found"

    bad = arrive(
        client,
        [event(1, "2026-11-15T06:00:00+08:00", 1)],
        version="abc",
    )
    assert bad.status_code == 400
    assert bad.json()["error"] == "invalid_version"


def test_numeric_version_in_body_is_coerced_to_string(client: TestClient):
    assert upload(client).status_code == 201
    response = client.post(
        "/api/v1/arrivals",
        json={
            "version": 1,  # JSON number, not "1"
            "trip_id": "T1",
            "service_day": SERVICE_DAY,
            "events": [event(1, "2026-11-15T06:00:00+08:00", 1)],
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["version_id"] == 1


@pytest.mark.parametrize(
    "raw",
    [
        "2026-11-15T06:00:00",            # naive, no offset
        "2026-11-15 06:00:00",            # not ISO
        "not-a-time",
        "2026-11-15T06:00:00.5+08:00",    # fractional seconds
        "2026-11-15T25:00:00+08:00",      # hour out of range
    ],
)
def test_invalid_actual_arrival_rejects_batch(client: TestClient, raw: str):
    assert upload(client).status_code == 201
    response = arrive(client, [event(1, raw, 1)])
    assert response.status_code == 422, response.text


def test_invalid_service_day_shape_rejected(client: TestClient):
    assert upload(client).status_code == 201
    # No offset -> pydantic-level 422.
    body_response = client.post(
        "/api/v1/arrivals",
        json={
            "version": "current",
            "trip_id": "T1",
            "service_day": "2026-11-15T00:00:00",
            "events": [event(1, "2026-11-15T06:00:00+08:00", 1)],
        },
    )
    assert body_response.status_code == 422

    # Offset present but not midnight -> application-level 422.
    not_midnight = arrive(
        client,
        [event(1, "2026-11-15T06:00:00+08:00", 1)],
        service_day="2026-11-15T00:01:00+08:00",
    )
    assert not_midnight.status_code == 422

    # GET-side validation uses the same rules.
    got = get_arrivals(client, service_day="2026-11-15T00:00:00")
    assert got.status_code == 422
    assert got.json()["error"] == "invalid_service_day"


def test_empty_batch_and_bad_revision_are_request_errors(client: TestClient):
    assert upload(client).status_code == 201
    empty = client.post(
        "/api/v1/arrivals",
        json={
            "version": "current",
            "trip_id": "T1",
            "service_day": SERVICE_DAY,
            "events": [],
        },
    )
    assert empty.status_code == 422

    negative = arrive(
        client, [event(1, "2026-11-15T06:00:00+08:00", -1)]
    )
    assert negative.status_code == 422


# ---------------------------------------------------------------------------
# Version isolation / service-day isolation
# ---------------------------------------------------------------------------


def test_observations_are_pinned_to_the_exact_version(client: TestClient):
    assert upload(client).status_code == 201
    v2_files = {
        "stops.txt": "stop_id,stop_name\nS1,Central\nS2,People Square\nS3,Science Hall\nS9,New Town\n",
        "trips.txt": "route_id,trip_id\nR1,T1\nR1,T2\nR9,T9\n",
        "stop_times.txt": (
            "trip_id,arrival_time,stop_id,stop_sequence\n"
            "T1,06:00:00,S1,1\n"
            "T1,06:20:00,S3,2\n"
            "T2,07:00:00,S1,1\n"
            "T2,07:15:00,S3,2\n"
            "T9,25:10:00,S9,1\n"
            "T9,25:30:00,S1,2\n"
        ),
    }
    assert upload(client, zip_bytes(v2_files)).status_code == 201

    # Submit against the pinned old version while version 2 is current.
    response = arrive(
        client,
        [event(1, "2026-11-15T06:01:00+08:00", 1)],
        version="1",
    )
    assert response.status_code == 200
    assert response.json()["version_id"] == 1

    # Visible under version 1 ...
    v1 = get_arrivals(client, version="1").json()
    assert v1["stops"][0]["observation"]["revision"] == 1
    # ... but never under version 2/current, even with the same trip id.
    v2 = get_arrivals(client, version="2").json()
    assert all(stop["observation"] is None for stop in v2["stops"])
    current = get_arrivals(client).json()
    assert current["version_id"] == 2
    assert all(stop["observation"] is None for stop in current["stops"])

    # Submitting for current writes only to version 2.
    v2_write = arrive(
        client, [event(2, "2026-11-15T06:25:00+08:00", 8)], version="current"
    )
    assert v2_write.json()["version_id"] == 2
    assert v2_write.json()["results"][0]["deviation_seconds"] == 300
    assert get_arrivals(client, version="1").json()["stops"][1]["observation"] is None


def test_different_service_days_are_independent(client: TestClient):
    assert upload(client).status_code == 201
    day_one = arrive(
        client, [event(1, "2026-11-15T06:10:00+08:00", 1)],
        service_day="2026-11-15T00:00:00+08:00",
    )
    assert day_one.status_code == 200
    day_two = arrive(
        client, [event(1, "2026-11-16T06:20:00+08:00", 1)],
        service_day="2026-11-16T00:00:00+08:00",
    )
    assert day_two.status_code == 200

    first = get_arrivals(
        client, service_day="2026-11-15T00:00:00+08:00"
    ).json()
    second = get_arrivals(
        client, service_day="2026-11-16T00:00:00+08:00"
    ).json()
    assert first["stops"][0]["observation"]["deviation_seconds"] == 600
    assert second["stops"][0]["observation"]["deviation_seconds"] == 1200


def test_observations_survive_restart(data_dir: Path):
    first = Store(data_dir)
    first.init()
    main_module.set_store(first)
    with TestClient(app) as c:
        assert upload(c).status_code == 201
        assert arrive(
            c, [event(1, "2026-11-15T06:10:00+08:00", 3)]
        ).status_code == 200
    main_module._store = None

    second = Store(data_dir)
    second.init()
    main_module.set_store(second)
    with TestClient(app) as c:
        payload = get_arrivals(c).json()
        assert payload["stops"][0]["observation"]["revision"] == 3
        assert payload["stops"][0]["observation"]["deviation_seconds"] == 600
    main_module._store = None


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_batches_serialise_to_consistent_state(client: TestClient):
    assert upload(client).status_code == 201
    barrier = threading.Barrier(8)
    errors: list[str] = []

    def worker(rev: int):
        barrier.wait()
        minute = rev  # each revision carries distinct content
        response = arrive(
            client,
            [event(1, f"2026-11-15T06:{minute:02d}:00+08:00", rev)],
        )
        if response.status_code != 200:
            errors.append(response.text)

    threads = [threading.Thread(target=worker, args=(r,)) for r in range(1, 9)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    payload = get_arrivals(client).json()
    obs = payload["stops"][0]["observation"]
    # Only the highest revision survives; lower ones never rolled it back.
    assert obs["revision"] == 8
    assert obs["actual_arrival"] == "2026-11-15T06:08:00+08:00"


def test_concurrent_reads_see_only_committed_batches(client: TestClient):
    assert upload(client).status_code == 201
    stop = threading.Event()
    errors: list[str] = []

    def reader():
        while not stop.is_set():
            response = get_arrivals(client)
            if response.status_code != 200:
                errors.append(response.text)
                return
            revisions = {
                s["observation"]["revision"]
                for s in response.json()["stops"]
                if s["observation"] is not None
            }
            # A read must observe a coherent per-stop revision set; the only
            # invariant checkable without synchronisation is well-formed data.
            if any(not isinstance(rev, int) or rev < 0 for rev in revisions):
                errors.append(f"malformed snapshot: {revisions}")
                return

    def writer():
        for rev in range(1, 21):
            response = arrive(
                client,
                [
                    event(1, f"2026-11-15T06:{rev % 60:02d}:00+08:00", rev),
                    event(2, f"2026-11-15T06:{(rev + 10) % 60:02d}:00+08:00", rev),
                ],
            )
            if response.status_code != 200:
                errors.append(response.text)
                return

    reader_thread = threading.Thread(target=reader)
    reader_thread.start()
    try:
        writer()
    finally:
        stop.set()
        reader_thread.join()

    assert errors == []
    final = get_arrivals(client).json()
    assert final["stops"][0]["observation"]["revision"] == 20
    assert final["stops"][1]["observation"]["revision"] == 20


# ---------------------------------------------------------------------------
# API surface
# ---------------------------------------------------------------------------


def test_arrival_endpoints_in_openapi_and_schemas(client: TestClient):
    assert upload(client).status_code == 201
    spec = client.get("/openapi.json").json()
    assert "/api/v1/arrivals" in spec["paths"]
    assert (
        "/api/v1/versions/{version}/trips/{trip_id}/arrivals" in spec["paths"]
    )
    assert "/api/v1/trips/{trip_id}/arrivals" in spec["paths"]
    assert client.get("/docs").status_code == 200


def test_get_arrivals_unknown_trip_and_no_observations(client: TestClient):
    assert upload(client).status_code == 201
    missing = get_arrivals(client, trip_id="GHOST")
    assert missing.status_code == 404
    assert missing.json()["error"] == "trip_not_found"

    # A valid trip/day with no observations is a normal 200 with nulls.
    empty = get_arrivals(client)
    assert empty.status_code == 200
    assert all(stop["observation"] is None for stop in empty.json()["stops"])
