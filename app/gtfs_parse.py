"""GTFS feed parsing and validation with file/line-accurate diagnostics.

Only the three files required by this service are interpreted:

* ``stops.txt``      – unique ``stop_id``, ``stop_name``
* ``trips.txt``      – unique ``trip_id`` (``route_id`` is kept when present)
* ``stop_times.txt`` – ``trip_id``, ``stop_id``, ``stop_sequence``, ``arrival_time``

Validation rules implemented here (all diagnostics carry the member file name
and the 1-based physical line number):

1. ``stop_id`` values are unique in ``stops.txt``.
2. ``trip_id`` values are unique in ``trips.txt``.
3. Every ``stop_times`` row has a non-empty ``arrival_time`` in ``HH:MM:SS``
   form; hours may exceed 23 (GTFS service days run past midnight).
4. Every ``stop_times`` row references a stop defined in ``stops.txt`` and a
   trip defined in ``trips.txt``.
5. ``stop_sequence`` is not repeated within one trip.
6. Arrival times do not go backwards when stops are ordered by
   ``stop_sequence`` (equal times are allowed).

The parser never extracts anything to disk: it reads members directly from the
ZIP archive, so a malicious archive cannot write outside the data directory.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Tuple

REQUIRED_FILES = ("stops.txt", "trips.txt", "stop_times.txt")

ARCHIVE_MAX_MEMBERS = 5000
ARCHIVE_MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
MEMBER_MAX_BYTES = 256 * 1024 * 1024
LINE_MAX_CHARS = 16 * 1024 * 1024

# Hours may exceed 23; minutes/seconds must be 00-59.
TIME_RE = re.compile(r"^(\d{1,3}):([0-5][0-9]):([0-5][0-9])$")
NON_NEGATIVE_INT_RE = re.compile(r"^\d+$")


class FeedRejected(Exception):
    """Raised when a feed cannot be published."""

    def __init__(self, errors: List[dict]):
        self.errors = errors
        super().__init__(f"{len(errors)} validation error(s)")


@dataclass
class ParsedStop:
    stop_id: str
    stop_name: str
    line: int


@dataclass
class ParsedTrip:
    trip_id: str
    route_id: Optional[str]
    line: int


@dataclass
class ParsedStopTime:
    trip_id: str
    stop_id: str
    stop_sequence: int
    arrival_time: str
    arrival_seconds: int
    line: int


@dataclass
class ParsedFeed:
    stops: Dict[str, ParsedStop] = field(default_factory=dict)
    trips: Dict[str, ParsedTrip] = field(default_factory=dict)
    # In stop_sequence order, ready for persistence.
    stop_times_by_trip: Dict[str, List[ParsedStopTime]] = field(default_factory=dict)
    total_stop_times: int = 0


def error(file: str, line: int, code: str, message: str, **extra) -> dict:
    """Build one structured validation diagnostic."""
    payload = {"file": file, "line": line, "code": code, "message": message}
    payload.update(extra)
    return payload


# ----------------------------------------------------------------------------
# CSV reading helpers
# ----------------------------------------------------------------------------


def _decode_lines(
    zf: zipfile.ZipFile, name: str, max_bytes: int = MEMBER_MAX_BYTES
) -> Iterator[Tuple[int, str]]:
    """Yield ``(physical_line_number, text)`` for a ZIP member.

    Lines are decoded one at a time so an encoding failure can be attributed to
    the exact line where it occurs.  Decompressed bytes are counted here as
    well, because a ZIP header may understate the real stream size.
    """
    consumed = 0
    with zf.open(name) as raw:
        for lineno, raw_line in enumerate(raw, start=1):
            consumed += len(raw_line)
            if consumed > max_bytes:
                raise FeedRejected(
                    [
                        error(
                            name,
                            lineno,
                            "member_too_large",
                            "decompressed file exceeds the supported maximum size",
                        )
                    ]
                )
            try:
                text = raw_line.decode("utf-8-sig" if lineno == 1 else "utf-8")
            except UnicodeDecodeError:
                raise FeedRejected(
                    [
                        error(
                            name,
                            lineno,
                            "invalid_encoding",
                            "file is not valid UTF-8",
                        )
                    ]
                )
            if "\x00" in text:
                raise FeedRejected(
                    [error(name, lineno, "invalid_encoding", "NUL byte in text file")]
                )
            if len(text) > LINE_MAX_CHARS:
                raise FeedRejected(
                    [
                        error(
                            name,
                            lineno,
                            "line_too_long",
                            "line exceeds the supported maximum length",
                        )
                    ]
                )
            yield lineno, text


def _scan_quotes(text: str, in_quotes: bool) -> bool:
    """Advance the quoted-field state across one physical line."""
    i = 0
    length = len(text)
    while i < length:
        ch = text[i]
        if ch == '"':
            if in_quotes and i + 1 < length and text[i + 1] == '"':
                i += 2          # escaped quote inside a quoted field
                continue
            in_quotes = not in_quotes
        i += 1
    return in_quotes


def _split_csv_record(text: str) -> List[str]:
    try:
        rows = list(csv.reader(io.StringIO(text), strict=True))
    except csv.Error:
        raise ValueError("not valid CSV")
    if len(rows) != 1:
        raise ValueError("expected a single CSV record")
    return rows[0]


class CsvFile:
    """A header + data rows for one CSV member, with physical line numbers.

    Quoted fields may span physical lines; the reported line is always the
    physical line on which the record starts.
    """

    def __init__(self, file_name: str, lines: Iterator[Tuple[int, str]]):
        self.file_name = file_name
        self._lines = lines
        self._eof = False
        self.header_line, header_text = self._next_content_line()
        try:
            header = _split_csv_record(header_text)
        except ValueError:
            raise FeedRejected(
                [
                    error(
                        file_name,
                        self.header_line,
                        "malformed_header",
                        "header row is not valid CSV",
                    )
                ]
            )
        self.header: List[str] = [cell.strip().lstrip("﻿") for cell in header]
        header_errors: List[dict] = []
        seen: Dict[str, int] = {}
        for index, name in enumerate(self.header):
            if not name:
                header_errors.append(
                    error(
                        file_name,
                        self.header_line,
                        "empty_column_name",
                        f"column {index + 1} has an empty header name",
                    )
                )
            elif name in seen:
                header_errors.append(
                    error(
                        file_name,
                        self.header_line,
                        "duplicate_column",
                        f"duplicate column header {name!r}",
                    )
                )
            else:
                seen[name] = index
        if header_errors:
            raise FeedRejected(header_errors)
        self._index: Dict[str, int] = seen

    # -- public helpers ------------------------------------------------------
    def require(self, names: List[str]) -> None:
        missing = [name for name in names if name not in self._index]
        if missing:
            raise FeedRejected(
                [
                    error(
                        self.file_name,
                        self.header_line,
                        "missing_column",
                        "required column(s) missing: " + ", ".join(missing),
                    )
                ]
            )

    def cell(self, row: List[str], name: str) -> str:
        # Columns that are not required may be absent; ``require`` has already
        # rejected archives where a required column is missing.
        index = self._index.get(name)
        if index is None or index >= len(row):
            return ""
        return row[index].strip()

    # -- internals ------------------------------------------------------------
    def _next_content_line(self) -> Tuple[int, str]:
        """Return the next non-blank, non-comment physical line."""
        while True:
            if self._eof:
                raise FeedRejected(
                    [
                        error(
                            self.file_name,
                            0,
                            "empty_file",
                            "file is empty or has no data rows",
                        )
                    ]
                )
            try:
                lineno, text = next(self._lines)
            except StopIteration:
                self._eof = True
                raise FeedRejected(
                    [
                        error(
                            self.file_name,
                            0,
                            "empty_file",
                            "file is empty or has no data rows",
                        )
                    ]
                )
            if text.strip() == "" or text.lstrip().startswith("#"):
                continue
            return lineno, text

    def __iter__(self) -> Iterator[Tuple[int, List[str]]]:
        pending: List[str] = []
        start_line = 0
        in_quotes = False
        while True:
            try:
                lineno, text = next(self._lines)
            except StopIteration:
                break
            if not pending:
                if text.strip() == "" or text.lstrip().startswith("#"):
                    continue
                start_line = lineno
            pending.append(text)
            in_quotes = _scan_quotes(text, in_quotes)
            if in_quotes:
                continue
            joined = "".join(pending)
            pending = []
            try:
                row = _split_csv_record(joined)
            except ValueError:
                raise FeedRejected(
                    [
                        error(
                            self.file_name,
                            start_line,
                            "malformed_csv",
                            "row is not valid CSV (check quoting)",
                        )
                    ]
                )
            yield start_line, row
        if pending:
            raise FeedRejected(
                [
                    error(
                        self.file_name,
                        start_line,
                        "malformed_csv",
                        "unterminated quoted field at end of file",
                    )
                ]
            )


# ----------------------------------------------------------------------------
# Archive-level checks
# ----------------------------------------------------------------------------


def _member_name(info: zipfile.ZipInfo) -> str:
    return info.filename.rsplit("/", 1)[-1]


def check_archive(zf: zipfile.ZipFile, upload_name: str) -> Dict[str, str]:
    """Return a mapping of required file name -> member path, or raise."""
    infos = zf.infolist()
    errors: List[dict] = []

    if not infos:
        errors.append(error(upload_name, 0, "invalid_archive", "archive is empty"))
    if len(infos) > ARCHIVE_MAX_MEMBERS:
        errors.append(
            error(upload_name, 0, "archive_too_large", "archive has too many members")
        )
    total_size = 0
    members: Dict[str, str] = {}
    for info in infos:
        if info.is_dir():
            continue
        base = _member_name(info)
        if info.flag_bits & 0x1:
            errors.append(
                error(
                    upload_name,
                    0,
                    "encrypted_member",
                    f"ZIP member {info.filename!r} is encrypted",
                )
            )
            continue
        if info.file_size > MEMBER_MAX_BYTES:
            errors.append(
                error(
                    upload_name,
                    0,
                    "member_too_large",
                    f"ZIP member {info.filename!r} is too large",
                )
            )
            continue
        total_size += info.file_size
        if total_size > ARCHIVE_MAX_UNCOMPRESSED_BYTES:
            errors.append(
                error(
                    upload_name,
                    0,
                    "archive_too_large",
                    "uncompressed archive size exceeds the supported maximum",
                )
            )
            break
        if base in REQUIRED_FILES:
            if base in members:
                errors.append(
                    error(
                        upload_name,
                        0,
                        "duplicate_file",
                        f"required file {base!r} appears more than once in the archive",
                    )
                )
                continue
            members[base] = info.filename

    for missing in [name for name in REQUIRED_FILES if name not in members]:
        errors.append(
            error(
                missing,
                0,
                "missing_file",
                "required GTFS file is missing from the archive",
            )
        )
    if errors:
        raise FeedRejected(errors)
    return members


def parse_time(raw: str) -> Optional[int]:
    """Parse a GTFS time (``HH:MM:SS``, hours >= 24 allowed) to seconds."""
    match = TIME_RE.match(raw)
    if not match:
        return None
    hours, minutes, seconds = (int(match.group(i)) for i in (1, 2, 3))
    return hours * 3600 + minutes * 60 + seconds


# -----------------------------------------------------------------------------
# Feed validation
# -----------------------------------------------------------------------------


def validate_archive(zf: zipfile.ZipFile, upload_name: str = "feed.zip") -> ParsedFeed:
    """Validate the required members of an already-opened ZIP archive.

    Raises :class:`FeedRejected` with every diagnostic found; a feed is only
    returned when no error was raised, so callers never persist an invalid
    feed.
    """
    members = check_archive(zf, upload_name)
    feed = ParsedFeed()
    errors: List[dict] = []

    # ---- stops.txt ----------------------------------------------------------
    stops_csv = CsvFile("stops.txt", _decode_lines(zf, members["stops.txt"]))
    stops_csv.require(["stop_id", "stop_name"])
    for lineno, row in stops_csv:
        stop_id = stops_csv.cell(row, "stop_id")
        stop_name = stops_csv.cell(row, "stop_name")
        if not stop_id:
            errors.append(
                error("stops.txt", lineno, "empty_stop_id", "stop_id must not be empty")
            )
            continue
        previous = feed.stops.get(stop_id)
        if previous is not None:
            errors.append(
                error(
                    "stops.txt",
                    lineno,
                    "duplicate_stop_id",
                    f"duplicate stop_id {stop_id!r} (first seen on line {previous.line})",
                    value=stop_id,
                )
            )
            continue
        feed.stops[stop_id] = ParsedStop(
            stop_id=stop_id, stop_name=stop_name, line=lineno
        )

    # ---- trips.txt ----------------------------------------------------------
    trips_csv = CsvFile("trips.txt", _decode_lines(zf, members["trips.txt"]))
    trips_csv.require(["trip_id"])
    for lineno, row in trips_csv:
        trip_id = trips_csv.cell(row, "trip_id")
        route_id = trips_csv.cell(row, "route_id")
        if not trip_id:
            errors.append(
                error("trips.txt", lineno, "empty_trip_id", "trip_id must not be empty")
            )
            continue
        previous = feed.trips.get(trip_id)
        if previous is not None:
            errors.append(
                error(
                    "trips.txt",
                    lineno,
                    "duplicate_trip_id",
                    f"duplicate trip_id {trip_id!r} (first seen on line {previous.line})",
                    value=trip_id,
                )
            )
            continue
        feed.trips[trip_id] = ParsedTrip(
            trip_id=trip_id, route_id=route_id or None, line=lineno
        )

    # ---- stop_times.txt -----------------------------------------------------
    stop_times_csv = CsvFile(
        "stop_times.txt", _decode_lines(zf, members["stop_times.txt"])
    )
    stop_times_csv.require(
        ["trip_id", "stop_id", "stop_sequence", "arrival_time"]
    )
    by_trip: Dict[str, List[ParsedStopTime]] = {}
    for lineno, row in stop_times_csv:
        trip_id = stop_times_csv.cell(row, "trip_id")
        stop_id = stop_times_csv.cell(row, "stop_id")
        sequence_raw = stop_times_csv.cell(row, "stop_sequence")
        arrival_time = stop_times_csv.cell(row, "arrival_time")
        row_errors: List[dict] = []

        if not trip_id:
            row_errors.append(
                error("stop_times.txt", lineno, "empty_trip_id", "trip_id must not be empty")
            )
        elif trip_id not in feed.trips:
            row_errors.append(
                error(
                    "stop_times.txt",
                    lineno,
                    "unknown_trip_id",
                    f"trip_id {trip_id!r} is not defined in trips.txt",
                    value=trip_id,
                )
            )

        if not stop_id:
            row_errors.append(
                error("stop_times.txt", lineno, "empty_stop_id", "stop_id must not be empty")
            )
        elif stop_id not in feed.stops:
            row_errors.append(
                error(
                    "stop_times.txt",
                    lineno,
                    "unknown_stop_id",
                    f"stop_id {stop_id!r} is not defined in stops.txt",
                    value=stop_id,
                )
            )

        if not sequence_raw:
            row_errors.append(
                error(
                    "stop_times.txt",
                    lineno,
                    "empty_stop_sequence",
                    "stop_sequence must not be empty",
                )
            )
        elif not NON_NEGATIVE_INT_RE.match(sequence_raw):
            row_errors.append(
                error(
                    "stop_times.txt",
                    lineno,
                    "invalid_stop_sequence",
                    f"stop_sequence {sequence_raw!r} is not a non-negative integer",
                )
            )

        if not arrival_time:
            row_errors.append(
                error(
                    "stop_times.txt",
                    lineno,
                    "empty_arrival_time",
                    "arrival_time must not be empty",
                )
            )
        elif parse_time(arrival_time) is None:
            row_errors.append(
                error(
                    "stop_times.txt",
                    lineno,
                    "invalid_arrival_time",
                    f"arrival_time {arrival_time!r} is not a valid HH:MM:SS time",
                )
            )

        if row_errors:
            errors.extend(row_errors)
            continue

        parsed = ParsedStopTime(
            trip_id=trip_id,
            stop_id=stop_id,
            stop_sequence=int(sequence_raw),
            arrival_time=arrival_time,
            arrival_seconds=parse_time(arrival_time),  # type: ignore[arg-type]
            line=lineno,
        )
        by_trip.setdefault(trip_id, []).append(parsed)

    # Check ordering rules per trip, after sorting by stop_sequence.  GTFS does
    # not require rows in stop_times.txt to appear in sequence order.
    for trip_id, rows in by_trip.items():
        rows.sort(key=lambda item: (item.stop_sequence, item.line))
        seen_sequences: Dict[int, ParsedStopTime] = {}
        previous: Optional[ParsedStopTime] = None
        for item in rows:
            first = seen_sequences.get(item.stop_sequence)
            if first is not None:
                errors.append(
                    error(
                        "stop_times.txt",
                        item.line,
                        "duplicate_sequence",
                        f"stop_sequence {item.stop_sequence} is repeated for trip_id "
                        f"{trip_id!r} (first seen on line {first.line})",
                        value=trip_id,
                    )
                )
                continue
            seen_sequences[item.stop_sequence] = item
            if previous is not None and item.arrival_seconds < previous.arrival_seconds:
                errors.append(
                    error(
                        "stop_times.txt",
                        item.line,
                        "arrival_time_regression",
                        f"arrival_time {item.arrival_time} for trip_id {trip_id!r} is "
                        f"earlier than the previous stop (line {previous.line}, "
                        f"stop_sequence {previous.stop_sequence}, "
                        f"{previous.arrival_time})",
                        value=trip_id,
                    )
                )
                continue
            previous = item

    if errors:
        raise FeedRejected(errors)

    # A published trip must have at least one stop; otherwise it cannot be
    # queried meaningfully and is almost certainly a feed mistake.
    for trip_id, trip in feed.trips.items():
        if trip_id not in by_trip:
            errors.append(
                error(
                    "trips.txt",
                    trip.line,
                    "trip_without_stops",
                    f"trip_id {trip_id!r} has no rows in stop_times.txt",
                    value=trip_id,
                )
            )
    if errors:
        raise FeedRejected(errors)

    feed.stop_times_by_trip = by_trip
    feed.total_stop_times = sum(len(rows) for rows in by_trip.values())
    return feed
