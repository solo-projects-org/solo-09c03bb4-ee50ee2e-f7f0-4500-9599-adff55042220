"""Unit tests for the GTFS parser, focused on line-accurate diagnostics."""

from __future__ import annotations

import io
import zipfile

import pytest

from app.gtfs_parse import FeedRejected, parse_time, validate_archive


def archive(files: dict[str, str]) -> zipfile.ZipFile:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    buffer.seek(0)
    return zipfile.ZipFile(buffer)


def base_files() -> dict[str, str]:
    return {
        "stops.txt": "stop_id,stop_name\nS1,A\nS2,B\nS3,C\n",
        "trips.txt": "trip_id\nT1\n",
        "stop_times.txt": (
            "trip_id,arrival_time,stop_id,stop_sequence\n"
            "T1,06:00:00,S1,1\n"
            "T1,06:10:00,S2,2\n"
            "T1,06:20:00,S3,3\n"
        ),
    }


def reject(files: dict[str, str]):
    with pytest.raises(FeedRejected) as excinfo:
        validate_archive(archive(files), "feed.zip")
    return excinfo.value.errors


def codes(errors):
    return {(e["file"], e["line"], e["code"]) for e in errors}


def test_parse_time_accepts_hours_past_24():
    assert parse_time("00:00:00") == 0
    assert parse_time("23:59:59") == 23 * 3600 + 59 * 60 + 59
    assert parse_time("24:00:00") == 24 * 3600
    assert parse_time("25:10:00") == 25 * 3600 + 10 * 60
    assert parse_time("100:00:00") == 100 * 3600


@pytest.mark.parametrize(
    "raw",
    ["", "abc", "6:00", "06:60:00", "06:00:60", "06:00", "-1:00:00", "06:00:00Z"],
)
def test_parse_time_rejects_invalid(raw):
    assert parse_time(raw) is None


def test_valid_feed_parses_rows_in_sequence_order():
    feed = validate_archive(archive(base_files()), "feed.zip")
    assert set(feed.stops) == {"S1", "S2", "S3"}
    assert feed.stops["S1"].stop_name == "A"
    assert set(feed.trips) == {"T1"}
    rows = feed.stop_times_by_trip["T1"]
    assert [r.stop_sequence for r in rows] == [1, 2, 3]
    assert feed.total_stop_times == 3


def test_header_line_number_includes_bom_and_leading_blank_lines():
    files = base_files()
    files["stops.txt"] = "﻿\n\nstop_id,stop_name\nS1,A\nS2,B\nS3,C\n"
    feed = validate_archive(archive(files), "feed.zip")
    assert set(feed.stops) == {"S1", "S2", "S3"}


def test_quoted_field_spanning_lines_keeps_record_start_line():
    files = base_files()
    files["stops.txt"] = (
        "stop_id,stop_name\n"
        "S1,\"Central\nStation\"\n"
        "S2,B\n"
        "S1,Duplicate\n"       # physical line 5
    )
    files["stop_times.txt"] = (
        "trip_id,arrival_time,stop_id,stop_sequence\n"
        "T1,06:00:00,S1,1\n"
        "T1,06:10:00,S2,2\n"
    )
    errors = reject(files)
    assert ("stops.txt", 5, "duplicate_stop_id") in codes(errors)


def test_crlf_line_endings_and_trailing_spaces_are_handled():
    files = {
        "stops.txt": "stop_id,stop_name\r\nS1, A \r\nS2,B\r\n",
        "trips.txt": "trip_id\r\nT1\r\n",
        "stop_times.txt": (
            "trip_id,arrival_time,stop_id,stop_sequence\r\n"
            "T1,06:00:00,S1,1\r\n"
            "T1,06:10:00,S2,2\r\n"
        ),
    }
    feed = validate_archive(archive(files), "feed.zip")
    assert feed.stops["S1"].stop_name == "A"


def test_sequence_regression_is_detected_when_rows_are_out_of_file_order():
    files = base_files()
    files["stop_times.txt"] = (
        "trip_id,arrival_time,stop_id,stop_sequence\n"
        "T1,06:05:00,S3,3\n"     # line 2 – sequence 3, but too early
        "T1,06:00:00,S1,1\n"     # line 3
        "T1,06:20:00,S2,2\n"     # line 4
    )
    errors = reject(files)
    assert ("stop_times.txt", 2, "arrival_time_regression") in codes(errors)
    # The diagnostic names the row that violates the ordering.
    regression = next(e for e in errors if e["code"] == "arrival_time_regression")
    assert "stop_sequence 2" in regression["message"]


def test_trip_without_stops_by_sequence():
    files = base_files()
    files["trips.txt"] = "trip_id\nT1\nT2\n"
    errors = reject(files)
    assert ("trips.txt", 3, "trip_without_stops") in codes(errors)


def test_duplicate_header_column_reported_on_header_line():
    files = base_files()
    files["stops.txt"] = "stop_id,stop_name,stop_name\nS1,A,A\n"
    errors = reject(files)
    assert ("stops.txt", 1, "duplicate_column") in codes(errors)


def test_empty_file_reported():
    files = base_files()
    files["trips.txt"] = ""
    errors = reject(files)
    assert any(e["code"] == "empty_file" and e["file"] == "trips.txt" for e in errors)


def test_invalid_utf8_reported_with_line():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("stops.txt", b"stop_id,stop_name\nS1,\xff\xfe\n")
        zf.writestr("trips.txt", "trip_id\nT1\n")
        zf.writestr(
            "stop_times.txt",
            "trip_id,arrival_time,stop_id,stop_sequence\nT1,06:00:00,S1,1\n",
        )
    buffer.seek(0)
    with pytest.raises(FeedRejected) as excinfo:
        validate_archive(zipfile.ZipFile(buffer), "feed.zip")
    assert (excinfo.value.errors[0]["file"], excinfo.value.errors[0]["line"]) == (
        "stops.txt",
        2,
    )


def test_negative_or_fractional_sequence_rejected():
    files = base_files()
    files["stop_times.txt"] = (
        "trip_id,arrival_time,stop_id,stop_sequence\n"
        "T1,06:00:00,S1,-1\n"
        "T1,06:10:00,S2,1.5\n"
    )
    errors = reject(files)
    lines = {e["line"] for e in errors if e["code"] == "invalid_stop_sequence"}
    assert lines == {2, 3}


def test_nested_zip_directories_are_accepted():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for name, text in base_files().items():
            zf.writestr(f"feed/{name}", text)
    buffer.seek(0)
    feed = validate_archive(zipfile.ZipFile(buffer), "feed.zip")
    assert feed.total_stop_times == 3
