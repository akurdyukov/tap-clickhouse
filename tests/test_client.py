"""Tests for ClickHouse stream behavior."""

from __future__ import annotations

import sqlalchemy as sa

from tap_clickhouse.client import ClickHouseStream


class _DateTime64Nine(sa.types.TypeEngine):
    def __str__(self) -> str:
        return "DateTime64(9)"


class _DateTime64Six(sa.types.TypeEngine):
    def __str__(self) -> str:
        return "DateTime64(6)"


class _DateTime64Bare(sa.types.TypeEngine):
    def __str__(self) -> str:
        return "DateTime64"


def test_ordered_query_normalizes_high_precision_datetime64():
    stream = object.__new__(ClickHouseStream)
    column = sa.column("_peerdb_synced_at", _DateTime64Nine())
    stream.build_query = lambda context=None: sa.select(column)

    ordered_query = stream._ordered_query(context=None)

    assert "toDateTime64" in str(ordered_query)


def test_uuid_like_incremental_id_is_detected():
    stream = object.__new__(ClickHouseStream)
    stream.replication_key = "id"
    table = sa.table("t", sa.column("id", sa.String()))
    stream._sqlalchemy_table = lambda: table

    assert stream._is_incremental_uuid_id is True


def test_datetime64_precision_greater_than_six_is_normalized():
    stream = object.__new__(ClickHouseStream)
    column = sa.column("_peerdb_synced_at", _DateTime64Nine())
    query = sa.select(column)

    normalized_query = stream._normalize_datetime64_precision(query)

    assert "toDateTime64" in str(normalized_query)
    assert "AS _peerdb_synced_at" in str(normalized_query)


def test_datetime64_precision_six_is_not_normalized():
    stream = object.__new__(ClickHouseStream)
    column = sa.column("created_at", _DateTime64Six())
    query = sa.select(column)

    normalized_query = stream._normalize_datetime64_precision(query)

    assert "toDateTime64" not in str(normalized_query)


def test_datetime64_without_precision_list_is_normalized():
    stream = object.__new__(ClickHouseStream)
    column = sa.column("ts", _DateTime64Bare())
    query = sa.select(column)

    normalized_query = stream._normalize_datetime64_precision(query)

    assert "toDateTime64" in str(normalized_query)


def test_incremental_datetime_bookmark_uses_datetime64_parser():
    stream = object.__new__(ClickHouseStream)
    stream.replication_key = "created_at"
    stream.supports_nulls_first = False
    stream.get_starting_replication_key_value = lambda context: (
        "2025-10-28T16:04:47.778895+00:00"
    )
    table = sa.table("t", sa.column("created_at", _DateTime64Six()))
    query = table.select()

    filtered = ClickHouseStream.apply_query_filters(
        stream, query, table, context=None
    )

    assert "parseDateTime64BestEffort" in str(filtered)


def test_incremental_integer_bookmark_does_not_use_datetime64_parser():
    stream = object.__new__(ClickHouseStream)
    stream.replication_key = "id"
    stream.supports_nulls_first = False
    stream.get_starting_replication_key_value = lambda context: 42
    table = sa.table("t", sa.column("id", sa.Integer()))
    query = table.select()

    filtered = ClickHouseStream.apply_query_filters(
        stream, query, table, context=None
    )

    assert "parseDateTime64BestEffort" not in str(filtered)
