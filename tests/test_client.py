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


def _build_stream(
    replication_key: str | None,
    column_type: sa.types.TypeEngine,
    column_name: str = "id",
):
    stream = object.__new__(ClickHouseStream)
    stream.replication_key = replication_key
    stream.name = "default-test_table"
    stream.table = sa.table("test_table", sa.column(column_name, column_type))
    return stream


def test_ordered_query_adds_order_by_for_incremental_stream():
    stream = _build_stream("id", sa.Integer())
    query = sa.select(stream.table.c.id)

    stream.build_query = lambda context=None: query

    ordered_query = stream._ordered_query(context=None)

    assert "ORDER BY test_table.id ASC" in str(ordered_query)


def test_uuid_like_incremental_id_is_rejected():
    stream = _build_stream("id", sa.String())

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


def test_incremental_datetime_bookmark_uses_datetime64_parser():
    stream = _build_stream(
        "created_at",
        _DateTime64Six(),
        column_name="created_at",
    )
    stream.get_starting_replication_key_value = lambda context: (
        "2025-10-28T16:04:47.778895+00:00"
    )

    query = stream.build_query(context=None)

    assert "parseDateTime64BestEffort" in str(query)


def test_incremental_integer_bookmark_does_not_use_datetime64_parser():
    stream = _build_stream("id", sa.Integer())
    stream.get_starting_replication_key_value = lambda context: 42

    query = stream.build_query(context=None)

    assert "parseDateTime64BestEffort" not in str(query)

