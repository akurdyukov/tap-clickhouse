"""Tests for HTTP stream resilience and URL options."""

from __future__ import annotations

import http.client
from unittest.mock import MagicMock

import pytest
from urllib3.exceptions import ProtocolError

from tap_clickhouse.client import ClickHouseConnector, ClickHouseStream


def test_http_sqlalchemy_url_includes_timeout_when_configured():
    connector = ClickHouseConnector(
        config={
            "driver": "http",
            "host": "ch.example",
            "port": 8123,
            "database": "db",
            "username": "u",
            "password": "p",
            "secure": False,
            "verify": False,
            "http_timeout_seconds": 7200,
        }
    )
    url = connector.get_sqlalchemy_url(connector.config)
    assert "timeout=7200" in url
    assert "protocol=http" in url


def test_native_sqlalchemy_url_omits_http_timeout():
    connector = ClickHouseConnector(
        config={
            "driver": "native",
            "host": "ch.example",
            "port": 9000,
            "database": "db",
            "username": "u",
            "password": "p",
            "secure": False,
            "verify": False,
            "http_timeout_seconds": 7200,
        }
    )
    url = connector.get_sqlalchemy_url(connector.config)
    assert "timeout=" not in url
    assert "clickhouse+native://" in url


def test_get_records_retries_transient_error_before_any_row():
    fetch_calls = {"n": 0}

    class _Mappings:
        def fetchmany(self, size: int) -> list:
            fetch_calls["n"] += 1
            if fetch_calls["n"] == 1:
                raise ProtocolError(
                    "Connection broken: IncompleteRead(0 bytes read, 2 more expected)",
                    http.client.IncompleteRead(b"", 2),
                )
            return []

    class _Result:
        def mappings(self):
            return _Mappings()

    class _Conn:
        def execution_options(self, **kwargs):
            return self

        def execute(self, query):
            return _Result()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    stream = object.__new__(ClickHouseStream)
    stream.name = "test-stream"
    stream.replication_key = None
    stream._config = {
        "batch_size": 10,
        "stream_retry_max_attempts": 2,
        "stream_retry_wait_seconds": 0,
    }
    stream._ordered_query = lambda context: None
    stream._connector = MagicMock()
    stream._connector._connect.return_value = _Conn()

    rows = list(stream.get_records(None))
    assert rows == []
    assert fetch_calls["n"] == 2


def test_get_records_raises_after_row_on_transient_error():
    fetch_calls = {"n": 0}

    class _Mappings:
        def fetchmany(self, size: int) -> list:
            fetch_calls["n"] += 1
            if fetch_calls["n"] == 1:
                return [{"id": 1}]
            raise ProtocolError(
                "Connection broken: IncompleteRead(0 bytes read, 2 more expected)",
                http.client.IncompleteRead(b"", 2),
            )

    class _Result:
        def mappings(self):
            return _Mappings()

    class _Conn:
        def execution_options(self, **kwargs):
            return self

        def execute(self, query):
            return _Result()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    stream = object.__new__(ClickHouseStream)
    stream.name = "test-stream"
    stream.replication_key = None
    stream._config = {
        "batch_size": 10,
        "stream_retry_max_attempts": 3,
        "stream_retry_wait_seconds": 0,
    }
    stream._ordered_query = lambda context: None
    stream._connector = MagicMock()
    stream._connector._connect.return_value = _Conn()

    with pytest.raises(ProtocolError):
        list(stream.get_records(None))
    assert fetch_calls["n"] == 2
