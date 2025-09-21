"""ClickHouse tap class."""

from __future__ import annotations

from singer_sdk.sql import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_clickhouse.client import ClickHouseStream


class TapClickHouse(SQLTap):
    """ClickHouse tap class."""

    name = "tap-clickhouse"
    default_stream_class = ClickHouseStream

    # TODO: cluster
    config_jsonschema = th.PropertiesList(
        th.Property(
            "driver",
            th.StringType,
            required=False,
            description="Driver type",
            default="http",
            allowed_values=["http", "native", "asynch"]
        ),
        th.Property(
            "username",
            th.StringType,
            required=False,
            description="Database user",
            default="default",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Username password",
            secret=True
        ),
        th.Property(
            "host",
            th.StringType,
            required=False,
            description="Database host",
            default="localhost"
        ),
        th.Property(
            "port",
            th.IntegerType,
            required=False,
            description="Database connection port",
            default=8123,
        ),
        th.Property(
            "database",
            th.StringType,
            required=False,
            description="Database name",
            default="default",
        ),
        th.Property(
            "secure",
            th.BooleanType,
            description="Should the connection be secure",
            default=False
        ),
        th.Property(
            "verify",
            th.BooleanType,
            description="Should secure connection need to verify SSL/TLS",
            default=True
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="Rows to fetch per batch",
            default=10000,
            minimum=1000,
        )
    ).to_dict()


if __name__ == "__main__":
    TapClickHouse.cli()
