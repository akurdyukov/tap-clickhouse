"""Tests standard tap features using the built-in SDK tests library."""
import datetime
import json

import pytest
import sqlalchemy
from faker import Faker
from singer_sdk.testing.templates import TapTestTemplate
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, text
from singer_sdk.testing import get_tap_test_class, suites
from clickhouse_sqlalchemy import engines

from tap_clickhouse.tap import TapClickHouse

SAMPLE_CONFIG = {
    "driver": "http",
    "host": "localhost",
    "port": 8123,
    "username": "test_user",
    "password": "default",
    "database": "default",
    "secure": False,
    "verify": False,
    "sqlalchemy_url": "clickhouse+http://test_user:default@localhost:8123/default"
}
TABLE_NAME = "test_table"


def setup_test_table(table_name, sqlalchemy_url):
    """setup any state specific to the execution of the given module."""
    engine = sqlalchemy.create_engine(sqlalchemy_url)
    fake = Faker()

    date1 = datetime.date(2022, 11, 1)
    date2 = datetime.date(2022, 11, 30)
    metadata_obj = MetaData()
    test_replication_key_table = Table(
        table_name,
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("updated_at", DateTime(), nullable=False),
        Column("name", String()),
        engines.MergeTree(order_by="id", primary_key=["id"])
    )
    with engine.connect() as conn:
        metadata_obj.create_all(conn)
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        for _ in range(1000):
            insert = test_replication_key_table.insert().values(
                updated_at=fake.date_between(date1, date2), name=fake.name()
            )
            conn.execute(insert)


def teardown_test_table(table_name, sqlalchemy_url):
    engine = sqlalchemy.create_engine(sqlalchemy_url)
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE {table_name}"))


def key_properties_test():
    # not loading catalog from file, trying to real discover
    tap = TapClickHouse(config=SAMPLE_CONFIG)
    tap.run_discovery()
    tap_catalog = json.loads(tap.catalog_json_text)
    for stream in tap_catalog["streams"]:
        assert "key_properties" in stream


class TapTestKeyProperties(TapTestTemplate):
    name = "key_properties"
    table_name = TABLE_NAME

    def test(self):
        key_properties_test()


custom_test_key_properties = suites.TestSuite(
    kind="tap",
    tests=[TapTestKeyProperties]
)

# Run standard built-in tap tests from the SDK:
TapClickHouseTest = get_tap_test_class(
    tap_class=TapClickHouse,
    config=SAMPLE_CONFIG,
    catalog="tests/resources/data.json",
    custom_suites=[custom_test_key_properties],
)


class TestTapClockHouse(TapClickHouseTest):
    table_name = TABLE_NAME
    sqlalchemy_url = SAMPLE_CONFIG["sqlalchemy_url"]

    @pytest.fixture(scope="class")
    def resource(self):
        setup_test_table(self.table_name, self.sqlalchemy_url)
        yield
        teardown_test_table(self.table_name, self.sqlalchemy_url)

