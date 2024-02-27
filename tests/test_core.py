"""Tests standard tap features using the built-in SDK tests library."""
import datetime
import enum
import json
import os

from faker import Faker
import pytest
import sqlalchemy
from clickhouse_sqlalchemy.types import String, Int, Float, Boolean, Array, Nullable, UUID, LowCardinality, Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Int128, UInt128, Int256, UInt256, Float32, Float64, Date, DateTime, DateTime64, Enum, Enum8, Enum16, Decimal, Tuple, Map
from singer_sdk.testing.templates import TapTestTemplate
from sqlalchemy import Column
from singer_sdk.testing import get_tap_test_class, suites
from clickhouse_sqlalchemy import engines
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

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
sqlalchemy_url="clickhouse+http://test_user:default@localhost:8123/default"


TABLE_NAME = "test_array_col"
# class SimpleTable(Base):
#     __tablename__="simple_table"
#     id = Column(Integer, primary_key=True)
#     name = Column(String)
#     updated_at = Column(DateTime, nullable=False)
#     ARRAY_COL = Column(Array(Integer), primary_key=False, nullable=True)
#     __table_args__ = (
#         engines.MergeTree(order_by="id", primary_key=["id"]),
#     )

def setup_test_table(table: DeclarativeMeta, sqlalchemy_url):
    engine = sqlalchemy.create_engine(sqlalchemy_url)
    try:
        table.__table__.drop(engine)
    except:
        print("Table doesn't exists")
    Base.metadata.create_all(engine, tables=[table.__table__])

def setup_insert_table(table: DeclarativeMeta, sqlalchemy_url, rows): 
    engine = sqlalchemy.create_engine(sqlalchemy_url)
   
    with engine.connect() as conn:
        for row in rows:
            insert = table.__table__.insert().values(
                **row
            )
            conn.execute(insert)

def teardown_test_table(table: DeclarativeMeta, sqlalchemy_url):
    engine = sqlalchemy.create_engine(sqlalchemy_url)
    table.__table__.drop(engine)

class TapDiscoveryExactTest(TapTestTemplate):
    """Test that discovery mode generates a valid tap catalog."""

    name = "discovery_against_file"

    def test(self) -> None:
        # not loading catalog from file, trying to real discover
        tap = TapClickHouse(config=SAMPLE_CONFIG)
        tap.run_discovery()
        tap_catalog = json.loads(tap.catalog_json_text)
        test_file_path = os.path.abspath(os.path.join(os.path.join(os.path.join(__file__, os.pardir), "resources"), f"expected_catalog.json"))
        with open(test_file_path, "r") as fd:
            test_dict = json.loads(fd.read())
        
        assert json.dumps(test_dict) == json.dumps(tap_catalog)

custom_test_key_properties = suites.TestSuite(
    kind="tap",
    tests=[TapDiscoveryExactTest]
)

# Run standard built-in tap tests from the SDK:
TapClickHouseTest = get_tap_test_class(
    tap_class=TapClickHouse,
    config=SAMPLE_CONFIG,
    custom_suites=[custom_test_key_properties],
 	catalog="tests/resources/data.json",
)
class MyEnum(enum.Enum):
    Foo = 1
    Bar = 2
    
class TestTapClickHouse(TapClickHouseTest):
    table_name = TABLE_NAME
    sqlalchemy_url = SAMPLE_CONFIG["sqlalchemy_url"]

    class ArrayTable(Base):
        __tablename__="test_array_col"
        INT_COL = Column(Int, primary_key=True)
        FLOAT_COL = Column(Float)
        BOOLEAN_COL = Column(Boolean)
        ARRAY_COL = Column(Array(String))
        NULLABLE_COL = Column(Nullable(String))
        NULLABLE_COL_INT = Column(Nullable(Int8))
        UUID_COL = Column(UUID)
        LOW_CARDINALITY_STRING_COL = Column(LowCardinality(String))
        INT8_COL = Column(Int8)
        UINT8_COL = Column(UInt8)
        INT16_COL = Column(Int16)
        UINT16_COL = Column(UInt16)
        INT32_COL = Column(Int32)
        UINT32_COL = Column(UInt32)
        INT64_COL = Column(Int64)
        UINT64_COL = Column(UInt64)
        INT128_COL = Column(Int128)
        UINT128_COL = Column(UInt128)
        INT256_COL = Column(Int256)
        UINT256_COL = Column(UInt256)
        FLOAT32_COL = Column(Float32)
        FLOAT64_COL = Column(Float64)
        DATE_COL = Column(Date)
        DATETIME_COL = Column(DateTime)
        DATETIME64_COL = Column(DateTime64)
        ENUM_COL = Column(Enum(MyEnum))
        ENUM8_COL = Column(Enum8(MyEnum))
        ENUM16_COL = Column(Enum16(MyEnum))
        DECIMAL_COL = Column(Decimal(8,2))
        TUPLE_COL = Column(Tuple(String, String))
        MAP_COL = Column(Map(String, UInt64))
        __table_args__ = (
            engines.MergeTree(order_by="INT_COL", primary_key=["INT_COL"]),
    )

    @pytest.fixture(scope="class")
    def resource(self):
        setup_test_table(self.ArrayTable, sqlalchemy_url)
        fake = Faker()
        testing_rows = [
            {
                "INT_COL": x,
                "FLOAT_COL": x,
                "BOOLEAN_COL": 1 ,
                "ARRAY_COL": str(["patata", "cebolla"]) ,
                "NULLABLE_COL": None,
                "NULLABLE_COL_INT": 3,
                "UUID_COL": "61f0c404-5cb3-11e7-907b-a6006ad3dba0",
                "LOW_CARDINALITY_STRING_COL": "lechuga",
                "INT8_COL": x,
                "UINT8_COL": x,
                "INT16_COL": x,
                "UINT16_COL": x,
                "INT32_COL": x,
                "UINT32_COL": x,
                "INT64_COL": x,
                "UINT64_COL": x,
                "INT128_COL": x,
                "UINT128_COL": x,
                "INT256_COL": x,
                "UINT256_COL": x,
                "FLOAT32_COL": x,
                "FLOAT64_COL": x,
                "DATE_COL": "2019-01-01",
                "DATETIME_COL": "2019-01-01 00:00:00",
                "DATETIME64_COL": 1546300800123,
                "ENUM_COL": MyEnum.Bar ,
                "ENUM8_COL": MyEnum.Bar,
                "ENUM16_COL": MyEnum.Foo,
                "DECIMAL_COL": x,
                "TUPLE_COL": str(("tuple", "tuple")),
                "MAP_COL": {"Spain": 2},
            } for x in range(1, 100)
        ]
        setup_insert_table(self.ArrayTable, sqlalchemy_url, testing_rows)
        yield
        #teardown_test_table(self.ArrayTable, sqlalchemy_url)   


