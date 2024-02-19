"""SQL client handling.

This includes ClickHouseStream and ClickHouseConnector.
"""

from __future__ import annotations

import datetime
from typing import Any, Iterable

import sqlalchemy  # noqa: TCH002
from singer_sdk import SQLConnector, SQLStream
from sqlalchemy.engine import Engine, Inspector
import singer_sdk.helpers._typing

unpatched_conform = singer_sdk.helpers._typing._conform_primitive_property
def patched_conform(
    elem: Any,
    property_schema: dict,
) -> Any:
    """Overrides Singer SDK type conformance to prevent dates turning into datetimes.
    Converts a primitive (i.e. not object or array) to a json compatible type.
    Returns:
        The appropriate json compatible type.
    """
    if isinstance(elem, datetime.date):
        return elem.isoformat()
    return unpatched_conform(elem=elem, property_schema=property_schema)

singer_sdk.helpers._typing._conform_primitive_property = patched_conform
class ClickHouseConnector(SQLConnector):
    """Connects to the ClickHouse SQL source."""

    def to_jsonschema_type_array(
            from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Return the JSON Schema dict that describes the sql type.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.
        """
        sqltype_lookup: dict[str, dict] = {
            # NOTE: This is an ordered mapping, with earlier mappings taking precedence.
            #       If the SQL-provided type contains the type name on the left, the mapping
            #       will return the respective singer type.
            "timestamp": DateTimeType,
            "datetime": DateTimeType,
            "date": DateType,
            "int": IntegerType,
            "number": NumberType,
            "decimal": NumberType,
            "double": NumberType,
            "float": NumberType,
            "string": StringType,
            "text": StringType,
            "char": StringType,
            "bool": BooleanType,
            "variant": StringType,
        }
        import clickhouse_sqlalchemy
        if isinstance(from_type, clickhouse_sqlalchemy.types.common.Array):
            sqltype_lookup["array"] = ArrayType(to_jsonschema_type(from_type.item_type_impl))
        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
                from_type,
                sqlalchemy.types.TypeEngine,
        ):
            type_name = from_type.__name__
        else:
            msg = "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
            raise ValueError(msg)

        # Look for the type name within the known SQL type names:
        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                return jsonschema_type

        return sqltype_lookup["string"]  # safe failover to str

    def to_jsonschema_type(
        sql_type: (
            str  # noqa: ANN401
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine]
            | t.Any
        ),
    ) -> dict:
        if isinstance(sql_type, (str, sqlalchemy.types.TypeEngine)):
            return self.to_jsonschema_type_array(sql_type).type_dict

        if isinstance(sql_type, type):
            if issubclass(sql_type, sqlalchemy.types.TypeEngine):
                return self.to_jsonschema_type_array(sql_type).type_dict

            msg = f"Unexpected type received: '{sql_type.__name__}'"
            raise ValueError(msg)

        msg = f"Unexpected type received: '{type(sql_type).__name__}'"
        raise ValueError(msg)
    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        if config['driver'] == 'http':
            if config['secure']:
                secure_options = f"protocol=https&verify={config['verify']}"

                if not config['verify']:
                    # disable urllib3 warning
                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            else:
                secure_options = "protocol=http"
        else:
            secure_options = f"secure={config['secure']}&verify={config['verify']}"
        return (
            f"clickhouse+{config['driver']}://{config['username']}:{config['password']}@"
            f"{config['host']}:{config['port']}/"
            f"{config['database']}?{secure_options}"
        )

    def create_engine(self) -> Engine:
        return sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
        )

    @staticmethod
    def to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_jsonschema_type(from_type)

    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        schemas = super().get_schema_names(engine, inspected)

        # remove system tables
        try:
            schemas.remove('system')
            schemas.remove('INFORMATION_SCHEMA')
            schemas.remove('information_schema')
        except ValueError:
            pass

        return schemas

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            jsonschema_type: A dict

        Returns:
            SQLAlchemy type
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)


class ClickHouseStream(SQLStream):
    """Stream class for ClickHouse streams."""

    connector_class = ClickHouseConnector

    def get_records(self, partition: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.
        yield from super().get_records(partition)
