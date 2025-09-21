"""SQL client handling.

This includes ClickHouseStream and ClickHouseConnector.
"""

from __future__ import annotations

from typing import Iterable
from urllib.parse import quote

import sqlalchemy  # noqa: TCH002
from singer_sdk.sql import SQLConnector, SQLStream
from sqlalchemy.engine import Engine, Inspector
from singer_sdk.helpers.types import Context, Record


class ClickHouseConnector(SQLConnector):
    """Connects to the ClickHouse SQL source."""

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
            f"clickhouse+{config['driver']}://{quote(config['username'])}:{quote(config['password'])}@"
            f"{config['host']}:{config['port']}/"
            f"{config['database']}?{secure_options}"
        )

    def create_engine(self) -> Engine:
        return sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
        )

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


class ClickHouseStream(SQLStream):
    """Stream class for ClickHouse streams."""

    connector_class = ClickHouseConnector

    def get_records(self, context: Context | None) -> Iterable[Record]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:  # pragma: no cover
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        batch_size = self.config.get("batch_size", 10000)

        query = self.build_query(context=context)
        with self.connector._connect() as conn:  # noqa: SLF001
            result = conn.execution_options(stream_results=True).execute(query)

            # Use mappings() to get dictionary-like RowMapping objects
            mapped_result = result.mappings()

            while True:
                # Fetch batch of RowMapping objects
                batch = mapped_result.fetchmany(batch_size)
                if not batch:
                    break

                for row in batch:
                    # row is already a RowMapping (dict-like object)
                    # Convert to regular dict for consistency
                    yield dict(row)
