"""SQL client handling.

This includes ClickHouseStream and ClickHouseConnector.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Iterable
from urllib.parse import quote

import sqlalchemy  # noqa: TCH002
import requests  # noqa: TCH002
from singer_sdk.helpers.types import Context, Record
from singer_sdk.sql import SQLConnector, SQLStream
from sqlalchemy.engine import Engine, Inspector
from sqlalchemy.sql.selectable import Select
from sqlalchemy import Table
from urllib3.exceptions import ProtocolError

LOGGER = logging.getLogger(__name__)
DATETIME64_PRECISION_PATTERN = re.compile(r"datetime64\((\d+)\)")


class _StreamRetryFromScratch(Exception):
    """Raised to retry a stream read after a transient HTTP error."""


def _transient_http_read_errors() -> tuple[type[Exception], ...]:
    """Return exception types that may be retried before any rows are emitted."""
    return (
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        ProtocolError,
    )


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
            query_parts = [secure_options]
            http_timeout = config.get("http_timeout_seconds")
            if http_timeout is not None:
                query_parts.append(f"timeout={int(http_timeout)}")
            query_string = "&".join(query_parts)
        else:
            query_string = f"secure={config['secure']}&verify={config['verify']}"
        return (
            f"clickhouse+{config['driver']}://{quote(config['username'])}:{quote(config['password'])}@"
            f"{config['host']}:{config['port']}/"
            f"{config['database']}?{query_string}"
        )

    def create_engine(self) -> Engine:
        return sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
            pool_pre_ping=True,
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

    def _sqlalchemy_table(self) -> Table:
        """Return the reflected table for the stream's selected catalog properties."""
        selected_column_names = self.get_selected_schema()["properties"].keys()
        return self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )

    @property
    def _is_incremental_uuid_id(self) -> bool:
        """Return true when incremental sync uses uuid-like id values."""
        if self.replication_key != "id":
            return False

        table_column = self._sqlalchemy_table().columns.get(self.replication_key)
        if table_column is None:
            return False

        column_type_name = str(table_column.type).lower()
        if "uuid" in column_type_name:
            return True

        string_types = (
            sqlalchemy.types.String,
            sqlalchemy.types.Text,
            sqlalchemy.types.Unicode,
            sqlalchemy.types.UnicodeText,
            sqlalchemy.types.CHAR,
        )
        return isinstance(table_column.type, string_types)

    def _ordered_query(self, context: Context | None):
        """Build query and normalize DateTime64 column precision for the driver."""
        query = self.build_query(context=context)
        return self._normalize_datetime64_precision(query)

    def apply_query_filters(
        self,
        query: Select,
        table: Table,
        *,
        context: Context | None = None,
    ) -> Select:
        """Apply replication filters with ClickHouse-safe datetime bookmark values."""
        if self.replication_key:
            column = table.columns[self.replication_key]
            order_by = (
                sqlalchemy.nulls_first(column.asc())
                if self.supports_nulls_first
                else column.asc()
            )
            query = query.order_by(order_by)

            start_val = self.get_starting_replication_key_value(context)
            if start_val is not None:
                filter_value = self._coerce_replication_value(column, start_val)
                query = query.where(column >= filter_value)

        return query

    def _normalize_datetime64_precision(self, query):
        """Cast DateTime64 to scale 6 for HTTP TSV parsing (%f allows 6 digits max)."""
        selected_columns = list(query.selected_columns)
        normalized_columns = []
        updated = False

        for selected_column in selected_columns:
            type_name = str(selected_column.type).lower()
            if "datetime64" not in type_name:
                normalized_columns.append(selected_column)
                continue

            match = DATETIME64_PRECISION_PATTERN.search(type_name)
            precision = int(match.group(1)) if match else None
            if precision is not None and precision <= 6:
                normalized_columns.append(selected_column)
                continue

            label_name = selected_column.key or selected_column.name
            normalized_columns.append(
                sqlalchemy.func.toDateTime64(
                    selected_column,
                    sqlalchemy.literal_column("6"),
                ).label(label_name)
            )
            updated = True

        if not updated:
            return query

        return query.with_only_columns(*normalized_columns)

    def _coerce_replication_value(self, table_column, start_value):
        """Coerce replication value for safe ClickHouse comparisons."""
        type_name = str(table_column.type).lower()
        is_datetime_type = "datetime64" in type_name or "datetime" in type_name

        if is_datetime_type and isinstance(start_value, str):
            precision = 6
            match = DATETIME64_PRECISION_PATTERN.search(type_name)
            if match:
                precision = int(match.group(1))

            return sqlalchemy.func.parseDateTime64BestEffort(
                sqlalchemy.literal(start_value),
                sqlalchemy.literal_column(str(precision)),
            )

        return sqlalchemy.literal(start_value)

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

        if self._is_incremental_uuid_id:
            msg = (
                "Incremental replication key 'id' appears uuid-like. "
                "Use a monotonic key such as 'updated_at' or 'created_at', "
                "or switch the stream to full-table sync."
            )
            raise ValueError(msg)

        batch_size = self.config.get("batch_size", 10000)
        max_attempts = int(self.config.get("stream_retry_max_attempts", 3))
        retry_wait = float(self.config.get("stream_retry_wait_seconds", 2.0))
        transient_errors = _transient_http_read_errors()

        query = self._ordered_query(context=context)
        if self.replication_key:
            LOGGER.info(
                "Applying ORDER BY %s for incremental stream %s.",
                self.replication_key,
                self.name,
            )

        rows_yielded_total = 0
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                with self.connector._connect() as conn:  # noqa: SLF001
                    try:
                        result = conn.execution_options(
                            stream_results=True,
                        ).execute(query)
                    except transient_errors as exc:
                        last_error = exc
                        if rows_yielded_total == 0:
                            LOGGER.warning(
                                "Transient HTTP error executing query for stream "
                                "%s (%s/%s); %s",
                                self.name,
                                attempt,
                                max_attempts,
                                exc,
                            )
                            raise _StreamRetryFromScratch from exc
                        raise

                    mapped_result = result.mappings()

                    while True:
                        try:
                            batch = mapped_result.fetchmany(batch_size)
                        except transient_errors as exc:
                            last_error = exc
                            if rows_yielded_total == 0:
                                LOGGER.warning(
                                    "Transient HTTP error reading stream %s "
                                    "(%s/%s); %s",
                                    self.name,
                                    attempt,
                                    max_attempts,
                                    exc,
                                )
                                raise _StreamRetryFromScratch from exc
                            LOGGER.error(
                                "HTTP read failed after %s row(s) for stream "
                                "%s; use driver=native or fix timeouts. %s",
                                rows_yielded_total,
                                self.name,
                                exc,
                            )
                            raise

                        if not batch:
                            return

                        for row in batch:
                            rows_yielded_total += 1
                            yield dict(row)

            except _StreamRetryFromScratch:
                if attempt >= max_attempts:
                    if last_error is not None:
                        raise last_error
                    msg = "Stream retry exhausted without captured error."
                    raise RuntimeError(msg)
                time.sleep(retry_wait)
