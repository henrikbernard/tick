"""Universal SQLAlchemy connection handling and dialect-specific metadata adapters."""

from __future__ import annotations

import importlib.util
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import make_url

from tick.models import CanonicalType, CdcDatabaseAudit


class DriverMissingError(RuntimeError):
    """Raised when a database dialect driver is not installed."""


DRIVER_INSTALL_HINTS: dict[str, tuple[str, str]] = {
    "postgresql": ("psycopg[binary]", 'pip install "tick-db[postgres]"'),
    "postgres": ("psycopg[binary]", 'pip install "tick-db[postgres]"'),
    "mssql": ("pyodbc", 'pip install "tick-db[mssql]"'),
    "duckdb": ("duckdb duckdb-engine", 'pip install "tick-db[duckdb]"'),
    "mysql": ("pymysql", 'pip install "tick-db[mysql]"'),
    "mariadb": ("pymysql", 'pip install "tick-db[mysql]"'),
    "snowflake": ("snowflake-sqlalchemy", 'pip install "tick-db[snowflake]"'),
    "bigquery": ("sqlalchemy-bigquery", 'pip install "tick-db[bigquery]"'),
    "oracle": ("oracledb", 'pip install "tick-db[oracle]"'),
    "databricks": ("databricks-sqlalchemy", 'pip install "tick-db[databricks]"'),
    "ibm_db_sa": ("ibm-db-sa", 'pip install "tick-db[db2]"'),
    "db2": ("ibm-db-sa", 'pip install "tick-db[db2]"'),
    "hana": ("sqlalchemy-hana", 'pip install "tick-db[hana]"'),
}


SUPPORTED_DRIVERS: list[dict[str, str]] = [
    {
        "dialect": "sqlite",
        "module": "sqlite3",
        "example_url": "sqlite:///path/to/database.db",
        "install": "Built into Python",
    },
    {
        "dialect": "duckdb",
        "module": "duckdb_engine",
        "example_url": "duckdb:///path/to/analytics.duckdb",
        "install": 'pip install "tick-db[duckdb]"',
    },
    {
        "dialect": "databricks (Unity Catalog)",
        "module": "databricks.sqlalchemy",
        "example_url": "databricks://token:dapi...@host?http_path=/sql/1.0/warehouses/...&catalog=main",
        "install": 'pip install "tick-db[databricks]"',
    },
    {
        "dialect": "postgresql (psycopg3)",
        "module": "psycopg",
        "example_url": "postgresql+psycopg://user:pass@host:5432/dbname",
        "install": 'pip install "tick-db[postgres]"',
    },
    {
        "dialect": "postgresql (psycopg2)",
        "module": "psycopg2",
        "example_url": "postgresql://user:pass@host:5432/dbname",
        "install": "pip install psycopg2-binary",
    },
    {
        "dialect": "mssql (SQL Server)",
        "module": "pyodbc",
        "example_url": "mssql+pyodbc://user:pass@host:1433/db?driver=ODBC+Driver+18+for+SQL+Server",
        "install": 'pip install "tick-db[mssql]"',
    },
    {
        "dialect": "mysql / mariadb",
        "module": "pymysql",
        "example_url": "mysql+pymysql://user:pass@host:3306/dbname",
        "install": 'pip install "tick-db[mysql]"',
    },
    {
        "dialect": "oracle",
        "module": "oracledb",
        "example_url": "oracle+oracledb://user:pass@host:1521/?service_name=orcl",
        "install": 'pip install "tick-db[oracle]"',
    },
    {
        "dialect": "ibm db2 / as400",
        "module": "ibm_db_sa",
        "example_url": "db2+ibm_db://user:pass@host:50000/dbname",
        "install": 'pip install "tick-db[db2]"',
    },
    {
        "dialect": "sap hana",
        "module": "sqlalchemy_hana",
        "example_url": "hana://user:pass@host:30015",
        "install": 'pip install "tick-db[hana]"',
    },
    {
        "dialect": "snowflake",
        "module": "snowflake.sqlalchemy",
        "example_url": "snowflake://user:pass@account/db/schema",
        "install": 'pip install "tick-db[snowflake]"',
    },
    {
        "dialect": "bigquery",
        "module": "sqlalchemy_bigquery",
        "example_url": "bigquery://gcp-project/dataset",
        "install": 'pip install "tick-db[bigquery]"',
    },
]


def _module_installed(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except (ImportError, ValueError):
        return False


def get_driver_status() -> list[dict[str, Any]]:
    """Return availability status of all supported SQLAlchemy database drivers."""
    return [{**item, "installed": _module_installed(item["module"])} for item in SUPPORTED_DRIVERS]


def resolve_connection_url(target: str) -> str:
    """Resolve file shortcuts and normalize dialect driver prefixes into a SQLAlchemy URL."""
    stripped = target.strip()
    if stripped == ":memory:":
        return "sqlite:///:memory:"

    if "://" not in stripped:
        path = Path(stripped).expanduser().resolve()
        suffix = path.suffix.lower()
        if suffix in {".duckdb", ".ddb"}:
            return f"duckdb:///{path}"
        if suffix in {".sqlite", ".sqlite3", ".db"}:
            return f"sqlite:///{path}"
        raise ValueError(
            f"Unrecognized connection target '{target}'. Provide a SQLAlchemy URL "
            "(e.g. postgresql://..., mssql+pyodbc://..., databricks://..., duckdb:///file.duckdb) "
            "or a local .duckdb / .sqlite / .db file path."
        )

    if stripped.startswith("postgres://"):
        stripped = "postgresql://" + stripped[len("postgres://") :]

    if stripped.startswith("postgresql://"):
        if not _module_installed("psycopg2") and _module_installed("psycopg"):
            stripped = "postgresql+psycopg://" + stripped[len("postgresql://") :]
    elif stripped.startswith("mysql://"):
        if not _module_installed("MySQLdb") and _module_installed("pymysql"):
            stripped = "mysql+pymysql://" + stripped[len("mysql://") :]
    elif stripped.startswith("mssql://"):
        if _module_installed("pyodbc"):
            stripped = "mssql+pyodbc://" + stripped[len("mssql://") :]
    elif stripped.startswith("oracle://"):
        if _module_installed("oracledb"):
            stripped = "oracle+oracledb://" + stripped[len("oracle://") :]
    elif stripped.startswith("db2://"):
        if _module_installed("ibm_db_sa"):
            stripped = "db2+ibm_db://" + stripped[len("db2://") :]

    return stripped


def sanitize_url(url: str) -> str:
    """Return a URL string with any password/token masked."""
    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:
        return re.sub(r"://([^:/@]+):([^@]+)@", r"://\1:***@", url)


def create_db_engine(target: str, read_only: bool = True) -> tuple[Engine, str]:
    """Create a SQLAlchemy engine from a URL or file path, returning (engine, resolved_url)."""
    resolved = resolve_connection_url(target)
    url_obj = make_url(resolved)
    backend = url_obj.get_backend_name()

    connect_args: dict[str, Any] = {}
    if backend == "duckdb" and read_only and url_obj.database and url_obj.database != ":memory:":
        connect_args["read_only"] = True

    try:
        engine = create_engine(resolved, connect_args=connect_args, future=True)
        return engine, resolved
    except ModuleNotFoundError as exc:
        _, hint = DRIVER_INSTALL_HINTS.get(
            backend, (exc.name or backend, f"pip install {exc.name or backend}")
        )
        raise DriverMissingError(
            f"Missing driver for '{backend}' ({exc.name}). Install it with:\n  {hint}"
        ) from exc
    except Exception as exc:
        if "NoSuchModuleError" in type(exc).__name__ or "Can't load plugin" in str(exc):
            _, hint = DRIVER_INSTALL_HINTS.get(backend, (backend, f"pip install {backend}"))
            raise DriverMissingError(
                f"SQLAlchemy plugin for '{backend}' is not installed. Install it with:\n  {hint}"
            ) from exc
        raise


def normalize_type(raw_type: str) -> CanonicalType:
    """Map vendor-specific SQL data types to a canonical data engineering type."""
    t = raw_type.strip().upper()
    base = re.split(r"[\s(]", t, maxsplit=1)[0]

    if any(k in t for k in ("UUID", "UNIQUEIDENTIFIER", "GUID")):
        return "uuid"
    if any(k in t for k in ("JSON", "JSONB", "VARIANT", "SUPER", "OBJECT", "STRUCT", "MAP", "ARRAY")):
        return "json"
    if any(
        k in t
        for k in (
            "TIMESTAMP",
            "DATETIME",
            "SMALLDATETIME",
            "TIMESTAMPTZ",
            "DATETIME2",
            "DATETIMEOFFSET",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_LTZ",
        )
    ):
        return "timestamp"
    if base == "DATE":
        return "date"
    if base in {"TIME", "TIMETZ", "TIME_WITHOUT_TIME_ZONE"} or t.startswith("TIME"):
        return "time"
    if base in {"BOOL", "BOOLEAN", "BIT"}:
        return "boolean"
    if any(k in base for k in ("DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY", "NUMBER", "DEC")):
        if "(" in t and ",0)" in t.replace(" ", ""):
            return "integer"
        return "decimal"
    if any(
        k in base
        for k in (
            "INT",
            "SERIAL",
            "BIGSERIAL",
            "SMALLSERIAL",
            "HUGEINT",
            "UBIGINT",
            "UINTEGER",
            "SHORT",
            "LONG",
            "BYTEINT",
            "TINYINT",
            "SMALLINT",
        )
    ):
        return "integer"
    if any(k in base for k in ("FLOAT", "DOUBLE", "REAL", "BINARY_FLOAT", "BINARY_DOUBLE")):
        return "float"
    if any(k in base for k in ("BLOB", "BYTEA", "BINARY", "VARBINARY", "IMAGE", "BYTES", "RAW", "ROWVERSION")):
        return "binary"
    if any(
        k in base
        for k in (
            "CHAR",
            "TEXT",
            "STRING",
            "CLOB",
            "NVARCHAR",
            "NCHAR",
            "VARCHAR",
            "ENUM",
            "CITEXT",
            "XML",
            "INET",
            "CIDR",
            "NTEXT",
        )
    ):
        return "string"
    return "other"


def safe_rollback(conn: Connection) -> None:
    """Clear an aborted SQL transaction (e.g. PostgreSQL InFailedSqlTransaction) after a caught query error."""
    try:
        if conn.in_transaction():
            conn.rollback()
    except Exception:
        pass


@dataclass
class TableStats:
    row_count: int | None = None
    row_count_exact: bool = False
    size_bytes: int | None = None
    comment: str | None = None
    is_materialized_view: bool = False
    view_definition: str | None = None


@dataclass
class TableCdcProbe:
    cdc_enabled: bool | None = None
    change_tracking_enabled: bool | None = None
    replica_identity: str | None = None
    notes: list[str] = field(default_factory=list)


class DialectAdapter:
    """Base adapter encapsulating dialect-specific catalog, safety, CDC, and sampling queries."""

    name: str = "generic"
    sqlglot_dialect: str | None = None
    system_schemas: set[str] = {
        "information_schema",
        "INFORMATION_SCHEMA",
        "pg_catalog",
        "pg_toast",
        "sys",
        "performance_schema",
        "mysql",
    }

    def get_database_name(self, engine: Engine) -> str:
        db = engine.url.database
        if not db:
            catalog = engine.url.query.get("catalog")
            if catalog:
                return str(catalog)
            return engine.dialect.name
        if db == ":memory:":
            return "memory"
        if "/" in db or "\\" in db:
            return Path(db).name
        return str(db)

    def get_server_version(self, conn: Connection) -> str | None:
        info = getattr(conn.dialect, "server_version_info", None)
        if info:
            return ".".join(str(x) for x in info)
        return None

    def configure_safe_session(
        self,
        conn: Connection,
        read_uncommitted: bool = False,
        query_timeout_seconds: int | None = None,
    ) -> None:
        """Apply dialect-appropriate read-only / lock-free / timeout guardrails."""
        return None

    def audit_database_cdc(self, conn: Connection) -> CdcDatabaseAudit:
        return CdcDatabaseAudit(
            dialect=self.name,
            notes=["Source-native CDC inspection not specialized for this dialect; using schema & watermark heuristics."],
        )

    def audit_table_cdc(self, conn: Connection, schema_name: str) -> dict[str, TableCdcProbe]:
        return {}

    def get_table_stats(self, conn: Connection, schema_name: str) -> dict[str, TableStats]:
        return {}

    def get_schema_names(self, conn: Connection, inspector: Any) -> list[str]:
        return list(inspector.get_schema_names())

    def get_table_names(self, conn: Connection, inspector: Any, schema_name: str) -> list[str]:
        return list(inspector.get_table_names(schema=schema_name))

    def get_view_names(self, conn: Connection, inspector: Any, schema_name: str) -> list[str]:
        return list(inspector.get_view_names(schema=schema_name))

    def get_columns(
        self, conn: Connection, inspector: Any, schema_name: str, table_name: str
    ) -> list[dict[str, Any]]:
        return list(inspector.get_columns(table_name, schema=schema_name))

    def get_indexes(
        self, conn: Connection, inspector: Any, schema_name: str, table_name: str
    ) -> list[dict[str, Any]]:
        return list(inspector.get_indexes(table_name, schema=schema_name))

    def get_view_definition(
        self, conn: Connection, inspector: Any, schema_name: str, view_name: str
    ) -> str | None:
        try:
            definition = inspector.get_view_definition(view_name, schema=schema_name)
            return str(definition).strip() if definition else None
        except Exception:
            return None

    def get_materialized_views(self, conn: Connection, schema_name: str) -> list[str]:
        return []

    def quote_identifier(self, conn: Connection, name: str) -> str:
        return conn.dialect.identifier_preparer.quote(name)

    def quote_table(
        self, conn: Connection, schema_name: str, table_name: str, default_schema: str | None
    ) -> str:
        q_table = self.quote_identifier(conn, table_name)
        if schema_name and schema_name != default_schema:
            quoted_schema = ".".join(self.quote_identifier(conn, p) for p in schema_name.split("."))
            return f"{quoted_schema}.{q_table}"
        return q_table

    def build_sample_query(
        self,
        quoted_table: str,
        columns_sql: str,
        sample_size: int,
        total_rows: int | None,
        *,
        use_tablesample: bool = False,
        read_uncommitted: bool = False,
    ) -> str:
        return f"SELECT {columns_sql} FROM {quoted_table} LIMIT {sample_size}"


class PostgresAdapter(DialectAdapter):
    name = "postgresql"
    sqlglot_dialect = "postgres"
    system_schemas = {"pg_catalog", "information_schema", "pg_toast"}

    def get_server_version(self, conn: Connection) -> str | None:
        try:
            row = conn.execute(text("SHOW server_version")).scalar()
            return str(row) if row else super().get_server_version(conn)
        except Exception:
            safe_rollback(conn)
            return super().get_server_version(conn)

    def configure_safe_session(
        self,
        conn: Connection,
        read_uncommitted: bool = False,
        query_timeout_seconds: int | None = None,
    ) -> None:
        try:
            if query_timeout_seconds and query_timeout_seconds > 0:
                conn.execute(text(f"SET statement_timeout = {int(query_timeout_seconds * 1000)}"))
        except Exception:
            safe_rollback(conn)

    def audit_database_cdc(self, conn: Connection) -> CdcDatabaseAudit:
        wal_level: str | None = None
        notes: list[str] = []
        try:
            val = conn.execute(text("SHOW wal_level")).scalar()
            wal_level = str(val) if val else None
            if wal_level == "logical":
                notes.append("PostgreSQL wal_level='logical' — ready for Databricks Lakeflow Connect logical replication.")
            else:
                notes.append(f"PostgreSQL wal_level='{wal_level}' (must be 'logical' for Lakeflow Connect / Debezium CDC).")
        except Exception as exc:
            safe_rollback(conn)
            notes.append(f"Could not inspect wal_level ({type(exc).__name__}); verify logical replication permissions.")
        return CdcDatabaseAudit(
            dialect=self.name,
            db_cdc_enabled=(wal_level == "logical") if wal_level is not None else None,
            wal_level=wal_level,
            notes=notes,
        )

    def audit_table_cdc(self, conn: Connection, schema_name: str) -> dict[str, TableCdcProbe]:
        probes: dict[str, TableCdcProbe] = {}
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT c.relname AS table_name, c.relreplident AS repl_ident
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = :schema AND c.relkind IN ('r', 'p')
                    """
                ),
                {"schema": schema_name},
            ).mappings()
            ident_map = {"d": "default", "n": "nothing", "f": "full", "i": "index"}
            for row in rows:
                ri = ident_map.get(str(row["repl_ident"]), str(row["repl_ident"]))
                probes[row["table_name"]] = TableCdcProbe(replica_identity=ri)
        except Exception:
            safe_rollback(conn)
        return probes

    def get_table_stats(self, conn: Connection, schema_name: str) -> dict[str, TableStats]:
        stats: dict[str, TableStats] = {}
        query = text(
            """
            SELECT
                c.relname AS table_name,
                c.relkind AS relkind,
                CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint ELSE NULL END AS est_rows,
                pg_total_relation_size(c.oid) AS total_bytes,
                obj_description(c.oid, 'pg_class') AS comment
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema
              AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
            """
        )
        try:
            for row in conn.execute(query, {"schema": schema_name}).mappings():
                stats[row["table_name"]] = TableStats(
                    row_count=int(row["est_rows"]) if row["est_rows"] is not None else None,
                    row_count_exact=False,
                    size_bytes=int(row["total_bytes"]) if row["total_bytes"] is not None else None,
                    comment=row["comment"],
                    is_materialized_view=(row["relkind"] == "m"),
                )
        except Exception:
            safe_rollback(conn)
        return stats

    def get_materialized_views(self, conn: Connection, schema_name: str) -> list[str]:
        try:
            rows = conn.execute(
                text("SELECT matviewname FROM pg_matviews WHERE schemaname = :schema"),
                {"schema": schema_name},
            ).scalars()
            return list(rows)
        except Exception:
            safe_rollback(conn)
            return []

    def get_view_definition(
        self, conn: Connection, inspector: Any, schema_name: str, view_name: str
    ) -> str | None:
        definition = super().get_view_definition(conn, inspector, schema_name, view_name)
        if definition:
            return definition
        try:
            res = conn.execute(
                text(
                    "SELECT definition FROM pg_matviews WHERE schemaname = :schema AND matviewname = :name"
                ),
                {"schema": schema_name, "name": view_name},
            ).scalar()
            return str(res).strip() if res else None
        except Exception:
            safe_rollback(conn)
            return None

    def build_sample_query(
        self,
        quoted_table: str,
        columns_sql: str,
        sample_size: int,
        total_rows: int | None,
        *,
        use_tablesample: bool = False,
        read_uncommitted: bool = False,
    ) -> str:
        if use_tablesample and total_rows and total_rows > sample_size * 4:
            pct = max(0.1, min(25.0, round((sample_size * 2.0 / total_rows) * 100.0, 2)))
            return f"SELECT {columns_sql} FROM {quoted_table} TABLESAMPLE SYSTEM ({pct}) LIMIT {sample_size}"
        return f"SELECT {columns_sql} FROM {quoted_table} LIMIT {sample_size}"


class SqlServerAdapter(DialectAdapter):
    name = "mssql"
    sqlglot_dialect = "tsql"
    system_schemas = {
        "sys",
        "INFORMATION_SCHEMA",
        "information_schema",
        "guest",
        "db_owner",
        "db_accessadmin",
        "db_securityadmin",
        "db_ddladmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_denydatareader",
        "db_denydatawriter",
    }

    def get_server_version(self, conn: Connection) -> str | None:
        try:
            val = conn.execute(text("SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))")).scalar()
            return str(val) if val else super().get_server_version(conn)
        except Exception:
            return super().get_server_version(conn)

    def configure_safe_session(
        self,
        conn: Connection,
        read_uncommitted: bool = False,
        query_timeout_seconds: int | None = None,
    ) -> None:
        try:
            if read_uncommitted:
                conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
            if query_timeout_seconds and query_timeout_seconds > 0:
                conn.execute(text(f"SET LOCK_TIMEOUT {int(query_timeout_seconds * 1000)}"))
        except Exception:
            pass

    def audit_database_cdc(self, conn: Connection) -> CdcDatabaseAudit:
        ct_enabled: bool | None = None
        cdc_enabled: bool | None = None
        notes: list[str] = []
        try:
            ct_row = conn.execute(
                text("SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID()")
            ).scalar()
            ct_enabled = bool(ct_row and int(ct_row) > 0)
        except Exception:
            notes.append("Could not query sys.change_tracking_databases (insufficient VIEW DATABASE STATE permission).")

        try:
            cdc_row = conn.execute(
                text("SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()")
            ).scalar()
            cdc_enabled = bool(cdc_row) if cdc_row is not None else None
        except Exception:
            notes.append("Could not query sys.databases.is_cdc_enabled.")

        if ct_enabled:
            notes.append("SQL Server Change Tracking is ENABLED (preferred lightweight mode for Databricks Lakeflow Connect).")
        elif cdc_enabled:
            notes.append("SQL Server CDC is ENABLED (compatible with Databricks Lakeflow Connect, including tables without PKs).")
        elif ct_enabled is False and cdc_enabled is False:
            notes.append("Neither Change Tracking nor SQL Server CDC is enabled on this database; enable CT or CDC for Lakeflow Connect.")

        return CdcDatabaseAudit(
            dialect=self.name,
            db_cdc_enabled=cdc_enabled,
            db_change_tracking_enabled=ct_enabled,
            notes=notes,
        )

    def audit_table_cdc(self, conn: Connection, schema_name: str) -> dict[str, TableCdcProbe]:
        probes: dict[str, TableCdcProbe] = {}
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        t.name AS table_name,
                        t.is_tracked_by_cdc AS cdc_on,
                        CASE WHEN ct.object_id IS NOT NULL THEN 1 ELSE 0 END AS ct_on
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = t.object_id
                    WHERE s.name = :schema
                    """
                ),
                {"schema": schema_name},
            ).mappings()
            for row in rows:
                probes[row["table_name"]] = TableCdcProbe(
                    cdc_enabled=bool(row["cdc_on"]),
                    change_tracking_enabled=bool(row["ct_on"]),
                )
        except Exception:
            pass
        return probes

    def get_table_stats(self, conn: Connection, schema_name: str) -> dict[str, TableStats]:
        stats: dict[str, TableStats] = {}
        query = text(
            """
            SELECT
                t.name AS table_name,
                SUM(p.rows) AS row_count,
                SUM(a.total_pages) * 8 * 1024 AS total_bytes
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON t.object_id = i.object_id
            JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE s.name = :schema AND i.index_id IN (0, 1)
            GROUP BY t.name
            """
        )
        try:
            for row in conn.execute(query, {"schema": schema_name}).mappings():
                stats[row["table_name"]] = TableStats(
                    row_count=int(row["row_count"]) if row["row_count"] is not None else None,
                    row_count_exact=True,
                    size_bytes=int(row["total_bytes"]) if row["total_bytes"] is not None else None,
                )
        except Exception:
            pass
        return stats

    def build_sample_query(
        self,
        quoted_table: str,
        columns_sql: str,
        sample_size: int,
        total_rows: int | None,
        *,
        use_tablesample: bool = False,
        read_uncommitted: bool = False,
    ) -> str:
        hint = " WITH (NOLOCK)" if read_uncommitted else ""
        if use_tablesample and total_rows and total_rows > sample_size * 4:
            # In T-SQL grammar, TABLESAMPLE must precede table hints such as WITH (NOLOCK)
            return f"SELECT TOP ({sample_size}) {columns_sql} FROM {quoted_table} TABLESAMPLE ({sample_size * 2} ROWS){hint}"
        return f"SELECT TOP ({sample_size}) {columns_sql} FROM {quoted_table}{hint}"


class DuckDBAdapter(DialectAdapter):
    name = "duckdb"
    sqlglot_dialect = "duckdb"
    system_schemas = {"information_schema", "pg_catalog", "system", "temp"}

    def get_server_version(self, conn: Connection) -> str | None:
        try:
            val = conn.execute(text("SELECT version()")).scalar()
            return str(val) if val else super().get_server_version(conn)
        except Exception:
            return super().get_server_version(conn)

    def get_schema_names(self, conn: Connection, inspector: Any) -> list[str]:
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT schema_name
                    FROM duckdb_schemas()
                    WHERE database_name = current_database()
                      AND NOT internal
                    ORDER BY schema_name
                    """
                )
            ).scalars()
            schemas = list(rows)
            if schemas:
                return schemas
        except Exception:
            pass
        return ["main"]

    def get_table_names(self, conn: Connection, inspector: Any, schema_name: str) -> list[str]:
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT table_name
                    FROM duckdb_tables()
                    WHERE database_name = current_database()
                      AND schema_name = :schema
                      AND NOT internal
                    ORDER BY table_name
                    """
                ),
                {"schema": schema_name},
            ).scalars()
            return list(rows)
        except Exception:
            return super().get_table_names(conn, inspector, schema_name)

    def get_view_names(self, conn: Connection, inspector: Any, schema_name: str) -> list[str]:
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT view_name
                    FROM duckdb_views()
                    WHERE database_name = current_database()
                      AND schema_name = :schema
                      AND NOT internal
                    ORDER BY view_name
                    """
                ),
                {"schema": schema_name},
            ).scalars()
            return list(rows)
        except Exception:
            return super().get_view_names(conn, inspector, schema_name)

    def get_columns(
        self, conn: Connection, inspector: Any, schema_name: str, table_name: str
    ) -> list[dict[str, Any]]:
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name, data_type, is_nullable, column_default, comment
                    FROM duckdb_columns()
                    WHERE database_name = current_database()
                      AND schema_name = :schema
                      AND table_name = :table
                      AND NOT internal
                    ORDER BY column_index
                    """
                ),
                {"schema": schema_name, "table": table_name},
            ).mappings()
            cols = [
                {
                    "name": row["column_name"],
                    "type": row["data_type"],
                    "nullable": bool(row["is_nullable"]),
                    "default": row["column_default"],
                    "comment": row["comment"],
                }
                for row in rows
            ]
            if cols:
                return cols
        except Exception:
            pass
        return super().get_columns(conn, inspector, schema_name, table_name)

    def get_indexes(
        self, conn: Connection, inspector: Any, schema_name: str, table_name: str
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT index_name, is_unique, sql
                    FROM duckdb_indexes()
                    WHERE database_name = current_database()
                      AND schema_name = :schema
                      AND table_name = :table
                    """
                ),
                {"schema": schema_name, "table": table_name},
            ).mappings()
            for row in rows:
                sql = str(row["sql"] or "")
                m = re.search(r"\(([^)]+)\)", sql)
                cols = [c.strip().strip('"') for c in m.group(1).split(",")] if m else []
                results.append(
                    {
                        "name": row["index_name"],
                        "column_names": cols,
                        "unique": bool(row["is_unique"]),
                    }
                )
        except Exception:
            pass
        return results

    def get_table_stats(self, conn: Connection, schema_name: str) -> dict[str, TableStats]:
        stats: dict[str, TableStats] = {}
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT table_name, estimated_size, comment
                    FROM duckdb_tables()
                    WHERE database_name = current_database()
                      AND schema_name = :schema
                      AND NOT internal
                    """
                ),
                {"schema": schema_name},
            ).mappings()
            for row in rows:
                stats[row["table_name"]] = TableStats(
                    row_count=int(row["estimated_size"]) if row["estimated_size"] is not None else None,
                    row_count_exact=False,
                    comment=row["comment"],
                )
        except Exception:
            pass

        try:
            vrows = conn.execute(
                text(
                    """
                    SELECT view_name, sql, comment
                    FROM duckdb_views()
                    WHERE database_name = current_database()
                      AND schema_name = :schema
                      AND NOT internal
                    """
                ),
                {"schema": schema_name},
            ).mappings()
            for row in vrows:
                stats[row["view_name"]] = TableStats(
                    comment=row["comment"],
                    view_definition=row["sql"],
                )
        except Exception:
            pass
        return stats

    def get_view_definition(
        self, conn: Connection, inspector: Any, schema_name: str, view_name: str
    ) -> str | None:
        try:
            val = conn.execute(
                text(
                    """
                    SELECT sql
                    FROM duckdb_views()
                    WHERE database_name = current_database()
                      AND schema_name = :schema
                      AND view_name = :name
                    """
                ),
                {"schema": schema_name, "name": view_name},
            ).scalar()
            if val:
                return str(val).strip()
        except Exception:
            pass
        return super().get_view_definition(conn, inspector, schema_name, view_name)

    def build_sample_query(
        self,
        quoted_table: str,
        columns_sql: str,
        sample_size: int,
        total_rows: int | None,
        *,
        use_tablesample: bool = False,
        read_uncommitted: bool = False,
    ) -> str:
        if use_tablesample and total_rows and total_rows > sample_size * 2:
            return f"SELECT {columns_sql} FROM {quoted_table} USING SAMPLE {sample_size} ROWS"
        return f"SELECT {columns_sql} FROM {quoted_table} LIMIT {sample_size}"


class SQLiteAdapter(DialectAdapter):
    name = "sqlite"
    sqlglot_dialect = "sqlite"
    system_schemas = set()

    def get_server_version(self, conn: Connection) -> str | None:
        try:
            val = conn.execute(text("SELECT sqlite_version()")).scalar()
            return str(val) if val else super().get_server_version(conn)
        except Exception:
            return super().get_server_version(conn)

    def get_table_stats(self, conn: Connection, schema_name: str) -> dict[str, TableStats]:
        stats: dict[str, TableStats] = {}
        try:
            rows = conn.execute(
                text("SELECT name, sql FROM sqlite_master WHERE type = 'view'")
            ).mappings()
            for row in rows:
                stats[row["name"]] = TableStats(view_definition=row["sql"])
        except Exception:
            pass
        try:
            size_rows = conn.execute(
                text("SELECT name, SUM(pgsize) AS size_bytes FROM dbstat GROUP BY name")
            ).mappings()
            for row in size_rows:
                existing = stats.get(row["name"], TableStats())
                existing.size_bytes = int(row["size_bytes"]) if row["size_bytes"] is not None else None
                stats[row["name"]] = existing
        except Exception:
            pass
        return stats

    def get_view_definition(
        self, conn: Connection, inspector: Any, schema_name: str, view_name: str
    ) -> str | None:
        try:
            val = conn.execute(
                text("SELECT sql FROM sqlite_master WHERE type = 'view' AND name = :name"),
                {"name": view_name},
            ).scalar()
            if val:
                return str(val).strip()
        except Exception:
            pass
        return super().get_view_definition(conn, inspector, schema_name, view_name)


class MySQLAdapter(DialectAdapter):
    name = "mysql"
    sqlglot_dialect = "mysql"
    system_schemas = {"information_schema", "mysql", "performance_schema", "sys"}

    def get_server_version(self, conn: Connection) -> str | None:
        try:
            val = conn.execute(text("SELECT VERSION()")).scalar()
            return str(val) if val else super().get_server_version(conn)
        except Exception:
            return super().get_server_version(conn)

    def configure_safe_session(
        self,
        conn: Connection,
        read_uncommitted: bool = False,
        query_timeout_seconds: int | None = None,
    ) -> None:
        try:
            if read_uncommitted:
                conn.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
            if query_timeout_seconds and query_timeout_seconds > 0:
                conn.execute(text(f"SET SESSION max_execution_time = {int(query_timeout_seconds * 1000)}"))
        except Exception:
            pass

    def audit_database_cdc(self, conn: Connection) -> CdcDatabaseAudit:
        fmt: str | None = None
        notes: list[str] = []
        try:
            row = conn.execute(text("SHOW VARIABLES LIKE 'binlog_format'")).first()
            if row:
                fmt = str(row[1])
                if fmt.upper() == "ROW":
                    notes.append("MySQL binlog_format='ROW' — ready for CDC / Lakeflow Connect.")
                else:
                    notes.append(f"MySQL binlog_format='{fmt}' (must be 'ROW' for binlog CDC).")
        except Exception:
            notes.append("Could not query MySQL binlog_format.")
        return CdcDatabaseAudit(
            dialect=self.name,
            db_cdc_enabled=(fmt.upper() == "ROW") if fmt else None,
            binlog_format=fmt,
            notes=notes,
        )

    def get_table_stats(self, conn: Connection, schema_name: str) -> dict[str, TableStats]:
        stats: dict[str, TableStats] = {}
        query = text(
            """
            SELECT table_name, table_rows,
                   COALESCE(data_length, 0) + COALESCE(index_length, 0) AS total_bytes,
                   table_comment
            FROM information_schema.tables
            WHERE table_schema = :schema
            """
        )
        try:
            for row in conn.execute(query, {"schema": schema_name}).mappings():
                comment = row["table_comment"]
                if comment and comment.upper() == "VIEW":
                    comment = None
                stats[row["table_name"]] = TableStats(
                    row_count=int(row["table_rows"]) if row["table_rows"] is not None else None,
                    row_count_exact=False,
                    size_bytes=int(row["total_bytes"]) if row["total_bytes"] is not None else None,
                    comment=comment or None,
                )
        except Exception:
            pass
        return stats


class DatabricksAdapter(DialectAdapter):
    name = "databricks"
    sqlglot_dialect = "databricks"
    system_schemas = {"information_schema", "INFORMATION_SCHEMA"}

    def get_server_version(self, conn: Connection) -> str | None:
        try:
            val = conn.execute(text("SELECT current_version().dbr_version")).scalar()
            return f"DBR {val}" if val else super().get_server_version(conn)
        except Exception:
            return super().get_server_version(conn)

    def audit_database_cdc(self, conn: Connection) -> CdcDatabaseAudit:
        return CdcDatabaseAudit(
            dialect=self.name,
            db_cdc_enabled=True,
            notes=["Databricks Unity Catalog Delta tables support Change Data Feed (delta.enableChangeDataFeed=true)."],
        )

    def get_table_stats(self, conn: Connection, schema_name: str) -> dict[str, TableStats]:
        stats: dict[str, TableStats] = {}
        try:
            rows = conn.execute(
                text(
                    """
                    SELECT table_name, table_type, comment
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                    """
                ),
                {"schema": schema_name},
            ).mappings()
            for row in rows:
                stats[row["table_name"]] = TableStats(
                    comment=row["comment"],
                    is_materialized_view=(str(row["table_type"]).upper() == "MATERIALIZED_VIEW"),
                )
        except Exception:
            pass
        return stats

    def build_sample_query(
        self,
        quoted_table: str,
        columns_sql: str,
        sample_size: int,
        total_rows: int | None,
        *,
        use_tablesample: bool = False,
        read_uncommitted: bool = False,
    ) -> str:
        if use_tablesample and total_rows and total_rows > sample_size * 2:
            return f"SELECT {columns_sql} FROM {quoted_table} TABLESAMPLE ({sample_size} ROWS)"
        return f"SELECT {columns_sql} FROM {quoted_table} LIMIT {sample_size}"


class OracleAdapter(DialectAdapter):
    name = "oracle"
    sqlglot_dialect = "oracle"
    system_schemas = {"SYS", "SYSTEM", "OUTLN", "XDB", "CTXSYS", "MDSYS", "ORDSYS", "DBSNMP"}

    def audit_database_cdc(self, conn: Connection) -> CdcDatabaseAudit:
        supp: str | None = None
        notes: list[str] = []
        try:
            row = conn.execute(
                text("SELECT log_mode, supplemental_log_data_min FROM v$database")
            ).first()
            if row:
                log_mode, supp = str(row[0]), str(row[1])
                ok = log_mode.upper() == "ARCHIVELOG" and supp.upper() in {"YES", "IMPLICIT"}
                notes.append(f"Oracle log_mode={log_mode}, supplemental_log_data_min={supp}.")
                return CdcDatabaseAudit(
                    dialect=self.name,
                    db_cdc_enabled=ok,
                    supplemental_logging=supp,
                    notes=notes,
                )
        except Exception:
            notes.append("Could not query v$database (requires SELECT_CATALOG_ROLE); verify ARCHIVELOG & supplemental logging.")
        return CdcDatabaseAudit(dialect=self.name, supplemental_logging=supp, notes=notes)

    def build_sample_query(
        self,
        quoted_table: str,
        columns_sql: str,
        sample_size: int,
        total_rows: int | None,
        *,
        use_tablesample: bool = False,
        read_uncommitted: bool = False,
    ) -> str:
        return f"SELECT {columns_sql} FROM {quoted_table} FETCH FIRST {sample_size} ROWS ONLY"


class DB2Adapter(DialectAdapter):
    name = "db2"
    sqlglot_dialect = None
    system_schemas = {"SYSIBM", "SYSCAT", "SYSSTAT", "SYSFUN", "SYSPROC", "SYSTOOLS", "NULLID"}

    def build_sample_query(
        self,
        quoted_table: str,
        columns_sql: str,
        sample_size: int,
        total_rows: int | None,
        *,
        use_tablesample: bool = False,
        read_uncommitted: bool = False,
    ) -> str:
        ur = " WITH UR" if read_uncommitted else ""
        return f"SELECT {columns_sql} FROM {quoted_table} FETCH FIRST {sample_size} ROWS ONLY{ur}"


class HanaAdapter(DialectAdapter):
    name = "hana"
    sqlglot_dialect = None
    system_schemas = {"SYS", "_SYS_BI", "_SYS_BIC", "_SYS_REPO", "_SYS_STATISTICS", "_SYS_XS"}


class SnowflakeAdapter(DialectAdapter):
    name = "snowflake"
    sqlglot_dialect = "snowflake"
    system_schemas = {"INFORMATION_SCHEMA", "information_schema"}


class BigQueryAdapter(DialectAdapter):
    name = "bigquery"
    sqlglot_dialect = "bigquery"
    system_schemas = {"INFORMATION_SCHEMA", "information_schema"}


def get_dialect_adapter(dialect_name: str) -> DialectAdapter:
    """Return the best matching dialect adapter for a SQLAlchemy dialect name."""
    name = dialect_name.lower()
    if name.startswith("postgres"):
        return PostgresAdapter()
    if name in {"mssql", "sqlserver"}:
        return SqlServerAdapter()
    if name == "duckdb":
        return DuckDBAdapter()
    if name == "sqlite":
        return SQLiteAdapter()
    if name in {"mysql", "mariadb"}:
        return MySQLAdapter()
    if name == "databricks":
        return DatabricksAdapter()
    if name == "snowflake":
        return SnowflakeAdapter()
    if name == "bigquery":
        return BigQueryAdapter()
    if name == "oracle":
        return OracleAdapter()
    if name in {"db2", "ibm_db_sa"}:
        return DB2Adapter()
    if name == "hana":
        return HanaAdapter()
    return DialectAdapter()
