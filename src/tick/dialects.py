"""Universal SQLAlchemy connection handling and dialect-specific metadata adapters."""

from __future__ import annotations

import importlib.util
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import make_url

from tick.models import CanonicalType


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
    {
        "dialect": "oracle",
        "module": "oracledb",
        "example_url": "oracle+oracledb://user:pass@host:1521/?service_name=orcl",
        "install": 'pip install "tick-db[oracle]"',
    },
]


def _module_installed(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except (ImportError, ValueError):
        return False


def get_driver_status() -> list[dict[str, Any]]:
    """Return availability status of all supported SQLAlchemy database drivers."""
    results = []
    for item in SUPPORTED_DRIVERS:
        results.append(
            {
                **item,
                "installed": _module_installed(item["module"]),
            }
        )
    return results


def resolve_connection_url(target: str) -> str:
    """Resolve file shortcuts and normalize dialect driver prefixes into a SQLAlchemy URL."""
    stripped = target.strip()
    if stripped == ":memory:":
        return "sqlite:///:memory:"

    # Local file path shorthand (e.g. ./warehouse.duckdb, /data/app.sqlite)
    if "://" not in stripped:
        path = Path(stripped).expanduser().resolve()
        suffix = path.suffix.lower()
        if suffix in {".duckdb", ".ddb"}:
            return f"duckdb:///{path}"
        if suffix in {".sqlite", ".sqlite3", ".db"}:
            return f"sqlite:///{path}"
        raise ValueError(
            f"Unrecognized connection target '{target}'. Provide a SQLAlchemy URL "
            "(e.g. postgresql://..., mssql+pyodbc://..., duckdb:///file.duckdb) "
            "or a local .duckdb / .sqlite / .db file path."
        )

    # Normalize heroku-style postgres:// URL
    if stripped.startswith("postgres://"):
        stripped = "postgresql://" + stripped[len("postgres://") :]

    # Auto-select installed driver when user supplies bare dialect prefix
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

    return stripped


def sanitize_url(url: str) -> str:
    """Return a URL string with any password masked."""
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
        pkg, hint = DRIVER_INSTALL_HINTS.get(
            backend, (exc.name or backend, f"pip install {exc.name or backend}")
        )
        raise DriverMissingError(
            f"Missing driver for '{backend}' ({exc.name}). Install it with:\n  {hint}"
        ) from exc
    except Exception as exc:
        if "NoSuchModuleError" in type(exc).__name__ or "Can't load plugin" in str(exc):
            pkg, hint = DRIVER_INSTALL_HINTS.get(backend, (backend, f"pip install {backend}"))
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
    if any(k in t for k in ("TIMESTAMP", "DATETIME", "SMALLDATETIME", "TIMESTAMPTZ", "DATETIME2", "DATETIMEOFFSET")):
        return "timestamp"
    if base == "DATE":
        return "date"
    if base in {"TIME", "TIMETZ", "TIME_WITHOUT_TIME_ZONE"} or t.startswith("TIME"):
        return "time"
    if base in {"BOOL", "BOOLEAN", "BIT"}:
        return "boolean"
    if any(k in base for k in ("DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY", "NUMBER")):
        if "(" in t and ",0)" in t.replace(" ", ""):
            return "integer"
        return "decimal"
    if any(k in base for k in ("INT", "SERIAL", "BIGSERIAL", "SMALLSERIAL", "HUGEINT", "UBIGINT", "UINTEGER", "SHORT", "LONG", "BYTEINT")):
        return "integer"
    if any(k in base for k in ("FLOAT", "DOUBLE", "REAL", "BINARY_FLOAT", "BINARY_DOUBLE")):
        return "float"
    if any(k in base for k in ("BLOB", "BYTEA", "BINARY", "VARBINARY", "IMAGE", "BYTES", "RAW")):
        return "binary"
    if any(k in base for k in ("CHAR", "TEXT", "STRING", "CLOB", "NVARCHAR", "NCHAR", "VARCHAR", "ENUM", "CITEXT", "XML", "INET", "CIDR")):
        return "string"
    return "other"


@dataclass
class TableStats:
    row_count: int | None = None
    row_count_exact: bool = False
    size_bytes: int | None = None
    comment: str | None = None
    is_materialized_view: bool = False
    view_definition: str | None = None


class DialectAdapter:
    """Base adapter encapsulating dialect-specific catalog & sampling queries."""

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
        self, quoted_table: str, columns_sql: str, sample_size: int, total_rows: int | None
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
            return super().get_server_version(conn)

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
            pass
        return stats

    def get_materialized_views(self, conn: Connection, schema_name: str) -> list[str]:
        try:
            rows = conn.execute(
                text("SELECT matviewname FROM pg_matviews WHERE schemaname = :schema"),
                {"schema": schema_name},
            ).scalars()
            return list(rows)
        except Exception:
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
            return None


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
        self, quoted_table: str, columns_sql: str, sample_size: int, total_rows: int | None
    ) -> str:
        return f"SELECT TOP ({sample_size}) {columns_sql} FROM {quoted_table}"


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
        # Try dbstat virtual table if compiled into sqlite
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


class SnowflakeAdapter(DialectAdapter):
    name = "snowflake"
    sqlglot_dialect = "snowflake"
    system_schemas = {"INFORMATION_SCHEMA", "information_schema"}


class BigQueryAdapter(DialectAdapter):
    name = "bigquery"
    sqlglot_dialect = "bigquery"
    system_schemas = {"INFORMATION_SCHEMA", "information_schema"}


class OracleAdapter(DialectAdapter):
    name = "oracle"
    sqlglot_dialect = "oracle"
    system_schemas = {"SYS", "SYSTEM", "OUTLN", "XDB", "CTXSYS", "MDSYS", "ORDSYS", "DBSNMP"}

    def build_sample_query(
        self, quoted_table: str, columns_sql: str, sample_size: int, total_rows: int | None
    ) -> str:
        return f"SELECT {columns_sql} FROM {quoted_table} FETCH FIRST {sample_size} ROWS ONLY"


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
    if name == "snowflake":
        return SnowflakeAdapter()
    if name == "bigquery":
        return BigQueryAdapter()
    if name == "oracle":
        return OracleAdapter()
    return DialectAdapter()
