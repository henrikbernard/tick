"""Database schema & catalog introspection across SQLAlchemy dialects."""

from __future__ import annotations

import fnmatch
import re
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

from tick.dialects import DialectAdapter, TableStats, normalize_type, safe_rollback
from tick.models import (
    ColumnInfo,
    DiscoveryConfig,
    ForeignKeyInfo,
    IndexInfo,
    SchemaSummary,
    TableInfo,
)


def _matches_patterns(value: str, includes: list[str], excludes: list[str]) -> bool:
    if includes and not any(fnmatch.fnmatchcase(value.lower(), p.lower()) for p in includes):
        return False
    if excludes and any(fnmatch.fnmatchcase(value.lower(), p.lower()) for p in excludes):
        return False
    return True


def _matches_table_patterns(
    obj_name: str, full_name: str, includes: list[str], excludes: list[str]
) -> bool:
    """Match table include/exclude globs against both short name and schema-qualified name."""
    if includes:
        if not (
            any(fnmatch.fnmatchcase(obj_name.lower(), p.lower()) for p in includes)
            or any(fnmatch.fnmatchcase(full_name.lower(), p.lower()) for p in includes)
        ):
            return False
    if excludes:
        if (
            any(fnmatch.fnmatchcase(obj_name.lower(), p.lower()) for p in excludes)
            or any(fnmatch.fnmatchcase(full_name.lower(), p.lower()) for p in excludes)
        ):
            return False
    return True


def _duckdb_foreign_keys(conn: Connection, schema_name: str, table_name: str) -> list[ForeignKeyInfo]:
    """Extract foreign keys from duckdb_constraints() as fallback for duckdb-engine."""
    fks: list[ForeignKeyInfo] = []
    try:
        rows = conn.execute(
            text(
                """
                SELECT constraint_text, constraint_column_names
                FROM duckdb_constraints()
                WHERE database_name = current_database()
                  AND schema_name = :schema
                  AND table_name = :table
                  AND constraint_type = 'FOREIGN KEY'
                """
            ),
            {"schema": schema_name, "table": table_name},
        ).mappings()
        for row in rows:
            ctext = str(row["constraint_text"] or "")
            m = re.search(
                r"FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(?:(?:\"?(\w+)\"?)\.)?\"?(\w+)\"?\s*\(([^)]+)\)",
                ctext,
                flags=re.IGNORECASE,
            )
            if m:
                src_cols = [c.strip().strip('"') for c in m.group(1).split(",")]
                ref_schema = m.group(2) or schema_name
                ref_table = m.group(3)
                ref_cols = [c.strip().strip('"') for c in m.group(4).split(",")]
                fks.append(
                    ForeignKeyInfo(
                        name=None,
                        constrained_columns=src_cols,
                        referred_schema=ref_schema,
                        referred_table=ref_table,
                        referred_columns=ref_cols,
                    )
                )
    except Exception:
        pass
    return fks


def _duckdb_primary_key(conn: Connection, schema_name: str, table_name: str) -> list[str]:
    try:
        rows = conn.execute(
            text(
                """
                SELECT constraint_column_names
                FROM duckdb_constraints()
                WHERE database_name = current_database()
                  AND schema_name = :schema
                  AND table_name = :table
                  AND constraint_type = 'PRIMARY KEY'
                """
            ),
            {"schema": schema_name, "table": table_name},
        ).scalars()
        for col_names in rows:
            if col_names:
                return list(col_names)
    except Exception:
        pass
    return []


def _duckdb_unique_columns(conn: Connection, schema_name: str, table_name: str) -> set[str]:
    uniques: set[str] = set()
    try:
        rows = conn.execute(
            text(
                """
                SELECT constraint_column_names
                FROM duckdb_constraints()
                WHERE database_name = current_database()
                  AND schema_name = :schema
                  AND table_name = :table
                  AND constraint_type = 'UNIQUE'
                """
            ),
            {"schema": schema_name, "table": table_name},
        ).scalars()
        for col_names in rows:
            if col_names and len(col_names) == 1:
                uniques.add(col_names[0])
    except Exception:
        pass
    return uniques


def _discover_schemas(
    conn: Connection, inspector: Any, adapter: DialectAdapter, config: DiscoveryConfig
) -> tuple[list[str], str | None]:
    default_schema = getattr(inspector, "default_schema_name", None)
    try:
        raw_schemas = adapter.get_schema_names(conn, inspector)
    except Exception:
        raw_schemas = [default_schema or "main"]

    filtered: list[str] = []
    for s in raw_schemas:
        if not s:
            continue
        if s in adapter.system_schemas and s not in config.include_schemas:
            continue
        if not _matches_patterns(s, config.include_schemas, config.exclude_schemas):
            continue
        filtered.append(s)

    if not filtered and default_schema and not config.include_schemas:
        filtered = [default_schema]
    return filtered, default_schema


def _count_rows_exact(conn: Connection, quoted_table: str) -> int | None:
    try:
        val = conn.execute(text(f"SELECT COUNT(*) FROM {quoted_table}")).scalar()
        return int(val) if val is not None else None
    except Exception:
        safe_rollback(conn)
        return None


def introspect_catalog(
    engine: Engine, adapter: DialectAdapter, config: DiscoveryConfig
) -> tuple[list[SchemaSummary], list[TableInfo], str | None]:
    """Introspect all schemas, tables, views, columns, constraints, and sizes."""
    inspector = inspect(engine)

    tables: list[TableInfo] = []
    schema_summaries: list[SchemaSummary] = []

    with engine.connect() as conn:
        adapter.configure_safe_session(
            conn,
            read_uncommitted=config.read_uncommitted,
            query_timeout_seconds=config.query_timeout_seconds,
        )
        schemas_list, default_schema = _discover_schemas(conn, inspector, adapter, config)
        for schema_name in schemas_list:
            stats_map: dict[str, TableStats] = adapter.get_table_stats(conn, schema_name)

            try:
                table_names = adapter.get_table_names(conn, inspector, schema_name)
            except Exception:
                table_names = []

            view_names: list[str] = []
            matview_names: list[str] = []
            if config.include_views:
                try:
                    view_names = adapter.get_view_names(conn, inspector, schema_name)
                except Exception:
                    view_names = []
                matview_names = adapter.get_materialized_views(conn, schema_name)

            # Avoid duplicate entries when dialects list matviews inside tables or views
            matview_set = set(matview_names)
            view_set = set(view_names) - matview_set
            table_set = [t for t in table_names if t not in matview_set and t not in view_set]

            objects: list[tuple[str, str]] = (
                [(t, "table") for t in sorted(table_set)]
                + [(v, "view") for v in sorted(view_set)]
                + [(mv, "materialized_view") for mv in sorted(matview_set)]
            )

            schema_table_count = 0
            schema_view_count = 0
            schema_rows = 0
            schema_size = 0
            has_any_size = False

            for obj_name, obj_type in objects:
                full_name = f"{schema_name}.{obj_name}"
                if not _matches_table_patterns(
                    obj_name, full_name, config.include_tables, config.exclude_tables
                ):
                    continue
                if config.max_tables is not None and len(tables) >= config.max_tables:
                    break

                # Skip SQLite internal tables
                if adapter.name == "sqlite" and obj_name.startswith("sqlite_"):
                    continue

                tbl_info = _introspect_object(
                    conn=conn,
                    inspector=inspector,
                    adapter=adapter,
                    schema_name=schema_name,
                    default_schema=default_schema,
                    obj_name=obj_name,
                    obj_type=obj_type,
                    stats=stats_map.get(obj_name),
                    config=config,
                )
                tables.append(tbl_info)

                if obj_type == "table":
                    schema_table_count += 1
                else:
                    schema_view_count += 1
                if tbl_info.row_count is not None and obj_type != "view":
                    schema_rows += tbl_info.row_count
                if tbl_info.size_bytes is not None:
                    schema_size += tbl_info.size_bytes
                    has_any_size = True

            schema_summaries.append(
                SchemaSummary(
                    name=schema_name,
                    table_count=schema_table_count,
                    view_count=schema_view_count,
                    total_rows=schema_rows,
                    total_size_bytes=schema_size if has_any_size else None,
                )
            )

    return schema_summaries, tables, default_schema


def _introspect_object(
    *,
    conn: Connection,
    inspector: Any,
    adapter: DialectAdapter,
    schema_name: str,
    default_schema: str | None,
    obj_name: str,
    obj_type: str,
    stats: TableStats | None,
    config: DiscoveryConfig,
) -> TableInfo:
    full_name = f"{schema_name}.{obj_name}"
    quoted_table = adapter.quote_table(conn, schema_name, obj_name, default_schema)

    # Columns
    try:
        raw_columns = adapter.get_columns(conn, inspector, schema_name, obj_name)
    except Exception:
        raw_columns = []

    # Primary key
    pk_cols: list[str] = []
    if obj_type == "table":
        if adapter.name == "duckdb":
            pk_cols = _duckdb_primary_key(conn, schema_name, obj_name)
        else:
            try:
                pk_constraint = inspector.get_pk_constraint(obj_name, schema=schema_name)
                pk_cols = list(pk_constraint.get("constrained_columns") or [])
            except Exception:
                pk_cols = []

    # Foreign keys
    foreign_keys: list[ForeignKeyInfo] = []
    fk_col_set: set[str] = set()
    if obj_type == "table":
        if adapter.name == "duckdb":
            foreign_keys = _duckdb_foreign_keys(conn, schema_name, obj_name)
            for fk in foreign_keys:
                fk_col_set.update(fk.constrained_columns)
        else:
            try:
                raw_fks = inspector.get_foreign_keys(obj_name, schema=schema_name)
            except Exception:
                raw_fks = []
            for fk in raw_fks:
                constrained = list(fk.get("constrained_columns") or [])
                referred = list(fk.get("referred_columns") or [])
                if not constrained or not fk.get("referred_table"):
                    continue
                options = fk.get("options") or {}
                foreign_keys.append(
                    ForeignKeyInfo(
                        name=fk.get("name"),
                        constrained_columns=constrained,
                        referred_schema=fk.get("referred_schema") or schema_name,
                        referred_table=fk["referred_table"],
                        referred_columns=referred,
                        on_delete=options.get("ondelete"),
                        on_update=options.get("onupdate"),
                    )
                )
                fk_col_set.update(constrained)

    # Indexes & unique constraints
    indexes: list[IndexInfo] = []
    indexed_cols: set[str] = set(pk_cols)
    unique_cols: set[str] = set()
    if len(pk_cols) == 1:
        unique_cols.add(pk_cols[0])

    if obj_type in {"table", "materialized_view"}:
        try:
            raw_indexes = adapter.get_indexes(conn, inspector, schema_name, obj_name)
        except Exception:
            raw_indexes = []
        for idx in raw_indexes:
            cols = [c for c in (idx.get("column_names") or []) if c]
            if not cols:
                continue
            is_unique = bool(idx.get("unique"))
            indexes.append(
                IndexInfo(
                    name=idx.get("name") or f"idx_{'_'.join(cols)}",
                    columns=cols,
                    unique=is_unique,
                    primary=False,
                )
            )
            indexed_cols.add(cols[0])  # leading index column supports lookups
            if is_unique and len(cols) == 1:
                unique_cols.add(cols[0])

        if adapter.name == "duckdb":
            unique_cols.update(_duckdb_unique_columns(conn, schema_name, obj_name))
        else:
            try:
                raw_uniques = inspector.get_unique_constraints(obj_name, schema=schema_name)
            except Exception:
                raw_uniques = []
            for uq in raw_uniques:
                cols = list(uq.get("column_names") or [])
                if len(cols) == 1:
                    unique_cols.add(cols[0])
                    indexed_cols.add(cols[0])

    # Table comment
    comment = stats.comment if stats else None
    if comment is None and obj_type == "table":
        try:
            c_info = inspector.get_table_comment(obj_name, schema=schema_name)
            comment = c_info.get("text") if isinstance(c_info, dict) else None
        except Exception:
            comment = None

    # View definition
    view_definition: str | None = None
    if obj_type in {"view", "materialized_view"}:
        view_definition = (stats.view_definition if stats else None) or adapter.get_view_definition(
            conn, inspector, schema_name, obj_name
        )

    # Row counts
    row_count = stats.row_count if stats else None
    row_count_exact = stats.row_count_exact if stats else False

    if obj_type == "table":
        if row_count is None or config.exact_row_counts or (adapter.name in {"sqlite", "duckdb"} and (row_count or 0) <= 500_000):
            exact = _count_rows_exact(conn, quoted_table)
            if exact is not None:
                row_count = exact
                row_count_exact = True
    elif obj_type == "materialized_view" and row_count is None:
        exact = _count_rows_exact(conn, quoted_table)
        if exact is not None:
            row_count = exact
            row_count_exact = True

    columns: list[ColumnInfo] = []
    pk_set = set(pk_cols)
    for idx, col in enumerate(raw_columns, start=1):
        col_name = str(col["name"])
        raw_type = str(col.get("type", "UNKNOWN"))
        columns.append(
            ColumnInfo(
                name=col_name,
                raw_type=raw_type,
                canonical_type=normalize_type(raw_type),
                nullable=bool(col.get("nullable", True)),
                default=str(col["default"]) if col.get("default") is not None else None,
                comment=col.get("comment"),
                ordinal_position=idx,
                is_primary_key=col_name in pk_set,
                is_foreign_key=col_name in fk_col_set,
                is_unique=col_name in unique_cols,
                is_indexed=col_name in indexed_cols,
            )
        )

    return TableInfo(
        schema_name=schema_name,
        name=obj_name,
        full_name=full_name,
        table_type=obj_type,  # type: ignore[arg-type]
        comment=comment,
        row_count=row_count,
        row_count_exact=row_count_exact,
        size_bytes=stats.size_bytes if stats else None,
        columns=columns,
        primary_key=pk_cols,
        foreign_keys=foreign_keys,
        indexes=indexes,
        view_definition=view_definition,
    )
