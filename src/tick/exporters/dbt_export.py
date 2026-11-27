"""Generate dbt sources.yml and staging SQL models from discovery results."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from tick.models import DiscoveryResult, RelationshipInfo, TableInfo


_SIMPLE_DBT_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_BACKTICK_DIALECTS = {"databricks", "bigquery", "mysql", "mariadb"}


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", text.lower()).strip("_") or "default"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned


def _jinja_str(val: str) -> str:
    return val.replace("\\", "\\\\").replace("'", "\\'")


def _quote_dbt_ident(val: str, dialect: str) -> str:
    if dialect.lower() in _BACKTICK_DIALECTS:
        return f"`{val.replace('`', '``')}`"
    return f'"{val.replace(chr(34), chr(34) * 2)}"'


def _build_schema_source_names(schemas: list[str], prefix: str) -> dict[str, str]:
    """Deterministically map each schema name to a unique dbt source name (avoids slug collisions)."""
    sorted_schemas = sorted(set(schemas))
    if len(sorted_schemas) <= 1:
        return {s: prefix for s in sorted_schemas}
    seen: set[str] = set()
    mapping: dict[str, str] = {}
    for schema_name in sorted_schemas:
        base = f"{prefix}_{_slug(schema_name)}"
        cand = base
        sfx = 2
        while cand in seen:
            cand = f"{base}_{sfx}"
            sfx += 1
        seen.add(cand)
        mapping[schema_name] = cand
    return mapping


def render_dbt_sources_yaml(result: DiscoveryResult, source_prefix: str | None = None) -> str:
    """Render a dbt version 2 sources.yml with freshness, metadata, and data tests."""
    dialect = result.overview.dialect
    rel_by_source_col: dict[tuple[str, str], RelationshipInfo] = {}
    for rel in result.relationships:
        if rel.kind == "explicit_fk" or rel.confidence >= 0.85:
            rel_by_source_col.setdefault((rel.source_table, rel.source_column), rel)

    schemas_map: dict[str, list[TableInfo]] = {}
    included_tables: dict[str, TableInfo] = {}
    for tbl in result.tables:
        if tbl.table_type == "table":
            schemas_map.setdefault(tbl.schema_name, []).append(tbl)
            included_tables[tbl.full_name] = tbl

    sources_list: list[dict[str, Any]] = []
    prefix = _slug(source_prefix or result.overview.database_name)
    source_name_map = _build_schema_source_names(list(schemas_map.keys()), prefix)

    for schema_name, tbls in sorted(schemas_map.items()):
        source_name = source_name_map[schema_name]
        source_tables: list[dict[str, Any]] = []

        for tbl in tbls:
            t_entry: dict[str, Any] = {"name": tbl.name}
            if not _SIMPLE_DBT_IDENT_RE.match(tbl.name):
                t_entry["quoting"] = {"identifier": True}
            desc_parts = []
            if tbl.comment:
                desc_parts.append(tbl.comment)
            if tbl.recommendations:
                desc_parts.append(
                    f"Discovered role: {tbl.recommendations.entity_role}; "
                    f"recommended ingestion: {tbl.recommendations.ingestion_strategy}."
                )
            if desc_parts:
                t_entry["description"] = " ".join(desc_parts)

            # Freshness config from best temporal watermark
            if tbl.recommendations:
                temporal_wm = next(
                    (
                        w
                        for w in tbl.recommendations.watermark_candidates
                        if w.kind in {"updated_at", "event_timestamp", "created_at"}
                    ),
                    None,
                )
                if temporal_wm:
                    loaded_field = (
                        temporal_wm.column
                        if _SIMPLE_DBT_IDENT_RE.match(temporal_wm.column)
                        else _quote_dbt_ident(temporal_wm.column, dialect)
                    )
                    t_entry["loaded_at_field"] = loaded_field
                    t_entry["freshness"] = {
                        "warn_after": {"count": 24, "period": "hour"},
                        "error_after": {"count": 72, "period": "hour"},
                    }

            col_entries: list[dict[str, Any]] = []
            for col in tbl.columns:
                c_entry: dict[str, Any] = {"name": col.name}
                if not _SIMPLE_DBT_IDENT_RE.match(col.name):
                    c_entry["quote"] = True
                if col.comment:
                    c_entry["description"] = col.comment

                tests: list[Any] = []
                if col.is_primary_key and len(tbl.primary_key) == 1:
                    tests.extend(["unique", "not_null"])
                else:
                    if not col.nullable:
                        tests.append("not_null")
                    if col.is_unique:
                        tests.append("unique")

                rel = rel_by_source_col.get((tbl.full_name, col.name))
                if rel and rel.target_table != tbl.full_name and rel.target_table in included_tables:
                    tgt_obj = included_tables[rel.target_table]
                    tgt_source_name = source_name_map[tgt_obj.schema_name]
                    rel_field = (
                        rel.target_column
                        if _SIMPLE_DBT_IDENT_RE.match(rel.target_column)
                        else _quote_dbt_ident(rel.target_column, dialect)
                    )
                    tests.append(
                        {
                            "relationships": {
                                "to": f"source('{_jinja_str(tgt_source_name)}', '{_jinja_str(tgt_obj.name)}')",
                                "field": rel_field,
                            }
                        }
                    )

                if tests:
                    c_entry["data_tests"] = tests

                if col.profile and col.profile.pii_findings:
                    c_entry["meta"] = {
                        "pii": True,
                        "pii_categories": [f.category for f in col.profile.pii_findings],
                    }

                col_entries.append(c_entry)

            t_entry["columns"] = col_entries
            source_tables.append(t_entry)

        src_entry: dict[str, Any] = {
            "name": source_name,
            "schema": schema_name,
            "description": f"Auto-discovered by tick from {result.overview.dialect} ({result.overview.database_name})",
            "tables": source_tables,
        }
        if not _SIMPLE_DBT_IDENT_RE.match(schema_name):
            src_entry["quoting"] = {"schema": True}
        sources_list.append(src_entry)

    payload = {"version": 2, "sources": sources_list}
    return yaml.safe_dump(payload, sort_keys=False, width=100)


def _render_staging_sql(source_name: str, tbl: TableInfo, dialect: str = "ansi") -> str:
    rec = tbl.recommendations
    header_lines = [
        f"-- Auto-generated staging model for {tbl.full_name}",
    ]
    if rec:
        header_lines.append(
            f"-- Role: {rec.entity_role} | Recommended strategy: {rec.ingestion_strategy}"
        )
        if rec.watermark_candidates:
            header_lines.append(
                f"-- Watermark candidate: {rec.watermark_candidates[0].column} ({rec.watermark_candidates[0].kind})"
            )

    col_lines: list[str] = []
    seen_aliases: set[str] = set()
    total_cols = len(tbl.columns)
    for idx, col in enumerate(tbl.columns):
        base_alias = _slug(col.name)
        alias = base_alias
        suffix = 2
        while alias in seen_aliases:
            alias = f"{base_alias}_{suffix}"
            suffix += 1
        seen_aliases.add(alias)

        comma = "" if idx == total_cols - 1 else ","
        pii_comment = ""
        if col.profile and col.profile.pii_findings:
            cats = ", ".join(f.category for f in col.profile.pii_findings)
            pii_comment = f" -- PII: {cats}"
        if alias != col.name:
            quoted_raw = _quote_dbt_ident(col.name, dialect)
            col_lines.append(f"        {quoted_raw} as {alias}{comma}{pii_comment}")
        else:
            col_lines.append(f"        {col.name}{comma}{pii_comment}")

    if not col_lines:
        col_lines.append("        *")

    body = [
        *header_lines,
        "",
        "with source as (",
        f"    select * from {{{{ source('{_jinja_str(source_name)}', '{_jinja_str(tbl.name)}') }}}}",
        "),",
        "",
        "renamed as (",
        "    select",
        *col_lines,
        "    from source",
        ")",
        "",
        "select * from renamed",
        "",
    ]
    return "\n".join(body)


def export_dbt_project(
    result: DiscoveryResult,
    output_dir: str | Path,
    source_prefix: str | None = None,
    generate_staging: bool = True,
) -> list[Path]:
    """Write dbt sources.yml and optional staging SQL models to output_dir."""
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    sources_path = root / "sources.yml"
    sources_path.write_text(
        render_dbt_sources_yaml(result, source_prefix=source_prefix), encoding="utf-8"
    )
    written.append(sources_path)

    if generate_staging:
        base_tables = [t for t in result.tables if t.table_type == "table"]
        prefix = _slug(source_prefix or result.overview.database_name)
        source_name_map = _build_schema_source_names([t.schema_name for t in base_tables], prefix)
        staging_dir = root / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)

        seen_models: set[str] = set()
        for tbl in base_tables:
            source_name = source_name_map[tbl.schema_name]
            base_model = f"stg_{_slug(tbl.schema_name)}__{_slug(tbl.name)}"
            model_stem = base_model
            suffix = 2
            while model_stem in seen_models:
                model_stem = f"{base_model}_{suffix}"
                suffix += 1
            seen_models.add(model_stem)
            sql_path = staging_dir / f"{model_stem}.sql"
            sql_path.write_text(
                _render_staging_sql(source_name, tbl, dialect=result.overview.dialect),
                encoding="utf-8",
            )
            written.append(sql_path)

    return written
