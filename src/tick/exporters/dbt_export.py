"""Generate dbt sources.yml and staging SQL models from discovery results."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from tick.models import DiscoveryResult, RelationshipInfo, TableInfo


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", text.lower()).strip("_") or "default"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned


def _jinja_str(val: str) -> str:
    return val.replace("\\", "\\\\").replace("'", "\\'")


def render_dbt_sources_yaml(result: DiscoveryResult, source_prefix: str | None = None) -> str:
    """Render a dbt version 2 sources.yml with freshness, metadata, and data tests."""
    rel_by_source_col: dict[tuple[str, str], RelationshipInfo] = {}
    for rel in result.relationships:
        if rel.kind == "explicit_fk" or rel.confidence >= 0.85:
            rel_by_source_col.setdefault((rel.source_table, rel.source_column), rel)

    schemas_map: dict[str, list[TableInfo]] = {}
    included_tables: set[str] = set()
    for tbl in result.tables:
        if tbl.table_type == "table":
            schemas_map.setdefault(tbl.schema_name, []).append(tbl)
            included_tables.add(tbl.full_name)

    sources_list: list[dict[str, Any]] = []
    prefix = _slug(source_prefix or result.overview.database_name)

    for schema_name, tbls in sorted(schemas_map.items()):
        source_name = f"{prefix}_{_slug(schema_name)}" if len(schemas_map) > 1 else prefix
        source_tables: list[dict[str, Any]] = []

        for tbl in tbls:
            t_entry: dict[str, Any] = {"name": tbl.name}
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
                    t_entry["loaded_at_field"] = temporal_wm.column
                    t_entry["freshness"] = {
                        "warn_after": {"count": 24, "period": "hour"},
                        "error_after": {"count": 72, "period": "hour"},
                    }

            col_entries: list[dict[str, Any]] = []
            for col in tbl.columns:
                c_entry: dict[str, Any] = {"name": col.name}
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
                    target_schema, target_tbl = (
                        rel.target_table.split(".", 1)
                        if "." in rel.target_table
                        else (schema_name, rel.target_table)
                    )
                    tgt_source_name = (
                        f"{prefix}_{_slug(target_schema)}" if len(schemas_map) > 1 else prefix
                    )
                    tests.append(
                        {
                            "relationships": {
                                "to": f"source('{_jinja_str(tgt_source_name)}', '{_jinja_str(target_tbl)}')",
                                "field": rel.target_column,
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

        sources_list.append(
            {
                "name": source_name,
                "schema": schema_name,
                "description": f"Auto-discovered by tick from {result.overview.dialect} ({result.overview.database_name})",
                "tables": source_tables,
            }
        )

    payload = {"version": 2, "sources": sources_list}
    return yaml.safe_dump(payload, sort_keys=False, width=100)


def _render_staging_sql(source_name: str, tbl: TableInfo) -> str:
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
    for col in tbl.columns:
        base_alias = _slug(col.name)
        alias = base_alias
        suffix = 2
        while alias in seen_aliases:
            alias = f"{base_alias}_{suffix}"
            suffix += 1
        seen_aliases.add(alias)

        pii_comment = ""
        if col.profile and col.profile.pii_findings:
            cats = ", ".join(f.category for f in col.profile.pii_findings)
            pii_comment = f" -- PII: {cats}"
        if alias != col.name:
            escaped_raw = col.name.replace('"', '""')
            col_lines.append(f'        "{escaped_raw}" as {alias},{pii_comment}')
        else:
            col_lines.append(f"        {col.name},{pii_comment}")

    if col_lines:
        # Remove trailing comma on last column while preserving any inline PII comment
        last = col_lines[-1]
        if ", --" in last:
            col_lines[-1] = last.replace(", --", " --", 1)
        elif last.endswith(","):
            col_lines[-1] = last[:-1]

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
        distinct_schemas = {t.schema_name for t in base_tables}
        prefix = _slug(source_prefix or result.overview.database_name)
        staging_dir = root / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)

        seen_models: set[str] = set()
        for tbl in base_tables:
            source_name = (
                f"{prefix}_{_slug(tbl.schema_name)}" if len(distinct_schemas) > 1 else prefix
            )
            base_model = f"stg_{_slug(tbl.schema_name)}__{_slug(tbl.name)}"
            model_stem = base_model
            suffix = 2
            while model_stem in seen_models:
                model_stem = f"{base_model}_{suffix}"
                suffix += 1
            seen_models.add(model_stem)
            sql_path = staging_dir / f"{model_stem}.sql"
            sql_path.write_text(_render_staging_sql(source_name, tbl), encoding="utf-8")
            written.append(sql_path)

    return written
