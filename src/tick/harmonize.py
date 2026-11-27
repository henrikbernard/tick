"""Cross-plant / multi-source schema harmonization for unified Bronze/Silver Databricks ingestion."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from tick.databricks import (
    DECIMAL_PREC_RE,
    deduplicate_column_names,
    map_to_spark_type,
    sanitize_delta_column_name,
)
from tick.glossary import (
    BUILTIN_COLUMN_GLOSSARY,
    BUILTIN_TABLE_GLOSSARY,
    load_custom_glossary,
)
from tick.models import (
    DiscoveryResult,
    HarmonizationReport,
    HarmonizedColumn,
    HarmonizedTable,
    TableInfo,
)
from tick.snowflake import (
    SF_DECIMAL_PREC_RE,
    deduplicate_snowflake_columns,
    map_to_snowflake_type,
    sanitize_snowflake_identifier,
)

# Widening hierarchy for Spark SQL types when plants disagree on column types
NUMERIC_WIDENING = {
    "SMALLINT": 1,
    "INT": 2,
    "BIGINT": 3,
    "DECIMAL": 4,
    "DOUBLE": 5,
}
_INT_PRECISION_MAP = {"SMALLINT": 5, "INT": 10, "INTEGER": 10, "BIGINT": 19}
SF_NUMERIC_WIDENING = {"SMALLINT", "INTEGER", "BIGINT", "NUMBER", "FLOAT"}


def _widen_decimal_types(types: list[str], prefix: str = "DECIMAL") -> str:
    """Compute a lossless DECIMAL/NUMBER(p, s) covering all input integer/decimal types."""
    max_int_digits = 0
    max_scale = 0
    for t in types:
        u = t.strip().upper()
        base = u.split("(", 1)[0]
        if base in _INT_PRECISION_MAP:
            max_int_digits = max(max_int_digits, _INT_PRECISION_MAP[base])
        else:
            m = DECIMAL_PREC_RE.search(u) or SF_DECIMAL_PREC_RE.search(u)
            if m:
                prec = int(m.group(1))
                scale = int(m.group(2)) if m.group(2) else 0
                max_int_digits = max(max_int_digits, max(0, prec - scale))
                max_scale = max(max_scale, scale)
            else:
                max_int_digits = max(max_int_digits, 14)
                max_scale = max(max_scale, 4)

    scale = min(max_scale, 38)
    prec = min(38, max(scale, max_int_digits + scale))
    if prec == 0:
        prec = 18
    return f"{prefix}({prec}, {scale})"


def _widen_spark_types(types: list[str]) -> tuple[str, bool]:
    """Given Spark types across plants for the same column, pick a safe widened target type."""
    unique_exact = sorted(set(types))
    if len(unique_exact) == 1:
        return unique_exact[0], False

    bases = [t.split("(")[0].upper() for t in unique_exact]
    # All numeric -> pick widest numeric
    if all(b in NUMERIC_WIDENING for b in bases):
        if "DOUBLE" in bases:
            return "DOUBLE", True
        if "DECIMAL" in bases:
            return _widen_decimal_types(unique_exact, prefix="DECIMAL"), True
        if "BIGINT" in bases:
            return "BIGINT", True
        return "INT", True

    # Date + Timestamp -> TIMESTAMP
    if set(bases) <= {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ"}:
        return ("TIMESTAMP" if "TIMESTAMP" in bases else "TIMESTAMP_NTZ"), True

    # Mixed string/numeric/other -> safe STRING widening
    return "STRING", True


def _widen_snowflake_types(types: list[str]) -> tuple[str, bool]:
    """Given Snowflake types across plants for the same column, pick a safe widened target type."""
    unique_exact = sorted(set(types))
    if len(unique_exact) == 1:
        return unique_exact[0], False

    bases = [t.split("(")[0].upper() for t in unique_exact]
    if all(b in SF_NUMERIC_WIDENING for b in bases):
        if "FLOAT" in bases:
            return "FLOAT", True
        if "NUMBER" in bases:
            return _widen_decimal_types(unique_exact, prefix="NUMBER"), True
        if "BIGINT" in bases:
            return "BIGINT", True
        return "INTEGER", True

    if set(bases) <= {"DATE", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP_NTZ"}:
        if "TIMESTAMP_TZ" in bases:
            return "TIMESTAMP_TZ", True
        return ("TIMESTAMP_LTZ" if "TIMESTAMP_LTZ" in bases else "TIMESTAMP_NTZ"), True

    return "VARCHAR", True


def _dq(val: str) -> str:
    return f'"{val.replace(chr(34), chr(34) * 2)}"'


def harmonize_snapshots(
    sources: dict[str, DiscoveryResult],
    catalog: str = "manufacturing",
    bronze_schema: str = "bronze",
    target_platform: Literal["databricks", "snowflake"] = "databricks",
    match_by_glossary: bool = False,
    glossary_path: str | Path | None = None,
) -> HarmonizationReport:
    """Compare N plant/site discovery snapshots and generate unified multi-plant Databricks or Snowflake SQL.

    When `match_by_glossary=True` (or `glossary_path` is provided), tables and columns from heterogeneous
    ERP/MES systems (e.g. SAP `AUFK.WERKS` vs MES `production_order_master.plant_code`) are aligned using
    their canonical glossary semantic names while still projecting each plant's native Bronze names in SQL.
    """
    use_glossary = bool(match_by_glossary or glossary_path)
    tbl_glossary: dict[str, tuple[str, str]] = {}
    col_glossary: dict[str, tuple[str, str]] = {}
    if use_glossary:
        tbl_glossary = dict(BUILTIN_TABLE_GLOSSARY)
        col_glossary = dict(BUILTIN_COLUMN_GLOSSARY)
        if glossary_path:
            user_t, user_c = load_custom_glossary(glossary_path)
            tbl_glossary.update(user_t)
            col_glossary.update(user_c)

    def _canon_table_name(tbl: TableInfo) -> str:
        if not use_glossary:
            return tbl.name.lower()
        hit = tbl_glossary.get(tbl.name.upper())
        if hit and hit[0]:
            return hit[0].lower()
        if tbl.semantic_name:
            return tbl.semantic_name.lower()
        return tbl.name.lower()

    def _canon_col_name(raw_col_name: str, existing_semantic: str | None) -> str:
        if not use_glossary:
            return raw_col_name
        hit = col_glossary.get(raw_col_name.upper())
        if hit and hit[0]:
            return hit[0]
        if existing_semantic:
            return existing_semantic
        return raw_col_name

    source_names = list(sources.keys())
    safe_site_map = deduplicate_column_names(source_names)
    sf_site_map = deduplicate_snowflake_columns(source_names)
    cat_esc = catalog.replace("`", "``")
    bsch_esc = bronze_schema.replace("`", "``")
    sf_db = catalog.upper() if catalog == "manufacturing" else catalog
    sf_bsch = bronze_schema.upper() if bronze_schema == "bronze" else bronze_schema

    # Index base tables across sites; qualify with schema if a single site has duplicate table keys across schemas
    dup_names_within_source: set[str] = set()
    for res in sources.values():
        seen_in_src: set[str] = set()
        for tbl in res.tables:
            if tbl.table_type != "table":
                continue
            low = _canon_table_name(tbl)
            if low in seen_in_src:
                dup_names_within_source.add(low)
            seen_in_src.add(low)

    tables_by_name: dict[str, dict[str, TableInfo]] = {}
    for src_name, res in sources.items():
        for tbl in res.tables:
            if tbl.table_type != "table":
                continue
            canon = _canon_table_name(tbl)
            key = (
                f"{tbl.schema_name.lower()}.{canon}"
                if canon in dup_names_within_source
                else canon
            )
            tables_by_name.setdefault(key, {})[src_name] = tbl

    harmonized_tables: list[HarmonizedTable] = []
    fully_aligned = 0
    sorted_tbl_keys = sorted(tables_by_name.keys())
    view_dbx_map = deduplicate_column_names(sorted_tbl_keys)
    view_sf_map = deduplicate_snowflake_columns(sorted_tbl_keys)

    for tbl_key in sorted_tbl_keys:
        site_map = tables_by_name[tbl_key]
        present = [s for s in source_names if s in site_map]
        missing = [s for s in source_names if s not in site_map]
        source_table_names = {s: site_map[s].name for s in present}
        tbl_matched_glossary = use_glossary and len({site_map[s].name.lower() for s in present}) > 1

        # Compare primary keys using canonical column names when glossary matching is enabled
        pks_by_source = {s: site_map[s].primary_key for s in present}
        if use_glossary:
            unique_pks = {
                tuple(
                    sanitize_delta_column_name(
                        _canon_col_name(
                            pk,
                            next((c.semantic_name for c in site_map[s].columns if c.name == pk), None),
                        )
                    )
                    for pk in site_map[s].primary_key
                )
                for s in present
            }
        else:
            unique_pks = {tuple(v) for v in pks_by_source.values()}
        pk_divergent = len(unique_pks) > 1

        # Gather union of all columns (matched by deduplicated sanitized/canonical name per site)
        col_order: list[str] = []
        col_sites: dict[str, dict[str, tuple[str, str, str, str, str, bool]]] = {}
        for s in present:
            canon_inputs = [_canon_col_name(c.name, c.semantic_name) for c in site_map[s].columns]
            # Map positional index safely even when multiple raw columns share identical raw/canonical strings
            seen_site_cols: set[str] = set()
            for col, canon_raw in zip(site_map[s].columns, canon_inputs):
                base_ck = sanitize_delta_column_name(canon_raw)
                ck = base_ck
                sfx = 2
                while ck in seen_site_cols:
                    ck = f"{base_ck}_{sfx}"
                    sfx += 1
                seen_site_cols.add(ck)
                if ck not in col_sites:
                    col_order.append(ck)
                    col_sites[ck] = {}
                spark_t = col.spark_type or map_to_spark_type(col.raw_type, col.canonical_type)
                sf_t = col.snowflake_type or map_to_snowflake_type(col.raw_type, col.canonical_type)
                col_sites[ck][s] = (col.name, col.raw_type, spark_t, sf_t, col.canonical_type, col.nullable)

        h_cols: list[HarmonizedColumn] = []
        conflicts = 0
        site_specific = 0

        for ck in col_order:
            smap = col_sites[ck]
            c_present = [s for s in present if s in smap]
            c_missing = [s for s in present if s not in smap]
            src_col_names = {s: smap[s][0] for s in c_present}
            col_matched_glossary = use_glossary and (
                len({sanitize_delta_column_name(smap[s][0]) for s in c_present}) > 1
                or any(sanitize_delta_column_name(smap[s][0]) != ck for s in c_present)
            )
            raw_types = {s: smap[s][1] for s in c_present}
            spark_types = {s: smap[s][2] for s in c_present}
            sf_types = {s: smap[s][3] for s in c_present}
            canonical_by_src = {s: smap[s][4] for s in c_present}
            target_spark, spark_conflict = _widen_spark_types(list(spark_types.values()))
            target_sf, sf_conflict = _widen_snowflake_types(list(sf_types.values()))
            if target_platform == "snowflake":
                has_conflict = sf_conflict
                cast_needed = [s for s in c_present if sf_types[s] != target_sf]
            else:
                has_conflict = spark_conflict
                cast_needed = [s for s in c_present if spark_types[s] != target_spark]
            nullable = bool(c_missing) or any(smap[s][5] for s in c_present)

            if has_conflict:
                conflicts += 1
            if c_missing:
                site_specific += 1

            display_name = ck if col_matched_glossary else smap[c_present[0]][0]
            h_cols.append(
                HarmonizedColumn(
                    name=display_name,
                    sanitized_name=ck,
                    present_in_sources=c_present,
                    missing_in_sources=c_missing,
                    source_column_names=src_col_names,
                    matched_via_glossary=col_matched_glossary,
                    types_by_source=raw_types,
                    canonical_types_by_source=canonical_by_src,
                    target_spark_type=target_spark,
                    target_snowflake_type=target_sf,
                    has_type_conflict=has_conflict,
                    nullable_in_target=nullable,
                    cast_required_sources=cast_needed,
                )
            )

        # Generate Unified Multi-Plant UNION ALL SQL (Databricks Spark SQL or Snowflake SQL)
        union_branches: list[str] = []
        if target_platform == "snowflake":
            sf_view_stem = view_sf_map[tbl_key]
            sf_col_aliases = deduplicate_snowflake_columns([hc.sanitized_name for hc in h_cols])
            existing_sf_aliases = set(sf_col_aliases.values())
            plant_col = "SOURCE_PLANT"
            if plant_col in existing_sf_aliases:
                plant_col = "_TICK_SOURCE_PLANT"
                sfx = 2
                while plant_col in existing_sf_aliases:
                    plant_col = f"_TICK_SOURCE_PLANT_{sfx}"
                    sfx += 1
            for s in present:
                sf_site = sf_site_map[s]
                src_tbl_obj = site_map[s]
                sf_src_tbl = sanitize_snowflake_identifier(
                    f"{src_tbl_obj.schema_name}_{src_tbl_obj.name}"
                    if "." in tbl_key
                    else src_tbl_obj.name
                )
                escaped_site_lit = s.replace("'", "''")
                selects = [f"    '{escaped_site_lit}' AS {_dq(plant_col)}"]
                for hc in h_cols:
                    sf_col_alias = sf_col_aliases[hc.sanitized_name]
                    tgt_t = hc.target_snowflake_type or "VARCHAR"
                    if s in hc.present_in_sources:
                        orig_col = col_sites[hc.sanitized_name][s][0]
                        if s in hc.cast_required_sources:
                            selects.append(f"    CAST({_dq(orig_col)} AS {tgt_t}) AS {_dq(sf_col_alias)}")
                        else:
                            selects.append(f"    {_dq(orig_col)} AS {_dq(sf_col_alias)}")
                    else:
                        selects.append(f"    CAST(NULL AS {tgt_t}) AS {_dq(sf_col_alias)}")
                branch = (
                    "SELECT\n"
                    + ",\n".join(selects)
                    + f"\nFROM {_dq(sf_db)}.{_dq(f'{sf_bsch}_{sf_site}')}.{_dq(sf_src_tbl)}"
                )
                union_branches.append(branch)

            unified_sql = (
                f"CREATE OR REPLACE VIEW {_dq(sf_db)}.\"SILVER\".{_dq(f'UNIFIED_{sf_view_stem}')} AS\n"
                + "\nUNION ALL\n".join(union_branches)
                + ";\n"
            )
        else:
            dbx_view_stem = view_dbx_map[tbl_key]
            existing_dbx_aliases = {hc.sanitized_name for hc in h_cols}
            plant_col_dbx = "source_plant"
            if plant_col_dbx in existing_dbx_aliases:
                plant_col_dbx = "_tick_source_plant"
                sfx = 2
                while plant_col_dbx in existing_dbx_aliases:
                    plant_col_dbx = f"_tick_source_plant_{sfx}"
                    sfx += 1
            for s in present:
                safe_site = safe_site_map[s]
                src_tbl_obj = site_map[s]
                safe_src_tbl = sanitize_delta_column_name(
                    f"{src_tbl_obj.schema_name}_{src_tbl_obj.name}"
                    if "." in tbl_key
                    else src_tbl_obj.name
                )
                escaped_site_lit = s.replace("'", "''")
                selects = [f"    '{escaped_site_lit}' AS `{plant_col_dbx}`"]
                for hc in h_cols:
                    if s in hc.present_in_sources:
                        orig_col = col_sites[hc.sanitized_name][s][0].replace("`", "``")
                        if s in hc.cast_required_sources:
                            selects.append(f"    CAST(`{orig_col}` AS {hc.target_spark_type}) AS `{hc.sanitized_name}`")
                        else:
                            selects.append(f"    `{orig_col}` AS `{hc.sanitized_name}`")
                    else:
                        selects.append(f"    CAST(NULL AS {hc.target_spark_type}) AS `{hc.sanitized_name}`")
                branch = (
                    "SELECT\n"
                    + ",\n".join(selects)
                    + f"\nFROM `{cat_esc}`.`{bsch_esc}_{safe_site}`.`{safe_src_tbl}`"
                )
                union_branches.append(branch)

            unified_sql = (
                f"CREATE OR REPLACE VIEW `{cat_esc}`.`silver`.`unified_{dbx_view_stem}` AS\n"
                + "\nUNION ALL\n".join(union_branches)
                + ";\n"
            )

        is_aligned = (not missing) and (not pk_divergent) and conflicts == 0 and site_specific == 0
        if is_aligned:
            fully_aligned += 1

        harmonized_tables.append(
            HarmonizedTable(
                table_name=tbl_key,
                present_in_sources=present,
                missing_in_sources=missing,
                source_table_names=source_table_names,
                matched_via_glossary=tbl_matched_glossary,
                primary_keys_by_source=pks_by_source,
                pk_divergent=pk_divergent,
                columns=h_cols,
                conflict_count=conflicts,
                site_specific_column_count=site_specific,
                unified_sql=unified_sql,
            )
        )

    return HarmonizationReport(
        target_platform=target_platform,
        glossary_matching_enabled=use_glossary,
        sources=source_names,
        total_unique_tables=len(harmonized_tables),
        fully_aligned_tables=fully_aligned,
        tables_with_drift=len(harmonized_tables) - fully_aligned,
        tables=harmonized_tables,
    )


def render_harmonization_markdown(report: HarmonizationReport) -> str:
    """Render a Multi-Plant Schema Harmonization Matrix as Markdown."""
    lines = [
        "# Multi-Plant Schema Harmonization Report",
        "",
        f"- **Target Platform:** `{'Snowflake' if report.target_platform == 'snowflake' else 'Databricks'}`"
        + (" · **Glossary-Aware Semantic Matching:** `ENABLED`" if report.glossary_matching_enabled else ""),
        f"- **Plants / Sources Compared ({len(report.sources)}):** " + ", ".join(f"`{s}`" for s in report.sources),
        f"- **Total Unique Tables:** `{report.total_unique_tables}`",
        f"- **Fully Aligned Tables:** `{report.fully_aligned_tables}`",
        f"- **Tables with Cross-Plant Drift:** `{report.tables_with_drift}`",
        "",
        "## Table Presence & Drift Matrix",
        "",
        "| Table | Present In | Missing In | PK Divergent | Type Conflicts | Site-Only Columns |",
        "|---|---|---|---|---:|---:|",
    ]
    for tbl in report.tables:
        pres = ", ".join(f"`{s}`" for s in tbl.present_in_sources)
        miss = ", ".join(f"`{s}`" for s in tbl.missing_in_sources) if tbl.missing_in_sources else "—"
        g_note = " *(glossary)*" if tbl.matched_via_glossary else ""
        lines.append(
            f"| `{tbl.table_name}`{g_note} | {pres} | {miss} | {'YES' if tbl.pk_divergent else 'no'} | "
            f"{tbl.conflict_count} | {tbl.site_specific_column_count} |"
        )
    lines.append("")

    for tbl in report.tables:
        has_glossary_cols = any(c.matched_via_glossary for c in tbl.columns)
        if (
            tbl.conflict_count == 0
            and tbl.site_specific_column_count == 0
            and not tbl.missing_in_sources
            and not tbl.pk_divergent
            and not tbl.matched_via_glossary
            and not has_glossary_cols
        ):
            continue
        lines.append(f"### `{tbl.table_name}` — Harmonization Plan")
        lines.append("")
        if tbl.matched_via_glossary and tbl.source_table_names:
            tmap_desc = ", ".join(f"`{s}`: `{t}`" for s, t in tbl.source_table_names.items())
            lines.append(f"- **Glossary-Matched Source Tables:** {tmap_desc}")
            lines.append("")
        if tbl.pk_divergent:
            pk_desc = ", ".join(
                f"`{s}`: `({', '.join(pks) or 'none'})`"
                for s, pks in tbl.primary_keys_by_source.items()
            )
            lines.append(f"- **Primary Key Divergence:** {pk_desc}")
            lines.append("")
        drifted_cols = [
            col
            for col in tbl.columns
            if col.has_type_conflict or col.missing_in_sources or col.matched_via_glossary
        ]
        if drifted_cols:
            type_col_hdr = "Target Snowflake Type" if report.target_platform == "snowflake" else "Target Spark Type"
            lines.append(f"| Target Column | {type_col_hdr} | Type Conflict | Source Columns & Types | Missing In |")
            lines.append("|---|---|---|---|---|")
            for col in drifted_cols:
                tsrc = ", ".join(
                    f"`{s}`: `{col.source_column_names.get(s, col.name)}` (`{t}`)"
                    if col.matched_via_glossary
                    else f"`{s}`: `{t}`"
                    for s, t in col.types_by_source.items()
                )
                miss = ", ".join(f"`{s}`" for s in col.missing_in_sources) if col.missing_in_sources else "—"
                tgt_type = (
                    (col.target_snowflake_type or col.target_spark_type)
                    if report.target_platform == "snowflake"
                    else col.target_spark_type
                )
                lines.append(
                    f"| `{col.sanitized_name}` | `{tgt_type}` | "
                    f"{'**YES**' if col.has_type_conflict else 'no'} | {tsrc} | {miss} |"
                )
            lines.append("")
        plat_label = "Snowflake" if report.target_platform == "snowflake" else "Databricks"
        lines.append(f"<details><summary>Unified Multi-Plant {plat_label} SQL</summary>")
        lines.append("")
        lines.append("```sql")
        lines.append(tbl.unified_sql.rstrip())
        lines.append("```")
        lines.append("</details>")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def render_harmonization_sql(report: HarmonizationReport) -> str:
    """Concatenate all unified multi-plant UNION ALL SQL views into a single SQL script."""
    header = [
        "-- Auto-generated by tick compare-sources",
        f"-- Harmonizing plants: {', '.join(report.sources)}",
        "",
    ]
    return "\n".join(header + [t.unified_sql for t in report.tables])


def export_harmonization(
    report: HarmonizationReport,
    *,
    md_out: str | Path | None = None,
    json_out: str | Path | None = None,
    sql_out: str | Path | None = None,
    html_out: str | Path | None = None,
) -> list[Path]:
    written: list[Path] = []
    if html_out:
        from tick.exporters.harmonize_html_export import export_harmonization_html

        written.append(export_harmonization_html(report, html_out))
    if md_out:
        p = Path(md_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(render_harmonization_markdown(report), encoding="utf-8")
        written.append(p)
    if json_out:
        p = Path(json_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(report.model_dump_json(indent=2), encoding="utf-8")
        written.append(p)
    if sql_out:
        p = Path(sql_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(render_harmonization_sql(report), encoding="utf-8")
        written.append(p)
    return written
