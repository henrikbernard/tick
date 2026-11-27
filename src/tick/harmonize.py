"""Cross-plant / multi-source schema harmonization for unified Bronze/Silver Databricks ingestion."""

from __future__ import annotations

from pathlib import Path

from tick.databricks import (
    DECIMAL_PREC_RE,
    deduplicate_column_names,
    map_to_spark_type,
    sanitize_delta_column_name,
)
from tick.models import (
    DiscoveryResult,
    HarmonizationReport,
    HarmonizedColumn,
    HarmonizedTable,
    TableInfo,
)

# Widening hierarchy for Spark SQL types when plants disagree on column types
NUMERIC_WIDENING = {
    "SMALLINT": 1,
    "INT": 2,
    "BIGINT": 3,
    "DECIMAL": 4,
    "DOUBLE": 5,
}
_INT_PRECISION_MAP = {"SMALLINT": 5, "INT": 10, "BIGINT": 19}


def _widen_decimal_types(types: list[str]) -> str:
    """Compute a lossless Spark DECIMAL(p, s) covering all input integer/decimal Spark types."""
    max_int_digits = 0
    max_scale = 0
    for t in types:
        u = t.strip().upper()
        base = u.split("(", 1)[0]
        if base in _INT_PRECISION_MAP:
            max_int_digits = max(max_int_digits, _INT_PRECISION_MAP[base])
        else:
            m = DECIMAL_PREC_RE.search(u)
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
    return f"DECIMAL({prec}, {scale})"


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
            return _widen_decimal_types(unique_exact), True
        if "BIGINT" in bases:
            return "BIGINT", True
        return "INT", True

    # Date + Timestamp -> TIMESTAMP
    if set(bases) <= {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ"}:
        return ("TIMESTAMP" if "TIMESTAMP" in bases else "TIMESTAMP_NTZ"), True

    # Mixed string/numeric/other -> safe STRING widening
    return "STRING", True


def harmonize_snapshots(
    sources: dict[str, DiscoveryResult],
    catalog: str = "manufacturing",
    bronze_schema: str = "bronze",
) -> HarmonizationReport:
    """Compare N plant/site discovery snapshots and generate a unified multi-plant Bronze/Silver schema."""
    source_names = list(sources.keys())
    safe_site_map = deduplicate_column_names(source_names)
    cat_esc = catalog.replace("`", "``")
    bsch_esc = bronze_schema.replace("`", "``")

    # Index base tables across sites; qualify with schema if a single site has duplicate table names across schemas
    dup_names_within_source: set[str] = set()
    for res in sources.values():
        seen_in_src: set[str] = set()
        for tbl in res.tables:
            if tbl.table_type != "table":
                continue
            low = tbl.name.lower()
            if low in seen_in_src:
                dup_names_within_source.add(low)
            seen_in_src.add(low)

    tables_by_name: dict[str, dict[str, TableInfo]] = {}
    for src_name, res in sources.items():
        for tbl in res.tables:
            if tbl.table_type != "table":
                continue
            key = (
                f"{tbl.schema_name.lower()}.{tbl.name.lower()}"
                if tbl.name.lower() in dup_names_within_source
                else tbl.name.lower()
            )
            tables_by_name.setdefault(key, {})[src_name] = tbl

    harmonized_tables: list[HarmonizedTable] = []
    fully_aligned = 0

    for tbl_key in sorted(tables_by_name.keys()):
        site_map = tables_by_name[tbl_key]
        present = [s for s in source_names if s in site_map]
        missing = [s for s in source_names if s not in site_map]

        pks_by_source = {s: site_map[s].primary_key for s in present}
        unique_pks = {tuple(v) for v in pks_by_source.values()}
        pk_divergent = len(unique_pks) > 1

        # Gather union of all columns (matched by deduplicated sanitized name per site)
        col_order: list[str] = []
        col_sites: dict[str, dict[str, tuple[str, str, str, str, bool]]] = {}
        for s in present:
            col_map = deduplicate_column_names([c.name for c in site_map[s].columns])
            for col in site_map[s].columns:
                ck = col_map[col.name]
                if ck not in col_sites:
                    col_order.append(ck)
                    col_sites[ck] = {}
                spark_t = col.spark_type or map_to_spark_type(col.raw_type, col.canonical_type)
                col_sites[ck][s] = (col.name, col.raw_type, spark_t, col.canonical_type, col.nullable)

        h_cols: list[HarmonizedColumn] = []
        conflicts = 0
        site_specific = 0

        for ck in col_order:
            smap = col_sites[ck]
            c_present = [s for s in present if s in smap]
            c_missing = [s for s in present if s not in smap]
            raw_types = {s: smap[s][1] for s in c_present}
            spark_types = {s: smap[s][2] for s in c_present}
            canonical_by_src = {s: smap[s][3] for s in c_present}
            target_spark, has_conflict = _widen_spark_types(list(spark_types.values()))
            cast_needed = [s for s in c_present if spark_types[s] != target_spark]
            nullable = bool(c_missing) or any(smap[s][4] for s in c_present)

            if has_conflict:
                conflicts += 1
            if c_missing:
                site_specific += 1

            display_name = smap[c_present[0]][0]
            h_cols.append(
                HarmonizedColumn(
                    name=display_name,
                    sanitized_name=ck,
                    present_in_sources=c_present,
                    missing_in_sources=c_missing,
                    types_by_source=raw_types,
                    canonical_types_by_source=canonical_by_src,
                    target_spark_type=target_spark,
                    has_type_conflict=has_conflict,
                    nullable_in_target=nullable,
                    cast_required_sources=cast_needed,
                )
            )

        # Generate Unified Multi-Plant Databricks UNION ALL SQL
        union_branches: list[str] = []
        safe_tbl = sanitize_delta_column_name(tbl_key)
        for s in present:
            safe_site = safe_site_map[s]
            escaped_site_lit = s.replace("'", "''")
            selects = [f"    '{escaped_site_lit}' AS `source_plant`"]
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
                + f"\nFROM `{cat_esc}`.`{bsch_esc}_{safe_site}`.`{safe_tbl}`"
            )
            union_branches.append(branch)

        unified_sql = (
            f"CREATE OR REPLACE VIEW `{cat_esc}`.`silver`.`unified_{safe_tbl}` AS\n"
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
                primary_keys_by_source=pks_by_source,
                pk_divergent=pk_divergent,
                columns=h_cols,
                conflict_count=conflicts,
                site_specific_column_count=site_specific,
                unified_sql=unified_sql,
            )
        )

    return HarmonizationReport(
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
        lines.append(
            f"| `{tbl.table_name}` | {pres} | {miss} | {'YES' if tbl.pk_divergent else 'no'} | "
            f"{tbl.conflict_count} | {tbl.site_specific_column_count} |"
        )
    lines.append("")

    for tbl in report.tables:
        if (
            tbl.conflict_count == 0
            and tbl.site_specific_column_count == 0
            and not tbl.missing_in_sources
            and not tbl.pk_divergent
        ):
            continue
        lines.append(f"### `{tbl.table_name}` — Harmonization Plan")
        lines.append("")
        if tbl.pk_divergent:
            pk_desc = ", ".join(
                f"`{s}`: `({', '.join(pks) or 'none'})`"
                for s, pks in tbl.primary_keys_by_source.items()
            )
            lines.append(f"- **Primary Key Divergence:** {pk_desc}")
            lines.append("")
        drifted_cols = [
            col for col in tbl.columns if col.has_type_conflict or col.missing_in_sources
        ]
        if drifted_cols:
            lines.append("| Target Column | Target Spark Type | Type Conflict | Types by Source | Missing In |")
            lines.append("|---|---|---|---|---|")
            for col in drifted_cols:
                tsrc = ", ".join(f"`{s}`: `{t}`" for s, t in col.types_by_source.items())
                miss = ", ".join(f"`{s}`" for s in col.missing_in_sources) if col.missing_in_sources else "—"
                lines.append(
                    f"| `{col.sanitized_name}` | `{col.target_spark_type}` | "
                    f"{'**YES**' if col.has_type_conflict else 'no'} | {tsrc} | {miss} |"
                )
            lines.append("")
        lines.append("<details><summary>Unified Multi-Plant Databricks SQL</summary>")
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
