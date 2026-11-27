"""Markdown data dictionary, Mermaid ERD/lineage, Databricks/Mfg signals, and diff exporters."""

from __future__ import annotations

import re
from pathlib import Path

from tick.models import DiscoveryResult, SnapshotDiff, TableInfo


def _fmt_bytes(num: int | None) -> str:
    if num is None:
        return "—"
    value = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024.0
    return f"{num} B"


def _fmt_int(num: int | None) -> str:
    return f"{num:,}" if num is not None else "—"


def _mermaid_id(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not cleaned:
        return "n_node"
    if cleaned[0].isdigit():
        return f"n_{cleaned}"
    return cleaned


def _escape_mermaid_label(val: str) -> str:
    return val.replace('"', "'").replace("\n", " ").replace("\r", " ")


def _escape_md(val: str | None) -> str:
    if not val:
        return "—"
    return val.replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def _md_code(val: str | None) -> str:
    if not val:
        return "—"
    escaped = _escape_md(val)
    if "`" in escaped:
        return f"`` {escaped} ``"
    return f"`{escaped}`"


def _render_mermaid_erd(result: DiscoveryResult) -> str:
    base_tables = [t for t in result.tables if t.table_type == "table"]
    if not base_tables:
        return ""

    lines = ["```mermaid", "erDiagram"]
    for tbl in base_tables[:40]:
        tid = _mermaid_id(tbl.full_name)
        lines.append(f"    {tid} {{")
        for col in tbl.columns[:25]:
            ctype = re.sub(r"[^A-Za-z0-9_]", "_", col.canonical_type)
            cname = _mermaid_id(col.name)
            key_flags = []
            if col.is_primary_key:
                key_flags.append("PK")
            if col.is_foreign_key:
                key_flags.append("FK")
            flag_str = f" {','.join(key_flags)}" if key_flags else ""
            lines.append(f"        {ctype} {cname}{flag_str}")
        lines.append("    }")

    for rel in result.relationships[:80]:
        src = _mermaid_id(rel.source_table)
        tgt = _mermaid_id(rel.target_table)
        if rel.kind == "explicit_fk":
            connector = "}o--||"
            label = _escape_mermaid_label(f"{rel.source_column} -> {rel.target_column}")
        else:
            connector = "}o..||"
            label = _escape_mermaid_label(
                f"inferred ({rel.confidence:.0%}) {rel.source_column} -> {rel.target_column}"
            )
        lines.append(f'    {src} {connector} {tgt} : "{label}"')

    lines.append("```")
    return "\n".join(lines)


def _render_mermaid_lineage(result: DiscoveryResult) -> str:
    view_edges = [e for e in result.lineage if e.edge_type == "view_dependency"]
    if not view_edges:
        return ""
    lines = ["```mermaid", "flowchart LR"]
    seen_nodes: set[str] = set()
    type_map = {t.full_name: t.table_type for t in result.tables}

    for edge in view_edges:
        for node in (edge.upstream_table, edge.downstream_table):
            if node not in seen_nodes:
                seen_nodes.add(node)
                nid = _mermaid_id(node)
                ttype = type_map.get(node, "table")
                safe_node = _escape_mermaid_label(node)
                if ttype == "table":
                    lines.append(f'    {nid}[("{safe_node}")]')
                else:
                    lines.append(f'    {nid}["{safe_node} ({ttype})"]')
        lines.append(
            f"    {_mermaid_id(edge.upstream_table)} --> {_mermaid_id(edge.downstream_table)}"
        )
    lines.append("```")
    return "\n".join(lines)


def _render_table_section(tbl: TableInfo) -> list[str]:
    lines: list[str] = []
    badge = tbl.table_type.upper()
    role = tbl.recommendations.entity_role if tbl.recommendations else "unknown"
    sem = f" — *{tbl.semantic_name}*" if tbl.semantic_name else ""
    lines.append(f"### `{tbl.full_name}`{sem} ({badge} · role: `{role}` · health: `{tbl.quality_score}/100`)")
    lines.append("")
    if tbl.business_description or tbl.comment:
        lines.append(f"> {tbl.business_description or tbl.comment}")
        lines.append("")

    meta_parts = [
        f"**Rows:** {_fmt_int(tbl.row_count)}{'' if tbl.row_count_exact or tbl.row_count is None else ' (est.)'}",
        f"**Size:** {_fmt_bytes(tbl.size_bytes)}",
        f"**PK:** `{', '.join(tbl.primary_key)}`" if tbl.primary_key else "**PK:** None",
        f"**Indexes:** {len(tbl.indexes)}",
    ]
    if tbl.cdc_readiness and tbl.table_type == "table":
        meta_parts.append(f"**CDC:** `{tbl.cdc_readiness.status}` (`{tbl.cdc_readiness.connector_recommendation}`)")
    lines.append(" | ".join(meta_parts))
    lines.append("")

    if tbl.skipped_profiling_reason:
        lines.append(f"- **Profiling skipped:** {tbl.skipped_profiling_reason}")

    if tbl.recommendations:
        rec = tbl.recommendations
        lines.append(
            f"- **Ingestion strategy:** `{rec.ingestion_strategy}` — {rec.ingestion_strategy_reason}"
        )
        if rec.watermark_candidates:
            wms = ", ".join(f"`{w.column}` ({w.kind})" for w in rec.watermark_candidates)
            lines.append(f"- **Watermark candidates:** {wms}")
        if tbl.delta_compatibility and tbl.delta_compatibility.liquid_clustering_keys:
            lck = ", ".join(f"`{k}`" for k in tbl.delta_compatibility.liquid_clustering_keys)
            lines.append(f"- **Databricks Liquid Clustering:** `CLUSTER BY ({lck})`")
        if tbl.snowflake_compatibility and tbl.snowflake_compatibility.clustering_keys:
            sck = ", ".join(f"`{k}`" for k in tbl.snowflake_compatibility.clustering_keys)
            lines.append(
                f"- **Snowflake Clustering & Warehouse:** `CLUSTER BY ({sck})` · Warehouse: `{tbl.snowflake_compatibility.recommended_warehouse_size}`"
            )
        if rec.primary_key_candidates:
            lines.append(
                f"- **Candidate PKs:** {', '.join(f'`{c}`' for c in rec.primary_key_candidates)}"
            )
    if tbl.telemetry_profile:
        tp = tbl.telemetry_profile
        lines.append(
            f"- **OT Telemetry:** `{tp.pattern}` (ts: `{tp.timestamp_column}`, "
            f"median cadence: `{tp.median_interval_seconds}s`, watermark delay: `{tp.recommended_watermark_delay}`)"
        )
    if tbl.hierarchy:
        lines.append(f"- **Recursive Hierarchy:** `{tbl.hierarchy.pattern}` — {tbl.hierarchy.reason}")
    if tbl.upstream_tables:
        lines.append(
            f"- **Upstream dependencies:** {', '.join(f'`{u}`' for u in tbl.upstream_tables)}"
        )
    lines.append("")

    lines.append(
        "| Column | Semantic Name | Type (Target) | Keys | Nullable | Null % | Distinct | Min / Max | PII / Gov / Legacy |"
    )
    lines.append("|---|---|---|---|---|---:|---:|---|---|")
    for col in tbl.columns:
        keys = []
        if col.is_primary_key:
            keys.append("PK")
        if col.is_foreign_key:
            keys.append("FK")
        if col.is_unique and not col.is_primary_key:
            keys.append("UQ")
        key_str = ", ".join(keys) or "—"

        prof = col.profile
        flags: list[str] = []
        if prof and not prof.profile_error:
            null_pct = f"{prof.null_rate:.1%}"
            distinct = _fmt_int(prof.distinct_count)
            minmax = (
                f"{_escape_md(prof.min_value)} .. {_escape_md(prof.max_value)}"
                if prof.min_value is not None
                else "—"
            )
            for f in prof.pii_findings:
                flags.append(f"PII:`{f.category}`")
            for g in prof.governance_findings:
                flags.append(f"UC:`{g.uc_policy}/{g.uc_tag_value}`")
            if prof.legacy_date:
                flags.append(f"LEGACY:`{prof.legacy_date.encoding}`")
        else:
            null_pct = distinct = minmax = "—"

        target_types: list[str] = []
        if col.spark_type:
            target_types.append(f"DBX:{_md_code(col.spark_type)}")
        if col.snowflake_type:
            target_types.append(f"SF:{_md_code(col.snowflake_type)}")
        type_note = f" → {' / '.join(target_types)}" if target_types else ""
        lines.append(
            f"| {_md_code(col.name)} | {_escape_md(col.semantic_name)} | {_md_code(col.raw_type)}{type_note} | {key_str} | "
            f"{'YES' if col.nullable else 'NO'} | {null_pct} | {distinct} | {minmax} | {_escape_md(', '.join(flags)) if flags else '—'} |"
        )

    lines.append("")
    if tbl.view_definition:
        lines.append("<details><summary>View SQL Definition</summary>")
        lines.append("")
        lines.append("```sql")
        lines.append(tbl.view_definition)
        lines.append("```")
        lines.append("</details>")
        lines.append("")

    return lines


def render_markdown_report(result: DiscoveryResult) -> str:
    """Render a full Markdown data discovery & Databricks readiness report."""
    ov = result.overview
    lines: list[str] = [
        f"# Database Discovery Report: `{ov.database_name}`",
        "",
        f"- **Dialect:** `{ov.dialect}` ({ov.driver})"
        + (f" — Server `{ov.server_version}`" if ov.server_version else ""),
        f"- **Target:** `{ov.sanitized_url}`",
        f"- **Discovered at:** `{ov.discovered_at}` (took `{ov.duration_seconds}s`)",
        "",
        "## Executive Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Schemas | {ov.schema_count} |",
        f"| Base Tables (Empty) | {ov.table_count} ({ov.empty_table_count}) |",
        f"| Views / Matviews | {ov.view_count} |",
        f"| Total Columns | {ov.column_count:,} |",
        f"| Total Rows | {ov.total_rows:,} |",
        f"| Total Size | {_fmt_bytes(ov.total_size_bytes)} |",
        f"| Explicit / Inferred Relationships | {ov.explicit_fk_count} / {ov.inferred_relationship_count} |",
        f"| PII / Governance Columns | {ov.pii_column_count} / {ov.governance_column_count} |",
        f"| CDC Ready / Blocked Tables | {ov.cdc_ready_table_count} / {ov.cdc_blocked_table_count} |",
        f"| Delta / Snowflake Compatibility Warnings | {ov.delta_warning_count} / {ov.snowflake_warning_count} |",
        f"| Average Health Score | {ov.avg_quality_score}/100 |",
        "",
    ]

    # Data Engineering & Cloud Ingestion Blueprint
    target_plat = result.config.target_platform
    cluster_col_hdr = "Snowflake Clustering (`CLUSTER BY`)" if target_plat == "snowflake" else "Clustering (`CLUSTER BY`)"
    lines.append("## Data Engineering & Cloud Ingestion Blueprint")
    lines.append("")
    lines.append(f"| Table | Semantic | Role | Rows | Est. MB | Strategy | CDC Status | {cluster_col_hdr} | Cadence |")
    lines.append("|---|---|---|---:|---:|---|---|---|---|")
    for tbl in result.tables:
        rec = tbl.recommendations
        cdc = tbl.cdc_readiness
        dc = tbl.delta_compatibility
        sfc = tbl.snowflake_compatibility
        sz = tbl.ingestion_sizing
        ckeys = (
            sfc.clustering_keys
            if (target_plat == "snowflake" and sfc and sfc.clustering_keys)
            else (
                dc.liquid_clustering_keys
                if (dc and dc.liquid_clustering_keys)
                else (sfc.clustering_keys if (sfc and sfc.clustering_keys) else [])
            )
        )
        lck = ", ".join(_md_code(k) for k in ckeys) if ckeys else "—"
        lines.append(
            f"| {_md_code(tbl.full_name)} | {_md_code(tbl.semantic_name)} | `{rec.entity_role if rec else 'unknown'}` | "
            f"{_fmt_int(tbl.row_count)} | {f'{sz.estimated_size_mb:.2f}' if sz else '—'} | "
            f"`{rec.ingestion_strategy if rec else '—'}` | `{cdc.status if cdc else '—'}` | {lck} | "
            f"`{sz.recommended_cadence if sz else '—'}` |"
        )
    lines.append("")

    # Manufacturing Signals summary (if any detected)
    mfg_lines: list[str] = []
    for tbl in result.tables:
        if tbl.telemetry_profile:
            tp = tbl.telemetry_profile
            mfg_lines.append(
                f"- **OT Telemetry (`{tbl.full_name}`)**: `{tp.pattern}` — ts=`{tp.timestamp_column}`, "
                f"median cadence={tp.median_interval_seconds}s, watermark delay=`{tp.recommended_watermark_delay}`"
                + (f", flatlines={tp.flatline_columns}" if tp.flatline_columns else "")
            )
        if tbl.hierarchy:
            mfg_lines.append(f"- **Recursive Hierarchy (`{tbl.full_name}`)**: `{tbl.hierarchy.pattern}` — {tbl.hierarchy.reason}")
        for col in tbl.columns:
            if col.profile and col.profile.legacy_date:
                ld = col.profile.legacy_date
                mfg_lines.append(
                    f"- **Legacy Date (`{tbl.full_name}.{col.name}`)**: `{ld.encoding}`"
                    + (f" paired with `{ld.paired_column}`" if ld.paired_column else "")
                    + f" — Spark: `{ld.spark_expression}`"
                )
    if mfg_lines:
        lines.append("## Manufacturing & OT Pattern Signals")
        lines.append("")
        lines.extend(mfg_lines)
        lines.append("")

    # Relationships & ERD
    erd = _render_mermaid_erd(result)
    if erd:
        lines.append("## Entity-Relationship Diagram")
        lines.append("")
        lines.append(erd)
        lines.append("")

    if result.relationships:
        lines.append("## Discovered Relationships")
        lines.append("")
        lines.append("| Source | Target | Kind | Confidence | Value Overlap | Evidence |")
        lines.append("|---|---|---|---:|---:|---|")
        for rel in result.relationships:
            ovlp = f"{rel.overlap_rate:.0%}" if rel.overlap_rate is not None else "—"
            lines.append(
                f"| {_md_code(f'{rel.source_table}.{rel.source_column}')} | {_md_code(f'{rel.target_table}.{rel.target_column}')} | "
                f"`{rel.kind}` | {rel.confidence:.0%} | {ovlp} | {_escape_md('; '.join(rel.evidence))} |"
            )
        lines.append("")

    lineage_md = _render_mermaid_lineage(result)
    if lineage_md:
        lines.append("## View SQL Lineage")
        lines.append("")
        lines.append(lineage_md)
        lines.append("")

    if result.quality_issues:
        lines.append("## Data Quality, Governance & Compatibility Findings")
        lines.append("")
        lines.append("| Severity | Category | Table | Column | Details |")
        lines.append("|---|---|---|---|---|")
        for issue in result.quality_issues:
            lines.append(
                f"| **{issue.severity.upper()}** | `{_escape_md(issue.category)}` | {_md_code(issue.table)} | "
                f"{_md_code(issue.column)} | {_escape_md(issue.message)} |"
            )
        lines.append("")

    lines.append("## Table & Column Catalog")
    lines.append("")
    for tbl in result.tables:
        lines.extend(_render_table_section(tbl))

    return "\n".join(lines).rstrip() + "\n"


def export_markdown(result: DiscoveryResult, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_markdown_report(result), encoding="utf-8")
    return path


def render_diff_markdown(diff: SnapshotDiff) -> str:
    """Render a SnapshotDiff as a Markdown report (suitable for PR comments / CI)."""
    status = "BREAKING CHANGES DETECTED" if diff.has_breaking_changes else "No Breaking Changes"
    lines = [
        f"# Schema & Data Drift Report ({status})",
        "",
        f"- **Before:** `{diff.before_database}` @ `{diff.before_discovered_at}`",
        f"- **After:** `{diff.after_database}` @ `{diff.after_discovered_at}`",
        f"- **Breaking Changes:** `{diff.breaking_changes_count}`",
        "",
    ]

    if diff.added_tables:
        lines.append("## Added Tables")
        lines.extend(f"- `+ {t}`" for t in diff.added_tables)
        lines.append("")

    if diff.removed_tables:
        lines.append("## Removed Tables (BREAKING)")
        lines.extend(f"- `- {t}`" for t in diff.removed_tables)
        lines.append("")

    if diff.primary_key_changes:
        lines.append("## Primary Key Changes (BREAKING)")
        lines.extend(f"- `{p}`" for p in diff.primary_key_changes)
        lines.append("")

    if diff.column_changes:
        lines.append("## Column Changes")
        lines.append("| Table | Column | Change | Before | After | Breaking |")
        lines.append("|---|---|---|---|---|---|")
        for c in diff.column_changes:
            lines.append(
                f"| {_md_code(c.table)} | {_md_code(c.column)} | `{_escape_md(c.change_type)}` | {_md_code(c.before)} | {_md_code(c.after)} | "
                f"{'YES' if c.breaking else 'no'} |"
            )
        lines.append("")

    if diff.new_pii_columns:
        lines.append("## Newly Detected PII Columns")
        lines.extend(f"- `{p}`" for p in diff.new_pii_columns)
        lines.append("")

    if diff.metric_drifts:
        lines.append("## Metric Drifts")
        lines.append("| Severity | Table | Column | Metric | Before | After | Delta |")
        lines.append("|---|---|---|---|---:|---:|---:|")
        for m in diff.metric_drifts:
            lines.append(
                f"| **{m.severity.upper()}** | {_md_code(m.table)} | {_md_code(m.column)} | `{_escape_md(m.metric)}` | "
                f"{m.before} | {m.after} | {m.delta} |"
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def export_diff_markdown(diff: SnapshotDiff, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_diff_markdown(diff), encoding="utf-8")
    return path
