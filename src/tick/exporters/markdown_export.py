"""Markdown data dictionary, Mermaid ERD/lineage, and snapshot diff exporters."""

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
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def _escape_md(val: str | None) -> str:
    if not val:
        return "—"
    return val.replace("|", "\\|").replace("\n", " ")


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
            label = f"{rel.source_column} -> {rel.target_column}"
        else:
            connector = "}o..||"
            label = f"inferred ({rel.confidence:.0%}) {rel.source_column} -> {rel.target_column}"
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
                if ttype == "table":
                    lines.append(f'    {nid}[("{node}")]')
                else:
                    lines.append(f'    {nid}["{node} ({ttype})"]')
        lines.append(
            f"    {_mermaid_id(edge.upstream_table)} --> {_mermaid_id(edge.downstream_table)}"
        )
    lines.append("```")
    return "\n".join(lines)


def _render_table_section(tbl: TableInfo) -> list[str]:
    lines: list[str] = []
    badge = tbl.table_type.upper()
    role = tbl.recommendations.entity_role if tbl.recommendations else "unknown"
    lines.append(f"### `{tbl.full_name}` ({badge} · role: `{role}` · health: `{tbl.quality_score}/100`)")
    lines.append("")
    if tbl.comment:
        lines.append(f"> {tbl.comment}")
        lines.append("")

    meta_parts = [
        f"**Rows:** {_fmt_int(tbl.row_count)}{'' if tbl.row_count_exact or tbl.row_count is None else ' (est.)'}",
        f"**Size:** {_fmt_bytes(tbl.size_bytes)}",
        f"**PK:** `{', '.join(tbl.primary_key)}`" if tbl.primary_key else "**PK:** None",
        f"**Indexes:** {len(tbl.indexes)}",
    ]
    lines.append(" | ".join(meta_parts))
    lines.append("")

    if tbl.recommendations:
        rec = tbl.recommendations
        lines.append(
            f"- **Ingestion strategy:** `{rec.ingestion_strategy}` — {rec.ingestion_strategy_reason}"
        )
        if rec.watermark_candidates:
            wms = ", ".join(f"`{w.column}` ({w.kind})" for w in rec.watermark_candidates)
            lines.append(f"- **Watermark candidates:** {wms}")
        if rec.partition_candidates:
            parts = ", ".join(f"`{p.column}` ({p.granularity})" for p in rec.partition_candidates)
            lines.append(f"- **Partition candidates:** {parts}")
        if rec.clustering_candidates:
            lines.append(
                f"- **Clustering keys:** {', '.join(f'`{c}`' for c in rec.clustering_candidates)}"
            )
        if rec.primary_key_candidates:
            lines.append(
                f"- **Candidate PKs:** {', '.join(f'`{c}`' for c in rec.primary_key_candidates)}"
            )
        lines.append("")

    if tbl.upstream_tables:
        lines.append(
            f"- **Upstream dependencies:** {', '.join(f'`{u}`' for u in tbl.upstream_tables)}"
        )
        lines.append("")

    lines.append(
        "| Column | Type | Keys | Nullable | Null % | Distinct | Cardinality | Min / Max | PII | Samples |"
    )
    lines.append("|---|---|---|---|---:|---:|---|---|---|---|")
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
        if prof and not prof.profile_error:
            null_pct = f"{prof.null_rate:.1%}"
            distinct = _fmt_int(prof.distinct_count)
            card = prof.cardinality_class
            minmax = (
                f"{_escape_md(prof.min_value)} .. {_escape_md(prof.max_value)}"
                if prof.min_value is not None
                else "—"
            )
            pii = (
                ", ".join(f"`{f.category}` ({f.confidence})" for f in prof.pii_findings)
                if prof.pii_findings
                else "—"
            )
            samples = (
                ", ".join(f"`{_escape_md(s[:30])}`" for s in prof.sample_values[:3])
                if prof.sample_values
                else "—"
            )
        else:
            null_pct = distinct = card = minmax = pii = samples = "—"

        lines.append(
            f"| `{col.name}` | `{col.raw_type}` ({col.canonical_type}) | {key_str} | "
            f"{'YES' if col.nullable else 'NO'} | {null_pct} | {distinct} | {card} | {minmax} | {pii} | {samples} |"
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
    """Render a full Markdown data discovery & engineering readiness report."""
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
        f"| Base Tables | {ov.table_count} |",
        f"| Views / Matviews | {ov.view_count} |",
        f"| Total Columns | {ov.column_count:,} |",
        f"| Total Rows | {ov.total_rows:,} |",
        f"| Total Size | {_fmt_bytes(ov.total_size_bytes)} |",
        f"| Explicit Foreign Keys | {ov.explicit_fk_count} |",
        f"| Inferred Relationships | {ov.inferred_relationship_count} |",
        f"| PII Columns Flagged | {ov.pii_column_count} |",
        f"| Average Health Score | {ov.avg_quality_score}/100 |",
        "",
    ]

    # Data Engineering Ingestion Plan
    lines.append("## Data Engineering Ingestion Blueprint")
    lines.append("")
    lines.append("| Table | Role | Rows | Health | Strategy | Primary Watermark | Partition | Clustering |")
    lines.append("|---|---|---:|---:|---|---|---|---|")
    for tbl in result.tables:
        rec = tbl.recommendations
        wm = f"`{rec.watermark_candidates[0].column}` ({rec.watermark_candidates[0].kind})" if rec and rec.watermark_candidates else "—"
        part = f"`{rec.partition_candidates[0].column}` ({rec.partition_candidates[0].granularity})" if rec and rec.partition_candidates else "—"
        clust = ", ".join(f"`{c}`" for c in rec.clustering_candidates[:3]) if rec and rec.clustering_candidates else "—"
        lines.append(
            f"| `{tbl.full_name}` | `{rec.entity_role if rec else 'unknown'}` | {_fmt_int(tbl.row_count)} | "
            f"{tbl.quality_score}/100 | `{rec.ingestion_strategy if rec else '—'}` | {wm} | {part} | {clust} |"
        )
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
                f"| `{rel.source_table}.{rel.source_column}` | `{rel.target_table}.{rel.target_column}` | "
                f"`{rel.kind}` | {rel.confidence:.0%} | {ovlp} | {_escape_md('; '.join(rel.evidence))} |"
            )
        lines.append("")

    # View Lineage
    lineage_md = _render_mermaid_lineage(result)
    if lineage_md:
        lines.append("## View SQL Lineage")
        lines.append("")
        lines.append(lineage_md)
        lines.append("")

    # Quality & PII Audit
    if result.quality_issues:
        lines.append("## Data Quality & PII Findings")
        lines.append("")
        lines.append("| Severity | Category | Table | Column | Details |")
        lines.append("|---|---|---|---|---|")
        for issue in result.quality_issues:
            lines.append(
                f"| **{issue.severity.upper()}** | `{issue.category}` | `{issue.table}` | "
                f"{f'`{issue.column}`' if issue.column else '—'} | {_escape_md(issue.message)} |"
            )
        lines.append("")

    # Detailed Catalog
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
                f"| `{c.table}` | `{c.column}` | `{c.change_type}` | `{c.before or '—'}` | `{c.after or '—'}` | "
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
                f"| **{m.severity.upper()}** | `{m.table}` | `{m.column or '—'}` | `{m.metric}` | "
                f"{m.before} | {m.after} | {m.delta} |"
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def export_diff_markdown(diff: SnapshotDiff, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_diff_markdown(diff), encoding="utf-8")
    return path
