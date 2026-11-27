"""Typer + Rich command-line interface for tick database discovery."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import typer
from rich import box
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table

from tick import __version__
from tick.demo import create_demo_database, create_manufacturing_demo_database
from tick.dialects import DriverMissingError, get_driver_status
from tick.diff import diff_snapshots, load_snapshot
from tick.engine import discover_database
from tick.exporters import (
    export_contracts_bundle,
    export_databricks_bundle,
    export_dbt_project,
    export_diff_html,
    export_diff_json,
    export_diff_markdown,
    export_html,
    export_json,
    export_markdown,
    export_snowflake_bundle,
)
from tick.harmonize import export_harmonization, harmonize_snapshots
from tick.models import (
    DiscoveryConfig,
    DiscoveryResult,
    HarmonizationReport,
    ProfileMode,
    SnapshotDiff,
    TargetPlatform,
)

app = typer.Typer(
    name="tick",
    help="Universal automatic database discovery, Databricks Lakeflow readiness, and manufacturing DE tool.",
    no_args_is_help=True,
)
console = Console()


def _fmt_bytes(num: int | None) -> str:
    if num is None:
        return "—"
    v = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024 or unit == "TB":
            return f"{int(v)} B" if unit == "B" else f"{v:.1f} {unit}"
        v /= 1024.0
    return f"{num} B"


def _render_terminal_summary(result: DiscoveryResult) -> None:
    ov = result.overview
    header = (
        f"[bold cyan]{escape(ov.database_name)}[/bold cyan]  "
        f"[dim]({escape(ov.dialect)} · {escape(ov.server_version or ov.driver)})[/dim]\n"
        f"[dim]Target:[/dim] {escape(ov.sanitized_url)}  [dim]·  Elapsed:[/dim] {ov.duration_seconds}s"
    )
    console.print(Panel(header, title="tick Discovery Complete", border_style="cyan"))

    kpi = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold")
    kpi.add_column("Schemas", justify="right")
    kpi.add_column("Tables (Empty)", justify="right")
    kpi.add_column("Views", justify="right")
    kpi.add_column("Rows", justify="right")
    kpi.add_column("FKs (E/I)", justify="right")
    kpi.add_column("PII/Gov", justify="right")
    kpi.add_column("CDC (R/B)", justify="right")
    kpi.add_column("Delta Warns", justify="right")
    kpi.add_column("Health", justify="right")
    health_style = "green" if ov.avg_quality_score >= 85 else ("yellow" if ov.avg_quality_score >= 65 else "red")
    kpi.add_row(
        str(ov.schema_count),
        f"{ov.table_count} ({ov.empty_table_count})",
        str(ov.view_count),
        f"{ov.total_rows:,}",
        f"{ov.explicit_fk_count}/{ov.inferred_relationship_count}",
        f"{ov.pii_column_count}/{ov.governance_column_count}",
        f"[green]{ov.cdc_ready_table_count}[/green]/[red]{ov.cdc_blocked_table_count}[/red]",
        str(ov.delta_warning_count),
        f"[{health_style}]{ov.avg_quality_score}/100[/{health_style}]",
    )
    console.print(kpi)

    tbl_view = Table(title="Discovered Objects & Databricks Blueprint", box=box.ROUNDED, show_lines=False)
    tbl_view.add_column("Object", style="bold")
    tbl_view.add_column("Semantic", style="cyan")
    tbl_view.add_column("Role", style="magenta")
    tbl_view.add_column("Rows", justify="right")
    tbl_view.add_column("CDC")
    tbl_view.add_column("Liquid Cluster")
    tbl_view.add_column("Health", justify="right")
    tbl_view.add_column("Flags")

    for t in result.tables:
        rec = t.recommendations
        cdc = t.cdc_readiness
        dc = t.delta_compatibility
        pii_cnt = sum(1 for c in t.columns if c.profile and c.profile.pii_findings)
        gov_cnt = sum(1 for c in t.columns if c.profile and c.profile.governance_findings)
        flags: list[str] = []
        if t.table_type == "table" and not t.primary_key:
            flags.append("[red]NO_PK[/red]")
        if pii_cnt:
            flags.append(f"[yellow]PII:{pii_cnt}[/yellow]")
        if gov_cnt:
            flags.append(f"[orange1]UC:{gov_cnt}[/orange1]")
        if t.telemetry_profile:
            flags.append("[cyan]OT[/cyan]")
        if t.hierarchy:
            flags.append("[magenta]BOM/TREE[/magenta]")
        if dc and dc.requires_column_mapping:
            flags.append("[red]COL_MAP[/red]")
        if t.row_count == 0 and t.table_type == "table":
            flags.append("[dim]EMPTY[/dim]")
        h_color = "green" if t.quality_score >= 85 else ("yellow" if t.quality_score >= 65 else "red")
        cdc_st = cdc.status if cdc else "—"
        cdc_col = "green" if cdc_st == "ready" else ("red" if cdc_st == "blocked" else "yellow")
        lck = ", ".join(dc.liquid_clustering_keys[:2]) if (dc and dc.liquid_clustering_keys) else "—"
        tbl_view.add_row(
            escape(t.full_name),
            escape(t.semantic_name or "—"),
            rec.entity_role if rec else "unknown",
            f"{t.row_count:,}" if t.row_count is not None else "—",
            f"[{cdc_col}]{cdc_st}[/{cdc_col}]",
            escape(lck),
            f"[{h_color}]{t.quality_score}[/{h_color}]",
            " ".join(flags) or "[green]OK[/green]",
        )
    console.print(tbl_view)

    high_med = [i for i in result.quality_issues if i.severity in {"high", "medium"}]
    if high_med:
        iss_tbl = Table(title=f"Top Quality, Governance & Delta Alerts ({len(high_med)} high/medium)", box=box.SIMPLE)
        iss_tbl.add_column("Sev")
        iss_tbl.add_column("Category", style="cyan")
        iss_tbl.add_column("Target", style="bold")
        iss_tbl.add_column("Details")
        for issue in high_med[:14]:
            sev_color = "red" if issue.severity == "high" else "yellow"
            target = f"{issue.table}.{issue.column}" if issue.column else issue.table
            iss_tbl.add_row(
                f"[{sev_color}]{issue.severity.upper()}[/{sev_color}]",
                escape(issue.category),
                escape(target),
                escape(issue.message),
            )
        console.print(iss_tbl)


def _render_diff_summary(diff: SnapshotDiff) -> None:
    color = "red" if diff.has_breaking_changes else "green"
    title = (
        f"BREAKING CHANGES DETECTED ({diff.breaking_changes_count})"
        if diff.has_breaking_changes
        else "No Breaking Schema Changes"
    )
    console.print(
        Panel(
            f"Before: [bold]{escape(diff.before_database)}[/bold] ({diff.before_discovered_at})\n"
            f"After:  [bold]{escape(diff.after_database)}[/bold] ({diff.after_discovered_at})",
            title=title,
            border_style=color,
        )
    )
    if diff.added_tables:
        console.print(f"[green]Added tables ({len(diff.added_tables)}):[/green] " + escape(", ".join(diff.added_tables)))
    if diff.removed_tables:
        console.print(f"[red]Removed tables ({len(diff.removed_tables)}):[/red] " + escape(", ".join(diff.removed_tables)))
    if diff.primary_key_changes:
        console.print("[red]Primary key changes:[/red]")
        for p in diff.primary_key_changes:
            console.print(f"  - {escape(p)}")
    if diff.column_changes:
        t = Table(title="Column Changes", box=box.SIMPLE)
        t.add_column("Table")
        t.add_column("Column")
        t.add_column("Change")
        t.add_column("Before")
        t.add_column("After")
        t.add_column("Breaking")
        for c in diff.column_changes:
            t.add_row(
                escape(c.table),
                escape(c.column),
                c.change_type,
                escape(c.before or "—"),
                escape(c.after or "—"),
                "[red]YES[/red]" if c.breaking else "no",
            )
        console.print(t)
    if diff.new_pii_columns:
        console.print("[yellow]Newly detected PII columns:[/yellow] " + escape(", ".join(diff.new_pii_columns)))
    if diff.metric_drifts:
        mt = Table(title="Metric Drifts", box=box.SIMPLE)
        mt.add_column("Sev")
        mt.add_column("Target")
        mt.add_column("Metric")
        mt.add_column("Before", justify="right")
        mt.add_column("After", justify="right")
        mt.add_column("Delta", justify="right")
        for m in diff.metric_drifts:
            tgt = f"{m.table}.{m.column}" if m.column else m.table
            mt.add_row(m.severity.upper(), escape(tgt), m.metric, str(m.before), str(m.after), str(m.delta))
        console.print(mt)


def _render_harmonization_summary(report: HarmonizationReport) -> None:
    console.print(
        Panel(
            f"Plants / Sources: [bold cyan]{escape(', '.join(report.sources))}[/bold cyan]\n"
            f"Unique tables: {report.total_unique_tables} · "
            f"[green]Fully aligned: {report.fully_aligned_tables}[/green] · "
            f"[yellow]With drift: {report.tables_with_drift}[/yellow]",
            title="Multi-Plant Schema Harmonization",
            border_style="cyan",
        )
    )
    t = Table(box=box.ROUNDED)
    t.add_column("Table", style="bold")
    t.add_column("Present In")
    t.add_column("Missing In")
    t.add_column("PK Divergent")
    t.add_column("Type Conflicts", justify="right")
    t.add_column("Site-Only Cols", justify="right")
    for tbl in report.tables:
        t.add_row(
            escape(tbl.table_name),
            escape(", ".join(tbl.present_in_sources)),
            f"[yellow]{escape(', '.join(tbl.missing_in_sources))}[/yellow]" if tbl.missing_in_sources else "[green]—[/green]",
            "[red]YES[/red]" if tbl.pk_divergent else "no",
            f"[red]{tbl.conflict_count}[/red]" if tbl.conflict_count else "0",
            f"[yellow]{tbl.site_specific_column_count}[/yellow]" if tbl.site_specific_column_count else "0",
        )
    console.print(t)


@app.command()
def discover(
    target: str = typer.Argument(
        ...,
        help="SQLAlchemy URL (postgresql://..., mssql+pyodbc://..., databricks://..., duckdb:///...) or local .duckdb/.sqlite/.db path.",
    ),
    out_dir: Optional[Path] = typer.Option(
        None,
        "--out-dir",
        "-o",
        help="Write full bundle (report.html, report.md, snapshot.json, dbt/, databricks/) to this directory.",
    ),
    html_out: Optional[Path] = typer.Option(None, "--html", help="Write standalone interactive HTML report."),
    md_out: Optional[Path] = typer.Option(None, "--md", help="Write Markdown data dictionary & Mermaid diagrams."),
    json_out: Optional[Path] = typer.Option(None, "--json", help="Write discovery snapshot JSON."),
    dbt_dir: Optional[Path] = typer.Option(None, "--dbt-dir", help="Write dbt sources.yml and staging SQL models."),
    databricks_dir: Optional[Path] = typer.Option(
        None, "--databricks-dir", help="Write Unity Catalog DDL, Lakeflow DLT pipeline, and sizing guide."
    ),
    snowflake_dir: Optional[Path] = typer.Option(
        None, "--snowflake-dir", help="Write Snowflake DDL, Dynamic Tables, DMFs, and sizing guide."
    ),
    contracts_dir: Optional[Path] = typer.Option(
        None, "--contracts-dir", help="Write Databricks DQX quality YAML and ODCS v3.0.0 data contract YAML."
    ),
    target_platform: TargetPlatform = typer.Option(
        "both", "--target", help="Target cloud platform(s): databricks | snowflake | both."
    ),
    databricks_catalog: str = typer.Option("manufacturing", "--databricks-catalog", help="Target Unity Catalog name."),
    databricks_schema: str = typer.Option("bronze", "--databricks-schema", help="Target Unity Catalog bronze schema."),
    snowflake_db: str = typer.Option("MANUFACTURING", "--snowflake-db", help="Target Snowflake database name."),
    snowflake_schema: str = typer.Option("BRONZE", "--snowflake-schema", help="Target Snowflake bronze schema."),
    snowflake_warehouse: str = typer.Option("TRANSFORMING_WH", "--snowflake-warehouse", help="Target Snowflake warehouse for Dynamic Tables."),
    profile: ProfileMode = typer.Option("full", "--profile", "-p", help="Profiling depth: none | fast | full."),
    sample_size: int = typer.Option(5000, "--sample-size", help="Max rows sampled per table for profiling."),
    workers: int = typer.Option(
        1, "--workers", "-w", min=1, max=32, help="Parallel profiling worker threads (bounded for large ERP databases)."
    ),
    include_schema: list[str] = typer.Option([], "--include-schema", help="Include schema glob(s) (repeatable)."),
    exclude_schema: list[str] = typer.Option([], "--exclude-schema", help="Exclude schema glob(s) (repeatable)."),
    include_table: list[str] = typer.Option([], "--include-table", help="Include table glob(s) (repeatable)."),
    exclude_table: list[str] = typer.Option([], "--exclude-table", help="Exclude table glob(s) (repeatable)."),
    no_views: bool = typer.Option(False, "--no-views", help="Skip views and materialized views."),
    no_pii: bool = typer.Option(False, "--no-pii", help="Disable PII heuristic scanning."),
    redact_pii: bool = typer.Option(False, "--redact-pii", help="Mask sample/top values on PII-flagged columns."),
    no_infer_rels: bool = typer.Option(False, "--no-infer-relationships", help="Disable automatic FK/join inference."),
    stale_days: int = typer.Option(30, "--stale-days", help="Flag timestamp columns older than N days as stale."),
    max_tables: Optional[int] = typer.Option(None, "--max-tables", help="Cap total number of tables/views inspected."),
    exact_counts: bool = typer.Option(False, "--exact-counts", help="Force exact COUNT(*) on all tables."),
    # Production safety guardrails
    read_uncommitted: bool = typer.Option(
        False, "--read-uncommitted", help="Use READ UNCOMMITTED / WITH (NOLOCK) on MSSQL/MySQL/DB2 for zero-lock plant safety."
    ),
    query_timeout: Optional[int] = typer.Option(
        None, "--query-timeout", help="Per-query timeout in seconds (statement_timeout / LOCK_TIMEOUT)."
    ),
    skip_empty_tables: bool = typer.Option(
        False, "--skip-empty-tables", help="Skip profiling 0-row tables (ideal for SAP/ERP DBs with thousands of empty tables)."
    ),
    skip_tables_over_mb: Optional[float] = typer.Option(
        None, "--skip-tables-over-mb", help="Skip profiling tables larger than N MB while preserving catalog metadata."
    ),
    use_tablesample: bool = typer.Option(
        False, "--use-tablesample", help="Use dialect-native TABLESAMPLE / USING SAMPLE for large tables."
    ),
    # Manufacturing & Databricks extensions
    glossary: Optional[Path] = typer.Option(
        None, "--glossary", exists=True, help="Custom ERP/MES table & column glossary YAML/JSON file."
    ),
    no_manufacturing: bool = typer.Option(False, "--no-manufacturing", help="Disable manufacturing/OT/BOM/governance detectors."),
    no_databricks_compat: bool = typer.Option(False, "--no-databricks-compat", help="Disable Spark/Delta compatibility linting."),
    no_snowflake_compat: bool = typer.Option(False, "--no-snowflake-compat", help="Disable Snowflake compatibility linting."),
    no_cdc_audit: bool = typer.Option(False, "--no-cdc-audit", help="Disable source CDC readiness audit."),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress terminal tables (still writes output files)."),
) -> None:
    """Point tick at any database and perform automatic data engineering discovery."""
    cfg = DiscoveryConfig(
        profile_mode=profile,
        sample_size=sample_size,
        include_schemas=include_schema,
        exclude_schemas=exclude_schema,
        include_tables=include_table,
        exclude_tables=exclude_table,
        include_views=not no_views,
        detect_pii=not no_pii,
        redact_pii=redact_pii,
        infer_relationships=not no_infer_rels,
        stale_after_days=stale_days,
        max_tables=max_tables,
        exact_row_counts=exact_counts,
        read_uncommitted=read_uncommitted,
        query_timeout_seconds=query_timeout,
        skip_empty_tables=skip_empty_tables,
        skip_tables_over_mb=skip_tables_over_mb,
        use_tablesample=use_tablesample,
        workers=workers,
        target_platform=target_platform,
        glossary_path=str(glossary) if glossary else None,
        detect_manufacturing=not no_manufacturing,
        detect_databricks_compat=not no_databricks_compat,
        detect_snowflake_compat=not no_snowflake_compat,
        audit_cdc=not no_cdc_audit,
        databricks_catalog=databricks_catalog,
        databricks_schema=databricks_schema,
        snowflake_database=snowflake_db,
        snowflake_schema=snowflake_schema,
        snowflake_warehouse=snowflake_warehouse,
    )

    try:
        if quiet:
            result = discover_database(target, config=cfg)
        else:
            with console.status("[cyan]Starting database discovery...[/cyan]") as status:
                result = discover_database(
                    target,
                    config=cfg,
                    progress_cb=lambda msg: status.update(f"[cyan]{escape(msg)}[/cyan]"),
                )
    except DriverMissingError as exc:
        console.print(f"[bold red]Driver error:[/bold red] {escape(str(exc))}")
        raise typer.Exit(code=2)
    except Exception as exc:
        console.print(f"[bold red]Discovery failed:[/bold red] {escape(str(exc))}")
        raise typer.Exit(code=1)

    if not quiet:
        _render_terminal_summary(result)

    written: list[tuple[str, Path]] = []
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        written.append(("HTML report", export_html(result, out_dir / "report.html")))
        written.append(("Markdown", export_markdown(result, out_dir / "report.md")))
        written.append(("JSON snapshot", export_json(result, out_dir / "snapshot.json")))
        dbt_files = export_dbt_project(result, out_dir / "dbt")
        written.append((f"dbt scaffolding ({len(dbt_files)} files)", out_dir / "dbt"))
        if target_platform in {"databricks", "both"}:
            dbx_files = export_databricks_bundle(result, out_dir / "databricks")
            written.append((f"Databricks bundle ({len(dbx_files)} files)", out_dir / "databricks"))
        if target_platform in {"snowflake", "both"}:
            sf_files = export_snowflake_bundle(result, out_dir / "snowflake")
            written.append((f"Snowflake bundle ({len(sf_files)} files)", out_dir / "snowflake"))
        contract_files = export_contracts_bundle(result, out_dir / "contracts")
        written.append((f"Data contracts ({len(contract_files)} files)", out_dir / "contracts"))

    if html_out is not None:
        written.append(("HTML report", export_html(result, html_out)))
    if md_out is not None:
        written.append(("Markdown", export_markdown(result, md_out)))
    if json_out is not None:
        written.append(("JSON snapshot", export_json(result, json_out)))
    if dbt_dir is not None:
        dbt_files = export_dbt_project(result, dbt_dir)
        written.append((f"dbt scaffolding ({len(dbt_files)} files)", dbt_dir))
    if databricks_dir is not None:
        dbx_files = export_databricks_bundle(result, databricks_dir)
        written.append((f"Databricks bundle ({len(dbx_files)} files)", databricks_dir))
    if snowflake_dir is not None:
        sf_files = export_snowflake_bundle(result, snowflake_dir)
        written.append((f"Snowflake bundle ({len(sf_files)} files)", snowflake_dir))
    if contracts_dir is not None:
        contract_files = export_contracts_bundle(result, contracts_dir)
        written.append((f"Data contracts ({len(contract_files)} files)", contracts_dir))

    if written and not quiet:
        console.print("\n[bold green]Artifacts written:[/bold green]")
        for label, path in written:
            console.print(f"  • {label}: [cyan]{escape(str(path.resolve()))}[/cyan]")


@app.command()
def diff(
    before: Path = typer.Argument(..., exists=True, help="Baseline snapshot JSON file."),
    after: Path = typer.Argument(..., exists=True, help="New snapshot JSON file."),
    html_out: Optional[Path] = typer.Option(None, "--html", help="Write interactive HTML drift report."),
    json_out: Optional[Path] = typer.Option(None, "--json", help="Write diff JSON."),
    md_out: Optional[Path] = typer.Option(None, "--md", help="Write diff Markdown report."),
    fail_on_breaking: bool = typer.Option(
        False, "--fail-on-breaking", help="Exit with code 1 if breaking schema changes are detected (for CI)."
    ),
) -> None:
    """Compare two tick discovery snapshots for schema drift, new PII, and metric regressions."""
    before_res = load_snapshot(before)
    after_res = load_snapshot(after)
    result = diff_snapshots(before_res, after_res)
    _render_diff_summary(result)

    if html_out:
        export_diff_html(result, html_out)
        console.print(f"[green]Diff HTML written to[/green] {escape(str(html_out.resolve()))}")
    if json_out:
        export_diff_json(result, json_out)
        console.print(f"[green]Diff JSON written to[/green] {escape(str(json_out.resolve()))}")
    if md_out:
        export_diff_markdown(result, md_out)
        console.print(f"[green]Diff Markdown written to[/green] {escape(str(md_out.resolve()))}")

    if fail_on_breaking and result.has_breaking_changes:
        raise typer.Exit(code=1)


@app.command(name="compare-sources")
def compare_sources(
    snapshots: list[Path] = typer.Argument(..., exists=True, help="2 or more plant/site discovery snapshot JSON files."),
    html_out: Optional[Path] = typer.Option(None, "--html", help="Write interactive multi-plant HTML harmonization dashboard."),
    md_out: Optional[Path] = typer.Option(None, "--md", help="Write harmonization matrix Markdown report."),
    json_out: Optional[Path] = typer.Option(None, "--json", help="Write harmonization report JSON."),
    sql_out: Optional[Path] = typer.Option(None, "--sql", help="Write unified multi-plant UNION ALL SQL."),
    target_platform: Literal["databricks", "snowflake"] = typer.Option(
        "databricks", "--target", help="Target SQL dialect for unified views: databricks | snowflake."
    ),
    catalog: str = typer.Option("manufacturing", "--catalog", "--database", help="Target catalog (Databricks) or database (Snowflake)."),
    bronze_schema: str = typer.Option("bronze", "--bronze-schema", help="Bronze schema prefix per plant."),
) -> None:
    """Harmonize schemas across multiple plants/sites and generate unified multi-plant Databricks or Snowflake SQL."""
    if len(snapshots) < 2:
        console.print("[bold red]Provide at least 2 snapshot JSON files to compare.[/bold red]")
        raise typer.Exit(code=2)

    loaded: dict[str, DiscoveryResult] = {}
    for p in snapshots:
        snap = load_snapshot(p)
        if p.stem.lower() == "snapshot":
            parent_name = p.resolve().parent.name
            base_label = parent_name or snap.overview.database_name or p.stem
        else:
            base_label = p.stem
        label = base_label
        suffix = 2
        while label in loaded:
            label = f"{base_label}_{suffix}"
            suffix += 1
        loaded[label] = snap

    report = harmonize_snapshots(
        loaded,
        catalog=catalog,
        bronze_schema=bronze_schema,
        target_platform=target_platform,
    )
    _render_harmonization_summary(report)

    written = export_harmonization(
        report, html_out=html_out, md_out=md_out, json_out=json_out, sql_out=sql_out
    )
    if written:
        console.print("\n[bold green]Harmonization artifacts written:[/bold green]")
        for p in written:
            console.print(f"  • [cyan]{escape(str(p.resolve()))}[/cyan]")


@app.command()
def demo(
    output_path: Path = typer.Argument(
        Path("demo_warehouse.duckdb"),
        help="Path to generate a realistic demo database (.duckdb or .sqlite).",
    ),
    domain: Literal["ecommerce", "manufacturing"] = typer.Option(
        "ecommerce",
        "--domain",
        "-d",
        help="Demo domain schema: ecommerce | manufacturing (SAP/MES/Historian/BOM).",
    ),
    plant: str = typer.Option(
        "plant_ohio",
        "--plant",
        help="Plant variant when --domain manufacturing (plant_ohio | plant_munich).",
    ),
) -> None:
    """Generate a realistic sample database (DuckDB or SQLite) to test tick discovery."""
    try:
        if domain == "manufacturing":
            created = create_manufacturing_demo_database(output_path, plant_variant=plant)
        else:
            created = create_demo_database(output_path)
    except Exception as exc:
        console.print(f"[bold red]Failed to create demo database:[/bold red] {escape(str(exc))}")
        raise typer.Exit(code=1)

    console.print(f"[bold green]Created {domain} demo database:[/bold green] [cyan]{escape(str(created))}[/cyan]")
    console.print(f"Run discovery with:\n  [bold]tick discover {escape(str(created))} -o ./discovery-output[/bold]")


@app.command()
def drivers() -> None:
    """List supported database dialects and check which Python drivers are installed."""
    table = Table(title="Supported Database Dialects & Drivers", box=box.ROUNDED)
    table.add_column("Dialect", style="bold")
    table.add_column("Status")
    table.add_column("Example Connection URL", style="cyan")
    table.add_column("Install Command", style="dim")

    for drv in get_driver_status():
        status = "[green]Installed[/green]" if drv["installed"] else "[yellow]Not installed[/yellow]"
        table.add_row(drv["dialect"], status, escape(drv["example_url"]), escape(drv["install"]))
    console.print(table)


@app.command()
def version() -> None:
    """Print the installed tick version."""
    console.print(f"tick {__version__}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
