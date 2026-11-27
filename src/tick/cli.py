"""Typer + Rich command-line interface for tick database discovery."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich import box
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table

from tick import __version__
from tick.demo import create_demo_database
from tick.dialects import DriverMissingError, get_driver_status
from tick.diff import diff_snapshots, load_snapshot
from tick.engine import discover_database
from tick.exporters import (
    export_dbt_project,
    export_diff_json,
    export_diff_markdown,
    export_html,
    export_json,
    export_markdown,
)
from tick.models import DiscoveryConfig, DiscoveryResult, ProfileMode, SnapshotDiff

app = typer.Typer(
    name="tick",
    help="Universal automatic database discovery, profiling, lineage, and data engineering readiness tool.",
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
        f"[bold cyan]{ov.database_name}[/bold cyan]  "
        f"[dim]({ov.dialect} · {ov.server_version or ov.driver})[/dim]\n"
        f"[dim]Target:[/dim] {ov.sanitized_url}  [dim]·  Elapsed:[/dim] {ov.duration_seconds}s"
    )
    console.print(Panel(header, title="tick Discovery Complete", border_style="cyan"))

    # KPI bar
    kpi = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold")
    kpi.add_column("Schemas", justify="right")
    kpi.add_column("Tables", justify="right")
    kpi.add_column("Views", justify="right")
    kpi.add_column("Columns", justify="right")
    kpi.add_column("Total Rows", justify="right")
    kpi.add_column("Size", justify="right")
    kpi.add_column("FKs (Exp/Inf)", justify="right")
    kpi.add_column("PII Cols", justify="right")
    kpi.add_column("Avg Health", justify="right")
    health_style = "green" if ov.avg_quality_score >= 85 else ("yellow" if ov.avg_quality_score >= 65 else "red")
    kpi.add_row(
        str(ov.schema_count),
        str(ov.table_count),
        str(ov.view_count),
        f"{ov.column_count:,}",
        f"{ov.total_rows:,}",
        _fmt_bytes(ov.total_size_bytes),
        f"{ov.explicit_fk_count} / {ov.inferred_relationship_count}",
        f"[red]{ov.pii_column_count}[/red]" if ov.pii_column_count else "0",
        f"[{health_style}]{ov.avg_quality_score}/100[/{health_style}]",
    )
    console.print(kpi)

    # Tables overview
    tbl_view = Table(title="Discovered Objects & DE Blueprint", box=box.ROUNDED, show_lines=False)
    tbl_view.add_column("Object", style="bold")
    tbl_view.add_column("Role", style="magenta")
    tbl_view.add_column("Rows", justify="right")
    tbl_view.add_column("Cols", justify="right")
    tbl_view.add_column("Health", justify="right")
    tbl_view.add_column("Strategy", style="cyan")
    tbl_view.add_column("Watermark")
    tbl_view.add_column("Flags")

    for t in result.tables:
        rec = t.recommendations
        wm = rec.watermark_candidates[0].column if rec and rec.watermark_candidates else "—"
        pii_cnt = sum(1 for c in t.columns if c.profile and c.profile.pii_findings)
        flags: list[str] = []
        if t.table_type == "table" and not t.primary_key:
            flags.append("[red]NO_PK[/red]")
        if pii_cnt:
            flags.append(f"[yellow]PII:{pii_cnt}[/yellow]")
        if any(i.category == "stale_data" for i in t.quality_issues):
            flags.append("[red]STALE[/red]")
        if t.row_count == 0 and t.table_type == "table":
            flags.append("[dim]EMPTY[/dim]")
        h_color = "green" if t.quality_score >= 85 else ("yellow" if t.quality_score >= 65 else "red")
        tbl_view.add_row(
            t.full_name,
            rec.entity_role if rec else "unknown",
            f"{t.row_count:,}" if t.row_count is not None else "—",
            str(len(t.columns)),
            f"[{h_color}]{t.quality_score}[/{h_color}]",
            rec.ingestion_strategy if rec else "—",
            wm,
            " ".join(flags) or "[green]OK[/green]",
        )
    console.print(tbl_view)

    # Top quality/PII issues
    high_med = [i for i in result.quality_issues if i.severity in {"high", "medium"}]
    if high_med:
        iss_tbl = Table(title=f"Top Quality & PII Alerts ({len(high_med)} high/medium)", box=box.SIMPLE)
        iss_tbl.add_column("Sev")
        iss_tbl.add_column("Category", style="cyan")
        iss_tbl.add_column("Target", style="bold")
        iss_tbl.add_column("Details")
        for issue in high_med[:12]:
            sev_color = "red" if issue.severity == "high" else "yellow"
            target = f"{issue.table}.{issue.column}" if issue.column else issue.table
            iss_tbl.add_row(
                f"[{sev_color}]{issue.severity.upper()}[/{sev_color}]",
                issue.category,
                target,
                issue.message,
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
            f"Before: [bold]{diff.before_database}[/bold] ({diff.before_discovered_at})\n"
            f"After:  [bold]{diff.after_database}[/bold] ({diff.after_discovered_at})",
            title=title,
            border_style=color,
        )
    )
    if diff.added_tables:
        console.print(f"[green]Added tables ({len(diff.added_tables)}):[/green] " + ", ".join(diff.added_tables))
    if diff.removed_tables:
        console.print(f"[red]Removed tables ({len(diff.removed_tables)}):[/red] " + ", ".join(diff.removed_tables))
    if diff.primary_key_changes:
        console.print("[red]Primary key changes:[/red]")
        for p in diff.primary_key_changes:
            console.print(f"  - {p}")
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
                c.table,
                c.column,
                c.change_type,
                c.before or "—",
                c.after or "—",
                "[red]YES[/red]" if c.breaking else "no",
            )
        console.print(t)
    if diff.new_pii_columns:
        console.print("[yellow]Newly detected PII columns:[/yellow] " + ", ".join(diff.new_pii_columns))
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
            mt.add_row(m.severity.upper(), tgt, m.metric, str(m.before), str(m.after), str(m.delta))
        console.print(mt)


@app.command()
def discover(
    target: str = typer.Argument(
        ...,
        help="SQLAlchemy URL (postgresql://..., mssql+pyodbc://..., duckdb:///...) or local .duckdb/.sqlite/.db path.",
    ),
    out_dir: Optional[Path] = typer.Option(
        None,
        "--out-dir",
        "-o",
        help="Write full bundle (report.html, report.md, snapshot.json, dbt/) to this directory.",
    ),
    html_out: Optional[Path] = typer.Option(None, "--html", help="Write standalone interactive HTML report."),
    md_out: Optional[Path] = typer.Option(None, "--md", help="Write Markdown data dictionary & Mermaid diagrams."),
    json_out: Optional[Path] = typer.Option(None, "--json", help="Write discovery snapshot JSON."),
    dbt_dir: Optional[Path] = typer.Option(None, "--dbt-dir", help="Write dbt sources.yml and staging SQL models."),
    profile: ProfileMode = typer.Option("full", "--profile", "-p", help="Profiling depth: none | fast | full."),
    sample_size: int = typer.Option(5000, "--sample-size", help="Max rows sampled per table for profiling."),
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
    )

    try:
        if quiet:
            result = discover_database(target, config=cfg)
        else:
            with console.status("[cyan]Starting database discovery...[/cyan]") as status:
                result = discover_database(
                    target,
                    config=cfg,
                    progress_cb=lambda msg: status.update(f"[cyan]{msg}[/cyan]"),
                )
    except DriverMissingError as exc:
        console.print(f"[bold red]Driver error:[/bold red] {escape(str(exc))}")
        raise typer.Exit(code=2)
    except Exception as exc:
        console.print(f"[bold red]Discovery failed:[/bold red] {exc}")
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

    if html_out is not None:
        written.append(("HTML report", export_html(result, html_out)))
    if md_out is not None:
        written.append(("Markdown", export_markdown(result, md_out)))
    if json_out is not None:
        written.append(("JSON snapshot", export_json(result, json_out)))
    if dbt_dir is not None:
        dbt_files = export_dbt_project(result, dbt_dir)
        written.append((f"dbt scaffolding ({len(dbt_files)} files)", dbt_dir))

    if written and not quiet:
        console.print("\n[bold green]Artifacts written:[/bold green]")
        for label, path in written:
            console.print(f"  • {label}: [cyan]{path.resolve()}[/cyan]")


@app.command()
def diff(
    before: Path = typer.Argument(..., exists=True, help="Baseline snapshot JSON file."),
    after: Path = typer.Argument(..., exists=True, help="New snapshot JSON file."),
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

    if json_out:
        export_diff_json(result, json_out)
        console.print(f"[green]Diff JSON written to[/green] {json_out.resolve()}")
    if md_out:
        export_diff_markdown(result, md_out)
        console.print(f"[green]Diff Markdown written to[/green] {md_out.resolve()}")

    if fail_on_breaking and result.has_breaking_changes:
        raise typer.Exit(code=1)


@app.command()
def demo(
    output_path: Path = typer.Argument(
        Path("demo_warehouse.duckdb"),
        help="Path to generate a realistic demo database (.duckdb or .sqlite).",
    ),
) -> None:
    """Generate a realistic sample database (DuckDB or SQLite) to test tick discovery."""
    try:
        created = create_demo_database(output_path)
    except Exception as exc:
        console.print(f"[bold red]Failed to create demo database:[/bold red] {exc}")
        raise typer.Exit(code=1)

    console.print(f"[bold green]Created demo database:[/bold green] [cyan]{created}[/cyan]")
    console.print(f"Run discovery with:\n  [bold]tick discover {created} -o ./discovery-output[/bold]")


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
