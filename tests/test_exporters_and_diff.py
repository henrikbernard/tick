"""Tests for JSON/Markdown/dbt/HTML exporters, snapshot diffing, and CLI commands."""

from __future__ import annotations

import json

import yaml
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from tick import DiscoveryConfig, diff_snapshots, discover_database
from tick.cli import app
from tick.demo import create_demo_database
from tick.exporters import (
    export_dbt_project,
    export_html,
    export_json,
    export_markdown,
    render_diff_markdown,
)

runner = CliRunner()


def test_all_exporters(tmp_path):
    db_path = create_demo_database(tmp_path / "export_demo.sqlite")
    result = discover_database(str(db_path))

    # JSON
    json_path = export_json(result, tmp_path / "out" / "snapshot.json")
    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded["overview"]["table_count"] == 7

    # Markdown + Mermaid diagrams
    md_path = export_markdown(result, tmp_path / "out" / "report.md")
    md_text = md_path.read_text(encoding="utf-8")
    assert "```mermaid\nerDiagram" in md_text
    assert "```mermaid\nflowchart LR" in md_text
    assert "Data Engineering Ingestion Blueprint" in md_text
    assert "v_customer_ltv" in md_text

    # dbt scaffolding
    dbt_files = export_dbt_project(result, tmp_path / "out" / "dbt")
    sources_yml = yaml.safe_load((tmp_path / "out" / "dbt" / "sources.yml").read_text(encoding="utf-8"))
    assert sources_yml["version"] == 2
    assert len(sources_yml["sources"]) == 1
    src_tables = {t["name"]: t for t in sources_yml["sources"][0]["tables"]}
    assert "customers" in src_tables
    assert src_tables["customers"]["loaded_at_field"] == "updated_at"
    # Staging SQL files generated for all 7 base tables
    assert len(dbt_files) == 1 + 7
    stg_sql = (tmp_path / "out" / "dbt" / "staging" / "stg_main__customers.sql").read_text(encoding="utf-8")
    assert "{{ source(" in stg_sql
    assert "-- PII:" in stg_sql

    # Standalone HTML report
    html_path = export_html(result, tmp_path / "out" / "report.html")
    html_text = html_path.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html_text
    assert 'id="tick-data"' in html_text
    assert "Interactive ERD" in html_text
    assert "v_customer_ltv" in html_text


def test_snapshot_diff_detects_breaking_changes_and_drift(tmp_path):
    db_file = tmp_path / "evolving.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    balance DECIMAL(10, 2) NOT NULL
                )
                """
            )
        )
        conn.execute(text("CREATE TABLE deprecated_logs (id INTEGER PRIMARY KEY, msg TEXT)"))
        conn.execute(
            text("INSERT INTO accounts VALUES (1, 'Acme', 100.0), (2, 'Beta', 200.0), (3, 'Gamma', 300.0)")
        )
        conn.execute(text("INSERT INTO deprecated_logs VALUES (1, 'ok')"))

    before = discover_database(str(db_file))

    # Apply schema mutations: drop table, add table, add PII column, wipe rows
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE deprecated_logs"))
        conn.execute(text("CREATE TABLE new_metrics (id INTEGER PRIMARY KEY, score INTEGER)"))
        conn.execute(text("ALTER TABLE accounts ADD COLUMN owner_email VARCHAR(120)"))
        conn.execute(text("UPDATE accounts SET owner_email = 'ops@acme.io'"))
        conn.execute(text("DELETE FROM accounts"))
    engine.dispose()

    after = discover_database(str(db_file))
    diff = diff_snapshots(before, after)

    assert diff.has_breaking_changes is True
    assert "main.deprecated_logs" in diff.removed_tables
    assert "main.new_metrics" in diff.added_tables
    assert any(c.column == "owner_email" and c.change_type == "added" for c in diff.column_changes)
    assert any("owner_email" in p for p in diff.new_pii_columns)
    assert any(m.metric == "row_count" and m.after == 0 and m.severity == "high" for m in diff.metric_drifts)

    diff_md = render_diff_markdown(diff)
    assert "BREAKING CHANGES DETECTED" in diff_md
    assert "deprecated_logs" in diff_md


def test_cli_commands_end_to_end(tmp_path):
    # drivers
    res_drv = runner.invoke(app, ["drivers"])
    assert res_drv.exit_code == 0
    assert "duckdb" in res_drv.stdout.lower()

    # demo
    demo_db = tmp_path / "cli_demo.duckdb"
    res_demo = runner.invoke(app, ["demo", str(demo_db)])
    assert res_demo.exit_code == 0
    assert demo_db.exists()

    # discover with --out-dir
    out_dir = tmp_path / "bundle"
    res_disc = runner.invoke(app, ["discover", str(demo_db), "--out-dir", str(out_dir)])
    assert res_disc.exit_code == 0
    assert (out_dir / "report.html").exists()
    assert (out_dir / "report.md").exists()
    assert (out_dir / "snapshot.json").exists()
    assert (out_dir / "dbt" / "sources.yml").exists()

    # diff same snapshot -> exit 0 with --fail-on-breaking
    snap = out_dir / "snapshot.json"
    res_diff = runner.invoke(app, ["diff", str(snap), str(snap), "--fail-on-breaking"])
    assert res_diff.exit_code == 0
    assert "No Breaking Schema Changes" in res_diff.stdout
