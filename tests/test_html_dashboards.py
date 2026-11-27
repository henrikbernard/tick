"""Tests for upgraded Discovery HTML dashboard, Multi-Plant Harmonization HTML, and Schema Diff HTML."""

from __future__ import annotations

import json
import re

from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from tick import DiscoveryConfig, diff_snapshots, discover_database, harmonize_snapshots
from tick.cli import app
from tick.demo import create_manufacturing_demo_database
from tick.exporters import (
    export_diff_html,
    export_harmonization_html,
    render_diff_html,
    render_harmonization_html,
    render_html_report,
)
from tick.models import ColumnInfo, DatabaseOverview, DiscoveryResult, TableInfo

runner = CliRunner()


def _extract_json_script(html_text: str, script_id: str) -> dict:
    m = re.search(
        rf'<script id="{re.escape(script_id)}" type="application/json">(.*?)</script>',
        html_text,
        flags=re.DOTALL,
    )
    assert m is not None, f"Missing JSON script block #{script_id}"
    return json.loads(m.group(1).replace("<\\/", "</"))


def test_upgraded_discovery_html_charts_waves_and_svg_controls(tmp_path):
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    res = discover_database(str(db_path), config=DiscoveryConfig(profile_mode="full"))
    html_text = render_html_report(res)

    assert "<!DOCTYPE html>" in html_text
    # Overview visual charts & migration wave planner
    assert 'id="chart-cdc-donut"' in html_text
    assert 'id="chart-health-bars"' in html_text
    assert 'id="chart-role-bars"' in html_text
    assert 'id="migration-waves"' in html_text
    assert "Wave 1 · Fast-Track Lakeflow" in html_text
    assert "Wave 2 · Needs Config" in html_text
    assert "Wave 3 · Remediation Required" in html_text

    # Catalog boxplot, top-value bars, and column filter controls
    assert "function renderBoxplotSvg(" in html_text
    assert 'class="val-bar-row"' in html_text
    assert 'id="col-search"' in html_text
    assert 'id="col-pii-only"' in html_text
    assert 'id="col-high-null"' in html_text

    # ERD & Lineage search, zoom, fit, and SVG download
    for control_id in (
        "erd-search",
        "erd-fit",
        "erd-download-svg",
        "lin-search",
        "lin-zoom-in",
        "lin-zoom-out",
        "lin-fit",
        "lin-download-svg",
        "download-code",
        "print-btn",
    ):
        assert f'id="{control_id}"' in html_text

    assert "@media print" in html_text
    assert 'localStorage.getItem("tick-theme")' in html_text

    payload = _extract_json_script(html_text, "tick-data")
    assert payload["result"]["overview"]["table_count"] == 7


def test_harmonization_html_dashboard_and_cli_e2e(tmp_path):
    ohio_db = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    munich_db = create_manufacturing_demo_database(tmp_path / "munich.duckdb", plant_variant="plant_munich")

    ohio_dir = tmp_path / "plant_ohio"
    munich_dir = tmp_path / "plant_munich"
    ohio_dir.mkdir()
    munich_dir.mkdir()

    r1 = runner.invoke(app, ["discover", str(ohio_db), "--json", str(ohio_dir / "snapshot.json"), "--quiet"])
    r2 = runner.invoke(app, ["discover", str(munich_db), "--json", str(munich_dir / "snapshot.json"), "--quiet"])
    assert r1.exit_code == 0 and r2.exit_code == 0

    harm_html = tmp_path / "harmonization.html"
    r3 = runner.invoke(
        app,
        [
            "compare-sources",
            str(ohio_dir / "snapshot.json"),
            str(munich_dir / "snapshot.json"),
            "--html",
            str(harm_html),
        ],
    )
    assert r3.exit_code == 0
    assert harm_html.exists()

    html_text = harm_html.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html_text
    assert "Multi-Plant Schema Harmonization" in html_text
    assert 'id="matrix-table"' in html_text
    assert 'id="chart-align-donut"' in html_text
    assert 'id="chart-drift-bars"' in html_text
    assert 'id="inspector-host"' in html_text
    assert 'id="download-all-sql"' in html_text
    assert "@media print" in html_text

    payload = _extract_json_script(html_text, "tick-harmonize-data")
    assert payload["report"]["sources"] == ["plant_ohio", "plant_munich"]
    assert payload["report"]["tables_with_drift"] >= 2
    assert "UNION ALL" in payload["all_sql"]


def test_harmonization_html_escapes_script_injection(tmp_path):
    res1 = DiscoveryResult(
        config=DiscoveryConfig(),
        overview=DatabaseOverview(
            dialect="duckdb",
            driver="duckdb_engine",
            database_name="p1",
            sanitized_url="duckdb:///p1",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=1,
            view_count=0,
            column_count=1,
        ),
        tables=[
            TableInfo(
                schema_name="main",
                name="t</script><img src=x>",
                full_name="main.t</script><img src=x>",
                columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", spark_type="INT")],
            )
        ],
    )
    report = harmonize_snapshots({"</script>plant_a": res1, "plant_b": res1})
    out = export_harmonization_html(report, tmp_path / "xss_harm.html")
    html_text = out.read_text(encoding="utf-8")
    assert "<title>tick Harmonize — &lt;/script&gt;plant_a vs plant_b</title>" in html_text
    # Ensure embedded JSON script tag cannot break out via raw </script> or <!--<script>
    json_block = html_text.split('id="tick-harmonize-data" type="application/json">')[1].split("</script>")[0]
    assert "</script>" not in json_block
    assert "\\u003c/script\\u003e" in json_block
    assert json.loads(json_block)["report"]["sources"][0] == "</script>plant_a"


def test_diff_html_dashboard_and_cli_e2e(tmp_path):
    db_file = tmp_path / "drift.sqlite"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount DECIMAL(10, 2) NOT NULL)"))
        conn.execute(text("CREATE TABLE legacy_tbl (id INTEGER PRIMARY KEY)"))
        conn.execute(text("INSERT INTO orders VALUES (1, 10.0), (2, 20.0), (3, 30.0)"))
        conn.execute(text("INSERT INTO legacy_tbl VALUES (1)"))

    before_json = tmp_path / "before.json"
    after_json = tmp_path / "after.json"
    diff_html = tmp_path / "diff.html"

    r1 = runner.invoke(app, ["discover", str(db_file), "--json", str(before_json), "--quiet"])
    assert r1.exit_code == 0

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE legacy_tbl"))
        conn.execute(text("CREATE TABLE new_tbl (id INTEGER PRIMARY KEY)"))
        conn.execute(text("ALTER TABLE orders ADD COLUMN customer_email TEXT"))
        conn.execute(text("UPDATE orders SET customer_email = 'a@b.com'"))
        conn.execute(text("DELETE FROM orders"))
    engine.dispose()

    r2 = runner.invoke(app, ["discover", str(db_file), "--json", str(after_json), "--quiet"])
    assert r2.exit_code == 0

    r3 = runner.invoke(app, ["diff", str(before_json), str(after_json), "--html", str(diff_html)])
    assert r3.exit_code == 0
    assert diff_html.exists()

    html_text = diff_html.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in html_text
    assert "Schema &amp; Data Drift Report" in html_text
    assert 'id="status-banner"' in html_text
    assert 'id="col-changes-table"' in html_text
    assert 'id="metric-drifts-host"' in html_text
    assert 'id="pii-alerts-host"' in html_text
    assert "@media print" in html_text

    payload = _extract_json_script(html_text, "tick-diff-data")
    assert payload["has_breaking_changes"] is True
    assert "main.legacy_tbl" in payload["removed_tables"]
    assert "main.new_tbl" in payload["added_tables"]
    assert any("customer_email" in p for p in payload["new_pii_columns"])

    # Also test non-breaking self-diff via direct render/export
    same_res = discover_database(str(db_file), config=DiscoveryConfig(profile_mode="fast"))
    safe_diff = diff_snapshots(same_res, same_res)
    safe_path = export_diff_html(safe_diff, tmp_path / "safe_diff.html")
    safe_html = safe_path.read_text(encoding="utf-8")
    safe_payload = _extract_json_script(safe_html, "tick-diff-data")
    assert safe_payload["has_breaking_changes"] is False
    assert render_diff_html(safe_diff) == safe_html
