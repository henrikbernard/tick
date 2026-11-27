"""Tests for Databricks Lakeflow/Delta readiness, manufacturing intelligence, safety guardrails, and multi-plant harmonization."""

from __future__ import annotations

import ast
import json

import pytest
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database, harmonize_snapshots
from tick.cli import app
from tick.databricks import map_to_spark_type, sanitize_delta_column_name
from tick.demo import create_manufacturing_demo_database
from tick.dialects import (
    DB2Adapter,
    DatabricksAdapter,
    HanaAdapter,
    PostgresAdapter,
    SqlServerAdapter,
    get_dialect_adapter,
)
from tick.exporters import export_databricks_bundle, export_html, export_markdown
from tick.harmonize import render_harmonization_markdown, render_harmonization_sql
from tick.pii import detect_governance

runner = CliRunner()


def test_enterprise_adapters_and_safe_sample_queries():
    dbx = get_dialect_adapter("databricks")
    db2 = get_dialect_adapter("db2")
    hana = get_dialect_adapter("hana")
    ms = get_dialect_adapter("mssql")
    pg = get_dialect_adapter("postgresql")

    assert isinstance(dbx, DatabricksAdapter)
    assert isinstance(db2, DB2Adapter)
    assert isinstance(hana, HanaAdapter)
    assert isinstance(ms, SqlServerAdapter)
    assert isinstance(pg, PostgresAdapter)

    assert "TABLESAMPLE (100 ROWS)" in dbx.build_sample_query("`t`", "`id`", 100, 1000, use_tablesample=True)
    assert "WITH (NOLOCK)" in ms.build_sample_query("[t]", "[id]", 100, 1000, read_uncommitted=True)
    assert "TABLESAMPLE (200 ROWS)" in ms.build_sample_query("[t]", "[id]", 100, 1000, use_tablesample=True, read_uncommitted=True)
    assert "TABLESAMPLE SYSTEM" in pg.build_sample_query('"t"', '"id"', 100, 1000, use_tablesample=True)
    assert "WITH UR" in db2.build_sample_query('"t"', '"id"', 100, 1000, read_uncommitted=True)


def test_spark_type_mapping_and_column_sanitization():
    assert sanitize_delta_column_name("Order #") == "order"
    assert sanitize_delta_column_name("Qty/Hr") == "qty_hr"
    assert sanitize_delta_column_name("123_sensor") == "c_123_sensor"

    assert map_to_spark_type("DECIMAL(50, 4)", "decimal") == "DECIMAL(38, 4)"
    assert map_to_spark_type("MONEY", "decimal") == "DECIMAL(19, 4)"
    assert map_to_spark_type("DATETIME2", "timestamp") == "TIMESTAMP_NTZ"
    assert map_to_spark_type("TIMESTAMPTZ", "timestamp") == "TIMESTAMP"
    assert map_to_spark_type("JSONB", "json") == "VARIANT"


def test_manufacturing_governance_classifier():
    plant_gov = detect_governance("WERKS", "string")
    assert any(g.category == "plant_isolation_key" and g.uc_policy == "row_filter" for g in plant_gov)

    cost_gov = detect_governance("unit_cost", "decimal")
    assert any(g.category == "commercial_sensitive" and g.uc_policy == "column_mask" for g in cost_gov)

    itar_gov = detect_governance("cad_drawing_path", "string")
    assert any(g.category == "ip_or_export_controlled" and g.uc_tag_value == "itar_ear_ip" for g in itar_gov)


@pytest.mark.parametrize("ext", [".sqlite", ".duckdb"])
def test_manufacturing_discovery_end_to_end(tmp_path, ext):
    db_path = create_manufacturing_demo_database(tmp_path / f"ohio{ext}", plant_variant="plant_ohio")

    # Custom glossary override test
    glossary_file = tmp_path / "custom_glossary.yml"
    glossary_file.write_text(
        "tables:\n  plc_press_telemetry:\n    name: hydraulic_press_plc\n    description: Shop floor hydraulic press PLC metrics\n",
        encoding="utf-8",
    )

    result = discover_database(
        str(db_path),
        config=DiscoveryConfig(
            profile_mode="full",
            skip_empty_tables=True,
            use_tablesample=True,
            glossary_path=str(glossary_file),
            databricks_catalog="mfg_prod",
            databricks_schema="bronze_ohio",
        ),
    )

    ov = result.overview
    assert ov.table_count == 7
    assert ov.view_count == 1
    assert ov.empty_table_count == 1
    assert ov.governance_column_count >= 4
    assert ov.delta_warning_count >= 2
    assert ov.cdc_audit is not None

    tbls = {t.name.lower(): t for t in result.tables}

    # 1. Production safety: empty_sap_ztable skipped profiling cleanly
    assert tbls["empty_sap_ztable"].row_count == 0
    assert tbls["empty_sap_ztable"].skipped_profiling_reason is not None

    # 2. ERP / SAP & custom glossary semantic enrichment
    assert tbls["mara"].semantic_name == "material_master"
    assert tbls["aufk"].semantic_name == "production_order_master"
    assert tbls["plc_press_telemetry"].semantic_name == "hydraulic_press_plc"
    aufk_cols = {c.name.upper(): c for c in tbls["aufk"].columns}
    assert aufk_cols["WERKS"].semantic_name == "plant_code"
    assert aufk_cols["MATNR"].semantic_name == "material_number"

    # 3. Legacy date + split date/time detection (ERDAT + ERZET on AUFK)
    assert aufk_cols["ERDAT"].profile is not None
    assert aufk_cols["ERDAT"].profile.legacy_date is not None
    assert aufk_cols["ERDAT"].profile.legacy_date.encoding == "yyyymmdd_int"
    assert aufk_cols["ERDAT"].profile.legacy_date.paired_column is not None
    assert aufk_cols["ERDAT"].profile.legacy_date.paired_column.upper() == "ERZET"
    assert "to_timestamp" in aufk_cols["ERDAT"].profile.legacy_date.spark_expression

    # 4. OT Telemetry detection (narrow historian + wide PLC with flatline)
    hist = tbls["historian_tags"]
    assert hist.telemetry_profile is not None
    assert hist.telemetry_profile.pattern == "narrow_historian_eav"
    assert hist.telemetry_profile.tag_column == "tag_id"
    # 3 staggered tags emitted every 10s -> 30.0s per-tag polling cadence
    assert hist.telemetry_profile.median_interval_seconds == 30.0
    assert hist.telemetry_profile.bad_quality_rate is not None and hist.telemetry_profile.bad_quality_rate >= 0.09

    plc = tbls["plc_press_telemetry"]
    assert plc.telemetry_profile is not None
    assert plc.telemetry_profile.pattern == "wide_plc_polling"
    assert "vibration_mm_s" in plc.telemetry_profile.flatline_columns
    assert any(i.category == "sensor_flatline" for i in plc.quality_issues)

    # 5. Recursive BOM hierarchy detection
    bom = tbls["bill_of_materials"]
    assert bom.hierarchy is not None
    assert bom.hierarchy.pattern == "bom"
    assert bom.hierarchy.parent_column == "parent_part_id"
    assert bom.hierarchy.child_column == "child_part_id"
    assert bom.hierarchy.overlap_rate is not None and bom.hierarchy.overlap_rate > 0.0

    # 6. Delta Lake compatibility linter & Liquid Clustering on work_orders
    wo = tbls["work_orders"]
    assert wo.delta_compatibility is not None
    assert wo.delta_compatibility.requires_column_mapping is True
    assert any(i.category == "illegal_column_name" and i.column == "Order #" for i in wo.delta_compatibility.issues)
    assert any(i.category == "timezone_naive_timestamp" for i in wo.delta_compatibility.issues)
    assert "plant_code" in wo.delta_compatibility.liquid_clustering_keys

    # 7. Governance tags on work_orders
    wo_cols = {c.name: c for c in wo.columns}
    assert any(g.category == "plant_isolation_key" for g in wo_cols["plant_code"].profile.governance_findings)
    assert any(g.category == "commercial_sensitive" for g in wo_cols["unit_cost"].profile.governance_findings)
    assert any(g.category == "ip_or_export_controlled" for g in wo_cols["cad_drawing_path"].profile.governance_findings)

    # 8. Ingestion sizing & CDC readiness
    assert wo.ingestion_sizing is not None
    assert wo.ingestion_sizing.estimated_row_width_bytes > 0
    assert wo.cdc_readiness is not None
    assert wo.cdc_readiness.status == "ready"

    # 9. Databricks Exporter bundle + Python AST validation of Lakeflow DLT code!
    dbx_dir = tmp_path / "databricks"
    written = export_databricks_bundle(result, dbx_dir)
    assert len(written) == 3

    ddl_text = (dbx_dir / "unity_catalog_ddl.sql").read_text(encoding="utf-8")
    assert "CREATE CATALOG IF NOT EXISTS `mfg_prod`;" in ddl_text
    assert "USING DELTA" in ddl_text
    assert "CLUSTER BY (" in ddl_text
    assert "'delta.columnMapping.mode' = 'name'" in ddl_text
    assert "plant_row_filter" in ddl_text
    assert "mask_sensitive_string" in ddl_text
    assert "SET TAGS ('compliance' = 'itar_ear_ip')" in ddl_text

    dlt_text = (dbx_dir / "lakeflow_pipeline.py").read_text(encoding="utf-8")
    ast.parse(dlt_text)  # Guarantees generated Lakeflow Python is syntactically valid!
    assert "dlt.apply_changes(" in dlt_text
    assert "@dlt.expect_all(" in dlt_text
    assert "withWatermark(" in dlt_text
    assert "to_timestamp" in dlt_text

    # Markdown & HTML include Databricks + Manufacturing signals
    md_path = export_markdown(result, tmp_path / "report.md")
    md_text = md_path.read_text(encoding="utf-8")
    assert "Manufacturing & OT Pattern Signals" in md_text
    assert "Liquid Clustering" in md_text

    html_path = export_html(result, tmp_path / "report.html")
    html_text = html_path.read_text(encoding="utf-8")
    assert "Databricks &amp; Mfg" in html_text
    assert "Unity Catalog Delta DDL" in html_text
    assert "Lakeflow Pipeline" in html_text


def test_multi_plant_schema_harmonization_and_cli(tmp_path):
    ohio_db = create_manufacturing_demo_database(tmp_path / "plant_ohio.duckdb", plant_variant="plant_ohio")
    munich_db = create_manufacturing_demo_database(tmp_path / "plant_munich.duckdb", plant_variant="plant_munich")

    ohio_snap = tmp_path / "plant_ohio.json"
    munich_snap = tmp_path / "plant_munich.json"

    r1 = runner.invoke(
        app,
        [
            "discover",
            str(ohio_db),
            "--json",
            str(ohio_snap),
            "--databricks-dir",
            str(tmp_path / "dbx_ohio"),
            "--skip-empty-tables",
            "--quiet",
        ],
    )
    assert r1.exit_code == 0

    r2 = runner.invoke(
        app,
        ["discover", str(munich_db), "--json", str(munich_snap), "--quiet"],
    )
    assert r2.exit_code == 0

    harm_md = tmp_path / "harmonize.md"
    harm_json = tmp_path / "harmonize.json"
    harm_sql = tmp_path / "unified.sql"

    r3 = runner.invoke(
        app,
        [
            "compare-sources",
            str(ohio_snap),
            str(munich_snap),
            "--md",
            str(harm_md),
            "--json",
            str(harm_json),
            "--sql",
            str(harm_sql),
        ],
    )
    assert r3.exit_code == 0
    assert harm_md.exists() and harm_json.exists() and harm_sql.exists()

    report_data = json.loads(harm_json.read_text(encoding="utf-8"))
    assert report_data["tables_with_drift"] >= 2

    htbls = {t["table_name"]: t for t in report_data["tables"]}
    # empty_sap_ztable exists only in Ohio
    assert htbls["empty_sap_ztable"]["missing_in_sources"] == ["plant_munich"]

    # work_orders has type conflict on Order # (INT vs STRING) + site-only column eu_shift_supervisor
    wo = htbls["work_orders"]
    assert wo["conflict_count"] >= 1
    assert wo["site_specific_column_count"] >= 1
    wo_cols = {c["sanitized_name"]: c for c in wo["columns"]}
    assert wo_cols["order"]["has_type_conflict"] is True
    assert wo_cols["order"]["target_spark_type"] == "STRING"
    assert "plant_ohio" in wo_cols["order"]["cast_required_sources"]
    assert wo_cols["eu_shift_supervisor"]["missing_in_sources"] == ["plant_ohio"]

    sql_text = harm_sql.read_text(encoding="utf-8")
    assert "'plant_ohio' AS `source_plant`" in sql_text
    assert "'plant_munich' AS `source_plant`" in sql_text
    assert "CAST(`Order #` AS STRING) AS `order`" in sql_text
    assert "CAST(NULL AS STRING) AS `eu_shift_supervisor`" in sql_text

    # Also test CLI demo --domain manufacturing
    demo_cli_db = tmp_path / "cli_mfg.duckdb"
    r4 = runner.invoke(app, ["demo", str(demo_cli_db), "--domain", "manufacturing"])
    assert r4.exit_code == 0
    assert demo_cli_db.exists()
