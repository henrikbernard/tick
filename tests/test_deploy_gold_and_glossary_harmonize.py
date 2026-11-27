"""Tests for v0.5.0 deployment bundles (DABs, Lakeview, Snowflake CLI, Streamlit), Gold BOM SQL & glossary harmonization."""

from __future__ import annotations

import ast
import json

import sqlglot
import yaml
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database, harmonize_snapshots
from tick.cli import app
from tick.demo import create_demo_database, create_manufacturing_demo_database
from tick.exporters import (
    export_databricks_bundle,
    export_snowflake_bundle,
    render_databricks_bundle_yml,
    render_databricks_gold_hierarchies_sql,
    render_harmonization_html,
    render_html_report,
    render_lakeview_dashboard_json,
    render_snowflake_gold_hierarchies_sql,
    render_snowflake_project_yml,
    render_snowflake_streamlit_app,
)
from tick.harmonize import render_harmonization_markdown
from tick.models import ColumnInfo, DatabaseOverview, DiscoveryResult, TableInfo

runner = CliRunner()


def test_databricks_asset_bundle_lakeview_and_gold_bom_sql(tmp_path):
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    res = discover_database(
        str(db_path),
        config=DiscoveryConfig(
            target_platform="both",
            databricks_catalog="mfg_prod",
            databricks_schema="bronze_ohio",
        ),
    )

    out_dir = tmp_path / "dbx_bundle"
    written = export_databricks_bundle(res, out_dir)
    names = {p.name for p in written}
    assert {
        "unity_catalog_ddl.sql",
        "lakeflow_pipeline.py",
        "ingestion_sizing.md",
        "databricks.yml",
        "lakeview_dashboard.lvdash.json",
        "gold_hierarchies.sql",
        "metric_views.sql",
        "genie_context.md",
    } == names

    # 1. Validate Databricks Asset Bundle YAML
    dab = yaml.safe_load((out_dir / "databricks.yml").read_text(encoding="utf-8"))
    assert dab["bundle"]["name"].startswith("tick_")
    assert dab["variables"]["catalog"]["default"] == "mfg_prod"
    assert dab["variables"]["bronze_schema"]["default"] == "bronze_ohio"
    assert len(dab["resources"]["pipelines"]) == 1
    pipeline_cfg = next(iter(dab["resources"]["pipelines"].values()))
    assert pipeline_cfg["serverless"] is True
    assert pipeline_cfg["libraries"] == [{"file": {"path": "./lakeflow_pipeline.py"}}]
    job_cfg = next(iter(dab["resources"]["jobs"].values()))
    task_keys = [t["task_key"] for t in job_cfg["tasks"]]
    assert task_keys == [
        "apply_unity_catalog_ddl",
        "run_lakeflow_silver_pipeline",
        "build_gold_hierarchies",
        "publish_gold_metric_views",
    ]
    assert set(dab["targets"]) == {"dev", "prod"}

    # 2. Validate AI/BI Lakeview Dashboard JSON
    dash = json.loads((out_dir / "lakeview_dashboard.lvdash.json").read_text(encoding="utf-8"))
    ds_names = {d["name"] for d in dash["datasets"]}
    assert "uc_catalog_inventory" in ds_names
    assert "ot_telemetry_health" in ds_names
    assert "gold_bom_explosion" in ds_names
    assert "primary_fact_daily_trend" in ds_names
    assert len(dash["pages"]) == 1
    assert len(dash["pages"][0]["layout"]) >= 4

    # 3. Validate Databricks Gold Recursive BOM SQL with sqlglot
    gold_sql = (out_dir / "gold_hierarchies.sql").read_text(encoding="utf-8")
    assert "WITH RECURSIVE hierarchy_edges AS (" in gold_sql
    assert "NOT ARRAY_CONTAINS(r.`path_nodes`, CAST(e.`child_part_id` AS STRING))" in gold_sql
    assert "`extended_qty`" in gold_sql
    assert "`plant_id`" in gold_sql  # multi-plant scoped recursion
    parsed = [s for s in sqlglot.parse(gold_sql, read="databricks") if s is not None]
    assert len(parsed) >= 2

    # 4. Non-hierarchy database (ecommerce) omits gold_hierarchies.sql & build_gold_hierarchies task
    ecom_db = create_demo_database(tmp_path / "ecom.duckdb")
    ecom_res = discover_database(str(ecom_db), config=DiscoveryConfig(target_platform="databricks"))
    ecom_out = tmp_path / "ecom_dbx"
    ecom_names = {p.name for p in export_databricks_bundle(ecom_res, ecom_out)}
    assert "gold_hierarchies.sql" not in ecom_names
    assert "databricks.yml" in ecom_names
    ecom_dab = yaml.safe_load((ecom_out / "databricks.yml").read_text(encoding="utf-8"))
    ecom_tasks = [t["task_key"] for t in next(iter(ecom_dab["resources"]["jobs"].values()))["tasks"]]
    assert "build_gold_hierarchies" not in ecom_tasks


def test_snowflake_cli_manifest_streamlit_app_and_gold_bom_sql(tmp_path):
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    res = discover_database(
        str(db_path),
        config=DiscoveryConfig(
            target_platform="snowflake",
            snowflake_database="MFG_PROD",
            snowflake_schema="BRONZE_OHIO",
            snowflake_warehouse="ETL_WH",
        ),
    )

    out_dir = tmp_path / "sf_bundle"
    written = export_snowflake_bundle(res, out_dir)
    names = {p.name for p in written}
    assert {
        "snowflake_ddl.sql",
        "snowflake_dynamic_tables.sql",
        "snowflake_dmf.sql",
        "snowflake_sizing.md",
        "snowflake.yml",
        "streamlit_app.py",
        "snowflake_gold_hierarchies.sql",
        "cortex_analyst_semantic_model.yml",
    } == names

    # 1. Validate Snowflake CLI v2 manifest
    sf_yml = yaml.safe_load((out_dir / "snowflake.yml").read_text(encoding="utf-8"))
    assert sf_yml["definition_version"] == "2"
    assert sf_yml["env"]["SNOWFLAKE_DATABASE"] == "MFG_PROD"
    assert sf_yml["env"]["SNOWFLAKE_WAREHOUSE"] == "ETL_WH"
    ent = next(iter(sf_yml["entities"].values()))
    assert ent["type"] == "streamlit"
    assert ent["main_file"] == "streamlit_app.py"
    assert ent["query_warehouse"] == "ETL_WH"

    # 2. Validate Streamlit in Snowflake Python AST & content
    sis_code = (out_dir / "streamlit_app.py").read_text(encoding="utf-8")
    ast.parse(sis_code)
    assert "get_active_session()" in sis_code
    assert "DYNAMIC_TABLE_REFRESH_HISTORY" in sis_code
    assert "DATA_QUALITY_MONITORING_RESULTS" in sis_code
    assert "OT Telemetry Health" in sis_code
    assert "Gold BOM Explosion" in sis_code

    # 3. Validate Snowflake Gold Recursive Dynamic Table SQL with sqlglot
    sf_gold = (out_dir / "snowflake_gold_hierarchies.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE DYNAMIC TABLE" in sf_gold
    assert 'WITH RECURSIVE HIERARCHY_EDGES ("PLANT_ID", "ROOT_NODE_ID", "PARENT_NODE_ID", "CHILD_NODE_ID", "EDGE_QTY", "EXTENDED_QTY", "DEPTH", "PATH_NODES") AS (' in sf_gold
    assert 'ARRAY_CONTAINS(TO_VARCHAR(e."CHILD_PART_ID")::VARIANT, r."PATH_NODES")' in sf_gold
    assert '"EXTENDED_QTY"' in sf_gold
    assert 'e."PLANT_ID" IS NOT DISTINCT FROM r."PLANT_ID"' in sf_gold
    parsed = [s for s in sqlglot.parse(sf_gold, read="snowflake") if s is not None]
    assert len(parsed) >= 2

    # 4. Discovery HTML report embeds all new Code & DDL tabs
    html_text = render_html_report(res)
    for tab_label in ("Databricks Asset Bundle", "Gold BOM SQL (DBX)", "Gold BOM SQL (SF)", "Snowflake Streamlit App"):
        assert tab_label in html_text

    # 5. Exotic DB name with quotes still yields valid Python AST for Streamlit app
    exotic = res.model_copy(deep=True)
    exotic.overview.database_name = 'plant "A" \'test\' \\ """'
    ast.parse(render_snowflake_streamlit_app(exotic, database="DB'1\"X"))


def test_glossary_aware_multi_plant_harmonization_sap_vs_mes_and_cli(tmp_path):
    """Plant SAP runs SAP table AUFK (AUFNR, WERKS, MATNR) while Plant MES runs production_order_master."""
    sap_res = DiscoveryResult(
        config=DiscoveryConfig(),
        overview=DatabaseOverview(
            dialect="mssql",
            driver="pyodbc",
            database_name="sap_plant_a",
            sanitized_url="mssql://sap_plant_a",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=1,
            view_count=0,
            column_count=4,
        ),
        tables=[
            TableInfo(
                schema_name="dbo",
                name="AUFK",
                full_name="dbo.AUFK",
                primary_key=["AUFNR"],
                columns=[
                    ColumnInfo(name="AUFNR", raw_type="VARCHAR(16)", canonical_type="string", is_primary_key=True, nullable=False),
                    ColumnInfo(name="WERKS", raw_type="VARCHAR(6)", canonical_type="string", nullable=False),
                    ColumnInfo(name="MATNR", raw_type="VARCHAR(18)", canonical_type="string", nullable=False),
                    ColumnInfo(name="MENGE", raw_type="DECIMAL(10, 2)", canonical_type="decimal", nullable=False),
                ],
            )
        ],
    )
    mes_res = DiscoveryResult(
        config=DiscoveryConfig(),
        overview=DatabaseOverview(
            dialect="postgresql",
            driver="psycopg",
            database_name="mes_plant_b",
            sanitized_url="postgresql://mes_plant_b",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=1,
            view_count=0,
            column_count=4,
        ),
        tables=[
            TableInfo(
                schema_name="public",
                name="production_order_master",
                full_name="public.production_order_master",
                primary_key=["production_order_number"],
                columns=[
                    ColumnInfo(name="production_order_number", raw_type="INTEGER", canonical_type="integer", is_primary_key=True, nullable=False),
                    ColumnInfo(name="plant_code", raw_type="VARCHAR(8)", canonical_type="string", nullable=False),
                    ColumnInfo(name="material_number", raw_type="VARCHAR(18)", canonical_type="string", nullable=False),
                    ColumnInfo(name="quantity", raw_type="DECIMAL(12, 4)", canonical_type="decimal", nullable=False),
                ],
            )
        ],
    )

    # Without glossary matching: 2 completely separate tables (zero alignment)
    literal_rep = harmonize_snapshots({"sap": sap_res, "mes": mes_res}, match_by_glossary=False)
    assert literal_rep.total_unique_tables == 2
    assert literal_rep.glossary_matching_enabled is False

    # With built-in glossary matching (Databricks): unifies into 1 table!
    glos_dbx = harmonize_snapshots({"sap": sap_res, "mes": mes_res}, target_platform="databricks", match_by_glossary=True)
    assert glos_dbx.glossary_matching_enabled is True
    assert glos_dbx.total_unique_tables == 1
    tbl = glos_dbx.tables[0]
    assert tbl.table_name == "production_order_master"
    assert tbl.matched_via_glossary is True
    assert tbl.source_table_names == {"sap": "AUFK", "mes": "production_order_master"}
    assert tbl.pk_divergent is False  # AUFNR == production_order_number via glossary!
    assert tbl.site_specific_column_count == 0
    assert tbl.conflict_count == 2  # production_order_number (STRING vs INT) & quantity (DECIMAL(10,2) vs (12,4))
    assert "FROM `manufacturing`.`bronze_sap`.`aufk`" in tbl.unified_sql
    assert "FROM `manufacturing`.`bronze_mes`.`production_order_master`" in tbl.unified_sql
    assert "`WERKS` AS `plant_code`" in tbl.unified_sql
    assert "CAST(`production_order_number` AS STRING) AS `production_order_number`" in tbl.unified_sql

    # With Snowflake target + glossary matching
    glos_sf = harmonize_snapshots({"sap": sap_res, "mes": mes_res}, target_platform="snowflake", match_by_glossary=True)
    sf_tbl = glos_sf.tables[0]
    assert 'FROM "MANUFACTURING"."BRONZE_SAP"."AUFK"' in sf_tbl.unified_sql
    assert 'FROM "MANUFACTURING"."BRONZE_MES"."PRODUCTION_ORDER_MASTER"' in sf_tbl.unified_sql
    assert '"WERKS" AS "PLANT_CODE"' in sf_tbl.unified_sql
    assert 'CAST("production_order_number" AS VARCHAR) AS "PRODUCTION_ORDER_NUMBER"' in sf_tbl.unified_sql

    # Markdown & HTML dashboards surface glossary match metadata
    md = render_harmonization_markdown(glos_sf)
    assert "Glossary-Aware Semantic Matching" in md
    assert "Glossary-Matched Source Tables" in md
    assert "`AUFK`" in md

    html_out = render_harmonization_html(glos_sf)
    assert "Glossary Semantic Matching: ON" in html_out
    assert "GLOSSARY" in html_out

    # Custom YAML glossary file via CLI `tick compare-sources --glossary`
    sap_json = tmp_path / "sap.json"
    mes_json = tmp_path / "mes.json"
    sap_json.write_text(sap_res.model_dump_json(), encoding="utf-8")
    mes_json.write_text(mes_res.model_dump_json(), encoding="utf-8")

    custom_glossary = tmp_path / "custom_glossary.yml"
    custom_glossary.write_text(
        yaml.safe_dump(
            {
                "tables": {"AUFK": {"name": "global_work_orders", "description": "Orders"}, "PRODUCTION_ORDER_MASTER": {"name": "global_work_orders", "description": "Orders"}},
                "columns": {"WERKS": "site_code", "PLANT_CODE": "site_code"},
            }
        ),
        encoding="utf-8",
    )
    sql_out = tmp_path / "custom_unified.sql"
    cli_res = runner.invoke(
        app,
        [
            "compare-sources",
            str(sap_json),
            str(mes_json),
            "--target",
            "snowflake",
            "--glossary",
            str(custom_glossary),
            "--sql",
            str(sql_out),
        ],
    )
    assert cli_res.exit_code == 0
    assert "Glossary Matching: ON" in cli_res.output
    sql_text = sql_out.read_text(encoding="utf-8")
    assert 'UNIFIED_GLOBAL_WORK_ORDERS' in sql_text
    assert '"WERKS" AS "SITE_CODE"' in sql_text
    assert '"plant_code" AS "SITE_CODE"' in sql_text
