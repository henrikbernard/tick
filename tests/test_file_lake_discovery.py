"""Tests for Local & Data Lake File Discovery (Parquet, CSV, TSV, JSON/JSONL, Hive partitions, Delta, Auto Loader & Snowpipe)."""

from __future__ import annotations

import ast
import csv
import gzip
import json

import duckdb
import pytest
import sqlglot
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database, harmonize_snapshots
from tick.cli import app
from tick.demo import create_files_demo_landing_zone, create_manufacturing_demo_database
from tick.dialects import DriverMissingError
from tick.exporters import (
    export_databricks_bundle,
    export_snowflake_bundle,
    render_databricks_autoloader_sql,
    render_html_report,
    render_markdown_report,
    render_snowflake_file_ingest_sql,
)
from tick.file_lake import is_file_lake_target, mount_file_lake_engine

runner = CliRunner()


def test_is_file_lake_target_detection(tmp_path):
    csv_f = tmp_path / "orders.csv"
    csv_f.write_text("id,name\n1,a\n", encoding="utf-8")
    assert is_file_lake_target(str(csv_f)) is True
    assert is_file_lake_target(str(tmp_path)) is True
    assert is_file_lake_target(str(tmp_path / "*.csv")) is True
    assert is_file_lake_target("data.parquet") is True
    assert is_file_lake_target("events.jsonl.gz") is True
    assert is_file_lake_target("postgresql://user:pass@host/db") is False
    assert is_file_lake_target("warehouse.duckdb") is False
    assert is_file_lake_target(":memory:") is False


def test_multi_format_landing_zone_discovery_and_cloud_ingest_codegen(tmp_path):
    lake_dir = create_files_demo_landing_zone(tmp_path / "landing_zone")
    res = discover_database(
        str(lake_dir),
        config=DiscoveryConfig(
            target_platform="both",
            databricks_catalog="mfg_lake",
            databricks_schema="bronze_drop",
            snowflake_database="MFG_LAKE",
            snowflake_schema="RAW_DROP",
        ),
    )

    assert res.overview.dialect == "files"
    assert res.overview.database_name == "landing_zone"
    assert res.overview.table_count == 5  # work_orders, historian_tags, bill_of_materials, telemetry (Hive), sap.MARA

    by_full = {t.full_name: t for t in res.tables}
    assert "files.work_orders" in by_full
    assert "files.historian_tags" in by_full
    assert "files.bill_of_materials" in by_full
    assert "files.telemetry" in by_full
    assert "sap.MARA" in by_full

    # Verify FileSourceInfo metadata
    wo = by_full["files.work_orders"]
    assert wo.file_source is not None
    assert wo.file_source.file_format == "csv"
    assert wo.file_source.delimiter == ","
    assert wo.file_source.has_header is True
    assert wo.row_count == 30
    assert wo.row_count_exact is True
    assert wo.size_bytes and wo.size_bytes > 0
    # PII & governance detected on raw CSV before touching Bronze!
    wo_cols = {c.name: c for c in wo.columns}
    assert wo_cols["operator_email"].profile and wo_cols["operator_email"].profile.pii_findings
    assert wo_cols["unit_cost"].profile and wo_cols["unit_cost"].profile.governance_findings

    # Hive-partitioned Parquet dataset grouped into ONE table with partition column `plant`
    tel = by_full["files.telemetry"]
    assert tel.file_source is not None
    assert tel.file_source.file_format == "parquet"
    assert tel.file_source.file_count == 2
    assert tel.file_source.hive_partitioned is True
    assert tel.file_source.partition_columns == ["plant"]
    assert tel.row_count == 40
    assert any(c.name == "plant" for c in tel.columns)
    assert tel.telemetry_profile is not None  # Wide PLC telemetry detected on Parquet!

    # BOM hierarchy detected on Parquet + OT historian detected on JSONL + SAP glossary on sap/MARA.csv!
    assert by_full["files.bill_of_materials"].hierarchy is not None
    assert by_full["files.historian_tags"].telemetry_profile is not None
    assert by_full["sap.MARA"].semantic_name == "material_master"

    # CDC readiness reports file-native cloud ingestion
    assert wo.cdc_readiness is not None
    assert wo.cdc_readiness.status == "ready"
    assert wo.cdc_readiness.connector_recommendation == "databricks_autoloader_cloudfiles"

    # Databricks bundle writes Auto Loader cloudFiles DLT Python + autoloader_ingest.sql + Gold BOM SQL!
    dbx_dir = tmp_path / "dbx_out"
    dbx_written = {p.name for p in export_databricks_bundle(res, dbx_dir)}
    assert "autoloader_ingest.sql" in dbx_written
    assert "gold_hierarchies.sql" in dbx_written

    dlt_py = (dbx_dir / "lakeflow_pipeline.py").read_text(encoding="utf-8")
    ast.parse(dlt_py)
    assert "spark.readStream.format('cloudFiles')" in dlt_py
    assert ".option('cloudFiles.format', 'csv')" in dlt_py
    assert ".option('cloudFiles.format', 'parquet')" in dlt_py
    assert ".option('cloudFiles.format', 'json')" in dlt_py

    al_sql = (dbx_dir / "autoloader_ingest.sql").read_text(encoding="utf-8")
    assert "CREATE VOLUME IF NOT EXISTS `mfg_lake`.`bronze_drop`.`landing_zone`;" in al_sql
    assert "COPY INTO `mfg_lake`.`bronze_drop`.`files_work_orders`" in al_sql
    assert "COPY INTO `mfg_lake`.`bronze_drop`.`sap_material_master`" in al_sql
    assert "FILEFORMAT = CSV" in al_sql
    assert "FILEFORMAT = PARQUET" in al_sql
    assert "FILEFORMAT = JSON" in al_sql

    # Snowflake bundle writes snowflake_file_ingest.sql (FILE FORMAT + STAGE + COPY INTO + Snowpipe)!
    sf_dir = tmp_path / "sf_out"
    sf_written = {p.name for p in export_snowflake_bundle(res, sf_dir)}
    assert "snowflake_file_ingest.sql" in sf_written
    sf_fi_sql = (sf_dir / "snowflake_file_ingest.sql").read_text(encoding="utf-8")
    assert 'CREATE FILE FORMAT IF NOT EXISTS "MFG_LAKE"."RAW_DROP"."TICK_FMT_CSV"' in sf_fi_sql
    assert 'CREATE FILE FORMAT IF NOT EXISTS "MFG_LAKE"."RAW_DROP"."TICK_FMT_PARQUET"' in sf_fi_sql
    assert 'CREATE FILE FORMAT IF NOT EXISTS "MFG_LAKE"."RAW_DROP"."TICK_FMT_JSONL"' in sf_fi_sql
    assert 'CREATE STAGE IF NOT EXISTS "MFG_LAKE"."RAW_DROP"."TICK_LANDING_STAGE";' in sf_fi_sql
    assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in sf_fi_sql
    assert "CREATE PIPE IF NOT EXISTS" in sf_fi_sql
    # Parse executable Snowflake statements with sqlglot
    parsed_sf = [s for s in sqlglot.parse(sf_fi_sql, read="snowflake") if s is not None]
    assert len(parsed_sf) >= 6

    # HTML & Markdown surface file metadata + file ingestion tabs
    html_text = render_html_report(res)
    assert "Auto Loader SQL (DBX)" in html_text
    assert "Snowflake File Ingest" in html_text
    assert '"has_file_sources": true' in html_text

    md_text = render_markdown_report(res)
    assert "**File Source:** `PARQUET`" in md_text
    assert "Hive partitions: `plant`" in md_text


def test_single_gzipped_csv_delta_dir_sharded_and_glob_discovery(tmp_path):
    # 1. Single gzipped semicolon-delimited CSV
    gz_csv = tmp_path / "shipments.csv.gz"
    with gzip.open(gz_csv, "wt", encoding="utf-8", newline="") as fh:
        fh.write("shipment_id;carrier;weight_kg\n1;DHL;12.5\n2;UPS;4.2\n")

    gz_res = discover_database(str(gz_csv))
    assert gz_res.overview.table_count == 1
    stbl = gz_res.tables[0]
    assert stbl.file_source is not None
    assert stbl.file_source.file_format == "csv"
    assert stbl.file_source.compression == "gzip"
    assert stbl.file_source.delimiter == ";"
    assert stbl.row_count == 2

    # 2. Delta Lake directory (_delta_log + parquet data)
    delta_dir = tmp_path / "orders_delta"
    (delta_dir / "_delta_log").mkdir(parents=True)
    (delta_dir / "_delta_log" / "00000000000000000000.json").write_text('{"protocol":{"minReaderVersion":1}}\n', encoding="utf-8")
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"COPY (SELECT 1 AS order_id, 99.5 AS total_usd UNION ALL SELECT 2, 150.0) TO '{delta_dir / 'part-0000.parquet'}' (FORMAT PARQUET)")
    finally:
        con.close()

    delta_res = discover_database(str(delta_dir))
    assert delta_res.overview.table_count == 1
    dtbl = delta_res.tables[0]
    assert dtbl.file_source is not None
    assert dtbl.file_source.file_format == "delta"
    assert dtbl.row_count == 2
    assert "SHALLOW CLONE delta." in render_databricks_autoloader_sql(delta_res)

    # 3. Sharded CSV files via glob pattern
    shard_dir = tmp_path / "shards"
    shard_dir.mkdir()
    (shard_dir / "part-0000.csv").write_text("id,val\n1,10\n2,20\n", encoding="utf-8")
    (shard_dir / "part-0001.csv").write_text("id,val\n3,30\n", encoding="utf-8")

    glob_res = discover_database(str(shard_dir / "*.csv"))
    assert glob_res.overview.table_count == 1
    assert glob_res.tables[0].file_source is not None
    assert glob_res.tables[0].file_source.file_count == 2
    assert glob_res.tables[0].row_count == 3


def test_sqlite_zero_dependency_fallback_for_csv_tsv_jsonl_and_hive_csv(tmp_path):
    drop = tmp_path / "fallback_drop"
    drop.mkdir()

    # TSV file
    (drop / "items.tsv").write_text("sku\tprice\tactive\nA1\t10.5\ttrue\nA2\t22.0\tfalse\n", encoding="utf-8")
    # JSON array file
    (drop / "vendors.json").write_text(
        json.dumps([{"vendor_id": 1, "name": "Acme"}, {"vendor_id": 2, "name": "Globex"}]),
        encoding="utf-8",
    )
    # Hive-partitioned CSV directory
    for region in ("us", "eu"):
        rdir = drop / "sales" / f"region={region}"
        rdir.mkdir(parents=True)
        with (rdir / "part-0000.csv").open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["sale_id", "amount"])
            w.writerow([1 if region == "us" else 2, 100.25])

    mounted = mount_file_lake_engine(str(drop), DiscoveryConfig(), prefer_duckdb=False)
    try:
        assert mounted.engine.dialect.name == "sqlite"
        assert set(mounted.file_sources.keys()) == {"main.items", "main.vendors", "main.sales"}
        assert mounted.exact_row_counts["main.sales"] == 2
        assert mounted.file_sources["main.sales"].hive_partitioned is True
        assert mounted.file_sources["main.sales"].partition_columns == ["region"]
    finally:
        mounted.engine.dispose()

    # Parquet without DuckDB raises a helpful DriverMissingError
    pq = tmp_path / "only.parquet"
    pq.write_bytes(b"PAR1")
    with pytest.raises(DriverMissingError, match="tick-db\\[duckdb\\]"):
        mount_file_lake_engine(str(pq), DiscoveryConfig(), prefer_duckdb=False)


def test_file_lake_cli_demo_discover_and_cross_source_harmonize(tmp_path):
    lake_dir = tmp_path / "lake_drop"
    r_demo = runner.invoke(app, ["demo", str(lake_dir), "--domain", "files"])
    assert r_demo.exit_code == 0
    assert (lake_dir / "work_orders.csv").exists()

    out_dir = tmp_path / "lake_out"
    r_disc = runner.invoke(app, ["discover", str(lake_dir), "-o", str(out_dir)], env={"COLUMNS": "180"})
    assert r_disc.exit_code == 0
    assert "PARQUET" in r_disc.output
    assert "CSV" in r_disc.output
    assert (out_dir / "report.html").exists()
    assert (out_dir / "databricks" / "autoloader_ingest.sql").exists()
    assert (out_dir / "snowflake" / "snowflake_file_ingest.sql").exists()

    # Harmonize raw file landing zone snapshot directly against a DuckDB database snapshot!
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")
    file_snap = discover_database(str(lake_dir))
    db_snap = discover_database(str(db_path))
    harm = harmonize_snapshots({"sftp_drop": file_snap, "ohio_db": db_snap}, target_platform="snowflake")
    tbl_by_name = {t.table_name: t for t in harm.tables}
    assert "work_orders" in tbl_by_name
    assert set(tbl_by_name["work_orders"].present_in_sources) == {"sftp_drop", "ohio_db"}
