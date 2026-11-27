"""Round-7 bug regression tests for Local & Data Lake File Discovery and Cloud Ingest Exporters."""

from __future__ import annotations

import ast
import gzip
import json

import duckdb
import sqlglot
import yaml
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database
from tick.cli import app
from tick.exporters import (
    export_snowflake_bundle,
    render_databricks_autoloader_sql,
    render_lakeflow_pipeline_py,
    render_snowflake_file_ingest_sql,
    render_snowflake_project_yml,
)
from tick.file_lake import mount_file_lake_engine

runner = CliRunner()


def test_delta_log_replay_excludes_removed_tombstones_and_uses_delta_stream(tmp_path):
    """Delta Lake discovery must replay _delta_log add/remove actions (including URL-encoded partitions)
    and Lakeflow DLT must read Delta sources with format('delta'), never raw cloudFiles parquet."""
    delta_dir = tmp_path / "shipments_delta"
    log_dir = delta_dir / "_delta_log"
    p_us = delta_dir / "plant=US01"
    p_de = delta_dir / "plant=DE02"
    log_dir.mkdir(parents=True)
    p_us.mkdir(parents=True)
    p_de.mkdir(parents=True)

    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT 1 AS shipment_id, 'OLD_DELETED' AS status UNION ALL SELECT 2, 'OLD_DELETED') "
            f"TO '{p_us / 'part-0000.parquet'}' (FORMAT PARQUET)"
        )
        con.execute(
            f"COPY (SELECT 10 AS shipment_id, 'ACTIVE' AS status) "
            f"TO '{p_de / 'part-0001.parquet'}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    # Commit 0 adds US01/part-0000; Commit 1 removes US01/part-0000 and adds URL-encoded DE02/part-0001
    (log_dir / "00000000000000000000.json").write_text(
        json.dumps({"protocol": {"minReaderVersion": 1}})
        + "\n"
        + json.dumps({"add": {"path": "plant%3DUS01/part-0000.parquet", "size": 100}})
        + "\n",
        encoding="utf-8",
    )
    (log_dir / "00000000000000000001.json").write_text(
        json.dumps({"remove": {"path": "plant%3DUS01/part-0000.parquet"}})
        + "\n"
        + json.dumps({"add": {"path": "plant%3DDE02/part-0001.parquet", "size": 100}})
        + "\n",
        encoding="utf-8",
    )

    res = discover_database(str(delta_dir))
    assert res.overview.table_count == 1
    tbl = res.tables[0]
    assert tbl.file_source is not None
    assert tbl.file_source.file_format == "delta"
    # Only the 1 active row from DE02/part-0001 must be counted — the removed US01 tombstone file is excluded!
    assert tbl.file_source.file_count == 1
    assert tbl.row_count == 1
    assert tbl.file_source.hive_partitioned is True
    assert tbl.file_source.partition_columns == ["plant"]

    # Lakeflow DLT pipeline must stream Delta natively with format('delta'), NOT raw cloudFiles parquet
    dlt_py = render_lakeflow_pipeline_py(res)
    ast.parse(dlt_py)
    assert "spark.readStream.format('delta').load(" in dlt_py
    assert "cloudFiles.format', 'parquet'" not in dlt_py

    # SHALLOW CLONE escapes backticks safely if catalog/schema contains a backtick
    al_sql = render_databricks_autoloader_sql(res, catalog="cat`weird", schema="bronze")
    assert "SHALLOW CLONE delta.`/Volumes/cat``weird/bronze/landing_zone/" in al_sql


def test_snowflake_file_ingest_explicit_columns_and_collision_free_formats(tmp_path):
    """Snowflake CSV/TSV COPY INTO & Snowpipe must specify explicit target columns (for Bronze audit cols),
    exclude synthetic Hive partition cols, and generate distinct FILE FORMAT names for ;/|/nohdr/gz."""
    drop = tmp_path / "mixed_drop"
    drop.mkdir()

    # 1. Semicolon CSV
    (drop / "semi_orders.csv").write_text("Order #;amount\n1;10.5\n2;20.0\n", encoding="utf-8")
    # 2. Pipe CSV
    (drop / "pipe_orders.csv").write_text("order_id|amount\n1|10.5\n2|20.0\n", encoding="utf-8")
    # 3. Headerless comma CSV
    (drop / "nohdr_metrics.csv").write_text("1,100.5,2026-01-01\n2,200.5,2026-01-02\n", encoding="utf-8")
    # 4. Gzipped semicolon CSV
    with gzip.open(drop / "gz_semi.csv.gz", "wt", encoding="utf-8", newline="") as fh:
        fh.write("id;val\n1;99\n2;88\n")
    # 5. Plain JSONL & Gzipped JSONL
    (drop / "events_plain.jsonl").write_text('{"id": 1, "msg": "ok"}\n', encoding="utf-8")
    with gzip.open(drop / "events_gz.jsonl.gz", "wt", encoding="utf-8") as fh:
        fh.write('{"id": 2, "msg": "gz"}\n')
    # 6. Hive-partitioned CSV
    for reg in ("US", "EU"):
        rdir = drop / "regional_sales" / f"region={reg}"
        rdir.mkdir(parents=True)
        (rdir / "part-0000.csv").write_text("sale_id,total\n1,50.0\n", encoding="utf-8")

    res = discover_database(
        str(drop),
        config=DiscoveryConfig(
            target_platform="both",
            snowflake_database="MFG_DB",
            snowflake_schema="RAW",
        ),
    )

    sf_sql = render_snowflake_file_ingest_sql(res)
    # All distinct format names emitted without colliding
    assert '"TICK_FMT_CSV_SEMI"' in sf_sql
    assert '"TICK_FMT_CSV_PIPE"' in sf_sql
    assert '"TICK_FMT_CSV_NOHDR"' in sf_sql
    assert '"TICK_FMT_CSV_SEMI_GZ"' in sf_sql
    assert '"TICK_FMT_JSONL"' in sf_sql
    assert '"TICK_FMT_JSONL_GZ"' in sf_sql
    assert "FIELD_DELIMITER = ';'" in sf_sql
    assert "FIELD_DELIMITER = '|'" in sf_sql
    assert "SKIP_HEADER = 0" in sf_sql
    assert "COMPRESSION = 'GZIP'" in sf_sql

    # Explicit column list emitted for CSV tables (matching Bronze sanitized column names, excluding _INGESTED_AT)
    assert 'COPY INTO "MFG_DB"."RAW"."SEMI_ORDERS" ("ORDER", "AMOUNT")' in sf_sql
    assert '-- COPY INTO "MFG_DB"."RAW"."SEMI_ORDERS" ("ORDER", "AMOUNT")' in sf_sql
    # Synthetic Hive partition column `REGION` excluded from CSV positional column list
    assert 'COPY INTO "MFG_DB"."RAW"."REGIONAL_SALES" ("SALE_ID", "TOTAL")' in sf_sql

    # All generated Snowflake SQL parses cleanly with sqlglot
    parsed = [s for s in sqlglot.parse(sf_sql, read="snowflake") if s is not None]
    assert len(parsed) >= 12

    # snowflake.yml deployment sequence includes snowflake_file_ingest.sql right after snowflake_ddl.sql
    sf_yml = render_snowflake_project_yml(res)
    assert "snow sql -f snowflake_file_ingest.sql" in sf_yml
    assert sf_yml.index("snowflake_ddl.sql") < sf_yml.index("snowflake_file_ingest.sql") < sf_yml.index("snowflake_dynamic_tables.sql")
    yaml.safe_load(sf_yml)

    sf_out = tmp_path / "sf_bundle"
    written = {p.name for p in export_snowflake_bundle(res, sf_out)}
    assert "snowflake_file_ingest.sql" in written

    # Databricks Lakeflow DLT enables delta.columnMapping.mode on Bronze Auto Loader tables with invalid col names (`Order #`)
    dlt_py = render_lakeflow_pipeline_py(res)
    ast.parse(dlt_py)
    assert "table_properties={'delta.columnMapping.mode': 'name'}" in dlt_py


def test_recursive_glob_preserves_root_name_and_hive_partitions(tmp_path):
    """Recursive glob ./lake/**/*.parquet must walk past `**` to resolve db_name and Hive partition columns."""
    lake = tmp_path / "factory_lake"
    con = duckdb.connect(":memory:")
    try:
        for plant in ("US01", "DE02"):
            pdir = lake / f"plant={plant}"
            pdir.mkdir(parents=True)
            con.execute(
                f"COPY (SELECT 1 AS id, 42.0 AS val) TO '{pdir / 'data.parquet'}' (FORMAT PARQUET)"
            )
    finally:
        con.close()

    res = discover_database(str(lake / "**" / "*.parquet"))
    assert res.overview.database_name == "factory_lake"
    assert res.overview.table_count == 1
    tbl = res.tables[0]
    assert tbl.file_source is not None
    assert tbl.file_source.hive_partitioned is True
    assert tbl.file_source.partition_columns == ["plant"]
    assert tbl.row_count == 2
    assert any(c.name == "plant" for c in tbl.columns)


def test_csv_utf8_bom_duplicate_headers_and_short_all_string_header_detection(tmp_path):
    """UTF-8 BOM must be stripped, duplicate/case-colliding headers deduplicated,
    and short all-string or 1-column CSVs must still detect has_header=True."""
    drop = tmp_path / "csv_edge_cases"
    drop.mkdir()

    # 1. Excel UTF-8 BOM CSV
    (drop / "bom_users.csv").write_bytes("\ufeffuser_id,email\n1,alice@example.com\n2,bob@example.com\n".encode("utf-8"))
    # 2. Duplicate + case-colliding CSV headers
    (drop / "dup_cols.csv").write_text("id,val,val,VAL\n1,a,b,c\n2,d,e,f\n", encoding="utf-8")
    # 3. Short all-string CSV (csv.Sniffer false-negative regression)
    (drop / "lookup_codes.csv").write_text("code,name\nA,Alpha\nB,Beta\n", encoding="utf-8")
    # 4. Single-column all-string CSV
    (drop / "tag_list.csv").write_text("tag_id\nT1\nT2\n", encoding="utf-8")
    # 5. Truly headerless numeric CSV
    (drop / "raw_readings.csv").write_text("1,10.5,2026-01-01\n2,20.5,2026-01-02\n", encoding="utf-8")

    # Test with primary DuckDB engine
    res = discover_database(str(drop))
    by_name = {t.name: t for t in res.tables}
    assert [c.name for c in by_name["bom_users"].columns] == ["user_id", "email"]
    assert len(by_name["dup_cols"].columns) == 4
    assert by_name["lookup_codes"].file_source and by_name["lookup_codes"].file_source.has_header is True
    assert [c.name for c in by_name["lookup_codes"].columns] == ["code", "name"]
    assert by_name["tag_list"].file_source and by_name["tag_list"].file_source.has_header is True
    assert [c.name for c in by_name["tag_list"].columns] == ["tag_id"]
    assert by_name["raw_readings"].file_source and by_name["raw_readings"].file_source.has_header is False

    # Test zero-dependency SQLite fallback on the exact same edge cases
    mounted = mount_file_lake_engine(str(drop), DiscoveryConfig(), prefer_duckdb=False)
    try:
        from sqlalchemy import inspect as sa_inspect

        insp = sa_inspect(mounted.engine)
        bom_cols = [c["name"] for c in insp.get_columns("bom_users")]
        assert bom_cols == ["user_id", "email"]
        dup_cols = [c["name"] for c in insp.get_columns("dup_cols")]
        assert len(dup_cols) == 4
        assert len({c.lower() for c in dup_cols}) == 4
        assert [c["name"] for c in insp.get_columns("lookup_codes")] == ["code", "name"]
        assert [c["name"] for c in insp.get_columns("tag_list")] == ["tag_id"]
    finally:
        mounted.engine.dispose()


def test_nested_sharded_table_inside_schema_dir_and_empty_files_safety(tmp_path):
    """Nested sharded folders inside a schema dir must group as one table (schema.table),
    and 0-byte CSV/JSON/JSONL/empty-gzip files must not crash discovery."""
    drop = tmp_path / "enterprise_drop"
    sap_orders = drop / "sap" / "orders"
    sap_orders.mkdir(parents=True)
    (sap_orders / "part-0000.csv").write_text("order_id,qty\n1,10\n", encoding="utf-8")
    (sap_orders / "part-0001.csv").write_text("order_id,qty\n2,20\n", encoding="utf-8")

    # Empty 0-byte files + empty gzip stream alongside real data
    (drop / "empty_extract.csv").write_text("", encoding="utf-8")
    (drop / "empty_events.jsonl").write_text("", encoding="utf-8")
    (drop / "empty_payload.json").write_text("   \n", encoding="utf-8")
    with gzip.open(drop / "empty_gz.csv.gz", "wt", encoding="utf-8") as fh:
        fh.write("")

    res = discover_database(str(drop))
    by_full = {t.full_name: t for t in res.tables}

    # Nested sharded folder grouped as ONE table `sap.orders` with 2 files and 2 rows
    assert "sap.orders" in by_full
    assert "sap.part_0000" not in by_full
    assert by_full["sap.orders"].file_source is not None
    assert by_full["sap.orders"].file_source.file_count == 2
    assert by_full["sap.orders"].row_count == 2

    # Empty files gracefully materialized with 0 rows instead of crashing
    for empty_name in ("files.empty_extract", "files.empty_events", "files.empty_payload", "files.empty_gz"):
        assert empty_name in by_full
        assert by_full[empty_name].row_count == 0

    # SQLite fallback handles the same empty files cleanly
    mounted = mount_file_lake_engine(str(drop), DiscoveryConfig(), prefer_duckdb=False)
    try:
        assert mounted.exact_row_counts["main.sap_orders"] == 2
        assert mounted.exact_row_counts["main.empty_payload"] == 0
        assert mounted.exact_row_counts["main.empty_extract"] == 0
    finally:
        mounted.engine.dispose()


def test_cli_demo_files_default_path_uses_landing_directory(tmp_path, monkeypatch):
    """`tick demo --domain files` with no path argument must default to demo_files_landing/ (not demo_warehouse.duckdb)."""
    monkeypatch.chdir(tmp_path)
    res = runner.invoke(app, ["demo", "--domain", "files"])
    assert res.exit_code == 0
    assert (tmp_path / "demo_files_landing").is_dir()
    assert (tmp_path / "demo_files_landing" / "work_orders.csv").is_file()
    assert not (tmp_path / "demo_warehouse.duckdb").exists()
