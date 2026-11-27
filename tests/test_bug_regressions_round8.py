"""Round-8 comprehensive bug regression tests across core discovery, file lake, dialects, and cloud exporters."""

from __future__ import annotations

import ast
import gzip
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import duckdb
import pytest
import sqlglot
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import StaticPool
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database, harmonize_snapshots
from tick.catalog import _discover_schemas
from tick.cdc import audit_cdc_readiness
from tick.cli import app
from tick.dialects import (
    DuckDBAdapter,
    PostgresAdapter,
    SqlServerAdapter,
    TableCdcProbe,
    normalize_type,
    resolve_connection_url,
    sanitize_url,
)
from tick.diff import _is_breaking_type_change
from tick.exporters import (
    export_dbt_project,
    render_databricks_autoloader_sql,
    render_dbt_sources_yaml,
    render_diff_html,
    render_html_report,
    render_lakeflow_pipeline_py,
    render_lakeview_dashboard_json,
    render_markdown_report,
    render_odcs_yaml,
    render_snowflake_ddl,
    render_snowflake_dmf_sql,
    render_snowflake_dynamic_tables_sql,
    render_snowflake_file_ingest_sql,
    render_snowflake_gold_hierarchies_sql,
    render_snowflake_project_yml,
    render_snowflake_streamlit_app,
    render_unity_catalog_ddl,
)
from tick.exporters.harmonize_html_export import render_harmonization_html
from tick.file_lake import is_file_lake_target, mount_file_lake_engine, scan_file_sources
from tick.glossary import load_custom_glossary
from tick.harmonize import render_harmonization_sql
from tick.manufacturing import analyze_manufacturing_patterns
from tick.models import (
    CdcDatabaseAudit,
    ColumnInfo,
    ColumnProfile,
    DatabaseOverview,
    DiscoveryResult,
    FileSourceInfo,
    ForeignKeyInfo,
    HierarchyInfo,
    LegacyDateEncoding,
    RelationshipInfo,
    SnapshotDiff,
    TableInfo,
    TableRecommendations,
    TelemetryProfile,
    WatermarkCandidate,
)
from tick.pii import detect_pii
from tick.profiler import ProfileArtifacts, _can_parallelize, _to_datetime_utc
from tick.recommendations import analyze_tables_and_quality
from tick.relationships import _extract_tables_from_sql, _match_column_to_target, discover_relationships

runner = CliRunner()


def _snap(tables: list[TableInfo], **cfg_kw) -> DiscoveryResult:
    cfg = DiscoveryConfig(**cfg_kw)
    return DiscoveryResult(
        config=cfg,
        overview=DatabaseOverview(
            dialect=cfg_kw.pop("_dialect", "postgresql") if "_dialect" in cfg_kw else "postgresql",
            driver="psycopg",
            database_name="test_db",
            sanitized_url="postgresql://host/test_db",
            discovered_at="2026-11-27T00:00:00Z",
            duration_seconds=0.1,
            schema_count=len({t.schema_name for t in tables}),
            table_count=sum(1 for t in tables if t.table_type == "table"),
            view_count=sum(1 for t in tables if t.table_type != "table"),
            column_count=sum(len(t.columns) for t in tables),
        ),
        tables=tables,
    )


def test_file_lake_dirty_csv_sharding_reserved_schemas_bracket_paths_and_fallback(tmp_path):
    """Covers Round-8 file_lake fixes: DuckDB rollback retry on dirty CSV, shard false-positive/Spark UUID shards,
    reserved schema subdir (`temp`), literal paths with `[` `]`, mixed gzip shards, leading 0-byte shard,
    SQLite leading blank line & >int64 JSON ints, Delta .compacted.json, and subdir Hive + sibling file."""
    drop = tmp_path / "lake_drop"
    drop.mkdir()

    # 1. Dirty CSV row (extra columns) -> DuckDB rollback + retry with ignore_errors=true succeeds
    (drop / "dirty.csv").write_text("id,val\n1,10\n2,bad,extra,col\n3,30\n", encoding="utf-8")

    # 2. Unrelated tables ending in _2026 must NOT merge into 1 sharded table!
    sep_dir = drop / "annual"
    sep_dir.mkdir()
    (sep_dir / "customers_2026.csv").write_text("cid,name\n1,Alice\n", encoding="utf-8")
    (sep_dir / "orders_2026.csv").write_text("oid,amt\n10,99.5\n", encoding="utf-8")

    # 3. Spark UUID shards MUST group into 1 sharded table!
    spark_dir = drop / "spark_events"
    spark_dir.mkdir()
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT 1 AS id, 'a' AS v) TO '{spark_dir / 'part-00000-a1b2c3d4-e5f6-7890-abcd-ef1234567890-c000.snappy.parquet'}' (FORMAT PARQUET)"
        )
        con.execute(
            f"COPY (SELECT 2 AS id, 'b' AS v) TO '{spark_dir / 'part-00001-f9e8d7c6-b5a4-3210-fedc-ba9876543210-c000.snappy.parquet'}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    # 4. Reserved DuckDB schema subdir `temp/` -> safely mapped to `s_temp` without BinderException
    temp_dir = drop / "temp"
    temp_dir.mkdir()
    (temp_dir / "staging.csv").write_text("k,v\n1,ok\n", encoding="utf-8")

    # 5. Subdir containing BOTH a direct file AND a Hive partition subdir -> do NOT merge into one Hive table
    mixed_hive = drop / "sales_mix"
    (mixed_hive / "region=US").mkdir(parents=True)
    (mixed_hive / "customers.csv").write_text("cid,name\n1,Acme\n", encoding="utf-8")
    (mixed_hive / "region=US" / "orders.csv").write_text("oid,amt\n1,5.0\n", encoding="utf-8")

    # 6. Leading 0-byte shard + populated semicolon shard
    shard_empty_first = drop / "semi_shards"
    shard_empty_first.mkdir()
    (shard_empty_first / "part-0000.csv").write_text("", encoding="utf-8")
    (shard_empty_first / "part-0001.csv").write_text("a;b\n1;hello\n2;world\n", encoding="utf-8")

    res = discover_database(str(drop))
    by_full = {t.full_name: t for t in res.tables}
    assert "files.dirty" in by_full
    assert by_full["files.dirty"].row_count >= 2
    assert "annual.customers_2026" in by_full
    assert "annual.orders_2026" in by_full
    assert "files.spark_events" in by_full
    assert by_full["files.spark_events"].file_source.file_count == 2
    assert by_full["files.spark_events"].row_count == 2
    assert "s_temp.staging" in by_full
    assert "sales_mix.customers" in by_full
    assert "files.semi_shards" in by_full
    assert [c.name for c in by_full["files.semi_shards"].columns] == ["a", "b"]
    assert by_full["files.semi_shards"].file_source.delimiter == ";"

    # 7. Literal paths containing `[` and `]`
    bracket_dir = tmp_path / "export[2026]"
    bracket_dir.mkdir()
    bracket_db = bracket_dir / "warehouse.sqlite"
    eng = create_engine(f"sqlite:///{bracket_db}", future=True)
    with eng.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (id INT PRIMARY KEY)")
    eng.dispose()
    assert is_file_lake_target(str(bracket_db)) is False
    bracket_csv = bracket_dir / "orders[final].csv"
    bracket_csv.write_text("id,qty\n1,5\n", encoding="utf-8")
    assert is_file_lake_target(str(bracket_csv)) is True
    b_res = discover_database(str(bracket_csv))
    assert b_res.overview.table_count == 1
    assert b_res.tables[0].row_count == 1

    # 8. Mixed .csv + .csv.gz shards, leading blank line, headerless empty cell, and >int64 JSON in SQLite fallback
    fb_dir = tmp_path / "fb_edge"
    mix_s = fb_dir / "mix_comp"
    mix_s.mkdir(parents=True)
    with gzip.open(mix_s / "part-0000.csv.gz", "wt", encoding="utf-8") as fh:
        fh.write("id,name\n1,gz_row\n")
    (mix_s / "part-0001.csv").write_text("\nid,name\n2,plain_row\n", encoding="utf-8")
    (fb_dir / "bigint_overflow.json").write_text(
        json.dumps([{"big_id": 99999999999999999999, "label": "huge"}]), encoding="utf-8"
    )
    (fb_dir / "nohdr_blank.csv").write_text("1,,10.5\n2,ok,20.5\n", encoding="utf-8")

    mounted = mount_file_lake_engine(str(fb_dir), DiscoveryConfig(), prefer_duckdb=False)
    try:
        assert mounted.file_sources["main.mix_comp"].compression is None
        assert mounted.exact_row_counts["main.mix_comp"] == 2
        assert mounted.exact_row_counts["main.bigint_overflow"] == 1
        with mounted.engine.connect() as conn:
            row0 = conn.exec_driver_sql("SELECT column1 FROM nohdr_blank WHERE column0 = '1'").scalar()
            assert row0 is None
    finally:
        mounted.engine.dispose()

    # 9. Delta .compacted.json must NOT be replayed before commit 0 or resurrect removed files
    d_dir = tmp_path / "delta_compact"
    (d_dir / "_delta_log").mkdir(parents=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"COPY (SELECT 1 AS id) TO '{d_dir / 'part-0000.parquet'}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT 2 AS id) TO '{d_dir / 'part-0001.parquet'}' (FORMAT PARQUET)")
    finally:
        con.close()
    (d_dir / "_delta_log" / "00000000000000000000.00000000000000000001.compacted.json").write_text(
        json.dumps({"add": {"path": "part-0000.parquet"}}) + "\n", encoding="utf-8"
    )
    (d_dir / "_delta_log" / "00000000000000000000.json").write_text(
        json.dumps({"add": {"path": "./part-0000.parquet"}}) + "\n", encoding="utf-8"
    )
    (d_dir / "_delta_log" / "00000000000000000001.json").write_text(
        json.dumps({"remove": {"path": "part-0000.parquet"}})
        + "\n"
        + json.dumps({"add": {"path": "part-0001.parquet"}})
        + "\n",
        encoding="utf-8",
    )
    _, _, d_sets = scan_file_sources(str(d_dir))
    assert len(d_sets) == 1
    assert [p.name for p in d_sets[0].source_paths] == ["part-0001.parquet"]


def test_dialects_sanitize_url_resolve_paths_and_normalize_type(tmp_path):
    # 1. Repeated odbc_connect, brace-escaped PWD={pa;ss}, ClientSecret, BigQuery credentials_base64, passphrase
    u1 = "mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 18};UID=sa;PWD={pa;ss};ClientSecret=csec;Database=mfg&odbc_connect=PWD=secret2"
    s1 = sanitize_url(u1)
    assert "pa;ss" not in s1 and "ss}" not in s1 and "csec" not in s1 and "secret2" not in s1

    u2 = "bigquery://proj/ds?credentials_base64=SECRET_B64&passphrase=my_pass"
    s2 = sanitize_url(u2)
    assert "SECRET_B64" not in s2 and "my_pass" not in s2

    # 2. Local paths with ? and %20 round-trip via URL.create
    q_db = tmp_path / "a?b=1.db"
    p_db = tmp_path / "a%20b.duckdb"
    assert make_url(resolve_connection_url(str(q_db))).database == str(q_db.resolve())
    assert make_url(resolve_connection_url(str(p_db))).database == str(p_db.resolve())

    # 3. Composite/array/enum type normalization
    assert normalize_type("UUID[]") == "json"
    assert normalize_type("ARRAY<UUID>") == "json"
    assert normalize_type("STRUCT<user_uuid UUID, name VARCHAR>") == "json"
    assert normalize_type("ENUM('TIMESTAMP', 'DATE')") == "string"
    assert normalize_type("UUID") == "uuid"

    # 4. SqlServerAdapter.get_table_stats uses OUTER APPLY (no allocation_units row-count fanout)
    captured_sql: list[str] = []
    fake_conn = MagicMock()
    fake_conn.execute.side_effect = lambda q, params=None: (captured_sql.append(str(q)), MagicMock(mappings=lambda: []))[1]
    SqlServerAdapter().get_table_stats(fake_conn, "dbo")
    assert captured_sql and "OUTER APPLY" in captured_sql[0]


def test_catalog_profiler_quoted_duckdb_fks_colon_idents_overflows_and_redact(tmp_path):
    db = tmp_path / "special.duckdb"
    eng = create_engine(f"duckdb:///{db}", future=True)
    with eng.begin() as conn:
        conn.exec_driver_sql('CREATE SCHEMA "my-schema"')
        conn.exec_driver_sql(
            'CREATE TABLE "my-schema"."Order Parents" ("Parent ID" INT PRIMARY KEY, "a,b" INT UNIQUE)'
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE "my-schema"."Order :Children" (
                "Child ID" INT PRIMARY KEY,
                "Parent ID" INT,
                "Temp (C):max" DOUBLE,
                "date_of_birth" DATE,
                FOREIGN KEY ("Parent ID") REFERENCES "my-schema"."Order Parents"("Parent ID")
            )
            """
        )
        conn.exec_driver_sql('INSERT INTO "my-schema"."Order Parents" VALUES (1, 10)')
        conn.exec_driver_sql(
            'INSERT INTO "my-schema"."Order :Children" VALUES (100, 1, 1e308, DATE \'1990-05-14\'), (101, 1, 1e308, DATE \'1995-03-01\')'
        )
    eng.dispose()

    res = discover_database(str(db), config=DiscoveryConfig(redact_pii=True))
    by_full = {t.full_name: t for t in res.tables}
    child = by_full["my-schema.Order :Children"]
    # Quoted DuckDB FK with space/hyphen discovered cleanly!
    assert len(child.foreign_keys) == 1
    assert child.foreign_keys[0].referred_table == "Order Parents"
    assert child.foreign_keys[0].referred_columns == ["Parent ID"]

    # Column with `:max` and extreme floats [1e308, 1e308] profiled cleanly with no profile_error or inf!
    cols = {c.name: c for c in child.columns}
    assert cols["Temp (C):max"].profile and cols["Temp (C):max"].profile.profile_error is None
    # --redact-pii also redacts freshness_max_timestamp on date_of_birth!
    dob_prof = cols["date_of_birth"].profile
    assert dob_prof and dob_prof.pii_findings
    assert dob_prof.freshness_max_timestamp and "1995" not in dob_prof.freshness_max_timestamp

    # --exclude-schema excludes default schema instead of re-adding it
    eng2 = create_engine("sqlite:///:memory:", future=True)
    with eng2.connect() as conn:
        from sqlalchemy import inspect as sa_inspect
        from tick.dialects import SQLiteAdapter

        schemas, _ = _discover_schemas(
            conn, sa_inspect(conn), SQLiteAdapter(), DiscoveryConfig(exclude_schemas=["main"])
        )
        assert schemas == []
    eng2.dispose()

    # SCD2 sentinel TIMESTAMPTZ clamps safely instead of OverflowError
    clamped = _to_datetime_utc(datetime(9999, 12, 31, 23, 0, tzinfo=timezone(timedelta(hours=-5))))
    assert clamped is not None and clamped.year == 9999

    # _can_parallelize blocks StaticPool & URI :memory:
    sp_eng = create_engine("sqlite://", future=True, poolclass=StaticPool)
    assert _can_parallelize(sp_eng, workers=4, table_count=4) is False
    sp_eng.dispose()


def test_pii_iban_checksum_temporal_guard_and_cdc_probes():
    # Manufacturing serials/work-orders fail ISO 13616 MOD-97 -> NOT flagged as IBAN
    assert detect_pii("work_order_number", "string", ["WO2026000001234", "WO2026000001235"]) == []
    assert detect_pii("serial_number", "string", ["SN2600000000001", "SN2600000000002"]) == []
    # Valid IBAN IS flagged
    valid_iban = detect_pii("bank_ref", "string", ["DE89370400440532013000", "GB82WEST12345698765432"])
    assert valid_iban and valid_iban[0].category == "iban"

    # Temporal metadata columns are NOT flagged as secret_token or email
    assert detect_pii("password_updated_at", "timestamp", ["2026-11-26T10:00:00+00:00"]) == []
    assert detect_pii("email_sent_at", "timestamp", ["2026-11-26T10:00:00+00:00"]) == []
    assert detect_pii("date_of_birth", "date", ["1990-01-01"])

    # PostgreSQL CDC readiness: source_cdc_enabled, REPLICA IDENTITY NOTHING vs INDEX
    pg_adapter = MagicMock(spec=PostgresAdapter)
    pg_adapter.name = "postgresql"
    pg_adapter.audit_database_cdc.return_value = CdcDatabaseAudit(
        dialect="postgresql", db_cdc_enabled=True, wal_level="logical"
    )
    pg_adapter.audit_table_cdc.return_value = {
        "ri_nothing": TableCdcProbe(replica_identity="nothing"),
        "ri_index": TableCdcProbe(replica_identity="index"),
    }
    t_nothing = TableInfo(
        schema_name="public", name="ri_nothing", full_name="public.ri_nothing", primary_key=["id"],
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
    )
    t_index = TableInfo(
        schema_name="public", name="ri_index", full_name="public.ri_index", primary_key=[],
        columns=[ColumnInfo(name="code", raw_type="VARCHAR", canonical_type="string", is_unique=True)],
    )
    fake_engine = MagicMock()
    audit_cdc_readiness(fake_engine, pg_adapter, [t_nothing, t_index])
    assert t_nothing.cdc_readiness and t_nothing.cdc_readiness.status == "blocked"
    assert t_nothing.cdc_readiness.source_cdc_enabled is True
    assert t_index.cdc_readiness and t_index.cdc_readiness.status == "ready"
    assert t_index.cdc_readiness.source_cdc_enabled is True

    # MSSQL CDC readiness: PK + CDC-only recommends mssql_cdc; keyless + db_cdc_enabled=False is blocked
    ms_adapter = MagicMock(spec=SqlServerAdapter)
    ms_adapter.name = "mssql"
    ms_adapter.audit_database_cdc.return_value = CdcDatabaseAudit(
        dialect="mssql", db_cdc_enabled=True, db_change_tracking_enabled=False
    )
    ms_adapter.audit_table_cdc.return_value = {
        "orders": TableCdcProbe(cdc_enabled=True, change_tracking_enabled=False),
    }
    t_ms = TableInfo(
        schema_name="dbo", name="orders", full_name="dbo.orders", primary_key=["id"],
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
    )
    audit_cdc_readiness(fake_engine, ms_adapter, [t_ms])
    assert t_ms.cdc_readiness and t_ms.cdc_readiness.connector_recommendation == "lakeflow_connect_mssql_cdc"


def test_relationships_recommendations_manufacturing_and_glossary(tmp_path):
    # 1. Deterministic _match_column_to_target (longest stem wins -> 0.88)
    tgt_tbl = TableInfo(schema_name="public", name="order_items", full_name="public.order_items", primary_key=["id"])
    tgt_col = ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)
    m = _match_column_to_target("order_item_id", tgt_tbl, tgt_col)
    assert m is not None and m[0] == 0.88

    # 2. CTE-only view returns empty refs (does not fall through to regex and emit CTE name)
    assert _extract_tables_from_sql("CREATE VIEW v AS WITH vals AS (SELECT 1 AS x) SELECT x FROM vals", "duckdb") == set()

    # 3. Cross-schema explicit FK not hijacked + integer/decimal overlap + same-schema affinity
    t_hr_emp = TableInfo(
        schema_name="hr", name="employees", full_name="hr.employees", primary_key=["id"],
        columns=[
            ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True),
            ColumnInfo(name="acct_id", raw_type="INT", canonical_type="integer"),
        ],
        foreign_keys=[
            ForeignKeyInfo(constrained_columns=["acct_id"], referred_schema="sales", referred_table="accounts", referred_columns=["id"])
        ],
    )
    t_hr_acct = TableInfo(
        schema_name="hr", name="accounts", full_name="hr.accounts", primary_key=["id"],
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
    )
    t_cust = TableInfo(
        schema_name="hr", name="customers", full_name="hr.customers", primary_key=["id"], row_count=3,
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
    )
    t_ord = TableInfo(
        schema_name="hr", name="orders", full_name="hr.orders", primary_key=["id"], row_count=3,
        columns=[
            ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True),
            ColumnInfo(name="customer_id", raw_type="DECIMAL(10, 2)", canonical_type="decimal"),
        ],
    )
    rels, _ = discover_relationships(
        [t_hr_emp, t_hr_acct, t_cust, t_ord],
        value_samples={("hr.customers", "id"): {"1", "2", "3"}, ("hr.orders", "customer_id"): {"1.00", "2.00", "3.00"}},
        config=DiscoveryConfig(),
        value_sample_complete={("hr.customers", "id"): True, ("hr.orders", "customer_id"): True},
    )
    exp_fk = [r for r in rels if r.kind == "explicit_fk"]
    assert exp_fk[0].target_table == "sales.accounts"  # NOT hijacked to hr.accounts!
    inf_rel = [r for r in rels if r.source_table == "hr.orders" and r.target_table == "hr.customers"]
    assert inf_rel and inf_rel[0].overlap_rate == 1.0

    # 4. Recommendations: boolean not PK candidate, row_count=None not reference, 1 non-null not constant_column
    t_rec = TableInfo(
        schema_name="public", name="order_statuses", full_name="public.order_statuses", row_count=None,
        columns=[
            ColumnInfo(
                name="is_active", raw_type="BOOLEAN", canonical_type="boolean",
                profile=ColumnProfile(sample_size=2, non_null_count=2, null_count=0, cardinality_class="unique"),
            ),
            ColumnInfo(
                name="sparse_note", raw_type="VARCHAR", canonical_type="string",
                profile=ColumnProfile(sample_size=500, non_null_count=1, null_count=499, cardinality_class="constant"),
            ),
        ],
    )
    issues = analyze_tables_and_quality([t_rec], [])
    assert t_rec.recommendations is not None
    assert "is_active" not in t_rec.recommendations.primary_key_candidates
    assert t_rec.recommendations.entity_role != "reference"
    assert not any(i.category == "constant_column" for i in issues)

    # 5. Manufacturing: wide PLC with sensor_timestamp & sensor_value must NOT emit duplicate cluster keys; CPUDT+CPUTM paired
    t_plc = TableInfo(
        schema_name="ot", name="plc_wide", full_name="ot.plc_wide", row_count=100,
        columns=[
            ColumnInfo(name="sensor_timestamp", raw_type="TIMESTAMP", canonical_type="timestamp"),
            ColumnInfo(name="sensor_value", raw_type="DOUBLE", canonical_type="float"),
            ColumnInfo(name="sensor_temp", raw_type="DOUBLE", canonical_type="float"),
            ColumnInfo(name="sensor_pressure", raw_type="DOUBLE", canonical_type="float"),
            ColumnInfo(name="sensor_rpm", raw_type="DOUBLE", canonical_type="float"),
        ],
    )
    t_sap = TableInfo(
        schema_name="sap", name="bkpf", full_name="sap.bkpf", row_count=10,
        columns=[
            ColumnInfo(name="CPUDT", raw_type="DECIMAL(8,2)", canonical_type="decimal", profile=ColumnProfile(sample_size=5, non_null_count=5)),
            ColumnInfo(name="CPUTM", raw_type="DECIMAL(6,2)", canonical_type="decimal", profile=ColumnProfile(sample_size=5, non_null_count=5)),
        ],
    )
    art = ProfileArtifacts(
        raw_str_samples={
            ("sap.bkpf", "CPUDT"): ["20260101.00", "20260102.00", "20260103.00", "0.00"],
            ("sap.bkpf", "CPUTM"): ["83015.00", "93015.00", "103015.00", "0.00"],
        }
    )
    analyze_manufacturing_patterns([t_plc, t_sap], art)
    assert t_plc.telemetry_profile is not None
    assert t_plc.telemetry_profile.pattern == "wide_plc_polling"
    assert len(set(t_plc.telemetry_profile.recommended_liquid_cluster)) == len(t_plc.telemetry_profile.recommended_liquid_cluster)
    assert t_sap.columns[0].profile and t_sap.columns[0].profile.legacy_date
    assert t_sap.columns[0].profile.legacy_date.paired_column == "CPUTM"
    assert ".cast('long').cast('string')" in t_sap.columns[0].profile.legacy_date.spark_expression

    # 6. Glossary YAML with boolean/numeric keys and null description
    g_file = tmp_path / "glossary.yml"
    g_file.write_text("tables:\n  NO:\n    name: norway_orders\n    description:\ncolumns:\n  100:\n    name: code_100\n    description: null\n", encoding="utf-8")
    t_map, c_map = load_custom_glossary(g_file)
    assert t_map["FALSE"] == ("norway_orders", "") or t_map.get("NO") == ("norway_orders", "")
    assert c_map["100"] == ("code_100", "")


def test_exporters_snowflake_databricks_dbt_odcs_harmonize_diff_html_and_cli(tmp_path):
    # 1. Snowflake DDL CLUSTER BY before CHANGE_TRACKING, Julian DATE_FROM_PARTS, DECIMAL TRUNC, NTZ FRESHNESS fallback, FLOAT OPC DMF, Gold WITH RECURSIVE cols
    t_ot = TableInfo(
        schema_name="main", name="plc", full_name="main.plc", row_count=500,
        columns=[
            ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True, nullable=False),
            ColumnInfo(name="recorded_at", raw_type="TIMESTAMP", canonical_type="timestamp", snowflake_type="TIMESTAMP_NTZ"),
            ColumnInfo(name="quality_code", raw_type="DOUBLE", canonical_type="float", snowflake_type="FLOAT"),
            ColumnInfo(
                name="jde_date", raw_type="DECIMAL(6,0)", canonical_type="decimal",
                profile=ColumnProfile(
                    legacy_date=LegacyDateEncoding(
                        encoding="julian_cyyddd", confidence="high", sample_match_rate=1.0, spark_expression="x"
                    )
                ),
            ),
            ColumnInfo(name="parent_part_id", raw_type="VARCHAR", canonical_type="string"),
            ColumnInfo(name="child_part_id", raw_type="VARCHAR", canonical_type="string"),
            ColumnInfo(name="plant_id", raw_type="VARCHAR", canonical_type="string"),
        ],
        primary_key=["id"],
        recommendations=TableRecommendations(
            watermark_candidates=[WatermarkCandidate(column="recorded_at", kind="event_timestamp", confidence="high", reason="ts")]
        ),
        telemetry_profile=TelemetryProfile(
            pattern="narrow_historian_eav", timestamp_column="recorded_at", quality_column="quality_code",
            recommended_liquid_cluster=["recorded_at"],
        ),
        hierarchy=HierarchyInfo(pattern="bom", parent_column="parent_part_id", child_column="child_part_id", reason="bom"),
        file_source=FileSourceInfo(path="/data/plc", file_format="delta", file_count=1, total_bytes=1024),
    )
    from tick.snowflake import lint_snowflake_compatibility

    lint_snowflake_compatibility([t_ot])
    sf_res = _snap([t_ot], target_platform="snowflake", snowflake_database="DW_SF", snowflake_schema="RAW_SF")

    sf_ddl = render_snowflake_ddl(sf_res)
    assert sf_ddl.index("CLUSTER BY (") < sf_ddl.index("CHANGE_TRACKING = TRUE")

    sf_dt = render_snowflake_dynamic_tables_sql(sf_res)
    assert "DATE_FROM_PARTS(" in sf_dt
    assert "YYYYDDD" not in sf_dt
    assert "TRUNC(" in sf_dt

    sf_dmf = render_snowflake_dmf_sql(sf_res)
    assert 'SNOWFLAKE.CORE.FRESHNESS ON ("_INGESTED_AT")' in sf_dmf
    assert 'BAD_OPC_QUALITY_COUNT_FLOAT' in sf_dmf
    assert 'ARG_T TABLE(ARG_C FLOAT)' in sf_dmf

    sf_fi = render_snowflake_file_ingest_sql(sf_res)
    assert "FORMAT_NAME = '\"DW_SF\".\"RAW_SF\".\"TICK_FMT_PARQUET\"'" in sf_fi
    assert "PATTERN = '^(?!.*_delta_log/).*\\\\.parquet$'" in sf_fi

    sf_gold = render_snowflake_gold_hierarchies_sql(sf_res)
    assert 'WITH RECURSIVE HIERARCHY_EDGES ("PLANT_ID", "ROOT_NODE_ID", "PARENT_NODE_ID", "CHILD_NODE_ID", "DEPTH", "PATH_NODES") AS (' in sf_gold
    assert 'IS NOT DISTINCT FROM' in sf_gold

    # snowflake.yml targets SILVER when no hierarchies exist
    t_plain = TableInfo(
        schema_name="main", name="items", full_name="main.items",
        comment="Item's desc \\ line1\nline2",
        columns=[
            ColumnInfo(name="Order #", raw_type="INT", canonical_type="integer", is_primary_key=True, comment="O'Brien \\ ref"),
            ColumnInfo(name="ORDER", raw_type="VARCHAR", canonical_type="string"),
            ColumnInfo(name="Part.No", raw_type="VARCHAR", canonical_type="string"),
            ColumnInfo(name="part_no", raw_type="VARCHAR", canonical_type="string"),
        ],
        primary_key=["Order #"],
    )
    plain_sf_res = _snap([t_plain], target_platform="snowflake", snowflake_database="PROD_DB", snowflake_schema="RAW")
    sf_yml = yaml.safe_load(render_snowflake_project_yml(plain_sf_res))
    ent = next(iter(sf_yml["entities"].values()))
    assert ent["identifier"]["schema"] == "SILVER"

    # ODCS for target_platform="snowflake" uses UPPERCASE names & honors catalog/schema overrides
    odcs = yaml.safe_load(render_odcs_yaml(plain_sf_res, catalog="OVR_DB", schema="OVR_SCH"))
    assert odcs["schema"][0]["name"] == "ITEMS"
    assert odcs["schema"][0]["properties"][0]["name"] == "ORDER"
    sf_srv = next(s for s in odcs["servers"] if s["server"] == "snowflake_target")
    assert sf_srv["database"] == "OVR_DB" and sf_srv["schema"] == "OVR_SCH"

    # 2. Databricks DDL escaping (`\'` & `\\`), UC Volume for file sources, Lakeflow case-insensitive collision & dotted col & headerless CSV
    dbx_ddl = render_unity_catalog_ddl(_snap([t_plain, t_ot]))
    assert "O\\'Brien \\\\ ref" in dbx_ddl
    assert "CREATE VOLUME IF NOT EXISTS `manufacturing`.`bronze`.`landing_zone`;" in dbx_ddl
    assert len([s for s in sqlglot.parse(dbx_ddl, read="databricks") if s is not None]) >= 3

    dlt_py = render_lakeflow_pipeline_py(_snap([t_plain]))
    ast.parse(dlt_py)
    assert ".select(" in dlt_py
    assert "F.col('`Part.No`').alias('part_no')" in dlt_py

    t_nohdr = TableInfo(
        schema_name="files", name="raw_csv", full_name="files.raw_csv",
        columns=[ColumnInfo(name="column0", raw_type="INT", canonical_type="integer")],
        file_source=FileSourceInfo(path="/d/raw.csv", file_format="csv", file_count=1, total_bytes=10, has_header=False),
    )
    dlt_nohdr = render_lakeflow_pipeline_py(_snap([t_nohdr]))
    ast.parse(dlt_nohdr)
    assert ".withColumnRenamed('_c0', 'column0')" in dlt_nohdr

    al_sql = render_databricks_autoloader_sql(_snap([t_nohdr]), catalog="cat'a")
    assert "/Volumes/cat\\'a/" in al_sql

    # 3. Lakeview dashboard LOWER(table_schema) + unescaped displayName; Streamlit backslash escape
    lv = json.loads(render_lakeview_dashboard_json(_snap([t_plain]), catalog="mfg'cat", bronze_schema="BRONZE"))
    assert "LOWER(table_schema) IN ('bronze', 'silver')" in lv["datasets"][0]["query"]
    assert lv["pages"][0]["layout"][0]["widget"]["spec"]["encodings"]["value"]["displayName"] == "Tables in mfg'cat"
    sis_py = render_snowflake_streamlit_app(plain_sf_res)
    ast.parse(sis_py)
    assert "replace('\\\\', '\\\\\\\\')" in sis_py

    # 4. dbt colliding schema slugs, dotted schema relationships, Databricks backtick quoting
    t_s1 = TableInfo(schema_name="sales-us", name="Orders Table", full_name="sales-us.Orders Table", columns=[ColumnInfo(name="Order #", raw_type="INT", canonical_type="integer", is_primary_key=True)], primary_key=["Order #"])
    t_s2 = TableInfo(schema_name="sales_us", name="items", full_name="sales_us.items", columns=[ColumnInfo(name="oid", raw_type="INT", canonical_type="integer")])
    dbt_res = _snap([t_s1, t_s2])
    dbt_res.overview.dialect = "databricks"
    dbt_res.relationships = [
        RelationshipInfo(source_table="sales_us.items", source_column="oid", target_table="sales-us.Orders Table", target_column="Order #", kind="explicit_fk", confidence=1.0)
    ]
    dbt_yml = yaml.safe_load(render_dbt_sources_yaml(dbt_res))
    src_names = [s["name"] for s in dbt_yml["sources"]]
    assert len(set(src_names)) == 2  # No duplicate source names!
    dbt_dir = tmp_path / "dbt_out"
    export_dbt_project(dbt_res, dbt_dir)
    stg_sql = (dbt_dir / "staging" / "stg_sales_us__orders_table.sql").read_text(encoding="utf-8")
    assert "`Order #` as order" in stg_sql

    # 5. Harmonize: non-glossary case-insensitive PKs, Snowflake GEOGRAPHY -> ST_ASWKT, CREATE SCHEMA IF NOT EXISTS, plant label escaping
    p1 = _snap([
        TableInfo(
            schema_name="public", name="sites", full_name="public.sites", primary_key=["site_id"],
            columns=[
                ColumnInfo(name="site_id", raw_type="INT", canonical_type="integer", is_primary_key=True),
                ColumnInfo(name="loc", raw_type="GEOGRAPHY", canonical_type="other", snowflake_type="GEOGRAPHY"),
            ],
        )
    ])
    p2 = _snap([
        TableInfo(
            schema_name="dbo", name="SITES", full_name="dbo.SITES", primary_key=["SITE_ID"],
            columns=[
                ColumnInfo(name="SITE_ID", raw_type="INT", canonical_type="integer", is_primary_key=True),
                ColumnInfo(name="LOC", raw_type="VARCHAR", canonical_type="string", snowflake_type="VARCHAR"),
            ],
        )
    ])
    h_sf = harmonize_snapshots({"plant'a\\": p1, "plant_b": p2}, target_platform="snowflake")
    assert h_sf.tables[0].pk_divergent is False
    assert 'ST_ASWKT("loc")' in h_sf.tables[0].unified_sql
    full_h_sql = render_harmonization_sql(h_sf)
    assert 'CREATE SCHEMA IF NOT EXISTS "MANUFACTURING"."SILVER";' in full_h_sql

    # 6. Diff breaking type narrowings
    assert _is_breaking_type_change("VARCHAR2(255 CHAR)", "string", "VARCHAR2(50 CHAR)", "string") is True
    assert _is_breaking_type_change("VARCHAR(MAX)", "string", "VARCHAR(50)", "string") is True
    assert _is_breaking_type_change("UBIGINT", "integer", "BIGINT", "integer") is True
    assert _is_breaking_type_change("BIGINT", "integer", "NUMBER(5, 0)", "integer") is True
    assert _is_breaking_type_change("MONEY", "decimal", "DECIMAL(10, 2)", "decimal") is True
    assert _is_breaking_type_change("TIMESTAMPTZ", "timestamp", "TIMESTAMP", "timestamp") is True
    assert _is_breaking_type_change("BLOB", "binary", "VARBINARY(16)", "binary") is True
    assert _is_breaking_type_change("FLOAT(53)", "float", "FLOAT(24)", "float") is True

    # 7. HTML5 <!--<script> & __PAYLOAD_JSON__ title injection + Markdown backtick/Mermaid safety
    xss_res = _snap([t_plain])
    xss_res.overview.database_name = "db___PAYLOAD_JSON__"
    xss_res.tables[0].comment = "line1\n<!--<script>alert(1)</script>\nline3"
    xss_res.tables[0].view_definition = "SELECT ```1```"
    html_out = render_html_report(xss_res)
    assert "<!--<script>" not in html_out
    assert "<title>tick Discovery — db___PAYLOAD_JSON__</title>" in html_out
    diff_html = render_diff_html(
        SnapshotDiff(
            before_database="a___PAYLOAD_JSON__", after_database="b",
            before_discovered_at="2026-01-01", after_discovered_at="2026-01-02",
        )
    )
    assert "<title>tick Diff — a___PAYLOAD_JSON__ vs b</title>" in diff_html
    h_html = render_harmonization_html(h_sf)
    assert "<!--<script>" not in h_html
    md_out = render_markdown_report(xss_res)
    assert "> line1\n> <!--<script>alert(1)</script>\n> line3" in md_out
    assert "````sql" in md_out

    # 8. CLI negative --sample-size rejected cleanly & corrupt diff snapshot prints clean error
    r_bad_ss = runner.invoke(app, ["discover", ":memory:", "--sample-size", "-1"])
    assert r_bad_ss.exit_code != 0
    bad_json = tmp_path / "broken.json"
    bad_json.write_text("{not valid json", encoding="utf-8")
    r_bad_diff = runner.invoke(app, ["diff", str(bad_json), str(bad_json)])
    assert r_bad_diff.exit_code == 1
    assert "Snapshot diff failed" in r_bad_diff.output
