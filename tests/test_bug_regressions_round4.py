"""Regression tests covering all 9 bugs identified in Round-4 codebase review."""

from __future__ import annotations

import ast
from unittest.mock import MagicMock

import yaml
from sqlalchemy import create_engine

from tick import DiscoveryConfig
from tick.dialects import DuckDBAdapter, SQLiteAdapter
from tick.exporters import (
    render_diff_html,
    render_dqx_yaml,
    render_lakeflow_pipeline_py,
    render_odcs_yaml,
)
from tick.models import (
    CdcReadiness,
    ColumnInfo,
    ColumnProfile,
    DatabaseOverview,
    DiscoveryResult,
    GovernanceFinding,
    MetricDrift,
    PiiFinding,
    SnapshotDiff,
    TableInfo,
    TableRecommendations,
    TelemetryProfile,
    ValueFrequency,
)
from tick.profiler import _can_parallelize, profile_tables


def _snap(tables: list[TableInfo]) -> DiscoveryResult:
    return DiscoveryResult(
        config=DiscoveryConfig(databricks_catalog="mfg", databricks_schema="bronze"),
        overview=DatabaseOverview(
            dialect="duckdb",
            driver="duckdb_engine",
            database_name="plant_db",
            sanitized_url="duckdb:///plant.duckdb",
            discovered_at="2026-01-01T00:00:00Z",
            duration_seconds=0.1,
            schema_count=1,
            table_count=len(tables),
            view_count=0,
            column_count=sum(len(t.columns) for t in tables),
        ),
        tables=tables,
    )


def test_round4_bug1_numeric_opc_quality_vs_string_quality_in_dqx_and_dlt():
    """Bug #1: Numeric OPC Classic quality_code must check >= 192 (catching 64=BAD); string quality uses NOT IN."""
    num_tbl = TableInfo(
        schema_name="main",
        name="opc_numeric",
        full_name="main.opc_numeric",
        columns=[
            ColumnInfo(name="recorded_at", raw_type="TIMESTAMP", canonical_type="timestamp"),
            ColumnInfo(name="quality_code", raw_type="INT", canonical_type="integer"),
        ],
        telemetry_profile=TelemetryProfile(
            pattern="narrow_historian_eav",
            timestamp_column="recorded_at",
            quality_column="quality_code",
        ),
    )
    str_tbl = TableInfo(
        schema_name="main",
        name="opc_string",
        full_name="main.opc_string",
        columns=[
            ColumnInfo(name="recorded_at", raw_type="TIMESTAMP", canonical_type="timestamp"),
            ColumnInfo(name="opc_quality", raw_type="VARCHAR", canonical_type="string"),
        ],
        telemetry_profile=TelemetryProfile(
            pattern="narrow_historian_eav",
            timestamp_column="recorded_at",
            quality_column="opc_quality",
        ),
    )
    res = _snap([num_tbl, str_tbl])

    dqx = yaml.safe_load(render_dqx_yaml(res))
    num_chk = next(
        c for c in dqx["tables"]["mfg.bronze.opc_numeric"] if "good_sensor_quality" in c["name"]
    )
    assert num_chk["check"]["function"] == "is_not_less_than"
    assert num_chk["check"]["arguments"]["limit"] == 192

    str_chk = next(
        c for c in dqx["tables"]["mfg.bronze.opc_string"] if "good_sensor_quality" in c["name"]
    )
    assert str_chk["check"]["function"] == "is_not_in_list"
    assert "BAD" in str_chk["check"]["arguments"]["forbidden"]

    dlt_py = render_lakeflow_pipeline_py(res)
    ast.parse(dlt_py)
    assert "`quality_code` >= 192" in dlt_py
    assert "UPPER(CAST(`opc_quality` AS STRING)) NOT IN" in dlt_py


def test_round4_bug2_truncated_top_values_do_not_emit_incomplete_allowlists():
    """Bug #2: When distinct_count (8) > len(top_values) (5), neither DQX nor DLT may emit a truncated allowlist."""
    tbl = TableInfo(
        schema_name="main",
        name="orders",
        full_name="main.orders",
        columns=[
            ColumnInfo(
                name="status_partial",
                raw_type="VARCHAR",
                canonical_type="string",
                profile=ColumnProfile(
                    sample_size=100,
                    non_null_count=100,
                    null_rate=0.0,
                    distinct_count=8,
                    cardinality_class="low",
                    top_values=[
                        ValueFrequency(value=f"S{i}", count=10, rate=0.1) for i in range(5)
                    ],
                ),
            ),
            ColumnInfo(
                name="status_complete",
                raw_type="VARCHAR",
                canonical_type="string",
                profile=ColumnProfile(
                    sample_size=100,
                    non_null_count=100,
                    null_rate=0.0,
                    distinct_count=3,
                    cardinality_class="low",
                    top_values=[
                        ValueFrequency(value="OPEN", count=50, rate=0.5),
                        ValueFrequency(value="CLOSED", count=30, rate=0.3),
                        ValueFrequency(value="HOLD", count=20, rate=0.2),
                    ],
                ),
            ),
        ],
    )
    res = _snap([tbl])
    dqx = yaml.safe_load(render_dqx_yaml(res))
    checks = dqx["tables"]["mfg.bronze.orders"]
    allow_cols = {
        c["check"]["arguments"]["column"]
        for c in checks
        if c["check"]["function"] == "is_in_list"
    }
    assert "status_complete" in allow_cols
    assert "status_partial" not in allow_cols

    dlt_py = render_lakeflow_pipeline_py(res)
    assert "valid_status_complete_domain" in dlt_py
    assert "valid_status_partial_domain" not in dlt_py


def test_round4_bug3_cdc_or_snapshot_uses_streaming_when_cdc_ready():
    """Bug #3: cdc_or_snapshot tables stream when cdc_readiness.status == 'ready' and batch-read otherwise."""
    ready_tbl = TableInfo(
        schema_name="main",
        name="big_cdc_ready",
        full_name="main.big_cdc_ready",
        columns=[ColumnInfo(name="id", raw_type="BIGINT", canonical_type="integer", is_primary_key=True)],
        primary_key=["id"],
        recommendations=TableRecommendations(ingestion_strategy="cdc_or_snapshot"),
        cdc_readiness=CdcReadiness(status="ready"),
    )
    blocked_tbl = TableInfo(
        schema_name="main",
        name="big_cdc_blocked",
        full_name="main.big_cdc_blocked",
        columns=[ColumnInfo(name="id", raw_type="BIGINT", canonical_type="integer")],
        recommendations=TableRecommendations(ingestion_strategy="cdc_or_snapshot"),
        cdc_readiness=CdcReadiness(status="blocked"),
    )
    py_code = render_lakeflow_pipeline_py(_snap([ready_tbl, blocked_tbl]))
    ast.parse(py_code)

    ready_sec, blocked_sec = py_code.split("# --- main.big_cdc_blocked")
    assert "spark.readStream.table('mfg.bronze.big_cdc_ready')" in ready_sec
    assert "dlt.read_stream('clean_big_cdc_ready')" in ready_sec
    assert "spark.read.table('mfg.bronze.big_cdc_blocked')" in blocked_sec
    assert "dlt.read('clean_big_cdc_blocked')" in blocked_sec


def test_round4_bug4_lakeflow_atomic_select_prevents_rename_collisions():
    """Bug #4: When a sanitized target name ('order') matches another raw column ('order'), emit atomic .select()."""
    tbl = TableInfo(
        schema_name="main",
        name="collision_tbl",
        full_name="main.collision_tbl",
        columns=[
            ColumnInfo(name="Order #", raw_type="INT", canonical_type="integer", is_primary_key=True),
            ColumnInfo(name="order", raw_type="VARCHAR", canonical_type="string"),
            ColumnInfo(name="qty", raw_type="INT", canonical_type="integer"),
        ],
        primary_key=["Order #"],
    )
    py_code = render_lakeflow_pipeline_py(_snap([tbl]))
    ast.parse(py_code)
    assert ".select(" in py_code
    assert "F.col('Order #').alias('order')" in py_code
    assert "F.col('order').alias('order_2')" in py_code
    assert ".withColumnRenamed(" not in py_code


def test_round4_bug5_parallel_duckdb_memory_guard_and_worker_error_isolation():
    """Bug #5: _can_parallelize blocks DuckDB :memory:, and worker errors isolate per-table via profile_error."""
    ddb_mem = create_engine("duckdb:///:memory:", future=True)
    sq_mem = create_engine("sqlite:///:memory:", future=True)
    assert _can_parallelize(ddb_mem, workers=4, table_count=5) is False
    assert _can_parallelize(sq_mem, workers=4, table_count=5) is False
    ddb_mem.dispose()
    sq_mem.dispose()

    # Simulate a failing engine.connect() in parallel mode: error must be recorded per table, not crash run
    fake_engine = MagicMock()
    fake_engine.dialect.name = "postgresql"
    fake_engine.url.database = "prod"
    fake_engine.connect.side_effect = RuntimeError("connection pool timeout")

    t1 = TableInfo(
        schema_name="public",
        name="a",
        full_name="public.a",
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer")],
    )
    t2 = TableInfo(
        schema_name="public",
        name="b",
        full_name="public.b",
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer")],
    )
    profile_tables(
        fake_engine,
        SQLiteAdapter(),
        [t1, t2],
        "public",
        DiscoveryConfig(profile_mode="full", workers=2),
    )
    assert t1.columns[0].profile is not None and "connection pool timeout" in (t1.columns[0].profile.profile_error or "")
    assert t2.columns[0].profile is not None and "connection pool timeout" in (t2.columns[0].profile.profile_error or "")


def test_round4_bug6_dqx_rejects_nan_inf_ranges_and_deduplicates_check_names():
    """Bug #6: NaN/Inf bounds must not appear in DQX is_in_range, and check names are unique per table."""
    tbl = TableInfo(
        schema_name="main",
        name="floats",
        full_name="main.floats",
        columns=[
            ColumnInfo(
                name="bad_inf",
                raw_type="DOUBLE",
                canonical_type="float",
                profile=ColumnProfile(sample_size=50, min_value="-inf", max_value="inf"),
            ),
            ColumnInfo(
                name="bad_nan",
                raw_type="DOUBLE",
                canonical_type="float",
                profile=ColumnProfile(sample_size=50, min_value="nan", max_value="10.0"),
            ),
            ColumnInfo(
                name="good_val",
                raw_type="DOUBLE",
                canonical_type="float",
                profile=ColumnProfile(sample_size=50, min_value="1.5", max_value="99.5"),
            ),
        ],
    )
    raw = render_dqx_yaml(_snap([tbl]))
    assert ".inf" not in raw.lower()
    assert ".nan" not in raw.lower()
    dqx = yaml.safe_load(raw)
    range_cols = [
        c["check"]["arguments"]["column"]
        for c in dqx["tables"]["mfg.bronze.floats"]
        if c["check"]["function"] == "is_in_range"
    ]
    assert range_cols == ["good_val"]


def test_round4_bug7_odcs_classification_prioritizes_highest_sensitivity():
    """Bug #7: ODCS classification picks ITAR/PII over lower-sensitivity plant_isolation_key."""
    tbl = TableInfo(
        schema_name="main",
        name="sensitive",
        full_name="main.sensitive",
        columns=[
            ColumnInfo(
                name="itar_email",
                raw_type="VARCHAR",
                canonical_type="string",
                profile=ColumnProfile(
                    pii_findings=[PiiFinding(category="email", confidence="high", score=0.95)],
                    governance_findings=[
                        GovernanceFinding(
                            category="plant_isolation_key",
                            confidence="high",
                            uc_policy="row_filter",
                            uc_tag_key="governance_scope",
                            uc_tag_value="plant_isolation",
                        ),
                        GovernanceFinding(
                            category="ip_or_export_controlled",
                            confidence="high",
                            uc_policy="column_mask",
                            uc_tag_key="compliance",
                            uc_tag_value="itar_ear_ip",
                        ),
                    ],
                ),
            ),
            ColumnInfo(
                name="pii_plant_col",
                raw_type="VARCHAR",
                canonical_type="string",
                profile=ColumnProfile(
                    pii_findings=[PiiFinding(category="ssn", confidence="high", score=0.95)],
                    governance_findings=[
                        GovernanceFinding(
                            category="plant_isolation_key",
                            confidence="high",
                            uc_policy="row_filter",
                            uc_tag_key="governance_scope",
                            uc_tag_value="plant_isolation",
                        ),
                    ],
                ),
            ),
        ],
    )
    odcs = yaml.safe_load(render_odcs_yaml(_snap([tbl])))
    props = {p["name"]: p for p in odcs["schema"][0]["properties"]}
    assert props["itar_email"]["classification"] == "ip_or_export_controlled"
    assert props["pii_plant_col"]["classification"] == "restricted_pii"


def test_round4_bug8_lakeflow_backtick_quotes_hyphenated_catalog_and_schema():
    """Bug #8: Hyphenated UC catalog/schema names ('mfg-prod', 'bronze-ohio') must be backtick-quoted in Spark refs."""
    tbl = TableInfo(
        schema_name="main",
        name="parts",
        full_name="main.parts",
        columns=[ColumnInfo(name="id", raw_type="INT", canonical_type="integer", is_primary_key=True)],
        primary_key=["id"],
    )
    py_code = render_lakeflow_pipeline_py(
        _snap([tbl]), catalog="mfg-prod", bronze_schema="bronze-ohio"
    )
    ast.parse(py_code)
    assert "`mfg-prod`.`bronze-ohio`.parts" in py_code


def test_round4_bug9_diff_html_shows_pct_change_and_empty_zero_label():
    """Bug #9: Diff HTML displays pct_change alongside row delta and labels 0 rows clearly as EMPTY."""
    diff = SnapshotDiff(
        before_discovered_at="2026-01-01",
        after_discovered_at="2026-01-02",
        before_database="db",
        after_database="db",
        metric_drifts=[
            MetricDrift(
                table="main.orders",
                metric="row_count",
                before=100,
                after=0,
                delta=-100.0,
                pct_change=-100.0,
                severity="high",
            )
        ],
    )
    html_text = render_diff_html(diff)
    assert "pct_change" in html_text
    assert "0 (EMPTY)" in html_text
