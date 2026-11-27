"""Tests for bounded parallel profiling (--workers) and Data Contract exporters (DQX & ODCS v3.0.0)."""

from __future__ import annotations

import yaml
from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from tick import DiscoveryConfig, discover_database
from tick.cli import app
from tick.demo import create_manufacturing_demo_database
from tick.exporters import export_contracts_bundle, render_dqx_yaml, render_odcs_yaml

runner = CliRunner()


def test_parallel_profiling_matches_sequential_on_sqlite_and_duckdb(tmp_path):
    for ext in (".sqlite", ".duckdb"):
        db_path = create_manufacturing_demo_database(tmp_path / f"par{ext}", plant_variant="plant_ohio")

        seq = discover_database(
            str(db_path),
            config=DiscoveryConfig(profile_mode="full", workers=1, skip_empty_tables=True),
        )
        par = discover_database(
            str(db_path),
            config=DiscoveryConfig(profile_mode="full", workers=4, skip_empty_tables=True),
        )

        assert len(par.tables) == len(seq.tables)
        assert [t.full_name for t in par.tables] == [t.full_name for t in seq.tables]
        assert par.overview.total_rows == seq.overview.total_rows
        assert par.overview.pii_column_count == seq.overview.pii_column_count
        assert par.overview.governance_column_count == seq.overview.governance_column_count
        assert par.overview.avg_quality_score == seq.overview.avg_quality_score
        assert len(par.relationships) == len(seq.relationships)

        for st, pt in zip(seq.tables, par.tables):
            assert pt.quality_score == st.quality_score
            assert pt.skipped_profiling_reason == st.skipped_profiling_reason
            if st.telemetry_profile:
                assert pt.telemetry_profile is not None
                assert pt.telemetry_profile.median_interval_seconds == st.telemetry_profile.median_interval_seconds
            for sc, pc in zip(st.columns, pt.columns):
                if sc.profile:
                    assert pc.profile is not None
                    assert pc.profile.null_rate == sc.profile.null_rate
                    assert pc.profile.distinct_count == sc.profile.distinct_count
                    assert pc.profile.cardinality_class == sc.profile.cardinality_class


def test_parallel_workers_gracefully_handles_sqlite_memory():
    res = discover_database(":memory:", config=DiscoveryConfig(profile_mode="full", workers=4))
    assert res.overview.dialect == "sqlite"
    assert res.tables == []


def test_dqx_and_odcs_data_contract_exporters_and_cli(tmp_path):
    db_path = create_manufacturing_demo_database(tmp_path / "ohio.duckdb", plant_variant="plant_ohio")

    # Add a low-cardinality PII/secret table to verify DQX does NOT leak sensitive values into allowed lists
    engine = create_engine(f"duckdb:///{db_path}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE secrets_tbl (id INT PRIMARY KEY, contact_email VARCHAR, status VARCHAR)"))
        for i in range(1, 25):
            conn.execute(
                text("INSERT INTO secrets_tbl VALUES (:i, :e, :s)"),
                {
                    "i": i,
                    "e": "ceo@factory.internal" if i % 2 == 0 else "cfo@factory.internal",
                    "s": "ACTIVE" if i % 2 == 0 else "PAUSED",
                },
            )
    engine.dispose()

    out_dir = tmp_path / "bundle"
    r = runner.invoke(
        app,
        [
            "discover",
            str(db_path),
            "--out-dir",
            str(out_dir),
            "--workers",
            "4",
            "--databricks-catalog",
            "mfg_prod",
            "--databricks-schema",
            "bronze_ohio",
            "--quiet",
        ],
    )
    assert r.exit_code == 0

    dqx_file = out_dir / "contracts" / "dqx_checks.yml"
    odcs_file = out_dir / "contracts" / "data_contract.odcs.yml"
    assert dqx_file.exists()
    assert odcs_file.exists()

    # Validate DQX YAML
    dqx = yaml.safe_load(dqx_file.read_text(encoding="utf-8"))
    assert dqx["generated_by"] == "tick"
    assert dqx["target_catalog"] == "mfg_prod"
    assert dqx["target_schema"] == "bronze_ohio"
    assert "mfg_prod.bronze_ohio.secrets_tbl" in dqx["tables"]
    assert "mfg_prod.bronze_ohio.historian_tags" in dqx["tables"]

    secrets_checks = dqx["tables"]["mfg_prod.bronze_ohio.secrets_tbl"]
    fn_by_col = {
        (c["check"]["function"], c["check"]["arguments"].get("column")): c
        for c in secrets_checks
    }
    # Clean status column gets is_in_list
    assert ("is_in_list", "status") in fn_by_col
    assert set(fn_by_col[("is_in_list", "status")]["check"]["arguments"]["allowed"]) == {"ACTIVE", "PAUSED"}
    # PII email column MUST NOT leak email addresses into is_in_list
    assert ("is_in_list", "contact_email") not in fn_by_col
    assert "ceo@factory.internal" not in dqx_file.read_text(encoding="utf-8")

    # Historian table includes OT sensor quality check
    hist_checks = dqx["tables"]["mfg_prod.bronze_ohio.historian_tags"]
    assert any(c["check"]["function"] == "is_not_in_list" for c in hist_checks)

    # Validate ODCS v3.0.0 YAML
    odcs = yaml.safe_load(odcs_file.read_text(encoding="utf-8"))
    assert odcs["apiVersion"] == "v3.0.0"
    assert odcs["kind"] == "DataContract"
    assert odcs["domain"] == "mfg_prod"
    assert len(odcs["servers"]) == 2
    assert odcs["servers"][1]["type"] == "databricks"
    assert odcs["servers"][1]["catalog"] == "mfg_prod"
    assert len(odcs["slaProperties"]) >= 1

    schema_by_name = {s["name"]: s for s in odcs["schema"]}
    assert "work_orders" in schema_by_name
    wo_props = {p["name"]: p for p in schema_by_name["work_orders"]["properties"]}
    assert wo_props["order"]["primaryKey"] is True
    assert wo_props["order"]["primaryKeyPosition"] == 1
    assert wo_props["unit_cost"]["classification"] == "commercial_sensitive"
    assert "databricks.liquidClusteringKeys" in {
        cp["property"] for cp in schema_by_name["work_orders"].get("customProperties", [])
    }

    # Direct --contracts-dir option also works standalone
    standalone_dir = tmp_path / "standalone_contracts"
    res = discover_database(str(db_path), config=DiscoveryConfig(workers=2))
    written = export_contracts_bundle(res, standalone_dir)
    assert len(written) == 2
    assert render_dqx_yaml(res) == (standalone_dir / "dqx_checks.yml").read_text(encoding="utf-8")
    assert render_odcs_yaml(res) == (standalone_dir / "data_contract.odcs.yml").read_text(encoding="utf-8")
