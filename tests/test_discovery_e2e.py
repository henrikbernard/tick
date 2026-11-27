"""End-to-end database discovery tests across SQLite and DuckDB."""

from __future__ import annotations

import pytest

from tick import DiscoveryConfig, discover_database
from tick.demo import create_demo_database


@pytest.mark.parametrize("ext", [".sqlite", ".duckdb"])
def test_full_discovery_sqlite_and_duckdb(tmp_path, ext):
    db_path = create_demo_database(tmp_path / f"warehouse{ext}")
    result = discover_database(str(db_path), config=DiscoveryConfig(profile_mode="full"))

    ov = result.overview
    assert ov.table_count == 7
    assert ov.view_count == 2
    assert ov.total_rows > 1000
    assert ov.pii_column_count >= 5
    assert ov.explicit_fk_count >= 2
    assert ov.inferred_relationship_count >= 2

    tbls = {t.name: t for t in result.tables}
    assert set(tbls.keys()) == {
        "customers",
        "products",
        "orders",
        "order_items",
        "click_events",
        "legacy_imports",
        "empty_staging_queue",
        "v_order_summary",
        "v_customer_ltv",
    }

    # Check explicit PKs & missing PK detection + candidate PK recommendation
    assert tbls["customers"].primary_key == ["id"]
    assert tbls["click_events"].primary_key == []
    assert tbls["click_events"].recommendations is not None
    assert "event_id" in tbls["click_events"].recommendations.primary_key_candidates
    assert tbls["click_events"].recommendations.entity_role == "event_log"
    assert tbls["click_events"].recommendations.ingestion_strategy == "incremental_append"

    # Check customers watermark & ingestion strategy
    cust_rec = tbls["customers"].recommendations
    assert cust_rec is not None
    assert cust_rec.ingestion_strategy == "incremental_merge"
    assert cust_rec.watermark_candidates[0].column == "updated_at"

    # Check profiling details: 100% null column, constant column, numeric quantiles
    ce_cols = {c.name: c for c in tbls["click_events"].columns}
    assert ce_cols["legacy_experiment_tag"].profile is not None
    assert ce_cols["legacy_experiment_tag"].profile.cardinality_class == "all_null"
    assert ce_cols["legacy_experiment_tag"].profile.null_rate == 1.0

    ord_cols = {c.name: c for c in tbls["orders"].columns}
    assert ord_cols["currency"].profile is not None
    assert ord_cols["currency"].profile.cardinality_class == "constant"
    assert ord_cols["total_amount"].profile is not None
    assert ord_cols["total_amount"].profile.p50 is not None
    assert ord_cols["total_amount"].profile.mean is not None

    # Check freshness / stale data detection on legacy_imports
    leg_cols = {c.name: c for c in tbls["legacy_imports"].columns}
    assert leg_cols["loaded_at"].profile is not None
    assert leg_cols["loaded_at"].profile.is_stale is True
    assert any(i.category == "stale_data" for i in tbls["legacy_imports"].quality_issues)

    # Check empty table detection
    assert tbls["empty_staging_queue"].row_count == 0
    assert any(i.category == "empty_table" for i in tbls["empty_staging_queue"].quality_issues)

    # Check PII detection on customers & click_events
    cust_cols = {c.name: c for c in tbls["customers"].columns}
    assert any(f.category == "email" for f in cust_cols["email"].profile.pii_findings)
    assert any(f.category == "ssn" for f in cust_cols["ssn"].profile.pii_findings)
    assert any(f.category == "phone" for f in cust_cols["phone"].profile.pii_findings)
    assert any(f.category == "ip_address" for f in ce_cols["ip_address"].profile.pii_findings)

    # Check inferred relationships with value overlap verification
    inferred_pairs = {
        (r.source_table.split(".")[-1], r.source_column, r.target_table.split(".")[-1], r.target_column): r
        for r in result.relationships
        if r.kind != "explicit_fk"
    }
    assert ("order_items", "product_id", "products", "id") in inferred_pairs
    assert inferred_pairs[("order_items", "product_id", "products", "id")].overlap_rate == 1.0
    assert ("click_events", "customer_id", "customers", "id") in inferred_pairs
    assert inferred_pairs[("click_events", "customer_id", "customers", "id")].overlap_rate == 1.0

    # Check multi-hop view SQL lineage
    v_sum_up = {u.split(".")[-1] for u in tbls["v_order_summary"].upstream_tables}
    assert v_sum_up == {"orders", "customers", "order_items"}
    v_ltv_up = {u.split(".")[-1] for u in tbls["v_customer_ltv"].upstream_tables}
    assert v_ltv_up == {"customers", "v_order_summary"}


def test_pii_redaction_and_filtering(tmp_path):
    db_path = create_demo_database(tmp_path / "redact.sqlite")
    result = discover_database(
        str(db_path),
        config=DiscoveryConfig(
            profile_mode="fast",
            redact_pii=True,
            include_tables=["customers", "orders"],
            include_views=False,
        ),
    )
    assert {t.name for t in result.tables} == {"customers", "orders"}
    cust = next(t for t in result.tables if t.name == "customers")
    ssn_col = next(c for c in cust.columns if c.name == "ssn")
    email_col = next(c for c in cust.columns if c.name == "email")

    assert ssn_col.profile is not None
    assert all(v == "[REDACTED:ssn]" for v in ssn_col.profile.sample_values)
    assert email_col.profile is not None
    assert all("***@" in v for v in email_col.profile.sample_values)


def test_metadata_only_mode(tmp_path):
    db_path = create_demo_database(tmp_path / "meta.duckdb")
    result = discover_database(str(db_path), config=DiscoveryConfig(profile_mode="none"))
    assert result.overview.table_count == 7
    assert result.overview.view_count == 2
    # Column-name PII detection still catches email/ssn even without sampling data
    assert result.overview.pii_column_count >= 4
