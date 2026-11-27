"""Unit tests for dialect utilities, canonical type normalization, and PII detection."""

from __future__ import annotations

import pytest

from tick.dialects import (
    DuckDBAdapter,
    PostgresAdapter,
    SqlServerAdapter,
    get_dialect_adapter,
    normalize_type,
    resolve_connection_url,
    sanitize_url,
)
from tick.pii import detect_pii, redact_value


def test_resolve_connection_url_shortcuts(tmp_path):
    ddb = tmp_path / "analytics.duckdb"
    sq = tmp_path / "app.sqlite"

    assert resolve_connection_url(str(ddb)) == f"duckdb:///{ddb.resolve()}"
    assert resolve_connection_url(str(sq)) == f"sqlite:///{sq.resolve()}"
    assert resolve_connection_url(":memory:") == "sqlite:///:memory:"
    assert resolve_connection_url("postgres://u:secret@localhost:5432/db").startswith("postgresql")

    with pytest.raises(ValueError):
        resolve_connection_url("not_a_db.txt")


def test_sanitize_url_hides_password():
    masked = sanitize_url("postgresql+psycopg://admin:SuperSecret123@db.internal:5432/prod")
    assert "SuperSecret123" not in masked
    assert "admin" in masked
    assert "db.internal" in masked


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("BIGINT", "integer"),
        ("INT4", "integer"),
        ("SERIAL", "integer"),
        ("NUMERIC(18, 0)", "integer"),
        ("DECIMAL(12, 2)", "decimal"),
        ("MONEY", "decimal"),
        ("DOUBLE PRECISION", "float"),
        ("NVARCHAR(255)", "string"),
        ("CITEXT", "string"),
        ("BOOLEAN", "boolean"),
        ("BIT", "boolean"),
        ("DATE", "date"),
        ("TIMESTAMPTZ", "timestamp"),
        ("DATETIME2", "timestamp"),
        ("TIME WITHOUT TIME ZONE", "time"),
        ("JSONB", "json"),
        ("VARIANT", "json"),
        ("UNIQUEIDENTIFIER", "uuid"),
        ("UUID", "uuid"),
        ("BYTEA", "binary"),
    ],
)
def test_normalize_type_cross_dialect(raw, expected):
    assert normalize_type(raw) == expected


def test_dialect_adapters_sample_queries():
    pg = get_dialect_adapter("postgresql")
    ms = get_dialect_adapter("mssql")
    dk = get_dialect_adapter("duckdb")
    assert isinstance(pg, PostgresAdapter)
    assert isinstance(ms, SqlServerAdapter)
    assert isinstance(dk, DuckDBAdapter)

    assert "LIMIT 100" in pg.build_sample_query('"orders"', '"id"', 100, 1000)
    assert "TOP (100)" in ms.build_sample_query("[orders]", "[id]", 100, 1000)


def test_pii_detection_and_redaction():
    # Pure value detection even on generic column name
    emails = ["alice@acme.io", "bob@example.com", "carol@shop.org"]
    findings = detect_pii("contact_info", "string", emails)
    assert any(f.category == "email" and f.confidence in {"high", "medium"} for f in findings)
    assert redact_value("alice@acme.io", findings) == "a***@acme.io"

    # SSN detection
    ssns = ["123-45-6789", "234-56-7890"]
    ssn_findings = detect_pii("ssn", "string", ssns)
    assert ssn_findings[0].category == "ssn"
    assert ssn_findings[0].confidence == "high"
    assert redact_value("123-45-6789", ssn_findings) == "[REDACTED:ssn]"

    # Credit card Luhn check (valid vs invalid)
    valid_cc = ["4532-0151-1283-0366", "5425-2334-3010-9903"]
    cc_findings = detect_pii("payment_val", "string", valid_cc)
    assert any(f.category == "credit_card" for f in cc_findings)

    invalid_cc = ["1234-5678-9012-3456", "9999-9999-9999-9999"]
    bad_cc_findings = detect_pii("generic_num", "string", invalid_cc)
    assert not any(f.category == "credit_card" for f in bad_cc_findings)

    # Non-PII column should produce zero findings
    clean = detect_pii("order_status", "string", ["shipped", "processing", "cancelled"])
    assert clean == []
