"""Dialect-portable data profiling, statistical summaries, freshness, PII, and governance scanning."""

from __future__ import annotations

import math
import statistics
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from tick.dialects import DialectAdapter, safe_rollback
from tick.models import ColumnInfo, ColumnProfile, DiscoveryConfig, TableInfo, ValueFrequency
from tick.pii import detect_governance, detect_pii, redact_value


@dataclass
class ProfileArtifacts:
    """In-memory sample caches captured during profiling for downstream analyzers."""

    value_samples: dict[tuple[str, str], set[str]] = field(default_factory=dict)
    raw_str_samples: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    timestamp_samples: dict[tuple[str, str], list[datetime]] = field(default_factory=dict)


def _to_float(val: Any) -> float | None:
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float, Decimal)):
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    try:
        f = float(str(val))
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return None


def _to_datetime_utc(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.astimezone(timezone.utc) if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day, tzinfo=timezone.utc)
    s = str(val).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    for candidate in (s, s.replace(" ", "T")):
        try:
            dt = datetime.fromisoformat(candidate)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _stringify(val: Any, max_len: int = 120) -> str:
    if isinstance(val, (bytes, bytearray, memoryview)):
        b = bytes(val)
        return f"<binary:{len(b)}B>"
    s = str(val)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def _classify_cardinality(sample_size: int, non_null: int, distinct: int | None) -> str:
    if sample_size == 0:
        return "empty"
    if non_null == 0:
        return "all_null"
    if distinct is None:
        return "unknown"
    if distinct == 1:
        return "constant"
    rate = distinct / non_null
    if distinct == non_null:
        return "unique"
    if rate >= 0.9:
        return "high"
    if distinct <= 25 or rate <= 0.05:
        return "low"
    return "medium"


def _quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


def _fetch_full_table_aggregates(
    conn: Connection,
    adapter: DialectAdapter,
    quoted_table: str,
    columns: list[ColumnInfo],
) -> dict[str, dict[str, Any]]:
    """Run a single batched SQL aggregate query for exact null counts and min/max."""
    if not columns:
        return {}

    select_parts: list[str] = ["COUNT(*) AS __total__"]
    for idx, col in enumerate(columns):
        qcol = adapter.quote_identifier(conn, col.name)
        select_parts.append(f"COUNT({qcol}) AS __nn_{idx}__")
        if col.canonical_type not in {"binary", "json", "boolean", "other"}:
            select_parts.append(f"MIN({qcol}) AS __min_{idx}__")
            select_parts.append(f"MAX({qcol}) AS __max_{idx}__")

    sql = f"SELECT {', '.join(select_parts)} FROM {quoted_table}"
    try:
        row = conn.execute(text(sql)).mappings().first()
    except Exception:
        safe_rollback(conn)
        return {}
    if not row:
        return {}

    total = int(row["__total__"] or 0)
    out: dict[str, dict[str, Any]] = {}
    for idx, col in enumerate(columns):
        nn = int(row[f"__nn_{idx}__"] or 0)
        out[col.name] = {
            "total": total,
            "non_null": nn,
            "min": row.get(f"__min_{idx}__"),
            "max": row.get(f"__max_{idx}__"),
        }
    return out


def _apply_metadata_only_scanners(tables: list[TableInfo], config: DiscoveryConfig) -> None:
    for tbl in tables:
        for col in tbl.columns:
            pii = detect_pii(col.name, col.canonical_type, []) if config.detect_pii else []
            gov = detect_governance(col.name, col.canonical_type) if config.detect_manufacturing else []
            if pii or gov:
                col.profile = ColumnProfile(pii_findings=pii, governance_findings=gov)


def profile_tables(
    engine: Engine,
    adapter: DialectAdapter,
    tables: list[TableInfo],
    default_schema: str | None,
    config: DiscoveryConfig,
) -> ProfileArtifacts:
    """Profile all tables in-place honoring production safety guardrails."""
    artifacts = ProfileArtifacts()

    if config.profile_mode == "none":
        _apply_metadata_only_scanners(tables, config)
        return artifacts

    effective_sample = min(config.sample_size, 1000) if config.profile_mode == "fast" else config.sample_size
    now_utc = datetime.now(timezone.utc)

    with engine.connect() as conn:
        adapter.configure_safe_session(
            conn,
            read_uncommitted=config.read_uncommitted,
            query_timeout_seconds=config.query_timeout_seconds,
        )

        for tbl in tables:
            if not tbl.columns:
                continue

            # Production safety guardrails: empty table triage & byte-budget skip
            if config.skip_empty_tables and tbl.table_type == "table" and tbl.row_count == 0:
                tbl.skipped_profiling_reason = "Empty table (skipped via --skip-empty-tables)"
                _apply_metadata_only_scanners([tbl], config)
                continue

            if (
                config.skip_tables_over_mb is not None
                and tbl.size_bytes is not None
                and (tbl.size_bytes / (1024 * 1024)) > config.skip_tables_over_mb
            ):
                size_mb = tbl.size_bytes / (1024 * 1024)
                tbl.skipped_profiling_reason = (
                    f"Size ({size_mb:.1f} MB) exceeds --skip-tables-over-mb ({config.skip_tables_over_mb} MB)"
                )
                _apply_metadata_only_scanners([tbl], config)
                continue

            quoted_table = adapter.quote_table(conn, tbl.schema_name, tbl.name, default_schema)

            # Full-table exact aggregates only when NOT in tablesample/fast/read-uncommitted large mode
            exact_aggs: dict[str, dict[str, Any]] = {}
            if (
                config.profile_mode == "full"
                and not config.use_tablesample
                and tbl.table_type == "table"
                and tbl.row_count is not None
                and tbl.row_count > effective_sample
                and len(tbl.columns) <= 60
            ):
                exact_aggs = _fetch_full_table_aggregates(conn, adapter, quoted_table, tbl.columns)

            col_names_sql = ", ".join(adapter.quote_identifier(conn, c.name) for c in tbl.columns)
            sample_sql = adapter.build_sample_query(
                quoted_table=quoted_table,
                columns_sql=col_names_sql,
                sample_size=effective_sample,
                total_rows=tbl.row_count,
                use_tablesample=config.use_tablesample,
                read_uncommitted=config.read_uncommitted,
            )

            try:
                rows = conn.execute(text(sample_sql)).fetchall()
            except Exception as exc:
                safe_rollback(conn)
                adapter.configure_safe_session(
                    conn,
                    read_uncommitted=config.read_uncommitted,
                    query_timeout_seconds=config.query_timeout_seconds,
                )
                err_msg = f"Profiling query failed: {type(exc).__name__}: {str(exc)[:180]}"
                for col in tbl.columns:
                    col.profile = ColumnProfile(profile_error=err_msg)
                continue

            sample_n = len(rows)
            if tbl.row_count is None and sample_n < effective_sample:
                tbl.row_count = sample_n
                tbl.row_count_exact = True

            for col_idx, col in enumerate(tbl.columns):
                raw_vals = [r[col_idx] for r in rows]
                non_null_vals = [v for v in raw_vals if v is not None]
                non_null_n = len(non_null_vals)
                null_n = sample_n - non_null_n

                agg = exact_aggs.get(col.name)
                if agg and agg["total"] > 0:
                    total_n = agg["total"]
                    exact_nn = agg["non_null"]
                    exact_null = total_n - exact_nn
                    null_rate = round(exact_null / total_n, 4)
                    profile_non_null = exact_nn
                    profile_null = exact_null
                    profile_base = total_n
                else:
                    null_rate = round(null_n / sample_n, 4) if sample_n > 0 else 0.0
                    profile_non_null = non_null_n
                    profile_null = null_n
                    profile_base = sample_n

                str_vals = [_stringify(v) for v in non_null_vals]
                counter = Counter(str_vals)
                distinct_n = len(counter)
                distinct_rate = round(distinct_n / non_null_n, 4) if non_null_n > 0 else None

                if distinct_n > 0 and col.canonical_type in {"integer", "string", "uuid", "decimal"}:
                    artifacts.value_samples[(tbl.full_name, col.name)] = set(list(counter.keys())[:2000])
                if str_vals:
                    artifacts.raw_str_samples[(tbl.full_name, col.name)] = str_vals[:500]

                cardinality = _classify_cardinality(sample_n, non_null_n, distinct_n)

                min_val: str | None = None
                max_val: str | None = None
                parsed_dts: list[datetime] = []
                if col.canonical_type in {"date", "timestamp"} and non_null_vals:
                    parsed_dts = [dt for v in non_null_vals if (dt := _to_datetime_utc(v)) is not None]
                    if parsed_dts:
                        artifacts.timestamp_samples[(tbl.full_name, col.name)] = parsed_dts

                if agg and agg.get("min") is not None:
                    min_val = _stringify(agg["min"])
                    max_val = _stringify(agg["max"]) if agg.get("max") is not None else min_val
                elif non_null_vals:
                    if col.canonical_type in {"integer", "float", "decimal"}:
                        nums = [f for f in (_to_float(v) for v in non_null_vals) if f is not None]
                        if nums:
                            min_val = _stringify(min(nums))
                            max_val = _stringify(max(nums))
                    elif col.canonical_type in {"date", "timestamp"} and parsed_dts:
                        min_val = min(parsed_dts).isoformat()
                        max_val = max(parsed_dts).isoformat()
                    elif col.canonical_type != "binary":
                        min_val = min(str_vals)
                        max_val = max(str_vals)

                mean_val = stddev_val = p25 = p50 = p75 = None
                if col.canonical_type in {"integer", "float", "decimal"} and non_null_vals:
                    nums = sorted(f for f in (_to_float(v) for v in non_null_vals) if f is not None)
                    if nums:
                        mean_val = round(statistics.fmean(nums), 4)
                        if len(nums) > 1:
                            stddev_val = round(statistics.stdev(nums), 4)
                        p25 = round(_quantile(nums, 0.25), 4)
                        p50 = round(_quantile(nums, 0.50), 4)
                        p75 = round(_quantile(nums, 0.75), 4)

                min_len = max_len = None
                avg_len = None
                if col.canonical_type == "string" and non_null_vals:
                    lengths = [len(str(v)) for v in non_null_vals]
                    min_len = min(lengths)
                    max_len = max(lengths)
                    avg_len = round(sum(lengths) / len(lengths), 2)

                freshness_max_ts: str | None = None
                freshness_age_hours: float | None = None
                is_stale = False
                if col.canonical_type in {"date", "timestamp"}:
                    candidate_max = agg.get("max") if agg else None
                    max_dt = _to_datetime_utc(candidate_max) if candidate_max is not None else None
                    if max_dt is None and parsed_dts:
                        max_dt = max(parsed_dts)
                    if max_dt is not None:
                        freshness_max_ts = max_dt.isoformat()
                        age_h = max(0.0, (now_utc - max_dt).total_seconds() / 3600.0)
                        freshness_age_hours = round(age_h, 2)
                        is_stale = age_h > (config.stale_after_days * 24)

                pii_findings = (
                    detect_pii(col.name, col.canonical_type, str_vals) if config.detect_pii else []
                )
                gov_findings = (
                    detect_governance(col.name, col.canonical_type)
                    if config.detect_manufacturing
                    else []
                )

                top_values: list[ValueFrequency] = []
                for val_str, cnt in counter.most_common(config.top_k):
                    display = redact_value(val_str, pii_findings) if config.redact_pii else val_str
                    top_values.append(
                        ValueFrequency(
                            value=display,
                            count=cnt,
                            rate=round(cnt / non_null_n, 4) if non_null_n > 0 else 0.0,
                        )
                    )

                sample_display = [
                    redact_value(v, pii_findings) if config.redact_pii else v
                    for v in list(dict.fromkeys(str_vals))[:5]
                ]

                if config.redact_pii and pii_findings:
                    if min_val is not None:
                        min_val = redact_value(min_val, pii_findings)
                    if max_val is not None:
                        max_val = redact_value(max_val, pii_findings)

                col.profile = ColumnProfile(
                    sample_size=profile_base,
                    non_null_count=profile_non_null,
                    null_count=profile_null,
                    null_rate=null_rate,
                    distinct_count=distinct_n,
                    distinct_rate=distinct_rate,
                    cardinality_class=cardinality,
                    min_value=min_val,
                    max_value=max_val,
                    mean=mean_val,
                    stddev=stddev_val,
                    p25=p25,
                    p50=p50,
                    p75=p75,
                    min_length=min_len,
                    max_length=max_len,
                    avg_length=avg_len,
                    top_values=top_values,
                    sample_values=sample_display,
                    freshness_max_timestamp=freshness_max_ts,
                    freshness_age_hours=freshness_age_hours,
                    is_stale=is_stale,
                    pii_findings=pii_findings,
                    governance_findings=gov_findings,
                )

    return artifacts
