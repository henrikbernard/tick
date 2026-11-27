"""Dialect-portable data profiling, statistical summaries, freshness, PII, and governance scanning."""

from __future__ import annotations

import math
import re
import statistics
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import SingletonThreadPool, StaticPool

from tick.dialects import DialectAdapter, safe_rollback
from tick.models import ColumnInfo, ColumnProfile, DiscoveryConfig, TableInfo, ValueFrequency
from tick.pii import detect_governance, detect_pii, redact_value

VALUE_SAMPLE_LIMIT = 2000
_TAG_HINT_RE = re.compile(
    r"^(?:tag|sensor)$|(?:^|_)(?:tag_id|tag_name|tag_code|sensor_id|sensor_name|sensor_code|point_id|point_name|signal_id|channel_id|opc_tag)(?:$|_)",
    re.I,
)
_NON_ORDERABLE_RAW_BASES = {
    "CLOB",
    "NCLOB",
    "DBCLOB",
    "LONG",
    "XML",
    "XMLTYPE",
    "NTEXT",
    "INET",
    "CIDR",
    "MACADDR",
    "MACADDR8",
}


@dataclass
class ProfileArtifacts:
    """In-memory sample caches captured during profiling for downstream analyzers."""

    value_samples: dict[tuple[str, str], set[str]] = field(default_factory=dict)
    value_sample_complete: dict[tuple[str, str], bool] = field(default_factory=dict)
    raw_str_samples: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    timestamp_samples: dict[tuple[str, str], list[datetime]] = field(default_factory=dict)
    # (table_full_name, ts_col_name, tag_col_name) -> {tag_value: [timestamps]}
    timestamp_by_tag_samples: dict[tuple[str, str, str], dict[str, list[datetime]]] = field(
        default_factory=dict
    )
    tablesampled_tables: set[str] = field(default_factory=set)

    def merge(self, other: ProfileArtifacts) -> None:
        self.value_samples.update(other.value_samples)
        self.value_sample_complete.update(other.value_sample_complete)
        self.raw_str_samples.update(other.raw_str_samples)
        self.timestamp_samples.update(other.timestamp_samples)
        self.timestamp_by_tag_samples.update(other.timestamp_by_tag_samples)
        self.tablesampled_tables.update(other.tablesampled_tables)


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


def _safe_astimezone_utc(dt: datetime) -> datetime:
    if not dt.tzinfo:
        return dt.replace(tzinfo=timezone.utc)
    try:
        return dt.astimezone(timezone.utc)
    except OverflowError:
        if dt.year >= 9999:
            return datetime.max.replace(tzinfo=timezone.utc)
        return datetime.min.replace(tzinfo=timezone.utc)


def _to_datetime_utc(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return _safe_astimezone_utc(val)
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
            return _safe_astimezone_utc(dt)
        except (ValueError, OverflowError):
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


def _quantile(sorted_vals: list[float], q: float) -> float | None:
    if not sorted_vals:
        return 0.0
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    val = sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac
    return val if math.isfinite(val) else None


def _supports_sql_min_max(col: ColumnInfo, adapter_name: str) -> bool:
    if col.canonical_type in {"binary", "json", "boolean", "other", "uuid"}:
        return False
    base = re.split(r"[\s(<]", col.raw_type.strip().upper(), maxsplit=1)[0]
    if base in _NON_ORDERABLE_RAW_BASES:
        return False
    if base == "TEXT" and adapter_name in {"mssql", "hana"}:
        return False
    return True


def _fetch_full_table_aggregates(
    conn: Connection,
    adapter: DialectAdapter,
    quoted_table: str,
    columns: list[ColumnInfo],
    config: DiscoveryConfig | None = None,
) -> dict[str, dict[str, Any]]:
    """Run a single batched SQL aggregate query for exact null counts and min/max."""
    if not columns:
        return {}

    # Use alphabetic alias prefixes so Oracle/DB2 (which forbid leading '_') parse cleanly
    select_parts: list[str] = ["COUNT(*) AS t_total"]
    for idx, col in enumerate(columns):
        qcol = adapter.quote_identifier(conn, col.name)
        select_parts.append(f"COUNT({qcol}) AS c_nn_{idx}")
        if _supports_sql_min_max(col, adapter.name):
            select_parts.append(f"MIN({qcol}) AS c_min_{idx}")
            select_parts.append(f"MAX({qcol}) AS c_max_{idx}")

    sql = f"SELECT {', '.join(select_parts)} FROM {quoted_table}"
    try:
        row = conn.exec_driver_sql(sql).mappings().first()
    except Exception:
        safe_rollback(conn)
        if config is not None:
            adapter.configure_safe_session(
                conn,
                read_uncommitted=config.read_uncommitted,
                query_timeout_seconds=config.query_timeout_seconds,
            )
        return {}
    if not row:
        return {}

    # Normalize keys case-insensitively (Snowflake/Oracle/DB2 fold unquoted aliases to UPPERCASE)
    row_ci = {str(k).lower(): v for k, v in row.items()}
    total = int(row_ci.get("t_total") or 0)
    out: dict[str, dict[str, Any]] = {}
    for idx, col in enumerate(columns):
        nn = int(row_ci.get(f"c_nn_{idx}") or 0)
        out[col.name] = {
            "total": total,
            "non_null": nn,
            "min": row_ci.get(f"c_min_{idx}"),
            "max": row_ci.get(f"c_max_{idx}"),
        }
    return out


def _apply_metadata_only_scanners(tables: list[TableInfo], config: DiscoveryConfig) -> None:
    for tbl in tables:
        for col in tbl.columns:
            pii = detect_pii(col.name, col.canonical_type, []) if config.detect_pii else []
            gov = (
                detect_governance(col.name, col.canonical_type, semantic_name=col.semantic_name)
                if config.detect_manufacturing
                else []
            )
            if pii or gov:
                col.profile = ColumnProfile(pii_findings=pii, governance_findings=gov)


def _profile_single_table(
    conn: Connection,
    adapter: DialectAdapter,
    tbl: TableInfo,
    default_schema: str | None,
    config: DiscoveryConfig,
    effective_sample: int,
    now_utc: datetime,
) -> ProfileArtifacts:
    """Profile a single table on `conn`, mutating `tbl` in-place and returning local sample artifacts."""
    artifacts = ProfileArtifacts()
    if not tbl.columns:
        return artifacts

    quoted_table = adapter.quote_table(conn, tbl.schema_name, tbl.name, default_schema)

    # Production safety guardrails: empty table triage & byte-budget skip
    if config.skip_empty_tables and tbl.table_type == "table" and tbl.row_count == 0:
        truly_empty = tbl.row_count_exact
        if not truly_empty:
            # Verify stale stats (e.g. Postgres reltuples=0 after INSERT before ANALYZE) with a cheap 1-row probe
            probe_sql = adapter.build_sample_query(
                quoted_table=quoted_table,
                columns_sql="1",
                sample_size=1,
                total_rows=0,
                use_tablesample=False,
                read_uncommitted=config.read_uncommitted,
            )
            try:
                truly_empty = conn.exec_driver_sql(probe_sql).first() is None
            except Exception:
                safe_rollback(conn)
                truly_empty = False
        if truly_empty:
            tbl.row_count_exact = True
            tbl.skipped_profiling_reason = "Empty table (skipped via --skip-empty-tables)"
            _apply_metadata_only_scanners([tbl], config)
            return artifacts

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
        return artifacts

    # Full-table exact aggregates only when NOT in tablesample/fast/read-uncommitted large mode
    exact_aggs: dict[str, dict[str, Any]] = {}
    if (
        config.profile_mode == "full"
        and not config.use_tablesample
        and not config.read_uncommitted
        and tbl.table_type == "table"
        and tbl.row_count is not None
        and tbl.row_count > effective_sample
        and len(tbl.columns) <= 60
    ):
        exact_aggs = _fetch_full_table_aggregates(conn, adapter, quoted_table, tbl.columns, config)

    col_names_sql = ", ".join(adapter.quote_identifier(conn, c.name) for c in tbl.columns)
    used_tablesample = bool(
        config.use_tablesample and tbl.row_count and tbl.row_count > effective_sample * 4
    )
    sample_sql = adapter.build_sample_query(
        quoted_table=quoted_table,
        columns_sql=col_names_sql,
        sample_size=effective_sample,
        total_rows=tbl.row_count,
        use_tablesample=config.use_tablesample,
        read_uncommitted=config.read_uncommitted,
    )

    try:
        rows = conn.exec_driver_sql(sample_sql).fetchall()
        # TABLESAMPLE SYSTEM / N ROWS can return 0 rows on non-empty tables — fall back once to LIMIT
        if used_tablesample and not rows and (tbl.row_count is None or tbl.row_count > 0):
            used_tablesample = False
            fallback_sql = adapter.build_sample_query(
                quoted_table=quoted_table,
                columns_sql=col_names_sql,
                sample_size=effective_sample,
                total_rows=tbl.row_count,
                use_tablesample=False,
                read_uncommitted=config.read_uncommitted,
            )
            rows = conn.exec_driver_sql(fallback_sql).fetchall()
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
        return artifacts

    if used_tablesample:
        artifacts.tablesampled_tables.add(tbl.full_name)

    sample_n = len(rows)
    if not used_tablesample:
        if sample_n < effective_sample and (tbl.row_count is None or not tbl.row_count_exact):
            tbl.row_count = sample_n
            tbl.row_count_exact = True
        elif not tbl.row_count_exact and sample_n > (tbl.row_count or 0):
            tbl.row_count = sample_n

    # Capture per-tag timestamp groups for multi-tag OT historian tables
    ts_col_indices = [
        (idx, c.name) for idx, c in enumerate(tbl.columns) if c.canonical_type == "timestamp"
    ]
    tag_col_indices = [
        (idx, c.name)
        for idx, c in enumerate(tbl.columns)
        if c.canonical_type in {"string", "integer", "uuid"}
        and _TAG_HINT_RE.search(re.sub(r"[\s\-]+", "_", c.name.strip()))
    ]
    if rows and ts_col_indices and tag_col_indices:
        for ts_idx, ts_name in ts_col_indices:
            for tag_idx, tag_name in tag_col_indices:
                if tag_idx == ts_idx:
                    continue
                grouped: dict[str, list[datetime]] = {}
                for r in rows:
                    tv = r[tag_idx]
                    dt_raw = r[ts_idx]
                    if tv is None or dt_raw is None:
                        continue
                    dt = _to_datetime_utc(dt_raw)
                    if dt is not None:
                        grouped.setdefault(_stringify(tv), []).append(dt)
                if grouped:
                    artifacts.timestamp_by_tag_samples[(tbl.full_name, ts_name, tag_name)] = grouped

    table_fully_fetched = (not used_tablesample) and (
        sample_n < effective_sample
        or (tbl.row_count_exact and tbl.row_count is not None and sample_n >= tbl.row_count)
    )

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
        distinct_n: int | None = len(counter)
        distinct_rate = round(distinct_n / non_null_n, 4) if non_null_n > 0 else None

        if distinct_n > 0 and col.canonical_type in {"integer", "string", "uuid", "decimal"}:
            artifacts.value_samples[(tbl.full_name, col.name)] = set(
                list(counter.keys())[:VALUE_SAMPLE_LIMIT]
            )
            artifacts.value_sample_complete[(tbl.full_name, col.name)] = (
                table_fully_fetched and distinct_n <= VALUE_SAMPLE_LIMIT
            )
        if str_vals:
            artifacts.raw_str_samples[(tbl.full_name, col.name)] = str_vals[:500]

        if non_null_n == 0 and profile_non_null > 0:
            # Sample saw only NULLs, but full-table exact aggregates confirm non-null rows exist
            distinct_n = None
            distinct_rate = None
            cardinality = "unknown"
        else:
            cardinality = _classify_cardinality(sample_n, non_null_n, distinct_n)
            if (
                cardinality == "constant"
                and agg
                and agg.get("min") is not None
                and agg.get("max") is not None
                and _stringify(agg["min"]) != _stringify(agg["max"])
            ):
                cardinality = "low"

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
                try:
                    m = statistics.fmean(nums)
                    if math.isfinite(m):
                        mean_val = round(m, 4)
                except (OverflowError, ValueError):
                    mean_val = None
                if len(nums) > 1:
                    try:
                        sd = statistics.stdev(nums)
                        if math.isfinite(sd):
                            stddev_val = round(sd, 4)
                    except (OverflowError, ValueError):
                        stddev_val = None
                q25 = _quantile(nums, 0.25)
                q50 = _quantile(nums, 0.50)
                q75 = _quantile(nums, 0.75)
                p25 = round(q25, 4) if q25 is not None else None
                p50 = round(q50, 4) if q50 is not None else None
                p75 = round(q75, 4) if q75 is not None else None

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
            detect_governance(col.name, col.canonical_type, semantic_name=col.semantic_name)
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
            if freshness_max_ts is not None:
                freshness_max_ts = redact_value(freshness_max_ts, pii_findings)

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


def _can_parallelize(engine: Engine, workers: int, table_count: int) -> bool:
    if workers <= 1 or table_count <= 1:
        return False
    if isinstance(engine.pool, (StaticPool, SingletonThreadPool)):
        return False
    url_str = str(engine.url).lower()
    if ":memory:" in url_str or "mode=memory" in url_str:
        return False
    # In-memory SQLite and DuckDB engines share a single in-memory instance not safe for multi-thread checkout
    if engine.dialect.name in {"sqlite", "duckdb"} and engine.url.database in {None, "", ":memory:"}:
        return False
    return True


def profile_tables(
    engine: Engine,
    adapter: DialectAdapter,
    tables: list[TableInfo],
    default_schema: str | None,
    config: DiscoveryConfig,
) -> ProfileArtifacts:
    """Profile all tables in-place honoring production safety guardrails and optional worker concurrency."""
    artifacts = ProfileArtifacts()

    if config.profile_mode == "none":
        _apply_metadata_only_scanners(tables, config)
        return artifacts

    effective_sample = min(config.sample_size, 1000) if config.profile_mode == "fast" else config.sample_size
    now_utc = datetime.now(timezone.utc)

    if _can_parallelize(engine, config.workers, len(tables)):
        pool_cap = 10 if engine.dialect.name in {"sqlite", "duckdb"} else config.workers
        max_workers = min(config.workers, len(tables), pool_cap)

        def _worker(tbl: TableInfo) -> ProfileArtifacts:
            try:
                with engine.connect() as conn:
                    adapter.configure_safe_session(
                        conn,
                        read_uncommitted=config.read_uncommitted,
                        query_timeout_seconds=config.query_timeout_seconds,
                    )
                    return _profile_single_table(
                        conn, adapter, tbl, default_schema, config, effective_sample, now_utc
                    )
            except Exception as exc:
                err_msg = f"Worker profiling failed: {type(exc).__name__}: {str(exc)[:180]}"
                for col in tbl.columns:
                    col.profile = ColumnProfile(profile_error=err_msg)
                return ProfileArtifacts()

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_worker, tbl) for tbl in tables]
            for fut in futures:
                artifacts.merge(fut.result())
        return artifacts

    with engine.connect() as conn:
        adapter.configure_safe_session(
            conn,
            read_uncommitted=config.read_uncommitted,
            query_timeout_seconds=config.query_timeout_seconds,
        )
        for tbl in tables:
            local_artifacts = _profile_single_table(
                conn, adapter, tbl, default_schema, config, effective_sample, now_utc
            )
            artifacts.merge(local_artifacts)

    return artifacts
