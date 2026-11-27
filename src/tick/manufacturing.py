"""Manufacturing-specific data discovery: legacy dates, OT telemetry, BOM hierarchies, and backfill sizing."""

from __future__ import annotations

import re
import statistics
from datetime import datetime

from tick.models import (
    ColumnInfo,
    HierarchyInfo,
    IngestionSizing,
    LegacyDateEncoding,
    TableInfo,
    TelemetryProfile,
)
from tick.profiler import ProfileArtifacts, _to_datetime_utc

# Regexes for legacy date/time & OT patterns
YYYYMMDD_RE = re.compile(r"^(19\d{2}|20\d{2}|2100)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$")
JULIAN_CYYDDD_RE = re.compile(r"^[01]\d{2}(00[1-9]|0[1-9]\d|[12]\d{2}|3[0-5]\d|36[0-6])$")
JULIAN_YYYYDDD_RE = re.compile(r"^(19\d{2}|20\d{2})(00[1-9]|0[1-9]\d|[12]\d{2}|3[0-5]\d|36[0-6])$")
HHMMSS_RE = re.compile(r"^([01]\d|2[0-3])[0-5]\d[0-5]\d$")

DATE_NAME_RE = re.compile(r"(?:^|_)(?:date|dt|erdat|aedat|budat|bldat|day|mfg_dt|shift_dt|start_dt|end_dt)(?:$|_)", re.I)
TIME_NAME_RE = re.compile(r"(?:^|_)(?:time|tm|erzet|aezeit|hr|hhmmss|shift_tm|start_tm|end_tm)(?:$|_)", re.I)

TAG_COL_RE = re.compile(r"(?:^|_)(?:tag|tag_id|tag_name|sensor|sensor_id|point_id|point_name|signal_id|channel_id|opc_tag)(?:$|_)", re.I)
VALUE_COL_RE = re.compile(r"(?:^|_)(?:val|value|reading|numeric_val|float_val|measurement|pv|sensor_value)(?:$|_)", re.I)
QUALITY_COL_RE = re.compile(r"(?:^|_)(?:quality|quality_code|opc_quality|qc|signal_quality|status_code)(?:$|_)", re.I)
SENSOR_METRIC_RE = re.compile(
    r"(?:temp|temperature|pressure|rpm|vibration|vib|voltage|current|amp|flow|humidity|torque|speed|level|cycle|plc|spindle|motor|bearing|load|power|kw)",
    re.I,
)

# Parent/child hierarchy column pairs
HIERARCHY_PAIRS: list[tuple[re.Pattern[str], re.Pattern[str], str]] = [
    (
        re.compile(r"^(?:parent_part_id|parent_part|parent_material|parent_matnr|assembly_id|assembly_part_id|parent_sku|parent_item_id)$", re.I),
        re.compile(r"^(?:child_part_id|child_part|component_material|component_id|component_part_id|idnrk|part_id|material_id|child_sku|child_item_id)$", re.I),
        "bom",
    ),
    (
        re.compile(r"^(?:parent_lot_id|parent_lot|parent_batch_id|parent_batch|source_lot_id|parent_serial)$", re.I),
        re.compile(r"^(?:child_lot_id|lot_id|batch_id|child_batch_id|serial_number|child_serial|charg)$", re.I),
        "lot_genealogy",
    ),
    (
        re.compile(r"^(?:parent_work_order_id|parent_order_id|parent_aufnr|super_order_id)$", re.I),
        re.compile(r"^(?:work_order_id|order_id|aufnr|id)$", re.I),
        "work_order_tree",
    ),
    (
        re.compile(r"^(?:parent_asset_id|parent_equipment_id|parent_location_id|parent_floc|parent_id)$", re.I),
        re.compile(r"^(?:asset_id|equipment_id|location_id|equnr|tplnr|id)$", re.I),
        "asset_or_org_tree",
    ),
]


TELEMETRY_TS_RE = re.compile(
    r"(?:^|_)(?:recorded_at|event_time|event_ts|sample_time|reading_time|measured_at|occurred_at|timestamp|ts|time_stamp|opc_ts)(?:$|_)",
    re.I,
)


def _detect_legacy_dates_for_table(table: TableInfo, artifacts: ProfileArtifacts) -> None:
    """Detect YYYYMMDD, Julian, Excel serial, HHMMSS, and split date+time column pairs."""
    date_cols: dict[str, ColumnInfo] = {}
    time_cols: dict[str, ColumnInfo] = {}

    for col in table.columns:
        if col.profile is None or col.canonical_type not in {"integer", "string", "decimal"}:
            continue
        samples = [
            s.strip()
            for s in artifacts.raw_str_samples.get((table.full_name, col.name), [])
            if s.strip() and s.strip() not in {"0", "00000000", "000000", "99991231"}
        ]
        if len(samples) < 3:
            continue

        # Normalize decimal-formatted ints like "20261126.0" or "20261126.00"
        norm_samples = [re.sub(r"\.0+$", "", s) for s in samples[:200]]
        n = len(norm_samples)
        col_lit = repr(col.name)
        norm_col = re.sub(r"[\s\-]+", "_", col.name.strip())

        # 1. YYYYMMDD
        ymd_hits = sum(1 for s in norm_samples if YYYYMMDD_RE.match(s))
        if ymd_hits / n >= 0.80:
            enc = "yyyymmdd_int" if col.canonical_type in {"integer", "decimal"} else "yyyymmdd_str"
            col.profile.legacy_date = LegacyDateEncoding(
                encoding=enc,  # type: ignore[arg-type]
                confidence="high" if (ymd_hits / n >= 0.95 or DATE_NAME_RE.search(norm_col)) else "medium",
                sample_match_rate=round(ymd_hits / n, 4),
                spark_expression=f"F.to_date(F.col({col_lit}).cast('string'), 'yyyyMMdd')",
                note=f"Legacy {enc} date encoding detected in '{col.name}'; parse to Delta DATE in Silver.",
            )
            date_cols[col.name] = col
            continue

        # 2. HHMMSS (require time-like column name to avoid matching 6-digit zip/part numbers)
        if TIME_NAME_RE.search(norm_col):
            padded = [s.zfill(6) if s.isdigit() and len(s) <= 6 else s for s in norm_samples]
            hms_hits = sum(1 for s in padded if HHMMSS_RE.match(s))
            if hms_hits / n >= 0.80:
                col.profile.legacy_date = LegacyDateEncoding(
                    encoding="hhmmss",
                    confidence="high",
                    sample_match_rate=round(hms_hits / n, 4),
                    spark_expression=f"F.lpad(F.col({col_lit}).cast('string'), 6, '0')",
                    note=f"Legacy HHMMSS time encoding detected in '{col.name}'; stitch with companion date column.",
                )
                time_cols[col.name] = col
                continue

        # 3. Julian CYYDDD (SAP/JDE) or YYYYDDD (require date-like column name)
        if DATE_NAME_RE.search(norm_col):
            cyy_hits = sum(1 for s in norm_samples if JULIAN_CYYDDD_RE.match(s))
            if cyy_hits / n >= 0.80:
                col.profile.legacy_date = LegacyDateEncoding(
                    encoding="julian_cyyddd",
                    confidence="high",
                    sample_match_rate=round(cyy_hits / n, 4),
                    spark_expression=(
                        f"F.to_date(F.concat((F.substring(F.col({col_lit}).cast('string'), 1, 1).cast('int') * 100 + 1900 "
                        f"+ F.substring(F.col({col_lit}).cast('string'), 2, 2).cast('int')).cast('string'), "
                        f"F.substring(F.col({col_lit}).cast('string'), 4, 3)), 'yyyyDDD')"
                    ),
                    note=f"Legacy JDE/ERP CYYDDD Julian date detected in '{col.name}'.",
                )
                date_cols[col.name] = col
                continue

            yyyyddd_hits = sum(1 for s in norm_samples if JULIAN_YYYYDDD_RE.match(s))
            if yyyyddd_hits / n >= 0.80:
                col.profile.legacy_date = LegacyDateEncoding(
                    encoding="julian_yyyyddd",
                    confidence="high",
                    sample_match_rate=round(yyyyddd_hits / n, 4),
                    spark_expression=f"F.to_date(F.col({col_lit}).cast('string'), 'yyyyDDD')",
                    note=f"Legacy YYYYDDD Julian date detected in '{col.name}'.",
                )
                date_cols[col.name] = col
                continue

            # 4. Excel serial dates (1995-2040 -> 34700..51500)
            excel_hits = sum(1 for s in norm_samples if s.isdigit() and 34700 <= int(s) <= 51500)
            if excel_hits / n >= 0.85:
                col.profile.legacy_date = LegacyDateEncoding(
                    encoding="excel_serial",
                    confidence="medium",
                    sample_match_rate=round(excel_hits / n, 4),
                    spark_expression=f"F.date_add(F.lit('1899-12-30'), F.col({col_lit}).cast('int'))",
                    note=f"Possible Excel serial date integer detected in '{col.name}'.",
                )
                date_cols[col.name] = col

    # Link split DATE + TIME pairs (e.g. erdat + erzet, mfg_date + mfg_time, "Mfg Date" + "Mfg Time")
    sap_pairs = [("erdat", "erzet"), ("aedat", "aezeit"), ("cpudt", "cputm")]
    for dname, dcol in date_cols.items():
        dl = re.sub(r"[\s\-]+", "_", dname.strip()).lower()
        matched_time: str | None = None
        for sd, st in sap_pairs:
            if dl == sd:
                matched_time = next(
                    (t for t in time_cols if re.sub(r"[\s\-]+", "_", t.strip()).lower() == st),
                    None,
                )
        if not matched_time:
            stem = re.sub(r"_?(?:date|dt)$", "", dl)
            for tname in time_cols:
                tl = re.sub(r"[\s\-]+", "_", tname.strip()).lower()
                tstem = re.sub(r"_?(?:time|tm)$", "", tl)
                if (stem and stem == tstem) or (not stem and not tstem and dl in {"date", "dt"} and tl in {"time", "tm"}):
                    matched_time = tname
                    break
        if matched_time and dcol.profile and dcol.profile.legacy_date:
            dcol.profile.legacy_date.paired_column = matched_time
            if dcol.profile.legacy_date.encoding in {"yyyymmdd_int", "yyyymmdd_str"}:
                dcol.profile.legacy_date.spark_expression = (
                    f"F.to_timestamp(F.concat(F.col({dname!r}).cast('string'), "
                    f"F.lpad(F.col({matched_time!r}).cast('string'), 6, '0')), 'yyyyMMddHHmmss')"
                )
            else:
                date_expr = dcol.profile.legacy_date.spark_expression
                dcol.profile.legacy_date.spark_expression = (
                    f"F.to_timestamp(F.concat(F.date_format({date_expr}, 'yyyyMMdd'), "
                    f"F.lpad(F.col({matched_time!r}).cast('string'), 6, '0')), 'yyyyMMddHHmmss')"
                )
            dcol.profile.legacy_date.note = (
                f"Split legacy DATE ('{dname}') + TIME ('{matched_time}') pair; stitch into TIMESTAMP in Silver."
            )
            tcol = time_cols[matched_time]
            if tcol.profile and tcol.profile.legacy_date:
                tcol.profile.legacy_date.paired_column = dname


def _norm_col(name: str) -> str:
    return re.sub(r"[\s\-]+", "_", name.strip())


def _detect_telemetry_for_table(table: TableInfo, artifacts: ProfileArtifacts) -> None:
    """Detect OT/SCADA narrow historian tag tables or wide PLC polling tables."""
    if table.table_type != "table":
        return

    ts_cols = [c for c in table.columns if c.canonical_type == "timestamp"]
    if not ts_cols:
        return
    # Prefer sensor/event timestamp names over ETL/audit timestamps like ingested_at or created_at
    ts_col = next((c for c in ts_cols if TELEMETRY_TS_RE.search(_norm_col(c.name))), ts_cols[0])

    tag_col = next((c for c in table.columns if TAG_COL_RE.search(_norm_col(c.name))), None)
    val_col = next(
        (
            c
            for c in table.columns
            if VALUE_COL_RE.search(_norm_col(c.name)) and c.canonical_type in {"float", "decimal", "integer"}
        ),
        None,
    )
    qual_col = next((c for c in table.columns if QUALITY_COL_RE.search(_norm_col(c.name))), None)
    sensor_num_cols = [
        c
        for c in table.columns
        if c.canonical_type in {"float", "decimal", "integer"}
        and not c.is_primary_key
        and not c.is_foreign_key
        and SENSOR_METRIC_RE.search(_norm_col(c.name))
    ]

    pattern: str | None = None
    value_cols: list[str] = []
    if tag_col and val_col:
        pattern = "narrow_historian_eav"
        value_cols = [val_col.name]
    elif len(sensor_num_cols) >= 3:
        pattern = "wide_plc_polling"
        value_cols = [c.name for c in sensor_num_cols]
    else:
        return

    # Polling cadence & max gap from sampled timestamps (per-tag for narrow historian EAV tables)
    median_interval: float | None = None
    max_gap: float | None = None
    deltas: list[float] = []
    if pattern == "narrow_historian_eav" and tag_col is not None:
        grouped = artifacts.timestamp_by_tag_samples.get((table.full_name, ts_col.name, tag_col.name))
        if grouped:
            for tag_dts in grouped.values():
                if len(tag_dts) < 2:
                    continue
                s_dts = sorted(tag_dts)
                for i in range(1, len(s_dts)):
                    diff_s = (s_dts[i] - s_dts[i - 1]).total_seconds()
                    if diff_s > 0:
                        deltas.append(diff_s)

    if not deltas:
        dts = sorted(artifacts.timestamp_samples.get((table.full_name, ts_col.name), []))
        if len(dts) >= 3:
            deltas = [
                (dts[i] - dts[i - 1]).total_seconds()
                for i in range(1, len(dts))
                if (dts[i] - dts[i - 1]).total_seconds() > 0
            ]

    if deltas:
        median_interval = round(statistics.median(deltas), 2)
        max_gap = round(max(deltas), 2)

    # Flatline detection (stuck sensor values with zero variance)
    flatlines: list[str] = []
    for cname in value_cols:
        c = next((col for col in table.columns if col.name == cname), None)
        if c and c.profile and c.profile.sample_size >= 10:
            if c.profile.cardinality_class == "constant" or (c.profile.stddev is not None and c.profile.stddev == 0.0):
                flatlines.append(cname)

    # Bad quality code detection (OPC UA / historian quality codes)
    bad_qual_rate: float | None = None
    if qual_col:
        q_samples = artifacts.raw_str_samples.get((table.full_name, qual_col.name), [])
        if q_samples:
            bad = 0
            for qv in q_samples:
                ql = qv.strip().lower()
                if ql in {"bad", "uncertain", "error", "fault", "0", "false"}:
                    bad += 1
                elif ql.isdigit() and int(ql) < 192:
                    # Standard OPC Classic quality: >= 192 is GOOD
                    bad += 1
            bad_qual_rate = round(bad / len(q_samples), 4)

    watermark_delay = "5 minutes" if (median_interval is not None and median_interval <= 60) else "15 minutes"
    cluster_keys = [tag_col.name, ts_col.name] if tag_col else [ts_col.name]
    notes = [
        f"Detected {pattern.replace('_', ' ')} OT telemetry pattern.",
        f"Recommended Databricks Liquid Clustering: CLUSTER BY ({', '.join(cluster_keys)}).",
    ]
    if flatlines:
        notes.append(f"Potential flatlined/stuck sensor column(s) with zero variance: {', '.join(flatlines)}.")
    if bad_qual_rate is not None and bad_qual_rate > 0:
        notes.append(f"Quality column '{qual_col.name}' shows {bad_qual_rate:.1%} non-GOOD sensor readings; filter or quarantine in Silver.")

    table.telemetry_profile = TelemetryProfile(
        pattern=pattern,  # type: ignore[arg-type]
        tag_column=tag_col.name if tag_col else None,
        timestamp_column=ts_col.name,
        value_columns=value_cols,
        quality_column=qual_col.name if qual_col else None,
        median_interval_seconds=median_interval,
        max_gap_seconds=max_gap,
        flatline_columns=flatlines,
        bad_quality_rate=bad_qual_rate,
        recommended_watermark_delay=watermark_delay,
        recommended_liquid_cluster=cluster_keys,
        notes=notes,
    )


def _detect_hierarchy_for_table(table: TableInfo, artifacts: ProfileArtifacts) -> None:
    """Detect recursive BOM, lot genealogy, work-order trees, and asset hierarchies."""
    if table.table_type != "table":
        return

    # 1. Explicit self-referential FK (must match both schema and table name!)
    for fk in table.foreign_keys:
        ref_schema = (fk.referred_schema or table.schema_name).lower()
        if (
            ref_schema == table.schema_name.lower()
            and fk.referred_table.lower() == table.name.lower()
            and fk.constrained_columns
            and fk.referred_columns
        ):
            pcol = fk.constrained_columns[0]
            ccol = fk.referred_columns[0]
            pat = "bom" if any(k in table.name.lower() for k in ("bom", "material", "part")) else (
                "lot_genealogy" if any(k in table.name.lower() for k in ("lot", "batch", "serial")) else "asset_or_org_tree"
            )
            table.hierarchy = HierarchyInfo(
                pattern=pat,  # type: ignore[arg-type]
                parent_column=pcol,
                child_column=ccol,
                reason=f"Explicit self-referential foreign key ({pcol} -> {ccol}); model with recursive CTE / BOM explosion in Silver/Gold.",
            )
            return

    # 2. Named parent/child column pair on the same table
    col_names = [c.name for c in table.columns]
    for parent_re, child_re, pat in HIERARCHY_PAIRS:
        pcol = next((n for n in col_names if parent_re.match(n)), None)
        ccol = next((n for n in col_names if child_re.match(n) and n != pcol), None)
        if not (pcol and ccol):
            continue

        p_vals = artifacts.value_samples.get((table.full_name, pcol))
        c_vals = artifacts.value_samples.get((table.full_name, ccol))
        overlap: float | None = None
        if p_vals and c_vals:
            overlap = round(len(p_vals & c_vals) / max(1, len(p_vals)), 4)

        table.hierarchy = HierarchyInfo(
            pattern=pat,  # type: ignore[arg-type]
            parent_column=pcol,
            child_column=ccol,
            overlap_rate=overlap,
            reason=(
                f"Recursive {pat.replace('_', ' ')} hierarchy detected ({pcol} -> {ccol})"
                + (f" with {overlap:.0%} multi-level node overlap." if overlap is not None else ".")
                + " Plan for recursive CTE / GraphFrames explosion in Databricks Silver/Gold."
            ),
        )
        return


def _estimate_row_width(table: TableInfo) -> int:
    width = 0
    for col in table.columns:
        ct = col.canonical_type
        if ct == "integer":
            width += 8
        elif ct in {"float", "decimal", "timestamp"}:
            width += 8
        elif ct == "date":
            width += 4
        elif ct == "boolean":
            width += 1
        elif ct == "uuid":
            width += 16
        elif ct == "string":
            avg_l = col.profile.avg_length if (col.profile and col.profile.avg_length) else 32
            width += int(avg_l) + 4
        elif ct in {"json", "binary"}:
            width += 128
        else:
            width += 16
    return max(width, 16)


def _estimate_sizing_for_table(table: TableInfo, artifacts: ProfileArtifacts) -> None:
    """Estimate backfill volume, daily incremental delta rate, and Databricks compute tier."""
    if table.table_type != "table":
        table.ingestion_sizing = IngestionSizing(
            recommended_cadence="view_downstream",
            recommended_databricks_tier="Lakeflow Declarative Pipeline (Silver/Gold Materialized View)",
            notes=["Rebuild view logic in Databricks Silver/Gold rather than ingesting directly."],
        )
        return

    row_count = table.row_count or 0
    row_width = _estimate_row_width(table)
    if table.size_bytes is not None and table.size_bytes > 0:
        est_size_mb = round(table.size_bytes / (1024 * 1024), 3)
        if row_count > 0:
            row_width = max(8, int(table.size_bytes / row_count))
    else:
        est_size_mb = round((row_count * row_width) / (1024 * 1024), 3)

    # Estimate daily rows using best temporal column sample
    daily_rows: float | None = None
    daily_mb: float | None = None
    best_ts_col: str | None = None

    if table.recommendations and table.recommendations.watermark_candidates:
        for wm in table.recommendations.watermark_candidates:
            if (table.full_name, wm.column) in artifacts.timestamp_samples:
                best_ts_col = wm.column
                break
    if best_ts_col is None:
        for col in table.columns:
            if (table.full_name, col.name) in artifacts.timestamp_samples:
                best_ts_col = col.name
                break

    if best_ts_col and row_count > 0:
        dts = artifacts.timestamp_samples[(table.full_name, best_ts_col)]
        ts_col_obj = next((c for c in table.columns if c.name == best_ts_col), None)
        full_min = (
            _to_datetime_utc(ts_col_obj.profile.min_value)
            if (ts_col_obj and ts_col_obj.profile and ts_col_obj.profile.min_value)
            else None
        )
        full_max = (
            _to_datetime_utc(ts_col_obj.profile.max_value)
            if (ts_col_obj and ts_col_obj.profile and ts_col_obj.profile.max_value)
            else None
        )
        if full_min is not None and full_max is not None and full_max > full_min:
            span_seconds = (full_max - full_min).total_seconds()
        elif len(dts) >= 2:
            span_seconds = (max(dts) - min(dts)).total_seconds()
        else:
            span_seconds = 0.0

        if span_seconds > 0:
            if table.telemetry_profile is not None and span_seconds >= 60.0:
                span_days = span_seconds / 86400.0
            else:
                span_days = max(1.0, span_seconds / 86400.0)
            daily_rows = round(row_count / span_days, 1)
            daily_mb = round((daily_rows * row_width) / (1024 * 1024), 4)

    notes: list[str] = []
    if table.telemetry_profile is not None:
        cadence = "continuous_streaming" if (daily_rows or 0) >= 50_000 else "micro_batch_15m"
        tier = "Serverless Lakeflow Pipeline (Structured Streaming / Auto Loader)"
        notes.append("OT telemetry table: micro-batch or continuous streaming recommended for shift/OEE dashboards.")
    elif (daily_rows or 0) >= 100_000 or est_size_mb >= 500:
        cadence = "hourly_batch"
        tier = "Lakeflow Connect Managed CDC + Serverless Lakeflow Pipeline"
        notes.append("High volume table; prefer log-based CDC / Lakeflow Connect with Liquid Clustering.")
    elif row_count <= 2000:
        cadence = "weekly_full_refresh" if (daily_rows is not None and daily_rows < 2) else "daily_batch"
        tier = "Serverless Databricks Workflow / Lakeflow Batch"
        notes.append("Small table; lightweight Serverless batch full refresh or merge.")
    else:
        cadence = "daily_batch"
        tier = "Serverless Lakeflow Pipeline (Incremental Merge)"
        notes.append("Standard daily or shift-aligned incremental batch load.")

    table.ingestion_sizing = IngestionSizing(
        estimated_row_width_bytes=row_width,
        estimated_size_mb=est_size_mb,
        estimated_daily_rows=daily_rows,
        estimated_daily_mb=daily_mb,
        recommended_cadence=cadence,  # type: ignore[arg-type]
        recommended_databricks_tier=tier,
        notes=notes,
    )


def analyze_manufacturing_patterns(
    tables: list[TableInfo],
    artifacts: ProfileArtifacts,
) -> None:
    """Run all manufacturing-specific intelligence detectors on discovered tables."""
    for tbl in tables:
        _detect_legacy_dates_for_table(tbl, artifacts)
        _detect_telemetry_for_table(tbl, artifacts)
        _detect_hierarchy_for_table(tbl, artifacts)
        _estimate_sizing_for_table(tbl, artifacts)
