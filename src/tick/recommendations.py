"""Data engineering recommendations, entity classification, and data quality scoring."""

from __future__ import annotations

import re

from tick.models import (
    ColumnInfo,
    PartitionCandidate,
    QualityIssue,
    RelationshipInfo,
    TableInfo,
    TableRecommendations,
    WatermarkCandidate,
)
from tick.pii import HIGH_RISK_CATEGORIES

UPDATED_AT_RE = re.compile(
    r"(?:^|_)(?:updated|modified|changed|last_updated|last_modified|synced|loaded|ingested|etl_updated)(?:_at|_on|_ts|_time|_timestamp|_date)?$",
    re.I,
)
CREATED_AT_RE = re.compile(
    r"(?:^|_)(?:created|inserted|registered|signed_up|ordered|purchased|placed)(?:_at|_on|_ts|_time|_timestamp|_date)?$",
    re.I,
)
EVENT_TS_RE = re.compile(
    r"(?:^|_)(?:event|occurred|happened|logged|recorded|emitted|sent|timestamp|ts)(?:_at|_on|_ts|_time|_timestamp|_date)?$",
    re.I,
)
EVENT_TABLE_RE = re.compile(
    r"(?:^|_)(?:events?|logs?|audit|clicks?|impressions?|telemetry|tracking|sessions?|metrics?|streams?)(?:$|_)",
    re.I,
)
STAGING_TABLE_RE = re.compile(r"^(?:stg_|staging_|raw_|tmp_|temp_|landing_|bronze_)", re.I)
REF_TABLE_RE = re.compile(
    r"(?:^|_)(?:ref|lkp|lookup|codes?|statuses?|types?|countries|currencies|regions|categories)(?:$|_)",
    re.I,
)
DIM_TABLE_RE = re.compile(
    r"(?:^dim_|^(?:users?|customers?|accounts?|products?|items?|stores?|employees?|orgs?|organizations?|vendors?|merchants?)$)",
    re.I,
)
CLUSTER_NAME_RE = re.compile(
    r"(?:^|_)(?:tenant_id|org_id|organization_id|account_id|customer_id|user_id|status|state|type|category|country|region|currency|channel|environment)(?:$|_)",
    re.I,
)


def _find_watermarks(table: TableInfo) -> list[WatermarkCandidate]:
    candidates: list[WatermarkCandidate] = []
    for col in table.columns:
        max_val = col.profile.max_value if col.profile else None
        is_temporal = col.canonical_type in {"timestamp", "date"}
        low_nulls = (col.profile.null_rate < 0.15) if col.profile else True

        if is_temporal and UPDATED_AT_RE.search(col.name):
            candidates.append(
                WatermarkCandidate(
                    column=col.name,
                    kind="updated_at",
                    confidence="high" if low_nulls else "medium",
                    reason="Mutable update timestamp suitable for incremental MERGE/UPSERT",
                    max_value=max_val,
                )
            )
        elif is_temporal and EVENT_TS_RE.search(col.name):
            candidates.append(
                WatermarkCandidate(
                    column=col.name,
                    kind="event_timestamp",
                    confidence="high" if low_nulls else "medium",
                    reason="Event timestamp suitable for time-windowed incremental append",
                    max_value=max_val,
                )
            )
        elif is_temporal and CREATED_AT_RE.search(col.name):
            candidates.append(
                WatermarkCandidate(
                    column=col.name,
                    kind="created_at",
                    confidence="high" if low_nulls else "medium",
                    reason="Creation timestamp suitable for append-only or lookback incremental loads",
                    max_value=max_val,
                )
            )
        elif (
            col.is_primary_key
            and len(table.primary_key) == 1
            and col.canonical_type == "integer"
        ):
            candidates.append(
                WatermarkCandidate(
                    column=col.name,
                    kind="monotonic_id",
                    confidence="medium",
                    reason="Single integer primary key (usable as high-watermark if strictly append-only)",
                    max_value=max_val,
                )
            )

    order = {"updated_at": 0, "event_timestamp": 1, "created_at": 2, "monotonic_id": 3}
    candidates.sort(key=lambda w: (order[w.kind], w.confidence != "high"))
    return candidates


def _find_partitions(table: TableInfo) -> list[PartitionCandidate]:
    if table.table_type != "table":
        return []
    candidates: list[PartitionCandidate] = []
    row_count = table.row_count or 0
    granularity = "day" if (row_count >= 100_000 or EVENT_TABLE_RE.search(table.name)) else "month"

    for col in table.columns:
        if col.canonical_type not in {"timestamp", "date"}:
            continue
        if col.profile and col.profile.null_rate > 0.20:
            continue
        if EVENT_TS_RE.search(col.name) or CREATED_AT_RE.search(col.name) or col.name.lower().endswith("_date"):
            candidates.append(
                PartitionCandidate(
                    column=col.name,
                    granularity=granularity,  # type: ignore[arg-type]
                    reason=f"Low-null {col.canonical_type} column ideal for {granularity}-level time partitioning",
                )
            )
    return candidates[:3]


def _find_clustering_keys(table: TableInfo) -> list[str]:
    scored: list[tuple[int, str]] = []
    for col in table.columns:
        if col.is_primary_key:
            continue
        score = 0
        if col.is_foreign_key:
            score += 3
        if CLUSTER_NAME_RE.search(col.name):
            score += 2
        if col.profile and col.profile.cardinality_class in {"low", "medium"} and col.profile.null_rate < 0.25:
            score += 1
        if score >= 3:
            scored.append((score, col.name))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [name for _, name in scored[:4]]


def _find_pk_candidates(table: TableInfo) -> list[str]:
    if table.primary_key or table.table_type != "table":
        return []
    candidates: list[tuple[int, str]] = []
    for col in table.columns:
        if col.is_unique and not col.nullable:
            candidates.append((3, col.name))
            continue
        if col.profile and col.profile.cardinality_class == "unique" and col.profile.null_count == 0 and col.profile.sample_size > 0:
            bonus = 2 if col.name.lower() == "id" or col.name.lower().endswith(("_id", "_key", "_uuid")) else 1
            candidates.append((bonus, col.name))
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return [name for _, name in candidates[:3]]


def _classify_entity(
    table: TableInfo,
    inbound_count: int,
    outbound_count: int,
    watermarks: list[WatermarkCandidate],
) -> tuple[str, str]:
    if table.table_type in {"view", "materialized_view"}:
        return "view", f"Database {table.table_type.replace('_', ' ')} derived from upstream objects"
    if STAGING_TABLE_RE.search(table.name):
        return "staging", "Table naming matches staging/raw landing pattern"

    non_key_cols = [c for c in table.columns if not c.is_primary_key and not c.is_foreign_key]
    if len(table.primary_key) >= 2 and outbound_count >= 2 and len(non_key_cols) <= 3:
        return "bridge", "Composite key connecting multiple foreign keys with minimal attributes"

    if EVENT_TABLE_RE.search(table.name):
        return "event_log", "Naming and timestamp structure match append-only event/log data"

    rows = table.row_count or 0
    numeric_measures = [
        c
        for c in table.columns
        if c.canonical_type in {"decimal", "float", "integer"}
        and not c.is_primary_key
        and not c.is_foreign_key
        and not c.name.lower().endswith(("_id", "_key", "_code", "_flag"))
    ]

    if outbound_count >= 2 and (len(numeric_measures) >= 1 or rows >= 1000):
        return (
            "fact",
            f"Has {outbound_count} outbound relationships and {len(numeric_measures)} measure column(s)",
        )
    if REF_TABLE_RE.search(table.name) and rows <= 500:
        return "reference", f"Small lookup/reference table ({rows:,} rows)"
    if inbound_count >= 1 or DIM_TABLE_RE.search(table.name):
        return (
            "dimension",
            f"Referenced by {inbound_count} downstream relationship(s) / matches entity dimension naming",
        )
    if any(w.kind == "event_timestamp" for w in watermarks) and not any(w.kind == "updated_at" for w in watermarks):
        return "event_log", "Contains event timestamp without update watermark"
    if outbound_count >= 1 and len(numeric_measures) >= 1:
        return "fact", "Links to parent entity and stores quantitative measures"
    return "dimension", "Standalone entity table"


def _choose_ingestion_strategy(
    table: TableInfo,
    entity_role: str,
    watermarks: list[WatermarkCandidate],
) -> tuple[str, str]:
    if table.table_type in {"view", "materialized_view"}:
        return "view_downstream", "Transform/rebuild as a downstream dbt model rather than ingesting directly"

    rows = table.row_count or 0
    has_updated = next((w for w in watermarks if w.kind == "updated_at"), None)
    has_append_ts = next((w for w in watermarks if w.kind in {"event_timestamp", "created_at"}), None)
    has_mono_id = next((w for w in watermarks if w.kind == "monotonic_id"), None)

    if entity_role == "reference" or (rows > 0 and rows <= 2000 and not has_updated and entity_role != "event_log"):
        return "full_refresh", f"Small table ({rows:,} rows); simple full refresh is fast and avoids watermark drift"

    if has_updated:
        return (
            "incremental_merge",
            f"Use '{has_updated.column}' watermark with MERGE/UPSERT on primary key to capture inserts and updates",
        )
    if entity_role == "event_log" and (has_append_ts or has_mono_id):
        wm = has_append_ts or has_mono_id
        assert wm is not None
        return (
            "incremental_append",
            f"Append-only pattern; incrementally load new rows using '{wm.column}'",
        )
    if has_append_ts and rows > 10_000:
        return (
            "incremental_append",
            f"Use '{has_append_ts.column}' with a lookback window (verify whether historical rows mutate)",
        )
    if rows > 50_000:
        return (
            "cdc_or_snapshot",
            "Large table without an explicit update watermark; prefer log-based CDC or periodic diff snapshots",
        )
    return "full_refresh", "No update watermark detected and table volume supports full refresh"


def analyze_tables_and_quality(
    tables: list[TableInfo],
    relationships: list[RelationshipInfo],
) -> list[QualityIssue]:
    """Generate DE recommendations, quality issues, and 0-100 health scores for every table."""
    all_issues: list[QualityIssue] = []

    inbound_map: dict[str, int] = {}
    outbound_map: dict[str, int] = {}
    for rel in relationships:
        outbound_map[rel.source_table] = outbound_map.get(rel.source_table, 0) + 1
        inbound_map[rel.target_table] = inbound_map.get(rel.target_table, 0) + 1

    for tbl in tables:
        watermarks = _find_watermarks(tbl)
        partitions = _find_partitions(tbl)
        clustering = _find_clustering_keys(tbl)
        pk_candidates = _find_pk_candidates(tbl)
        role, role_reason = _classify_entity(
            tbl,
            inbound_count=inbound_map.get(tbl.full_name, 0),
            outbound_count=outbound_map.get(tbl.full_name, 0),
            watermarks=watermarks,
        )
        strategy, strategy_reason = _choose_ingestion_strategy(tbl, role, watermarks)

        tbl.recommendations = TableRecommendations(
            entity_role=role,  # type: ignore[arg-type]
            entity_role_reason=role_reason,
            ingestion_strategy=strategy,  # type: ignore[arg-type]
            ingestion_strategy_reason=strategy_reason,
            watermark_candidates=watermarks,
            partition_candidates=partitions,
            clustering_candidates=clustering,
            primary_key_candidates=pk_candidates,
        )

        # Data quality audit
        issues: list[QualityIssue] = []
        penalty = 0

        if tbl.table_type == "table":
            if not tbl.primary_key:
                sev = "high" if (tbl.row_count or 0) > 0 else "medium"
                cand_note = (
                    f" (candidate PK: {', '.join(pk_candidates)})" if pk_candidates else ""
                )
                issues.append(
                    QualityIssue(
                        severity=sev,  # type: ignore[arg-type]
                        category="missing_pk",
                        table=tbl.full_name,
                        message=f"Table has no declared primary key{cand_note}",
                    )
                )
                penalty += 18 if sev == "high" else 10

            if tbl.row_count == 0:
                issues.append(
                    QualityIssue(
                        severity="medium",
                        category="empty_table",
                        table=tbl.full_name,
                        message="Table contains 0 rows",
                    )
                )
                penalty += 10

            # Unindexed foreign keys on non-trivial tables
            if (tbl.row_count or 0) >= 100:
                for fk in tbl.foreign_keys:
                    for fk_col in fk.constrained_columns:
                        col_obj = next((c for c in tbl.columns if c.name == fk_col), None)
                        if col_obj and not col_obj.is_indexed:
                            issues.append(
                                QualityIssue(
                                    severity="low",
                                    category="unindexed_fk",
                                    table=tbl.full_name,
                                    column=fk_col,
                                    message=f"Foreign key column '{fk_col}' has no leading index",
                                )
                            )
                            penalty += 4

        # Column-level quality & PII issues
        for col in tbl.columns:
            prof = col.profile
            if prof is None:
                continue
            if prof.profile_error:
                issues.append(
                    QualityIssue(
                        severity="medium",
                        category="profile_error",
                        table=tbl.full_name,
                        column=col.name,
                        message=prof.profile_error,
                    )
                )
                continue

            if prof.sample_size > 0:
                if prof.cardinality_class == "all_null":
                    issues.append(
                        QualityIssue(
                            severity="medium",
                            category="all_null_column",
                            table=tbl.full_name,
                            column=col.name,
                            message=f"Column '{col.name}' is 100% NULL across all profiled rows",
                        )
                    )
                    penalty += 6
                elif prof.null_rate >= 0.80:
                    issues.append(
                        QualityIssue(
                            severity="medium",
                            category="high_nulls",
                            table=tbl.full_name,
                            column=col.name,
                            message=f"Column '{col.name}' has {prof.null_rate:.1%} NULL values",
                        )
                    )
                    penalty += 4
                elif prof.null_rate >= 0.50:
                    issues.append(
                        QualityIssue(
                            severity="low",
                            category="high_nulls",
                            table=tbl.full_name,
                            column=col.name,
                            message=f"Column '{col.name}' has {prof.null_rate:.1%} NULL values",
                        )
                    )
                    penalty += 2

                if prof.cardinality_class == "constant" and prof.sample_size >= 10:
                    issues.append(
                        QualityIssue(
                            severity="low",
                            category="constant_column",
                            table=tbl.full_name,
                            column=col.name,
                            message=f"Column '{col.name}' has only 1 distinct value ({prof.min_value})",
                        )
                    )
                    penalty += 2

            if prof.is_stale and prof.freshness_age_hours is not None:
                days = prof.freshness_age_hours / 24.0
                issues.append(
                    QualityIssue(
                        severity="medium",
                        category="stale_data",
                        table=tbl.full_name,
                        column=col.name,
                        message=f"Latest value in '{col.name}' is {days:.1f} days old ({prof.freshness_max_timestamp})",
                    )
                )
                penalty += 8

            if prof.pii_findings:
                top = prof.pii_findings[0]
                sev = "high" if top.category in HIGH_RISK_CATEGORIES else "medium"
                issues.append(
                    QualityIssue(
                        severity=sev,  # type: ignore[arg-type]
                        category="pii_detected",
                        table=tbl.full_name,
                        column=col.name,
                        message=f"Potential PII ({top.category}, {top.confidence} confidence): {'; '.join(top.evidence)}",
                    )
                )
                # PII is a governance alert rather than broken data, apply small governance weight
                penalty += 3 if sev == "high" else 1

            for gov in prof.governance_findings:
                sev = "high" if gov.category == "ip_or_export_controlled" else "medium"
                issues.append(
                    QualityIssue(
                        severity=sev,  # type: ignore[arg-type]
                        category=f"governance_{gov.category}",
                        table=tbl.full_name,
                        column=col.name,
                        message=f"UC {gov.uc_policy} candidate ({gov.uc_tag_key}={gov.uc_tag_value}): {'; '.join(gov.evidence)}",
                    )
                )

            if prof.legacy_date:
                issues.append(
                    QualityIssue(
                        severity="low",
                        category="legacy_date_encoding",
                        table=tbl.full_name,
                        column=col.name,
                        message=prof.legacy_date.note,
                    )
                )

        # Surface telemetry flatlines & bad sensor quality
        if tbl.telemetry_profile:
            for fl_col in tbl.telemetry_profile.flatline_columns:
                issues.append(
                    QualityIssue(
                        severity="medium",
                        category="sensor_flatline",
                        table=tbl.full_name,
                        column=fl_col,
                        message=f"OT sensor column '{fl_col}' appears flatlined (zero variance across sampled readings)",
                    )
                )
                penalty += 4
            if tbl.telemetry_profile.bad_quality_rate and tbl.telemetry_profile.bad_quality_rate >= 0.05:
                issues.append(
                    QualityIssue(
                        severity="medium",
                        category="sensor_bad_quality",
                        table=tbl.full_name,
                        column=tbl.telemetry_profile.quality_column,
                        message=f"{tbl.telemetry_profile.bad_quality_rate:.1%} of sampled OT readings have non-GOOD quality codes",
                    )
                )

        # Surface CDC blockers & Delta Lake compatibility issues
        if tbl.cdc_readiness:
            for blocker in tbl.cdc_readiness.blockers:
                issues.append(
                    QualityIssue(
                        severity="high",
                        category="cdc_blocker",
                        table=tbl.full_name,
                        message=blocker,
                    )
                )
                penalty += 6

        if tbl.delta_compatibility:
            for d_issue in tbl.delta_compatibility.issues:
                if d_issue.severity in {"high", "medium"}:
                    issues.append(
                        QualityIssue(
                            severity=d_issue.severity,
                            category=f"delta_{d_issue.category}",
                            table=tbl.full_name,
                            column=d_issue.column,
                            message=f"{d_issue.message} Remediation: {d_issue.remediation}",
                        )
                    )
                    penalty += 5 if d_issue.severity == "high" else 2

        tbl.quality_issues = issues
        tbl.quality_score = max(0, min(100, 100 - penalty))
        all_issues.extend(issues)

    severity_rank = {"high": 0, "medium": 1, "low": 2}
    all_issues.sort(key=lambda i: (severity_rank[i.severity], i.table, i.column or ""))
    return all_issues
