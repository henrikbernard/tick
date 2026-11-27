"""Pydantic data models for tick database discovery, Databricks readiness, and harmonization."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ProfileMode = Literal["none", "fast", "full"]
TargetPlatform = Literal["databricks", "snowflake", "both"]
CanonicalType = Literal[
    "integer",
    "float",
    "decimal",
    "string",
    "boolean",
    "date",
    "timestamp",
    "time",
    "json",
    "binary",
    "uuid",
    "other",
]
Severity = Literal["high", "medium", "low"]
TableType = Literal["table", "view", "materialized_view"]
RelationshipKind = Literal["explicit_fk", "inferred_name_and_values", "inferred_name"]


class PiiFinding(BaseModel):
    category: str
    confidence: Literal["high", "medium", "low"]
    score: float = Field(ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list)
    sample_match_rate: float | None = None


class GovernanceFinding(BaseModel):
    category: Literal["plant_isolation_key", "commercial_sensitive", "ip_or_export_controlled"]
    confidence: Literal["high", "medium", "low"]
    uc_policy: Literal["row_filter", "column_mask", "uc_tag"]
    uc_tag_key: str
    uc_tag_value: str
    evidence: list[str] = Field(default_factory=list)


class LegacyDateEncoding(BaseModel):
    encoding: Literal[
        "yyyymmdd_int",
        "yyyymmdd_str",
        "julian_cyyddd",
        "julian_yyyyddd",
        "excel_serial",
        "hhmmss",
    ]
    confidence: Literal["high", "medium", "low"]
    sample_match_rate: float = Field(ge=0.0, le=1.0)
    paired_column: str | None = None
    spark_expression: str
    note: str = ""


class ValueFrequency(BaseModel):
    value: str
    count: int
    rate: float


class ColumnProfile(BaseModel):
    sample_size: int = 0
    non_null_count: int = 0
    null_count: int = 0
    null_rate: float = 0.0
    distinct_count: int | None = None
    distinct_rate: float | None = None
    cardinality_class: str = "unknown"
    min_value: str | None = None
    max_value: str | None = None
    mean: float | None = None
    stddev: float | None = None
    p25: float | None = None
    p50: float | None = None
    p75: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None
    top_values: list[ValueFrequency] = Field(default_factory=list)
    sample_values: list[str] = Field(default_factory=list)
    freshness_max_timestamp: str | None = None
    freshness_age_hours: float | None = None
    is_stale: bool = False
    pii_findings: list[PiiFinding] = Field(default_factory=list)
    governance_findings: list[GovernanceFinding] = Field(default_factory=list)
    legacy_date: LegacyDateEncoding | None = None
    profile_error: str | None = None


class ColumnInfo(BaseModel):
    name: str
    raw_type: str
    canonical_type: CanonicalType
    spark_type: str | None = None
    snowflake_type: str | None = None
    semantic_name: str | None = None
    business_description: str | None = None
    nullable: bool = True
    default: str | None = None
    comment: str | None = None
    ordinal_position: int = 0
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_unique: bool = False
    is_indexed: bool = False
    profile: ColumnProfile | None = None


class ForeignKeyInfo(BaseModel):
    name: str | None = None
    constrained_columns: list[str]
    referred_schema: str | None = None
    referred_table: str
    referred_columns: list[str]
    on_delete: str | None = None
    on_update: str | None = None


class IndexInfo(BaseModel):
    name: str
    columns: list[str]
    unique: bool = False
    primary: bool = False


class WatermarkCandidate(BaseModel):
    column: str
    kind: Literal["updated_at", "created_at", "event_timestamp", "monotonic_id"]
    confidence: Literal["high", "medium", "low"]
    reason: str
    max_value: str | None = None


class PartitionCandidate(BaseModel):
    column: str
    granularity: Literal["day", "month", "year"]
    reason: str


class TableRecommendations(BaseModel):
    entity_role: Literal[
        "fact",
        "dimension",
        "event_log",
        "bridge",
        "reference",
        "staging",
        "view",
        "unknown",
    ] = "unknown"
    entity_role_reason: str = ""
    ingestion_strategy: Literal[
        "incremental_merge",
        "incremental_append",
        "full_refresh",
        "cdc_or_snapshot",
        "view_downstream",
    ] = "full_refresh"
    ingestion_strategy_reason: str = ""
    watermark_candidates: list[WatermarkCandidate] = Field(default_factory=list)
    partition_candidates: list[PartitionCandidate] = Field(default_factory=list)
    clustering_candidates: list[str] = Field(default_factory=list)
    primary_key_candidates: list[str] = Field(default_factory=list)


class CdcReadiness(BaseModel):
    status: Literal["ready", "needs_config", "blocked", "not_applicable"] = "needs_config"
    connector_recommendation: str = "watermark_incremental"
    source_cdc_enabled: bool | None = None
    source_change_tracking_enabled: bool | None = None
    has_primary_key: bool = False
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class DeltaCompatibilityIssue(BaseModel):
    severity: Severity
    category: Literal[
        "illegal_column_name",
        "decimal_precision_overflow",
        "unsupported_type",
        "timezone_naive_timestamp",
        "money_type",
        "over_partitioning",
    ]
    column: str | None = None
    message: str
    remediation: str


class DeltaCompatibility(BaseModel):
    is_compatible: bool = True
    requires_column_mapping: bool = False
    liquid_clustering_keys: list[str] = Field(default_factory=list)
    enable_change_data_feed: bool = True
    enable_deletion_vectors: bool = True
    issues: list[DeltaCompatibilityIssue] = Field(default_factory=list)


class SnowflakeCompatibilityIssue(BaseModel):
    severity: Severity
    category: Literal[
        "quoted_identifier",
        "decimal_precision_overflow",
        "unsupported_type",
        "timezone_naive_timestamp",
        "variant_size_limit",
        "clustering_advisory",
    ]
    column: str | None = None
    message: str
    remediation: str


class SnowflakeCompatibility(BaseModel):
    is_compatible: bool = True
    requires_quoted_identifiers: bool = False
    clustering_keys: list[str] = Field(default_factory=list)
    enable_change_tracking: bool = True
    recommended_warehouse_size: str = "XSMALL"
    issues: list[SnowflakeCompatibilityIssue] = Field(default_factory=list)


class TelemetryProfile(BaseModel):
    pattern: Literal["narrow_historian_eav", "wide_plc_polling"]
    tag_column: str | None = None
    timestamp_column: str
    value_columns: list[str] = Field(default_factory=list)
    quality_column: str | None = None
    median_interval_seconds: float | None = None
    max_gap_seconds: float | None = None
    flatline_columns: list[str] = Field(default_factory=list)
    bad_quality_rate: float | None = None
    recommended_watermark_delay: str = "15 minutes"
    recommended_liquid_cluster: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class HierarchyInfo(BaseModel):
    pattern: Literal["bom", "lot_genealogy", "work_order_tree", "asset_or_org_tree"]
    parent_column: str
    child_column: str
    overlap_rate: float | None = None
    reason: str


class IngestionSizing(BaseModel):
    estimated_row_width_bytes: int = 0
    estimated_size_mb: float = 0.0
    estimated_daily_rows: float | None = None
    estimated_daily_mb: float | None = None
    recommended_cadence: Literal[
        "continuous_streaming",
        "micro_batch_15m",
        "hourly_batch",
        "daily_batch",
        "weekly_full_refresh",
        "view_downstream",
    ] = "daily_batch"
    recommended_databricks_tier: str = "Serverless Lakeflow Pipeline"
    notes: list[str] = Field(default_factory=list)


class QualityIssue(BaseModel):
    severity: Severity
    category: str
    table: str
    column: str | None = None
    message: str


class FileSourceInfo(BaseModel):
    path: str
    file_format: Literal["parquet", "csv", "tsv", "json", "jsonl", "delta"]
    file_count: int = 1
    total_bytes: int = 0
    compression: str | None = None
    delimiter: str | None = None
    has_header: bool | None = None
    hive_partitioned: bool = False
    partition_columns: list[str] = Field(default_factory=list)


class TableInfo(BaseModel):
    schema_name: str
    name: str
    full_name: str
    table_type: TableType = "table"
    file_source: FileSourceInfo | None = None
    semantic_name: str | None = None
    business_description: str | None = None
    comment: str | None = None
    row_count: int | None = None
    row_count_exact: bool = False
    size_bytes: int | None = None
    skipped_profiling_reason: str | None = None
    columns: list[ColumnInfo] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list)
    indexes: list[IndexInfo] = Field(default_factory=list)
    view_definition: str | None = None
    upstream_tables: list[str] = Field(default_factory=list)
    recommendations: TableRecommendations | None = None
    cdc_readiness: CdcReadiness | None = None
    delta_compatibility: DeltaCompatibility | None = None
    snowflake_compatibility: SnowflakeCompatibility | None = None
    telemetry_profile: TelemetryProfile | None = None
    hierarchy: HierarchyInfo | None = None
    ingestion_sizing: IngestionSizing | None = None
    quality_issues: list[QualityIssue] = Field(default_factory=list)
    quality_score: int = 100


class RelationshipInfo(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    kind: RelationshipKind
    confidence: float = Field(ge=0.0, le=1.0)
    overlap_rate: float | None = None
    evidence: list[str] = Field(default_factory=list)


class LineageEdge(BaseModel):
    upstream_table: str
    downstream_table: str
    edge_type: Literal["view_dependency", "foreign_key"]
    details: str | None = None


class SchemaSummary(BaseModel):
    name: str
    table_count: int = 0
    view_count: int = 0
    total_rows: int = 0
    total_size_bytes: int | None = None


class CdcDatabaseAudit(BaseModel):
    dialect: str
    db_cdc_enabled: bool | None = None
    db_change_tracking_enabled: bool | None = None
    wal_level: str | None = None
    supplemental_logging: str | None = None
    binlog_format: str | None = None
    notes: list[str] = Field(default_factory=list)


class DiscoveryConfig(BaseModel):
    profile_mode: ProfileMode = "full"
    sample_size: int = Field(default=5000, ge=1)
    top_k: int = Field(default=5, ge=1)
    include_schemas: list[str] = Field(default_factory=list)
    exclude_schemas: list[str] = Field(default_factory=list)
    include_tables: list[str] = Field(default_factory=list)
    exclude_tables: list[str] = Field(default_factory=list)
    include_views: bool = True
    detect_pii: bool = True
    redact_pii: bool = False
    infer_relationships: bool = True
    verify_relationship_values: bool = True
    stale_after_days: int = 30
    max_tables: int | None = None
    exact_row_counts: bool = False
    # Production safety guardrails
    read_uncommitted: bool = False
    query_timeout_seconds: int | None = None
    skip_empty_tables: bool = False
    skip_tables_over_mb: float | None = None
    use_tablesample: bool = False
    workers: int = Field(default=1, ge=1, le=32)
    # Manufacturing, Databricks & Snowflake extensions
    target_platform: TargetPlatform = "both"
    detect_manufacturing: bool = True
    detect_databricks_compat: bool = True
    detect_snowflake_compat: bool = True
    audit_cdc: bool = True
    glossary_path: str | None = None
    databricks_catalog: str = "manufacturing"
    databricks_schema: str = "bronze"
    snowflake_database: str = "MANUFACTURING"
    snowflake_schema: str = "BRONZE"
    snowflake_warehouse: str = "TRANSFORMING_WH"


class DatabaseOverview(BaseModel):
    dialect: str
    driver: str
    database_name: str
    server_version: str | None = None
    sanitized_url: str
    discovered_at: str
    duration_seconds: float = 0.0
    schema_count: int = 0
    table_count: int = 0
    view_count: int = 0
    empty_table_count: int = 0
    column_count: int = 0
    total_rows: int = 0
    total_size_bytes: int | None = None
    pii_column_count: int = 0
    governance_column_count: int = 0
    explicit_fk_count: int = 0
    inferred_relationship_count: int = 0
    cdc_ready_table_count: int = 0
    cdc_blocked_table_count: int = 0
    delta_warning_count: int = 0
    snowflake_warning_count: int = 0
    avg_quality_score: float = 100.0
    cdc_audit: CdcDatabaseAudit | None = None


class DiscoveryResult(BaseModel):
    schema_version: str = "1.1"
    config: DiscoveryConfig
    overview: DatabaseOverview
    schemas: list[SchemaSummary] = Field(default_factory=list)
    tables: list[TableInfo] = Field(default_factory=list)
    relationships: list[RelationshipInfo] = Field(default_factory=list)
    lineage: list[LineageEdge] = Field(default_factory=list)
    quality_issues: list[QualityIssue] = Field(default_factory=list)


class ColumnChange(BaseModel):
    table: str
    column: str
    change_type: Literal["added", "removed", "type_changed", "nullability_changed"]
    before: str | None = None
    after: str | None = None
    breaking: bool = False


class MetricDrift(BaseModel):
    table: str
    column: str | None = None
    metric: str
    before: float | int | None = None
    after: float | int | None = None
    delta: float | None = None
    pct_change: float | None = None
    severity: Severity = "low"


class SnapshotDiff(BaseModel):
    before_discovered_at: str
    after_discovered_at: str
    before_database: str
    after_database: str
    added_tables: list[str] = Field(default_factory=list)
    removed_tables: list[str] = Field(default_factory=list)
    column_changes: list[ColumnChange] = Field(default_factory=list)
    primary_key_changes: list[str] = Field(default_factory=list)
    new_pii_columns: list[str] = Field(default_factory=list)
    metric_drifts: list[MetricDrift] = Field(default_factory=list)
    breaking_changes_count: int = 0
    has_breaking_changes: bool = False


class HarmonizedColumn(BaseModel):
    name: str
    sanitized_name: str
    present_in_sources: list[str] = Field(default_factory=list)
    missing_in_sources: list[str] = Field(default_factory=list)
    source_column_names: dict[str, str] = Field(default_factory=dict)
    matched_via_glossary: bool = False
    types_by_source: dict[str, str] = Field(default_factory=dict)
    canonical_types_by_source: dict[str, str] = Field(default_factory=dict)
    target_spark_type: str
    target_snowflake_type: str | None = None
    has_type_conflict: bool = False
    nullable_in_target: bool = True
    cast_required_sources: list[str] = Field(default_factory=list)


class HarmonizedTable(BaseModel):
    table_name: str
    present_in_sources: list[str] = Field(default_factory=list)
    missing_in_sources: list[str] = Field(default_factory=list)
    source_table_names: dict[str, str] = Field(default_factory=dict)
    matched_via_glossary: bool = False
    primary_keys_by_source: dict[str, list[str]] = Field(default_factory=dict)
    pk_divergent: bool = False
    columns: list[HarmonizedColumn] = Field(default_factory=list)
    conflict_count: int = 0
    site_specific_column_count: int = 0
    unified_sql: str = ""


class HarmonizationReport(BaseModel):
    target_platform: Literal["databricks", "snowflake"] = "databricks"
    glossary_matching_enabled: bool = False
    sources: list[str] = Field(default_factory=list)
    total_unique_tables: int = 0
    fully_aligned_tables: int = 0
    tables_with_drift: int = 0
    tables: list[HarmonizedTable] = Field(default_factory=list)
