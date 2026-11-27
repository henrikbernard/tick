"""Pydantic data models for tick database discovery snapshots and diffs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ProfileMode = Literal["none", "fast", "full"]
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
    profile_error: str | None = None


class ColumnInfo(BaseModel):
    name: str
    raw_type: str
    canonical_type: CanonicalType
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


class QualityIssue(BaseModel):
    severity: Severity
    category: str
    table: str
    column: str | None = None
    message: str


class TableInfo(BaseModel):
    schema_name: str
    name: str
    full_name: str
    table_type: TableType = "table"
    comment: str | None = None
    row_count: int | None = None
    row_count_exact: bool = False
    size_bytes: int | None = None
    columns: list[ColumnInfo] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list)
    indexes: list[IndexInfo] = Field(default_factory=list)
    view_definition: str | None = None
    upstream_tables: list[str] = Field(default_factory=list)
    recommendations: TableRecommendations | None = None
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


class DiscoveryConfig(BaseModel):
    profile_mode: ProfileMode = "full"
    sample_size: int = 5000
    top_k: int = 5
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
    column_count: int = 0
    total_rows: int = 0
    total_size_bytes: int | None = None
    pii_column_count: int = 0
    explicit_fk_count: int = 0
    inferred_relationship_count: int = 0
    avg_quality_score: float = 100.0


class DiscoveryResult(BaseModel):
    schema_version: str = "1.0"
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
