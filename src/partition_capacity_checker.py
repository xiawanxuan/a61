import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum

from .config_manager import CapacityCheckConfig
from .metadata_collector import MetadataCollector
from .db_adapter import BaseDBAdapter, TimescaleDBAdapter
from .diff_engine import DiffType


class IssueSeverity(str, Enum):
    WARNING = "warning"
    ERROR = "error"


class IssueType(str, Enum):
    DATABASE_OVERFLOW = "database_overflow"
    TABLE_OVERFLOW = "table_overflow"
    PARTITION_OVERFLOW = "partition_overflow"
    CHUNK_GROWTH_RATE = "chunk_growth_rate"
    INDEX_SPACE_ESTIMATE = "index_space_estimate"
    NEW_COLUMN_SPACE = "new_column_space"
    RESERVED_SPACE_INSUFFICIENT = "reserved_space_insufficient"


@dataclass
class CapacityIssue:
    severity: IssueSeverity
    issue_type: IssueType
    table_name: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    partition_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity.value,
            "issue_type": self.issue_type.value,
            "table_name": self.table_name,
            "partition_name": self.partition_name,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class CapacityCheckResult:
    has_errors: bool = False
    has_warnings: bool = False
    is_blocked: bool = False
    issues: List[CapacityIssue] = field(default_factory=list)
    current_sizes: Dict[str, Any] = field(default_factory=dict)
    estimated_impacts: Dict[str, Any] = field(default_factory=dict)

    def add_issue(self, issue: CapacityIssue):
        self.issues.append(issue)
        if issue.severity == IssueSeverity.ERROR:
            self.has_errors = True
        elif issue.severity == IssueSeverity.WARNING:
            self.has_warnings = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "has_errors": self.has_errors,
            "has_warnings": self.has_warnings,
            "is_blocked": self.is_blocked,
            "issues": [issue.to_dict() for issue in self.issues],
            "current_sizes": self.current_sizes,
            "estimated_impacts": self.estimated_impacts,
        }


class DDLType(str, Enum):
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    ALTER_COLUMN_TYPE = "alter_column_type"
    ADD_INDEX = "add_index"
    DROP_INDEX = "drop_index"
    ADD_PARTITION = "add_partition"
    CREATE_HYPERTABLE = "create_hypertable"
    CREATE_TABLE = "create_table"
    SET_COMPRESSION = "set_compression"
    SET_RETENTION = "set_retention"
    ALTER_PARTITION = "alter_partition"
    ADD_PARTITION_INDEX = "add_partition_index"
    DROP_PARTITION_INDEX = "drop_partition_index"
    ALTER_PARTITION_INDEX = "alter_partition_index"
    ALTER_HYPERTABLE_DIMENSION = "alter_hypertable_dimension"


@dataclass
class DDLEstimate:
    ddl_type: DDLType
    table_name: str
    estimated_size_change_bytes: int
    affected_partitions: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


class PartitionCapacityChecker:
    def __init__(
        self,
        target_adapter: BaseDBAdapter,
        config: CapacityCheckConfig,
        metadata_collector: MetadataCollector,
        logger: Optional[logging.Logger] = None,
    ):
        self.target_adapter = target_adapter
        self.config = config
        self.metadata_collector = metadata_collector
        self.logger = logger or logging.getLogger(__name__)
        self.schema = config.excluded_tables_from_check[0] if config.excluded_tables_from_check else "public"

    def _bytes_to_gb(self, bytes_val: float) -> float:
        return bytes_val / (1024 ** 3)

    def _gb_to_bytes(self, gb_val: float) -> float:
        return gb_val * (1024 ** 3)

    def _get_percent_used(self, current: float, max_limit: float) -> float:
        if max_limit <= 0:
            return 0.0
        return (current / max_limit) * 100

    def _is_table_excluded(self, table_name: str) -> bool:
        for pattern in self.config.excluded_tables_from_check:
            if pattern.endswith("*"):
                prefix = pattern[:-1]
                if table_name.startswith(prefix):
                    return True
            elif table_name == pattern:
                return True
        return False

    def _get_default_column_size(self, data_type: str) -> int:
        type_lower = data_type.lower()
        for type_pattern, size in self.config.default_column_size_bytes.items():
            if type_pattern in type_lower:
                return size
        return self.config.default_column_size_bytes.get("other", 256)

    def _get_estimated_row_count(self, table_name: str) -> int:
        try:
            table_size = self.target_adapter.get_table_size(table_name)
            row_count = table_size.get("estimated_row_count", 0)
            if row_count is None or row_count < 1:
                return 1000
            return int(row_count)
        except Exception as e:
            self.logger.warning(f"Failed to get row count for {table_name}: {e}")
            return 1000

    def _estimate_ddl_impact(
        self,
        diff: Dict[str, Any],
        table_metadata: Dict[str, Any],
    ) -> DDLEstimate:
        diff_type = diff.get("diff_type")
        table_name = diff.get("table_name", "")
        estimated_size_change = 0
        affected_partitions: List[str] = []
        details: Dict[str, Any] = {}

        if self._is_table_excluded(table_name):
            return DDLEstimate(
                ddl_type=DDLType.CREATE_TABLE,
                table_name=table_name,
                estimated_size_change_bytes=0,
                affected_partitions=[],
                details={"excluded": True},
            )

        row_count = self._get_estimated_row_count(table_name)
        index_overhead_multiplier = 1 + (self.config.estimated_index_overhead_percent / 100)

        if diff_type == DiffType.COLUMN_ADDED.value:
            ddl_type = DDLType.ADD_COLUMN
            col_type = diff.get("column", {}).get("data_type", "")
            col_size = self._get_default_column_size(col_type) if self.config.estimate_new_column_size else 256
            estimated_size_change = int(row_count * col_size * index_overhead_multiplier)
            details = {
                "column_name": diff.get("column", {}).get("name"),
                "data_type": col_type,
                "estimated_column_size_bytes": col_size,
                "row_count": row_count,
                "index_overhead_percent": self.config.estimated_index_overhead_percent,
            }

        elif diff_type == DiffType.COLUMN_TYPE_CHANGED.value:
            ddl_type = DDLType.ALTER_COLUMN_TYPE
            old_type = diff.get("old_column", {}).get("data_type", "")
            new_type = diff.get("new_column", {}).get("data_type", "")
            old_size = self._get_default_column_size(old_type)
            new_size = self._get_default_column_size(new_type)
            size_diff = new_size - old_size
            if size_diff > 0:
                estimated_size_change = int(row_count * size_diff * index_overhead_multiplier)
            details = {
                "column_name": diff.get("old_column", {}).get("name"),
                "old_type": old_type,
                "new_type": new_type,
                "old_size_bytes": old_size,
                "new_size_bytes": new_size,
                "row_count": row_count,
            }

        elif diff_type in (DiffType.INDEX_ADDED.value, DiffType.PARTITION_INDEX_ADDED.value):
            ddl_type = DDLType.ADD_INDEX if diff_type == DiffType.INDEX_ADDED.value else DDLType.ADD_PARTITION_INDEX
            table_size_info = self.target_adapter.get_table_size(table_name)
            table_bytes = table_size_info.get("table_size_bytes", 0)
            estimated_size_change = int(table_bytes * 0.2)
            details = {
                "index_name": diff.get("index", {}).get("name", ""),
                "index_definition": diff.get("index", {}).get("definition", ""),
                "table_size_bytes": table_bytes,
                "estimated_index_size_bytes": estimated_size_change,
            }
            partition_name = diff.get("partition_name")
            if partition_name:
                affected_partitions = [partition_name]

        elif diff_type in (DiffType.COLUMN_DROPPED.value, DiffType.INDEX_DROPPED.value,
                           DiffType.PARTITION_INDEX_DROPPED.value):
            estimated_size_change = 0
            ddl_type = DDLType.DROP_COLUMN if diff_type == DiffType.COLUMN_DROPPED.value else \
                      DDLType.DROP_INDEX if diff_type == DiffType.INDEX_DROPPED.value else \
                      DDLType.DROP_PARTITION_INDEX
            details = {"note": "Space will be reclaimed on VACUUM"}
            partition_name = diff.get("partition_name")
            if partition_name:
                affected_partitions = [partition_name]

        elif diff_type == DiffType.PARTITION_ADDED.value:
            ddl_type = DDLType.ADD_PARTITION
            estimated_size_change = int(self._gb_to_bytes(0.1))
            details = {"partition_name": diff.get("partition_name", "")}
            affected_partitions = [diff.get("partition_name", "")]

        elif diff_type in (DiffType.PARTITION_INDEX_DEFINITION_CHANGED.value,
                           DiffType.INDEX_DEFINITION_CHANGED.value):
            ddl_type = DDLType.ALTER_PARTITION_INDEX
            table_size_info = self.target_adapter.get_table_size(table_name)
            table_bytes = table_size_info.get("table_size_bytes", 0)
            estimated_size_change = int(table_bytes * 0.2)
            details = {
                "old_index": diff.get("old_index", {}),
                "new_index": diff.get("new_index", {}),
                "estimated_rebuild_size_bytes": estimated_size_change,
            }

        else:
            ddl_type = DDLType.CREATE_TABLE
            estimated_size_change = 0
            details = {"diff_type": diff_type, "note": "No size estimation for this change type"}

        return DDLEstimate(
            ddl_type=ddl_type,
            table_name=table_name,
            estimated_size_change_bytes=estimated_size_change,
            affected_partitions=affected_partitions,
            details=details,
        )

    def _check_database_overflow(
        self,
        result: CapacityCheckResult,
        current_db_size: int,
        total_estimated_growth: int,
    ):
        if not self.config.check_database_overflow:
            return

        max_db_bytes = self._gb_to_bytes(self.config.max_database_size_gb)
        projected_size = current_db_size + total_estimated_growth

        result.current_sizes["database"] = {
            "current_bytes": current_db_size,
            "max_bytes": max_db_bytes,
            "current_percent": self._get_percent_used(current_db_size, max_db_bytes),
            "projected_bytes": projected_size,
            "projected_percent": self._get_percent_used(projected_size, max_db_bytes),
        }

        projected_percent = self._get_percent_used(projected_size, max_db_bytes)
        current_percent = self._get_percent_used(current_db_size, max_db_bytes)

        if projected_percent >= self.config.error_threshold_percent:
            result.add_issue(CapacityIssue(
                severity=IssueSeverity.ERROR,
                issue_type=IssueType.DATABASE_OVERFLOW,
                table_name="*",
                message=(
                    f"Database would exceed {self.config.error_threshold_percent}% capacity "
                    f"after sync: {self._bytes_to_gb(projected_size):.2f}GB / {self.config.max_database_size_gb}GB "
                    f"({projected_percent:.1f}%)"
                ),
                details={
                    "current_size_gb": self._bytes_to_gb(current_db_size),
                    "max_size_gb": self.config.max_database_size_gb,
                    "estimated_growth_gb": self._bytes_to_gb(total_estimated_growth),
                    "projected_size_gb": self._bytes_to_gb(projected_size),
                    "projected_percent": projected_percent,
                    "error_threshold_percent": self.config.error_threshold_percent,
                },
            ))
        elif projected_percent >= self.config.warning_threshold_percent:
            result.add_issue(CapacityIssue(
                severity=IssueSeverity.WARNING,
                issue_type=IssueType.DATABASE_OVERFLOW,
                table_name="*",
                message=(
                    f"Database would exceed {self.config.warning_threshold_percent}% capacity "
                    f"after sync: {self._bytes_to_gb(projected_size):.2f}GB / {self.config.max_database_size_gb}GB "
                    f"({projected_percent:.1f}%)"
                ),
                details={
                    "current_size_gb": self._bytes_to_gb(current_db_size),
                    "max_size_gb": self.config.max_database_size_gb,
                    "estimated_growth_gb": self._bytes_to_gb(total_estimated_growth),
                    "projected_size_gb": self._bytes_to_gb(projected_size),
                    "projected_percent": projected_percent,
                    "warning_threshold_percent": self.config.warning_threshold_percent,
                },
            ))

        if current_percent >= self.config.error_threshold_percent:
            result.add_issue(CapacityIssue(
                severity=IssueSeverity.WARNING,
                issue_type=IssueType.RESERVED_SPACE_INSUFFICIENT,
                table_name="*",
                message=(
                    f"Database is already at {current_percent:.1f}% capacity, "
                    f"little headroom for new data"
                ),
                details={
                    "current_percent": current_percent,
                    "error_threshold_percent": self.config.error_threshold_percent,
                },
            ))

    def _check_table_overflow(
        self,
        result: CapacityCheckResult,
        table_name: str,
        estimated_growth: int,
    ):
        if not self.config.check_table_overflow or self._is_table_excluded(table_name):
            return

        try:
            table_size_info = self.target_adapter.get_table_size(table_name)
            current_size = table_size_info.get("total_size_bytes", 0)
        except Exception as e:
            self.logger.warning(f"Failed to get table size for {table_name}: {e}")
            return

        max_table_bytes = self._gb_to_bytes(self.config.max_table_size_gb)
        projected_size = current_size + estimated_growth

        if table_name not in result.current_sizes:
            result.current_sizes[table_name] = {}
        result.current_sizes[table_name]["table"] = {
            "current_bytes": current_size,
            "max_bytes": max_table_bytes,
            "current_percent": self._get_percent_used(current_size, max_table_bytes),
            "projected_bytes": projected_size,
            "projected_percent": self._get_percent_used(projected_size, max_table_bytes),
            "estimated_growth_bytes": estimated_growth,
        }

        projected_percent = self._get_percent_used(projected_size, max_table_bytes)

        if projected_percent >= self.config.error_threshold_percent:
            result.add_issue(CapacityIssue(
                severity=IssueSeverity.ERROR,
                issue_type=IssueType.TABLE_OVERFLOW,
                table_name=table_name,
                message=(
                    f"Table {table_name} would exceed {self.config.error_threshold_percent}% of max table size "
                    f"after sync: {self._bytes_to_gb(projected_size):.2f}GB / {self.config.max_table_size_gb}GB "
                    f"({projected_percent:.1f}%)"
                ),
                details={
                    "current_size_gb": self._bytes_to_gb(current_size),
                    "max_size_gb": self.config.max_table_size_gb,
                    "estimated_growth_gb": self._bytes_to_gb(estimated_growth),
                    "projected_size_gb": self._bytes_to_gb(projected_size),
                    "projected_percent": projected_percent,
                },
            ))
        elif projected_percent >= self.config.warning_threshold_percent:
            result.add_issue(CapacityIssue(
                severity=IssueSeverity.WARNING,
                issue_type=IssueType.TABLE_OVERFLOW,
                table_name=table_name,
                message=(
                    f"Table {table_name} would exceed {self.config.warning_threshold_percent}% of max table size "
                    f"after sync: {self._bytes_to_gb(projected_size):.2f}GB / {self.config.max_table_size_gb}GB "
                    f"({projected_percent:.1f}%)"
                ),
                details={
                    "current_size_gb": self._bytes_to_gb(current_size),
                    "max_size_gb": self.config.max_table_size_gb,
                    "estimated_growth_gb": self._bytes_to_gb(estimated_growth),
                    "projected_size_gb": self._bytes_to_gb(projected_size),
                    "projected_percent": projected_percent,
                },
            ))

    def _check_partition_overflow(
        self,
        result: CapacityCheckResult,
        table_name: str,
        estimated_growth: int,
        affected_partitions: List[str],
    ):
        if not self.config.check_partition_overflow or self._is_table_excluded(table_name):
            return

        try:
            partitions = self.target_adapter.get_partition_sizes(table_name)
        except Exception as e:
            self.logger.warning(f"Failed to get partition sizes for {table_name}: {e}")
            return

        if not partitions:
            return

        max_partition_bytes = self._gb_to_bytes(self.config.max_partition_size_gb)

        if table_name not in result.current_sizes:
            result.current_sizes[table_name] = {}
        result.current_sizes[table_name]["partitions"] = []

        partitions_to_check = partitions
        if affected_partitions:
            partitions_to_check = [p for p in partitions if p["partition_name"] in affected_partitions]
            if not partitions_to_check:
                partitions_to_check = partitions[:1]

        for partition in partitions_to_check:
            partition_name = partition["partition_name"]
            current_size = partition.get("total_size_bytes", 0)

            growth_per_partition = estimated_growth // max(len(partitions_to_check), 1)
            projected_size = current_size + growth_per_partition
            projected_percent = self._get_percent_used(projected_size, max_partition_bytes)

            result.current_sizes[table_name]["partitions"].append({
                "partition_name": partition_name,
                "partition_schema": partition.get("partition_schema"),
                "current_bytes": current_size,
                "projected_bytes": projected_size,
                "projected_percent": projected_percent,
                "partition_bound": partition.get("partition_bound"),
            })

            if projected_percent >= self.config.error_threshold_percent:
                result.add_issue(CapacityIssue(
                    severity=IssueSeverity.ERROR,
                    issue_type=IssueType.PARTITION_OVERFLOW,
                    table_name=table_name,
                    partition_name=partition_name,
                    message=(
                        f"Partition {partition_name} of table {table_name} would exceed "
                        f"{self.config.error_threshold_percent}% of max partition size "
                        f"after sync: {self._bytes_to_gb(projected_size):.2f}GB / {self.config.max_partition_size_gb}GB "
                        f"({projected_percent:.1f}%)"
                    ),
                    details={
                        "current_size_gb": self._bytes_to_gb(current_size),
                        "max_size_gb": self.config.max_partition_size_gb,
                        "estimated_growth_gb": self._bytes_to_gb(growth_per_partition),
                        "projected_size_gb": self._bytes_to_gb(projected_size),
                        "projected_percent": projected_percent,
                        "partition_bound": partition.get("partition_bound"),
                    },
                ))
            elif projected_percent >= self.config.warning_threshold_percent:
                result.add_issue(CapacityIssue(
                    severity=IssueSeverity.WARNING,
                    issue_type=IssueType.PARTITION_OVERFLOW,
                    table_name=table_name,
                    partition_name=partition_name,
                    message=(
                        f"Partition {partition_name} of table {table_name} would exceed "
                        f"{self.config.warning_threshold_percent}% of max partition size "
                        f"after sync: {self._bytes_to_gb(projected_size):.2f}GB / {self.config.max_partition_size_gb}GB "
                        f"({projected_percent:.1f}%)"
                    ),
                    details={
                        "current_size_gb": self._bytes_to_gb(current_size),
                        "max_size_gb": self.config.max_partition_size_gb,
                        "estimated_growth_gb": self._bytes_to_gb(growth_per_partition),
                        "projected_size_gb": self._bytes_to_gb(projected_size),
                        "projected_percent": projected_percent,
                        "partition_bound": partition.get("partition_bound"),
                    },
                ))

    def _check_chunk_growth_rate(
        self,
        result: CapacityCheckResult,
        table_name: str,
    ):
        if not self.config.check_chunk_growth_rate:
            return

        if not isinstance(self.target_adapter, TimescaleDBAdapter):
            return

        try:
            if not self.target_adapter.is_hypertable(table_name):
                return
        except Exception:
            return

        try:
            growth_stats = self.target_adapter.get_chunk_growth_rate(
                table_name,
                days=self.config.chunk_growth_rate_days,
            )
        except Exception as e:
            self.logger.warning(f"Failed to get chunk growth rate for {table_name}: {e}")
            return

        if not growth_stats or growth_stats.get("chunk_count", 0) < 2:
            return

        avg_chunk_size = growth_stats.get("avg_chunk_size_bytes", 0)
        daily_growth = growth_stats.get("estimated_daily_growth_bytes", 0)
        max_partition_bytes = self._gb_to_bytes(self.config.max_partition_size_gb)

        if table_name not in result.current_sizes:
            result.current_sizes[table_name] = {}
        result.current_sizes[table_name]["chunk_growth"] = {
            "avg_chunk_size_bytes": avg_chunk_size,
            "estimated_daily_growth_bytes": daily_growth,
            "chunk_count": growth_stats.get("chunk_count"),
            "max_chunk_size_bytes": growth_stats.get("max_chunk_size_bytes"),
            "min_chunk_size_bytes": growth_stats.get("min_chunk_size_bytes"),
            "total_size_bytes": growth_stats.get("total_size_bytes"),
            "days_analyzed": self.config.chunk_growth_rate_days,
        }

        if avg_chunk_size > 0:
            growth_percent = (daily_growth / avg_chunk_size) * 100

            if growth_percent >= self.config.max_chunk_growth_percent:
                result.add_issue(CapacityIssue(
                    severity=IssueSeverity.WARNING,
                    issue_type=IssueType.CHUNK_GROWTH_RATE,
                    table_name=table_name,
                    message=(
                        f"Hypertable {table_name} chunk growth rate is {growth_percent:.1f}% per day, "
                        f"exceeds {self.config.max_chunk_growth_percent}% threshold. "
                        f"Estimated daily growth: {self._bytes_to_gb(daily_growth):.3f}GB"
                    ),
                    details={
                        "avg_chunk_size_gb": self._bytes_to_gb(avg_chunk_size),
                        "daily_growth_gb": self._bytes_to_gb(daily_growth),
                        "growth_percent_per_day": growth_percent,
                        "max_growth_percent": self.config.max_chunk_growth_percent,
                        "days_analyzed": self.config.chunk_growth_rate_days,
                        "chunk_count": growth_stats.get("chunk_count"),
                    },
                ))

    def check_capacity(
        self,
        diffs: List[Dict[str, Any]],
        direction: str,
    ) -> CapacityCheckResult:
        result = CapacityCheckResult()

        if not self.config.enabled or len(diffs) == 0:
            self.logger.info("Capacity check skipped: disabled or no changes")
            return result

        self.logger.info(f"Starting capacity check for {len(diffs)} diffs, direction={direction}")

        try:
            db_size_info = self.target_adapter.get_database_size()
            current_db_size = db_size_info.get("size_bytes", 0)
        except Exception as e:
            self.logger.warning(f"Failed to get database size: {e}")
            current_db_size = 0

        total_estimated_growth = 0
        table_estimated_growth: Dict[str, int] = {}
        table_affected_partitions: Dict[str, List[str]] = {}
        ddl_estimates: List[Dict[str, Any]] = []

        target_metadata = None
        try:
            target_metadata = self.metadata_collector.collect(
                self.target_adapter,
                "target",
            )
        except Exception as e:
            self.logger.warning(f"Failed to collect target metadata for capacity check: {e}")

        for diff in diffs:
            try:
                estimate = self._estimate_ddl_impact(diff, target_metadata or {})
                ddl_estimates.append({
                    "diff_type": diff.get("diff_type"),
                    "table_name": estimate.table_name,
                    "ddl_type": estimate.ddl_type.value,
                    "estimated_size_change_bytes": estimate.estimated_size_change_bytes,
                    "affected_partitions": estimate.affected_partitions,
                    "details": estimate.details,
                })

                if estimate.estimated_size_change_bytes > 0 and not estimate.details.get("excluded"):
                    total_estimated_growth += estimate.estimated_size_change_bytes
                    table_estimated_growth[estimate.table_name] = (
                        table_estimated_growth.get(estimate.table_name, 0)
                        + estimate.estimated_size_change_bytes
                    )
                    if estimate.affected_partitions:
                        if estimate.table_name not in table_affected_partitions:
                            table_affected_partitions[estimate.table_name] = []
                        table_affected_partitions[estimate.table_name].extend(estimate.affected_partitions)

            except Exception as e:
                self.logger.error(f"Failed to estimate DDL impact for diff {diff}: {e}")
                continue

        result.estimated_impacts = {
            "total_estimated_growth_bytes": total_estimated_growth,
            "total_estimated_growth_gb": self._bytes_to_gb(total_estimated_growth),
            "table_breakdown": table_estimated_growth,
            "ddl_estimates": ddl_estimates,
        }

        self._check_database_overflow(result, current_db_size, total_estimated_growth)

        for table_name, estimated_growth in table_estimated_growth.items():
            if self._is_table_excluded(table_name):
                continue

            self._check_table_overflow(result, table_name, estimated_growth)

            affected_partitions = table_affected_partitions.get(table_name, [])
            self._check_partition_overflow(result, table_name, estimated_growth, affected_partitions)

            self._check_chunk_growth_rate(result, table_name)

        if result.has_errors and self.config.fail_on_overflow:
            result.is_blocked = True
            self.logger.error(
                f"Capacity check blocked sync: {len(result.issues)} issues found, "
                f"{sum(1 for i in result.issues if i.severity == IssueSeverity.ERROR)} errors"
            )
        elif result.has_warnings:
            self.logger.warning(
                f"Capacity check completed with warnings: "
                f"{sum(1 for i in result.issues if i.severity == IssueSeverity.WARNING)} warnings"
            )
        else:
            self.logger.info("Capacity check passed: no issues detected")

        return result
