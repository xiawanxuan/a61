from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from metadata_collector import (
    DatabaseMetadata,
    TableMetadata,
    ColumnMetadata,
    IndexMetadata,
    PartitionMetadata,
)
from config_manager import ConfigManager
from logger_rollback import LoggerRollbackManager, SyncPhase


class DiffType(str, Enum):
    TABLE_MISSING_SOURCE = "table_missing_source"
    TABLE_MISSING_TARGET = "table_missing_target"
    COLUMN_ADDED = "column_added"
    COLUMN_DROPPED = "column_dropped"
    COLUMN_TYPE_CHANGED = "column_type_changed"
    COLUMN_NULLABLE_CHANGED = "column_nullable_changed"
    COLUMN_DEFAULT_CHANGED = "column_default_changed"
    INDEX_ADDED = "index_added"
    INDEX_DROPPED = "index_dropped"
    INDEX_DEFINITION_CHANGED = "index_definition_changed"
    PARTITION_ADDED = "partition_added"
    PARTITION_DROPPED = "partition_dropped"
    PARTITION_CONFIG_CHANGED = "partition_config_changed"
    HYPERTABLE_MISSING = "hypertable_missing"
    COMPRESSION_CONFIG_CHANGED = "compression_config_changed"
    RETENTION_CONFIG_CHANGED = "retention_config_changed"
    CHUNK_INTERVAL_CHANGED = "chunk_interval_changed"


@dataclass
class DiffItem:
    diff_type: DiffType
    table_name: str
    schema: str
    source_value: Optional[Any] = None
    target_value: Optional[Any] = None
    column_name: Optional[str] = None
    index_name: Optional[str] = None
    partition_name: Optional[str] = None
    extra_info: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "diff_type": self.diff_type.value,
            "table_name": self.table_name,
            "schema": self.schema,
            "source_value": self.source_value.to_dict() if hasattr(self.source_value, 'to_dict') else self.source_value,
            "target_value": self.target_value.to_dict() if hasattr(self.target_value, 'to_dict') else self.target_value,
            "column_name": self.column_name,
            "index_name": self.index_name,
            "partition_name": self.partition_name,
            "extra_info": self.extra_info,
        }


@dataclass
class DiffResult:
    source_db: str
    target_db: str
    direction: str
    diffs: List[DiffItem] = field(default_factory=list)
    tables_in_sync: List[str] = field(default_factory=list)
    tables_missing_source: List[str] = field(default_factory=list)
    tables_missing_target: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_db": self.source_db,
            "target_db": self.target_db,
            "direction": self.direction,
            "diffs": [d.to_dict() for d in self.diffs],
            "tables_in_sync": self.tables_in_sync,
            "tables_missing_source": self.tables_missing_source,
            "tables_missing_target": self.tables_missing_target,
            "total_diffs": len(self.diffs),
            "summary": self._get_summary(),
        }

    def _get_summary(self) -> Dict[str, int]:
        summary: Dict[str, int] = {}
        for diff in self.diffs:
            diff_type = diff.diff_type.value
            summary[diff_type] = summary.get(diff_type, 0) + 1
        return summary


class DiffEngine:
    def __init__(
        self,
        config_manager: ConfigManager,
        logger_manager: LoggerRollbackManager,
    ):
        self.config_manager = config_manager
        self.logger_manager = logger_manager
        self.sync_config = config_manager.load_sync_config()
        self.type_mapping = self.sync_config.sync_rules.column_type_mapping

    def compare_bidirectional(
        self,
        source_metadata: DatabaseMetadata,
        target_metadata: DatabaseMetadata,
        direction: str = "bidirectional",
    ) -> Dict[str, DiffResult]:
        self.logger_manager.log_phase(
            SyncPhase.DIFF_ANALYSIS,
            f"Starting bidirectional diff analysis between {source_metadata.db_type} and {target_metadata.db_type}",
        )

        results = {}

        if direction in ("forward", "bidirectional"):
            results["forward"] = self.compare_unidirectional(
                source_metadata,
                target_metadata,
                "forward",
            )

        if direction in ("backward", "bidirectional"):
            results["backward"] = self.compare_unidirectional(
                target_metadata,
                source_metadata,
                "backward",
            )

        combined_diffs = []
        if "forward" in results:
            combined_diffs.extend(results["forward"].diffs)
        if "backward" in results:
            combined_diffs.extend(results["backward"].diffs)

        self.logger_manager.log_diff_result({
            "forward": results.get("forward", {}),
            "backward": results.get("backward", {}),
            "total_diffs": len(combined_diffs),
        })

        return results

    def compare_unidirectional(
        self,
        source_metadata: DatabaseMetadata,
        target_metadata: DatabaseMetadata,
        direction: str,
    ) -> DiffResult:
        self.logger_manager.log_phase(
            SyncPhase.DIFF_ANALYSIS,
            f"Comparing {source_metadata.db_type} -> {target_metadata.db_type} ({direction})",
        )

        result = DiffResult(
            source_db=source_metadata.db_type,
            target_db=target_metadata.db_type,
            direction=direction,
        )

        source_tables = {t.table_name: t for t in source_metadata.tables}
        target_tables = {t.table_name: t for t in target_metadata.tables}

        all_table_names = set(source_tables.keys()) | set(target_tables.keys())

        for table_name in all_table_names:
            source_table = source_tables.get(table_name)
            target_table = target_tables.get(table_name)

            if source_table and not target_table:
                result.tables_missing_target.append(table_name)
                result.diffs.append(
                    DiffItem(
                        diff_type=DiffType.TABLE_MISSING_TARGET,
                        table_name=table_name,
                        schema=source_table.table_schema,
                        source_value=source_table,
                        extra_info={"is_hypertable": source_table.is_hypertable},
                    )
                )
                continue

            if target_table and not source_table:
                result.tables_missing_source.append(table_name)
                result.diffs.append(
                    DiffItem(
                        diff_type=DiffType.TABLE_MISSING_SOURCE,
                        table_name=table_name,
                        schema=target_table.table_schema,
                        target_value=target_table,
                        extra_info={"is_hypertable": target_table.is_hypertable},
                    )
                )
                continue

            if source_table and target_table:
                table_diffs = self._compare_table(
                    source_table, target_table, direction
                )
                if table_diffs:
                    result.diffs.extend(table_diffs)
                else:
                    result.tables_in_sync.append(table_name)

        self.logger_manager.log_phase(
            SyncPhase.DIFF_ANALYSIS,
            f"Direction {direction}: {len(result.diffs)} diffs found, "
            f"{len(result.tables_in_sync)} tables in sync, "
            f"{len(result.tables_missing_target)} tables missing in target, "
            f"{len(result.tables_missing_source)} tables missing in source",
        )

        return result

    def _compare_table(
        self,
        source_table: TableMetadata,
        target_table: TableMetadata,
        direction: str,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        diffs.extend(self._compare_columns(source_table, target_table))
        diffs.extend(self._compare_indexes(source_table, target_table))
        diffs.extend(self._compare_partitions(source_table, target_table, direction))
        diffs.extend(self._compare_hypertable_config(source_table, target_table, direction))

        return diffs

    def _compare_columns(
        self,
        source_table: TableMetadata,
        target_table: TableMetadata,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        source_columns = {c.column_name: c for c in source_table.columns}
        target_columns = {c.column_name: c for c in target_table.columns}

        all_columns = set(source_columns.keys()) | set(target_columns.keys())

        for col_name in all_columns:
            source_col = source_columns.get(col_name)
            target_col = target_columns.get(col_name)

            if source_col and not target_col:
                diffs.append(
                    DiffItem(
                        diff_type=DiffType.COLUMN_ADDED,
                        table_name=source_table.table_name,
                        schema=source_table.table_schema,
                        column_name=col_name,
                        source_value=source_col,
                    )
                )
                continue

            if target_col and not source_col:
                diffs.append(
                    DiffItem(
                        diff_type=DiffType.COLUMN_DROPPED,
                        table_name=source_table.table_name,
                        schema=source_table.table_schema,
                        column_name=col_name,
                        target_value=target_col,
                    )
                )
                continue

            if source_col and target_col:
                col_diffs = self._compare_single_column(
                    source_col, target_col, source_table
                )
                diffs.extend(col_diffs)

        return diffs

    def _compare_single_column(
        self,
        source_col: ColumnMetadata,
        target_col: ColumnMetadata,
        source_table: TableMetadata,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        source_type = self._normalize_type(source_col.data_type, source_col.udt_name)
        target_type = self._normalize_type(target_col.data_type, target_col.udt_name)

        mapped_source_type = self.type_mapping.get(source_type, source_type)
        mapped_target_type = self.type_mapping.get(target_type, target_type)

        if mapped_source_type != mapped_target_type:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.COLUMN_TYPE_CHANGED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    column_name=source_col.column_name,
                    source_value=source_type,
                    target_value=target_type,
                    extra_info={
                        "mapped_source_type": mapped_source_type,
                        "mapped_target_type": mapped_target_type,
                        "source_udt": source_col.udt_name,
                        "target_udt": target_col.udt_name,
                    },
                )
            )

        if source_col.is_nullable != target_col.is_nullable:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.COLUMN_NULLABLE_CHANGED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    column_name=source_col.column_name,
                    source_value=source_col.is_nullable,
                    target_value=target_col.is_nullable,
                )
            )

        if source_col.column_default != target_col.column_default:
            if not (source_col.column_default is None and target_col.column_default is None):
                diffs.append(
                    DiffItem(
                        diff_type=DiffType.COLUMN_DEFAULT_CHANGED,
                        table_name=source_table.table_name,
                        schema=source_table.table_schema,
                        column_name=source_col.column_name,
                        source_value=source_col.column_default,
                        target_value=target_col.column_default,
                    )
                )

        return diffs

    def _normalize_type(self, data_type: str, udt_name: str) -> str:
        if data_type == "USER-DEFINED":
            return udt_name
        return data_type

    def _compare_indexes(
        self,
        source_table: TableMetadata,
        target_table: TableMetadata,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        if not self.sync_config.sync_rules.index_sync.get("enabled", True):
            return diffs

        source_indexes = {i.index_name: i for i in source_table.indexes}
        target_indexes = {i.index_name: i for i in target_table.indexes}

        all_indexes = set(source_indexes.keys()) | set(target_indexes.keys())

        for idx_name in all_indexes:
            source_idx = source_indexes.get(idx_name)
            target_idx = target_indexes.get(idx_name)

            if source_idx and not target_idx:
                if self._should_include_index(source_idx):
                    diffs.append(
                        DiffItem(
                            diff_type=DiffType.INDEX_ADDED,
                            table_name=source_table.table_name,
                            schema=source_table.table_schema,
                            index_name=idx_name,
                            source_value=source_idx,
                        )
                    )
                continue

            if target_idx and not source_idx:
                if self._should_include_index(target_idx):
                    diffs.append(
                        DiffItem(
                            diff_type=DiffType.INDEX_DROPPED,
                            table_name=source_table.table_name,
                            schema=source_table.table_schema,
                            index_name=idx_name,
                            target_value=target_idx,
                        )
                    )
                continue

            if source_idx and target_idx:
                if self._indexes_differ(source_idx, target_idx):
                    diffs.append(
                        DiffItem(
                            diff_type=DiffType.INDEX_DEFINITION_CHANGED,
                            table_name=source_table.table_name,
                            schema=source_table.table_schema,
                            index_name=idx_name,
                            source_value=source_idx,
                            target_value=target_idx,
                        )
                    )

        return diffs

    def _should_include_index(self, index: IndexMetadata) -> bool:
        rules = self.sync_config.sync_rules.index_sync
        if index.is_primary and not rules.get("include_primary_key", True):
            return False
        if index.is_unique and not rules.get("include_unique", True):
            return False
        return True

    def _indexes_differ(self, idx1: IndexMetadata, idx2: IndexMetadata) -> bool:
        if idx1.index_type != idx2.index_type:
            return True
        if idx1.is_unique != idx2.is_unique:
            return True
        if idx1.columns != idx2.columns:
            return True
        if idx1.where_clause != idx2.where_clause:
            return True
        return False

    def _compare_partitions(
        self,
        source_table: TableMetadata,
        target_table: TableMetadata,
        direction: str,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        if not self.sync_config.sync_rules.partition_sync.get("enabled", True):
            return diffs

        source_part = source_table.partition_info
        target_part = target_table.partition_info

        if source_part and not target_part:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.PARTITION_ADDED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    source_value=source_part,
                )
            )
            return diffs

        if target_part and not source_part:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.PARTITION_DROPPED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    target_value=target_part,
                )
            )
            return diffs

        if source_part and target_part:
            diffs.extend(
                self._compare_partition_config(
                    source_part, target_part, source_table
                )
            )

        return diffs

    def _compare_partition_config(
        self,
        source_part: PartitionMetadata,
        target_part: PartitionMetadata,
        source_table: TableMetadata,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        if source_part.partition_type != target_part.partition_type:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.PARTITION_CONFIG_CHANGED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    source_value=source_part.partition_type,
                    target_value=target_part.partition_type,
                    extra_info={"config_type": "partition_type"},
                )
            )

        if source_part.partition_key != target_part.partition_key:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.PARTITION_CONFIG_CHANGED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    source_value=source_part.partition_key,
                    target_value=target_part.partition_key,
                    extra_info={"config_type": "partition_key"},
                )
            )

        if source_part.chunk_time_interval != target_part.chunk_time_interval:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.CHUNK_INTERVAL_CHANGED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    source_value=source_part.chunk_time_interval,
                    target_value=target_part.chunk_time_interval,
                )
            )

        return diffs

    def _compare_hypertable_config(
        self,
        source_table: TableMetadata,
        target_table: TableMetadata,
        direction: str,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        if direction == "forward" and source_table.is_hypertable and not target_table.is_hypertable:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.HYPERTABLE_MISSING,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    source_value=source_table.hypertable_info,
                    extra_info={
                        "time_column": source_table.partition_info.hypertable_time_column if source_table.partition_info else None,
                        "chunk_interval": source_table.partition_info.chunk_time_interval if source_table.partition_info else None,
                    },
                )
            )
            return diffs

        if source_table.is_hypertable and target_table.is_hypertable:
            ts_config = self.sync_config.timescale_config

            if ts_config.compression.get("enabled", False):
                diffs.extend(
                    self._compare_compression_config(
                        source_table, target_table
                    )
                )

            if ts_config.retention.get("enabled", False):
                diffs.extend(
                    self._compare_retention_config(
                        source_table, target_table
                    )
                )

        return diffs

    def _compare_compression_config(
        self,
        source_table: TableMetadata,
        target_table: TableMetadata,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        source_comp = source_table.compression_info
        target_comp = target_table.compression_info

        if not source_comp and not target_comp:
            return diffs

        ts_config = self.sync_config.timescale_config
        expected_compression = ts_config.compression

        if not target_comp or not target_comp.get("compression_enabled"):
            diffs.append(
                DiffItem(
                    diff_type=DiffType.COMPRESSION_CONFIG_CHANGED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    source_value=source_comp,
                    target_value=None,
                    extra_info={
                        "expected": expected_compression,
                        "action": "enable_compression",
                    },
                )
            )
            return diffs

        compress_after = target_comp.get("compress_after")
        expected_after = expected_compression.get("compress_after")
        if compress_after and expected_after and compress_after != expected_after:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.COMPRESSION_CONFIG_CHANGED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    source_value=compress_after,
                    target_value=expected_after,
                    extra_info={"config_type": "compress_after"},
                )
            )

        return diffs

    def _compare_retention_config(
        self,
        source_table: TableMetadata,
        target_table: TableMetadata,
    ) -> List[DiffItem]:
        diffs: List[DiffItem] = []

        ts_config = self.sync_config.timescale_config
        expected_retention = ts_config.retention.get("drop_after")

        if expected_retention:
            diffs.append(
                DiffItem(
                    diff_type=DiffType.RETENTION_CONFIG_CHANGED,
                    table_name=source_table.table_name,
                    schema=source_table.table_schema,
                    extra_info={
                        "expected_drop_after": expected_retention,
                    },
                )
            )

        return diffs
