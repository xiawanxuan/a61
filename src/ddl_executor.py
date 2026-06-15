import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

from diff_engine import DiffItem, DiffType, DiffResult
from metadata_collector import ColumnMetadata, IndexMetadata, TableMetadata
from db_adapter import BaseDBAdapter, TimescaleDBAdapter, BusinessPGAdapter
from config_manager import ConfigManager
from logger_rollback import (
    LoggerRollbackManager,
    SyncPhase,
    OperationType,
    SyncOperation,
)


@dataclass
class GeneratedDDL:
    operation_type: OperationType
    sql: str
    rollback_sql: str
    table_name: str
    schema: str
    diff_item: DiffItem
    target_db: str
    source_db: str
    execution_order: int = 0


class DDLGenerator:
    def __init__(
        self,
        config_manager: ConfigManager,
        logger_manager: LoggerRollbackManager,
    ):
        self.config_manager = config_manager
        self.logger_manager = logger_manager
        self.sync_config = config_manager.load_sync_config()
        self.type_mapping = self.sync_config.sync_rules.column_type_mapping

    def generate_from_diff_results(
        self,
        diff_results: Dict[str, DiffResult],
    ) -> List[GeneratedDDL]:
        self.logger_manager.log_phase(
            SyncPhase.DDL_GENERATION,
            "Generating DDL statements from diff results",
        )

        all_ddls: List[GeneratedDDL] = []
        execution_order = 0

        for direction, diff_result in diff_results.items():
            target_db = diff_result.target_db
            source_db = diff_result.source_db

            for diff in diff_result.diffs:
                execution_order += 1
                ddl = self._generate_ddl_for_diff(
                    diff, source_db, target_db, execution_order
                )
                if ddl:
                    all_ddls.append(ddl)

        all_ddls.sort(key=lambda x: (x.execution_order, self._get_operation_priority(x.operation_type)))

        self.logger_manager.log_phase(
            SyncPhase.DDL_GENERATION,
            f"Generated {len(all_ddls)} DDL statements",
        )

        return all_ddls

    def _get_operation_priority(self, op_type: OperationType) -> int:
        priority_map = {
            OperationType.CREATE_TABLE: 1,
            OperationType.CREATE_HYPERTABLE: 2,
            OperationType.ADD_COLUMN: 3,
            OperationType.ALTER_COLUMN_TYPE: 4,
            OperationType.ALTER_CONSTRAINT: 5,
            OperationType.ADD_INDEX: 6,
            OperationType.ADD_PARTITION: 7,
            OperationType.ALTER_PARTITION: 8,
            OperationType.SET_COMPRESSION: 9,
            OperationType.SET_RETENTION: 10,
            OperationType.DROP_INDEX: 11,
            OperationType.DROP_COLUMN: 12,
        }
        return priority_map.get(op_type, 99)

    def _generate_ddl_for_diff(
        self,
        diff: DiffItem,
        source_db: str,
        target_db: str,
        execution_order: int,
    ) -> Optional[GeneratedDDL]:
        diff_type = diff.diff_type
        full_table_name = f"{diff.schema}.{diff.table_name}"

        handlers = {
            DiffType.TABLE_MISSING_TARGET: self._generate_create_table,
            DiffType.COLUMN_ADDED: self._generate_add_column,
            DiffType.COLUMN_DROPPED: self._generate_drop_column,
            DiffType.COLUMN_TYPE_CHANGED: self._generate_alter_column_type,
            DiffType.COLUMN_NULLABLE_CHANGED: self._generate_alter_nullable,
            DiffType.COLUMN_DEFAULT_CHANGED: self._generate_alter_default,
            DiffType.INDEX_ADDED: self._generate_create_index,
            DiffType.INDEX_DROPPED: self._generate_drop_index,
            DiffType.INDEX_DEFINITION_CHANGED: self._generate_recreate_index,
            DiffType.HYPERTABLE_MISSING: self._generate_create_hypertable,
            DiffType.CHUNK_INTERVAL_CHANGED: self._generate_alter_chunk_interval,
            DiffType.COMPRESSION_CONFIG_CHANGED: self._generate_compression_config,
            DiffType.RETENTION_CONFIG_CHANGED: self._generate_retention_config,
            DiffType.PARTITION_INDEX_ADDED: self._generate_partition_index_added,
            DiffType.PARTITION_INDEX_DROPPED: self._generate_partition_index_dropped,
            DiffType.PARTITION_INDEX_DEFINITION_CHANGED: self._generate_partition_index_changed,
            DiffType.HYPERTABLE_INDEX_PROPAGATED: self._generate_hypertable_index_propagation,
            DiffType.SPACE_PARTITION_CONFIG_CHANGED: self._generate_space_partition_config,
        }

        handler = handlers.get(diff_type)
        if not handler:
            self.logger_manager.log_phase(
                SyncPhase.DDL_GENERATION,
                f"No handler for diff type: {diff_type}",
            )
            return None

        try:
            sql, rollback_sql, op_type = handler(diff, full_table_name, target_db)
            return GeneratedDDL(
                operation_type=op_type,
                sql=sql,
                rollback_sql=rollback_sql,
                table_name=diff.table_name,
                schema=diff.schema,
                diff_item=diff,
                target_db=target_db,
                source_db=source_db,
                execution_order=execution_order,
            )
        except Exception as e:
            self.logger_manager.log_phase(
                SyncPhase.DDL_GENERATION,
                f"Failed to generate DDL for {diff_type} on {full_table_name}: {str(e)}",
                level=40,
            )
            return None

    def _generate_create_table(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        source_table: TableMetadata = diff.source_value
        columns_sql = []

        for col in source_table.columns:
            col_type = self._map_column_type(col)
            col_def = f'"{col.column_name}" {col_type}'

            if not col.is_nullable:
                col_def += " NOT NULL"

            if col.column_default:
                col_def += f" DEFAULT {col.column_default}"

            columns_sql.append(col_def)

        constraints_sql = []
        for constraint in source_table.constraints:
            if constraint.constraint_type == "PRIMARY KEY":
                constraints_sql.append(
                    f'CONSTRAINT "{constraint.constraint_name}" PRIMARY KEY ("{constraint.column_name}")'
                )
            elif constraint.constraint_type == "UNIQUE":
                constraints_sql.append(
                    f'CONSTRAINT "{constraint.constraint_name}" UNIQUE ("{constraint.column_name}")'
                )

        all_defs = columns_sql + constraints_sql
        create_sql = f'CREATE TABLE {full_table_name} (\n    ' + ',\n    '.join(all_defs) + '\n)'

        if source_table.table_comment:
            create_sql += f";\nCOMMENT ON TABLE {full_table_name} IS '{source_table.table_comment}'"

        for col in source_table.columns:
            if col.column_comment:
                create_sql += f";\nCOMMENT ON COLUMN {full_table_name}.\"{col.column_name}\" IS '{col.column_comment}'"

        rollback_sql = f'DROP TABLE IF EXISTS {full_table_name} CASCADE'

        return create_sql, rollback_sql, OperationType.CREATE_TABLE

    def _generate_add_column(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        col: ColumnMetadata = diff.source_value
        col_type = self._map_column_type(col)

        alter_sql = f'ALTER TABLE {full_table_name} ADD COLUMN "{col.column_name}" {col_type}'

        if not col.is_nullable:
            alter_sql += " NOT NULL"

        if col.column_default:
            alter_sql += f" DEFAULT {col.column_default}"

        if col.column_comment:
            alter_sql += f";\nCOMMENT ON COLUMN {full_table_name}.\"{col.column_name}\" IS '{col.column_comment}'"

        rollback_sql = f'ALTER TABLE {full_table_name} DROP COLUMN IF EXISTS "{col.column_name}" CASCADE'

        return alter_sql, rollback_sql, OperationType.ADD_COLUMN

    def _generate_drop_column(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        col: ColumnMetadata = diff.target_value
        col_type = self._map_column_type(col)

        drop_sql = f'ALTER TABLE {full_table_name} DROP COLUMN IF EXISTS "{col.column_name}" CASCADE'

        rollback_sql = f'ALTER TABLE {full_table_name} ADD COLUMN "{col.column_name}" {col_type}'
        if not col.is_nullable:
            rollback_sql += " NOT NULL"
        if col.column_default:
            rollback_sql += f" DEFAULT {col.column_default}"

        return drop_sql, rollback_sql, OperationType.DROP_COLUMN

    def _generate_alter_column_type(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        col_name = diff.column_name
        source_type = str(diff.source_value)
        target_type = str(diff.target_value)

        mapped_type = self.type_mapping.get(source_type, source_type)

        alter_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" TYPE {mapped_type} USING "{col_name}"::{mapped_type}'

        rollback_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" TYPE {target_type} USING "{col_name}"::{target_type}'

        return alter_sql, rollback_sql, OperationType.ALTER_COLUMN_TYPE

    def _generate_alter_nullable(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        col_name = diff.column_name
        source_nullable = diff.source_value

        if source_nullable:
            alter_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" DROP NOT NULL'
            rollback_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" SET NOT NULL'
        else:
            alter_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" SET NOT NULL'
            rollback_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" DROP NOT NULL'

        return alter_sql, rollback_sql, OperationType.ALTER_COLUMN_TYPE

    def _generate_alter_default(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        col_name = diff.column_name
        source_default = diff.source_value
        target_default = diff.target_value

        if source_default:
            alter_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" SET DEFAULT {source_default}'
        else:
            alter_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" DROP DEFAULT'

        if target_default:
            rollback_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" SET DEFAULT {target_default}'
        else:
            rollback_sql = f'ALTER TABLE {full_table_name} ALTER COLUMN "{col_name}" DROP DEFAULT'

        return alter_sql, rollback_sql, OperationType.ALTER_COLUMN_TYPE

    def _generate_create_index(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        index: IndexMetadata = diff.source_value
        create_sql = index.index_definition

        rollback_sql = f'DROP INDEX IF EXISTS "{diff.schema}"."{index.index_name}" CASCADE'

        return create_sql, rollback_sql, OperationType.ADD_INDEX

    def _generate_drop_index(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        index: IndexMetadata = diff.target_value

        drop_sql = f'DROP INDEX IF EXISTS "{diff.schema}"."{index.index_name}" CASCADE'

        rollback_sql = index.index_definition

        return drop_sql, rollback_sql, OperationType.DROP_INDEX

    def _generate_recreate_index(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        source_index: IndexMetadata = diff.source_value
        target_index: IndexMetadata = diff.target_value

        drop_sql = f'DROP INDEX IF EXISTS "{diff.schema}"."{target_index.index_name}" CASCADE'
        create_sql = source_index.index_definition

        full_sql = f"{drop_sql};\n{create_sql}"

        rollback_drop = f'DROP INDEX IF EXISTS "{diff.schema}"."{source_index.index_name}" CASCADE'
        rollback_create = target_index.index_definition
        rollback_sql = f"{rollback_drop};\n{rollback_create}"

        return full_sql, rollback_sql, OperationType.ADD_INDEX

    def _generate_create_hypertable(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        ts_config = self.sync_config.timescale_config
        time_column = diff.extra_info.get("time_column") or ts_config.hypertable_time_column
        chunk_interval = diff.extra_info.get("chunk_interval") or ts_config.chunk_time_interval
        partitioning_column = ts_config.partitioning_column
        number_partitions = ts_config.number_partitions

        params = [f"'{full_table_name}'", f"'{time_column}'", f"chunk_time_interval => {chunk_interval}"]
        if partitioning_column and number_partitions:
            params.extend([
                f"partitioning_column => '{partitioning_column}'",
                f"number_partitions => {number_partitions}",
            ])

        create_sql = f"SELECT create_hypertable(\n    " + ",\n    ".join(params) + "\n)"

        rollback_sql = f"-- Cannot easily rollback hypertable conversion. Manual intervention required.\n-- To undo: CREATE TABLE {full_table_name}_backup AS SELECT * FROM {full_table_name};\n-- DROP TABLE {full_table_name} CASCADE;\n-- ALTER TABLE {full_table_name}_backup RENAME TO {full_table_name.split('.')[-1]};"

        return create_sql, rollback_sql, OperationType.CREATE_HYPERTABLE

    def _generate_alter_chunk_interval(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        target_interval = diff.target_value
        source_interval = diff.source_value

        alter_sql = f"SELECT set_chunk_time_interval('{full_table_name}', {target_interval})"
        rollback_sql = f"SELECT set_chunk_time_interval('{full_table_name}', {source_interval})"

        return alter_sql, rollback_sql, OperationType.ALTER_PARTITION

    def _generate_compression_config(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        extra_info = diff.extra_info
        config_type = extra_info.get("config_type")
        source_val = diff.source_value
        target_val = diff.target_value

        if config_type == "compression_enabled":
            action = extra_info.get("action")
            if action == "enable_compression":
                ts_config = self.sync_config.timescale_config
                compression = ts_config.compression
                compress_after = compression.get("compress_after", "interval '7 days'")
                segmentby = compression.get("segmentby", [])
                orderby = compression.get("orderby", [])

                alter_sql = f'ALTER TABLE {full_table_name} SET (timescaledb.compress = true'
                if segmentby:
                    alter_sql += f", timescaledb.compress_segmentby = '{','.join(segmentby)}'"
                if orderby:
                    alter_sql += f", timescaledb.compress_orderby = '{','.join(orderby)}'"
                alter_sql += ')'

                policy_sql = f"SELECT add_compression_policy('{full_table_name}', {compress_after})"
                full_sql = f"{alter_sql};\n{policy_sql}"

                rollback_sql = f"SELECT remove_compression_policy('{full_table_name}');\nALTER TABLE {full_table_name} SET (timescaledb.compress = false)"

                return full_sql, rollback_sql, OperationType.SET_COMPRESSION
            elif action == "disable_compression":
                disable_sql = f"SELECT remove_compression_policy('{full_table_name}');\nALTER TABLE {full_table_name} SET (timescaledb.compress = false)"
                rollback_sql = f"ALTER TABLE {full_table_name} SET (timescaledb.compress = true)"
                return disable_sql, rollback_sql, OperationType.SET_COMPRESSION

        if config_type == "compress_after":
            target_after = target_val
            source_after = source_val

            alter_sql = f"SELECT remove_compression_policy('{full_table_name}');\nSELECT add_compression_policy('{full_table_name}', {target_after})"
            rollback_sql = f"SELECT remove_compression_policy('{full_table_name}');\nSELECT add_compression_policy('{full_table_name}', {source_after})"

            return alter_sql, rollback_sql, OperationType.SET_COMPRESSION

        if config_type in ("segmentby", "orderby"):
            ts_config = self.sync_config.timescale_config
            compression = ts_config.compression
            compress_after = compression.get("compress_after", "interval '7 days'")
            segmentby = compression.get("segmentby", [])
            orderby = compression.get("orderby", [])

            alter_sql = f"SELECT remove_compression_policy('{full_table_name}');"
            alter_sql += f"\nALTER TABLE {full_table_name} SET (timescaledb.compress = true"
            if segmentby:
                alter_sql += f", timescaledb.compress_segmentby = '{','.join(segmentby)}'"
            if orderby:
                alter_sql += f", timescaledb.compress_orderby = '{','.join(orderby)}'"
            alter_sql += ')'
            alter_sql += f"\nSELECT add_compression_policy('{full_table_name}', {compress_after})"

            rollback_sql = f"SELECT remove_compression_policy('{full_table_name}');"
            rollback_sql += f"\nALTER TABLE {full_table_name} RESET (timescaledb.compress)"

            return alter_sql, rollback_sql, OperationType.SET_COMPRESSION

        return "", "", OperationType.SET_COMPRESSION

    def _generate_retention_config(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        extra_info = diff.extra_info
        config_type = extra_info.get("config_type")

        if config_type == "retention_enabled":
            target_has_retention = extra_info.get("target_has_retention", False)
            if not target_has_retention:
                ts_config = self.sync_config.timescale_config
                drop_after = ts_config.retention.get("drop_after", "interval '365 days'")
                alter_sql = f"SELECT add_retention_policy('{full_table_name}', {drop_after})"
                rollback_sql = f"SELECT remove_retention_policy('{full_table_name}')"
                return alter_sql, rollback_sql, OperationType.SET_RETENTION
            else:
                alter_sql = f"SELECT remove_retention_policy('{full_table_name}')"
                rollback_sql = f"SELECT add_retention_policy('{full_table_name}', interval '365 days')"
                return alter_sql, rollback_sql, OperationType.SET_RETENTION

        if config_type == "drop_after":
            target_drop_after = diff.target_value
            source_drop_after = diff.source_value

            if target_drop_after:
                alter_sql = f"SELECT remove_retention_policy('{full_table_name}');\nSELECT add_retention_policy('{full_table_name}', {target_drop_after})"
            else:
                alter_sql = f"SELECT remove_retention_policy('{full_table_name}')"

            if source_drop_after:
                rollback_sql = f"SELECT remove_retention_policy('{full_table_name}');\nSELECT add_retention_policy('{full_table_name}', {source_drop_after})"
            else:
                rollback_sql = f"SELECT remove_retention_policy('{full_table_name}')"

            return alter_sql, rollback_sql, OperationType.SET_RETENTION

        return "", "", OperationType.SET_RETENTION

    def _generate_partition_index_added(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        extra_info = diff.extra_info
        config_type = extra_info.get("config_type")
        is_hypertable = extra_info.get("is_hypertable", False)

        if config_type == "partition_index_coverage":
            parent_index = extra_info.get("parent_index", "")
            if is_hypertable and parent_index:
                alter_sql = f"-- Index '{parent_index}' may not be fully propagated to all chunks\n"
                alter_sql += f"-- Re-creating index to ensure full propagation:\n"
                alter_sql += f"DROP INDEX IF EXISTS {diff.schema}.{parent_index} CASCADE;\n"
                alter_sql += f"-- Re-run original CREATE INDEX statement for full propagation\n"

                source_index_def = extra_info.get("source_index_def", "")
                if source_index_def:
                    alter_sql += f"{source_index_def}"

                rollback_sql = f"-- Rollback: Index propagation cannot be easily reversed\n"
                rollback_sql += f"-- Manual intervention may be required"

                return alter_sql, rollback_sql, OperationType.ADD_INDEX
            else:
                return "", "", OperationType.ADD_INDEX

        source_idx = diff.source_value
        if source_idx and hasattr(source_idx, 'index_definition'):
            create_sql = source_idx.index_definition
            drop_sql = f'DROP INDEX IF EXISTS "{diff.schema}"."{source_idx.index_name}" CASCADE'
            return create_sql, drop_sql, OperationType.ADD_INDEX

        return "", "", OperationType.ADD_INDEX

    def _generate_partition_index_dropped(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        target_idx = diff.target_value
        if target_idx and hasattr(target_idx, 'index_definition'):
            drop_sql = f'DROP INDEX IF EXISTS "{diff.schema}"."{target_idx.index_name}" CASCADE'
            create_sql = target_idx.index_definition
            return drop_sql, create_sql, OperationType.DROP_INDEX

        return "", "", OperationType.DROP_INDEX

    def _generate_partition_index_changed(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        source_idx = diff.source_value
        target_idx = diff.target_value

        if source_idx and target_idx and hasattr(source_idx, 'index_definition'):
            drop_sql = f'DROP INDEX IF EXISTS "{diff.schema}"."{target_idx.index_name}" CASCADE'
            create_sql = source_idx.index_definition
            full_sql = f"{drop_sql};\n{create_sql}"

            rollback_drop = f'DROP INDEX IF EXISTS "{diff.schema}"."{source_idx.index_name}" CASCADE'
            rollback_create = target_idx.index_definition
            rollback_sql = f"{rollback_drop};\n{rollback_create}"

            return full_sql, rollback_sql, OperationType.ADD_INDEX

        return "", "", OperationType.ADD_INDEX

    def _generate_hypertable_index_propagation(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        extra_info = diff.extra_info
        parent_index = extra_info.get("parent_index", diff.index_name)
        source_index_def = extra_info.get("source_index_def", "")

        sql_lines = [
            f"-- Hypertable index propagation check for: {parent_index}",
            f"-- Source chunks with index: {diff.source_value.get('chunk_index_count', 'N/A') if diff.source_value else 'N/A'}",
            f"-- Target chunks with index: {diff.target_value.get('chunk_index_count', 'N/A') if diff.target_value else 'N/A'}",
            f"-- Re-creating index to ensure propagation to all chunks:",
            f"DROP INDEX IF EXISTS {diff.schema}.{parent_index} CASCADE;",
        ]

        if source_index_def:
            sql_lines.append(source_index_def)
        else:
            sql_lines.append(f"-- Original index definition unavailable - manual re-creation required")

        full_sql = "\n".join(sql_lines)

        rollback_sql = (
            f"-- Rollback: Index propagation cannot be easily reversed\n"
            f"-- Manual intervention may be required to restore previous state"
        )

        return full_sql, rollback_sql, OperationType.ADD_INDEX

    def _generate_space_partition_config(
        self, diff: DiffItem, full_table_name: str, target_db: str
    ) -> Tuple[str, str, OperationType]:
        extra_info = diff.extra_info
        config_type = extra_info.get("config_type")
        source_val = diff.source_value
        target_val = diff.target_value

        sql_lines = [f"-- Space partition configuration change: {config_type}"]
        rollback_lines = [f"-- Rollback for space partition {config_type} change"]

        if config_type == "num_dimensions":
            sql_lines.append(f"-- WARNING: Changing number of dimensions requires re-creating the hypertable")
            sql_lines.append(f"-- Current dimensions: {target_val}")
            sql_lines.append(f"-- Target dimensions: {source_val}")
            sql_lines.append(f"-- Manual migration required: create_new_hypertable -> copy_data -> swap_tables")

            rollback_lines.append(f"-- Reverse migration required to restore original dimensions")

        elif config_type == "partitioning_column":
            sql_lines.append(f"-- WARNING: Changing space partitioning column requires re-creating the hypertable")
            sql_lines.append(f"-- Current column: {target_val}")
            sql_lines.append(f"-- Target column: {source_val}")
            sql_lines.append(f"-- Manual migration required")

            rollback_lines.append(f"-- Reverse migration required to restore original partitioning column")

        full_sql = "\n".join(sql_lines)
        rollback_sql = "\n".join(rollback_lines)

        return full_sql, rollback_sql, OperationType.ALTER_PARTITION

    def _map_column_type(self, col: ColumnMetadata) -> str:
        base_type = self.type_mapping.get(col.data_type, col.data_type)

        if base_type == "character varying" and col.character_maximum_length:
            return f"varchar({col.character_maximum_length})"
        elif base_type == "numeric" and col.numeric_precision and col.numeric_scale:
            return f"numeric({col.numeric_precision}, {col.numeric_scale})"
        elif base_type == "timestamp without time zone":
            return "timestamptz"

        return base_type


class DDLExecutor:
    def __init__(
        self,
        config_manager: ConfigManager,
        logger_manager: LoggerRollbackManager,
    ):
        self.config_manager = config_manager
        self.logger_manager = logger_manager
        self.sync_config = config_manager.load_sync_config()
        self.exec_options = self.sync_config.execution_options
        self.ddl_generator = DDLGenerator(config_manager, logger_manager)

    def execute(
        self,
        ddls: List[GeneratedDDL],
        adapters: Dict[str, BaseDBAdapter],
        mode: str = "preview",
    ) -> List[SyncOperation]:
        operations: List[SyncOperation] = []

        if mode == "preview":
            self.logger_manager.log_phase(
                SyncPhase.DDL_GENERATION,
                "Running in PREVIEW mode - no changes will be executed",
            )
            for ddl in ddls:
                operation = self.logger_manager.create_operation(
                    operation_type=ddl.operation_type,
                    source_db=ddl.source_db,
                    target_db=ddl.target_db,
                    table_name=ddl.table_name,
                    sql_statement=ddl.sql,
                    rollback_sql=ddl.rollback_sql,
                )
                operations.append(operation)
            return operations

        self.logger_manager.log_phase(
            SyncPhase.DDL_EXECUTION,
            f"Running in EXECUTE mode - will execute {len(ddls)} DDL statements",
        )

        for ddl in ddls:
            operation = self._execute_single_ddl(ddl, adapters)
            operations.append(operation)

            if operation.status == "failed" and self.exec_options.stop_on_error:
                self.logger_manager.log_phase(
                    SyncPhase.DDL_EXECUTION,
                    "Stopping execution due to error (stop_on_error=True)",
                    level=40,
                )
                break

        return operations

    def _execute_single_ddl(
        self,
        ddl: GeneratedDDL,
        adapters: Dict[str, BaseDBAdapter],
    ) -> SyncOperation:
        operation = self.logger_manager.create_operation(
            operation_type=ddl.operation_type,
            source_db=ddl.source_db,
            target_db=ddl.target_db,
            table_name=ddl.table_name,
            sql_statement=ddl.sql,
            rollback_sql=ddl.rollback_sql,
        )

        adapter = adapters.get(ddl.target_db)
        if not adapter:
            error = Exception(f"No adapter found for target database: {ddl.target_db}")
            self.logger_manager.mark_operation_failed(operation, error)
            return operation

        self.logger_manager.mark_operation_start(operation)

        try:
            self._execute_with_retry(adapter, ddl.sql)
            self.logger_manager.mark_operation_success(operation)
        except Exception as e:
            self.logger_manager.mark_operation_failed(operation, e)

        return operation

    def _execute_with_retry(self, adapter: BaseDBAdapter, sql: str) -> bool:
        attempts = 0
        last_error = None

        while attempts < self.exec_options.retry_attempts:
            attempts += 1
            try:
                adapter.execute_ddl(sql, timeout=self.exec_options.timeout_per_ddl)
                return True
            except Exception as e:
                last_error = e
                if attempts < self.exec_options.retry_attempts:
                    self.logger_manager.log_phase(
                        SyncPhase.DDL_EXECUTION,
                        f"DDL execution failed (attempt {attempts}/{self.exec_options.retry_attempts}). "
                        f"Retrying in {self.exec_options.retry_interval}s: {str(e)}",
                        level=30,
                    )
                    time.sleep(self.exec_options.retry_interval)

        raise last_error

    def generate_preview_script(self, ddls: List[GeneratedDDL]) -> str:
        lines = []
        lines.append("-- ========================================")
        lines.append("-- Generated DDL Sync Script")
        lines.append(f"-- Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"-- Total operations: {len(ddls)}")
        lines.append("-- ========================================")
        lines.append("")

        lines.append("-- ROLLBACK SCRIPT (run this to rollback changes):")
        lines.append("/*")
        for ddl in reversed(ddls):
            lines.append(f"-- Operation: {ddl.operation_type.value} on {ddl.schema}.{ddl.table_name}")
            lines.append(ddl.rollback_sql)
            lines.append("")
        lines.append("*/")
        lines.append("")

        lines.append("-- EXECUTION SCRIPT:")
        for i, ddl in enumerate(ddls, 1):
            lines.append(f"-- [{i}/{len(ddls)}] Operation: {ddl.operation_type.value}")
            lines.append(f"-- Target DB: {ddl.target_db}")
            lines.append(f"-- Table: {ddl.schema}.{ddl.table_name}")
            lines.append(ddl.sql)
            lines.append("")

        return "\n".join(lines)
