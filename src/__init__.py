__version__ = "1.0.0"
__author__ = "IoT Time Series Sync Team"

from config_manager import ConfigManager, DBConfig, TimescaleConfig, SyncConfig, CapacityCheckConfig
from logger_rollback import (
    LoggerRollbackManager,
    SyncPhase,
    OperationType,
    SyncOperation,
    SyncSession,
)
from db_adapter import (
    BaseDBAdapter,
    BusinessPGAdapter,
    TimescaleDBAdapter,
    DBAdapterFactory,
    DualDBManager,
)
from metadata_collector import (
    MetadataCollector,
    DatabaseMetadata,
    TableMetadata,
    ColumnMetadata,
    IndexMetadata,
    ConstraintMetadata,
    PartitionMetadata,
    PartitionIndexMetadata,
)
from diff_engine import DiffEngine, DiffType, DiffItem, DiffResult
from ddl_executor import DDLGenerator, DDLExecutor, GeneratedDDL
from partition_capacity_checker import (
    PartitionCapacityChecker,
    CapacityCheckResult,
    CapacityIssue,
    IssueSeverity,
    IssueType,
    DDLType,
    DDLEstimate,
)

__all__ = [
    "ConfigManager",
    "DBConfig",
    "TimescaleConfig",
    "SyncConfig",
    "CapacityCheckConfig",
    "LoggerRollbackManager",
    "SyncPhase",
    "OperationType",
    "SyncOperation",
    "SyncSession",
    "BaseDBAdapter",
    "BusinessPGAdapter",
    "TimescaleDBAdapter",
    "DBAdapterFactory",
    "DualDBManager",
    "MetadataCollector",
    "DatabaseMetadata",
    "TableMetadata",
    "ColumnMetadata",
    "IndexMetadata",
    "ConstraintMetadata",
    "PartitionMetadata",
    "PartitionIndexMetadata",
    "DiffEngine",
    "DiffType",
    "DiffItem",
    "DiffResult",
    "DDLGenerator",
    "DDLExecutor",
    "GeneratedDDL",
    "PartitionCapacityChecker",
    "CapacityCheckResult",
    "CapacityIssue",
    "IssueSeverity",
    "IssueType",
    "DDLType",
    "DDLEstimate",
]
