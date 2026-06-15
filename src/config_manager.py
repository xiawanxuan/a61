import configparser
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DBConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str
    connect_timeout: int


@dataclass
class TimescaleConfig:
    hypertable_time_column: str
    chunk_time_interval: str
    partitioning_column: Optional[str]
    number_partitions: int
    compression: Dict[str, Any]
    retention: Dict[str, Any]


@dataclass
class SyncRules:
    column_type_mapping: Dict[str, str]
    index_sync: Dict[str, Any]
    partition_sync: Dict[str, Any]
    constraint_sync: Dict[str, Any]


@dataclass
class ExecutionOptions:
    batch_size: int
    timeout_per_ddl: int
    retry_attempts: int
    retry_interval: int
    stop_on_error: bool


@dataclass
class LoggingConfig:
    log_level: str
    log_dir: str
    log_file_prefix: str
    enable_file_logging: bool
    enable_console_logging: bool
    max_log_size_mb: int
    backup_count: int


@dataclass
class CapacityCheckConfig:
    enabled: bool
    fail_on_overflow: bool
    warning_threshold_percent: int
    error_threshold_percent: int
    max_database_size_gb: int
    max_table_size_gb: int
    max_partition_size_gb: int
    estimated_index_overhead_percent: int
    check_partition_overflow: bool
    check_database_overflow: bool
    check_table_overflow: bool
    check_chunk_growth_rate: bool
    chunk_growth_rate_days: int
    max_chunk_growth_percent: int
    estimate_new_column_size: bool
    default_column_size_bytes: Dict[str, int]
    excluded_tables_from_check: List[str]


@dataclass
class SyncConfig:
    sync_direction: str
    sync_mode: str
    auto_rollback: bool
    sync_interval_cron: str
    whitelist: Dict[str, List[str]]
    blacklist: Dict[str, List[str]]
    timescale_config: TimescaleConfig
    sync_rules: SyncRules
    execution_options: ExecutionOptions
    logging: LoggingConfig
    capacity_check: CapacityCheckConfig


class ConfigManager:
    def __init__(self, db_config_path: str, sync_rules_path: str):
        self.db_config_path = db_config_path
        self.sync_rules_path = sync_rules_path
        self._db_config: Optional[Dict[str, DBConfig]] = None
        self._sync_config: Optional[SyncConfig] = None

    def load_db_config(self) -> Dict[str, DBConfig]:
        if self._db_config is not None:
            return self._db_config

        config = configparser.ConfigParser()
        config.read(self.db_config_path)

        self._db_config = {
            "business_pg": DBConfig(
                host=config.get("business_pg", "host"),
                port=config.getint("business_pg", "port"),
                database=config.get("business_pg", "database"),
                user=config.get("business_pg", "user"),
                password=config.get("business_pg", "password"),
                schema=config.get("business_pg", "schema"),
                connect_timeout=config.getint("business_pg", "connect_timeout"),
            ),
            "timescale_db": DBConfig(
                host=config.get("timescale_db", "host"),
                port=config.getint("timescale_db", "port"),
                database=config.get("timescale_db", "database"),
                user=config.get("timescale_db", "user"),
                password=config.get("timescale_db", "password"),
                schema=config.get("timescale_db", "schema"),
                connect_timeout=config.getint("timescale_db", "connect_timeout"),
            ),
        }
        return self._db_config

    def load_sync_config(self) -> SyncConfig:
        if self._sync_config is not None:
            return self._sync_config

        with open(self.sync_rules_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._sync_config = SyncConfig(
            sync_direction=data.get("sync_direction", "bidirectional"),
            sync_mode=data.get("sync_mode", "preview"),
            auto_rollback=data.get("auto_rollback", True),
            sync_interval_cron=data.get("sync_interval_cron", "0 */6 * * *"),
            whitelist=data.get("whitelist", {"tables": [], "schemas": ["public"]}),
            blacklist=data.get("blacklist", {"tables": [], "columns": []}),
            timescale_config=TimescaleConfig(**data.get("timescale_config", {})),
            sync_rules=SyncRules(**data.get("sync_rules", {})),
            execution_options=ExecutionOptions(**data.get("execution_options", {})),
            logging=LoggingConfig(**data.get("logging", {})),
            capacity_check=CapacityCheckConfig(**data.get("capacity_check", {
                "enabled": False,
                "fail_on_overflow": True,
                "warning_threshold_percent": 80,
                "error_threshold_percent": 95,
                "max_database_size_gb": 1000,
                "max_table_size_gb": 500,
                "max_partition_size_gb": 100,
                "estimated_index_overhead_percent": 20,
                "check_partition_overflow": True,
                "check_database_overflow": True,
                "check_table_overflow": True,
                "check_chunk_growth_rate": True,
                "chunk_growth_rate_days": 7,
                "max_chunk_growth_percent": 50,
                "estimate_new_column_size": True,
                "default_column_size_bytes": {},
                "excluded_tables_from_check": [],
            })),
        )
        return self._sync_config

    def is_table_allowed(self, table_name: str) -> bool:
        sync_config = self.load_sync_config()

        for pattern in sync_config.blacklist.get("tables", []):
            if self._match_pattern(pattern, table_name):
                return False

        whitelist_tables = sync_config.whitelist.get("tables", [])
        if whitelist_tables:
            for pattern in whitelist_tables:
                if self._match_pattern(pattern, table_name):
                    return True
            return False

        return True

    def is_column_allowed(self, column_name: str) -> bool:
        sync_config = self.load_sync_config()
        for pattern in sync_config.blacklist.get("columns", []):
            if self._match_pattern(pattern, column_name):
                return False
        return True

    def _match_pattern(self, pattern: str, value: str) -> bool:
        if "*" in pattern or "%" in pattern:
            regex_pattern = pattern.replace("*", ".*").replace("%", ".*")
            return re.fullmatch(regex_pattern, value) is not None
        return pattern == value

    def get_db_connection_string(self, db_key: str) -> str:
        db_config = self.load_db_config()[db_key]
        return (
            f"host={db_config.host} port={db_config.port} "
            f"dbname={db_config.database} user={db_config.user} "
            f"password={db_config.password} connect_timeout={db_config.connect_timeout}"
        )

    def get_sqlalchemy_url(self, db_key: str) -> str:
        db_config = self.load_db_config()[db_key]
        return (
            f"postgresql+psycopg2://{db_config.user}:{db_config.password}"
            f"@{db_config.host}:{db_config.port}/{db_config.database}"
        )

    def is_table_excluded_from_capacity_check(self, table_name: str) -> bool:
        sync_config = self.load_sync_config()
        excluded_tables = sync_config.capacity_check.excluded_tables_from_check
        for pattern in excluded_tables:
            if self._match_pattern(pattern, table_name):
                return True
        return False
