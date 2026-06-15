import logging
import os
import json
import uuid
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum


class SyncPhase(str, Enum):
    METADATA_COLLECT = "metadata_collect"
    DIFF_ANALYSIS = "diff_analysis"
    DDL_GENERATION = "ddl_generation"
    DDL_EXECUTION = "ddl_execution"
    ROLLBACK = "rollback"
    COMPLETED = "completed"
    FAILED = "failed"


class OperationType(str, Enum):
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    ALTER_COLUMN_TYPE = "alter_column_type"
    ADD_INDEX = "add_index"
    DROP_INDEX = "drop_index"
    ADD_PARTITION = "add_partition"
    ALTER_PARTITION = "alter_partition"
    CREATE_HYPERTABLE = "create_hypertable"
    CREATE_TABLE = "create_table"
    ALTER_CONSTRAINT = "alter_constraint"
    SET_COMPRESSION = "set_compression"
    SET_RETENTION = "set_retention"


@dataclass
class SyncOperation:
    operation_id: str
    phase: SyncPhase
    operation_type: OperationType
    source_db: str
    target_db: str
    table_name: str
    sql_statement: str
    rollback_sql: str
    status: str = "pending"
    error_message: Optional[str] = None
    executed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None


@dataclass
class SyncSession:
    session_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    status: str = "running"
    operations: List[SyncOperation] = field(default_factory=list)
    metadata_source: Optional[Dict[str, Any]] = None
    metadata_target: Optional[Dict[str, Any]] = None
    diff_result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "status": self.status,
            "operations": [asdict(op) for op in self.operations],
            "error_message": self.error_message,
        }


class LoggerRollbackManager:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.sync_config = config_manager.load_sync_config()
        self.log_config = self.sync_config.logging
        self._logger = None
        self.current_session: Optional[SyncSession] = None
        self._rollback_stack: List[SyncOperation] = []

        self._setup_logger()
        self._ensure_log_dir()

    def _ensure_log_dir(self):
        log_dir = self.log_config.log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        rollback_dir = os.path.join(log_dir, "rollback")
        if not os.path.exists(rollback_dir):
            os.makedirs(rollback_dir, exist_ok=True)

        sessions_dir = os.path.join(log_dir, "sessions")
        if not os.path.exists(sessions_dir):
            os.makedirs(sessions_dir, exist_ok=True)

    def _setup_logger(self):
        self._logger = logging.getLogger("iot_sync")
        self._logger.setLevel(getattr(logging, self.log_config.log_level))
        self._logger.handlers.clear()

        log_format = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(session_id)s | %(phase)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        if self.log_config.enable_console_logging:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(log_format)
            self._logger.addHandler(console_handler)

        if self.log_config.enable_file_logging:
            log_file = os.path.join(
                self.log_config.log_dir,
                f"{self.log_config.log_file_prefix}{datetime.now().strftime('%Y%m%d')}.log",
            )
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=self.log_config.max_log_size_mb * 1024 * 1024,
                backupCount=self.log_config.backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(log_format)
            self._logger.addHandler(file_handler)

    def start_session(self) -> SyncSession:
        session_id = str(uuid.uuid4())
        self.current_session = SyncSession(
            session_id=session_id,
            started_at=datetime.now(),
        )
        self._rollback_stack = []
        self.log_phase(
            SyncPhase.METADATA_COLLECT,
            f"Starting sync session: {session_id}",
            level=logging.INFO,
        )
        return self.current_session

    def end_session(self, success: bool, error_message: Optional[str] = None):
        if self.current_session is None:
            return

        self.current_session.ended_at = datetime.now()
        self.current_session.status = "success" if success else "failed"
        self.current_session.error_message = error_message

        self._save_session()

        self.log_phase(
            SyncPhase.COMPLETED if success else SyncPhase.FAILED,
            f"Session {self.current_session.session_id} ended with status: {self.current_session.status}",
            level=logging.INFO,
        )

    def _save_session(self):
        if self.current_session is None:
            return

        sessions_dir = os.path.join(self.log_config.log_dir, "sessions")
        session_file = os.path.join(
            sessions_dir,
            f"session_{self.current_session.session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )
        with open(session_file, "w", encoding="utf-8") as f:
            json.dump(self.current_session.to_dict(), f, indent=2, ensure_ascii=False, default=str)

    def log_phase(self, phase: SyncPhase, message: str, level: int = logging.INFO, **kwargs):
        extra = {
            "session_id": self.current_session.session_id if self.current_session else "no_session",
            "phase": phase.value,
        }
        extra.update(kwargs)
        self._logger.log(level, message, extra=extra)

    def log_metadata(self, source_db: str, metadata: Dict[str, Any]):
        if self.current_session:
            if source_db == "business_pg":
                self.current_session.metadata_source = metadata
            else:
                self.current_session.metadata_target = metadata

        self.log_phase(
            SyncPhase.METADATA_COLLECT,
            f"Collected metadata from {source_db}: {len(metadata.get('tables', []))} tables",
            level=logging.INFO,
        )

    def log_diff_result(self, diff_result: Dict[str, Any]):
        if self.current_session:
            self.current_session.diff_result = diff_result

        total_changes = sum(
            len(v) for v in diff_result.values() if isinstance(v, list)
        )
        self.log_phase(
            SyncPhase.DIFF_ANALYSIS,
            f"Diff analysis complete: {total_changes} changes detected",
            level=logging.INFO,
        )

    def create_operation(
        self,
        operation_type: OperationType,
        source_db: str,
        target_db: str,
        table_name: str,
        sql_statement: str,
        rollback_sql: str,
    ) -> SyncOperation:
        operation = SyncOperation(
            operation_id=str(uuid.uuid4()),
            phase=SyncPhase.DDL_GENERATION,
            operation_type=operation_type,
            source_db=source_db,
            target_db=target_db,
            table_name=table_name,
            sql_statement=sql_statement,
            rollback_sql=rollback_sql,
        )
        if self.current_session:
            self.current_session.operations.append(operation)

        self.log_phase(
            SyncPhase.DDL_GENERATION,
            f"Generated DDL for {operation_type.value} on {table_name}: {sql_statement[:100]}...",
            level=logging.DEBUG,
        )
        return operation

    def mark_operation_start(self, operation: SyncOperation):
        operation.phase = SyncPhase.DDL_EXECUTION
        operation.status = "executing"
        operation.executed_at = datetime.now()

        self.log_phase(
            SyncPhase.DDL_EXECUTION,
            f"Executing operation {operation.operation_id}: {operation.operation_type.value} on {operation.table_name}",
            level=logging.INFO,
        )

    def mark_operation_success(self, operation: SyncOperation):
        operation.status = "success"
        operation.duration_ms = (
            datetime.now() - operation.executed_at
        ).total_seconds() * 1000
        self._rollback_stack.append(operation)

        self.log_phase(
            SyncPhase.DDL_EXECUTION,
            f"Operation {operation.operation_id} succeeded in {operation.duration_ms:.2f}ms",
            level=logging.INFO,
        )

    def mark_operation_failed(self, operation: SyncOperation, error: Exception):
        operation.status = "failed"
        operation.error_message = str(error)
        operation.duration_ms = (
            datetime.now() - operation.executed_at
        ).total_seconds() * 1000

        self.log_phase(
            SyncPhase.DDL_EXECUTION,
            f"Operation {operation.operation_id} failed: {str(error)}",
            level=logging.ERROR,
        )

        if self.sync_config.auto_rollback:
            self._perform_rollback()

    def _perform_rollback(self):
        self.log_phase(
            SyncPhase.ROLLBACK,
            f"Starting rollback of {len(self._rollback_stack)} operations",
            level=logging.WARNING,
        )

        while self._rollback_stack:
            operation = self._rollback_stack.pop()
            try:
                self.log_phase(
                    SyncPhase.ROLLBACK,
                    f"Rolling back operation {operation.operation_id}: {operation.rollback_sql[:100]}...",
                    level=logging.WARNING,
                )
                operation.phase = SyncPhase.ROLLBACK
                operation.status = "rollback"
            except Exception as e:
                self.log_phase(
                    SyncPhase.ROLLBACK,
                    f"Rollback failed for operation {operation.operation_id}: {str(e)}",
                    level=logging.CRITICAL,
                )

        self._save_rollback_script()

    def _save_rollback_script(self):
        if not self._rollback_stack and not self.current_session:
            return

        rollback_dir = os.path.join(self.log_config.log_dir, "rollback")
        rollback_file = os.path.join(
            rollback_dir,
            f"rollback_{self.current_session.session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
        )

        sql_statements = []
        for op in reversed(self.current_session.operations):
            if op.status == "success":
                sql_statements.append(f"-- Operation: {op.operation_type.value} on {op.table_name}")
                sql_statements.append(f"-- Original SQL: {op.sql_statement}")
                sql_statements.append(op.rollback_sql)
                sql_statements.append("")

        if sql_statements:
            with open(rollback_file, "w", encoding="utf-8") as f:
                f.write(f"-- Rollback script for session {self.current_session.session_id}\n")
                f.write(f"-- Generated at: {datetime.now().isoformat()}\n")
                f.write(f"-- Use this script to manually rollback if needed\n\n")
                f.write("\n".join(sql_statements))

            self.log_phase(
                SyncPhase.ROLLBACK,
                f"Rollback script saved to: {rollback_file}",
                level=logging.INFO,
            )

    def generate_rollback_script(self) -> str:
        if not self.current_session:
            return ""

        sql_statements = []
        for op in reversed(self.current_session.operations):
            if op.status == "success":
                sql_statements.append(f"-- Operation: {op.operation_type.value} on {op.table_name}")
                sql_statements.append(op.rollback_sql)
                sql_statements.append("")

        return "\n".join(sql_statements)

    def get_logger(self) -> logging.Logger:
        return self._logger
