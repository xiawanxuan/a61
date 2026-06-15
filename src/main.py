import os
import sys
import argparse
import json
import signal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from typing import Dict, Optional

from config_manager import ConfigManager
from logger_rollback import LoggerRollbackManager, SyncPhase
from db_adapter import DualDBManager, BaseDBAdapter
from metadata_collector import MetadataCollector, DatabaseMetadata
from diff_engine import DiffEngine, DiffResult
from ddl_executor import DDLExecutor, DDLGenerator, GeneratedDDL


class TimeSeriesSyncOrchestrator:
    def __init__(
        self,
        db_config_path: str,
        sync_rules_path: str,
        mode: Optional[str] = None,
        direction: Optional[str] = None,
    ):
        self.config_manager = ConfigManager(db_config_path, sync_rules_path)
        self.sync_config = self.config_manager.load_sync_config()

        self.mode = mode or self.sync_config.sync_mode
        self.direction = direction or self.sync_config.sync_direction

        self.logger_manager = LoggerRollbackManager(self.config_manager)
        self.metadata_collector = MetadataCollector(
            self.config_manager, self.logger_manager
        )
        self.diff_engine = DiffEngine(self.config_manager, self.logger_manager)
        self.ddl_executor = DDLExecutor(self.config_manager, self.logger_manager)
        self.ddl_generator = DDLGenerator(self.config_manager, self.logger_manager)

        self.dual_db = DualDBManager(
            self.config_manager, self.logger_manager.get_logger()
        )

        self._register_signal_handlers()

    def _register_signal_handlers(self):
        def handle_signal(signum, frame):
            self.logger_manager.log_phase(
                SyncPhase.FAILED,
                f"Received signal {signum}, shutting down gracefully...",
                level=40,
            )
            if self.sync_config.auto_rollback:
                self.logger_manager._perform_rollback()
            self.dual_db.disconnect_all()
            self.logger_manager.end_session(success=False, error_message=f"Signal {signum} received")
            sys.exit(1)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

    def run_sync(self) -> bool:
        session = self.logger_manager.start_session()
        success = False

        try:
            self.logger_manager.log_phase(
                SyncPhase.METADATA_COLLECT,
                f"Starting sync in {self.mode} mode, direction: {self.direction}",
            )

            with self.dual_db:
                metadata = self._collect_metadata()
                diff_results = self._analyze_diffs(metadata)
                ddls = self._generate_ddls(diff_results)

                if not ddls:
                    self.logger_manager.log_phase(
                        SyncPhase.COMPLETED,
                        "No DDL changes required - databases are in sync",
                    )
                    success = True
                    return success

                preview_script = self.ddl_executor.generate_preview_script(ddls)
                self._save_preview_script(preview_script, session.session_id)

                if self.mode == "preview":
                    self._print_preview(ddls, preview_script)
                    success = True
                else:
                    operations = self._execute_ddls(ddls)

                    failed_ops = [op for op in operations if op.status == "failed"]
                    success = len(failed_ops) == 0

                    if success:
                        self.logger_manager.log_phase(
                            SyncPhase.COMPLETED,
                            f"Successfully executed {len(operations)} DDL operations",
                        )
                    else:
                        self.logger_manager.log_phase(
                            SyncPhase.FAILED,
                            f"Failed operations: {len(failed_ops)} out of {len(operations)}",
                            level=40,
                        )

            self.logger_manager.end_session(success=success)
            return success

        except Exception as e:
            self.logger_manager.log_phase(
                SyncPhase.FAILED,
                f"Sync failed with error: {str(e)}",
                level=40,
            )
            self.logger_manager.end_session(success=False, error_message=str(e))
            raise

    def _collect_metadata(self) -> Dict[str, DatabaseMetadata]:
        return self.metadata_collector.collect_all(
            self.dual_db.business_pg,
            self.dual_db.timescale_db,
        )

    def _analyze_diffs(
        self, metadata: Dict[str, DatabaseMetadata]
    ) -> Dict[str, DiffResult]:
        return self.diff_engine.compare_bidirectional(
            metadata["business_pg"],
            metadata["timescale_db"],
            direction=self.direction,
        )

    def _generate_ddls(
        self, diff_results: Dict[str, DiffResult]
    ) -> list:
        return self.ddl_generator.generate_from_diff_results(diff_results)

    def _execute_ddls(self, ddls: list) -> list:
        adapters = {
            "business_pg": self.dual_db.business_pg,
            "timescale_db": self.dual_db.timescale_db,
        }
        return self.ddl_executor.execute(ddls, adapters, mode=self.mode)

    def _save_preview_script(self, script: str, session_id: str):
        log_dir = self.sync_config.logging.log_dir
        preview_dir = os.path.join(log_dir, "previews")
        os.makedirs(preview_dir, exist_ok=True)

        preview_file = os.path.join(
            preview_dir,
            f"preview_{session_id}_{self.mode}.sql",
        )

        with open(preview_file, "w", encoding="utf-8") as f:
            f.write(script)

        self.logger_manager.log_phase(
            SyncPhase.DDL_GENERATION,
            f"Preview script saved to: {preview_file}",
        )

    def _print_preview(self, ddls: list, script: str):
        print("\n" + "=" * 80)
        print(f"PREVIEW MODE - {len(ddls)} DDL Operations Generated")
        print("=" * 80)
        print()

        summary = {}
        for ddl in ddls:
            op_type = ddl.operation_type.value
            summary[op_type] = summary.get(op_type, 0) + 1

        print("Summary by operation type:")
        for op_type, count in sorted(summary.items()):
            print(f"  - {op_type}: {count}")
        print()

        print("-" * 80)
        print("Generated SQL Script:")
        print("-" * 80)
        print(script)
        print("=" * 80)
        print(f"To execute these changes, run with --mode execute")
        print("=" * 80 + "\n")

    def run_scheduled(self):
        import schedule
        import time

        cron_expr = self.sync_config.sync_interval_cron
        self.logger_manager.get_logger().info(
            f"Starting scheduled sync with cron: {cron_expr}"
        )

        def job():
            try:
                self.run_sync()
            except Exception as e:
                self.logger_manager.get_logger().error(
                    f"Scheduled sync failed: {str(e)}"
                )

        schedule.every().day.at("00:00").do(job)

        if cron_expr:
            parts = cron_expr.split()
            if len(parts) == 5:
                minute, hour, day, month, weekday = parts
                if minute != "*":
                    schedule.every().hour.at(f":{minute.zfill(2)}").do(job)
                if hour != "*" and minute == "*":
                    for h in hour.split(","):
                        schedule.every().day.at(f"{h.zfill(2)}:00").do(job)

        self.logger_manager.get_logger().info("Scheduler started. Press Ctrl+C to exit.")

        while True:
            schedule.run_pending()
            time.sleep(60)


def main():
    parser = argparse.ArgumentParser(
        description="IoT Time Series Database Sync - PostgreSQL <-> TimescaleDB"
    )

    parser.add_argument(
        "--db-config",
        default="config/db_config.ini",
        help="Path to database configuration file (default: config/db_config.ini)",
    )

    parser.add_argument(
        "--sync-rules",
        default="config/sync_rules.json",
        help="Path to sync rules configuration file (default: config/sync_rules.json)",
    )

    parser.add_argument(
        "--mode",
        choices=["preview", "execute"],
        default=None,
        help="Sync mode: preview (default) or execute",
    )

    parser.add_argument(
        "--direction",
        choices=["forward", "backward", "bidirectional"],
        default=None,
        help="Sync direction: forward (PG->TS), backward (TS->PG), bidirectional (default)",
    )

    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run in scheduled mode using cron from config",
    )

    parser.add_argument(
        "--output-json",
        help="Path to save diff results as JSON",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_config_path = os.path.join(project_root, args.db_config)
    sync_rules_path = os.path.join(project_root, args.sync_rules)

    if not os.path.exists(db_config_path):
        print(f"Error: Database config file not found: {db_config_path}")
        sys.exit(1)

    if not os.path.exists(sync_rules_path):
        print(f"Error: Sync rules file not found: {sync_rules_path}")
        sys.exit(1)

    try:
        orchestrator = TimeSeriesSyncOrchestrator(
            db_config_path=db_config_path,
            sync_rules_path=sync_rules_path,
            mode=args.mode,
            direction=args.direction,
        )

        if args.verbose:
            orchestrator.logger_manager.get_logger().setLevel(10)

        if args.schedule:
            orchestrator.run_scheduled()
        else:
            success = orchestrator.run_sync()

            if args.output_json and orchestrator.logger_manager.current_session:
                with open(args.output_json, "w", encoding="utf-8") as f:
                    json.dump(
                        orchestrator.logger_manager.current_session.to_dict(),
                        f,
                        indent=2,
                        ensure_ascii=False,
                        default=str,
                    )
                print(f"Results saved to: {args.output_json}")

            sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\nSync interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
