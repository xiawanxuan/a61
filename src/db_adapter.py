import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Iterator, Tuple
import time
from abc import ABC, abstractmethod


class BaseDBAdapter(ABC):
    def __init__(self, db_config, logger):
        self.db_config = db_config
        self.logger = logger
        self._connection: Optional[PgConnection] = None

    @abstractmethod
    def get_connection_string(self) -> str:
        pass

    def connect(self) -> PgConnection:
        if self._connection and not self._connection.closed:
            return self._connection

        conn_str = self.get_connection_string()
        try:
            self._connection = psycopg2.connect(conn_str)
            self._connection.autocommit = False
            self.logger.info(f"Connected to {self.__class__.__name__}: {self.db_config.host}:{self.db_config.port}/{self.db_config.database}")
            return self._connection
        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise

    def disconnect(self):
        if self._connection and not self._connection.closed:
            self._connection.close()
            self.logger.info(f"Disconnected from {self.__class__.__name__}")
        self._connection = None

    @contextmanager
    def transaction(self) -> Iterator[PgConnection]:
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Transaction rolled back due to: {str(e)}")
            raise

    def execute_query(self, query: str, params: Optional[Tuple] = None, fetch: bool = True) -> List[Dict[str, Any]]:
        with self.transaction() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params or ())
                if fetch:
                    return [dict(row) for row in cur.fetchall()]
                return []

    def execute_update(self, query: str, params: Optional[Tuple] = None, timeout: int = 300) -> int:
        with self.transaction() as conn:
            conn.cursor().execute(f"SET statement_timeout = {timeout * 1000}")
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                return cur.rowcount

    def execute_ddl(self, ddl: str, timeout: int = 300) -> bool:
        conn = self.connect()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {timeout * 1000}")
                cur.execute(ddl)
            return True
        except psycopg2.Error as e:
            self.logger.error(f"DDL execution failed: {str(e)}")
            raise
        finally:
            conn.autocommit = False

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        schema = schema or self.db_config.schema
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """
        result = self.execute_query(query, (schema, table_name))
        return result[0]["exists"] if result else False

    def is_hypertable(self, table_name: str, schema: Optional[str] = None) -> bool:
        return False

    def get_database_size(self) -> Dict[str, Any]:
        query = """
            SELECT
                pg_database_size(current_database()) as size_bytes,
                pg_size_pretty(pg_database_size(current_database())) as size_pretty
        """
        result = self.execute_query(query)
        return result[0] if result else {"size_bytes": 0, "size_pretty": "0 bytes"}

    def get_table_size(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        schema = schema or self.db_config.schema
        full_table_name = f"{schema}.{table_name}"
        query = """
            SELECT
                pg_total_relation_size(%s) as total_size_bytes,
                pg_size_pretty(pg_total_relation_size(%s)) as total_size_pretty,
                pg_relation_size(%s) as table_size_bytes,
                pg_size_pretty(pg_relation_size(%s)) as table_size_pretty,
                pg_indexes_size(%s) as index_size_bytes,
                pg_size_pretty(pg_indexes_size(%s)) as index_size_pretty,
                pg_toast_relation_size(%s) as toast_size_bytes,
                pg_size_pretty(pg_toast_relation_size(%s)) as toast_size_pretty,
                c.reltuples as estimated_row_count
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s AND c.relname = %s
        """
        params = (
            full_table_name, full_table_name,
            full_table_name, full_table_name,
            full_table_name, full_table_name,
            full_table_name, full_table_name,
            schema, table_name,
        )
        result = self.execute_query(query, params)
        return result[0] if result else {}

    def get_all_tables_sizes(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        schema = schema or self.db_config.schema
        query = """
            SELECT
                c.relname as table_name,
                n.nspname as table_schema,
                pg_total_relation_size(c.oid) as total_size_bytes,
                pg_size_pretty(pg_total_relation_size(c.oid)) as total_size_pretty,
                pg_relation_size(c.oid) as table_size_bytes,
                pg_size_pretty(pg_relation_size(c.oid)) as table_size_pretty,
                pg_indexes_size(c.oid) as index_size_bytes,
                pg_size_pretty(pg_indexes_size(c.oid)) as index_size_pretty,
                c.reltuples as estimated_row_count
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s AND c.relkind = 'r'
            ORDER BY pg_total_relation_size(c.oid) DESC
        """
        return self.execute_query(query, (schema,))

    def get_partition_sizes(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        schema = schema or self.db_config.schema
        query = """
            SELECT
                nm_child.nspname as partition_schema,
                c_child.relname as partition_name,
                pg_total_relation_size(c_child.oid) as total_size_bytes,
                pg_size_pretty(pg_total_relation_size(c_child.oid)) as total_size_pretty,
                pg_relation_size(c_child.oid) as table_size_bytes,
                pg_size_pretty(pg_relation_size(c_child.oid)) as table_size_pretty,
                pg_indexes_size(c_child.oid) as index_size_bytes,
                pg_size_pretty(pg_indexes_size(c_child.oid)) as index_size_pretty,
                c_child.reltuples as estimated_row_count,
                pg_get_expr(c_child.relpartbound, c_child.oid) as partition_bound
            FROM pg_inherits i
            JOIN pg_class c_parent ON i.inhparent = c_parent.oid
            JOIN pg_class c_child ON i.inhrelid = c_child.oid
            JOIN pg_namespace nm_child ON c_child.relnamespace = nm_child.oid
            JOIN pg_namespace nm_parent ON c_parent.relnamespace = nm_parent.oid
            WHERE nm_parent.nspname = %s AND c_parent.relname = %s
            ORDER BY pg_total_relation_size(c_child.oid) DESC
        """
        return self.execute_query(query, (schema, table_name))

    def get_tablespace_info(self) -> List[Dict[str, Any]]:
        query = """
            SELECT
                spcname as tablespace_name,
                pg_tablespace_location(oid) as location,
                pg_size_pretty(pg_tablespace_size(oid)) as size_pretty,
                pg_tablespace_size(oid) as size_bytes
            FROM pg_tablespace
        """
        return self.execute_query(query)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class BusinessPGAdapter(BaseDBAdapter):
    def __init__(self, db_config, logger):
        super().__init__(db_config, logger)

    def get_connection_string(self) -> str:
        return (
            f"host={self.db_config.host} port={self.db_config.port} "
            f"dbname={self.db_config.database} user={self.db_config.user} "
            f"password={self.db_config.password} connect_timeout={self.db_config.connect_timeout}"
        )

    def get_standard_tables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        schema = schema or self.db_config.schema
        query = """
            SELECT
                t.table_name,
                t.table_schema,
                obj_description((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass) as table_comment
            FROM information_schema.tables t
            WHERE t.table_schema = %s
              AND t.table_type = 'BASE TABLE'
              AND NOT EXISTS (
                  SELECT 1 FROM timescaledb_information.hypertables h
                  WHERE h.schema = t.table_schema AND h.table_name = t.table_name
              )
            ORDER BY t.table_name
        """
        return self.execute_query(query, (schema,))


class TimescaleDBAdapter(BaseDBAdapter):
    def __init__(self, db_config, logger):
        super().__init__(db_config, logger)

    def get_connection_string(self) -> str:
        return (
            f"host={self.db_config.host} port={self.db_config.port} "
            f"dbname={self.db_config.database} user={self.db_config.user} "
            f"password={self.db_config.password} connect_timeout={self.db_config.connect_timeout}"
        )

    def is_hypertable(self, table_name: str, schema: Optional[str] = None) -> bool:
        schema = schema or self.db_config.schema
        query = """
            SELECT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables
                WHERE hypertable_schema = %s AND hypertable_name = %s
            )
        """
        result = self.execute_query(query, (schema, table_name))
        return result[0]["exists"] if result else False

    def create_hypertable(
        self,
        table_name: str,
        time_column: str,
        chunk_time_interval: str,
        schema: Optional[str] = None,
        partitioning_column: Optional[str] = None,
        number_partitions: Optional[int] = None,
    ) -> bool:
        schema = schema or self.db_config.schema
        full_table_name = f"{schema}.{table_name}"

        params = [full_table_name, time_column]
        query_parts = [
            "SELECT create_hypertable(",
            "    %s::regclass,",
            "    %s,",
            f"    chunk_time_interval => {chunk_time_interval}"
        ]

        if partitioning_column and number_partitions:
            query_parts.append(f",    partitioning_column => %s")
            query_parts.append(f",    number_partitions => %s")
            params.extend([partitioning_column, number_partitions])

        query_parts.append(")")
        query = "\n".join(query_parts)

        try:
            self.execute_ddl(query, tuple(params))
            self.logger.info(f"Created hypertable: {full_table_name}")
            return True
        except psycopg2.Error as e:
            if "already a hypertable" in str(e):
                self.logger.info(f"Table {full_table_name} is already a hypertable")
                return True
            raise

    def get_hypertables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        schema = schema or self.db_config.schema
        query = """
            SELECT
                h.hypertable_name as table_name,
                h.hypertable_schema as table_schema,
                h.time_column_name,
                h.chunk_time_interval,
                h.num_dimensions,
                obj_description((quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name))::regclass) as table_comment
            FROM timescaledb_information.hypertables h
            WHERE h.hypertable_schema = %s
            ORDER BY h.hypertable_name
        """
        return self.execute_query(query, (schema,))

    def get_chunks(self, hypertable_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        schema = schema or self.db_config.schema
        query = """
            SELECT
                c.chunk_name,
                c.chunk_schema,
                c.range_start,
                c.range_end,
                c.is_compressed,
                pg_size_pretty(pg_total_relation_size(c.chunk_schema || '.' || c.chunk_name)) as size
            FROM timescaledb_information.chunks c
            WHERE c.hypertable_schema = %s
              AND c.hypertable_name = %s
            ORDER BY c.range_start
        """
        return self.execute_query(query, (schema, hypertable_name))

    def get_compression_settings(self, hypertable_name: str, schema: Optional[str] = None) -> Optional[Dict[str, Any]]:
        schema = schema or self.db_config.schema
        query = """
            SELECT
                h.hypertable_name,
                h.hypertable_schema,
                h.compression_enabled,
                h.compress_after,
                h.segmentby,
                h.orderby
            FROM timescaledb_information.compression_settings h
            WHERE h.hypertable_schema = %s
              AND h.hypertable_name = %s
        """
        result = self.execute_query(query, (schema, hypertable_name))
        return result[0] if result else None

    def enable_compression(
        self,
        table_name: str,
        compress_after: str,
        segmentby: Optional[List[str]] = None,
        orderby: Optional[List[str]] = None,
        schema: Optional[str] = None,
    ) -> bool:
        schema = schema or self.db_config.schema
        full_table_name = f"{schema}.{table_name}"

        alter_sql = f"ALTER TABLE {full_table_name} SET (timescaledb.compress = true)"

        if segmentby:
            alter_sql += f", timescaledb.compress_segmentby = '{','.join(segmentby)}'"
        if orderby:
            alter_sql += f", timescaledb.compress_orderby = '{','.join(orderby)}'"

        self.execute_ddl(alter_sql)

        policy_sql = (
            f"SELECT add_compression_policy('{full_table_name}', {compress_after})"
        )
        self.execute_ddl(policy_sql)

        self.logger.info(f"Enabled compression for {full_table_name}")
        return True

    def set_retention_policy(self, table_name: str, drop_after: str, schema: Optional[str] = None) -> bool:
        schema = schema or self.db_config.schema
        full_table_name = f"{schema}.{table_name}"

        query = f"SELECT add_retention_policy('{full_table_name}', {drop_after})"
        self.execute_ddl(query)

        self.logger.info(f"Set retention policy for {full_table_name}: {drop_after}")
        return True

    def set_chunk_time_interval(self, table_name: str, interval: str, schema: Optional[str] = None) -> bool:
        schema = schema or self.db_config.schema
        full_table_name = f"{schema}.{table_name}"

        query = f"SELECT set_chunk_time_interval('{full_table_name}', {interval})"
        self.execute_ddl(query)

        self.logger.info(f"Set chunk time interval for {full_table_name}: {interval}")
        return True

    def get_hypertable_total_size(
        self, hypertable_name: str, schema: Optional[str] = None
    ) -> Dict[str, Any]:
        schema = schema or self.db_config.schema
        full_table_name = f"{schema}.{hypertable_name}"
        query = """
            SELECT
                h.hypertable_name,
                h.hypertable_schema,
                h.total_size as total_size_bytes,
                pg_size_pretty(h.total_size) as total_size_pretty,
                h.table_size as table_size_bytes,
                pg_size_pretty(h.table_size) as table_size_pretty,
                h.index_size as index_size_bytes,
                pg_size_pretty(h.index_size) as index_size_pretty,
                h.toast_size as toast_size_bytes,
                pg_size_pretty(h.toast_size) as toast_size_pretty,
                h.compressed_size as compressed_size_bytes,
                pg_size_pretty(h.compressed_size) as compressed_size_pretty,
                h.uncompressed_size as uncompressed_size_bytes,
                pg_size_pretty(h.uncompressed_size) as uncompressed_size_pretty,
                h.compression_ratio
            FROM timescaledb_information.hypertable_total_size h
            WHERE h.hypertable_schema = %s AND h.hypertable_name = %s
        """
        result = self.execute_query(query, (schema, hypertable_name))
        if result:
            return result[0]

        fallback_query = """
            SELECT
                %s as hypertable_name,
                %s as hypertable_schema,
                pg_total_relation_size(%s) as total_size_bytes,
                pg_size_pretty(pg_total_relation_size(%s)) as total_size_pretty,
                pg_relation_size(%s) as table_size_bytes,
                pg_size_pretty(pg_relation_size(%s)) as table_size_pretty,
                pg_indexes_size(%s) as index_size_bytes,
                pg_size_pretty(pg_indexes_size(%s)) as index_size_pretty
        """
        params = (
            hypertable_name, schema,
            full_table_name, full_table_name,
            full_table_name, full_table_name,
            full_table_name, full_table_name,
        )
        result = self.execute_query(fallback_query, params)
        return result[0] if result else {}

    def get_hypertable_detailed_sizes(
        self, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        schema = schema or self.db_config.schema
        query = """
            SELECT
                h.hypertable_name,
                h.hypertable_schema,
                h.total_size as total_size_bytes,
                pg_size_pretty(h.total_size) as total_size_pretty,
                h.table_size as table_size_bytes,
                pg_size_pretty(h.table_size) as table_size_pretty,
                h.index_size as index_size_bytes,
                pg_size_pretty(h.index_size) as index_size_pretty,
                h.compression_ratio,
                h.num_chunks
            FROM timescaledb_information.hypertable_total_size h
            WHERE h.hypertable_schema = %s
            ORDER BY h.total_size DESC
        """
        return self.execute_query(query, (schema,))

    def get_chunk_size_history(
        self,
        hypertable_name: str,
        days: int = 7,
        schema: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        schema = schema or self.db_config.schema
        query = """
            SELECT
                c.chunk_name,
                c.chunk_schema,
                c.hypertable_name,
                c.range_start,
                c.range_end,
                c.is_compressed,
                pg_total_relation_size(c.chunk_schema || '.' || c.chunk_name) as size_bytes,
                pg_size_pretty(pg_total_relation_size(c.chunk_schema || '.' || c.chunk_name)) as size_pretty,
                pg_relation_size(c.chunk_schema || '.' || c.chunk_name) as table_bytes,
                pg_indexes_size(c.chunk_schema || '.' || c.chunk_name) as index_bytes
            FROM timescaledb_information.chunks c
            WHERE c.hypertable_schema = %s
              AND c.hypertable_name = %s
              AND c.range_start >= NOW() - INTERVAL '%s days'
            ORDER BY c.range_start DESC
        """
        return self.execute_query(query, (schema, hypertable_name, days))

    def get_chunk_growth_rate(
        self,
        hypertable_name: str,
        days: int = 7,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        schema = schema or self.db_config.schema
        query = """
            WITH recent_chunks AS (
                SELECT
                    range_start,
                    range_end,
                    pg_total_relation_size(chunk_schema || '.' || chunk_name) as size_bytes,
                    (range_end - range_start) as time_range
                FROM timescaledb_information.chunks
                WHERE hypertable_schema = %s
                  AND hypertable_name = %s
                  AND range_start >= NOW() - INTERVAL '%s days'
                ORDER BY range_start
            ),
            chunk_stats AS (
                SELECT
                    COUNT(*) as chunk_count,
                    SUM(size_bytes) as total_size_bytes,
                    AVG(size_bytes) as avg_chunk_size_bytes,
                    MAX(size_bytes) as max_chunk_size_bytes,
                    MIN(size_bytes) as min_chunk_size_bytes
                FROM recent_chunks
            ),
            time_span AS (
                SELECT
                    MAX(range_end) - MIN(range_start) as total_time_span
                FROM recent_chunks
            )
            SELECT
                cs.*,
                ts.total_time_span,
                CASE
                    WHEN EXTRACT(EPOCH FROM ts.total_time_span) > 0
                    THEN cs.total_size_bytes::float / EXTRACT(EPOCH FROM ts.total_time_span) * 86400
                    ELSE 0
                END as estimated_daily_growth_bytes
            FROM chunk_stats cs, time_span ts
        """
        result = self.execute_query(query, (schema, hypertable_name, days))
        return result[0] if result else {}

    def get_disk_usage_summary(self) -> Dict[str, Any]:
        query = """
            SELECT
                SUM(pg_total_relation_size(c.oid)) as total_table_size_bytes,
                pg_size_pretty(SUM(pg_total_relation_size(c.oid))) as total_table_size_pretty,
                SUM(pg_indexes_size(c.oid)) as total_index_size_bytes,
                pg_size_pretty(SUM(pg_indexes_size(c.oid))) as total_index_size_pretty,
                COUNT(*) FILTER (WHERE c.relkind = 'r') as table_count,
                COUNT(*) FILTER (WHERE c.relkind = 'i') as index_count
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', '_timescaledb_internal')
              AND c.relkind IN ('r', 'i')
        """
        result = self.execute_query(query)
        return result[0] if result else {}


class DBAdapterFactory:
    @staticmethod
    def create_adapter(db_type: str, db_config, logger) -> BaseDBAdapter:
        if db_type == "business_pg":
            return BusinessPGAdapter(db_config, logger)
        elif db_type == "timescale_db":
            return TimescaleDBAdapter(db_config, logger)
        else:
            raise ValueError(f"Unknown database type: {db_type}")


class DualDBManager:
    def __init__(self, config_manager, logger):
        self.config_manager = config_manager
        self.logger = logger
        self.db_configs = config_manager.load_db_config()

        self.business_pg: Optional[BusinessPGAdapter] = None
        self.timescale_db: Optional[TimescaleDBAdapter] = None

    def connect_all(self):
        self.logger.info("Connecting to both databases...")

        self.business_pg = BusinessPGAdapter(
            self.db_configs["business_pg"],
            self.logger,
        )
        self.business_pg.connect()

        self.timescale_db = TimescaleDBAdapter(
            self.db_configs["timescale_db"],
            self.logger,
        )
        self.timescale_db.connect()

        self.logger.info("Both databases connected successfully")

    def disconnect_all(self):
        self.logger.info("Disconnecting from both databases...")
        if self.business_pg:
            self.business_pg.disconnect()
        if self.timescale_db:
            self.timescale_db.disconnect()
        self.logger.info("Both databases disconnected")

    def __enter__(self):
        self.connect_all()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect_all()
