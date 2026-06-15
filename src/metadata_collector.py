from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import re

from db_adapter import BaseDBAdapter, BusinessPGAdapter, TimescaleDBAdapter
from config_manager import ConfigManager
from logger_rollback import LoggerRollbackManager, SyncPhase


@dataclass
class ColumnMetadata:
    column_name: str
    data_type: str
    udt_name: str
    is_nullable: bool
    column_default: Optional[str]
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    ordinal_position: int
    column_comment: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "column_name": self.column_name,
            "data_type": self.data_type,
            "udt_name": self.udt_name,
            "is_nullable": self.is_nullable,
            "column_default": self.column_default,
            "character_maximum_length": self.character_maximum_length,
            "numeric_precision": self.numeric_precision,
            "numeric_scale": self.numeric_scale,
            "ordinal_position": self.ordinal_position,
            "column_comment": self.column_comment,
        }


@dataclass
class IndexMetadata:
    index_name: str
    index_definition: str
    index_type: str
    is_unique: bool
    is_primary: bool
    columns: List[str]
    where_clause: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index_name": self.index_name,
            "index_definition": self.index_definition,
            "index_type": self.index_type,
            "is_unique": self.is_unique,
            "is_primary": self.is_primary,
            "columns": self.columns,
            "where_clause": self.where_clause,
        }


@dataclass
class ConstraintMetadata:
    constraint_name: str
    constraint_type: str
    column_name: Optional[str]
    foreign_table_schema: Optional[str]
    foreign_table_name: Optional[str]
    foreign_column_name: Optional[str]
    check_clause: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "constraint_name": self.constraint_name,
            "constraint_type": self.constraint_type,
            "column_name": self.column_name,
            "foreign_table_schema": self.foreign_table_schema,
            "foreign_table_name": self.foreign_table_name,
            "foreign_column_name": self.foreign_column_name,
            "check_clause": self.check_clause,
        }


@dataclass
class PartitionIndexMetadata:
    partition_name: str
    partition_schema: str
    index_name: str
    index_definition: str
    index_type: str
    is_unique: bool
    parent_index_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partition_name": self.partition_name,
            "partition_schema": self.partition_schema,
            "index_name": self.index_name,
            "index_definition": self.index_definition,
            "index_type": self.index_type,
            "is_unique": self.is_unique,
            "parent_index_name": self.parent_index_name,
        }


@dataclass
class PartitionMetadata:
    partition_type: str
    partition_key: List[str]
    partitions: List[Dict[str, Any]] = field(default_factory=list)
    partition_indexes: List[PartitionIndexMetadata] = field(default_factory=list)
    chunk_time_interval: Optional[str] = None
    is_hypertable: bool = False
    hypertable_time_column: Optional[str] = None
    num_dimensions: Optional[int] = None
    partitioning_column: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partition_type": self.partition_type,
            "partition_key": self.partition_key,
            "partitions": self.partitions,
            "partition_indexes": [pi.to_dict() for pi in self.partition_indexes],
            "chunk_time_interval": self.chunk_time_interval,
            "is_hypertable": self.is_hypertable,
            "hypertable_time_column": self.hypertable_time_column,
            "num_dimensions": self.num_dimensions,
            "partitioning_column": self.partitioning_column,
        }


@dataclass
class TableMetadata:
    table_name: str
    table_schema: str
    table_type: str
    columns: List[ColumnMetadata]
    indexes: List[IndexMetadata]
    constraints: List[ConstraintMetadata]
    partition_info: Optional[PartitionMetadata] = None
    table_comment: Optional[str] = None
    is_hypertable: bool = False
    hypertable_info: Optional[Dict[str, Any]] = None
    compression_info: Optional[Dict[str, Any]] = None
    chunks: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "table_schema": self.table_schema,
            "table_type": self.table_type,
            "columns": [c.to_dict() for c in self.columns],
            "indexes": [i.to_dict() for i in self.indexes],
            "constraints": [c.to_dict() for c in self.constraints],
            "partition_info": self.partition_info.to_dict() if self.partition_info else None,
            "table_comment": self.table_comment,
            "is_hypertable": self.is_hypertable,
            "hypertable_info": self.hypertable_info,
            "compression_info": self.compression_info,
            "chunks": self.chunks,
        }


@dataclass
class DatabaseMetadata:
    db_type: str
    tables: List[TableMetadata]
    schemas: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "db_type": self.db_type,
            "tables": [t.to_dict() for t in self.tables],
            "schemas": self.schemas,
        }


class MetadataCollector:
    def __init__(
        self,
        config_manager: ConfigManager,
        logger_manager: LoggerRollbackManager,
    ):
        self.config_manager = config_manager
        self.logger_manager = logger_manager
        self.sync_config = config_manager.load_sync_config()

    def collect_all(
        self,
        business_pg: BusinessPGAdapter,
        timescale_db: TimescaleDBAdapter,
    ) -> Dict[str, DatabaseMetadata]:
        self.logger_manager.log_phase(
            SyncPhase.METADATA_COLLECT,
            "Starting metadata collection from both databases",
        )

        business_metadata = self.collect_from_business_pg(business_pg)
        timescale_metadata = self.collect_from_timescale(timescale_db)

        self.logger_manager.log_metadata("business_pg", business_metadata.to_dict())
        self.logger_manager.log_metadata("timescale_db", timescale_metadata.to_dict())

        return {
            "business_pg": business_metadata,
            "timescale_db": timescale_metadata,
        }

    def collect_from_business_pg(
        self, adapter: BusinessPGAdapter
    ) -> DatabaseMetadata:
        self.logger_manager.log_phase(
            SyncPhase.METADATA_COLLECT,
            "Collecting metadata from Business PostgreSQL",
        )

        schemas = self._get_schemas(adapter)
        tables: List[TableMetadata] = []

        for schema in self.sync_config.whitelist.get("schemas", ["public"]):
            table_list = self._get_tables(adapter, schema)
            for table_info in table_list:
                table_name = table_info["table_name"]
                if not self.config_manager.is_table_allowed(table_name):
                    continue

                table_metadata = self._collect_table_metadata(
                    adapter, table_name, schema, is_timescale=False
                )
                table_metadata.table_comment = table_info.get("table_comment")
                tables.append(table_metadata)

        return DatabaseMetadata(
            db_type="business_pg",
            tables=tables,
            schemas=schemas,
        )

    def collect_from_timescale(
        self, adapter: TimescaleDBAdapter
    ) -> DatabaseMetadata:
        self.logger_manager.log_phase(
            SyncPhase.METADATA_COLLECT,
            "Collecting metadata from TimescaleDB",
        )

        schemas = self._get_schemas(adapter)
        tables: List[TableMetadata] = []

        for schema in self.sync_config.whitelist.get("schemas", ["public"]):
            standard_tables = self._get_tables(adapter, schema)
            for table_info in standard_tables:
                table_name = table_info["table_name"]
                if not self.config_manager.is_table_allowed(table_name):
                    continue

                table_metadata = self._collect_table_metadata(
                    adapter, table_name, schema, is_timescale=True
                )
                table_metadata.table_comment = table_info.get("table_comment")
                tables.append(table_metadata)

            hypertables = adapter.get_hypertables(schema)
            for hyper_info in hypertables:
                table_name = hyper_info["table_name"]
                if not self.config_manager.is_table_allowed(table_name):
                    continue

                table_metadata = self._collect_hypertable_metadata(
                    adapter, hyper_info, schema
                )
                tables.append(table_metadata)

        return DatabaseMetadata(
            db_type="timescale_db",
            tables=tables,
            schemas=schemas,
        )

    def _get_schemas(self, adapter: BaseDBAdapter) -> List[str]:
        query = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'timescaledb_information', '_timescaledb_internal')
            ORDER BY schema_name
        """
        result = adapter.execute_query(query)
        return [row["schema_name"] for row in result]

    def _get_tables(self, adapter: BaseDBAdapter, schema: str) -> List[Dict[str, Any]]:
        query = """
            SELECT
                t.table_name,
                t.table_schema,
                t.table_type,
                obj_description((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass) as table_comment
            FROM information_schema.tables t
            WHERE t.table_schema = %s
              AND t.table_type = 'BASE TABLE'
              AND NOT EXISTS (
                  SELECT 1 FROM timescaledb_information.hypertables h
                  WHERE h.hypertable_schema = t.table_schema AND h.hypertable_name = t.table_name
              )
            ORDER BY t.table_name
        """
        return adapter.execute_query(query, (schema,))

    def _collect_table_metadata(
        self,
        adapter: BaseDBAdapter,
        table_name: str,
        schema: str,
        is_timescale: bool,
    ) -> TableMetadata:
        self.logger_manager.log_phase(
            SyncPhase.METADATA_COLLECT,
            f"Collecting metadata for table: {schema}.{table_name}",
        )

        columns = self._get_columns(adapter, table_name, schema)
        indexes = self._get_indexes(adapter, table_name, schema)
        constraints = self._get_constraints(adapter, table_name, schema)
        partition_info = self._get_partition_info(adapter, table_name, schema)

        return TableMetadata(
            table_name=table_name,
            table_schema=schema,
            table_type="BASE TABLE",
            columns=columns,
            indexes=indexes,
            constraints=constraints,
            partition_info=partition_info,
            is_hypertable=False,
        )

    def _collect_hypertable_metadata(
        self,
        adapter: TimescaleDBAdapter,
        hyper_info: Dict[str, Any],
        schema: str,
    ) -> TableMetadata:
        table_name = hyper_info["table_name"]

        self.logger_manager.log_phase(
            SyncPhase.METADATA_COLLECT,
            f"Collecting hypertable metadata: {schema}.{table_name}",
        )

        columns = self._get_columns(adapter, table_name, schema)
        indexes = self._get_indexes(adapter, table_name, schema)
        constraints = self._get_constraints(adapter, table_name, schema)

        partition_key = [hyper_info["time_column_name"]]
        num_dimensions = hyper_info.get("num_dimensions", 1)
        partitioning_column = None

        if num_dimensions > 1:
            dims_query = """
                SELECT
                    column_name,
                    interval_length,
                    num_partitions
                FROM timescaledb_information.dimensions
                WHERE hypertable_schema = %s
                  AND hypertable_name = %s
                  AND dimension_type = 'Space'
                ORDER BY dimension_number
            """
            try:
                dims_result = adapter.execute_query(dims_query, (schema, table_name))
                for dim in dims_result:
                    partitioning_column = dim["column_name"]
                    partition_key.append(dim["column_name"])
            except Exception:
                pass

        partition_indexes = self._get_partition_indexes(
            adapter, table_name, schema, is_hypertable=True
        )

        partition_info = PartitionMetadata(
            partition_type="timescale_hypertable",
            partition_key=partition_key,
            chunk_time_interval=str(hyper_info["chunk_time_interval"]),
            is_hypertable=True,
            hypertable_time_column=hyper_info["time_column_name"],
            num_dimensions=num_dimensions,
            partitioning_column=partitioning_column,
            partition_indexes=partition_indexes,
        )

        chunks = adapter.get_chunks(table_name, schema)
        compression_info = adapter.get_compression_settings(table_name, schema)

        return TableMetadata(
            table_name=table_name,
            table_schema=schema,
            table_type="HYPERTABLE",
            columns=columns,
            indexes=indexes,
            constraints=constraints,
            partition_info=partition_info,
            table_comment=hyper_info.get("table_comment"),
            is_hypertable=True,
            hypertable_info={
                "time_column_name": hyper_info["time_column_name"],
                "chunk_time_interval": str(hyper_info["chunk_time_interval"]),
                "num_dimensions": num_dimensions,
                "partitioning_column": partitioning_column,
            },
            compression_info=compression_info,
            chunks=chunks,
        )

    def _get_columns(
        self, adapter: BaseDBAdapter, table_name: str, schema: str
    ) -> List[ColumnMetadata]:
        query = """
            SELECT
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable = 'YES' as is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.ordinal_position,
                pgd.description as column_comment
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_description pgd
                ON pgd.objoid = (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass
                AND pgd.objsubid = c.ordinal_position
            WHERE c.table_schema = %s
              AND c.table_name = %s
            ORDER BY c.ordinal_position
        """
        result = adapter.execute_query(query, (schema, table_name))

        columns = []
        for row in result:
            if not self.config_manager.is_column_allowed(row["column_name"]):
                continue

            columns.append(
                ColumnMetadata(
                    column_name=row["column_name"],
                    data_type=row["data_type"],
                    udt_name=row["udt_name"],
                    is_nullable=row["is_nullable"],
                    column_default=row["column_default"],
                    character_maximum_length=row["character_maximum_length"],
                    numeric_precision=row["numeric_precision"],
                    numeric_scale=row["numeric_scale"],
                    ordinal_position=row["ordinal_position"],
                    column_comment=row["column_comment"],
                )
            )
        return columns

    def _get_indexes(
        self, adapter: BaseDBAdapter, table_name: str, schema: str
    ) -> List[IndexMetadata]:
        query = """
            SELECT
                i.indexname as index_name,
                i.indexdef as index_definition,
                am.amname as index_type,
                i.indisunique as is_unique,
                i.indisprimary as is_primary
            FROM pg_indexes idx
            JOIN pg_class c ON idx.indexname = c.relname
            JOIN pg_am am ON c.relam = am.oid
            JOIN (
                SELECT
                    indexname,
                    indexdef,
                    indisunique,
                    indisprimary
                FROM pg_indexes
                CROSS JOIN LATERAL (
                    SELECT
                        indexrelid,
                        indisunique,
                        indisprimary
                    FROM pg_index
                    WHERE (indexrelid::regclass)::text = indexname
                       OR (indexrelid::regclass)::text = quote_ident(schemaname) || '.' || quote_ident(indexname)
                ) sub
                WHERE schemaname = %s AND tablename = %s
            ) i ON idx.indexname = i.indexname
            WHERE idx.schemaname = %s AND idx.tablename = %s
            ORDER BY i.indisprimary DESC, i.indexname
        """
        result = adapter.execute_query(query, (schema, table_name, schema, table_name))

        indexes = []
        for row in result:
            index_name = row["index_name"]
            if self._should_exclude_index(index_name):
                continue

            columns = self._parse_index_columns(row["index_definition"])
            where_clause = self._parse_index_where(row["index_definition"])

            indexes.append(
                IndexMetadata(
                    index_name=index_name,
                    index_definition=row["index_definition"],
                    index_type=row["index_type"],
                    is_unique=row["is_unique"],
                    is_primary=row["is_primary"],
                    columns=columns,
                    where_clause=where_clause,
                )
            )
        return indexes

    def _should_exclude_index(self, index_name: str) -> bool:
        exclude_patterns = self.sync_config.sync_rules.index_sync.get(
            "exclude_patterns", []
        )
        for pattern in exclude_patterns:
            regex_pattern = pattern.replace("*", ".*").replace("%", ".*")
            if re.fullmatch(regex_pattern, index_name):
                return True
        return False

    def _parse_index_columns(self, index_def: str) -> List[str]:
        match = re.search(r"USING \w+ \((.+?)\)(?: WHERE|$)", index_def)
        if not match:
            return []

        columns_str = match.group(1)
        columns = []
        for col in columns_str.split(","):
            col = col.strip()
            col = re.sub(r"\s+(ASC|DESC|NULLS FIRST|NULLS LAST)", "", col, flags=re.IGNORECASE)
            col = col.strip().strip('"')
            if col:
                columns.append(col)
        return columns

    def _parse_index_where(self, index_def: str) -> Optional[str]:
        match = re.search(r" WHERE (.+?)$", index_def)
        return match.group(1).strip() if match else None

    def _get_constraints(
        self, adapter: BaseDBAdapter, table_name: str, schema: str
    ) -> List[ConstraintMetadata]:
        query = """
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name,
                ccu.table_schema as foreign_table_schema,
                ccu.table_name as foreign_table_name,
                ccu.column_name as foreign_column_name,
                cc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
                ON tc.constraint_catalog = kcu.constraint_catalog
                AND tc.constraint_schema = kcu.constraint_schema
                AND tc.constraint_name = kcu.constraint_name
            LEFT JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_catalog = ccu.constraint_catalog
                AND tc.constraint_schema = ccu.constraint_schema
                AND tc.constraint_name = ccu.constraint_name
            LEFT JOIN information_schema.check_constraints cc
                ON tc.constraint_catalog = cc.constraint_catalog
                AND tc.constraint_schema = cc.constraint_schema
                AND tc.constraint_name = cc.constraint_name
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY tc.constraint_type, tc.constraint_name
        """
        result = adapter.execute_query(query, (schema, table_name))

        constraints = []
        for row in result:
            if row["constraint_type"] == "FOREIGN KEY" and \
               self.sync_config.sync_rules.constraint_sync.get("exclude_foreign_keys", True):
                continue

            constraints.append(
                ConstraintMetadata(
                    constraint_name=row["constraint_name"],
                    constraint_type=row["constraint_type"],
                    column_name=row["column_name"],
                    foreign_table_schema=row["foreign_table_schema"],
                    foreign_table_name=row["foreign_table_name"],
                    foreign_column_name=row["foreign_column_name"],
                    check_clause=row["check_clause"],
                )
            )
        return constraints

    def _get_partition_indexes(
        self,
        adapter: BaseDBAdapter,
        table_name: str,
        schema: str,
        is_hypertable: bool = False,
    ) -> List[PartitionIndexMetadata]:
        if is_hypertable:
            query = """
                SELECT
                    c.chunk_name as partition_name,
                    c.chunk_schema as partition_schema,
                    idx.indexname as index_name,
                    idx.indexdef as index_definition,
                    am.amname as index_type,
                    i.indisunique as is_unique,
                    pi.parent_index_name
                FROM timescaledb_information.chunks c
                JOIN pg_class pc ON pc.relname = c.chunk_name
                JOIN pg_namespace pn ON pc.relnamespace = pn.oid
                JOIN pg_indexes idx ON idx.indexname = pc.relname
                    AND idx.schemaname = pn.nspname
                JOIN pg_am am ON pc.relam = am.oid
                JOIN pg_index i ON i.indexrelid = pc.oid
                LEFT JOIN (
                    SELECT
                        h.schema as hypertable_schema,
                        h.table_name as hypertable_name,
                        idx2.indexname as parent_index_name
                    FROM timescaledb_information.hypertables h
                    JOIN pg_indexes idx2 ON idx2.tablename = h.table_name
                        AND idx2.schemaname = h.schema
                ) pi ON pi.hypertable_schema = c.hypertable_schema
                    AND pi.hypertable_name = c.hypertable_name
                WHERE c.hypertable_schema = %s
                  AND c.hypertable_name = %s
                  AND pc.relkind = 'i'
                ORDER BY c.chunk_name, idx.indexname
            """
        else:
            query = """
                SELECT
                    c_child.relname as partition_name,
                    nm_child.nspname as partition_schema,
                    idx.indexname as index_name,
                    idx.indexdef as index_definition,
                    am.amname as index_type,
                    i.indisunique as is_unique,
                    NULL as parent_index_name
                FROM pg_inherits inh
                JOIN pg_class c_parent ON inh.inhparent = c_parent.oid
                JOIN pg_namespace nm_parent ON c_parent.relnamespace = nm_parent.oid
                JOIN pg_class c_child ON inh.inhrelid = c_child.oid
                JOIN pg_namespace nm_child ON c_child.relnamespace = nm_child.oid
                JOIN pg_indexes idx ON idx.tablename = c_child.relname
                    AND idx.schemaname = nm_child.nspname
                JOIN pg_class ic ON ic.relname = idx.indexname
                JOIN pg_am am ON ic.relam = am.oid
                JOIN pg_index i ON i.indexrelid = ic.oid
                WHERE nm_parent.nspname = %s
                  AND c_parent.relname = %s
                ORDER BY c_child.relname, idx.indexname
            """

        try:
            result = adapter.execute_query(query, (schema, table_name))
        except Exception:
            return []

        indexes = []
        for row in result:
            if self._should_exclude_index(row["index_name"]):
                continue
            indexes.append(
                PartitionIndexMetadata(
                    partition_name=row["partition_name"],
                    partition_schema=row["partition_schema"],
                    index_name=row["index_name"],
                    index_definition=row["index_definition"],
                    index_type=row["index_type"],
                    is_unique=row["is_unique"],
                    parent_index_name=row.get("parent_index_name"),
                )
            )
        return indexes

    def _get_partition_info(
        self, adapter: BaseDBAdapter, table_name: str, schema: str
    ) -> Optional[PartitionMetadata]:
        query = """
            SELECT
                pm.partstrat as partition_strategy,
                pg_get_partition_key(c.oid) as partition_key
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_partitioned_table pm ON c.oid = pm.partrelid
            WHERE n.nspname = %s AND c.relname = %s
        """
        result = adapter.execute_query(query, (schema, table_name))

        if not result:
            return None

        row = result[0]
        partition_type_map = {
            "r": "range",
            "l": "list",
            "h": "hash",
        }
        partition_type = partition_type_map.get(row["partition_strategy"], "unknown")

        partition_key = []
        if row["partition_key"]:
            partition_key = [k.strip() for k in row["partition_key"].split(",")]

        partitions_query = """
            SELECT
                nm_child.nspname as partition_schema,
                c_child.relname as partition_name,
                pg_get_expr(c_child.relpartbound, c_child.oid) as partition_bound
            FROM pg_inherits i
            JOIN pg_class c_parent ON i.inhparent = c_parent.oid
            JOIN pg_class c_child ON i.inhrelid = c_child.oid
            JOIN pg_namespace nm_child ON c_child.relnamespace = nm_child.oid
            JOIN pg_namespace nm_parent ON c_parent.relnamespace = nm_parent.oid
            WHERE nm_parent.nspname = %s AND c_parent.relname = %s
            ORDER BY c_child.relname
        """
        partitions = adapter.execute_query(partitions_query, (schema, table_name))

        partition_indexes = self._get_partition_indexes(
            adapter, table_name, schema, is_hypertable=False
        )

        return PartitionMetadata(
            partition_type=partition_type,
            partition_key=partition_key,
            partitions=partitions,
            partition_indexes=partition_indexes,
            is_hypertable=False,
        )
