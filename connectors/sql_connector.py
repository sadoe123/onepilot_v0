import time
import logging
from typing import Dict, List, Optional, Any
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

from core.base_connector import BaseConnector, ConnectorMetadata, Entity, Field, Relation

logger = logging.getLogger(__name__)

TYPE_MAP = {
    "INTEGER": "integer", "BIGINT": "integer", "SMALLINT": "integer",
    "NUMERIC": "decimal", "DECIMAL": "decimal", "FLOAT": "float", "DOUBLE": "float",
    "VARCHAR": "string",  "TEXT": "string",    "CHAR": "string", "NVARCHAR": "string",
    "BOOLEAN": "boolean", "BOOL": "boolean",
    "DATE": "date", "TIMESTAMP": "datetime", "DATETIME": "datetime", "TIME": "time",
    "JSON": "json", "JSONB": "json", "UUID": "uuid",
    "BYTEA": "binary", "BLOB": "binary",
}

def normalize_type(sql_type: str) -> str:
    upper = str(sql_type).upper().split("(")[0].strip()
    return TYPE_MAP.get(upper, "string")


class SQLConnector(BaseConnector):
    """
    Connecteur SQL generique.
    Supporte : PostgreSQL, MySQL, SQL Server.

    Config:
        url      : "postgresql://user:pass@host:5432/db"
        dialect  : postgresql | mysql | mssql | sqlite
        host, port, database, username, password, schema
    """

    SUPPORTED_DIALECTS = {
        "postgresql": "postgresql+psycopg2",
        "mysql":      "mysql+pymysql",
        "mssql":      "mssql+pyodbc",
        "sqlite":     "sqlite",
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._engine = None
        self._schema = config.get("schema")

    def _build_url(self) -> str:
        if "url" in self.config:
            return self.config["url"]
        dialect  = self.config.get("dialect", "postgresql")
        driver   = self.SUPPORTED_DIALECTS.get(dialect, dialect)
        host     = self.config.get("host", "localhost")
        port     = self.config.get("port", 5432)
        database = self.config.get("database", "")
        username = self.config.get("username", "")
        password = self.config.get("password", "")
        if dialect == "sqlite":
            return f"sqlite:///{database}"
        return f"{driver}://{username}:{password}@{host}:{port}/{database}"

    def connect(self) -> bool:
        try:
            self._engine = create_engine(self._build_url(), pool_pre_ping=True, pool_size=5)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._connected = True
            logger.info("[SQLConnector] Connexion etablie.")
            return True
        except SQLAlchemyError as e:
            logger.error(f"[SQLConnector] Erreur: {e}")
            self._connected = False
            raise

    def test_connection(self) -> Dict[str, Any]:
        start = time.time()
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {"success": True, "message": "Connexion OK",
                    "latency_ms": int((time.time() - start) * 1000)}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": -1}

    def get_metadata(self) -> ConnectorMetadata:
        inspector   = inspect(self._engine)
        schema      = self._schema
        table_names = inspector.get_table_names(schema=schema)
        entities: List[Entity] = []
        all_fks = {}

        for table_name in table_names:
            columns = inspector.get_columns(table_name, schema=schema)
            pk_cols = set(inspector.get_pk_constraint(table_name, schema=schema)
                         .get("constrained_columns", []))
            fields = [
                Field(name=c["name"], type=normalize_type(c["type"]),
                      nullable=c.get("nullable", True), primary_key=c["name"] in pk_cols)
                for c in columns
            ]
            all_fks[table_name] = inspector.get_foreign_keys(table_name, schema=schema)
            entities.append(Entity(name=table_name, fields=fields, source_type="sql"))

        entity_map = {e.name: e for e in entities}
        for table_name, fks in all_fks.items():
            for fk in fks:
                ref = fk.get("referred_table")
                if ref and ref in entity_map:
                    for lc, rc in zip(fk.get("constrained_columns", []),
                                      fk.get("referred_columns", [])):
                        entity_map[table_name].relations.append(
                            Relation(source_entity=table_name, target_entity=ref,
                                     source_field=lc, target_field=rc)
                        )

        return ConnectorMetadata(connector_id=self.config.get("id", "sql"),
                                 connector_type="sql", entities=entities)

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        with self._engine.connect() as conn:
            result  = conn.execute(text(query), params or {})
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]

    def disconnect(self):
        if self._engine:
            self._engine.dispose()
        super().disconnect()
        logger.info("[SQLConnector] Deconnecte.")