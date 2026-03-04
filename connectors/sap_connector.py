import time
import logging
from typing import Dict, List, Optional, Any

from core.base_connector import BaseConnector, ConnectorMetadata, Entity, Field

logger = logging.getLogger(__name__)

ABAP_TYPE_MAP = {
    "C": "string",  "N": "string",  "D": "date",    "T": "time",
    "I": "integer", "P": "decimal", "F": "float",   "X": "binary",
    "S": "integer", "B": "integer",
    "STRING": "string", "XSTRING": "binary",
}

def normalize_abap_type(t: str) -> str:
    return ABAP_TYPE_MAP.get(t, "string")


class SAPConnector(BaseConnector):
    """
    Connecteur SAP via RFC (pyrfc).

    Config:
        ashost           : "sap.company.com"
        sysnr            : "00"
        client           : "100"
        user             : "RFC_USER"
        passwd           : "password"
        lang             : "FR"
        function_modules : ["BAPI_CUSTOMER_GETLIST", ...]
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._connection = None

    def connect(self) -> bool:
        try:
            import pyrfc
            self._connection = pyrfc.Connection(
                ashost=self.config["ashost"],
                sysnr=self.config.get("sysnr", "00"),
                client=self.config.get("client", "100"),
                user=self.config["user"],
                passwd=self.config["passwd"],
                lang=self.config.get("lang", "FR")
            )
            self._connected = True
            logger.info(f"[SAPConnector] Connecte a {self.config['ashost']}")
            return True
        except ImportError:
            raise ImportError("pyrfc non installe. Voir : pip install pyrfc")

    def test_connection(self) -> Dict[str, Any]:
        start = time.time()
        try:
            self._connection.call("RFC_PING")
            return {"success": True, "message": "RFC_PING OK",
                    "latency_ms": int((time.time() - start) * 1000)}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": -1}

    def get_metadata(self) -> ConnectorMetadata:
        entities = []
        for fm in self.config.get("function_modules", []):
            try:
                result = self._connection.call("RFC_GET_FUNCTION_INTERFACE", FUNCNAME=fm)
                fields = [
                    Field(name=p.get("PARAMETER", ""),
                          type=normalize_abap_type(p.get("TABNAME", "C")))
                    for p in result.get("PARAMS_AND_EXCPS", [])
                ]
                entities.append(Entity(name=fm, fields=fields, source_type="sap"))
            except Exception as e:
                logger.warning(f"[SAPConnector] '{fm}': {e}")
        if not entities:
            entities.append(Entity(
                name="sap_connection",
                source_type="sap",
                fields=[
                    Field(name="system", type="string"),
                    Field(name="client", type="string"),
                ],
                description="Connexion SAP OK. Configurez 'function_modules' pour explorer."
            ))
        return ConnectorMetadata(connector_id=self.config.get("id", "sap"),
                                 connector_type="sap", entities=entities)

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        result = self._connection.call(query, **(params or {}))
        for v in result.values():
            if isinstance(v, list): return v
        return [{"key": k, "value": str(v)} for k, v in result.items()]

    def disconnect(self):
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
        super().disconnect()
        logger.info("[SAPConnector] Deconnecte.")