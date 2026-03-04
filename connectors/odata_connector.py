import time
import logging
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any

import requests

from core.base_connector import BaseConnector, ConnectorMetadata, Entity, Field, Relation
from core.auth_manager   import auth_manager

logger = logging.getLogger(__name__)

EDM_TYPE_MAP = {
    "Edm.String":          "string",
    "Edm.Int16":           "integer",
    "Edm.Int32":           "integer",
    "Edm.Int64":           "integer",
    "Edm.Decimal":         "decimal",
    "Edm.Single":          "float",
    "Edm.Double":          "float",
    "Edm.Boolean":         "boolean",
    "Edm.DateTime":        "datetime",
    "Edm.DateTimeOffset":  "datetime",
    "Edm.Date":            "date",
    "Edm.Time":            "time",
    "Edm.TimeOfDay":       "time",
    "Edm.Guid":            "uuid",
    "Edm.Binary":          "binary",
}

def normalize_edm_type(t: str) -> str:
    return EDM_TYPE_MAP.get(t, "string")


class ODataConnector(BaseConnector):
    """
    Connecteur OData generique.
    Compatible Microsoft Dynamics, Business Central, etc.

    Config:
        base_url : "https://org.api.crm.dynamics.com/api/data/v9.2"
        auth     : {type: oauth2, token_url, client_id, client_secret, scope}
        timeout  : 30
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._session      = requests.Session()
        self._base_url     = config["base_url"].rstrip("/")
        self._timeout      = config.get("timeout", 30)
        self._metadata_xml = None

    def connect(self) -> bool:
        self._session.headers.update(
            auth_manager.get_headers(self.config.get("auth", {"type": "none"}))
        )
        resp = self._session.get(f"{self._base_url}/$metadata", timeout=self._timeout)
        resp.raise_for_status()
        self._metadata_xml = ET.fromstring(resp.text)
        self._connected    = True
        logger.info(f"[ODataConnector] Metadonnees chargees depuis {self._base_url}")
        return True

    def test_connection(self) -> Dict[str, Any]:
        start = time.time()
        try:
            r = self._session.get(f"{self._base_url}/$metadata", timeout=self._timeout)
            return {"success": r.status_code == 200,
                    "message": f"HTTP {r.status_code}",
                    "latency_ms": int((time.time() - start) * 1000)}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": -1}

    def get_metadata(self) -> ConnectorMetadata:
        if not self._metadata_xml:
            raise RuntimeError("Non connecte. Appelez connect() d'abord.")
        entities = []
        for elem in self._metadata_xml.iter():
            if not (elem.tag.endswith("}EntityType") or elem.tag == "EntityType"):
                continue
            name      = elem.get("Name", "Unknown")
            fields    = []
            relations = []
            for child in elem:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag == "Property":
                    fields.append(Field(
                        name=child.get("Name", ""),
                        type=normalize_edm_type(child.get("Type", "Edm.String")),
                        nullable=child.get("Nullable", "true").lower() == "true"
                    ))
                elif tag == "NavigationProperty":
                    target = child.get("ToRole") or child.get("Type", "").split(".")[-1]
                    relations.append(Relation(
                        source_entity=name,
                        target_entity=target,
                        source_field=child.get("Name", ""),
                        target_field="id"
                    ))
            entities.append(Entity(name=name, fields=fields,
                                   relations=relations, source_type="odata"))
            logger.info(f"[ODataConnector] '{name}': {len(fields)} champs, {len(relations)} relations")
        return ConnectorMetadata(connector_id=self.config.get("id", "odata"),
                                 connector_type="odata", entities=entities)

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        resp = self._session.get(f"{self._base_url}{query}",
                                 params=params, timeout=self._timeout)
        resp.raise_for_status()
        data = resp.json()
        return data.get("value", [data])

    def disconnect(self):
        self._session.close()
        super().disconnect()