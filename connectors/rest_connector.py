import time
import re
import logging
from typing import Dict, List, Optional, Any

import requests

from core.base_connector import BaseConnector, ConnectorMetadata, Entity, Field
from core.auth_manager   import auth_manager

logger = logging.getLogger(__name__)


def infer_type(value: Any) -> str:
    if isinstance(value, bool):   return "boolean"
    if isinstance(value, int):    return "integer"
    if isinstance(value, float):  return "float"
    if isinstance(value, dict):   return "object"
    if isinstance(value, list):   return "array"
    if isinstance(value, str):
        if re.match(r"\d{4}-\d{2}-\d{2}", value): return "date"
        return "string"
    return "string"


def json_to_entity(name: str, data: Any) -> Optional[Entity]:
    if isinstance(data, list) and data:
        sample = data[0]
    elif isinstance(data, dict):
        sample = data
    else:
        return None
    if not isinstance(sample, dict):
        return None
    fields = [Field(name=k, type=infer_type(v)) for k, v in sample.items()]
    return Entity(name=name, fields=fields, source_type="rest")


class RESTConnector(BaseConnector):
    """
    Connecteur API REST generique.

    Config:
        base_url  : "https://api.example.com"
        auth      : {type: bearer|basic|oauth2|api_key|none, ...}
        endpoints : [{path: "/users", entity_name: "users"}, ...]
        timeout   : 30
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._session  = requests.Session()
        self._base_url = config["base_url"].rstrip("/")
        self._timeout  = config.get("timeout", 30)

    def _get_headers(self) -> Dict[str, str]:
        headers = auth_manager.get_headers(self.config.get("auth", {"type": "none"}))
        headers["Accept"]       = "application/json"
        headers["Content-Type"] = "application/json"
        return headers

    def connect(self) -> bool:
        self._session.headers.update(self._get_headers())
        try:
            self._session.get(self._base_url, timeout=self._timeout).raise_for_status()
        except requests.RequestException as e:
            if not (hasattr(e, "response") and e.response is not None):
                raise
        self._connected = True
        logger.info(f"[RESTConnector] Connecte a {self._base_url}")
        return True

    def test_connection(self) -> Dict[str, Any]:
        start = time.time()
        try:
            r = self._session.get(self._base_url, timeout=self._timeout)
            return {"success": r.status_code < 500,
                    "message": f"HTTP {r.status_code}",
                    "latency_ms": int((time.time() - start) * 1000)}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": -1}

    def get_metadata(self) -> ConnectorMetadata:
        entities  = []
        endpoints = self.config.get("endpoints", [{"path": "/", "entity_name": "root"}])
        for ep in endpoints:
            path = ep["path"]
            name = ep.get("entity_name", path.strip("/").replace("/", "_") or "data")
            try:
                data   = self._session.get(f"{self._base_url}{path}",
                                           timeout=self._timeout).json()
                entity = json_to_entity(name, data)
                if entity:
                    entities.append(entity)
                    logger.info(f"[RESTConnector] '{path}' -> {len(entity.fields)} champs")
            except Exception as e:
                logger.warning(f"[RESTConnector] '{path}': {e}")
        return ConnectorMetadata(connector_id=self.config.get("id", "rest"),
                                 connector_type="rest", entities=entities)

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        data = self._session.get(f"{self._base_url}{query}",
                                 params=params, timeout=self._timeout).json()
        if isinstance(data, list): return data
        for v in data.values():
            if isinstance(v, list): return v
        return [data]

    def disconnect(self):
        self._session.close()
        super().disconnect()