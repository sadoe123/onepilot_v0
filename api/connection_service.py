"""
OnePilot - Connection Service
Teste les vraies connexions et synchronise les metadonnees
"""
from __future__ import annotations

import os
import time
import json
import logging
from uuid import UUID
from typing import Dict, Any, List

from .schemas import ConnectorType, SourceCategory, CONNECTOR_CATEGORY_MAP
from .repository import get_source, get_source_secrets, save_test_result, save_metadata

logger = logging.getLogger(__name__)


def _build_sqlalchemy_url(connector_type: str, host: str, port: int,
                           db: str, user: str, password: str) -> str:
    if connector_type == "sqlite":
        return f"sqlite:///{db}"
    if connector_type == "mssql":
        return (
            f"mssql+pyodbc://{user}:{password}@{host}:{port}/{db}"
            f"?driver=ODBC+Driver+18+for+SQL+Server"
            f"&TrustServerCertificate=yes"
            f"&Encrypt=no"
        )
    drivers = {
        "postgresql": "postgresql+psycopg2",
        "mysql":      "mysql+pymysql",
    }
    driver = drivers.get(connector_type, connector_type)
    return f"{driver}://{user}:{password}@{host}:{port}/{db}"


# ── TEST CONNECTION ───────────────────────────────────────────

async def test_connection(source_id: UUID) -> Dict[str, Any]:
    source = await get_source(source_id)
    if not source:
        return {"success": False, "message": "Source introuvable", "latency_ms": -1}

    secrets = await get_source_secrets(source_id)
    start = time.time()

    try:
        category = CONNECTOR_CATEGORY_MAP.get(source.connector_type, SourceCategory.DATABASE)

        if category == SourceCategory.DATABASE:
            result = await _test_db_connection(source, secrets)
        elif category == SourceCategory.WEBSERVICE:
            result = await _test_webservice_connection(source, secrets)
        elif category == SourceCategory.FILE:
            result = await _test_file_connection(source)
        else:
            result = {"success": False, "message": "Type non supporte"}

    except Exception as e:
        result = {"success": False, "message": str(e)}

    latency = int((time.time() - start) * 1000)
    result["latency_ms"] = result.get("latency_ms", latency)

    await save_test_result(
        source_id, result["success"], result["message"], result["latency_ms"]
    )
    return result


async def _test_db_connection(source, secrets: Dict) -> Dict:
    try:
        from sqlalchemy import create_engine, text
        password = secrets.get("password", "")
        url = _build_sqlalchemy_url(
            source.connector_type.value,
            source.host, source.port or 1433,
            source.database_name, source.username or "", password
        )
        start = time.time()
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        latency = int((time.time() - start) * 1000)
        return {"success": True, "message": "Connexion reussie", "latency_ms": latency}
    except Exception as e:
        return {"success": False, "message": str(e), "latency_ms": -1}


async def _test_webservice_connection(source, secrets: Dict) -> Dict:
    try:
        import requests
        headers: Dict[str, str] = {}

        if source.auth_type.value == "bearer":
            headers["Authorization"] = f"Bearer {secrets.get('token', '')}"
        elif source.auth_type.value == "basic":
            import base64
            creds = base64.b64encode(
                f"{source.username}:{secrets.get('password','')}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {creds}"
        elif source.auth_type.value == "api_key":
            header_name = source.options.get("api_key_header", "X-API-Key")
            headers[header_name] = secrets.get("api_key_value", "")

        url = source.base_url
        if source.connector_type.value == "odata":
            url = f"{url}/$metadata"

        start = time.time()
        resp = requests.get(url, headers=headers, timeout=10)
        latency = int((time.time() - start) * 1000)

        if resp.status_code < 500:
            return {"success": True, "message": f"HTTP {resp.status_code}", "latency_ms": latency}
        return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}
    except Exception as e:
        return {"success": False, "message": str(e), "latency_ms": -1}


async def _test_file_connection(source) -> Dict:
    start = time.time()
    try:
        file_path = _resolve_file_path(source)
        if not file_path:
            return {"success": False, "message": "Aucun chemin de fichier specifie", "latency_ms": -1}

        if file_path.startswith("http"):
            import requests
            resp = requests.head(file_path, timeout=10)
            latency = int((time.time() - start) * 1000)
            if resp.status_code < 400:
                return {"success": True, "message": f"Fichier accessible (HTTP {resp.status_code})", "latency_ms": latency}
            return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}

        if not os.path.exists(file_path):
            return {"success": False, "message": f"Fichier introuvable : {file_path}", "latency_ms": -1}

        size = os.path.getsize(file_path)
        latency = int((time.time() - start) * 1000)
        return {
            "success": True,
            "message": f"Fichier accessible ({_human_size(size)})",
            "latency_ms": latency
        }
    except Exception as e:
        return {"success": False, "message": str(e), "latency_ms": -1}


def _resolve_file_path(source) -> str:
    opts = source.options or {}

    if opts.get("file_path"):
        path = opts["file_path"]
        # Chemin Windows local : C:\... ou C:/... -> /mnt/host/...
        if len(path) > 2 and path[1] == ":":
            path = "/mnt/host/" + path[3:].replace("\\", "/")
        # Chemin reseau UNC : \\serveur\... -> /mnt/network/serveur/...
        elif path.startswith("\\\\"):
            path = "/mnt/network/" + path[2:].replace("\\", "/")
        elif path.startswith("//"):
            path = "/mnt/network/" + path[2:]
        return path

    if opts.get("uploaded_path"):
        return opts["uploaded_path"]

    if source.base_url:
        return source.base_url

    return ""


def _human_size(size: int) -> str:
    for unit in ["o", "Ko", "Mo", "Go"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} To"


# ── SYNC METADATA ─────────────────────────────────────────────

async def sync_metadata(source_id: UUID) -> Dict[str, Any]:
    source = await get_source(source_id)
    if not source:
        return {"success": False, "message": "Source introuvable"}

    secrets = await get_source_secrets(source_id)
    start = time.time()

    try:
        category = CONNECTOR_CATEGORY_MAP.get(source.connector_type, SourceCategory.DATABASE)

        if category == SourceCategory.DATABASE:
            entities = await _fetch_db_metadata(source, secrets)
        elif category == SourceCategory.WEBSERVICE:
            entities = await _fetch_webservice_metadata(source, secrets)
        elif category == SourceCategory.FILE:
            entities = await _fetch_file_metadata(source)
        else:
            return {"success": False, "message": "Sync non supporte pour ce type"}

        entity_count = await save_metadata(source_id, entities)
        field_count = sum(len(e.get("fields", [])) for e in entities)
        duration = int((time.time() - start) * 1000)

        return {
            "success": True,
            "entity_count": entity_count,
            "field_count": field_count,
            "relation_count": 0,
            "duration_ms": duration,
            "message": f"{entity_count} entites, {field_count} champs synchronises"
        }

    except Exception as e:
        logger.error(f"[Sync] Erreur source {source_id}: {e}", exc_info=True)
        return {
            "success": False, "entity_count": 0, "field_count": 0,
            "relation_count": 0, "duration_ms": int((time.time() - start) * 1000),
            "message": str(e)
        }


async def _fetch_db_metadata(source, secrets: Dict) -> List[Dict]:
    from sqlalchemy import create_engine, inspect
    password = secrets.get("password", "")
    url = _build_sqlalchemy_url(
        source.connector_type.value,
        source.host, source.port or 1433,
        source.database_name, source.username or "", password
    )
    engine = create_engine(url, pool_pre_ping=True)
    inspector = inspect(engine)
    schema = source.schema_name

    TYPE_MAP = {
        "INTEGER": "integer", "BIGINT": "integer", "SMALLINT": "integer", "INT": "integer",
        "NUMERIC": "decimal", "DECIMAL": "decimal", "FLOAT": "float", "DOUBLE": "float", "REAL": "float",
        "VARCHAR": "string", "TEXT": "string", "CHAR": "string", "NVARCHAR": "string",
        "BOOLEAN": "boolean", "BOOL": "boolean",
        "DATE": "date", "TIMESTAMP": "datetime", "DATETIME": "datetime",
        "JSON": "json", "JSONB": "json", "UUID": "uuid",
    }

    entities = []
    for table_name in inspector.get_table_names(schema=schema):
        columns = inspector.get_columns(table_name, schema=schema)
        pk_cols = set(inspector.get_pk_constraint(table_name, schema=schema).get("constrained_columns", []))
        fk_cols = set()
        for fk in inspector.get_foreign_keys(table_name, schema=schema):
            fk_cols.update(fk.get("constrained_columns", []))

        fields = []
        for col in columns:
            native = str(col["type"]).upper().split("(")[0].strip()
            fields.append({
                "name":        col["name"],
                "type":        TYPE_MAP.get(native, "string"),
                "native_type": str(col["type"]),
                "nullable":    col.get("nullable", True),
                "primary_key": col["name"] in pk_cols,
                "foreign_key": col["name"] in fk_cols,
            })

        entities.append({"name": table_name, "entity_type": "table", "fields": fields})

    engine.dispose()
    return entities


async def _fetch_webservice_metadata(source, secrets: Dict) -> List[Dict]:
    import requests
    headers: Dict[str, str] = {"Accept": "application/json"}

    if source.auth_type.value == "bearer":
        headers["Authorization"] = f"Bearer {secrets.get('token','')}"
    elif source.auth_type.value == "basic":
        import base64
        creds = base64.b64encode(
            f"{source.username}:{secrets.get('password','')}".encode()
        ).decode()
        headers["Authorization"] = f"Basic {creds}"

    if source.connector_type.value == "odata":
        return await _parse_odata_metadata(source.base_url, headers)

    endpoints = source.options.get("endpoints", [{"path": "/", "entity_name": "root"}])
    entities = []

    for ep in endpoints:
        path = ep.get("path", "/")
        name = ep.get("entity_name", path.strip("/") or "data")
        try:
            resp = requests.get(f"{source.base_url}{path}", headers=headers, timeout=10)
            data = resp.json()
            sample = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else {}
            if isinstance(sample, dict):
                fields = [{"name": k, "type": _infer_type(v), "nullable": True} for k, v in sample.items()]
                entities.append({"name": name, "entity_type": "endpoint", "fields": fields})
        except Exception as e:
            logger.warning(f"[Sync REST] {path}: {e}")

    return entities


async def _fetch_file_metadata(source) -> List[Dict]:
    file_path = _resolve_file_path(source)
    if not file_path:
        raise ValueError("Aucun chemin de fichier specifie dans les options")

    connector_type = source.connector_type.value
    entity_name = source.name.replace(" ", "_").lower()
    content = await _read_file_content(file_path)

    if connector_type == "file_csv":
        return _parse_csv(content, entity_name)
    elif connector_type == "file_json":
        return _parse_json(content, entity_name)
    else:
        stripped = content.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            return _parse_json(content, entity_name)
        return _parse_csv(content, entity_name)


async def _read_file_content(file_path: str) -> str:
    if file_path.startswith("http"):
        import requests
        resp = requests.get(file_path, timeout=30)
        resp.raise_for_status()
        return resp.text
    with open(file_path, "r", encoding="utf-8-sig") as f:
        return f.read()


def _parse_csv(content: str, entity_name: str) -> List[Dict]:
    import csv, io
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)
    if not rows:
        return [{"name": entity_name, "entity_type": "file", "fields": []}]

    fields = []
    sample_rows = rows[:100]
    for col in (reader.fieldnames or []):
        values = [r.get(col, "") for r in sample_rows if r.get(col)]
        fields.append({
            "name": col.strip(),
            "type": _infer_column_type(values),
            "native_type": "csv_column",
            "nullable": True,
        })

    return [{
        "name": entity_name,
        "entity_type": "file",
        "description": f"CSV - {len(rows)} lignes, {len(fields)} colonnes",
        "fields": fields
    }]


def _parse_json(content: str, entity_name: str) -> List[Dict]:
    data = json.loads(content)
    row_count = 1

    if isinstance(data, list):
        sample = data[0] if data else {}
        row_count = len(data)
    elif isinstance(data, dict):
        sample = data
        for v in data.values():
            if isinstance(v, list) and v:
                sample = v[0]
                row_count = len(v)
                break
    else:
        return [{"name": entity_name, "entity_type": "file", "fields": []}]

    fields = []
    if isinstance(sample, dict):
        for k, v in sample.items():
            fields.append({
                "name": k,
                "type": _infer_type(v),
                "native_type": "json_field",
                "nullable": True,
            })

    return [{
        "name": entity_name,
        "entity_type": "file",
        "description": f"JSON - {row_count} enregistrements, {len(fields)} champs",
        "fields": fields
    }]


def _infer_column_type(values: List[str]) -> str:
    import re
    if not values:
        return "string"
    total = len(values)
    int_count   = sum(1 for v in values if re.match(r"^-?\d+$", v.strip()))
    float_count = sum(1 for v in values if re.match(r"^-?\d+\.\d+$", v.strip()))
    date_count  = sum(1 for v in values if re.match(r"^\d{4}-\d{2}-\d{2}", v.strip()))
    bool_count  = sum(1 for v in values if v.strip().lower() in ("true","false","1","0","oui","non"))
    if date_count  / total > 0.8: return "date"
    if bool_count  / total > 0.8: return "boolean"
    if int_count   / total > 0.8: return "integer"
    if float_count / total > 0.8: return "float"
    return "string"


def _infer_type(value) -> str:
    if isinstance(value, bool):  return "boolean"
    if isinstance(value, int):   return "integer"
    if isinstance(value, float): return "float"
    if isinstance(value, dict):  return "object"
    if isinstance(value, list):  return "array"
    import re
    if isinstance(value, str) and re.match(r"\d{4}-\d{2}-\d{2}", value):
        return "date"
    return "string"


async def _parse_odata_metadata(base_url: str, headers: Dict) -> List[Dict]:
    import requests, xml.etree.ElementTree as ET
    resp = requests.get(f"{base_url}/$metadata", headers=headers, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    EDM_MAP = {
        "Edm.String": "string", "Edm.Int32": "integer", "Edm.Int64": "integer",
        "Edm.Decimal": "decimal", "Edm.Double": "float", "Edm.Boolean": "boolean",
        "Edm.DateTime": "datetime", "Edm.DateTimeOffset": "datetime",
        "Edm.Date": "date", "Edm.Guid": "uuid",
    }
    entities = []
    for elem in root.iter():
        if not (elem.tag.endswith("}EntityType") or elem.tag == "EntityType"):
            continue
        name = elem.get("Name", "Unknown")
        fields = []
        for child in elem:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "Property":
                fields.append({
                    "name": child.get("Name", ""),
                    "type": EDM_MAP.get(child.get("Type", "Edm.String"), "string"),
                    "native_type": child.get("Type"),
                    "nullable": child.get("Nullable", "true").lower() == "true",
                })
        entities.append({"name": name, "entity_type": "odata_entity", "fields": fields})
    return entities