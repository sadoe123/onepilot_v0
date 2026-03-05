"""
OnePilot - Connection Service
Teste les vraies connexions et synchronise les metadonnees
Supporte : SQL Server, PostgreSQL, MySQL, OData, REST, Files, SAP, Dynamics 365, SAGE
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
        ct = source.connector_type.value
        category = CONNECTOR_CATEGORY_MAP.get(source.connector_type, SourceCategory.DATABASE)

        if ct in ("sap_rfc", "sap_odata"):
            result = await _test_sap_connection(source, secrets)
        elif ct == "dynamics365":
            result = await _test_dynamics_connection(source, secrets)
        elif ct in ("sage_x3", "sage_100", "sage_cloud"):
            result = await _test_sage_connection(source, secrets)
        elif category == SourceCategory.DATABASE:
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
            headers["Accept"] = "application/xml, text/xml, */*"

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
        return {"success": True, "message": f"Fichier accessible ({_human_size(size)})", "latency_ms": latency}
    except Exception as e:
        return {"success": False, "message": str(e), "latency_ms": -1}


async def _test_sap_connection(source, secrets: Dict) -> Dict:
    import requests
    start = time.time()
    ct = source.connector_type.value

    if ct == "sap_rfc":
        try:
            import pyrfc
            conn = pyrfc.Connection(
                ashost=source.host or "",
                sysnr=source.options.get("system_number", "00"),
                client=source.options.get("client", "100"),
                user=source.username or "",
                passwd=secrets.get("password", ""),
            )
            conn.call("RFC_PING")
            conn.close()
            return {"success": True, "message": "SAP RFC_PING OK",
                    "latency_ms": int((time.time() - start) * 1000)}
        except ImportError:
            return {"success": False, "message": "pyrfc non installe. Requiert SAP NetWeaver RFC SDK.", "latency_ms": -1}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": -1}
    else:
        try:
            url = (source.base_url or "").rstrip("/")
            headers = {"Accept": "application/xml, text/xml, */*"}
            if secrets.get("token"):
                headers["Authorization"] = f"Bearer {secrets['token']}"
            elif source.username:
                import base64
                creds = base64.b64encode(f"{source.username}:{secrets.get('password','')}".encode()).decode()
                headers["Authorization"] = f"Basic {creds}"
            resp = requests.get(f"{url}/$metadata", headers=headers, timeout=10)
            latency = int((time.time() - start) * 1000)
            if resp.status_code < 500:
                return {"success": True, "message": f"SAP OData HTTP {resp.status_code}", "latency_ms": latency}
            return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": -1}


async def _test_dynamics_connection(source, secrets: Dict) -> Dict:
    import requests
    start = time.time()
    try:
        api_url = (source.base_url or "").rstrip("/")
        headers = _build_dynamics_headers(source, secrets)
        resp = requests.get(f"{api_url}/api/data/v9.2/", headers=headers, timeout=10)
        latency = int((time.time() - start) * 1000)
        if resp.status_code < 400:
            return {"success": True, "message": f"Dynamics 365 HTTP {resp.status_code}", "latency_ms": latency}
        return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}
    except Exception as e:
        return {"success": False, "message": str(e), "latency_ms": -1}


async def _test_sage_connection(source, secrets: Dict) -> Dict:
    import requests
    start = time.time()
    ct = source.connector_type.value

    try:
        if ct == "sage_x3":
            base_url = (source.base_url or "").rstrip("/")
            folder = source.options.get("folder", "SEED")
            headers = {"Accept": "application/json"}
            auth = None
            if secrets.get("token"):
                headers["Authorization"] = f"Bearer {secrets['token']}"
            elif source.username:
                auth = (source.username, secrets.get("password", ""))
            resp = requests.get(f"{base_url}/api/x3/erp/{folder}", headers=headers, auth=auth, timeout=10)
            latency = int((time.time() - start) * 1000)
            if resp.status_code < 500:
                return {"success": True, "message": f"SAGE X3 HTTP {resp.status_code}", "latency_ms": latency}
            return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}

        elif ct == "sage_100":
            return await _test_db_connection(source, secrets)

        elif ct == "sage_cloud":
            headers = {"Authorization": f"Bearer {secrets.get('token', '')}"}
            resp = requests.get(
                "https://api.accounting.sage.com/v3.1/ledger_accounts",
                headers=headers, params={"$top": 1}, timeout=10
            )
            latency = int((time.time() - start) * 1000)
            if resp.status_code < 400:
                return {"success": True, "message": f"SAGE Cloud HTTP {resp.status_code}", "latency_ms": latency}
            return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}

    except Exception as e:
        return {"success": False, "message": str(e), "latency_ms": -1}


# ── SYNC METADATA ─────────────────────────────────────────────

async def sync_metadata(source_id: UUID) -> Dict[str, Any]:
    source = await get_source(source_id)
    if not source:
        return {"success": False, "message": "Source introuvable"}

    secrets = await get_source_secrets(source_id)
    start = time.time()

    try:
        ct = source.connector_type.value
        category = CONNECTOR_CATEGORY_MAP.get(source.connector_type, SourceCategory.DATABASE)

        if ct == "sap_rfc":
            entities = await _fetch_sap_rfc_metadata(source, secrets)
        elif ct == "sap_odata":
            entities = await _fetch_sap_odata_metadata(source, secrets)
        elif ct == "dynamics365":
            entities = await _fetch_dynamics_metadata(source, secrets)
        elif ct in ("sage_x3", "sage_100", "sage_cloud"):
            entities = await _fetch_sage_metadata(source, secrets)
        elif category == SourceCategory.DATABASE:
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


# ── SAP ───────────────────────────────────────────────────────

ABAP_TYPE_MAP = {
    "C": "string", "N": "string", "D": "date", "T": "string",
    "I": "integer", "P": "decimal", "F": "float", "X": "string",
    "S": "integer", "B": "integer", "STRING": "string",
    "INT1": "integer", "INT2": "integer", "INT4": "integer",
    "CURR": "decimal", "QUAN": "decimal", "DEC": "decimal",
    "DATS": "date", "CHAR": "string", "NUMC": "string",
}

EDM_TYPE_MAP = {
    "Edm.String": "string", "Edm.Int32": "integer", "Edm.Int64": "integer",
    "Edm.Int16": "integer", "Edm.Byte": "integer", "Edm.Decimal": "decimal",
    "Edm.Double": "float", "Edm.Single": "float", "Edm.Boolean": "boolean",
    "Edm.DateTime": "datetime", "Edm.DateTimeOffset": "datetime",
    "Edm.Date": "date", "Edm.Guid": "uuid", "Edm.Binary": "string",
    "Edm.Time": "string",
}


async def _fetch_sap_rfc_metadata(source, secrets: Dict) -> List[Dict]:
    try:
        import pyrfc
    except ImportError:
        raise ImportError("pyrfc non installe. Requiert SAP NetWeaver RFC SDK + pip install pyrfc")

    conn = pyrfc.Connection(
        ashost=source.host or "",
        sysnr=source.options.get("system_number", "00"),
        client=source.options.get("client", "100"),
        user=source.username or "",
        passwd=secrets.get("password", ""),
        lang=source.options.get("lang", "FR"),
    )

    entities = []
    table_names = source.options.get("tables", [])

    if not table_names:
        try:
            result = conn.call(
                "RFC_READ_TABLE",
                QUERY_TABLE="DD02L",
                FIELDS=[{"FIELDNAME": "TABNAME"}],
                OPTIONS=[{"TEXT": "TABCLASS = 'TRANSP'"}],
                ROWCOUNT=300,
            )
            table_names = [r["WA"].strip() for r in result.get("DATA", []) if r.get("WA", "").strip()]
        except Exception as e:
            logger.warning(f"[SAP RFC] DD02L: {e}")

    for table_name in table_names[:200]:
        try:
            result = conn.call("RFC_GET_STRUCTURE_DEFINITION", TABNAME=table_name)
            fields = [
                {
                    "name": f.get("FIELDNAME", ""),
                    "type": ABAP_TYPE_MAP.get(f.get("DATATYPE", "C"), "string"),
                    "native_type": f.get("DATATYPE", ""),
                    "nullable": True,
                    "primary_key": f.get("KEYFLAG", "") == "X",
                    "foreign_key": False,
                    "description": f.get("FIELDTEXT", ""),
                }
                for f in result.get("FIELDS", [])
            ]
            if fields:
                entities.append({"name": table_name, "entity_type": "sap_table", "fields": fields})
        except Exception as e:
            logger.warning(f"[SAP RFC] {table_name}: {e}")

    for fm in source.options.get("function_modules", []):
        try:
            result = conn.call("RFC_GET_FUNCTION_INTERFACE", FUNCNAME=fm)
            fields = [
                {
                    "name": p.get("PARAMETER", ""),
                    "type": ABAP_TYPE_MAP.get(p.get("TABNAME", "C"), "string"),
                    "native_type": p.get("TABNAME", ""),
                    "nullable": True,
                    "primary_key": False,
                    "foreign_key": False,
                    "description": p.get("PARAMTEXT", ""),
                }
                for p in result.get("PARAMS_AND_EXCPS", [])
            ]
            entities.append({"name": fm, "entity_type": "sap_bapi", "fields": fields})
        except Exception as e:
            logger.warning(f"[SAP RFC] BAPI {fm}: {e}")

    conn.close()
    return entities


async def _fetch_sap_odata_metadata(source, secrets: Dict) -> List[Dict]:
    import requests
    import xml.etree.ElementTree as ET

    base_url = (source.base_url or "").rstrip("/")
    headers = {"Accept": "application/xml, text/xml, */*"}

    if secrets.get("token"):
        headers["Authorization"] = f"Bearer {secrets['token']}"
    elif source.username:
        import base64
        creds = base64.b64encode(f"{source.username}:{secrets.get('password','')}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"

    resp = requests.get(f"{base_url}/$metadata", headers=headers, timeout=15)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    EDM_NS = "http://docs.oasis-open.org/odata/ns/edm"
    ns = f"{{{EDM_NS}}}"
    if not list(root.iter(f"{ns}EntityType")):
        ns = "{http://schemas.microsoft.com/ado/2008/09/edm}"

    entities = []
    for elem in root.iter(f"{ns}EntityType"):
        name = elem.get("Name", "Unknown")
        fields = []
        pk_set = set()
        key_elem = elem.find(f"{ns}Key")
        if key_elem is not None:
            for pr in key_elem.findall(f"{ns}PropertyRef"):
                pk_set.add(pr.get("Name", ""))
        for prop in elem.findall(f"{ns}Property"):
            prop_name = prop.get("Name", "")
            prop_type = prop.get("Type", "Edm.String")
            sap_label = prop.get("{http://www.sap.com/Protocols/SAPData}label", "")
            fields.append({
                "name": prop_name, "type": EDM_TYPE_MAP.get(prop_type, "string"),
                "native_type": prop_type, "nullable": prop.get("Nullable", "true").lower() != "false",
                "primary_key": prop_name in pk_set, "foreign_key": False, "description": sap_label,
            })
        for nav in elem.findall(f"{ns}NavigationProperty"):
            fields.append({
                "name": nav.get("Name", ""), "type": "relation",
                "native_type": nav.get("Type", ""), "nullable": True,
                "primary_key": False, "foreign_key": True,
            })
        if fields:
            entities.append({"name": name, "entity_type": "sap_odata_entity", "fields": fields})

    logger.info(f"[SAP OData] {len(entities)} entites parsees")
    return entities


# ── DYNAMICS 365 ──────────────────────────────────────────────

def _build_dynamics_headers(source, secrets: Dict) -> Dict:
    headers = {
        "OData-MaxVersion": "4.0", "OData-Version": "4.0",
        "Accept": "application/json", "Content-Type": "application/json",
    }
    auth_type = source.auth_type.value if hasattr(source.auth_type, "value") else str(source.auth_type)

    if auth_type == "oauth2":
        import requests
        tenant_id = source.options.get("tenant_id", "")
        client_id = secrets.get("client_id", source.options.get("client_id", ""))
        client_secret = secrets.get("client_secret", "")
        api_url = (source.base_url or "").rstrip("/")
        resp = requests.post(
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
            data={"grant_type": "client_credentials", "client_id": client_id,
                  "client_secret": client_secret, "resource": api_url},
            timeout=15
        )
        resp.raise_for_status()
        headers["Authorization"] = f"Bearer {resp.json().get('access_token', '')}"
    elif auth_type == "bearer" and secrets.get("token"):
        headers["Authorization"] = f"Bearer {secrets['token']}"
    elif auth_type == "basic" and source.username:
        import base64
        creds = base64.b64encode(f"{source.username}:{secrets.get('password','')}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"

    return headers


async def _fetch_dynamics_metadata(source, secrets: Dict) -> List[Dict]:
    import requests
    import xml.etree.ElementTree as ET

    api_url = (source.base_url or "").rstrip("/")
    headers = _build_dynamics_headers(source, secrets)
    entities = []

    # Méthode 1 : EntityDefinitions API
    try:
        resp = requests.get(
            f"{api_url}/api/data/v9.2/EntityDefinitions",
            headers=headers,
            params={"$select": "LogicalName,DisplayName,PrimaryIdAttribute,PrimaryNameAttribute,IsCustomEntity",
                    "$filter": "IsIntersect eq false", "$top": 200},
            timeout=30
        )
        if resp.ok:
            for ed in resp.json().get("value", []):
                logical_name = ed.get("LogicalName", "")
                display_name = ed.get("DisplayName", {}).get("UserLocalizedLabel", {}).get("Label", logical_name)
                entities.append({
                    "name": logical_name,
                    "entity_type": "dynamics_entity",
                    "description": f"{display_name} ({'Custom' if ed.get('IsCustomEntity') else 'Standard'})",
                    "fields": [
                        {"name": ed.get("PrimaryIdAttribute", "id"), "type": "uuid",
                         "native_type": "Edm.Guid", "nullable": False, "primary_key": True, "foreign_key": False},
                        {"name": ed.get("PrimaryNameAttribute", "name"), "type": "string",
                         "native_type": "Edm.String", "nullable": True, "primary_key": False, "foreign_key": False},
                    ],
                })
            if entities:
                logger.info(f"[Dynamics] {len(entities)} entites via EntityDefinitions")
                return entities
    except Exception as e:
        logger.warning(f"[Dynamics] EntityDefinitions: {e}")

    # Méthode 2 : $metadata XML
    meta_headers = {**headers, "Accept": "application/xml, text/xml, */*"}
    resp = requests.get(f"{api_url}/api/data/v9.2/$metadata", headers=meta_headers, timeout=15)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    EDM_NS = "http://docs.oasis-open.org/odata/ns/edm"
    ns = f"{{{EDM_NS}}}"
    if not list(root.iter(f"{ns}EntityType")):
        ns = "{http://schemas.microsoft.com/ado/2008/09/edm}"

    for elem in root.iter(f"{ns}EntityType"):
        if elem.get("Abstract", "false").lower() == "true":
            continue
        name = elem.get("Name", "Unknown")
        fields = []
        pk_set = set()
        key_elem = elem.find(f"{ns}Key")
        if key_elem is not None:
            for pr in key_elem.findall(f"{ns}PropertyRef"):
                pk_set.add(pr.get("Name", ""))
        for prop in elem.findall(f"{ns}Property"):
            prop_name = prop.get("Name", "")
            fields.append({
                "name": prop_name, "type": EDM_TYPE_MAP.get(prop.get("Type", "Edm.String"), "string"),
                "native_type": prop.get("Type", "Edm.String"),
                "nullable": prop.get("Nullable", "true").lower() != "false",
                "primary_key": prop_name in pk_set, "foreign_key": False,
            })
        for nav in elem.findall(f"{ns}NavigationProperty"):
            fields.append({
                "name": nav.get("Name", ""), "type": "relation",
                "native_type": nav.get("Type", ""), "nullable": True,
                "primary_key": False, "foreign_key": True,
            })
        if fields:
            entities.append({"name": name, "entity_type": "dynamics_entity", "fields": fields})

    logger.info(f"[Dynamics] {len(entities)} entites via $metadata XML")
    return entities


# ── SAGE ──────────────────────────────────────────────────────

SAGE_TYPE_MAP = {
    "A": "string", "ANM": "string", "D": "date", "DCB": "decimal",
    "L": "integer", "W": "integer", "M": "string", "MD": "decimal",
    "QTY": "decimal", "C": "string", "Y": "boolean",
}


async def _fetch_sage_metadata(source, secrets: Dict) -> List[Dict]:
    ct = source.connector_type.value
    if ct == "sage_x3":
        return await _fetch_sage_x3_metadata(source, secrets)
    elif ct == "sage_100":
        return await _fetch_db_metadata(source, secrets)
    elif ct == "sage_cloud":
        return await _fetch_sage_cloud_metadata(source, secrets)
    return []


async def _fetch_sage_x3_metadata(source, secrets: Dict) -> List[Dict]:
    import requests

    base_url = (source.base_url or "").rstrip("/")
    folder = source.options.get("folder", "SEED")
    headers = {"Accept": "application/json"}
    auth = None

    if secrets.get("token"):
        headers["Authorization"] = f"Bearer {secrets['token']}"
    elif source.username:
        auth = (source.username, secrets.get("password", ""))

    objects = source.options.get("objects", [
        "CUSTOMER", "SUPPLIER", "SORDER", "SINVOICE", "GACCENTRY",
        "ITMMASTER", "FACILITY", "BPCUSTOMER", "BPSUPPLIER",
        "PORDER", "PINVOICE", "BPARTNER",
    ])

    entities = []
    for obj_name in objects:
        try:
            resp = requests.get(
                f"{base_url}/api/x3/erp/{folder}/{obj_name}/$descriptor",
                headers=headers, auth=auth, timeout=15
            )
            if resp.ok:
                descriptor = resp.json()
                fields = [
                    {
                        "name": f.get("$fieldName", ""),
                        "type": SAGE_TYPE_MAP.get(f.get("$type", "A"), "string"),
                        "native_type": f.get("$type", "A"),
                        "nullable": not f.get("$isKey", False),
                        "primary_key": f.get("$isKey", False),
                        "foreign_key": f.get("$isForeignKey", False),
                        "description": f.get("$description", ""),
                    }
                    for f in descriptor.get("$fields", [])
                ]
            else:
                fields = [
                    {"name": "ROWID", "type": "integer", "native_type": "L",
                     "nullable": False, "primary_key": True, "foreign_key": False},
                    {"name": "CODE", "type": "string", "native_type": "A",
                     "nullable": False, "primary_key": False, "foreign_key": False},
                    {"name": "DESCRIPTION", "type": "string", "native_type": "A",
                     "nullable": True, "primary_key": False, "foreign_key": False},
                ]
            entities.append({
                "name": obj_name, "entity_type": "sage_x3_object",
                "description": f"SAGE X3 - {folder} - {obj_name}", "fields": fields,
            })
        except Exception as e:
            logger.warning(f"[SAGE X3] {obj_name}: {e}")

    return entities


async def _fetch_sage_cloud_metadata(source, secrets: Dict) -> List[Dict]:
    import requests

    base_url = source.base_url or "https://api.accounting.sage.com/v3.1"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {secrets.get('token', '')}"}

    resources = [
        ("ledger_accounts", "Comptes comptables"), ("journals", "Journaux"),
        ("journal_entries", "Ecritures"), ("contacts", "Contacts"),
        ("sales_invoices", "Factures ventes"), ("purchase_invoices", "Factures achats"),
        ("products", "Produits"), ("tax_rates", "TVA"),
        ("bank_accounts", "Comptes bancaires"), ("payment_methods", "Moyens de paiement"),
        ("currencies", "Devises"), ("cost_centres", "Centres de cout"),
    ]

    entities = []
    for resource_name, description in resources:
        try:
            resp = requests.get(f"{base_url}/{resource_name}", headers=headers,
                                params={"$top": 1}, timeout=10)
            if not resp.ok:
                continue
            data = resp.json()
            items = data.get("$items", data.get("value", []))
            sample = items[0] if items else {}
            fields = [
                {"name": k, "type": _infer_type(v), "native_type": type(v).__name__,
                 "nullable": True, "primary_key": k == "id",
                 "foreign_key": k.endswith("_id") and k != "id"}
                for k, v in sample.items()
            ] if isinstance(sample, dict) else [
                {"name": "id", "type": "uuid", "native_type": "string",
                 "nullable": False, "primary_key": True, "foreign_key": False},
                {"name": "display_name", "type": "string", "native_type": "string",
                 "nullable": True, "primary_key": False, "foreign_key": False},
            ]
            entities.append({
                "name": resource_name, "entity_type": "sage_cloud_resource",
                "description": f"SAGE Business Cloud - {description}", "fields": fields,
            })
        except Exception as e:
            logger.warning(f"[SAGE Cloud] {resource_name}: {e}")

    return entities


# ── DB / WEBSERVICE / FILE ────────────────────────────────────

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
                "name": col["name"], "type": TYPE_MAP.get(native, "string"),
                "native_type": str(col["type"]), "nullable": col.get("nullable", True),
                "primary_key": col["name"] in pk_cols, "foreign_key": col["name"] in fk_cols,
            })
        entities.append({"name": table_name, "entity_type": "table", "fields": fields})

    engine.dispose()
    return entities


async def _fetch_webservice_metadata(source, secrets: Dict) -> List[Dict]:
    import requests
    headers: Dict[str, str] = {}

    if source.auth_type.value == "bearer":
        headers["Authorization"] = f"Bearer {secrets.get('token','')}"
    elif source.auth_type.value == "basic":
        import base64
        creds = base64.b64encode(f"{source.username}:{secrets.get('password','')}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"
    elif source.auth_type.value == "api_key":
        header_name = source.options.get("api_key_header", "X-API-Key")
        headers[header_name] = secrets.get("api_key_value", "")

    if source.connector_type.value == "odata":
        return await _parse_odata_metadata(source.base_url, headers)

    headers["Accept"] = "application/json"
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
        fields.append({"name": col.strip(), "type": _infer_column_type(values),
                       "native_type": "csv_column", "nullable": True})
    return [{"name": entity_name, "entity_type": "file",
             "description": f"CSV - {len(rows)} lignes, {len(fields)} colonnes", "fields": fields}]


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
            fields.append({"name": k, "type": _infer_type(v), "native_type": "json_field", "nullable": True})
    return [{"name": entity_name, "entity_type": "file",
             "description": f"JSON - {row_count} enregistrements, {len(fields)} champs", "fields": fields}]


def _resolve_file_path(source) -> str:
    opts = source.options or {}
    if opts.get("file_path"):
        path = opts["file_path"]
        if len(path) > 2 and path[1] == ":":
            path = "/mnt/host/" + path[3:].replace("\\", "/")
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
    import requests
    import xml.etree.ElementTree as ET

    meta_headers = {k: v for k, v in headers.items() if k != "Accept"}
    meta_headers["Accept"] = "application/xml, text/xml, */*"

    resp = requests.get(f"{base_url}/$metadata", headers=meta_headers, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)

    EDM_NS = "http://docs.oasis-open.org/odata/ns/edm"
    ns = f"{{{EDM_NS}}}"
    if not list(root.iter(f"{ns}EntityType")):
        ns = "{http://schemas.microsoft.com/ado/2008/09/edm}"

    entities = []
    for elem in root.iter(f"{ns}EntityType"):
        name = elem.get("Name", "Unknown")
        fields = []
        pk_set = set()
        key_elem = elem.find(f"{ns}Key")
        if key_elem is not None:
            for pr in key_elem.findall(f"{ns}PropertyRef"):
                pk_set.add(pr.get("Name", ""))
        for prop in elem.findall(f"{ns}Property"):
            prop_name = prop.get("Name", "")
            prop_type = prop.get("Type", "Edm.String")
            fields.append({
                "name": prop_name, "type": EDM_TYPE_MAP.get(prop_type, "string"),
                "native_type": prop_type,
                "nullable": prop.get("Nullable", "true").lower() != "false",
                "primary_key": prop_name in pk_set, "foreign_key": False,
            })
        for nav in elem.findall(f"{ns}NavigationProperty"):
            fields.append({
                "name": nav.get("Name", ""), "type": "relation",
                "native_type": nav.get("Type", nav.get("ToRole", "")),
                "nullable": True, "primary_key": False, "foreign_key": True,
            })
        if fields:
            entities.append({"name": name, "entity_type": "odata_entity", "fields": fields})

    logger.info(f"[OData] {len(entities)} entites parsees depuis {base_url}")
    return entities