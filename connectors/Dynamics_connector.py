"""
OnePilot - Microsoft Dynamics 365 / Dataverse Connector
Supporte : Dynamics Web API (OData v4) + Dataverse API
Auth     : OAuth2 (client credentials) ou Bearer token
"""
import time
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

EDM_TYPE_MAP = {
    "Edm.String": "string",         "Edm.Int32": "integer",
    "Edm.Int64": "integer",         "Edm.Int16": "integer",
    "Edm.Decimal": "decimal",       "Edm.Double": "float",
    "Edm.Single": "float",          "Edm.Boolean": "boolean",
    "Edm.DateTime": "datetime",     "Edm.DateTimeOffset": "datetime",
    "Edm.Date": "date",             "Edm.Guid": "uuid",
    "Edm.Binary": "string",
}

# Types Dynamics spécifiques
DYNAMICS_TYPE_MAP = {
    "lookup":      "uuid",
    "picklist":    "integer",
    "boolean":     "boolean",
    "integer":     "integer",
    "bigint":      "integer",
    "decimal":     "decimal",
    "double":      "float",
    "money":       "decimal",
    "datetime":    "datetime",
    "memo":        "string",
    "string":      "string",
    "uniqueidentifier": "uuid",
    "owner":       "uuid",
    "customer":    "uuid",
    "state":       "integer",
    "status":      "integer",
    "virtual":     "string",
    "image":       "string",
}


def _get_oauth2_token(config: Dict, secrets: Dict) -> str:
    """Obtient un token OAuth2 via client credentials (Azure AD)."""
    import requests

    tenant_id = config.get("tenant_id", "")
    client_id = secrets.get("client_id", config.get("client_id", ""))
    client_secret = secrets.get("client_secret", "")
    resource = config.get("api_url", "").rstrip("/")

    if not all([tenant_id, client_id, client_secret]):
        raise ValueError(
            "Dynamics OAuth2 requiert : tenant_id, client_id, client_secret"
        )

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    resp = requests.post(token_url, data={
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
        "resource":      resource,
    }, timeout=15)
    resp.raise_for_status()
    return resp.json().get("access_token", "")


def _build_headers(config: Dict, secrets: Dict) -> Dict:
    """Construit les headers HTTP pour Dynamics API."""
    headers = {
        "OData-MaxVersion": "4.0",
        "OData-Version":    "4.0",
        "Accept":           "application/json",
        "Content-Type":     "application/json",
    }

    auth_type = config.get("auth_type", "bearer")

    if auth_type == "oauth2":
        token = _get_oauth2_token(config, secrets)
        headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "bearer" and secrets.get("token"):
        headers["Authorization"] = f"Bearer {secrets['token']}"
    elif auth_type == "basic":
        import base64
        user = config.get("username", "")
        pwd = secrets.get("password", "")
        creds = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"

    return headers


def _parse_metadata_xml(xml_text: str) -> List[Dict]:
    """Parse le $metadata OData XML de Dynamics."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_text)

    # Dynamics utilise OData v4
    EDM_NS = "http://docs.oasis-open.org/odata/ns/edm"
    ns = f"{{{EDM_NS}}}"

    # Fallback OData v2/v3
    if not list(root.iter(f"{ns}EntityType")):
        ns = "{http://schemas.microsoft.com/ado/2008/09/edm}"

    entities = []

    for elem in root.iter(f"{ns}EntityType"):
        name = elem.get("Name", "Unknown")

        # Ignorer les types abstraits
        if elem.get("Abstract", "false").lower() == "true":
            continue

        fields = []

        # Clés primaires
        pk_set = set()
        key_elem = elem.find(f"{ns}Key")
        if key_elem is not None:
            for pr in key_elem.findall(f"{ns}PropertyRef"):
                pk_set.add(pr.get("Name", ""))

        # Propriétés
        for prop in elem.findall(f"{ns}Property"):
            prop_name = prop.get("Name", "")
            prop_type = prop.get("Type", "Edm.String")

            # Annotations Dynamics (display name, etc.)
            display_name = ""
            for ann in prop.findall(f"{ns}Annotation"):
                if ann.get("Term", "").endswith("DisplayName"):
                    display_name = ann.get("String", "")

            fields.append({
                "name":        prop_name,
                "type":        EDM_TYPE_MAP.get(prop_type, "string"),
                "native_type": prop_type,
                "nullable":    prop.get("Nullable", "true").lower() != "false",
                "primary_key": prop_name in pk_set,
                "foreign_key": False,
                "description": display_name,
            })

        # Navigation Properties (lookups/relations)
        for nav in elem.findall(f"{ns}NavigationProperty"):
            nav_name = nav.get("Name", "")
            fields.append({
                "name":        nav_name,
                "type":        "relation",
                "native_type": nav.get("Type", ""),
                "nullable":    True,
                "primary_key": False,
                "foreign_key": True,
            })

        if fields:
            entities.append({
                "name":        name,
                "entity_type": "dynamics_entity",
                "description": f"Dynamics 365 Entity: {name}",
                "fields":      fields,
            })

    return entities


def _parse_entity_definitions(api_url: str, headers: Dict) -> List[Dict]:
    """
    Récupère les définitions d'entités via l'API Dynamics EntityDefinitions.
    Plus détaillé que le $metadata XML.
    """
    import requests

    entities = []
    url = f"{api_url}/api/data/v9.2/EntityDefinitions"
    params = {
        "$select": "LogicalName,DisplayName,PrimaryIdAttribute,PrimaryNameAttribute,IsCustomEntity",
        "$filter": "IsIntersect eq false",
        "$top": 200,
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        entity_defs = resp.json().get("value", [])
    except Exception as e:
        logger.warning(f"[Dynamics] EntityDefinitions: {e}")
        return entities

    for entity_def in entity_defs:
        logical_name = entity_def.get("LogicalName", "")
        display_name = (
            entity_def.get("DisplayName", {})
            .get("UserLocalizedLabel", {})
            .get("Label", logical_name)
        )

        # Récupérer les attributs de l'entité
        try:
            attr_url = f"{api_url}/api/data/v9.2/EntityDefinitions(LogicalName='{logical_name}')/Attributes"
            attr_params = {
                "$select": "LogicalName,DisplayName,AttributeType,IsPrimaryId,IsPrimaryName,RequiredLevel",
                "$top": 200,
            }
            attr_resp = requests.get(attr_url, headers=headers, params=attr_params, timeout=15)
            attributes = attr_resp.json().get("value", []) if attr_resp.ok else []
        except Exception:
            attributes = []

        fields = []
        for attr in attributes:
            attr_name = attr.get("LogicalName", "")
            attr_type = attr.get("AttributeType", "String").lower()
            attr_display = (
                attr.get("DisplayName", {})
                .get("UserLocalizedLabel", {})
                .get("Label", attr_name)
            )
            fields.append({
                "name":        attr_name,
                "type":        DYNAMICS_TYPE_MAP.get(attr_type, "string"),
                "native_type": attr.get("AttributeType", "String"),
                "nullable":    attr.get("RequiredLevel", {}).get("Value", "None") == "None",
                "primary_key": attr.get("IsPrimaryId", False),
                "foreign_key": attr_type == "lookup",
                "description": attr_display,
            })

        if not fields:
            # Champs par défaut si pas d'attributs récupérés
            fields = [
                {"name": entity_def.get("PrimaryIdAttribute", "id"), "type": "uuid",
                 "native_type": "Edm.Guid", "nullable": False, "primary_key": True, "foreign_key": False},
                {"name": entity_def.get("PrimaryNameAttribute", "name"), "type": "string",
                 "native_type": "Edm.String", "nullable": True, "primary_key": False, "foreign_key": False},
            ]

        entities.append({
            "name":        logical_name,
            "entity_type": "dynamics_entity",
            "description": f"{display_name} ({'Custom' if entity_def.get('IsCustomEntity') else 'Standard'})",
            "fields":      fields,
        })

    return entities


def sync_dynamics(config: Dict, secrets: Dict) -> List[Dict]:
    """
    Synchronise les métadonnées Dynamics 365 / Dataverse.
    Essaie d'abord l'API EntityDefinitions, puis fallback sur $metadata XML.
    """
    import requests

    api_url = config.get("api_url", "").rstrip("/")
    headers = _build_headers(config, secrets)

    # Méthode 1 : EntityDefinitions API (plus complète)
    entities = _parse_entity_definitions(api_url, headers)
    if entities:
        logger.info(f"[Dynamics] {len(entities)} entités via EntityDefinitions")
        return entities

    # Méthode 2 : $metadata XML (fallback)
    try:
        meta_headers = {**headers, "Accept": "application/xml, text/xml, */*"}
        resp = requests.get(f"{api_url}/api/data/v9.2/$metadata", headers=meta_headers, timeout=15)
        resp.raise_for_status()
        entities = _parse_metadata_xml(resp.text)
        logger.info(f"[Dynamics] {len(entities)} entités via $metadata XML")
        return entities
    except Exception as e:
        logger.error(f"[Dynamics] Sync failed: {e}")
        raise


def test_dynamics_connection(config: Dict, secrets: Dict) -> Dict:
    """Test la connexion Dynamics 365."""
    import requests
    start = time.time()
    try:
        api_url = config.get("api_url", "").rstrip("/")
        headers = _build_headers(config, secrets)
        resp = requests.get(
            f"{api_url}/api/data/v9.2/",
            headers=headers,
            timeout=10,
        )
        latency = int((time.time() - start) * 1000)
        if resp.status_code < 400:
            version = resp.json().get("value", [{}])[0] if resp.ok else {}
            return {
                "success": True,
                "message": f"Dynamics 365 connecté (HTTP {resp.status_code})",
                "latency_ms": latency,
            }
        return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}
    except Exception as e:
        return {"success": False, "message": str(e), "latency_ms": -1}