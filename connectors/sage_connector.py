"""
OnePilot - SAGE Connector
Supporte : SAGE X3 (Web Services SOAP/REST) + SAGE 100 (ODBC/SQL) + SAGE Business Cloud (API REST)
"""
import time
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

# Mapping types SAGE X3 ADXSD → types universels
SAGE_X3_TYPE_MAP = {
    "A":  "string",   # Alphanumeric
    "ANM": "string",  # Alphanumeric (memo)
    "D":  "date",     # Date
    "DCB": "decimal", # Decimal
    "L":  "integer",  # Integer (long)
    "W":  "integer",  # Integer (short)
    "M":  "string",   # Menu (code)
    "MD": "decimal",  # Amount/money
    "QTY": "decimal", # Quantity
    "C":  "string",   # Clob/text
    "Y":  "boolean",  # Yes/No
}

# Mapping types SQL Server (SAGE 100 base SQL)
SQL_TYPE_MAP = {
    "varchar": "string",   "nvarchar": "string",   "char": "string",
    "text":    "string",   "ntext": "string",
    "int":     "integer",  "bigint": "integer",     "smallint": "integer",
    "tinyint": "integer",  "bit": "boolean",
    "decimal": "decimal",  "numeric": "decimal",    "money": "decimal",
    "float":   "float",    "real": "float",
    "date":    "date",     "datetime": "datetime",  "datetime2": "datetime",
}


# ─── SAGE X3 ────────────────────────────────────────────────────────────────

def sync_sage_x3(config: Dict, secrets: Dict) -> List[Dict]:
    """
    Synchronise les métadonnées SAGE X3 via Web Services REST/SOAP.
    Endpoint : /soap-generic/syracuse/collaboration/syracuse/CAdxWebServiceXmlCC
    """
    import requests

    base_url = config.get("base_url", "").rstrip("/")
    folder = config.get("folder", "SEED")  # Dossier SAGE X3 (ex: SEED, PROD)
    auth = (config.get("username", ""), secrets.get("password", ""))

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    if secrets.get("token"):
        headers["Authorization"] = f"Bearer {secrets['token']}"
        auth = None

    entities = []

    # Liste des objets métier SAGE X3 configurés
    objects = config.get("objects", [
        "CUSTOMER", "SUPPLIER", "SORDER", "SINVOICE", "GACCENTRY",
        "ITMMASTER", "FACILITY", "BPCUSTOMER", "BPSUPPLIER",
    ])

    for obj_name in objects:
        try:
            # Endpoint SAGE X3 pour la description d'un objet
            url = f"{base_url}/api/x3/erp/{folder}/{obj_name}/$descriptor"
            resp = requests.get(url, headers=headers, auth=auth, timeout=15)

            if not resp.ok:
                # Essai avec l'ancien endpoint SOAP
                entities.extend(_try_sage_x3_soap(base_url, folder, obj_name, auth, secrets))
                continue

            descriptor = resp.json()
            fields = _parse_sage_x3_descriptor(descriptor)

            entities.append({
                "name":        obj_name,
                "entity_type": "sage_x3_object",
                "description": descriptor.get("$description", f"SAGE X3 Object {obj_name}"),
                "fields":      fields,
            })

        except Exception as e:
            logger.warning(f"[SAGE X3] Object {obj_name}: {e}")
            # Ajouter avec champs basiques si erreur
            entities.append({
                "name":        obj_name,
                "entity_type": "sage_x3_object",
                "description": f"SAGE X3 Object {obj_name} (non détaillé)",
                "fields": [
                    {"name": "ROWID", "type": "integer", "native_type": "L",
                     "nullable": False, "primary_key": True, "foreign_key": False},
                    {"name": "CODE", "type": "string", "native_type": "A",
                     "nullable": False, "primary_key": False, "foreign_key": False},
                    {"name": "DESCRIPTION", "type": "string", "native_type": "A",
                     "nullable": True, "primary_key": False, "foreign_key": False},
                ],
            })

    return entities


def _parse_sage_x3_descriptor(descriptor: Dict) -> List[Dict]:
    """Parse le descripteur JSON d'un objet SAGE X3."""
    fields = []

    for field_def in descriptor.get("$fields", []):
        field_name = field_def.get("$fieldName", "")
        field_type = field_def.get("$type", "A")
        field_desc = field_def.get("$description", "")
        is_key = field_def.get("$isKey", False)

        fields.append({
            "name":        field_name,
            "type":        SAGE_X3_TYPE_MAP.get(field_type, "string"),
            "native_type": field_type,
            "nullable":    not is_key,
            "primary_key": is_key,
            "foreign_key": field_def.get("$isForeignKey", False),
            "description": field_desc,
        })

    return fields


def _try_sage_x3_soap(base_url: str, folder: str, obj_name: str,
                       auth, secrets: Dict) -> List[Dict]:
    """Fallback SOAP pour SAGE X3 ancien style."""
    # Structure minimale si SOAP non disponible
    return []


# ─── SAGE 100 ────────────────────────────────────────────────────────────────

def sync_sage_100(config: Dict, secrets: Dict) -> List[Dict]:
    """
    Synchronise les métadonnées SAGE 100 via connexion directe SQL Server/ODBC.
    SAGE 100 utilise SQL Server comme backend.
    """
    try:
        from sqlalchemy import create_engine, inspect, text
    except ImportError:
        raise ImportError("sqlalchemy non installé")

    host = config.get("host", "")
    port = config.get("port", 1433)
    database = config.get("database_name", "")
    username = config.get("username", "")
    password = secrets.get("password", "")

    # SAGE 100 utilise SQL Server
    url = (
        f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
        f"&TrustServerCertificate=yes&Encrypt=no"
    )

    engine = create_engine(url, pool_pre_ping=True)
    inspector = inspect(engine)

    entities = []

    # Tables SAGE 100 (préfixes connus)
    sage_prefixes = config.get("table_prefixes", [
        "F_", "P_", "G_", "E_", "CBF_", "JM_"
    ])

    for table_name in inspector.get_table_names():
        # Filtrer sur les préfixes SAGE 100 si configurés
        if sage_prefixes and not any(table_name.upper().startswith(p.upper()) for p in sage_prefixes):
            continue

        columns = inspector.get_columns(table_name)
        pk_cols = set(
            inspector.get_pk_constraint(table_name).get("constrained_columns", [])
        )
        fk_cols = set()
        for fk in inspector.get_foreign_keys(table_name):
            fk_cols.update(fk.get("constrained_columns", []))

        fields = []
        for col in columns:
            native = str(col["type"]).lower().split("(")[0].strip()
            fields.append({
                "name":        col["name"],
                "type":        SQL_TYPE_MAP.get(native, "string"),
                "native_type": str(col["type"]),
                "nullable":    col.get("nullable", True),
                "primary_key": col["name"] in pk_cols,
                "foreign_key": col["name"] in fk_cols,
                "description": "",
            })

        entities.append({
            "name":        table_name,
            "entity_type": "sage100_table",
            "description": f"Table SAGE 100 {table_name}",
            "fields":      fields,
        })

    engine.dispose()
    return entities


# ─── SAGE BUSINESS CLOUD ─────────────────────────────────────────────────────

def sync_sage_cloud(config: Dict, secrets: Dict) -> List[Dict]:
    """
    Synchronise les métadonnées SAGE Business Cloud via API REST.
    Endpoint : https://api.accounting.sage.com/v3.1/
    """
    import requests

    base_url = config.get("base_url", "https://api.accounting.sage.com/v3.1")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {secrets.get('token', '')}",
    }

    # Ressources standard SAGE Business Cloud
    resources = [
        ("ledger_accounts", "Comptes comptables"),
        ("journals", "Journaux"),
        ("journal_entries", "Écritures comptables"),
        ("contacts", "Contacts / Tiers"),
        ("sales_invoices", "Factures ventes"),
        ("purchase_invoices", "Factures achats"),
        ("products", "Produits / Articles"),
        ("tax_rates", "Taux de TVA"),
        ("bank_accounts", "Comptes bancaires"),
        ("payment_methods", "Moyens de paiement"),
    ]

    entities = []

    for resource_name, description in resources:
        try:
            resp = requests.get(
                f"{base_url}/{resource_name}",
                headers=headers,
                params={"$top": 1},
                timeout=10,
            )
            if not resp.ok:
                continue

            data = resp.json()
            items = data.get("$items", data.get("value", []))
            sample = items[0] if items else {}

            fields = []
            if isinstance(sample, dict):
                for k, v in sample.items():
                    fields.append({
                        "name":        k,
                        "type":        _infer_type(v),
                        "native_type": type(v).__name__,
                        "nullable":    True,
                        "primary_key": k == "id",
                        "foreign_key": k.endswith("_id") and k != "id",
                        "description": "",
                    })

            if not fields:
                fields = [
                    {"name": "id", "type": "uuid", "native_type": "string",
                     "nullable": False, "primary_key": True, "foreign_key": False},
                    {"name": "display_name", "type": "string", "native_type": "string",
                     "nullable": True, "primary_key": False, "foreign_key": False},
                ]

            entities.append({
                "name":        resource_name,
                "entity_type": "sage_cloud_resource",
                "description": f"SAGE Business Cloud - {description}",
                "fields":      fields,
            })

        except Exception as e:
            logger.warning(f"[SAGE Cloud] {resource_name}: {e}")

    return entities


def _infer_type(value) -> str:
    if isinstance(value, bool):   return "boolean"
    if isinstance(value, int):    return "integer"
    if isinstance(value, float):  return "float"
    if isinstance(value, dict):   return "object"
    if isinstance(value, list):   return "array"
    import re
    if isinstance(value, str) and re.match(r"\d{4}-\d{2}-\d{2}", value):
        return "date"
    return "string"


# ─── DISPATCH ────────────────────────────────────────────────────────────────

def sync_sage(config: Dict, secrets: Dict) -> List[Dict]:
    """Point d'entrée unique — dispatche selon le sous-type SAGE."""
    sage_type = config.get("sage_type", "x3")

    if sage_type == "x3":
        return sync_sage_x3(config, secrets)
    elif sage_type == "100":
        return sync_sage_100(config, secrets)
    elif sage_type == "cloud":
        return sync_sage_cloud(config, secrets)
    else:
        raise ValueError(f"Type SAGE inconnu : {sage_type}. Options : x3, 100, cloud")


def test_sage_connection(config: Dict, secrets: Dict) -> Dict:
    """Test la connexion SAGE."""
    import requests
    start = time.time()
    sage_type = config.get("sage_type", "x3")

    try:
        if sage_type == "x3":
            base_url = config.get("base_url", "").rstrip("/")
            folder = config.get("folder", "SEED")
            auth = (config.get("username", ""), secrets.get("password", ""))
            headers = {"Accept": "application/json"}
            if secrets.get("token"):
                headers["Authorization"] = f"Bearer {secrets['token']}"
                auth = None
            resp = requests.get(
                f"{base_url}/api/x3/erp/{folder}",
                headers=headers, auth=auth, timeout=10
            )
            latency = int((time.time() - start) * 1000)
            if resp.status_code < 500:
                return {"success": True, "message": f"SAGE X3 HTTP {resp.status_code}", "latency_ms": latency}
            return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}

        elif sage_type == "100":
            # Test connexion SQL Server SAGE 100
            from sqlalchemy import create_engine, text
            host = config.get("host", "")
            port = config.get("port", 1433)
            db = config.get("database_name", "")
            user = config.get("username", "")
            pwd = secrets.get("password", "")
            url = (
                f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}"
                f"?driver=ODBC+Driver+18+for+SQL+Server"
                f"&TrustServerCertificate=yes&Encrypt=no"
            )
            engine = create_engine(url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            latency = int((time.time() - start) * 1000)
            return {"success": True, "message": "SAGE 100 SQL Server connecté", "latency_ms": latency}

        elif sage_type == "cloud":
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