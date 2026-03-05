"""
OnePilot - SAP Connector
Supporte : SAP RFC/BAPI + SAP OData (S/4HANA)
"""
import time
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

ABAP_TYPE_MAP = {
    "C": "string",   "N": "string",   "D": "date",    "T": "string",
    "I": "integer",  "P": "decimal",  "F": "float",   "X": "string",
    "S": "integer",  "B": "integer",
    "STRING": "string", "XSTRING": "string",
    "INT1": "integer", "INT2": "integer", "INT4": "integer", "INT8": "integer",
    "CURR": "decimal", "QUAN": "decimal", "DEC": "decimal",
    "DATS": "date", "TIMS": "string", "CHAR": "string", "NUMC": "string",
}

EDM_TYPE_MAP = {
    "Edm.String": "string",    "Edm.Int32": "integer",  "Edm.Int64": "integer",
    "Edm.Int16": "integer",    "Edm.Byte": "integer",   "Edm.Decimal": "decimal",
    "Edm.Double": "float",     "Edm.Single": "float",   "Edm.Boolean": "boolean",
    "Edm.DateTime": "datetime","Edm.DateTimeOffset": "datetime",
    "Edm.Date": "date",        "Edm.Guid": "uuid",      "Edm.Binary": "string",
}


def sync_sap_rfc(config: Dict, secrets: Dict) -> List[Dict]:
    """
    Synchronise les métadonnées SAP via RFC.
    Explore les tables SAP via DD02L (dictionnaire ABAP).
    """
    try:
        import pyrfc
    except ImportError:
        raise ImportError(
            "pyrfc non installé. Installez SAP NetWeaver RFC SDK + pip install pyrfc"
        )

    conn = pyrfc.Connection(
        ashost=config.get("host", ""),
        sysnr=config.get("system_number", "00"),
        client=config.get("client", "100"),
        user=config.get("username", ""),
        passwd=secrets.get("password", ""),
        lang=config.get("lang", "FR"),
    )

    entities = []
    table_names = config.get("tables", [])

    # Si pas de tables configurées, lire depuis DD02L (tables actives)
    if not table_names:
        result = conn.call(
            "RFC_READ_TABLE",
            QUERY_TABLE="DD02L",
            FIELDS=[{"FIELDNAME": "TABNAME"}],
            OPTIONS=[{"TEXT": "TABCLASS = 'TRANSP'"}],
            ROWCOUNT=500,
        )
        table_names = [
            row["WA"].strip()
            for row in result.get("DATA", [])
            if row.get("WA", "").strip()
        ]

    for table_name in table_names[:200]:  # Limite 200 tables
        try:
            result = conn.call(
                "RFC_GET_STRUCTURE_DEFINITION",
                TABNAME=table_name,
            )
            fields = []
            for f in result.get("FIELDS", []):
                fields.append({
                    "name":        f.get("FIELDNAME", ""),
                    "type":        ABAP_TYPE_MAP.get(f.get("DATATYPE", "C"), "string"),
                    "native_type": f.get("DATATYPE", ""),
                    "nullable":    True,
                    "primary_key": f.get("KEYFLAG", "") == "X",
                    "foreign_key": False,
                    "description": f.get("FIELDTEXT", ""),
                })
            if fields:
                entities.append({
                    "name":        table_name,
                    "entity_type": "sap_table",
                    "description": f"Table SAP ABAP {table_name}",
                    "fields":      fields,
                })
        except Exception as e:
            logger.warning(f"[SAP RFC] Table {table_name}: {e}")

    # BAPIs configurées
    for fm in config.get("function_modules", []):
        try:
            result = conn.call("RFC_GET_FUNCTION_INTERFACE", FUNCNAME=fm)
            fields = [
                {
                    "name":        p.get("PARAMETER", ""),
                    "type":        ABAP_TYPE_MAP.get(p.get("TABNAME", "C"), "string"),
                    "native_type": p.get("TABNAME", ""),
                    "nullable":    True,
                    "primary_key": False,
                    "foreign_key": False,
                    "description": p.get("PARAMTEXT", ""),
                }
                for p in result.get("PARAMS_AND_EXCPS", [])
            ]
            entities.append({
                "name":        fm,
                "entity_type": "sap_bapi",
                "description": f"BAPI/Function Module SAP {fm}",
                "fields":      fields,
            })
        except Exception as e:
            logger.warning(f"[SAP RFC] BAPI {fm}: {e}")

    conn.close()
    return entities


def sync_sap_odata(base_url: str, headers: Dict) -> List[Dict]:
    """
    Synchronise les métadonnées SAP via OData (S/4HANA, SAP Gateway).
    Découverte automatique via $metadata.
    """
    import requests
    import xml.etree.ElementTree as ET

    meta_headers = {k: v for k, v in headers.items() if k != "Accept"}
    meta_headers["Accept"] = "application/xml, text/xml, */*"

    # SAP Gateway liste les services OData disponibles
    services_url = f"{base_url}/sap/opu/odata/IWFND/CATALOGSERVICE/ServiceCollection"
    services = []
    try:
        resp = requests.get(services_url, headers=meta_headers, timeout=10)
        if resp.ok:
            data = resp.json()
            services = [
                s.get("TechnicalServiceName", "")
                for s in data.get("value", [])
                if s.get("TechnicalServiceName")
            ]
    except Exception:
        pass

    # Si pas de catalogue, utiliser le service configuré directement
    if not services:
        service_path = base_url if base_url.endswith("/") else base_url + "/"
        services = [service_path]

    entities = []
    for service in services[:20]:  # Limite 20 services
        try:
            if not service.startswith("http"):
                meta_url = f"{base_url}/sap/opu/odata/sap/{service}/$metadata"
            else:
                meta_url = f"{service}/$metadata"

            resp = requests.get(meta_url, headers=meta_headers, timeout=15)
            if not resp.ok:
                continue

            root = ET.fromstring(resp.text)
            EDM_NS = "http://docs.oasis-open.org/odata/ns/edm"
            ns = f"{{{EDM_NS}}}"

            # Fallback namespace pour OData v2 SAP
            if not list(root.iter(f"{ns}EntityType")):
                ns = "{http://schemas.microsoft.com/ado/2008/09/edm}"

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
                    sap_label = prop.get(
                        "{http://www.sap.com/Protocols/SAPData}label", ""
                    )
                    fields.append({
                        "name":        prop_name,
                        "type":        EDM_TYPE_MAP.get(prop_type, "string"),
                        "native_type": prop_type,
                        "nullable":    prop.get("Nullable", "true").lower() != "false",
                        "primary_key": prop_name in pk_set,
                        "foreign_key": False,
                        "description": sap_label,
                    })

                for nav in elem.findall(f"{ns}NavigationProperty"):
                    fields.append({
                        "name":        nav.get("Name", ""),
                        "type":        "relation",
                        "native_type": nav.get("Type", ""),
                        "nullable":    True,
                        "primary_key": False,
                        "foreign_key": True,
                    })

                if fields:
                    entities.append({
                        "name":        f"{service}__{name}" if len(services) > 1 else name,
                        "entity_type": "sap_odata_entity",
                        "description": f"SAP OData Entity - {service}",
                        "fields":      fields,
                    })
        except Exception as e:
            logger.warning(f"[SAP OData] Service {service}: {e}")

    return entities


def test_sap_connection(config: Dict, secrets: Dict) -> Dict:
    """Test la connexion SAP RFC ou OData."""
    start = time.time()
    connector_type = config.get("sap_type", "odata")

    if connector_type == "rfc":
        try:
            import pyrfc
            conn = pyrfc.Connection(
                ashost=config.get("host", ""),
                sysnr=config.get("system_number", "00"),
                client=config.get("client", "100"),
                user=config.get("username", ""),
                passwd=secrets.get("password", ""),
            )
            conn.call("RFC_PING")
            conn.close()
            return {
                "success": True,
                "message": "SAP RFC_PING OK",
                "latency_ms": int((time.time() - start) * 1000),
            }
        except ImportError:
            return {"success": False, "message": "pyrfc non installé", "latency_ms": -1}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": -1}
    else:
        # OData test
        import requests
        try:
            url = config.get("base_url", "")
            headers = {"Accept": "application/xml, text/xml, */*"}
            if secrets.get("token"):
                headers["Authorization"] = f"Bearer {secrets['token']}"
            resp = requests.get(f"{url}/$metadata", headers=headers, timeout=10)
            latency = int((time.time() - start) * 1000)
            if resp.status_code < 500:
                return {"success": True, "message": f"SAP OData HTTP {resp.status_code}", "latency_ms": latency}
            return {"success": False, "message": f"HTTP {resp.status_code}", "latency_ms": latency}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": -1}