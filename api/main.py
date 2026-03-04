"""
OnePilot – API FastAPI
Universal Data Access Layer – Phase 2
"""
from __future__ import annotations

import logging
import os
import shutil
import io
import csv as csv_module
import json as json_module
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .database import init_schema, close_connections, get_pg_pool, get_redis
from .schemas  import (
    DataSourceCreate, DataSourceUpdate, DataSourceOut,
    DataSourceDetail, DataSourceList, ConnectionTestResult,
    MetadataSyncResult, ConnectorType, SourceCategory
)
from .repository import (
    create_source, list_sources, get_source,
    get_source_with_entities, update_source, delete_source
)
from .connection_service import test_connection, sync_metadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

UPLOAD_DIR = "/tmp/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 OnePilot API démarrage...")
    try:
        await get_pg_pool()
        await init_schema()
        logger.info("✅ PostgreSQL connecté et schema initialisé")
    except Exception as e:
        logger.error(f"❌ PostgreSQL erreur: {e}")
    try:
        await get_redis()
        logger.info("✅ Redis connecté")
    except Exception as e:
        logger.warning(f"⚠️  Redis non disponible: {e}")
    yield
    await close_connections()
    logger.info("👋 OnePilot API arrêté")


app = FastAPI(
    title="OnePilot API",
    description="Universal Data Access Layer – Phase 2",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health ────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
async def root():
    return {"service": "OnePilot", "version": "2.0.0", "status": "running", "docs": "/docs"}


@app.get("/health", tags=["Health"])
async def health():
    checks = {}
    try:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"error: {e}"
    try:
        r = await get_redis()
        await r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"unavailable: {e}"

    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if all_ok else 207,
        content={"status": "healthy" if all_ok else "degraded", "checks": checks}
    )


# ── Connector Types ───────────────────────────────────────────

@app.get("/connector-types", tags=["Meta"])
async def get_connector_types():
    return {
        "database": {
            "label": "Base de données directe",
            "description": "Connexion directe via driver SQL",
            "icon": "database",
            "types": [
                {"id": "postgresql", "label": "PostgreSQL",  "icon": "🐘", "default_port": 5432},
                {"id": "mysql",      "label": "MySQL",       "icon": "🐬", "default_port": 3306},
                {"id": "mssql",      "label": "SQL Server",  "icon": "🪟", "default_port": 1433},
                {"id": "sqlite",     "label": "SQLite",      "icon": "📁", "default_port": None},
            ]
        },
        "webservice": {
            "label": "Web Service / API",
            "description": "Connexion via protocole HTTP",
            "icon": "globe",
            "types": [
                {"id": "rest",    "label": "REST API",         "icon": "🔗", "default_port": None},
                {"id": "odata",   "label": "OData (Dynamics)", "icon": "⚡", "default_port": None},
                {"id": "graphql", "label": "GraphQL",          "icon": "◈",  "default_port": None},
                {"id": "soap",    "label": "SOAP / WSDL",      "icon": "📮", "default_port": None},
                {"id": "sap_rfc", "label": "SAP RFC/BAPI",     "icon": "🔷", "default_port": 3300},
            ]
        },
        "file": {
            "label": "Fichiers",
            "description": "Import de fichiers locaux ou upload",
            "icon": "file",
            "types": [
                {"id": "file_csv",   "label": "CSV",   "icon": "📄", "default_port": None},
                {"id": "file_excel", "label": "Excel", "icon": "📊", "default_port": None},
                {"id": "file_json",  "label": "JSON",  "icon": "📋", "default_port": None},
            ]
        }
    }


# ── Sources CRUD ──────────────────────────────────────────────

@app.post("/sources", response_model=DataSourceOut, status_code=201, tags=["Sources"])
async def create_data_source(data: DataSourceCreate):
    try:
        return await create_source(data)
    except ValueError as e:
        raise HTTPException(422, str(e))
    except Exception as e:
        logger.error(f"[API] create_source: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@app.get("/sources", response_model=DataSourceList, tags=["Sources"])
async def list_data_sources(
    category: Optional[str] = Query(None),
    status:   Optional[str] = Query(None),
    search:   Optional[str] = Query(None),
):
    sources = await list_sources(category=category, status=status, search=search)
    return DataSourceList(total=len(sources), sources=sources)


@app.get("/sources/{source_id}", tags=["Sources"])
async def get_data_source(
    source_id: UUID,
    page:      int = Query(1, ge=1, description="Numéro de page"),
    page_size: int = Query(50, ge=1, le=200, description="Entités par page"),
    search:    str = Query("", description="Filtrer les entités par nom"),
):
    """
    Retourne une source avec ses entités paginées.
    - page: numéro de page (défaut 1)
    - page_size: entités par page (défaut 50, max 200)
    - search: filtre sur le nom de l'entité
    """
    source = await get_source_with_entities(
        source_id, page=page, page_size=page_size, search=search
    )
    if not source:
        raise HTTPException(404, f"Source {source_id} introuvable")

    pagination = source.__dict__.get("_pagination", {})
    data = source.model_dump()
    data["pagination"] = pagination
    return data


@app.patch("/sources/{source_id}", response_model=DataSourceOut, tags=["Sources"])
async def update_data_source(source_id: UUID, data: DataSourceUpdate):
    source = await update_source(source_id, data)
    if not source:
        raise HTTPException(404, f"Source {source_id} introuvable")
    return source


@app.delete("/sources/{source_id}", status_code=204, tags=["Sources"])
async def delete_data_source(source_id: UUID):
    deleted = await delete_source(source_id)
    if not deleted:
        raise HTTPException(404, f"Source {source_id} introuvable")


# ── Test + Sync ───────────────────────────────────────────────

@app.post("/sources/{source_id}/test", response_model=ConnectionTestResult, tags=["Connections"])
async def test_source_connection(source_id: UUID):
    source = await get_source(source_id)
    if not source:
        raise HTTPException(404, f"Source {source_id} introuvable")

    result = await test_connection(source_id)
    return ConnectionTestResult(
        source_id=source_id,
        success=result["success"],
        message=result["message"],
        latency_ms=result.get("latency_ms", -1),
        tested_at=result.get("tested_at") or __import__("datetime").datetime.utcnow(),
    )


@app.post("/sources/{source_id}/sync", response_model=MetadataSyncResult, tags=["Connections"])
async def sync_source_metadata(source_id: UUID):
    source = await get_source(source_id)
    if not source:
        raise HTTPException(404, f"Source {source_id} introuvable")

    result = await sync_metadata(source_id)
    return MetadataSyncResult(source_id=source_id, **result)


@app.get("/sources/{source_id}/entities", tags=["Metadata"])
async def get_source_entities(
    source_id: UUID,
    page:      int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search:    str = Query(""),
):
    """Liste les entités d'une source avec pagination et recherche."""
    source = await get_source_with_entities(
        source_id, page=page, page_size=page_size, search=search
    )
    if not source:
        raise HTTPException(404, f"Source {source_id} introuvable")

    pagination = source.__dict__.get("_pagination", {})
    return {
        "source_id": source_id,
        "entities":  source.entities,
        "pagination": pagination,
    }


# ── File Upload ───────────────────────────────────────────────

@app.post("/upload", tags=["Files"])
async def upload_file(file: UploadFile = File(...)):
    """Upload un fichier CSV ou JSON et retourne un aperçu des données."""
    allowed = {".csv", ".json", ".txt"}
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in allowed:
        raise HTTPException(400, f"Type de fichier non supporté : {ext}. Acceptés : {allowed}")

    content = await file.read()

    # Sauvegarde
    dest = os.path.join(UPLOAD_DIR, file.filename)
    with open(dest, "wb") as f:
        f.write(content)

    # Decode
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    # Preview
    if ext in (".csv", ".txt"):
        reader = csv_module.DictReader(io.StringIO(text))
        rows = []
        for i, row in enumerate(reader):
            if i >= 5:
                break
            rows.append(dict(row))
        columns = list(rows[0].keys()) if rows else []
        return {
            "filename":      file.filename,
            "uploaded_path": dest,
            "size":          len(content),
            "format":        "csv",
            "columns":       columns,
            "column_count":  len(columns),
            "preview":       rows,
            "message":       f"Fichier uploadé — {len(columns)} colonnes détectées"
        }
    elif ext == ".json":
        data = json_module.loads(text)
        if isinstance(data, list):
            preview = data[:5]
            columns = list(preview[0].keys()) if preview else []
        else:
            preview = [data]
            columns = list(data.keys())
        return {
            "filename":      file.filename,
            "uploaded_path": dest,
            "size":          len(content),
            "format":        "json",
            "columns":       columns,
            "column_count":  len(columns),
            "preview":       preview,
            "message":       f"Fichier uploadé — {len(columns)} colonnes détectées"
        }


@app.get("/uploads", tags=["Files"])
async def list_uploads():
    """Liste les fichiers uploadés disponibles."""
    if not os.path.exists(UPLOAD_DIR):
        return {"files": []}
    files = []
    for fname in os.listdir(UPLOAD_DIR):
        fpath = os.path.join(UPLOAD_DIR, fname)
        if os.path.isfile(fpath):
            files.append({
                "filename": fname,
                "path":     fpath,
                "size":     os.path.getsize(fpath),
            })
    return {"files": files}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)