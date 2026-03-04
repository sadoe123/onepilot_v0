CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS data_sources (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name             VARCHAR(255) NOT NULL,
    description      TEXT,
    category         VARCHAR(50)  NOT NULL DEFAULT 'database',
    connector_type   VARCHAR(50)  NOT NULL,
    status           VARCHAR(50)  NOT NULL DEFAULT 'pending',
    host             VARCHAR(255),
    port             INTEGER,
    database_name    VARCHAR(255),
    schema_name      VARCHAR(255),
    base_url         TEXT,
    auth_type        VARCHAR(50)  NOT NULL DEFAULT 'none',
    username         VARCHAR(255),
    options          JSONB        NOT NULL DEFAULT '{}',
    tags             TEXT[]       NOT NULL DEFAULT '{}',
    entity_count     INTEGER      NOT NULL DEFAULT 0,
    test_latency_ms  INTEGER,
    error_message    TEXT,
    last_tested_at   TIMESTAMP WITH TIME ZONE,
    last_synced_at   TIMESTAMP WITH TIME ZONE,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS connection_secrets (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id    UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    secret_key   VARCHAR(100) NOT NULL,
    secret_value TEXT NOT NULL,
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(source_id, secret_key)
);

CREATE TABLE IF NOT EXISTS connection_tests (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id   UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    success     BOOLEAN NOT NULL,
    latency_ms  INTEGER,
    message     TEXT,
    tested_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_entities (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id    UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    name         VARCHAR(255) NOT NULL,
    display_name VARCHAR(255),
    entity_type  VARCHAR(50)  NOT NULL DEFAULT 'table',
    description  TEXT,
    row_count    BIGINT,
    is_visible   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS entity_fields (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id      UUID NOT NULL REFERENCES source_entities(id) ON DELETE CASCADE,
    name           VARCHAR(255) NOT NULL,
    display_name   VARCHAR(255),
    data_type      VARCHAR(50)  NOT NULL DEFAULT 'string',
    native_type    VARCHAR(100),
    is_nullable    BOOLEAN NOT NULL DEFAULT TRUE,
    is_primary_key BOOLEAN NOT NULL DEFAULT FALSE,
    is_foreign_key BOOLEAN NOT NULL DEFAULT FALSE,
    position       INTEGER NOT NULL DEFAULT 0,
    UNIQUE(entity_id, name)
);

CREATE TABLE IF NOT EXISTS entity_relations (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id         UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    source_entity_id  UUID REFERENCES source_entities(id) ON DELETE CASCADE,
    target_entity_id  UUID REFERENCES source_entities(id) ON DELETE CASCADE,
    source_field      VARCHAR(255),
    target_field      VARCHAR(255),
    relation_type     VARCHAR(50) NOT NULL DEFAULT 'many_to_one',
    confidence        FLOAT NOT NULL DEFAULT 1.0,
    is_confirmed      BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_data_sources_category   ON data_sources(category);
CREATE INDEX IF NOT EXISTS idx_data_sources_status     ON data_sources(status);
CREATE INDEX IF NOT EXISTS idx_data_sources_type       ON data_sources(connector_type);
CREATE INDEX IF NOT EXISTS idx_source_entities_source  ON source_entities(source_id);
CREATE INDEX IF NOT EXISTS idx_entity_fields_entity    ON entity_fields(entity_id);
CREATE INDEX IF NOT EXISTS idx_connection_tests_source ON connection_tests(source_id);