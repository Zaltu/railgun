CREATE TABLE schemas (
    uid INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    host TEXT NOT NULL,
    db_type TEXT NOT NULL,
    _ss_archived BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE entities (
    uid INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    code TEXT NOT NULL,
    soloname TEXT NOT NULL,
    multiname TEXT NOT NULL,
    display_name_col TEXT NOT NULL,
    _ss_archived BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE fields (
    uid INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    field_type TEXT NOT NULL,
    indexed BOOLEAN NOT NULL DEFAULT false,
    params JSONB NOT NULL,
    description TEXT,
    _ss_archived BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE _ss_entities_schemas (
    entities_col TEXT NOT NULL,
    fk_entities INT NOT NULL REFERENCES entities (uid) ON DELETE CASCADE,
    uid INT GENERATED ALWAYS AS IDENTITY,
    fk_schemas INT NOT NULL REFERENCES schemas (uid) ON DELETE CASCADE,
    schemas_col TEXT NOT NULL
);

CREATE TABLE _ss_fields_entities (
    fields_col TEXT NOT NULL,
    fk_fields INT NOT NULL REFERENCES fields (uid) ON DELETE CASCADE,
    uid INT GENERATED ALWAYS AS IDENTITY,
    fk_entities INT NOT NULL REFERENCES entities (uid) ON DELETE CASCADE,
    entities_col TEXT NOT NULL
);

INSERT INTO schemas (code, name, host, db_type) VALUES ('railgun_internal', 'Railgun Internal', 'stellardb', 'PSQL');
INSERT INTO schemas (code, name, host, db_type) VALUES ('archive', 'Archive', 'localhost:4414', 'PSQL');

INSERT INTO entities (code, multiname, soloname, display_name_col) VALUES ('schemas', 'Schemas', 'Schema', 'name');
INSERT INTO _ss_entities_schemas (entities_col, fk_entities, fk_schemas, schemas_col) VALUES ('schema', 1, 1, 'entities');

INSERT INTO entities (code, multiname, soloname, display_name_col) VALUES ('entities', 'Entities', 'Entity', 'soloname');
INSERT INTO _ss_entities_schemas (entities_col, fk_entities, fk_schemas, schemas_col) VALUES ('schema', 2, 1, 'entities');

INSERT INTO entities (code, multiname, soloname, display_name_col) VALUES ('fields', 'Fields', 'Field', 'name');
INSERT INTO _ss_entities_schemas (entities_col, fk_entities, fk_schemas, schemas_col) VALUES ('schema', 3, 1, 'entities');

INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('uid', 'ID', 'INT', true, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 1, 1, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('code', 'Code', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 2, 1, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('name', 'Display Name', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 3, 1, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('host', 'Host', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 4, 1, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('db_type', 'Database Type', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 5, 1, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('entities', 'Entities', 'MULTIENTITY', false, '{"constraints":{"Entity": {"relation": "_ss_entities_schemas", "table": "entities", "col": "schema"}}}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 6, 1, 'fields');

INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('uid', 'ID', 'INT', true, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 7, 2, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('code', 'Code', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 8, 2, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('soloname', 'Display Name', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 9, 2, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('multiname', 'Plural Name', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 10, 2, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('display_name_col', 'Display Name Column', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 11, 2, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('schema', 'Schema', 'ENTITY', false, '{"constraints":{"Schema": {"relation": "_ss_entities_schemas", "table": "schemas", "col": "entities"}}}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 12, 2, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('fields', 'Fields', 'MULTIENTITY', false, '{"constraints":{"Field": {"relation": "_ss_fields_entities", "table": "fields", "col": "entity"}}}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 13, 2, 'fields');

INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('uid', 'ID', 'INT', true, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 14, 3, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('code', 'Code', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 15, 3, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('name', 'Display Name', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 16, 3, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('field_type', 'Field Type', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 17, 3, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('indexed', 'Indexed', 'BOOL', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 18, 3, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('params', 'Parameters', 'JSON', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 19, 3, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('description', 'Description', 'TEXT', false, '{}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 20, 3, 'fields');
INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('entity', 'Entity', 'MULTIENTITY', false, '{"constraints":{"Entity": {"relation": "_ss_fields_entities", "table": "entities", "col": "fields"}}}');
INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', 21, 3, 'fields');
