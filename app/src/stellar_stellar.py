import json
from threading import Thread

import redis
import psycopg
from psycopg import sql


class StellarStellar():
    """
    *kira kira*
    """
    COMET_NAME = "STELLAR"  # Redis channel name

    def __init__(self):
        # Prep config
        self._load_config()

        # For you
        self.funny_factory = {
            "field": {
                "create": self.create_field,
                "update": self.update_field,
                "delete": self.delete_field
            },
            "entity": {
                "create": self.create_entity,
                "update": self.update_entity,
                "delete": self.delete_entity
            },
            "schema": {
                "update": self.update_schema,
                "release": self.release_schema
            }
        }

        self._field_factory = {
            "create": {
                "TEXT": self._field_create_simple,
                "INT": self._field_create_simple,
                "FLOAT": self._field_create_simple,
                "DATE": self._field_create_simple,
                "JSON": self._field_create_simple,
                "BOOL": self._field_create_bool,
                "LIST": self._field_create_list,
                "ENTITY": self._field_create_entity,
                "MULTIENTITY": self._field_create_entity
            },
            "update": {
                "LIST": self._field_update_list,
                "ENTITY": self._field_update_entity,
                "MULTIENTITY": self._field_update_entity
            },
            "delete": {
                "TEXT": self._field_delete_simple,
                "INT": self._field_delete_simple,
                "FLOAT": self._field_delete_simple,
                "DATE": self._field_delete_simple,
                "JSON": self._field_delete_simple,
                "BOOL": self._field_delete_simple,
                "LIST": self._field_delete_simple,
                "ENTITY": self._field_delete_entity,
                "MULTIENTITY": self._field_delete_entity
            }
        }

        self.connect()

        # *kira kira*
        self.STELLAR = self.stellar_stellar()

        self.comet = redis.StrictRedis(host="localhost", port=6379, db=0)  # TODO env vars
        self.reach_for_the_stars()


    def _load_config(self):
        """
        Set STELLAR DB config.
        """
        # TODO from env var (docker)
        # Required object attributes
        self.DB_NAME = "railgun_internal"
        self.DB_USER = "railgun"
        # Optional connection attributes
        self.DB_PASSWORD = None
        self.DB_HOST = "localhost"
        self.DB_PORT = 6969


    def connect(self):
        """
        Connect to STELLAR DB (railgun internal) through psycopg.
        Gets the DB version as simple check that the connection was successful.

        :raises ConnectionError: if the version cannot be fetched. This will and should crash the program.
        STELLAR is required.
        """
        self.database = psycopg.connect(
            dbname=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD,
            host=self.DB_HOST,
            port=self.DB_PORT,
            autocommit=True
        )
        self.version = self._run_command("SELECT version()")[0][0]  # lol
        if not self.version:
            raise ConnectionError


    def disconnect(self):
        """
        Closes the DB connection.
        Thoeretically only called when the program closes, never called explicitely.
        """
        self.database.close()


    def _run_command(self, command, params=None, include_descriptors=False, return_style="solo"):
        """
        Execute a (dirty) command. Sanitation is expected to happen first.

        :param str command: Clean SQL command to execute.
        :param list params: additional params to pass to psycopg cursor.execute. Unused, default None.
        :param bool include_descriptors: include the descriptors (column names), default False.
        :param str return_style: how much data this query will return. solo|multi|None

        :returns: SQL result of executed command.
        :rtype: list|tuple
        """
        try:
            with self.database.cursor() as cur:
                cur.execute(command, params)
                match return_style:
                    case "multi":
                        values = cur.fetchall()
                        if include_descriptors:
                            fieldcodes = [desc[0] for desc in cur.description]
                    case "solo":
                        values = cur.fetchone()
                    case _:
                        # Operation not expected to produce anything.
                        return
        except Exception as e:
            raise

        return (values, fieldcodes) if include_descriptors else values


    def reach_for_the_stars(self):
        """
        Set up the listener that will receive schema update requests.
        Update requests must be in the following format (str):
        {
            "level": all|schema|entity,
            > IF SCHEMA
            "schema": <schema_code>
            > IF ENTITY
            "schema": <schema_code>
            "entity": <entity_code>
        }
        Call only once in StellarStellar initialization.
        """
        def listener_handler():
            """
            Actual listen loop to let block within a thread.
            Performance has not been tested. May need to be async-ed a la AIGIS.
            TODO performance tests
            """
            pubsub = self.comet.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(StellarStellar.COMET_NAME)
            for message in pubsub.listen():
                if message["type"] == "message":
                    request = json.loads(message["data"].decode("utf-8"))
                    print("A comet streaks across the sky!")
                    self.stellar_update(request)

        self._listener_thread = Thread(target=listener_handler, daemon=True)
        self._listener_thread.start()


    def shoot_for_the_stars(self, level, schema=None, entity=None):
        """
        Send out notification to all Railgun apps that the schema has changed.
        This is a fire-and-forget operation.

        :param str level: update level, all|schema|entity
        :param int schema: schema to update if level is schema or entity
        :param int entity: entity to update if level is entity
        """
        cannonball = {"level": level}
        if schema:
            cannonball["schema"] = schema
        if entity:
            cannonball["entity"] = entity
        self.comet.publish(StellarStellar.COMET_NAME, json.dumps(cannonball))


    def fetch_schemas(self):
        """
        Direct SQL select on the schemas table. Used to help populate STELLAR, but should not be
        called directly otherwise.

        :returns: registered schemas
        :rtype: list
        """
        COMMAND = """SELECT code, uid, name, host, db_type, _ss_archived FROM schemas;"""
        return self._run_command(COMMAND, return_style="multi")


    def fetch_entities(self, schema):
        """
        Direct SQL select on the entities table. Used to help populate STELLAR, but should not be
        called directly otherwise.

        :param int schema: ID of the schema for which the entities should be fetched.

        :returns: registered entities
        :rtype: list
        """
        COMMAND = sql.SQL("""
            SELECT entities.code, entities.soloname, entities.multiname, entities.display_name_col, entities.uid, entities._ss_archived
            FROM entities
            INNER JOIN _ss_entities_schemas ON _ss_entities_schemas.fk_entities = entities.uid
            INNER JOIN schemas ON schemas.uid = _ss_entities_schemas.fk_schemas
            WHERE schemas.uid = (%s)"""
        )
        return self._run_command(COMMAND, (schema,), return_style="multi")


    def fetch_fields(self, entity):
        """
        Direct SQL select on the fields table. Used to help populate STELLAR, but should not be
        called directly otherwise.

        TODO add schema here too, since table names are not unique per all schemas in theory.

        :param int entity: ID of the entity for which the fields should be fetched.

        :returns: registered fields
        :rtype: list
        """
        COMMAND = sql.SQL("""
            SELECT fields.code, fields.name, fields.field_type, fields.uid, fields.indexed, fields.params, fields._ss_archived
            FROM fields
            INNER JOIN _ss_fields_entities ON _ss_fields_entities.fk_fields = fields.uid
            INNER JOIN entities ON entities.uid = _ss_fields_entities.fk_entities
            WHERE entities.uid = (%s)"""
        )
        return self._run_command(COMMAND, (entity,), return_style="multi")
    
    #####################################
    ########  Stellar  Stellar  #########
    #####################################
    def stellar_stellar(self):
        """
        *kira kira*
        Fetch all schemas and populate STELLAR.
        Subsequent update requests should mostly be done by schema or table.
        This does not update the STELLAR property. Assignation must be done by caller.

        :returns: STELLAR STELLAR
        :rtype: STELLAR
        """
        STELLAR = {}
        schemas = self.fetch_schemas()
        for schema in schemas:
            STELLAR[schema[0]] = {
                "code": schema[0],
                "id": schema[1],
                "name": schema[2],
                "host": schema[3],
                "db_type": schema[4],
                "archived": schema[5]
            }
            STELLAR[schema[0]]["entities"] = self.stellar_schema(schema[1])
        return STELLAR


    def stellar_schema(self, schema_id):
        """
        *kira kira*
        Fetch the STELLAR of a specific schema.
        This does not update the STELLAR property. Assignation must be done by caller.

        :param int schema_id: ID of the schema to fetch

        :returns: STELLAR SCHEMA
        :rtype: STELLAR
        """
        STELLAR = {}
        schema_entities = self.fetch_entities(schema_id)
        for entity in schema_entities:
            STELLAR[entity[1]] = {
                "code": entity[0],
                "soloname": entity[1],
                "multiname": entity[2],
                "display_name_col": entity[3],
                "id": entity[4],
                "archived": entity[5]
            }
            STELLAR[entity[1]]["fields"] = self.stellar_entity(entity[4])
        return STELLAR


    def stellar_entity(self, entity_id):
        """
        *kira kira*
        Fetch the STELLAR of a specific entity.
        This does not update the STELLAR property. Assignation must be done by caller.

        :param int entity_id: ID of the entity to fetch

        :returns: STELLAR ENTITY
        :rtype: STELLAR
        """
        STELLAR = {}
        entity_fields = self.fetch_fields(entity_id)
        for field in entity_fields:
            STELLAR[field[0]] = {
                "code": field[0],
                "name": field[1],
                "type": field[2],
                "id": field[3],
                "index": field[4],
                "params": field[5],
                "archived": field[6]
            }
        return STELLAR


    def stellar_update(self, request):
        """
        Update STELLAR based on received request.
        See StellarStellar.reach_for_the_stars for request format documentation.

        :param dict request: STELLAR update request.
        """
        match request["level"]:
            case "all":
                self.STELLAR = self.stellar_stellar()
            case "schema":
                self.STELLAR[request["schema"]]["entities"] = self.stellar_schema(
                    self.STELLAR[request["schema"]]["id"]
                )
            case "entity":
                self.STELLAR[request["schema"]]["entities"][request["entity"]]["fields"] = self.stellar_entity(
                    self.STELLAR[request["schema"]]["entities"][request["entity"]]["id"]
                )


    def create_field(self, request, db):
        """
        Create a DB column and register to Stellar.
        The steps of field creation depend on the type of field, and so are offloaded to factory
        functions based on type.

        Field creation requests are expected in the format:
        {
            "part": "field",
            "request_type": "create",
            "schema": <schema_code>,
            "entity": <entity_code>,
            "data": {
                "code": <field_code>,
                "name": <field_name>,
                "type": <field_type>
            }
        }
        """
        if request["data"]["type"] not in self._field_factory["create"]:
            raise NotImplementedError

        # Offload creation
        self._field_factory["create"][request["data"]["type"]](request, db)

        # Stellar Stellar
        self.shoot_for_the_stars(level="entity", schema=request["schema"], entity=request["entity"])

        return True


    def update_field(self, request, db):
        """
        Update a field's parameters. The possibilities vary based on field type, so offloaded
        to a factory function.
        Most types actually don't have update options.
        """
        field_sc = self.STELLAR[request["schema"]]["entities"][request["entity"]]["fields"][request["data"]["code"]]
        if field_sc["type"] not in self._field_factory["update"]:
            raise NotImplementedError

        # Offload creation
        self._field_factory["update"][field_sc["type"]](request, db)

        # Stellar Stellar
        self.shoot_for_the_stars(level="entity", schema=request["schema"], entity=request["entity"])
        return True


    def delete_field(self, request, db):
        """
        Archive a field, or delete it if it's already archived.
        Archiving a field involves:
            - Archive the field record: Stellar
            - Shoot for the stars
        Which is common procedure among all field types.
        
        The steps of field deletion depend on the type of field, and so are offloaded to factory
        functions based on type.

        Always Shoot for the stars!

        Field deletion requests are expected in the format:
        {
            "part": "field",
            "request_type": "delete",
            "schema": <schema_code>,
            "entity": <entity_code>,
            "data": {
                "code": <field_code>
            }
        }

        :returns: true to validate deletion
        :rtype: bool
        """
        stellar_field = self.STELLAR[request["schema"]]["entities"][request["entity"]]["fields"][request["data"]["code"]]
        if not stellar_field["archived"]:
            # It hasn't been "hidden" yet. Hide it first.
            HIDE_COMMAND = sql.SQL("""
                UPDATE fields
                SET _ss_archived = true
                WHERE uid = {uid}
            """).format(
                uid=stellar_field["id"]
            )
            self._run_command(HIDE_COMMAND, return_style=None)
        else:
            # This is the real deletion. Boom goes the dynamite
            if stellar_field["type"] not in self._field_factory["delete"]:
                raise NotImplementedError
            # Offload deletion
            self._field_factory["delete"][stellar_field["type"]](request, db)

        # Stellar Stellar
        self.shoot_for_the_stars(level="entity", schema=request["schema"], entity=request["entity"])
        return True


    def create_entity(self, request, db):
        """
        *kira kira*
        Create a new table in a DB.
        This involves:
            - Create physical DB table with default fields: DB
            - Create entity record and relation: Stellar
            - Create default field records and relations: Stellar
            - Shoot for the stars
        Request format for entity creation is expected as:
        {
            "part": "entity",
            "request_type": "create",
            "schema": <schema_code>,
            "data": {
                "code": <entity_code>,
                "soloname": <entity_soloname>,
                "multiname": <entity_multiname>
            }
        }

        :param dict request: entity creation request
        :param db db._database.Database: physical DB connection

        :returns: true to validate completion
        :rtype: bool
        """
        data = request["data"]
        try:
            assert data["code"] not in self.STELLAR[request["schema"]]["entities"]
        except AssertionError:
            return "Table with the name {name} already exists".format(name=data["code"])
        # Real table creation
        db.create_table(data["code"])

        # Stellar table creation
        ENT_COMMAND = sql.SQL("""
            INSERT INTO entities (code, multiname, soloname, display_name_col) VALUES ((%s), (%s), (%s), (%s)) RETURNING uid
        """)
        ent_id = self._run_command(ENT_COMMAND, (data["code"], data["multiname"], data["soloname"], 'code'))[0]

        # Stellar table Relation creation
        REL_COMMAND = sql.SQL("""
            INSERT INTO _ss_entities_schemas (entities_col, fk_entities, fk_schemas, schemas_col) VALUES ((%s), (%s), (%s), (%s))
        """)
        self._run_command(REL_COMMAND, ('schema', ent_id, self.STELLAR[request["schema"]]["id"], 'entities'), return_style=None)

        # Stellar Field and field Relation Creation
        FIELD_COMMANDS = [
            sql.SQL("""
                INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('uid', 'ID', 'INT', true, '{}') RETURNING uid
            """),
            sql.SQL("""
                INSERT INTO fields (code, name, field_type, indexed, params) VALUES ('code', 'Display Name', 'TEXT', false, '{}') RETURNING uid
            """)
        ]
        FREL_COMMAND = sql.SQL("""
            INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ((%s), (%s), (%s), (%s))
        """)
        for com in FIELD_COMMANDS:
            field_id = self._run_command(com)[0]
            self._run_command(FREL_COMMAND, ('entity', field_id, ent_id, 'fields'), return_style=None)

        # Stellar Stellar
        self.shoot_for_the_stars("schema", schema=request["schema"])
        return True


    def update_entity(self, request, db):
        """
        There isn't anything we really want to expose to the user in regards to actual
        ALTER TABLE commands. Column creation is handled separately, and things like
        soloname/multiname shouldn't be done through Stellar.
        """
        raise NotImplementedError


    def delete_entity(self, request, db):
        """
        Archive an entity, or delete it if it's already been archived.

        There's a couple logical ways of doing this, but to keep things
        simple, only the table key is archived as needed. It's assumed
        that Stellar validation is done before doing queries to only
        display unarchived info.

        Archiving an entity involves:
            - Archive the entity record: Stellar
            - Shoot for the stars

        Delete an entity involves:
            - Droping the entity table: DB
            - Droping the entity record: Stellar
            - Droping the entity's field records: Stellar
            - Drop relation tables related to the entity  # TODO, technically blocking for delete>recreate
            - Shoot for the stars

        Request format for entity deletion is expected as:
        {
            "part": "entity",
            "request_type": "delete",
            "schema": <schema_code>,
            "data": {
                "code": <entity_code>
            }
        }

        :param dict request: the entity deletion request
        :param db db._database.Database: physical DB connection

        :returns: true to validate deletion
        :rtype: bool
        """
        # Check if already archived
        prearchived = self.STELLAR[request["schema"]]["entities"][request["data"]["type"]]["archived"]

        if not prearchived:
            # It hasn't been "hidden" yet. Hide it first.
            HIDE_COMMAND = sql.SQL("""
                UPDATE entities
                SET _ss_archived = true
                WHERE uid = {uid}
            """).format(
                uid=sql.Literal(self.STELLAR[request["schema"]]["entities"][request["data"]["type"]]["id"])
            )
            self._run_command(HIDE_COMMAND, return_style=None)
        else:
            # This is a real delete. Boom goes the dynamite.
            # Drop entity table
            db.delete_table(self.STELLAR[request["schema"]]["entities"][request["data"]["type"]]["code"])

            # Drop entity record
            DEL_ENT_COMMAND = sql.SQL("""
                DELETE FROM entities
                WHERE uid = {uid}
            """).format(
                uid=sql.Literal(self.STELLAR[request["schema"]]["entities"][request["data"]["type"]]["id"])
            )
            self._run_command(DEL_ENT_COMMAND, return_style=None)

            # Drop field records
            for field in self.STELLAR[request["schema"]]["entities"][request["data"]["type"]]["fields"].values():
                DEL_FIELD_COMMAND = sql.SQL("""
                    DELETE FROM fields
                    WHERE uid = {uid}
                """).format(
                    uid=sql.Literal(field["id"])
                )
                self._run_command(DEL_FIELD_COMMAND, return_style=None)

        # Stellar Stellar
        self.shoot_for_the_stars(level="schema", schema=request["schema"])
        return True


    def update_schema(self, request, db):
        """
        There really isn't anything that can be updated in regards to the schema level.
        If you want to update the display name, no need to pass through Stellar Stellar.
        """
        raise NotImplementedError


    def release_schema(self, request, db):
        raise NotImplementedError


    #####################################
    #########   FIELD  TYPES   ##########
    #####################################
    def _field_create_simple(self, request, db, nullable=True, default=None):
        """
        Create a "simple" field.
        Many simple types have nothing special to do beyond:
            - Create physical DB field: DB
            - Create field record and relation: Stellar
        The field creation factory sends those simple field types here.

        :param dict request: the field creation request
        :param db._database.Database db: the physical DB connection

        :raises: NotImplementedError gets bubbled up from the DB connection if the type isn't valid
        :returns: true to validate creation
        :rtype: bool
        """
        # Create physical DB field
        # This will raise NotImplementedError if the type isn't valid for this DB connector.
        db.create_field(self.STELLAR[request["schema"]]["entities"][request["entity"]]["code"], request["data"]["code"], request["data"]["type"], nullable=nullable, default=default)

        # Create Stellar record
        F_COMMAND = sql.SQL("""
            INSERT INTO fields (code, name, field_type, indexed, params) VALUES ((%s), (%s), (%s), false, '{}') RETURNING uid;
        """)
        f_id = self._run_command(F_COMMAND, (request["data"]["code"], request["data"]["name"], request["data"]["type"]))[0]

        # Create Stellar relation
        self.__create_stellar_field_relation(f_id, self.STELLAR[request["schema"]]["entities"][request["entity"]]["id"])
        return True


    def _field_create_bool(self, request, db):
        """
        SQL booleans are tertiary... Make sure to set them as not nullable with false (unchecked) default.
        The rest is the same though.
        This could be configured as a lambda in the factory, but felt cleaner to make it explicit here.

        :param dict request: the field creation request
        :param db._database.Database db: the physical DB connection

        :raises: NotImplementedError gets bubbled up from the DB connection if the type isn't valid
        :returns: true to validate creation
        :rtype: bool
        """
        return self._field_create_simple(request, db, nullable=False, default="false")


    def _field_create_list(self, request, db):
        """
        List fields require some extra setup to create the constraint structure used by Railgun.
        We do not use enums for list fields as they cannot be easily modified in SQL.
        Creating a list/enum field involves the following:
            - Create physical field: DB
            - Create field record: Stellar

        List field creation requires the additional "data/options" key in the request,
        representing a list of the options that should be available for the field:
        {
            "part": "field",
            "request_type": "create",
            "schema": <schema_code>,
            "entity": <entity_code>,
            "data": {
                "code": <field_code>,
                "name": <field_name>,
                "type": "LIST"

                "options": [<list_options>]
            }
        }

        :param dict request: the field creation request
        :param db._database.Database db: the physical DB connection

        :returns: true to validate creation
        :rtype: bool
        """
        # Create field
        db.create_field(self.STELLAR[request["schema"]]["entities"][request["entity"]]["code"], request["data"]["code"], "TEXT")

        # Create Stellar record
        LF_PARAMS = {
            "constraints": request["data"]["options"]
        }
        LF_COMMAND = sql.SQL("""
            INSERT INTO fields (code, name, field_type, indexed, params) VALUES ((%s), (%s), (%s), false, (%s)) RETURNING uid;
        """)
        f_id = self._run_command(LF_COMMAND, (request["data"]["code"], request["data"]["name"], "LIST", json.dumps(LF_PARAMS)))[0]

        # Create Stellar relation
        self.__create_stellar_field_relation(f_id, self.STELLAR[request["schema"]]["entities"][request["entity"]]["id"])
        return True


    def _field_create_entity(self, request, db):
        """
        Creating an entity link is the most convoluted process of all. Thanks SQL.
        It generally involves the following steps:
            - Create a relation field for each target entity in the physical DB: DB
            - Create a field record for the source entity: Stellar
            - Create a field record for the target entity: Stellar
            - Shoot for the stars
        Entity field creation requires the additional "data/options" key in the request,
        representing a list of the foreign tables (entity types) that the entity field
        should link to:
        {
            "part": "field",
            "request_type": "create",
            "schema": <schema_code>,
            "entity": <entity_code>,
            "data": {
                "code": <field_code>,
                "name": <field_name>,
                "type": "ENTITY"|"MUTLIENTITY"

                "options": [<ftables>]

            }
        }

        :param dict request: the entity field creation request
        :param db._database.Database db: physical DB connection

        :returns: true to validate creation
        :rtype: bool
        """
        REL_TABLE = "_ss_{table}_{ftable}"
        # Create source field record
        SF_PARAMS = {
            "constraints":{
            }
        }
        table_sc = self.STELLAR[request["schema"]]["entities"][request["entity"]]
        for ftype in request["data"]["options"]:
            ftable = self.STELLAR[request["schema"]]["entities"][ftype]["code"]
            # Create relation table
            tab = REL_TABLE.format(table=table_sc["code"], ftable=ftable)
            REL_TABLE_COMMAND = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {relation} (
                    {table_col} TEXT NOT NULL,
                    {fk_table} INT NOT NULL REFERENCES {table} (uid) ON DELETE CASCADE,
                    uid INT GENERATED ALWAYS AS IDENTITY,
                    {fk_ftable} INT NOT NULL REFERENCES {ftable} (uid) ON DELETE CASCADE,
                    {ftable_col} TEXT NOT NULL
                );
            """).format(
                relation=sql.Identifier(tab),
                table_col=sql.Identifier(table_sc["code"]+"_col"),
                fk_table=sql.Identifier("fk_"+table_sc["code"]),
                table=sql.Identifier(table_sc["code"]),
                fk_ftable=sql.Identifier("fk_"+ftable),
                ftable=sql.Identifier(ftable),
                ftable_col=sql.Identifier(ftable+"_col")
            )
            db._run_command(REL_TABLE_COMMAND, return_style=None)

            # Create foreign field record
            FF_PARAMS = {
                "constraints":{
                    request['entity']:{
                        "relation": tab,
                        "table": table_sc["code"],
                        "col": request["data"]["code"]
                    }
                }
            }
            FF_CODE = f"{table_sc['code']}"
            # Since we're generating a field, make sure the code isn't already reserved
            if FF_CODE in self.STELLAR[request["schema"]]["entities"][ftype]["fields"]:
                FF_CODE+="_1"
                i = 2
                while FF_CODE in self.STELLAR[request["schema"]]["entities"][ftype]["fields"]:
                    FF_CODE = FF_CODE[:-1] + str(i)
                    i+=1
            FF_NAME = f"{table_sc['multiname']} <-> {self.STELLAR[request['schema']]['entities'][ftype]['multiname']}"
            FF_COMMAND = sql.SQL("""
                INSERT INTO fields (code, name, field_type, indexed, params) VALUES ((%s), (%s), 'MULTIENTITY', false, (%s)) RETURNING uid
            """)
            f_id = self._run_command(FF_COMMAND, (FF_CODE, FF_NAME, json.dumps(FF_PARAMS)))[0]
            self.__create_stellar_field_relation(f_id, self.STELLAR[request['schema']]['entities'][ftype]["id"])

            # Prep constraint type for source field record
            SF_PARAMS["constraints"][ftype] = {
                "relation": tab,
                "table": ftable,
                "col": FF_CODE
            }

        SF_COMMAND = sql.SQL("""
            INSERT INTO fields (code, name, field_type, indexed, params) VALUES ((%s), (%s), (%s), false, (%s)) RETURNING uid
        """)
        f_id = self._run_command(SF_COMMAND, (request["data"]["code"], request["data"]["name"], request["data"]["type"], json.dumps(SF_PARAMS)))[0]
        self.__create_stellar_field_relation(f_id, table_sc["id"])

        # Comet needs to be at schema level to capture reverse fields
        self.shoot_for_the_stars(level="schema", schema=request["schema"])
        return True


    def _field_update_list(self, request, _):
        """
        Update a list field. Effectively update the possible options for a list field.
        This involves:
            - Update the field record params: Stellar
        
        We assume it's easier for any front-end to just send an updated complete list,
        so we replace the constraints entirely.

        :param dict request: the field creation request
        :param db._database.Database _: the physical DB connection, unused but standardized

        :returns: true to validate creation
        :rtype: bool
        """
        field_sc = self.STELLAR[request["schema"]]["entities"][request["entity"]]["fields"][request["data"]["code"]]
        LF_PARAMS = field_sc["params"]
        LF_PARAMS["constraints"] = request["data"]["options"]
        COMMAND = sql.SQL("""
            UPDATE fields
            SET params = {params}
            WHERE uid = {uid}
        """).format(
            params=json.dumps(LF_PARAMS),
            uid=field_sc["id"]
        )
        self._run_command(COMMAND, return_style=None)


    def _field_update_entity(self, request, db):
        """
        """
        raise NotImplementedError


    def _field_delete_simple(self, request, db):
        """
        Delete a record. It's presumed that archival management is done elsewhere.
        Deleting a simple field involves:
            - Drop the column from the table: DB
            - Drop the field record: Stellar

        :param dict request: field deletion request
        :param db._database.Database db: physical DB connection

        :returns: true to validate deletion
        :rtype: bool
        """
        # Delete physical column
        db.delete_field(self.STELLAR[request["schema"]]["entities"][request["entity"]]["code"], request["data"]["code"])

        DEL_FIELD_COMMAND = sql.SQL("""
            DELETE FROM fields
            WHERE uid = {uid}
        """).format(
            uid=sql.Literal(self.STELLAR[request["schema"]]["entities"][request["entity"]]["fields"][request["data"]["code"]]["id"])
        )
        self._run_command(DEL_FIELD_COMMAND, return_style=None)
        return True


    def _field_delete_entity(self, request, _):
        """
        Delete an entity field. It's presumed that archival management is done elsewhere.
        Deleting an entity field involves:
            - Deleting the source field record: Stellar
            - IF target field record(s) only refer to the source field
                - Delete target field record(s)
            - ELSE
                - Remove source field from target field parameters
            - Shoot for the stars

        :param dict request: entity deletion request
        :param db._database.Database _: physical DB connection, unused but standardized

        :returns: true to validate deletion
        :rtype: bool
        """
        stellar_field = self.STELLAR[request["schema"]]["entities"][request["entity"]]["fields"][request["data"]["code"]]
        # Delete source field record
        SF_DEL_COMMAND = sql.SQL("""
            DELETE FROM fields
            WHERE uid = {uid}
        """).format(
            uid=stellar_field["id"]
        )
        self._run_command(SF_DEL_COMMAND, return_style=None)
        # Relation removal managed by FK cascade

        # Check each target field
        for ftype, target in stellar_field["params"]["constraints"].items():
            stellar_target = self.STELLAR[request["schema"]]["entities"][ftype]["fields"][target["col"]]
            if len(stellar_target["params"]["constraints"]) == 1:
                # Sole target
                FF_DEL_COMMAND = sql.SQL("""
                    DELETE FROM fields
                    WHERE uid = {uid}
                """).format(
                    uid=stellar_target["id"]
                )
                self._run_command(FF_DEL_COMMAND, return_style=None)
                # Relation removal managed by FK cascade
            else:
                # Target has other dependencies
                stellar_target["params"]["constraints"].pop(request["entity"])
                # for lftype, lftarget in stellar_target["params"]["constraints"].items():
                #     if lftarget["col"] == request["data"]["code"]:
                #         stellar_target["params"]["constraints"]["type"].pop(i)
                FF_UPDATE_COMMAND = sql.SQL("""
                    UPDATE fields
                    SET params = {params}
                    WHERE uid = {uid}
                """).format(
                    params=json.dumps(stellar_target["params"]),
                    uid=stellar_target["id"]
                )
                self._run_command(FF_UPDATE_COMMAND, return_style=None)

        # Comet needs to be at schema level to catch reverse field updates.
        # Two comets are sent due to current design. TODO
        self.shoot_for_the_stars(level="schema", schema=request["schema"])
        return True


# CALCULATED -> maybe
# CURRENCY -> maybe
# DATETIME -> maybe
# DURATION -> maybe
# FILE/LINK -> maybe
# PERCENTAGE -> maybe
    def __create_stellar_field_relation(self, f_id, e_id):
        """
        Simple function to generate a stellar field relation, since it's done all over.
        Refactor TODO
        """
        # Create Stellar relation
        R_COMMAND = sql.SQL("""
            INSERT INTO _ss_fields_entities (fields_col, fk_fields, fk_entities, entities_col) VALUES ('entity', (%s), (%s), 'fields');
        """)
        self._run_command(R_COMMAND, (f_id, e_id), return_style=None)
        return True
