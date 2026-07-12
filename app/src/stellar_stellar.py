import orjson
import asyncio
from pathlib import Path
from threading import Thread

import redis.asyncio as redis
from psycopg import sql
from psycopg.types.json import Jsonb

from lib.ragesync import execute_immediately

from db import PSQL
from config import CONFIG
from src.structures.returnfields import ReturnField, ReturnFieldSet
from src.structures.structure_structure import STELLARWrapper, Schema, Entity, Field, StellarUserCache, StellarUser


class StellarStellar():
    """
    *kira kira*
    """
    def __init__(self):
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
                "PASSWORD": self._field_create_simple,
                "MEDIA": self._field_create_simple,
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
                "TEXT": self._field_update_simple,
                "PASSWORD": self._field_update_simple,
                "MEDIA": self._field_update_simple,
                "INT": self._field_update_simple,
                "FLOAT": self._field_update_simple,
                "DATE": self._field_update_simple,
                "JSON": self._field_update_simple,
                "BOOL": self._field_update_simple,
                "LIST": self._field_update_list,
                "ENTITY": self._field_update_entity,
                "MULTIENTITY": self._field_update_entity
            },
            "delete": {
                "TEXT": self._field_delete_simple,
                "PASSWORD": self._field_delete_simple,
                "MEDIA": self._field_delete_media,
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

        self.database = PSQL({
            "DB_NAME": CONFIG.RAILGUN_DB_NAME,
            "DB_USER": CONFIG.RAILGUN_DB_USER,
            "DB_HOST": CONFIG.RAILGUN_DB_HOST,
            "DB_PORT": CONFIG.RAILGUN_DB_PORT
        })

        # *kira kira*
        self.STELLAR = execute_immediately(self.stellar_stellar())
        self.USER_CACHE = execute_immediately(self.stellar_users())

        self.comet = redis.Redis(
            host=CONFIG.COMET_HOST,
            port=CONFIG.COMET_PORT,
            db=CONFIG.COMET_DB
        )
        self.reach_for_the_stars()


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
        def listener_launcher():
            """
            We want to run async Redis, but creating and running the coroutine in different threads
            (main and _listener_thread) feels somewhat dangerous. As a result, we wrap the async wrapper
            in to do both in the dedicated thread.
            The layout of this is horrible though, and should probably be refactored in some way.
            """
            async def listener_handler():
                """
                Actual listen loop to let block within a thread.
                Performance has not been tested. May need to be async-ed a la AIGIS.
                TODO performance tests
                """
                async with self.comet.pubsub(ignore_subscribe_messages=True) as pubsub:
                    await pubsub.subscribe(CONFIG.COMET_NAME)
                    async for message in pubsub.listen():
                        if message["type"] == "message":
                            request = orjson.loads(message["data"].decode("utf-8"))
                            # Only listen for messages from the stars that aren't our own.
                            # Assume we update Stellar when we shoot for the stars for this
                            # instance in the first place.
                            if request["comet_id"] != CONFIG.COMET_ID:
                                print("A comet streaks across the sky!")  # TODO log
                                # BUG wouldn't this being async mean the possibility of multiple
                                # (potentially conflicting) comets being treated concurrently?
                                # This might need to be wrapped in `execute_immediately`.
                                await self.stellar_update(request)

            asyncio.run(listener_handler())

        self._listener_thread = Thread(target=listener_launcher, daemon=True)
        self._listener_thread.start()


    async def shoot_for_the_stars(self, comet):
        """
        Send out notification to all Railgun apps that the schema has changed.
        This is a fire-and-forget operation.

        :param Comet comet: configured Comet object
        """
        print("Shooting for the stars...")  # TODO log
        # Shoot for the stars
        await self.comet.publish(CONFIG.COMET_NAME, orjson.dumps(comet))
        # Update ourself immediately though, before we even actually return to RG.
        # This technically makes stellar calls slower to execute, but ensures that
        # further calls to this instance that immediately follow this one will have
        # the correct schema available.
        # In production, this logic would require a session_uuid manager/HAProxy to
        # ensure subsequent requests are triaged to the same endpoint.
        await self.stellar_update(comet)


    async def fetch_schemas(self):
        """
        Direct SQL select on the schemas table. Used to help populate STELLAR, but should not be
        called directly otherwise.

        :returns: registered schemas
        :rtype: list
        """
        COMMAND = """SELECT code, uid, name, host, db_type, _ss_archived FROM schemas;"""
        return await self.database._run_command(COMMAND, return_style="multi")


    async def fetch_entities(self, schema):
        """
        Direct SQL select on the entities table. Used to help populate STELLAR, but should not be
        called directly otherwise.

        :param int schema: ID of the schema for which the entities should be fetched.

        :returns: registered entities
        :rtype: list
        """
        COMMAND = sql.SQL("""
            SELECT entities.code, entities.soloname, entities.multiname, entities.display_name_col, entities.uid, entities._ss_archived, COALESCE(_ss_permission_rules_entities.permission_rules__ss_permission_rules_entities::jsonb, _ss_permission_rules_entities.permission_rules__ss_permission_rules_entities::jsonb) AS permission_rules
            FROM entities
            INNER JOIN _ss_entities_schemas ON _ss_entities_schemas.fk_entities = entities.uid
            INNER JOIN schemas ON schemas.uid = _ss_entities_schemas.fk_schemas
            LEFT JOIN (
                SELECT _ss_permission_rules_entities.fk_entities, json_agg(json_build_object('uid', permission_rules.uid, 'name', permission_rules.name, 'filter', permission_rules.filters)) AS permission_rules__ss_permission_rules_entities
                FROM _ss_permission_rules_entities
                LEFT JOIN permission_rules ON _ss_permission_rules_entities.fk_permission_rules = permission_rules.uid
                GROUP BY _ss_permission_rules_entities.fk_entities
            ) _ss_permission_rules_entities ON _ss_permission_rules_entities.fk_entities = entities.uid
            WHERE schemas.uid = (%s)"""
        )
        return await self.database._run_command(COMMAND, (schema,), return_style="multi")


    async def fetch_fields(self, entity):
        """
        Direct SQL select on the fields table. Used to help populate STELLAR, but should not be
        called directly otherwise.

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
        return await self.database._run_command(COMMAND, (entity,), return_style="multi")


    async def fetch_users(self):
        """
        """
        COMMAND = sql.SQL("""
            SELECT users.uid, users.login, users.username, users.password, users.invalid_before, COALESCE(_ss_users_permission_rules.permission_rules__ss_users_permission_rules::jsonb, _ss_users_permission_rules.permission_rules__ss_users_permission_rules::jsonb) AS permission_rules
            FROM users
            LEFT JOIN (
                SELECT _ss_users_permission_rules.fk_users, json_agg(json_build_object('type', 'Permission Rule', 'uid', permission_rules.uid, 'name', permission_rules.name)) AS permission_rules__ss_users_permission_rules
                FROM _ss_users_permission_rules
                LEFT JOIN permission_rules ON _ss_users_permission_rules.fk_permission_rules = permission_rules.uid
                GROUP BY _ss_users_permission_rules.fk_users
            ) _ss_users_permission_rules ON _ss_users_permission_rules.fk_users = users.uid
            WHERE users._ss_archived = false"""
        )
        return await self.database._run_command(COMMAND, return_style="multi")


    async def fetch_user(self, login):
        """
        """
        COMMAND = sql.SQL("""
            SELECT users.uid, users.login, users.username, users.password, users.invalid_before, COALESCE(_ss_users_permission_rules.permission_rules__ss_users_permission_rules::jsonb, _ss_users_permission_rules.permission_rules__ss_users_permission_rules::jsonb) AS permission_rules
            FROM users
            LEFT JOIN (
                SELECT _ss_users_permission_rules.fk_users, json_agg(json_build_object('type', 'Permission Rule', 'uid', permission_rules.uid, 'name', permission_rules.name)) AS permission_rules__ss_users_permission_rules
                FROM _ss_users_permission_rules
                LEFT JOIN permission_rules ON _ss_users_permission_rules.fk_permission_rules = permission_rules.uid
                GROUP BY _ss_users_permission_rules.fk_users
            ) _ss_users_permission_rules ON _ss_users_permission_rules.fk_users = users.uid
            WHERE users._ss_archived = false AND users.login = (%s)"""
        )
        return await self.database._run_command(COMMAND, (login,), return_style="solo")


    #####################################
    ########  Stellar  Stellar  #########
    #####################################
    async def stellar_stellar(self):
        """
        *kira kira*
        Fetch all schemas and populate STELLAR.
        Subsequent update requests should mostly be done by schema or table.
        This does not update the STELLAR property. Assignation must be done by caller.

        :returns: STELLAR STELLAR
        :rtype: STELLAR
        """
        STELLAR = STELLARWrapper()
        schemas = await self.fetch_schemas()  # BUG should only pull "public-facing" schemas
        for schema in schemas:
            STELLAR[schema["code"]] = Schema(
                code=schema["code"],
                id=schema["uid"],
                name=schema["name"],
                host=schema["host"],
                db_type=schema["db_type"],
                archived=schema["_ss_archived"]
            )
            await self.stellar_schema(STELLAR[schema["code"]])
        return STELLAR


    async def stellar_schema(self, schema):
        """
        *kira kira*
        Fetch the STELLAR of a specific schema.
        This *does* update the STELLAR property directly.

        :param Schema schema: Schema object of the schema for which to fetch the entities
        """
        schema_entities = await self.fetch_entities(schema.id)
        new_entity_data = {}
        for entity in schema_entities:
            # Register this entity
            new_entity_data[entity["soloname"]] = Entity(
                schema=schema,
                code=entity["code"],
                soloname=entity["soloname"],
                multiname=entity["multiname"],
                display_name_col=entity["display_name_col"],
                id=entity["uid"],
                archived=entity["_ss_archived"],
                permissionRules=entity["permission_rules"] or []
            )
            # Then register this entity's fields
            await self.stellar_entity(new_entity_data[entity["soloname"]])
        # Once we have all the necessary data, swap out any live STELLAR data for the new stuff.
        # This must be done at the end to ensure an asynchoneous call doesn't come in mid-update
        # and get a half-baked schema.
        schema.finalize_entity_data(new_entity_data)


    async def stellar_entity(self, entity, reset_return_fields=False):
        """
        *kira kira*
        Fetch the STELLAR of a specific entity.
        This *does* update the provided STELLAR entity property directly.

        Updating the ReturnFields is tricky due to sync issues.
        If this is being called during a schema load, the schema load process will take care of updating the ReturnFields
        If this is being called during an stellar update on the entity, the entity itself still needs a kick to repopulate it's ReturnFields.

        :param Entity entity: Entity object of the entity for which to fetch the fields
        :param bool reset_return_fields: whether to attempt to set the return fields of the entities fields immediately
        """
        entity_fields = await self.fetch_fields(entity.id)
        new_field_data = {}
        for field in entity_fields:
            # Register this field
            new_field_data[field["code"]] = Field(
                entity=entity,
                code=field["code"],
                name=field["name"],
                type=field["field_type"],
                uid=field["uid"],
                index=field["indexed"],
                params=field["params"],
                archived=field["_ss_archived"]
            )
        # Once we have all the necessary data, swap out any live STELLAR data for the new stuff.
        # This must be done at the end to ensure an asynchoneous call doesn't come in mid-update
        # and get a half-baked schema. Additionally, we sometimes need to update the return fields
        # of the entity. If we know that the schema is defined already but assume a change to the
        # entity layer, we won't call stellar_schema, so we need to re-parse this entity's return
        # fields.
        if reset_return_fields:
            entity.finalize_field_data(new_field_data)
        else:
            entity.fields = new_field_data


    async def stellar_user(self, user):
        """
        Update USER_CACHE with valid usercache data for a given user.
        This updates the USER_CACHE property directly.
        If no user is found, the USER_CACHE will pop the login to make sure it is gone.

        :param int uid: uid of the user to update

        :returns: user dict if such a user exists, else None
        :rtype: dict|None
        """
        self.USER_CACHE[user["login"]] = StellarUser(
            user["username"],
            user["login"],
            user["permission_rules"],
            user["invalid_before"]
        )
        # TODO shoot for the stars


    async def stellar_users(self):
        """
        Constructs a usable USER_CACHE from scratch with all available users.
        This should ostensibly only be run at startup, with users being updated
        individually via comets later on (same as schema).
        This does not update the USER_CACHE property. Assignation must be done by caller.

        :returns: STELLAR USER_CACHE config
        :rtype: StellarUserCache
        """
        users = await self.fetch_users()
        USER_CACHE = StellarUserCache()
        for user in users:
            USER_CACHE[user["login"]] = StellarUser(
            user["username"],
            user["login"],
            user["permission_rules"],
            user["invalid_before"]
        )
        return USER_CACHE


    async def stellar_update(self, request):
        """
        Update STELLAR based on received request.
        See StellarStellar.reach_for_the_stars for request format documentation.

        :param dict request: STELLAR update request.
        """
        if "entity" in request:
            # We assume an entity will always be provided in tandem with a schema
            await self.stellar_entity(
                self.STELLAR[request["schema"]].entities[request["entity"]],
                reset_return_fields=True
            )
        elif "schema" in request:
            # Assume that we will not be provided an entity if we only need to update the schema
            await self.stellar_schema(
                self.STELLAR[request["schema"]]
            )
        else:
            self.STELLAR = await self.stellar_stellar()


    async def create_field(self, request, db, stellardb):
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
        if request["data"]["code"] == "type":
            raise NotImplementedError  # TODO better error messaging

        # Offload creation
        # Creation subfunction must be async
        comet = await self._field_factory["create"][request["data"]["type"]](request, db, stellardb)

        # Send the default comet if a specific one is not set
        return comet or Comet(schema=request["schema"], entity=request["entity"])


    async def update_field(self, request, db, stellardb):
        """
        Update a field's parameters. The possibilities vary based on field type, so offloaded
        to a factory function.
        Most types actually don't have update options.
        """
        field_sc = self.STELLAR[request["schema"]].entities[request["entity"]].fields[request["data"]["code"]]
        if field_sc.type not in self._field_factory["update"]:
            raise NotImplementedError

        # Offload update
        # Update subfunction must be async
        comet = await self._field_factory["update"][field_sc.type](request, db, stellardb)

        # Stellar Stellar
        return comet or Comet(schema=request["schema"], entity=request["entity"])


    async def delete_field(self, request, db, stellardb):
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
        stellar_field = self.STELLAR[request["schema"]].entities[request["entity"]].fields[request["data"]["code"]]
        if not stellar_field.archived:
            # It hasn't been "hidden" yet. Hide it first.
            HIDE_OP = {
                "table": "fields",
                "entity": "Field",
                "entity_id": stellar_field.id,
                "data": {
                    "_ss_archived": True
                }
            }
            await stellardb.update(HIDE_OP)
        else:
            # This is the real deletion. Boom goes the dynamite
            if stellar_field.type not in self._field_factory["delete"]:
                raise NotImplementedError
            # Offload deletion
            # Delete subfunction must be async
            await self._field_factory["delete"][stellar_field.type](request, db, stellardb)

        # Stellar Stellar
        return Comet(schema=request["schema"], entity=request["entity"])


    async def create_entity(self, request, db, stellardb):
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
            assert data["code"] not in self.STELLAR[request["schema"]].entities
        except AssertionError:
            return "Table with the name {name} already exists".format(name=data["code"])
        # Real table creation
        await db.create_table(data["code"])

        # Stellar table creation
        ENT_OP = {
            "table": "entities",
            "entity": "Entity",
            "data": {
                "code": data["code"],  # A bit lossy to rebuild data, but prevents weird values from being passed
                "multiname": data["multiname"],
                "soloname": data["soloname"],
                "display_name_col": "code"
            }
        }
        ent_id = (await stellardb.create(ENT_OP))["uid"]

        # Stellar table Relation creation
        REL_OP = {
            "table": "_ss_entities_schemas",
            "entity": "_SS_CONNECTION",  # unused in practice
            "data": {
                "entities_col": "schema",
                "fk_entities": ent_id,
                "fk_schemas": self.STELLAR[request["schema"]].id,
                "schemas_col": "entities"
            }
        }
        await stellardb.create(REL_OP)

        # Stellar Field and field Relation Creation
        FIELD_OPS = [
            {
                "table": "fields",
                "entity": "Field",
                "data": {
                    "code": "uid",
                    "name": "ID",
                    "field_type": "INT",
                    "indexed": True,
                    "params": "{}"
                }
            },
            {
                "table": "fields",
                "entity": "Field",
                "data": {
                    "code": "code",
                    "name": "Display Name",
                    "field_type": "TEXT",
                    "indexed": False,
                    "params": "{}"
                }
            }
        ]
        FREL_OP_TEMPLATE = {
            "table": "_ss_fields_entities",
            "entity": "_SS_CONNECTION",  # unused in practice
            "data": {
                "fields_col": "entity",
                "fk_fields": None,  # POPULATED DURING ITERATION
                "fk_entities": ent_id,
                "entities_col": "fields"
            }
        }
        for op in FIELD_OPS:
            field_id = (await stellardb.create(op))["uid"]
            FREL_OP_TEMPLATE["data"]["fk_fields"] = field_id
            await stellardb.create(FREL_OP_TEMPLATE)

        return Comet(schema=request["schema"])


    async def update_entity(self, request, db, stellardb):
        """
        There isn't anything we really want to expose to the user in regards to actual
        ALTER TABLE commands. Column creation is handled separately, and things like
        soloname/multiname shouldn't be done through Stellar.
        """
        raise NotImplementedError


    async def delete_entity(self, request, db, stellardb):
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
            - Drop all MULTI/ENTITY fields of this entity (otherwise foreign table field params break)
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
        prearchived = self.STELLAR[request["schema"]].entities[request["data"]["type"]].archived

        if not prearchived:
            # It hasn't been "hidden" yet. Hide it first.
            HIDE_OP = {
                "table": "entities",
                "entity": "Entity",
                "entity_id": self.STELLAR[request["schema"]].entities[request["data"]["type"]].id,
                "data": {
                    "_ss_archived": True
                }
            }
            await stellardb.update(HIDE_OP)
        else:
            # This is a real delete. Boom goes the dynamite.

            # Remove any multi/entity fields
            for field in self.STELLAR[request["schema"]].entities[request["data"]["type"]].fields.values():
                if field.type in ["ENTITY", "MULTIENTITY"]:
                    # HACK to fully delete the field in one go. EXTREMELY BAD
                    self.STELLAR[request["schema"]].entities[request["data"]["type"]].fields[field.code].archived = True

                    # Call the internal function directly to avoid needlessly shooting for the stars.
                    # TODO, no longer necessary
                    await self._field_delete_entity({
                        "part": "field",
                        "request_type": "delete",
                        "schema": request["schema"],
                        "entity": request["data"]["type"],
                        "data": {
                            "code": field.code
                        }
                    }, db)

            # Drop entity table
            await db.delete_table(self.STELLAR[request["schema"]].entities[request["data"]["type"]].code)

            # Drop entity record
            DEL_ENT_OP = {
                "table": "entities",
                "entity": "Entity",
                "entity_id": self.STELLAR[request["schema"]].entities[request["data"]["type"]].id
            }
            await stellardb.delete(DEL_ENT_OP)

            # Drop field records
            DEL_FIELD_OP_TEMPLATE = {
                "table": "fields",
                "entity": "Field",
                "entity_id": None  # POPULATED DURING ITERATION
            }
            for field in self.STELLAR[request["schema"]].entities[request["data"]["type"]].fields.values():
                DEL_FIELD_OP_TEMPLATE["entity_id"] = field.id
                await stellardb.delete(DEL_FIELD_OP_TEMPLATE)

        return Comet(schema=request["schema"])


    async def update_schema(self, request, db, stellardb):
        raise NotImplementedError


    async def release_schema(self, request, db, stellardb):
        raise NotImplementedError


    #####################################
    #########   FIELD  TYPES   ##########
    #####################################
    async def _field_create_simple(self, request, db, stellardb, nullable=True, default=None):
        """
        Create a "simple" field.
        Many simple types have nothing special to do beyond:
            - Create physical DB field: DB
            - Create field record and relation: Stellar
        The field creation factory sends those simple field types here.

        :param dict request: the field creation request
        :param db._database.Database db: the physical DB connection

        :raises: NotImplementedError gets bubbled up from the DB connection if the type isn't valid
        """
        # Create physical DB field
        # This will raise NotImplementedError if the type isn't valid for this DB connector.
        await db.create_field(self.STELLAR[request["schema"]].entities[request["entity"]].code, request["data"]["code"], request["data"]["type"], nullable=nullable, default=default)

        # Allow storing of generic params for normal fields. Can be used by frontends.
        F_PARAMS = request["data"].get("options", {})
        # Create Stellar record
        F_OP = {
            "table": "fields",
            "entity": "Field",
            "data": {
                "code": request["data"]["code"],
                "name": request["data"]["name"],
                "field_type": request["data"]["type"],
                "indexed": False,
                "params": Jsonb(F_PARAMS),
            }
        }
        f_id = (await stellardb.create(F_OP))["uid"]

        # Create Stellar relation
        await _create_stellar_field_relation(f_id, self.STELLAR[request["schema"]].entities[request["entity"]].id, stellardb)


    async def _field_create_bool(self, request, db, stellardb):
        """
        SQL booleans are tertiary... Make sure to set them as not nullable with false (unchecked) default.
        The rest is the same though.
        This could be configured as a lambda in the factory, but felt cleaner to make it explicit here.

        :param dict request: the field creation request
        :param db._database.Database db: the physical DB connection

        :raises: NotImplementedError gets bubbled up from the DB connection if the type isn't valid
        """
        await self._field_create_simple(request, db, stellardb, nullable=False, default="false")


    async def _field_create_list(self, request, db, stellardb):
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
        await db.create_field(self.STELLAR[request["schema"]].entities[request["entity"]].code, request["data"]["code"], "TEXT")

        # Create Stellar record
        LF_OP = {
            "table": "fields",
            "entity": "Field",
            "data": {
                "code": request["data"]["code"],
                "name": request["data"]["name"],
                "field_type": "LIST",
                "indexed": False,
                "params": Jsonb({"constraints": request["data"]["options"]})
            }
        }
        f_id = (await stellardb.create(LF_OP))["uid"]

        # Create Stellar relation
        await _create_stellar_field_relation(f_id, self.STELLAR[request["schema"]].entities[request["entity"]].id, stellardb)


    async def _field_create_entity(self, request, db, stellardb):
        """
        Creating an entity link is the most convoluted process of all. Thanks SQL.
        It generally involves the following steps:
            - Create a relation table for each target entity in the physical DB: DB
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
        table_sc = self.STELLAR[request["schema"]].entities[request["entity"]]
        for ftype in request["data"]["options"]:
            ftable = self.STELLAR[request["schema"]].entities[ftype].code
            # Create relation table
            tab = REL_TABLE.format(table=table_sc.code, ftable=ftable)
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
                table_col=sql.Identifier(table_sc.code+"_col"),
                fk_table=sql.Identifier("fk_"+table_sc.code),
                table=sql.Identifier(table_sc.code),
                fk_ftable=sql.Identifier("fk_"+ftable),
                ftable=sql.Identifier(ftable),
                ftable_col=sql.Identifier(ftable+"_col")
            )
            await db.execute(REL_TABLE_COMMAND)  # Reminder, this is a non-standard table

            # Create foreign field record
            FF_PARAMS = {
                "constraints":{
                    request['entity']:{
                        "relation": tab,
                        "table": table_sc.code,
                        "col": request["data"]["code"]
                    }
                }
            }
            FF_CODE = f"{table_sc.code}"
            # Since we're generating a field, make sure the code isn't already reserved
            if FF_CODE in self.STELLAR[request["schema"]].entities[ftype].fields:
                FF_CODE+="_1"
                i = 2
                while FF_CODE in self.STELLAR[request["schema"]].entities[ftype].fields:
                    FF_CODE = FF_CODE[:-1] + str(i)
                    i+=1
            FF_NAME = f"{table_sc.multiname} <-> {self.STELLAR[request['schema']].entities[ftype].multiname}"
            FF_OP = {
                "table": "fields",
                "entity": "Field",
                "data": {
                    "code": FF_CODE,
                    "name": FF_NAME,
                    "field_type": "MULTIENTITY",
                    "indexed": False,
                    "params": Jsonb(FF_PARAMS)
                }
            }
            f_id = (await stellardb.create(FF_OP))["uid"]
            await _create_stellar_field_relation(f_id, self.STELLAR[request['schema']].entities[ftype].id, stellardb)

            # Prep constraint type for source field record
            SF_PARAMS["constraints"][ftype] = {
                "relation": tab,
                "table": ftable,
                "col": FF_CODE
            }

        SF_OP = {
            "table": "fields",
            "entity": "Field",
            "data": {
                "code": request["data"]["code"],
                "name": request["data"]["name"],
                "field_type": request["data"]["type"],
                "indexed": False,
                "params": Jsonb(SF_PARAMS)
            }
        }
        f_id = (await stellardb.create(SF_OP))["uid"]
        await _create_stellar_field_relation(f_id, table_sc.id, stellardb)

        # Comet needs to be at schema level to capture reverse fields
        return Comet(schema=request["schema"])


    async def _field_update_simple(self, request, _, stellardb):
        """
        Generic function for updating field definitions.
        In reality, the only attribute shared by all fields that can be updated is the
        field's display name. This functionality is therefore mostly called by other, specialized
        update functions.
        Updating the field's display name involves:
            - Update the field record name: Stellar

        :param dict request: the field update request
        :param db._database.Database _: the physical DB connection, unused but standardized
        """
        field_sc = self.STELLAR[request["schema"]].entities[request["entity"]].fields[request["data"]["code"]]
        if not request["data"].get("name"):
            # No update requested (malformed?) or attempted to set name to a falsy value
            # TODO this should probably raise an HTTP exception
            return
        OP = {
            "table": "fields",
            "entity": "Field",
            "entity_id": field_sc.id,
            "data": {
                "name": request["data"]["name"]
            }
        }
        await stellardb.update(OP)


    async def _field_update_list(self, request, _, stellardb):
        """
        Update a list field. Effectively update the possible options for a list field.
        This involves:
            - Update the field record params: Stellar
        
        IMPORTANT: We assume it's easier for any front-end to just send an updated complete list,
        so we replace the constraints entirely.

        :param dict request: the field creation request
        :param db._database.Database _: the physical DB connection, unused but standardized
        """
        field_sc = self.STELLAR[request["schema"]].entities[request["entity"]].fields[request["data"]["code"]]
        data = {}
        if "options" in request["data"]:  # Allow falsy values since an empty list is valid
            # *dabs in Japanese* kind of line
            # prepare of copy of the existing params, with the constraints overwritten by the new ones
            # if a falsy value is provided, use an empty list to improve compatibility on future deserialization
            data["params"] = Jsonb(field_sc.params.copy() | {"constraints": (request["data"]["options"] or []) })
        if request["data"].get("name"):  # You probably shouldn't try to set display name to None/empty
            data["name"] = request["data"]["name"]

        if not data:
            # we don't actually have anything to update...
            return

        OP = {
            "table": "fields",
            "entity": "Field",
            "entity_id": field_sc.id,
            "data": data
        }
        await stellardb.update(OP)


    async def _field_update_entity(self, request, db, stellardb):
        """
        Updating an entity link is a convoluted process. Thanks SQL.
        It generally involves the following steps:
            - Create a relation table for each new target entity in the physical DB: DB
            - Update the field record for the source entity: Stellar
            - Create a field record for the target entity: Stellar
            - Shoot for the stars
        Entity field updates require the additional "data/options" key in the request,
        representing a list of the foreign tables (entity types) that the entity field
        should link to:
        {
            "part": "field",
            "request_type": "update",
            "schema": <schema_code>,
            "entity": <entity_code>,
            "data": {
                "code": <field_code>,
                "name": <field_name>, #OPTIONAL (TODO)
                "options": [<ftables>], #OPTIONAL

            }
        }
        BUG
        While we assume it to be easier for any frontend to send a full list of entities the field should allow linking to,
        updating a multi-entity field to remove a link is currently not supported.
        Of the list sent, anything that doesn't already exist will be created. Everything else is ignored. Removing a link is currently not possible.

        :param dict request: the entity field creation request
        :param db._database.Database db: physical DB connection
        """
        REL_TABLE = "_ss_{table}_{ftable}"


        table_sc = self.STELLAR[request["schema"]].entities[request["entity"]]
        field_sc = table_sc.fields[request["data"]["code"]]
        # Prepare source field record
        SF_UPDATES = field_sc.params

        for ftype in request["data"]["options"]:
            if ftype in SF_UPDATES["constraints"]:
                # Field link already defined, skip it.
                continue
            ftable = self.STELLAR[request["schema"]].entities[ftype].code
            # Create relation table
            tab = REL_TABLE.format(table=table_sc.code, ftable=ftable)
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
                table_col=sql.Identifier(table_sc.code+"_col"),
                fk_table=sql.Identifier("fk_"+table_sc.code),
                table=sql.Identifier(table_sc.code),
                fk_ftable=sql.Identifier("fk_"+ftable),
                ftable=sql.Identifier(ftable),
                ftable_col=sql.Identifier(ftable+"_col")
            )
            await db.execute(REL_TABLE_COMMAND)

            # Create foreign field record
            FF_PARAMS = {
                "constraints":{
                    request['entity']:{
                        "relation": tab,
                        "table": table_sc.code,
                        "col": request["data"]["code"]
                    }
                }
            }
            FF_CODE = f"{table_sc.code}"
            # Since we're generating a field, make sure the code isn't already reserved
            if FF_CODE in self.STELLAR[request["schema"]].entities[ftype].fields:
                FF_CODE+="_1"
                i = 2
                while FF_CODE in self.STELLAR[request["schema"]].entities[ftype].fields:
                    FF_CODE = FF_CODE[:-1] + str(i)
                    i+=1
            FF_NAME = f"{table_sc.multiname} <-> {self.STELLAR[request['schema']].entities[ftype].multiname}"
            FF_OP = {
                "table": "fields",
                "entity": "Field",
                "data": {
                    "code": FF_CODE,
                    "name": FF_NAME,
                    "field_type": "MULTIENTITY",
                    "indexed": False,
                    "params": Jsonb(FF_PARAMS)
                }
            }
            f_id = (await stellardb.create(FF_OP))["uid"]
            # FF_COMMAND = sql.SQL("""
            #     INSERT INTO fields (code, name, field_type, indexed, params) VALUES ((%s), (%s), 'MULTIENTITY', false, (%s)) RETURNING uid
            # """)
            # f_id = self.database._run_command(FF_COMMAND, (FF_CODE, FF_NAME, orjson.dumps(FF_PARAMS)))[0]["uid"]
            await _create_stellar_field_relation(f_id, self.STELLAR[request['schema']].entities[ftype].id, stellardb)

            # Prep constraint type for source field record
            SF_UPDATES["constraints"][ftype] = {
                "relation": tab,
                "table": ftable,
                "col": FF_CODE
            }

        # Update source field record
        SF_OP = {
            "table": "fields",
            "entity": "Field",
            "entity_id": field_sc.id,
            "data": {
                "params": Jsonb(SF_UPDATES)
            }
        }
        await stellardb.update(SF_OP)

        # SF_COMMAND = sql.SQL("""
        #     UPDATE fields
        #     SET params=(%s)
        #     WHERE uid=(%s)
        #     RETURNING uid
        # """)
        # f_id = self.database._run_command(SF_COMMAND, (orjson.dumps(SF_UPDATES),field_sc.id))[0]["uid"]

        # Comet needs to be at schema level to capture reverse fields
        # self.shoot_for_the_stars(level="schema", schema=request["schema"])
        return Comet(schema=request["schema"])



    async def _field_delete_simple(self, request, db, stellardb):
        """
        Delete a record. It's presumed that archival management is done elsewhere.
        Deleting a simple field involves:
            - Drop the column from the table: DB
            - Drop the field record: Stellar

        :param dict request: field deletion request
        :param db._database.Database db: physical DB connection
        """
        # Delete physical column
        await db.delete_field(self.STELLAR[request["schema"]].entities[request["entity"]].code, request["data"]["code"])

        DEL_FIELD_OP = {
            "table": "fields",
            "entity": "Field",
            "entity_id": self.STELLAR[request["schema"]].entities[request["entity"]].fields[request["data"]["code"]].id
        }
        await stellardb.delete(DEL_FIELD_OP)


    async def _field_delete_entity(self, request, _, stellardb):
        """
        Delete an entity field. It's presumed that archival management is done elsewhere.
        Deleting an entity field involves:
            - Deleting the source field record: Stellar
            - IF target field record(s) only refer to the source field
                - Delete target field record(s)
            - ELSE
                - Remove source field from target field parameters
            - Shoot for the stars

        BUG
        Doesn't this leave lingering connection tables/entries?

        :param dict request: entity deletion request
        :param db._database.Database _: physical DB connection, unused but standardized
        """
        stellar_field = self.STELLAR[request["schema"]].entities[request["entity"]].fields[request["data"]["code"]]
        # Delete source field record
        SF_DEL_OP = {
            "table": "fields",
            "entity": "Field",
            "entity_id": stellar_field.id
        }
        await stellardb.delete(SF_DEL_OP)
        # SF_DEL_COMMAND = sql.SQL("""
        #     DELETE FROM fields
        #     WHERE uid = {uid}
        # """).format(
        #     uid=stellar_field.id
        # )
        # self.database._run_command(SF_DEL_COMMAND, return_style=None)
        # Relation removal managed by FK cascade

        # Check each target field
        for ftype, target in stellar_field.params["constraints"].items():
            stellar_target = self.STELLAR[request["schema"]].entities[ftype].fields[target["col"]]
            if len(stellar_target.params["constraints"]) == 1:
                # Sole target
                FF_DEL_OP = {
                    "table": "fields",
                    "entity": "Field",
                    "entity_id": stellar_target.id
                }
                await stellardb.delete(FF_DEL_OP)
                # FF_DEL_COMMAND = sql.SQL("""
                #     DELETE FROM fields
                #     WHERE uid = {uid}
                # """).format(
                #     uid=stellar_target.id
                # )
                # self.database._run_command(FF_DEL_COMMAND, return_style=None)
                # Relation removal managed by FK cascade
            else:
                # Target has other dependencies
                stellar_target.params["constraints"].pop(request["entity"])
                FF_UPDATE_OP = {
                    "table": "fields",
                    "entity": "Field",
                    "entity_id": stellar_target.id,
                    "data": {
                        "params": Jsonb(stellar_target.params)
                    }
                }
                await stellardb.update(FF_UPDATE_OP)
                # FF_UPDATE_COMMAND = sql.SQL("""
                #     UPDATE fields
                #     SET params = {params}
                #     WHERE uid = {uid}
                # """).format(
                #     params=orjson.dumps(stellar_target.params),
                #     uid=stellar_target.id
                # )
                # self.database._run_command(FF_UPDATE_COMMAND, return_style=None)

        # Comet needs to be at schema level to catch reverse field updates.
        # self.shoot_for_the_stars(level="schema", schema=request["schema"])
        return Comet(schema=request["schema"])


    async def _field_delete_media(self, request, db, stellardb):
        """
        TODO validate
        Delete a media field. It's assumed archival is handled elswhere.
        Deleting a media field involves most notibly DELETING ALL THE MEDIA TOO. Thus:
            - Fetch all records with this field filled out
            - Delete the media found in those fields
            - Drop the field normally
        """
        entity_sc = self.STELLAR[request["schema"]].entities[request["entity"]]
        existings = []
        # Internal op, go wild with page size
        # BUG if the DB is actually insane, this will run out of RAM
        while len(existings) % 10000 == 0:
            existings.extend(await db.query(
                table=entity_sc.code,
                fields=ReturnFieldSet(
                    table=entity_sc.code,
                    name=None,
                    values=[ReturnField(entity_sc.code, request["data"]["code"])]
                ),
                filters={
                    "filter_operator": "AND",
                    "filters": [[request["data"]["code"], "is_not", None]]
                },
                pagination=10000
            ))
            if len(existings) == 0:
                # Maybe there is no media at all...
                break
        print(existings)  # TODO log
        # Delete all the existing files
        for existing in existings:
            p = Path(existing[request["data"]["code"]])
            p.unlink(missing_ok=True)
        # Then delete the field as a normal text field
        await self._field_delete_simple(request, db, stellardb)

# TODO
# CALCULATED -> maybe
# CURRENCY -> maybe
# DATETIME -> maybe
# DURATION -> maybe
# FILE/LINK -> maybe
# PERCENTAGE -> maybe
async def _create_stellar_field_relation(f_id, e_id, stellardb):
    """
    Simple function to generate a stellar field relation, since it's done all over.
    Refactor TODO
    """
    # Create Stellar relation
    R_OP = {
        "table": "_ss_fields_entities",
        "entity": "_SS_CONNECTION",  # unused in practice
        "data": {
            "fields_col": "entity",
            "fk_fields": f_id,
            "fk_entities": e_id,
            "entities_col": "fields"
        }
    }
    await stellardb.create(R_OP)


class Comet(dict):
    """
    Extremely simple dict expansion to define comet parameter requirements.
    This allows stellar to report comet information back up to railgun in a
    more readable fashion.
    """
    def __init__(self, schema=None, entity=None):
        super().__init__()
        self["comet_id"] = CONFIG.COMET_ID  # Always needed.
        if schema:
            self["schema"] = schema
            if entity:
                self["entity"] = entity
