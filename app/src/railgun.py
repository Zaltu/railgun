from pathlib import Path
from json import JSONDecodeError

# Needed for one op
import shutil

from fastapi import FastAPI, HTTPException
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware

from src.modules.railconfig import RailConfig
from src.stellar_stellar import StellarStellar
from db._database import CUDError
from config import CONFIG
from src.structures.returnfields import ReturnFieldSet, PresetReturnField, ReturnField, EntityReturnField, MultiEntityReturnField
from src.structures.internal_ops import InternalOperations


_DEFAULT_QUERY_FILTER = lambda request:{
    "filter_operator": "AND",
    "filters": [
        ["_ss_archived", "is", bool(request["read"].get("show_archived", False))],  # Some hubris to allow proper parsing of false values
    ]
}


ALLOWED_CORS_ORIGINS = [
        # Some defaults
        'http://localhost',
        'http://127.0.0.1',
]
ALLOWED_CORS_ORIGINS.extend(CONFIG.RG_URLS)


class Railgun(FastAPI):
    """
    Kaboom.
    """
    def __init__(self):
        super().__init__(default_response_class=ORJSONResponse)

        self.add_middleware(
            CORSMiddleware,
            allow_origins=ALLOWED_CORS_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"]
        )

        # Les' gooo
        self.data = RailConfig()
        # Les' gooooooooo
        self.STELLAR = StellarStellar()

        self.internal_operations_factory = InternalOperations(self)


    async def validate_request(self, request):
        """
        Basic malformed request validation.
        TODO
        """
        try:
            request = await request.json()
            assert "schema" in request and request["schema"] in self.data
        except JSONDecodeError:
            raise #"Bad request"
        except AssertionError:
            raise #"No data found"
        except:
            raise
        return request


    async def read(self, request, permissions):
        """
        Prepare for a read request
        Essentially just pick your DB.
        """
        _db_pool = self.data[request["schema"]]
        async with _db_pool.stage() as db:
            resp = await self._read(db, request, permissions)
        return resp

    async def _read(self, db, request, permissions):
        """
        Fetch data from a DB.
        Expected format:
        {
            "schema": Schema code,
            "entity": Entity code,
            "read": {
                "return_fields": [fields, to, return],
                "page": Page number,
                "pagination": Entries per page,
                "order": Field to sort by
                "filters": Filter set (see app docs TODO)
            }
        }
        Expecting set of permissions.
        HACK (tbf the entire permission setup currently is a hack), we assume that if no
        permission set is provided, use full permissions. This is bad (obviously).
        """
        # Helpers for syntax
        schema_sc = self.STELLAR.STELLAR[request["schema"]].entities
        table_sc = schema_sc[request["entity"]].fields

        # Preformatting default archived filter
        filters = _DEFAULT_QUERY_FILTER(request)
        # Fetch relevant permission filters
        # Permission ID 1 is admin (hard-coded, TODO?)
        if 1 not in permissions:
            permissionRules = schema_sc[request["entity"]].parse_permissions(permissions)
            print(permissionRules)
            if permissionRules:
                filters["filters"].extend(permissionRules)
            elif permissionRules == False:  # IMPORTANT if no permissions explicitely defined, RETURN NOTHING. zero-trust
                return []

        if request["read"].get("filters"):
            filters["filters"].append(request["read"]["filters"])

        # Setup default structures
        return_fields = ReturnFieldSet(schema_sc[request["entity"]].code, None, [])

        # Ensure return_fields exists
        requested_return_fields = request["read"].get("return_fields", [])
        if "uid" not in requested_return_fields:
            requested_return_fields.append("uid")
        if schema_sc[request["entity"]].display_name_col not in requested_return_fields:
            requested_return_fields.append(schema_sc[request["entity"]].display_name_col)

        # Always include base type
        return_fields.put(PresetReturnField("type", request["entity"]))

        for field in requested_return_fields:
            if "." in field:
                # Assume linked field, normal fields should not have special characters in their codes
                linked_field = field.split(".")
                # Ensure we have a properly formatted linked field request
                assert len(linked_field)%2==1

                # Parse linked fields recursively for arbitrary depth
                return_fields.put(
                    self._linked_return_field_builder(linked_field, 0, schema_sc, request["entity"])
                )
            else:
                # TODO review this
                #if table_sc[field]["type"] == "PASSWORD":
                    # Bit of custom logic to never return password fields
                #    preset_return_fields[field] = "********"
                #    continue
                return_fields.put(table_sc[field].return_field)

        resp = await db.query(
            table=schema_sc[request["entity"]].code,
            fields=return_fields,
            filters=filters,
            pagination=request["read"].get("pagination") or 25,
            page=request["read"].get("page") or 1,
            order=request["read"].get("order") or "uid"
        )
        if bool(request["read"].get("include_count", False)):
            query_total_count = await db.count(schema_sc[request["entity"]].code, filters)
            resp.append(query_total_count)
        return resp


    async def batch(self, request, permissions=None):
        """
        Create - Update - Delete

        In order to manage effective rollback of batch requests, a separate DB connection is spawned.
        This DB connection stages all requests provided by the caller, and commits them once they've
        all been DB-validated. Long live PSQL. TODO adapt doc to Railgun refactor.

        We return a list ordered to the same index as given, with the IDs of the C/U/D'd entities.

        Standard CUD operation request goes like this:
        {
            "schema": <schema>,
            "batch": [{
                "request_type": "create"|"update"|"delete",
                "entity": <entity>,
                > IF "update"|"delete"
                "entity_id": <id of entity to update>,
                "permanent": <delete completely or only archive>
                > IF "create"|"update"
                "data": {
                    <field>: <value>,
                    etc...
                }
            },etc...
            ]
        }
        While it's in fact quite inefficient, Railgun processes each request individually.
        This can in fact end up causing performance concerns in production depenting on how
        the software is used. TODO (batch also needs a refactor related to [table])

        Just don't use it the bad way 4head.

        :param dict request: CUD batch request
        :param set permissions: user permissions (TODO)

        :returns: list of entities effected by the operation
        :rtype: list[dict]
        """
        try:
            return_values = []
            assert "batch" in request
            _db_pool = self.data[request["schema"]]
            async with _db_pool.stage() as db:
                for op in request["batch"]:
                    # We'll need to pass the actual table to the DB regardless of the operation
                    op["table"] = self.STELLAR.STELLAR[request["schema"]].entities[op["entity"]].code
                    op["schema"] = request["schema"]
                    # Process updates
                    if op["request_type"] == "update":
                        return_values.append(await self._update(db, op))

                    # Process creates
                    elif op["request_type"] == "create":
                        return_values.append(await self._create(db, op))

                    # Process deletes
                    elif op["request_type"] == "delete":
                        return_values.append(await self._delete(db, op))

                    else:
                        raise CUDError("Unrecognized request type: %s" % op["request_type"])

        except (AssertionError, CUDError, KeyError) as cude:
            raise HTTPException(
                status_code=500,
                detail=str(type(cude)) + " " + str(cude) + "\nAll operations rolled back."
            )
        return return_values


    async def create(self, request, permissions=None):
        """
        Railgun CRUD - Create. Create a record.
        Request format is expected as:
        {
            "schema": <schema>,
            "entity": <entity>,
            "data": {
                <field>: <value>
            }
        }

        :param dict request: creation request
        :param set permissions: user permissions (TODO)

        :returns: entity that was created
        :rtype: dict
        """
        _db_pool = self.data[request["schema"]]
        request["table"] = self.STELLAR.STELLAR[request["schema"]].entities[request["entity"]].code
        async with _db_pool.stage() as db:
            result = await self._create(db, request)
        return result

    async def _create(self, db, op, permissions=None):
        # Perform any data manipualtions needed, and
        # set up any relations we may need to add (entity field updates pepehands)
        create_rel = self._op_middleware(op)

        created = await db.create(op)
        for ftu in create_rel:  # TODO sucks that we have to iterate over these essentially twice
            for newrel in ftu["data"]:
                rel_config = ftu["sf"].params["constraints"][newrel["type"]]
                if self.STELLAR.STELLAR[op["schema"]].entities[newrel["type"]].fields[rel_config["col"]].type == "ENTITY":
                    # We need to wipe other foreign field relations too if the foreign field we're updating is a single-entity field,
                    # to make sure there's only one. Have to do this in the create relation part as we only want to delete it if we're
                    # replacing it with something.
                    for fdelrel in self.STELLAR.STELLAR[op["schema"]].entities[newrel["type"]].fields[rel_config["col"]].params["constraints"].values():
                        await db.delete_relation(
                            fdelrel["relation"],
                            self.STELLAR.STELLAR[op["schema"]].entities[newrel["type"]].code,
                            rel_config["col"],
                            newrel["uid"]
                        )
                await db.create_relation(
                    rel_config["relation"],
                    self.STELLAR.STELLAR[op["schema"]].entities[op["entity"]].code,
                    rel_config["table"],
                    (ftu["sf"].code, created["uid"], newrel["uid"], rel_config["col"])
                )
        return created


    async def update(self, request, permissions=None):
        """
        Railgun CRUD - Update. Update a record.
        Request format is expected as:
        {
            "schema": <schema>,
            "entity": <entity>,
            "entity_id": <id of entity to update>,
            "data": {
                <field>: <value>
            }
        }

        :param dict request: update request
        :param set permissions: user permissions (TODO)

        :returns: entity that was updated
        :rtype: dict
        """
        _db_pool = self.data[request["schema"]]
        request["table"] = self.STELLAR.STELLAR[request["schema"]].entities[request["entity"]].code
        async with _db_pool.stage() as db:
            result = await self._update(db, request)
        return result

    async def _update(self, db, op, permissions=None):
        # Perform any data manipualtions needed, and
        # set up any relations we may need to add (entity field updates pepehands)
        update_rel = self._op_middleware(op)

        if op["data"]:  # We only need to do a "normal" update if there's non-relation things to update
            updated = await db.update(op)
        else:
            # Still set a return dict for updated ops, even if we only changed relations
            updated = {"type": op["entity"], "uid": op["entity_id"]}

        for ftu in update_rel:  # TODO sucks that we have to iterate over these essentially twice
            for delrel in ftu["sf"].params["constraints"].values():
                await db.delete_relation(
                    delrel["relation"],
                    self.STELLAR.STELLAR[op["schema"]].entities[op["entity"]].code,
                    ftu["sf"].code,
                    op["entity_id"]
                )
            for newrel in ftu["data"]:
                rel_config = ftu["sf"].params["constraints"][newrel["type"]]
                if self.STELLAR.STELLAR[op["schema"]].entities[newrel["type"]].fields[rel_config["col"]].type == "ENTITY":
                    # We need to wipe other foreign field relations too if the foreign field we're updating is a single-entity field,
                    # to make sure there's only one. Have to do this in the create relation part as we only want to delete it if we're
                    # replacing it with something.
                    for fdelrel in self.STELLAR.STELLAR[op["schema"]].entities[newrel["type"]].fields[rel_config["col"]].params["constraints"].values():
                        await db.delete_relation(
                            fdelrel["relation"],
                            self.STELLAR.STELLAR[op["schema"]].entities[newrel["type"]].code,
                            rel_config["col"],
                            newrel["uid"]
                        )
                await db.create_relation(
                    rel_config["relation"],
                    self.STELLAR.STELLAR[op["schema"]].entities[op["entity"]].code,
                    rel_config["table"],
                    (ftu["sf"].code, updated["uid"], newrel["uid"], rel_config["col"])
                )
        return updated


    async def delete(self, request, permissions=None):
        """
        Railgun CRUD - Delete. Delete a record.
        Request format is expected as:
        {
            "schema": <schema>,
            "entity": <entity>,
            "entity_id": <id of entity to update>,
            "permanent": any | if this key is present, entity will be purged, not just archived
        }

        :param dict request: deletion request
        :param set permissions: user permissions (TODO)

        :returns: entity that was deleted
        :rtype: dict
        """
        _db_pool = self.data[request["schema"]]
        request["table"] = self.STELLAR.STELLAR[request["schema"]].entities[request["entity"]].code
        async with _db_pool.stage() as db:
            result = await self._delete(db, request)
        return result

    async def _delete(self, db, op, permissions=None):
        if bool(op.get("permanent", False)):  # Some hubris to allow proper parsing of false values
            result = await db.delete(op)
            # Delete any files
            # Part of the connection block as changes will un-commit if file op fails
            ent_file_dir = CONFIG.FILE_DIR / op["schema"] / op["table"] / str(op["entity_id"])
            if ent_file_dir.exists():
                shutil.rmtree(ent_file_dir)
        else:
            op["data"] = {"_ss_archived": True}
            result = await self._update(db, op)
        return result


    async def upload_file(self, filepath, filename, metadata, permissions=None):
        """
        Save uploaded file to correct formatted location on-disk and
        update the record's FILE type field with the path str.

        :params str filepath: path to the temporarily downloaded file
        :params str filename: the original uploaded filename
        :params dict metadata: the upload metadata dict, must container the entity and field for upload
        {
            "schema": <schema>,
            "type": <entity>,
            "uid": <entity_id>,
            "field": <FILE type field>
        }
        :param set permissions: user permissions (TODO)
        """
        if metadata["schema"] not in self.data:
            raise Exception("Schema %s not known" % metadata["schema"])
        elif metadata["type"] not in self.STELLAR.STELLAR[metadata["schema"]].entities:
            raise Exception("Entity %s not in schema %s" % (metadata["type"], metadata["schema"]))
        elif metadata["field"] not in self.STELLAR.STELLAR[metadata["schema"]].entities[metadata["type"]].fields:
            raise Exception("Field %s not in entity %s in schema %s" % (metadata["field"], metadata["type"], metadata["schema"]))
        elif not self.STELLAR.STELLAR[metadata["schema"]].entities[metadata["type"]].fields[metadata["field"]].type.startswith("MEDIA"):
            raise Exception("Field %s in entity %s in schema %s is not a media field" % (metadata["field"], metadata["type"], metadata["schema"]))
        
        entcode = self.STELLAR.STELLAR[metadata["schema"]].entities[metadata["type"]].code

        internal_final_path = Path(metadata["schema"]) / entcode / str(metadata["uid"]) / (metadata["field"]+"_"+filename.decode())
        absolute_final_path = CONFIG.FILE_DIR / internal_final_path

        print("Intended path:")
        print(absolute_final_path)

        if absolute_final_path.exists():
            raise Exception("Path \n%s\nalready taken...")

        # Update the path field with the local path
        # We intentionally don't reuse Railgun.update to bypass _opmiddleware
        # since we "know what we're doing"
        _db_pool = self.data[metadata["schema"]]
        async with _db_pool.stage() as db:
            update = await db.update({
                "table": self.STELLAR.STELLAR[metadata["schema"]].entities[metadata["type"]].code,
                "entity": metadata["type"],
                "entity_id": metadata["uid"],
                "data": {
                    metadata["field"]: str(internal_final_path)
                }
            })

        # Also validate that the target entity actually (still/ever did) exists
        if not update:
            raise Exception("There is no entity %s - %s" % (metadata["type"], metadata["uid"]))

        # Make sure the file directory exists
        absolute_final_path.parent.mkdir(parents=True, exist_ok=True)
        # Move the file to it's final destination (fox only, no items)
        filepath.rename(absolute_final_path)

        # We return the final path all the way to the user, so they can use it to download 
        return {"path": str(internal_final_path)}


    def telescope(self, request, permissions=None):
        """
        Read STELLAR. Individual DBs do not know their own schema. Use this.
        Expected format:
        {
            "schema": <schema_code>, # OPTIONAL
            "entity": <entity_code>, # OPTIONAL
            "lightweight": <bool> # OPTIONAL
        }
        If an entity is provided, a schema must be provided.

        If an entity and schema are provided, the telescope of that entity will be returned
        If only a schema is provided, the telescope of all entities within that schema will be returned
        If no schema is provided, a basic telescope of available schemas will be returned

        If lightweight is passed as true, only the top of the requested layer is returned
        (only the schemas, schema, or entity)

        :param dict request: schema read request
        :param set permissions: user permissions (TODO)

        :returns: STELLAR schema
        :rtype: STELLAR
        """
        if request.get("entity"):
            if not request.get("schema"):
                raise HTTPException(500, "Missing schema parameter")
            return self.STELLAR.STELLAR[request["schema"]].entities[request["entity"]].telescope()
        elif request.get("schema"):
            return self.STELLAR.STELLAR[request["schema"]].telescope()
        else:
            return self.STELLAR.STELLAR.telescope(lightweight=request.get("lightweight", False))


    async def stellar(self, request):
        """
        *kira kira*
        Processes a schema update request, the essense of Stellar Stellar.
        Offloads the actual work to the StellarStellar object.
        Request format is variable, depending on update request type:

        Field modification request:
        {
            "part": "field",
            "request_type": "create"|"update"|"delete",
            "schema": <schema_code>,
            "entity": <entity_code>,
            "data": {
                > IF request_type == CREATE
                "code": <field_code>,
                "name": <field_name>,
                "type": <field_type>,
                "options": <field_options>  # OPTIONAL

                > IF request_type == UPDATE
                "code": <field_code>,
                "name": <field_name>,  # OPTIONAL
                "options": <field_options>  # OPTIONAL
                
                > IF request_type == DELETE
                "code": <field_code>
            }
        }

        Entity modification request:
        {
            "part": "entity",
            "request_type": "create"|"update"|"delete",
            "schema": <schema_code>,
            "data": {
                > IF request_type == CREATE
                "code": <entity_code>,
                "soloname": <entity_soloname>,
                "multiname": <entity_multiname>

                > IF request_type == UPDATE
                "code": <entity_code>,
                "soloname": <entity_soloname>, # OPTIONAL
                "multiname": <entity_multiname>, # OPTIONAL

                > IF request_type == DELETE
                "code": <entity_code>
            }
        }

        Schema modification request:
        {
            "part": "schema",
            "request_type": "register"|"update"|"release"
            "schema": <schema_code>,
            "data": {
                > IF request_type == REGISTER
                TODO NYI

                > IF request_type == UPDATE
                "name": <schema_name>, # OPTIONAL

                > IF request_type == RELEASE
                TODO NYI
            }
        }
        """
        try:
            assert "part" in request and "request_type" in request and "schema" in request and request["schema"] in self.data
            # TODO permissions monkaS
            # Use the db and stellardb contexts. If the DB fails, we don't need to commit to stellar.
            # If stellar fails, the physical entries still exist, but in that very edge case, we can deal with it manually much more easily.
            # IN THEORY. TODO think about it.
            async with self.STELLAR.database.stage() as stellardb:
                async with self.data[request["schema"]].stage() as db:
                    comet = await self.STELLAR.funny_factory[request["part"]][request["request_type"]](request, db, stellardb)
            if comet:
                # Stellar Stellar
                await self.STELLAR.shoot_for_the_stars(comet)
        except NotImplementedError:
            resp = "NYI"
        except AssertionError:
            resp = "Bad Request"
        except KeyError:
            raise
        except:
            raise  # TODO
            resp = "Error"
        return None  # Explicit for visibility


    async def internal_operations(self, entity_type, operation, request, permissions):
        """
        Validator function for "always-on" authorized internal operations.
        This is for things like user, page, permission management in instances where `railgun_internal` is not
        an explicitely exposed DB (which it shouldn't.)

        TODO This will also envelop more explicit endpoints for Stellar components once I get around to that (requires refactor).

        Authorized entity values currently are:
            - Page
            - Page Setting
        These are defined in a helper class, instantiated during railgun initialization. Though could debatably be owned by Stellar
        or even railgun itself.

        The internal factory will raise a 404 error if the internal entity requested is not registered, and a 400 error if the operation
        being requested is not registered.

        :param str entity_type: internal entity type to perform the operation on
        :param str operation: operation to perform
        :param dict request: internal operation request
        :param dict permissions: permissions of the user to be applied for the operation

        :returns: the result of the operation:
        :rtype: variable, probably dict
        """
        async with self.STELLAR.database.stage() as db:
            return await self.internal_operations_factory[entity_type][operation](db, request, permissions)


    def disconnect(self):
        """
        Disconnect all DBs. Theoretically called on shutdown, but only used in tests.
        """
        for db in self.data:
            self.data[db].disconnect()


    def _op_middleware(self, op):
        """
        """
        rel_manager = []
        # We need to perform extra actions on some field types (list, entity), alas
        for op_field in list(op["data"].keys()):  # HACK allow us to pop for entity fields
            # Assume archival stuff is managed properly. Someone could meme it potentially though.
            # Not actually relevant on creation, realistically, but ease of generalization
            if op_field == "_ss_archived":  # TODO maybe archive should be its own type?
                continue
            rel = self.STELLAR.STELLAR[op["schema"]].entities[op["entity"]].fields[op_field].middleware(op)
            if rel:
                rel_manager.append(rel)
        return rel_manager


    def _linked_return_field_builder(self, linked_field, i, schema_sc, base_type):
        """
        Documentation TODO
        In general, we rebuild the return field here as the ftype is restricted to the dot-path definition.
        """
        table_sc = schema_sc[base_type].fields
        base_table = schema_sc[base_type].code

        if table_sc[linked_field[i]].type == "ENTITY":
            return_field_subset = EntityReturnField(
                name=linked_field[i],
                join={"constraints": table_sc[linked_field[i]].params["constraints"].values(), "local_table": base_table},
                values=[
                    PresetReturnField(name="type", value=linked_field[i+1]),
                    ReturnField(table=schema_sc[linked_field[i+1]].code, name="uid"),
                    ReturnField(table=schema_sc[linked_field[i+1]].code, name=schema_sc[linked_field[i+1]].display_name_col),
                ]
            )
        elif table_sc[linked_field[i]].type == "MULTIENTITY":
            return self._linked_multientity_return_field_builder(linked_field, i, schema_sc, base_type)


        if schema_sc[linked_field[i+1]].fields[linked_field[i+2]].type == "ENTITY":
            # We need to go deeper
            if i+3<len(linked_field):
                return_field_subset.put(self._linked_return_field_builder(
                    linked_field, i+2, schema_sc, linked_field[i+1]
                ))
            # This is as deep as it gets
            else:
                for ftype in schema_sc[linked_field[i+1]].fields[linked_field[i+2]].params["constraints"]:
                    target_sc = schema_sc[ftype]
                    return_field_subset.put(
                        EntityReturnField(
                            name=linked_field[i+2],
                            join={"constraints": schema_sc[linked_field[i+1]].fields[linked_field[i+2]].params["constraints"].values(), "local_table": schema_sc[linked_field[i+1]].code},
                            values=[
                                PresetReturnField(name="type", value=ftype),
                                ReturnField(table=target_sc.code, name="uid"),
                                ReturnField(table=target_sc.code, name=target_sc.display_name_col)
                            ]
                        )
                    )
        elif schema_sc[linked_field[i+1]].fields[linked_field[i+2]].type == "MULTIENTITY":
            pass  # TODO
        else:
            return_field_subset.put(
                ReturnField(table=schema_sc[linked_field[i+1]].code, name=linked_field[i+2]),
            )
        return return_field_subset


    def _linked_multientity_return_field_builder(self, linked_field, i, schema_sc, base_type):
        """
        """
        table_sc = schema_sc[base_type].fields
        base_table = schema_sc[base_type].code

        return_field_subset = MultiEntityReturnField(
                table=base_table,
                name=linked_field[i],
                join=table_sc[linked_field[i]].params["constraints"],
                values={
                    ftype: [
                        PresetReturnField(name="type", value=linked_field[i+1]),
                        ReturnField(table=schema_sc[linked_field[i+1]].code, name="uid"),
                        ReturnField(table=schema_sc[linked_field[i+1]].code, name=schema_sc[linked_field[i+1]].display_name_col),
                    ] for ftype in table_sc[linked_field[i]].params["constraints"]
                }
            )
        # for ftype in table_sc[linked_field[i]]["params"]["constraints"]:  # BUG - overwrites if multi-entity multi-type. JSON_AGG should be one level up | actually ftype is static because of the dot-path, so just set directly
        #     return_field_subset = MultiEntityReturnField(
        #         table=base_table,
        #         name=linked_field[i],
        #         join=table_sc[linked_field[i]]["params"]["constraints"][ftype],
        #         values=[
        #             PresetReturnField(name="type", value=linked_field[i+1]),
        #             ReturnField(table=schema_sc[linked_field[i+1]]["code"], name="uid"),
        #             ReturnField(table=schema_sc[linked_field[i+1]]["code"], name=schema_sc[linked_field[i+1]]["display_name_col"]),
        #         ]
        #     )

        if schema_sc[linked_field[i+1]].fields[linked_field[i+2]].type == "ENTITY":
            # We need to go deeper
            if i+3<len(linked_field):
                return_field_subset.put(
                    linked_field[i+1],
                    [self._linked_return_field_builder(
                        linked_field, i+2, schema_sc, linked_field[i+1]
                    )]
                )
            # This is as deep as it gets
            else:
                for ftype in schema_sc[linked_field[i+1]].fields[linked_field[i+2]].params["constraints"]:
                    target_sc = schema_sc[ftype]
                    return_field_subset.put(
                        linked_field[i+1],
                        [EntityReturnField(
                            name=linked_field[i+2],
                            join={"constraints": schema_sc[linked_field[i+1]].fields[linked_field[i+2]].params["constraints"].values(), "local_table": schema_sc[linked_field[i+1]].code},
                            values=[
                                PresetReturnField(name="type", value=ftype),
                                ReturnField(table=target_sc.code, name="uid"),
                                ReturnField(table=target_sc.code, name=target_sc.display_name_col)
                            ]
                        )]
                    )
        elif schema_sc[linked_field[i+1]].fields[linked_field[i+2]].type == "MULTIENTITY":
            pass  # TODO
        else:
            return_field_subset.put(
                linked_field[i+1],
                [ReturnField(table=schema_sc[linked_field[i+1]].code, name=linked_field[i+2])],
            )
        return return_field_subset
