import os
from pathlib import Path
from json import JSONDecodeError

# Needed for one op
import shutil

import bcrypt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src.modules.railconfig import RailConfig
from src.stellar_stellar import StellarStellar
from db._database import CUDError

# TEST TODO
from src.structures.returnfields import ReturnFieldSet, PresetReturnField, ReturnField


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
ALLOWED_CORS_ORIGINS.extend(os.environ["RG_URL"].split(","))


class Railgun(FastAPI):
    """
    Kaboom.
    """
    FILE_DIR=Path(os.environ.get("RG_FILE_DIR") or "/opt/railgun/files")
    FILE_TEMP_DIR="_ss_working"
    def __init__(self):
        super().__init__()

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


    def read(self, request):
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
        """
        # Preformatting default archived filter
        FILTERS = _DEFAULT_QUERY_FILTER(request)
        if request["read"].get("filters"):
            FILTERS["filters"].append(request["read"]["filters"])

        # Helpers for syntax
        schema_sc = self.STELLAR.STELLAR[request["schema"]]["entities"]
        table_sc = schema_sc[request["entity"]]["fields"]

        # Setup default structures
        return_fields = ReturnFieldSet(None, [])
        joins = {"ENTITY":{}, "MULTIENTITY": {}}

        # Ensure return_fields exists
        request["read"]["return_fields"] = request["read"].get("return_fields") or []
        if "uid" not in request["read"]["return_fields"]:
            request["read"]["return_fields"].append("uid")

        # Always include base type
        return_fields.put(PresetReturnField("type", request["entity"]))

        for field in request["read"]["return_fields"]:
            if "." in field:
                # Assume linked field, normal fields should not have special characters in their codes
                linked_field = field.split(".")
                # Ensure we have a properly formatted linked field request
                assert len(linked_field)%2==1

                # Parse linked fields recursively for arbitrary depth
                return_fields.put(
                    self._linked_return_field_builder(joins, linked_field, 0, schema_sc, request["entity"])
                )
            else:
                # TODO review this
                #if table_sc[field]["type"] == "PASSWORD":
                    # Bit of custom logic to never return password fields
                #    preset_return_fields[field] = "********"
                #    continue
                if table_sc[field]["type"] == "ENTITY":
                    joins["ENTITY"][field] = {"constraints": table_sc[field]["params"]["constraints"].values(), "local_table": schema_sc[request["entity"]]["code"]}
                    for ftype in table_sc[field]["params"]["constraints"]:
                        return_fields.put(ReturnFieldSet(
                            name=field,
                            values=[
                                PresetReturnField(name="type", value=ftype),
                                ReturnField(table=schema_sc[ftype]["code"], name="uid"),
                                ReturnField(table=schema_sc[ftype]["code"], name=schema_sc[ftype]["display_name_col"])
                            ]
                        ))
                elif table_sc[field]["type"] == "MULTIENTITY":
                    joins["MULTIENTITY"][field] = table_sc[field]["params"]["constraints"]
                    if "displaycols" not in joins:  # Sus
                        joins["displaycols"] = {key:value["display_name_col"] for key, value in schema_sc.items()}
                    for ftype in table_sc[field]["params"]["constraints"]:
                        return_fields.put(ReturnField(
                            table=table_sc[field]["params"]["constraints"][ftype]["relation"],
                            name=field
                        ))
                else:
                    return_fields.put(ReturnField(
                        table=schema_sc[request["entity"]]["code"],
                        name=field
                    ))

        target = self.data[request["schema"]]
        resp = target.query(
            table=schema_sc[request["entity"]]["code"],
            entity_type=schema_sc[request["entity"]]["soloname"],
            fields=return_fields,
            joins=joins,
            filters=FILTERS,
            pagination=request["read"].get("pagination") or 25,
            page=request["read"].get("page") or 1,
            order=request["read"].get("order") or "uid",
        )
        if bool(request["read"].get("include_count", False)):
            query_total_count = target.count(schema_sc[request["entity"]]["code"], FILTERS)
            resp.append(query_total_count)
        return resp


    def batch(self, request):
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
        the software is used.

        Just don't use it the bad way 4head.

        :param dict request: CUD batch request

        :returns: list of entities effected by the operation
        :rtype: list[dict]
        """
        try:
            return_values = []
            assert "batch" in request
            db = self.data[request["schema"]]
            with db.stage() as conn:
                for op in request["batch"]:
                    # We'll need to pass the actual table to the DB regardless of the operation
                    op["table"] = self.STELLAR.STELLAR[request["schema"]]["entities"][op["entity"]]["code"]
                    op["schema"] = request["schema"]
                    # Process updates
                    if op["request_type"] == "update":
                        return_values.append(self._update(db, op, conn))

                    # Process creates
                    elif op["request_type"] == "create":
                        return_values.append(self._create(db, op, conn))

                    # Process deletes
                    elif op["request_type"] == "delete":
                        return_values.append(self._delete(db, op, conn))

                    else:
                        raise CUDError("Unrecognized request type: %s" % op["request_type"])

        except (AssertionError, CUDError, KeyError) as cude:
            raise HTTPException(
                status_code=500,
                detail=str(type(cude)) + " " + str(cude) + "\nAll operations rolled back."
            )
        return return_values


    def create(self, request):
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

        :returns: entity that was created
        :rtype: dict
        """
        db = self.data[request["schema"]]
        request["table"] = self.STELLAR.STELLAR[request["schema"]]["entities"][request["entity"]]["code"]
        with db.stage() as conn:
            result = self._create(db, request, conn)
        return result

    def _create(self, db, op, conn):
        # Perform any data manipualtions needed, and
        # set up any relations we may need to add (entity field updates pepehands)
        create_rel = self._op_middleware(op)

        created = db.create(op, conn)
        for ftu in create_rel:  # TODO sucks that we have to iterate over these essentially twice
            for newrel in ftu["data"]:
                rel_config = ftu["sf"]["params"]["constraints"][newrel["type"]]
                if self.STELLAR.STELLAR[op["schema"]]["entities"][newrel["type"]]["fields"][rel_config["col"]]["type"] == "ENTITY":
                    # We need to wipe other foreign field relations too if the foreign field we're updating is a single-entity field,
                    # to make sure there's only one. Have to do this in the create relation part as we only want to delete it if we're
                    # replacing it with something.
                    for fdelrel in self.STELLAR.STELLAR[op["schema"]]["entities"][newrel["type"]]["fields"][rel_config["col"]]["params"]["constraints"].values():
                        db.delete_relation(
                            fdelrel["relation"],
                            self.STELLAR.STELLAR[op["schema"]]["entities"][newrel["type"]]["code"],
                            rel_config["col"],
                            newrel["uid"],
                            conn
                        )
                db.create_relation(
                    rel_config["relation"],
                    self.STELLAR.STELLAR[op["schema"]]["entities"][op["entity"]]["code"],
                    rel_config["table"],
                    (ftu["sf"]["code"], created["uid"], newrel["uid"], rel_config["col"]),
                    conn
                )
        return created


    def update(self, request):
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

        :returns: entity that was updated
        :rtype: dict
        """
        db = self.data[request["schema"]]
        request["table"] = self.STELLAR.STELLAR[request["schema"]]["entities"][request["entity"]]["code"]
        with db.stage() as conn:
            result = self._update(db, request, conn)
        return result

    def _update(self, db, op, conn):
        # Perform any data manipualtions needed, and
        # set up any relations we may need to add (entity field updates pepehands)
        update_rel = self._op_middleware(op)

        if op["data"]:  # We only need to do a "normal" update if there's non-relation things to update
            updated = db.update(op, conn)
        else:
            # Still set a return dict for updated ops, even if we only changed relations
            updated = {"type": op["entity"], "uid": op["entity_id"]}

        for ftu in update_rel:  # TODO sucks that we have to iterate over these essentially twice
            for delrel in ftu["sf"]["params"]["constraints"].values():
                db.delete_relation(
                    delrel["relation"],
                    self.STELLAR.STELLAR[op["schema"]]["entities"][op["entity"]]["code"],
                    ftu["sf"]["code"],
                    op["entity_id"],
                    conn
                )
            for newrel in ftu["data"]:
                rel_config = ftu["sf"]["params"]["constraints"][newrel["type"]]
                if self.STELLAR.STELLAR[op["schema"]]["entities"][newrel["type"]]["fields"][rel_config["col"]]["type"] == "ENTITY":
                    # We need to wipe other foreign field relations too if the foreign field we're updating is a single-entity field,
                    # to make sure there's only one. Have to do this in the create relation part as we only want to delete it if we're
                    # replacing it with something.
                    for fdelrel in self.STELLAR.STELLAR[op["schema"]]["entities"][newrel["type"]]["fields"][rel_config["col"]]["params"]["constraints"].values():
                        db.delete_relation(
                            fdelrel["relation"],
                            self.STELLAR.STELLAR[op["schema"]]["entities"][newrel["type"]]["code"],
                            rel_config["col"],
                            newrel["uid"],
                            conn
                        )
                db.create_relation(
                    rel_config["relation"],
                    self.STELLAR.STELLAR[op["schema"]]["entities"][op["entity"]]["code"],
                    rel_config["table"],
                    (ftu["sf"]["code"], updated["uid"], newrel["uid"], rel_config["col"]),
                    conn
                )
        return updated


    def delete(self, request):
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

        :returns: entity that was deleted
        :rtype: dict
        """
        db = self.data[request["schema"]]
        request["table"] = self.STELLAR.STELLAR[request["schema"]]["entities"][request["entity"]]["code"]
        with db.stage() as conn:
            result = self._delete(db, request, conn)
        return result

    def _delete(self, db, op, conn):
        if bool(op.get("permanent", False)):  # Some hubris to allow proper parsing of false values
            result = db.delete(op, conn)
            # Delete any files
            # Part of the connection block as changes will un-commit if file op fails
            ent_file_dir = Railgun.FILE_DIR / op["schema"] / op["table"] / str(op["entity_id"])
            if ent_file_dir.exists():
                shutil.rmtree(ent_file_dir)
        else:
            op["data"] = {"_ss_archived": True}
            result = self._update(db, op, conn)
        return result


    def upload_file(self, filepath, filename, metadata):
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
        """
        if metadata["schema"] not in self.data:
            raise Exception("Schema %s not known" % metadata["schema"])
        elif metadata["type"] not in self.STELLAR.STELLAR[metadata["schema"]]["entities"]:
            raise Exception("Entity %s not in schema %s" % (metadata["type"], metadata["schema"]))
        elif metadata["field"] not in self.STELLAR.STELLAR[metadata["schema"]]["entities"][metadata["type"]]["fields"]:
            raise Exception("Field %s not in entity %s in schema %s" % (metadata["field"], metadata["type"], metadata["schema"]))
        elif not self.STELLAR.STELLAR[metadata["schema"]]["entities"][metadata["type"]]["fields"][metadata["field"]]["type"].startswith("MEDIA"):
            raise Exception("Field %s in entity %s in schema %s is not a media field" % (metadata["field"], metadata["type"], metadata["schema"]))
        
        entcode = self.STELLAR.STELLAR[metadata["schema"]]["entities"][metadata["type"]]["code"]

        internal_final_path = Path(metadata["schema"]) / entcode / str(metadata["uid"]) / (metadata["field"]+"_"+filename.decode())
        absolute_final_path = Railgun.FILE_DIR / internal_final_path

        print("Intended path:")
        print(absolute_final_path)

        if absolute_final_path.exists():
            raise Exception("Path \n%s\nalready taken...")

        # Update the path field with the local path
        # We intentionally don't reuse Railgun.update to bypass opmiddleware
        # since we "know what we're doing"
        db = self.data[metadata["schema"]]
        with db.stage() as conn:
            update = db.update({
                "table": self.STELLAR.STELLAR[metadata["schema"]]["entities"][metadata["type"]]["code"],
                "entity": metadata["type"],
                "entity_id": metadata["uid"],
                "data": {
                    metadata["field"]: str(internal_final_path)
                }
            }, conn)

        # Also validate that the target entity actually (still/ever did) exists
        if not update:
            raise Exception("There is no entity %s - %s" % (metadata["type"], metadata["uid"]))

        # Make sure the file directory exists
        absolute_final_path.parent.mkdir(parents=True, exist_ok=True)
        # Move the file to it's final destination (fox only, no items)
        filepath.rename(absolute_final_path)

        # We return the final path all the way to the user, so they can use it to download 
        return {"path": str(internal_final_path)}


    def telescope(self, request):
        """
        Read STELLAR. Individual DBs do not know their own schema. Use this.
        Expected format:
        {
            "schema": <schema_code>,
            "entity": <entity_code> # OPTIONAL
        }

        :param dict request: schema read request

        :returns: STELLAR schema
        :rtype: STELLAR
        """
        if "entity" in request:
            return self.STELLAR.STELLAR[request["schema"]]["entities"][request["entity"]]
        return self.STELLAR.STELLAR[request["schema"]]


    def stellar(self, request):
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
            resp = self.STELLAR.funny_factory[request["part"]][request["request_type"]](request, self.data[request["schema"]])
        except NotImplementedError:
            resp = "NYI"
        except AssertionError:
            resp = "Bad Request"
        except KeyError:
            raise
            resp = request["request_type"] + " " + request["part"] + " is not an implemented STELLAR operation."
        except:
            raise  # TODO
            resp = "Error"
        return resp


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
            if op_field == "_ss_archived":
                continue
            # Optimization
            stellar_field = self.STELLAR.STELLAR[op["schema"]]["entities"][op["entity"]]["fields"][op_field]
            # Middleware for applicable field types
            if stellar_field["type"] == "LIST":
                # Validate list option exists
                assert op["data"][op_field] in stellar_field["params"].get("constraints", [])
            elif stellar_field["type"] == "MULTIENTITY":
                # we presume that the value being used for data is correct
                rel_manager.append({
                    "sf": stellar_field,
                    "data": op["data"].pop(op_field) or []  # [] in case set to None
                })
            elif stellar_field["type"] == "ENTITY":
                # we presume that the value being used for data is correct
                # Slap the single entity update into a list and hope for the best
                assert type(op["data"][op_field]) == dict
                rel_manager.append({
                    "sf": stellar_field,
                    "data": [op["data"].pop(op_field) or []]  # [] in case set to None
                })
            elif stellar_field["type"] == "MEDIA":
                # Media fields can be set to an existing local path within FILE_DIR or None to unset.
                # Otherwise new media needs to be added via /upload
                if op["data"][op_field]:
                    # We do this to allow manual manipulations if absolutely needed.
                    abs_path = (Railgun.FILE_DIR / Path(op["data"][op_field])).absolute().resolve()
                    assert Railgun.FILE_DIR in abs_path.parents
                    assert abs_path.exists()
                elif "entity_id" in op:  # This can only be done on update, on create there will be nothing to do
                    # Set to None in order to "wipe" the field, but then we need to delete the media...
                    # TODO this *really* shouldn't happen here, the op hasn't actually passed through yet.
                    # BUG field names could overlap and cause there to be more than one, or an incorrect file being matched.
                    # The real path needs to be fetched from DB for deletion.
                    # Any kind of failure and it's joever...
                    # Locate general entity path
                    ent_file_dir = Railgun.FILE_DIR / op["schema"] / op["table"] / str(op["entity_id"])
                    # Get all potential files (though should be one)
                    file = list(ent_file_dir.glob(op_field+"*"))
                    assert len(file) <= 1  # Could already be no file
                    file[0].unlink()
            elif stellar_field["type"] == "PASSWORD":
                # Encrypt incoming password data
                op["data"][op_field] = bcrypt.hashpw(op["data"][op_field].encode(), bcrypt.gensalt()).decode()
        return rel_manager


    def _linked_return_field_builder(self, joins, linked_field, i, schema_sc, base_type):
        """
        """
        table_sc = schema_sc[base_type]["fields"]
        base_table = schema_sc[base_type]["code"]

        joins["ENTITY"][linked_field[i]] = {
            "constraints": table_sc[linked_field[i]]["params"]["constraints"].values(),
            "local_table": base_table
        }
        return_field_subset = ReturnFieldSet(
            name=linked_field[i],
            values=[
                PresetReturnField(name="type", value=linked_field[i+1]),
                ReturnField(table=schema_sc[linked_field[i+1]]["code"], name="uid"),
                ReturnField(table=schema_sc[linked_field[i+1]]["code"], name=schema_sc[linked_field[i+1]]["display_name_col"]),
            ]
        )

        if schema_sc[linked_field[i+1]]["fields"][linked_field[i+2]]["type"] == "ENTITY":
            # We need to go deeper
            if i+3<len(linked_field):
                return_field_subset.put(self._linked_return_field_builder(
                    joins, linked_field, i+2, schema_sc, linked_field[i+1]
                ))
            # This is as deep as it gets
            else:
                joins["ENTITY"][linked_field[i+2]] = {
                    "constraints": schema_sc[linked_field[i+1]]["fields"][linked_field[i+2]]["params"]["constraints"].values(),
                    "local_table": schema_sc[linked_field[i+1]]["code"]
                }
                for ftype in schema_sc[linked_field[i+1]]["fields"][linked_field[i+2]]["params"]["constraints"]:
                    target_sc = schema_sc[ftype]
                    return_field_subset.put(
                        ReturnFieldSet(
                            name=linked_field[i+2],
                            values=[
                                PresetReturnField(name="type", value=ftype),
                                ReturnField(table=target_sc["code"], name="uid"),
                                ReturnField(table=target_sc["code"], name=target_sc["display_name_col"])
                            ]
                        )
                    )
        elif schema_sc[linked_field[i+1]]["fields"][linked_field[i+2]]["type"] == "MULTIENTITY":
            # TODO MULTIENTITY
            pass
        else:
            return_field_subset.put(
                ReturnField(table=schema_sc[linked_field[i+1]]["code"], name=linked_field[i+2]),
            )
        return return_field_subset
