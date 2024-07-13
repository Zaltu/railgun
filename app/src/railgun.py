from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from json import JSONDecodeError

from src.railconfig import RailConfig
from src.stellar_stellar import StellarStellar
from db._database import CUDError


class Railgun(FastAPI):
    """
    Kaboom.
    """
    allowed_origins = [
        'http://127.0.0.1:5500',
        'http://localhost',
        'http://0.0.0.0',
        'http://127.0.0.1',
        'http://127.0.0.1:8888',
        'http://127.0.0.1:5174',
        'http://127.0.0.1:5173',
        'http://localhost:5174',
        'http://localhost:5173'
    ]
    def __init__(self):
        super().__init__()

        self.add_middleware(
            CORSMiddleware,
            allow_origins=Railgun.allowed_origins,
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
                "page": Page Number,
                "pagination": Entries per page,
                "order": Field to sort by
                "filters": Filter set (see app docs TODO)
            }
        }
        """
        schema_sc = self.STELLAR.STELLAR[request["schema"]]["entities"]
        table_sc = schema_sc[request["entity"]]["fields"]
        return_fields = []
        joins = {"ENTITY":{}, "MULTIENTITY": {}}
        if "uid" not in request["read"]["return_fields"]:
            request["read"]["return_fields"].append("uid")
        for field in request["read"].get("return_fields") or ["uid"]:
            if table_sc[field]["type"] == "ENTITY":
                joins["ENTITY"][field] = table_sc[field]["params"]["constraints"].values()
                for ftype in table_sc[field]["params"]["constraints"]:
                    return_fields.append((ftype, schema_sc[ftype]["code"], schema_sc[ftype]["display_name_col"], field))
            elif table_sc[field]["type"] == "MULTIENTITY":
                joins["MULTIENTITY"][field] = table_sc[field]["params"]["constraints"]
                if "displaycols" not in joins:  # Sus
                    joins["displaycols"] = {key:value["display_name_col"] for key, value in schema_sc.items()}
                for ftype in table_sc[field]["params"]["constraints"]:
                    return_fields.append((table_sc[field]["params"]["constraints"][ftype]["relation"], field))
            else:
                return_fields.append((schema_sc[request["entity"]]["code"], field))
        target = self.data[request["schema"]]
        resp = target.query(
            table=schema_sc[request["entity"]]["code"],
            entity_type=schema_sc[request["entity"]]["soloname"],
            fields=return_fields,
            joins=joins,
            filters=request["read"].get("filters"),
            pagination=request["read"].get("pagination") or 25,
            page=request["read"].get("page") or 1,
            order=request["read"].get("order") or "uid"
        )
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
                    # Process updates
                    if op["request_type"] == "update":
                        op["schema"] = request["schema"]
                        return_values.append(self._update(db, op, conn))

                    # Process creates
                    elif op["request_type"] == "create":
                        op["schema"] = request["schema"]
                        return_values.append(self._create(db, op, conn))

                    # Process deletes
                    elif op["request_type"] == "delete":
                        # TODO archive management
                        return_values.append(self._delete(db, op, conn))

                    else:
                        raise CUDError("Unrecognized request type: %s" % op["request_type"])

        except (AssertionError, CUDError, KeyError) as cude:
            return str(cude) + "\nAll operations rolled back."
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
        # Any relations we may need to add (entity field updates pepehands)
        create_rel = []
        # We need to perform extra actions on some field types (list, entity), alas
        for update_field in list(op["data"].keys()):  # HACK allow us to pop for entity fields
            stellar_field = self.STELLAR.STELLAR[op["schema"]]["entities"][op["entity"]]["fields"][update_field]
            # Validate list ops
            if stellar_field["type"] == "LIST":
                _op_validator(op["data"][update_field], stellar_field)
            # Prep newops for entity fields
            elif stellar_field["type"] == "MULTIENTITY":
                # we presume that the value being used for data is correct
                create_rel.append({
                    "sf": stellar_field,
                    "data": op["data"].pop(update_field)
                })
            elif stellar_field["type"] == "ENTITY":
                # we presume that the value being used for data is correct
                # Slap the single entity update into a list and hope for the best
                assert type(op["data"][update_field]) == dict
                create_rel.append({
                    "sf": stellar_field,
                    "data": [op["data"].pop(update_field)]
                })

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
        # Any relations we may need to add (entity field updates pepehands)
        update_rel = []
        # We need to perform extra actions on some field types (list, entity), alas
        for update_field in list(op["data"].keys()):  # HACK allow us to pop for entity fields
            stellar_field = self.STELLAR.STELLAR[op["schema"]]["entities"][op["entity"]]["fields"][update_field]
            # Validate list ops
            if stellar_field["type"] == "LIST":
                _op_validator(op["data"][update_field], stellar_field)
            # Prep newops for entity fields
            elif stellar_field["type"] == "MULTIENTITY":
                update_rel.append({
                    "sf": stellar_field,
                    "data": op["data"].pop(update_field) or []
                })
            elif stellar_field["type"] == "ENTITY":
                # we presume that the value being used for data is correct
                # Slap the single entity update into a list and hope for the best
                assert type(op["data"][update_field]) == dict
                update_rel.append({
                    "sf": stellar_field,
                    "data": [op["data"].pop(update_field) or []]
                })
        if op["data"]:  # We only need to do a "normal" update if there's non-relation things to update
            updated = db.update(op, conn)
        else: updated = {"type": op["entity"], "uid": op["entity_id"]}
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
        return db.delete(op, conn)


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


def _op_validator(data, stellar):
    """
    Validate list fields. Pseudo-generalized since I'm lazy and to avoid premature optimization.

    TODO data validation shouldn't be a generic assertion error on the user side

    :param str data: the data a field will be given
    :param dict stellar: the Stellar of the field being set

    :raises: AssertionError if the intended value is illegal from an application standpoint.
    """
    assert data in stellar["params"].get("constraints", [])
