import orjson

from db import *
from config import CONFIG


DB_TYPES = {
    "PSQL": PSQL
}

def RailConfig():
    """
    """
    dbcs = []
    cfiles = CONFIG.DB_CONFIG_PATH.glob("*")
    for cfile in cfiles:
        with open(cfile, 'r') as infile:
            dbcs.append(orjson.loads(infile.read()))

    dbs = {}
    for db in dbcs:
        valid = _validate_config(db)
        if not valid or db.get("NAME") in dbs:
            continue  # or raise? TODO
        dbs[db["PARAMS"]["DB_NAME"]] = _db_factory(db)

    return dbs


def _validate_config(db):
    """
    Just make sure all required keys are there.
    TODO should probably be DB class method.
    """
    try:
        assert db.get("NAME") and db.get("DB_TYPE") and isinstance(db.get("PARAMS"), dict)
        assert db.get("DB_TYPE") in DB_TYPES
        assert "DB_NAME" in db["PARAMS"]
        assert "DB_USER" in db["PARAMS"]
    except AssertionError:
        return False
    return True


def _db_factory(db):
    """
    Generate the appropriate DB.
    """
    return DB_TYPES[db["DB_TYPE"]](db["PARAMS"])