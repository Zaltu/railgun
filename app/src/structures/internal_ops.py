"""
Vaguely bootleg wrapping functionality for always-on internal operation management.
This module houses the classes and functionality to remap and validate an internal
operation route (/stellar/<entity_type>/<operation>) to a valid callable.
"""
from fastapi import HTTPException


class SimpleInternal(dict):
    def __init__(self, railgun_app, entity_type):
        self.rg_entity_type = entity_type
        self.rg_entity_table = railgun_app.STELLAR.STELLAR["railgun_internal"].entities[entity_type].code
        self["read"] = railgun_app._read
        self["create"] = railgun_app._create
        self["update"] = railgun_app._update
        self["delete"] = railgun_app._delete
        self["batch"] = railgun_app.batch  # TODO fix

    def __getitem__(self, key):
        try:
            return lambda db, request, permissions: self._wrap_simple_internal(super(SimpleInternal, self).__getitem__(key), db, request, permissions)
        except KeyError:
            raise HTTPException(400)

    def _wrap_simple_internal(self, func, db, request, permissions):
        """
        The simplest forms of internal calls require adding the expected schema and entity type to the
        request, as well as the internal table name (based on what was supplied in the route).
        # TODO this returns a coroutine that really needs to be awaited. Might want to do that here just in case.
        """
        request["schema"] = "railgun_internal"
        request["entity"] = self.rg_entity_type
        request["table"] = self.rg_entity_table
        return func(db, request, permissions)


class InternalOperations(dict):
    def __init__(self, railgun_app):
        self["Page"] = SimpleInternal(railgun_app, "Page")
        self["Page Setting"] = SimpleInternal(railgun_app, "Page Setting")

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            raise HTTPException(404)
