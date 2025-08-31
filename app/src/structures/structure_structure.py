"""
Python object structures for STELLAR.
"""


class Schema():
    """
    """
    def __init__(self, code, name):
        pass


class Entity():
    """
    """
    def __init__(self, code, soloname):
        pass


class Field():
    """
    """
    def __init__(self, uid, code, name, field_type, entity, description, indexed, params=None, archived=False):
        self.uid = uid
        self.code = code
        self.name = name
        self.field_type = field_type
        self.description = description
        self.indexed = indexed
        self.params = params
        self.archived = archived

        self.entity = entity
