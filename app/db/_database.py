"""
Parent class for various DB connectors for GUD compatibility.
Implement each individually.
TODO
"""
import re

class Database():
    """
    A GUD-compliant database.
    """
    def __init__(self):
        """
        TODO
        """
    #####################################
    ###########   Abstract   ############
    #####################################
    def connect(self):
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

    def fetch_table_names(self):
        raise NotImplementedError()

    def fetch_table_columns(self, table):
        raise NotImplementedError()

    def query(self, table, fields, filters, pagination, page, order):
        raise NotImplementedError()

    def validate_table_naming(text):
        """
        TODO
        """
        return re.match("^[a-z_]+$", text) != None


class CUDError(Exception):
    """
    Error type for CrUD errors, caught by railgun CrUD wrappers.
    """