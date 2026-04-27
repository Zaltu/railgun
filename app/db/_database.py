"""
Parent class for various DB connectors for GUD compatibility.
Implement each individually.
TODO
"""
import re

class Database():
    """
    A GUD-compliant database.
    TODO
    """
    def __init__(self):
        """
        TODO
        """
    #####################################
    ###########   Abstract   ############
    #####################################
    # TODO, revisit


class CUDError(Exception):
    """
    Error type for CrUD errors, caught by railgun CrUD wrappers.
    """