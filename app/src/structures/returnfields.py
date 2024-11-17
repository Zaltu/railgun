"""
Definition of the ReturnField class
"""

class ReturnFieldSet():
    """
    """
    def __init__(self, name, values):
        self.name = name
        self._return_fields = {}
        for value in values:
            self.put(value)

    def put(self, return_field):
        if type(self._return_fields.get(return_field.name))==ReturnFieldSet:
            print("Found a dup")
            for value in return_field:
                self._return_fields[return_field.name].put(value)
        else:
            self._return_fields[return_field.name] = return_field
    
    def __iter__(self):
        for return_field in self._return_fields.values():
            yield return_field


class ReturnField():
    """
    """
    def __init__(self, table, name):
        self.table = table
        self.name = name

class PresetReturnField():
    """
    """
    def __init__(self, name, value):
        self.name = name
        self.value = value