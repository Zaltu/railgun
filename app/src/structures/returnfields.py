"""
Definition of the ReturnField class
"""

class ReturnFieldSet():
    """
    """
    def __init__(self, table, name, values):
        self.table = table
        self.name = name
        self._return_fields = {}
        for value in values:
            self.put(value)

    def put(self, return_field):
        try:
            for value in return_field:
                self._return_fields[return_field.name].put(value)
        except:
            self._return_fields[return_field.name] = return_field

    def __iter__(self):
        for return_field in self._return_fields.values():
            yield return_field

    def __str__(self):
        readable = f"ReturnFieldSet(name={self.name})"
        for value in self._return_fields.values():
            readable += f"\n\t{value}"
        return readable


class ReturnField():
    """
    """
    def __init__(self, table, name):
        self.table = table
        self.name = name

    def __str__(self):
        return f"ReturnField(name={self.name})"


class PresetReturnField():
    """
    """
    def __init__(self, name, value):
        self.name = name
        self.value = value
    
    def __str__(self):
        return f"PresetReturnField(name={self.name})"


class EntityReturnField():
    """
    """
    def __init__(self, name, join, values=[]):
        self.name = name
        self.join = join
        self._return_fields = {}
        for value in values:
            self.put(value)

    def put(self, return_field):
        try:
            for value in return_field:
                self._return_fields[return_field.name].put(value)
        except:
            self._return_fields[return_field.name] = return_field

    def __iter__(self):
        for return_field in self._return_fields.values():
            yield return_field

    def __str__(self):
        readable = f"EntityReturnField(name={self.name})"
        for value in self._return_fields.values():
            readable += f"\n\t{value}"
        return readable


class MultiEntityReturnField():
    """
    """
    def __init__(self, table, name, join, values=[]):
        self.name = name
        self.table = table
        self.join = join
        self._return_fields = {}
        for value in values:
            self.put(value)

    def put(self, return_field):
        try:
            for value in return_field:
                self._return_fields[return_field.name].put(value)
        except:
            self._return_fields[return_field.name] = return_field

    def __iter__(self):
        for return_field in self._return_fields.values():
            yield return_field

    def __str__(self):
        readable = f"MultiEntityReturnField(name={self.name})"
        for value in self._return_fields.values():
            readable += f"\n\t{value}"
        return readable
