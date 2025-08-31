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
            if type(return_field) == MultiEntityReturnField:
                for entype, value in return_field._return_fields.items():
                    self._return_fields[return_field.name].put(entype, value)
            else:
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
        for entype, values in values.items():
            self.put(entype, values)

    def put(self, entype, return_fields):
        # print(f"Putting in field {entype}, {[ret.name for ret in return_fields]}")
        # print(f"{entype} in self._return_fields?: {entype in self._return_fields}")
        if entype not in self._return_fields:
            # print(f"entype {entype} not in {self.name}")
            # print(f"current {self.name}: {self._return_fields.keys()}")
            self._return_fields[entype] = _SinglTypeMultiEntityReturnField(self.table, self.name, self.join[entype], return_fields)
        else:
            for return_field in return_fields:
                self._return_fields[entype].put(return_field)

    def __iter__(self):
        for return_field in self._return_fields.values():
            yield return_field

    def __str__(self):
        readable = f"MultiEntityReturnField(name={self.name})"
        for entype in self._return_fields:
            readable += f"\n\t{entype}"
            for value in self._return_fields[entype]:
                readable += f"\n\t\t{value}"
        return readable


class _SinglTypeMultiEntityReturnField():
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
        # print(f"trying to add {return_field.name} to subtype {self.table}")
        try:
            for value in return_field:
                self._return_fields[return_field.name].put(value)
        except:
            # print(f"{return_field.name} is a static field!")
            self._return_fields[return_field.name] = return_field

    def __iter__(self):
        for return_field in self._return_fields.values():
            yield return_field

    def __str__(self):
        readable = f"SubTypeMultiEntityReturnField(name={self.name})"
        for value in self._return_fields.values():
            readable += f"\n\t{value}"
        return readable
