"""
Python object structures for STELLAR.
In terms of direct dot-path, most of the time this more or less only simplifies some string-based
embedded dicts. Subclassing `dict` itself would be a possibility, but would likely just make things
(even) less legible.

TODO permissions
"""
from src.structures.returnfields import ReturnField, EntityReturnField, PresetReturnField, MultiEntityReturnField


class STELLARWrapper(dict):
    # TODO This might be a bit too whimsical
    def telescope(self, lightweight):
        """
        Provides a returnable, parsable form of all schema objs.
        :param bool lightweight: if true, only returns the schema layer and does not parse the underlying entities.
        """
        if lightweight:
            return [schema.telescope(lightweight=True) for schema in self.values()]
        else:
            return [schema.telescope() for schema in self.values()]


class Schema():
    """
    Schema ORM object.
    STELLAR Schemas define
    - code: db name
    - id: internal id
    - name: human-readable schema name
    - host: host on which db is found
    - db_type: DB implementation
    - archived: NYI
    """
    def __init__(self, code, id, name, host, db_type, archived):
        """
        Schema instantiator. Should really only be called by STELLAR.

        :param str code: db name
        :param int id: stellardb schema ID
        :param str name: human-readable schema name
        :param str host: host hosting db
        :param str db_type: DB implementation
        :param bool archived: NYI
        """
        self.code = code
        self.id = id
        self.name = name
        self.host = host
        self.db_type = db_type
        self.archived = archived
        self.deormed = {
            "code": self.code,
            "id": self.id,
            "name": self.name,
            "db_type": self.db_type,
            "archived": self.archived,
        }
        self._temp_entities = {}
        self.entities = {}

    def finalize_entity_data(self, new_entity_data):
        """
        Update the entities attribute.
        This needs to be done specially in order to update the ReturnField attributes of the
        underlying fields. As such, it can only be called once the schema objs themselves are
        fully registered in order to prevent missing links. The schema's own `entities` attribute
        is only updated after the ReturnFields get set to prevent desync issues.

        :param list[Entity] new_entity_data: entities objs for this schema, to set or replace.
        """
        # Render the ostentibly soon-to-be new entity data available at the schema level so we can
        # configure based on the impending schema, if it differs from the current one. Basically a
        # staging attribute.
        self._temp_entities = new_entity_data

        for entity in new_entity_data.values():
            entity.finalize_field_data(entity.fields)
        # Reset staging attribute just in case
        self._temp_entities = {}
        # hotswap
        self.entities = new_entity_data

    def telescope(self, lightweight=False):
        """
        Provides a returnable, parsable form of the schema obj.
        Permissions TODO

        :param bool lightweight: if true, only returns this layer and does not parse entities. 
        """
        if lightweight:
            return self.deormed
        return self.deormed | {"entities": {entity: self.entities[entity].telescope() for entity in self.entities}}


class Entity():
    """
    Entity ORM object.
    STELLAR Entities define:
    - schema: the entity's parent schema object
    - code: db's table name
    - soloname: human readable entity name (singular)
    - multiname: human readable entity name (plural)
    - display_name_col: default col used for human identification
    - id: internal id
    - archived: if entity archived
    - permission rules: map of permission rules that apply to this entity, id:filter
    """
    def __init__(self, schema, code, soloname, multiname, display_name_col, id, archived, permissionRules=[]):
        """
        Entity instantiator. Should really only be called by STELLAR.

        :param Schema schema: the entity's parent schema
        :param str code: db table name
        :param str soloname: human-readable entity name (singular)
        :param str multiname: human-readable entity name (plural)
        :param str display_name_col: field used as default human-readable display  TODO entitize this?
        :param int id: stellardb entity ID
        :param bool archived: archival status of the entity
        :param permissionRules: map of permission rule id/filters linked per RG definition to the entity
        """
        self.schema = schema
        self.code = code
        self.soloname = soloname
        self.multiname = multiname
        self.display_name_col = display_name_col
        self.id = id
        self.archived = archived
        self.deormed = {
            "schema": self.schema.code,
            "code": self.code,
            "soloname": self.soloname,
            "multiname": self.multiname,
            "display_name_col": self.display_name_col,
            "id": self.id,
            "archived": self.archived,
        }
        self.fields = {}
        self.rules = {perm["uid"]: perm["filter"] for perm in permissionRules}

    def finalize_field_data(self, new_field_data):
        """
        Update the entities attribute.
        This needs to be done specially in order to update the ReturnField attributes of the
        underlying fields. As such, it can only be called once the schema objs themselves are
        fully registered in order to prevent missing links. The entity's own `fields` attribute
        is only updated after the ReturnFields get set to prevent desync issues.

        Unlike the schema, the entity attributes used during finalization are completely immutable
        in practice. As such, we don't need a staging attribute.

        :param list[Field] new_field_data: New fields for this entity
        """
        for field in new_field_data.values():
            field._define_return_field()
        self.fields = new_field_data

    def parse_permissions(self, permission_rule_ids):
        """
        Build and return a filter set containing rules to be applied on this entity based on the
        provided set of permission rules.

        To note, we choose to lose time iterating over the entity's rules for a match rather
        than the user's since I think there's more likely to be fewer entity rules than user
        rules in large enough data sets for it to matter.

        :param set permission_rule_ids: permission rules to used as delimiters

        :returns: permission-enforcing filter
        :rtype: dict
        """
        permFilters = []
        allowed = False
        print(self.rules.keys())
        for ruleid in self.rules.keys():  # for each permission rule on this entity
            if ruleid in permission_rule_ids:  # if the rule is assigned to the user
                allowed = True
                if self.rules[ruleid]:
                    permFilters.append(self.rules[ruleid])  # add it to the list of filters to apply
        return allowed and permFilters

    def telescope(self, lightweight=False):
        """
        Provides a returnable, parsable form of the entity obj.
        Permissions TODO

        :param bool lightweight: if true, only returns this layer and does not parse fields.
        """
        if lightweight:
            return self.deormed
        return self.deormed | {"fields": {field: self.fields[field].telescope() for field in self.fields}}



class Field():
    """
    Field ORM object.
    STELLAR Entities define:
    - entity: the field's parent entity object
    - code: db column name
    - name: human-readable db column name
    - type: Railgun field type
    - id: internal id
    - index: if field has index
    - params: Railgun field parameters
    - archived: if field archived
    """
    def __init__(self, entity, code, name, type, id, index, params=None, archived=False):
        """
        Entity instantiator. Should really only be called by STELLAR.

        :param Entity entity: the field's parent entity
        :param str code: db column name
        :param str name: human-readable name
        :param str type: field type
        :param int id: stellardb field id
        :param bool index: if field is indexed in db
        :param bool archived: archival status of the field
        """
        self.entity = entity
        self.id = id
        self.code = code
        self.name = name
        self.type = type
        self.index = index
        self.params = params
        self.archived = archived
        self.deormed = {  # Somewhat excessive optimization, but matches other layouts
            "entity": self.entity.soloname,
            "code": self.code,
            "name": self.name,
            "type": self.type,
            "id": self.id,
            "index": self.index,
            "params": self.params,
            "archived": self.archived,
        }
        self.return_field = None

    def _define_return_field(self):
        """
        Define the self.return_field property of this Field Obj.
        This needs to be done after the instantiation of the full schema tree to ensure any linked
        fields on linked entities are defined and ready.
        """
        if self.type == "ENTITY":
            for ftype in self.params["constraints"]:
                ftype_obj = self.entity.schema.entities.get(ftype) or self.entity.schema._temp_entities[ftype]  # The schema may be mid-update
                oneiterrf = EntityReturnField(
                    name=self.code,
                    join={"constraints": self.params["constraints"].values(), "local_table": self.entity.code},
                    values=[
                        PresetReturnField(name="type", value=ftype),
                        ReturnField(table=ftype_obj.code, name="uid"),
                        ReturnField(table=ftype_obj.code, name=ftype_obj.display_name_col)
                    ]
                )
                # setdefault shenanigans
                try:
                    self.return_field.put(oneiterrf)
                except AttributeError:
                    self.return_field = oneiterrf
        elif self.type == "MULTIENTITY":
            value_prep = {}
            for ftype in self.params["constraints"]:
                ftype_obj = self.entity.schema.entities.get(ftype) or self.entity.schema._temp_entities[ftype]  # The schema may be mid-update
                value_prep[ftype] = [
                    PresetReturnField(name="type", value=ftype),
                    ReturnField(table=ftype_obj.code, name="uid"),
                    ReturnField(table=ftype_obj.code, name=ftype_obj.display_name_col)
                ]
            self.return_field = MultiEntityReturnField(
                table=self.entity.code,
                name=self.code,
                join=self.params["constraints"],
                values=value_prep
            )
            # BUG only one type is returned even for multi-entity multi-type right now. Fix will come with objectified fields
            # Is that true?
        else:
            self.return_field = ReturnField(
                self.entity.code,   # table name
                self.code           # field code
            )

    def telescope(self, lightweight=False):
        """
        Provides a returnable, parsable form of the field obj.
        Permissions TODO

        :param bool lightweight: Not used. Exposed in order to maintain a certain level of
                                 intercompatibility with all telescope operations.
        """
        return self.deormed

