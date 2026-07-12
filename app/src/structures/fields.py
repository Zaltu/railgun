import bcrypt
from pathlib import Path

from config import CONFIG
from src.structures.returnfields import ReturnField, EntityReturnField, PresetReturnField, MultiEntityReturnField


class Simple_Field():
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
    def __init__(self, entity, code, name, type, uid, index, params=None, archived=False):
        """
        Field instantiator. Should really only be called by STELLAR.

        :param Entity entity: the field's parent entity
        :param str code: db column name
        :param str name: human-readable name
        :param str type: field type
        :param int id: stellardb field id
        :param bool index: if field is indexed in db
        :param dict params: RG field parameters
        :param bool archived: archival status of the field
        """
        self.entity = entity
        self.id = uid
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
    
    def middleware(self, op):
        """
        Parent function for any middleware operations that may need to be performed on
        create/update calls. Simple_Fields have no intermediate steps, but this function
        may be overwritten by field subclasses.
        """
        pass


class Entity_Field(Simple_Field):
    def _define_return_field(self):
        for ftype in self.params["constraints"]:
            # The schema may be mid-update | TODO should the temp be first in this condition?
            ftype_obj = self.entity.schema.entities.get(ftype) or self.entity.schema._temp_entities[ftype]
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

    def middleware(self, op):
        # we presume that the value being used for data is correct
        # Slap the single entity update into a list and hope for the best
        assert type(op["data"][self.code]) == dict or op["data"][self.code] == None
        empty_checker = op["data"].pop(self.code)
        return {
            "sf": self,
            "data": [empty_checker] if empty_checker else []  # [] in case set to None
        }


class MultiEntity_Field(Simple_Field):
    def _define_return_field(self):
        value_prep = {}
        for ftype in self.params["constraints"]:
            # The schema may be mid-update | TODO should the temp be first in this condition?
            ftype_obj = self.entity.schema.entities.get(ftype) or self.entity.schema._temp_entities[ftype]
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

    def middleware(self, op):
        # we presume that the value being used for data is correct
        return {
            "sf": self,
            "data": op["data"].pop(self.code) or []  # [] in case set to None
        }


class List_Field(Simple_Field):
    def middleware(self, op):
        # Validate list option exists
        assert op["data"][self.code] in self.params.get("constraints", [])


class Password_Field(Simple_Field):
    def middleware(self, op):
        # Encrypt incoming password data
        op["data"][self.code] = bcrypt.hashpw(op["data"][self.code].encode(), bcrypt.gensalt()).decode()


class Bool_Field(Simple_Field):
    def middleware(self, op):
        # Consider a boolean "null" as false. Incredibly risky in code, logically sound in data
        op["data"][self.code] = op["data"][self.code] or False


class Media_Field(Simple_Field):
    def middleware(self, op):
        # Media fields can be set to an existing local path within FILE_DIR or None to unset.
        # Otherwise new media needs to be added via /upload
        if op["data"][self.code]:
            # We do this to allow manual manipulations if absolutely needed.
            abs_path = (CONFIG.FILE_DIR / Path(op["data"][self.code])).absolute().resolve()
            assert CONFIG.FILE_DIR in abs_path.parents
            assert abs_path.exists()
        elif "entity_id" in op:  # This can only be done on update, on create there will be nothing to do
            # Set to None in order to "wipe" the field, but then we need to delete the media...
            # TODO this *really* shouldn't happen here, the op hasn't actually passed through yet.
            # BUG field names could overlap and cause there to be more than one, or an incorrect file being matched.
            # The real path needs to be fetched from DB for deletion.
            # Any kind of failure and it's joever...
            # Locate general entity path
            ent_file_dir = CONFIG.FILE_DIR / op["schema"] / op["table"] / str(op["entity_id"])
            # Get all potential files (though should be one)
            file = list(ent_file_dir.glob(self.code+"*"))
            assert len(file) <= 1  # Could already be no file
            file[0].unlink()


FIELD_TYPES = {
    "default": Simple_Field,
    "ENTITY": Entity_Field,
    "MULTIENTITY": MultiEntity_Field,
    "LIST": List_Field,
    "PASSWORD": Password_Field,
    "BOOL": Bool_Field,
    "MEDIA": Media_Field
}
