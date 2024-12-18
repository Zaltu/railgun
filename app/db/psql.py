"""
GUD Database implementation for PostgreSQL.
"""
# Parent DB class
from db._database import Database

from src.structures.returnfields import ReturnFieldSet, PresetReturnField, ReturnField, EntityReturnField, MultiEntityReturnField

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


###############################
#### PSQL FILTER FUNCTIONS ####
###############################
# Filtering agains NULL/empty values needs special logic pepehands
def EQUALS(table, field, value):
    if value is not None:
        return sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" = ") + sql.Literal(value)
    else:
        return sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" IS ") + sql.Literal(value)
def NOT_EQUALS(table, field, value):
    if value is not None:
        return sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" != ") + sql.Literal(value)
    else:
        return sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" IS NOT ") + sql.Literal(value)


##########################
###### PSQL DB MAIN ######
##########################
class PSQL(Database):
    """
    GUD-compliant PSQL connector.
    """
    DB_TYPE = "PSQL"
    LOI = "FULL"
    FIELD_TYPES = {
        "TEXT": "TEXT",
        "PASSWORD": "TEXT",
        "MEDIA": "TEXT",
        "INT": "BIGINT",
        "FLOAT": "DOUBLE PRECISION",
        "BOOL": "BOOLEAN",
        "JSON": "JSONB",
        "DATE": "DATE"
    }
    FILTER_OPTIONS = {
        "is": EQUALS,
        "is_not": NOT_EQUALS,
        "contains": lambda table, field, value: sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" ILIKE ") + sql.Literal("%%"+value+"%%"),
        "not_contains": lambda table, field, value: sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" NOT ILIKE ") + sql.Literal("%%"+value+"%%"),
        "starts_with": lambda table, field, value: sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" ILIKE ") + sql.Literal(value+"%%"),
        "ends_with": lambda table, field, value: sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" ILIKE ") + sql.Literal("%%"+value),
        #"in": "", TODO
        "greater_than": lambda table, field, value: sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" > ") + sql.Literal(value),
        "less_than": lambda table, field, value: sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" < ") + sql.Literal(value)
    }
    def __init__(self, config_params):
        # Prep config
        self.connection_params = self._load_config(config_params)
        self.connect()

        # This is the floating connection without autocommit.
        # TODO change to async pools for more versatile scaling.
        self.stage = lambda: psycopg.connect(**self.connection_params, row_factory=dict_row)

        super().__init__()  # Blank


    def _load_config(self, config_params):
        """
        Load a (JSON) config file for DB connection info.
        """
        return {
            "dbname": config_params["DB_NAME"],
            "user": config_params["DB_USER"],
            "password": config_params.get("DB_PASSWORD"),
            "host": config_params.get("DB_HOST"),
            "port": config_params.get("DB_PORT"),
        }


    #####################################
    ###########  Connection  ############
    #####################################
    def connect(self):
        self.database = psycopg.connect(
            **self.connection_params,
            autocommit=True,
            row_factory=dict_row
        )
        self.version = self._run_command("SELECT version()")[0]#[0]  # lol
        if not self.version:
            raise ConnectionError


    def disconnect(self):
        self.database.close()

    
    def _run_command(self, command, params=None, include_descriptors=False, return_style="multi"):
        """
        Execute a (dirty) command.
        TODO fully migrate to prepared statements.
        """
        try:
            with self.database.cursor() as cur:
                cur.execute(command, params)
                match return_style:
                    case "multi":
                        values = cur.fetchall()
                        if include_descriptors:
                            fieldcodes = [desc[0] for desc in cur.description]
                    case "solo":
                        values = cur.fetchone()
                    case _:
                        # Operation not expected to produce anything.
                        return
        except Exception as e:
            raise

        return (values, fieldcodes) if include_descriptors else values


    #####################################
    ###### Practical Accessability ######
    #####################################
    ### SCHEMA ###
    def fetch_table_names(self):
        """
        Fetch all table names. No transformation.
        TODO fetch from Stellar
        """
        COMMAND = """SELECT table_name FROM information_schema.tables WHERE table_schema='public'"""
        return [table_name[0] for table_name in self._run_command(COMMAND)]


    def fetch_table_columns(self, table):
        """
        Fetch all columns of an existing table.
        TODO fetch from Stellar
        """
        COMMAND = sql.SQL(
            """SELECT column_name, data_type FROM information_schema.columns WHERE table_name=(%s)"""
        )
        # COMMAND = sql.SQL(
        #     """SELECT * FROM {} LIMIT 0"""
        # ).format(sql.Identifier(table))
        field_codes = self._run_command(COMMAND, (table,))
        return field_codes


    def create_table(self, table_name):
        """
        Create a new DB table, including default fields.
        Validation is assumed to be done by StellarStellar.

        :param str table_name: the name of the actual table to create (entity code)
        """
        COMMAND = sql.SQL("""
        CREATE TABLE {table_name} (
            uid INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            code TEXT NOT NULL,
            _ss_archived BOOLEAN NOT NULL DEFAULT false
        )
        """).format(table_name=sql.Identifier(table_name))
        return self._run_command(COMMAND, return_style=None)


    def update_table(self, table_name):
        """
        Nothing to do.
        """
        raise NotImplementedError


    def delete_table(self, table_name):
        """
        Remove a DB table.
        Validation and archival management is assumed to be done by StellarStellar.

        :param str table_name: the name of the actual table to drop (entity code)
        """
        COMMAND = sql.SQL("""
            DROP TABLE {table} CASCADE;
        """).format(
            table=sql.Identifier(table_name)
        )
        return self._run_command(COMMAND, return_style=None)


    def create_field(self, table_name, field_name, field_type, nullable=True, default=None):
        """
        Create a column in the physical DB.
        Validation is assumed to be done by Stellar Stellar.

        :param str table_name: table to create column in
        :param str field_name: name of column to create
        :param str field_type: type of field to create
        :param str default: optional default value of field

        :raises: NotImplementedError if field_type is not recognized by the connector
        :returns: true to validation creation
        :rtype: bool
        """
        if field_type not in PSQL.FIELD_TYPES:
            raise NotImplementedError
        COMMAND = sql.SQL("""
            ALTER TABLE {table}
            ADD {field} {ftype}
        """).format(
            table=sql.Identifier(table_name),
            field=sql.Identifier(field_name),
            ftype=sql.SQL(PSQL.FIELD_TYPES[field_type])
        )
        if not nullable:
            COMMAND += sql.SQL(" NOT NULL")
        if default:
            COMMAND += sql.SQL(" DEFAULT {default}").format(
                default=default
            )
        self._run_command(COMMAND, return_style=None)
        return True


    def delete_field(self, table_name, field_name):
        """
        Drop a column from a table.
        Validation and archival management is assumed to be done by Stellar Stellar.

        :param str table_name: table of the column
        :param str field_name: name of column to delete

        :returns: true to validate deletion
        :rtype: bool
        """
        COMMAND = sql.SQL("""
            ALTER TABLE {table}
            DROP COLUMN {field}
        """).format(
            table=sql.Identifier(table_name),
            field=sql.Identifier(field_name)
        )
        self._run_command(COMMAND, return_style=None)
        return True


    def create_enum(self, enum_name, enum_options):
        """
        Create an enum type. Railgun never uses these on it's own, but uses them for data
        validation in List fields.

        :param str enum_name: name of the type/enum, must be unique
        :param list[str] enum_options: options that should be available

        :returns: true to validate creation
        :rtype: bool
        """
        COMMAND = sql.SQL("""
            CREATE TYPE {enum_name} AS ENUM ({enum_options})
        """).format(
            enum_name=sql.Identifier(enum_name),
            enum_options=sql.SQL(", ").join(enum_options)
        )
        self._run_command(COMMAND, return_style=None)
        # ULTRA MEGA HACK
        PSQL.FIELD_TYPES[enum_name] = enum_name
        return True


    def update_enum(self, enum_name, new_enum_options):
        """
        """
        raise NotImplementedError


    def delete_enum(self, enum_name):
        """
        Drop the enum type from the DB.
        It's presumed that validation is done elsewhere.

        :param str enum_name: Enum type to drop

        :returns: true to validate deletion
        :rtype: bool
        """
        COMMAND = sql.SQL("""
            DROP TYPE {enum_name}
        """).format(
            enum_name=sql.Identifier(enum_name)
        )
        self._run_command(COMMAND, return_style=None)
        return True


    ### DATA ###
    def query(self, table, fields, filters=[], pagination=0, page=1, order="uid", conn=None):
        """
        Run an optimized (TODO lol) guery.
        """
        baseGroup = sql.SQL("")  # TODO

        COMMAND = sql.SQL("""
            SELECT {select}
            {filters}
            {group}
            ORDER BY {table}.{order}
            LIMIT (%s)
            OFFSET (%s)
        """).format(
            select=_build_select_chunk(fields),
            table=sql.Identifier(table),
            filters=_build_filters(filters, table),
            order=sql.Identifier(order),
            group=baseGroup
        )
        print(COMMAND.as_string(self.database))  # TODO log
        if conn:
            return conn.execute(COMMAND,  (pagination, (page*pagination)-pagination)).fetchall()
        else:
            return self._run_command(COMMAND, (pagination, (page*pagination)-pagination))


    def count(self, table, filters):
        """
        Run an optimized (TODO lol) guery.
        """
        # Prep all JOINs
        # TODO, since filters on joined tables would have an effect, but that's not in place right now.
        baseRTJoin = sql.SQL("")
        # baseRTJoin = _build_joins(joins, table)
        
        # Groups are needed in case grouping causes duplication, which increases count...
        baseGroup = sql.SQL("")
        # if joins:
        #     baseGroup += sql.SQL("GROUP BY {table}.{uid}").format(
        #         table=sql.Identifier(table),
        #         uid=sql.Identifier("uid")
        #     )

        # Prep all filters
        baseFilter = _build_filters(filters, table)

        COMMAND = sql.SQL("""
            SELECT count(*) as total_count
            FROM {table}
            {joins}
            {filters}
            {group}
        """).format(
            table=sql.Identifier(table),
            joins=baseRTJoin,
            filters=baseFilter,
            group=baseGroup
        )
        print(COMMAND.as_string(self.database))  # TODO log
        return self._run_command(COMMAND, return_style="solo")


    def create(self, op, conn):
        """
        Create a record of a certain type, using column values found in the requested operation.
        Relations are handled by Railgun.

        :param dict op: insert operation to perform
        :param sql.Connection conn: active psycopg connection

        :returns: type-uid dict of the created record
        :rtype: dict
        """
        params = ", ".join("(%s)" for _ in op['data'].values())
        COMMAND = sql.SQL("INSERT INTO {table} ({fields}) VALUES ("+params+") RETURNING {nicetype} as type, uid").format(
            table=sql.Identifier(op["table"]),
            fields=sql.SQL(", ").join([sql.Identifier(field) for field in op["data"].keys()]),
            nicetype=sql.Literal(op["entity"])
        )
        print(COMMAND.as_string(conn))
        return conn.execute(COMMAND, tuple(op["data"].values())).fetchone()


    def update(self, op, conn):
        """
        Update a record of a certain type, using column values found in the requested operation.
        Relations are handled by Railgun.

        :param dict op: update operation to perform
        :param sql.Connection conn: active psycopg connection

        :returns: type-uid dict of the updated record
        :rtype: dict
        """
        COMMAND = sql.SQL("""
            UPDATE {table}
            SET {vupdate}
            WHERE uid = {uid}
            RETURNING {nicetype} as type, uid
        """).format(
            table=sql.Identifier(op["table"]),
            vupdate=sql.SQL(", ").join([sql.SQL("{key} = {value}").format(key=sql.Identifier(key), value=sql.Literal(value)) for key, value in op["data"].items()]),
            uid=sql.Literal(op["entity_id"]),
            nicetype=sql.Literal(op["entity"])
        )
        print(COMMAND.as_string(conn))
        return conn.execute(COMMAND).fetchone()


    def delete(self, op, conn):
        """
        Delete a record. It's assumed that archival management is done elsewhere.
        Boom goes the dynamite.

        :param dict op: delete operation to perform
        :param sql.Connection conn: active psycopg connection

        :returns: the entity dict of the deleted record, even if it's gone forever ;_;
        :rtype: dict
        """
        COMMAND = sql.SQL(
            "DELETE FROM {table} WHERE uid={uid}"
        ).format(
            table=sql.Identifier(op["table"]),
            uid=sql.Literal(op["entity_id"])
        )
        print(COMMAND.as_string(conn))
        conn.execute(COMMAND)
        return {"type": op["entity"], "uid": op["entity_id"]}


    def delete_relation(self, rtable, tableA, s_col, s_uid, conn):
        """
        Removes all relations between table A and table B for a specific column and foreign key.
        Managing diffs would be a massive pain compared to how simple it is to just wipe and reset
        every time, from a DB standpoint.

        DELETE FROM _ss_table_table WHERE table_col = {col} AND fk_table = {uid};

        :param str rtable: relation table between tableA and tableB
        :param str tableA: the table of the foreign key to be removed.
        :param str s_col: the field code that is getting cleared
        :param str s_uid: the foreign key to be removed
        :param psycopg.Connection conn: DB connection
        """
        COMMAND = sql.SQL("""
            DELETE FROM {rtable} WHERE {tableA_col} = {s_col} AND {fk_tableA} = {s_uid}
        """).format(
            rtable=sql.Identifier(rtable),
            tableA_col=sql.Identifier(tableA+"_col"),
            s_col=sql.Literal(s_col),
            fk_tableA=sql.Identifier("fk_"+tableA),
            s_uid=sql.Literal(s_uid)
        )
        print(COMMAND.as_string(conn))
        conn.execute(COMMAND)


    def create_relation(self, rtable, tableA, tableB, values, conn):
        """
        Create a relation between two records on a specific column.
        All relations are standardized by Stellar Stellar, thankfully.

        INSERT INTO _ss_table_table (table_col, fk_table, fk_table, table_col)

        :param str rtable: relation table to insert in to
        :param str tableA: the table referring to the entity we are modifying
        :param str tableB: the table referring to the entity we are linking to
        :param tuple values: the values to insert - (tableA_col, fk_tableA, fk_tableB, tableB_col)
        """
        COMMAND = sql.SQL("""
            INSERT INTO {rtable} ({tableA_col}, {fk_tableA}, {fk_tableB}, {tableB_col}) VALUES ((%s), (%s), (%s), (%s))
        """).format(
            rtable=sql.Identifier(rtable),
            tableA_col=sql.Identifier(tableA+"_col"),
            fk_tableA=sql.Identifier("fk_"+tableA),
            fk_tableB=sql.Identifier("fk_"+tableB),
            tableB_col=sql.Identifier(tableB+"_col")
        )
        print(COMMAND.as_string(conn))
        conn.execute(COMMAND, values)




#####################################
########  Command   Helpers  ########
#####################################
def _build_filters(filters, table):
    """
    Build the WHERE statement of a query.
    See Railgun docs for filter format.

    :param dict filters: Railgun filters
    :param str table: current table

    :returns: WHERE statement
    :rtype: psycopg.sql.SQL
    """
    return sql.SQL("WHERE ") + _rec_filter_con(sql.SQL(""), filters, table) if filters else sql.SQL("")


def _build_select_chunk(return_fields):
    """
    Sets up the SQL "SELECT" syntax defining what values should be fetched from the table or joined
    foreign tables. This includes any JOINs required in order to SELECT the data.

    :param ReturnFieldSet return_fields: return fields for this query

    :returns: SELECT segment of SQL query
    :rtype: psycopg.sql.SQL
    """
    base_select_chunk = sql.SQL("{return_fields} FROM {table} {entity_joins} {multientity_joins}")
    rts, ejoins, mjoins = _build_return_fields(return_fields)

    return base_select_chunk.format(
        return_fields=sql.SQL(",").join(rts),
        table=sql.Identifier(return_fields.table),
        entity_joins=_build_entity_joins(ejoins),
        multientity_joins=_build_multientity_joins(mjoins)
    )


def _build_return_fields(return_fields):
    """
    Builds the top-level return fields for a query. Split into subfunction for readability.
    By doing so, also populates the entity and multi-entity join dicts later used by the
    SELECT builder to populate JOINs.

    :param ReturnFieldSet return_fields: return fields for this query

    :returns: list of top-level return fields, entity join dict and multi-entity join dict
    :rtype: list, dict, dict
    """
    rts = []
    ejoins = {}
    mjoins = {}
    for field in return_fields:
        if type(field)==PresetReturnField:
            rts.append(
                sql.SQL("{value} as {display}").format(
                    value=sql.Literal(field.value),
                    display=sql.Identifier(field.name)
                )
            )
        elif type(field)==ReturnField:
            # No jsonification to do. It's either a local field or aggregated by join
            rts.append(sql.SQL("{table}.{field}").format(
                    table=sql.Identifier(field.table),
                    field=sql.Identifier(field.name)
                )
            )
        elif type(field)==EntityReturnField:
            # Entity fields are built at the parent level, from normal left-joins
            # JOIN THIS LEVEL BEFORE THE NEXT
            ejoins[field.name] = field.join
            rts.append(_embed_json_build(field, ejoins)+sql.SQL(" AS {display_name}").format(
                display_name=sql.Identifier(field.name)
            ))
        elif type(field)==MultiEntityReturnField:
            mjoins[field.name] = field
            # Append the top-level return reference
            # TODO json_agg should be done here, and formatted to accept multi-types
            rts.append(sql.SQL("{table}.{field}").format(
                    table=sql.Identifier(field.join["relation"]),
                    field=sql.Identifier(field.name)
                )
            )
        else:
            raise NotImplementedError("Unknown ReturnField type %s" % field)
    return rts, ejoins, mjoins


def _build_entity_joins(joins):
    """
    Sets up the SQL "JOIN" syntax required for any linked entity field queries.
    Needs bits and pieces of STELLAR schema data for entity fields. The "joins" parameter
    as such expects the following format:
    {
        field: [
            {
                local_table: <source_table>,
                constraints: {"relation": <relation_table>,
                "table": <foreign_table>
                "col": <foreign_column>}
            }
        ]
    }

    :param dict joins: entity join dict

    :returns: entity JOIN segment of SQL query
    :rtype: psycopg.sql.SQL
    """
    baseRTJoin = sql.SQL("")
    for field, join in joins.items():
        for ftable in join["constraints"]:
            baseRTJoin += sql.SQL("\nLEFT JOIN {relation} ON {relation}.{fk_table} = {table}.{uid} AND {relation}.{table_col} = {field}").format(
                relation=sql.Identifier(ftable["relation"]),
                fk_table=sql.Identifier("fk_{table}".format(table=join["local_table"])),
                table=sql.Identifier(join["local_table"]),
                uid=sql.Identifier("uid"),
                table_col=sql.Identifier("{table}_col".format(table=join["local_table"])),
                field=sql.Literal(field)
            )
            baseRTJoin += sql.SQL("\nLEFT JOIN {ftable} ON {relation}.{fk_ftable} = {ftable}.{uid}").format(
                ftable=sql.Identifier(ftable["table"]),
                relation=sql.Identifier(ftable["relation"]),
                fk_ftable=sql.Identifier("fk_{ftable}".format(ftable=ftable["table"])),
                uid=sql.Identifier("uid"),
                ftable_col=sql.Identifier("{ftable}_col".format(ftable=ftable["table"])),
                ffield=sql.Literal(ftable["col"])
            )
    return baseRTJoin


def _build_multientity_joins(joins):
    """
    Sets up the SQL "JOIN" syntax required for any linked multi-entity field queries.
    Needs bits and pieces of STELLAR schema data for entity fields. The "joins" parameter
    as such expects the following format:
    {
        "field": MultiEntityReturnField
    }

    :param dict joins: multi-entity join dict

    :returns: multi-entity SQL join section
    :rtype: psycopg.sql.SQL
    """
    baseRTJoin = sql.SQL("")
    for field, returnfield in joins.items():
        ejoins = {}
        baseRTJoin += sql.SQL("""
            LEFT JOIN (
                SELECT {relation}.{fk_table}, json_agg({embedded_object}) AS {field}
                FROM {relation}
                LEFT JOIN {ftable} ON {relation}.{fk_ftable} = {ftable}.uid
                {ejoins}
                GROUP BY {relation}.{fk_table}
            ) {relation} ON {relation}.{fk_table} = {table}.uid
        """).format(
            relation=sql.Identifier(returnfield.join["relation"]),
            fk_table=sql.Identifier("fk_"+returnfield.table),
            ftable=sql.Identifier(returnfield.join["table"]),
            field=sql.Identifier(field),
            fk_ftable=sql.Identifier("fk_"+returnfield.join["table"]),
            table=sql.Identifier(returnfield.table),
            embedded_object=_embed_json_build(returnfield, ejoins),
            ejoins=_build_entity_joins(ejoins)
        )
    return baseRTJoin


def _rec_filter_con(straight, filter, table):
    """
    Recursive function to decompose the Railgun filter syntax into PSQL WHERE statements.
    See Railgun docs for expected filter format.

    :param str straight: previous deconstructed filter
    :param dict|list filter: this section's filter config
    :param str table: this table's code

    :returns: this section and below's deconstructed, PSQL-compliant WHERE statement
    :rtype: psycopg.sql.SQL
    """
    simpletons = []
    for subfilter in filter["filters"]:
        if isinstance(subfilter, dict):
            simpletons.append(_rec_filter_con(straight, subfilter, table))
        else:
            simpletons.append(
                ### We assume receiving an element of format [<field>, <filter_operation>, <value>]
                # HACK subfilter[4] never exists. Filters currently only work on the top level table, not joined tables
                PSQL.FILTER_OPTIONS[subfilter[1]](table if len(subfilter)==3 else subfilter[4], subfilter[0], subfilter[2])
            )
    straight += sql.SQL(" "+filter["filter_operator"]+" ").join(simpletons)
    return straight


def _embed_json_build(return_fields, joins):
    """
    Build the PSQL json_build_object chunk that's used to forward pre-formatted
    entity fields to Railgun.

    :param EntityReturnField return_fields: EntityReturnField object to build
    :param dict joins: shared JOIN dict for the active SELECT block

    :returns: entity return field SQL chunk
    :rtype: psycopg.sql.SQL
    """
    baseBuild = sql.SQL("json_build_object({field_builder})")
    field_builder = []
    for field in return_fields:
        if type(field)==PresetReturnField:
            field_builder.append(sql.Literal(field.name))
            field_builder.append(sql.Literal(field.value))
        elif type(field)==ReturnField:
            field_builder.append(sql.Literal(field.name))
            field_builder.append(
                sql.Identifier(field.table)+sql.SQL(".")+sql.Identifier(field.name)
            )
        elif type(field)==EntityReturnField:
            # JOIN THIS LEVEL BEFORE THE NEXT
            joins[field.name] = field.join
            field_builder.append(sql.Literal(field.name))
            field_builder.append(_embed_json_build(field, joins))
        elif type(field)==MultiEntityReturnField:
            # TODO
            field_builder.append(sql.Literal(field.name))
            field_builder.append(_embed_json_build(field))
    return baseBuild.format(field_builder=sql.SQL(", ").join(field_builder))
