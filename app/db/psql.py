"""
GUD Database implementation for PostgreSQL.
"""
# Parent DB class
from db._database import Database

from src.structures.returnfields import PresetReturnField, ReturnField, EntityReturnField, MultiEntityReturnField

from psycopg import sql, AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import set_json_dumps, set_json_loads
import orjson

from lib.ragesync import execute_immediately
from psycopg_pool import AsyncConnectionPool


###############################
#### PSQL FILTER FUNCTIONS ####
###############################
# Filtering agains NULL/empty values needs special logic pepehands
def EQUALS(table, field, value):
    if value is not None:
        if type(value)==str:
            value = value.replace("%", "%%")
        return sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" = ") + sql.Literal(value)
    else:
        return sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" IS ") + sql.Literal(value)
def NOT_EQUALS(table, field, value):
    if value is not None:
        if type(value)==str:
            value = value.replace("%", "%%")
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
    DEFAULT_WAIT_TIMEOUT = 2
    DEFAULT_IDLE_TIMEOUT = 600
    DEFAULT_MIN_POOL_SIZE = 4
    DEFAULT_MAX_POOL_SIZE = 20
    DEFAULT_MAX_QUEUE_SIZE = 0  # No limit
    DEFAULT_MAX_LIFETIME = 3600
    GENERAL_CONNECTION_KWARGS = {"autocommit": False, "row_factory": dict_row}

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
        #"in": "", TODO, should be table.field = ANY([list])
        "greater_than": lambda table, field, value: sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" > ") + sql.Literal(value),
        "less_than": lambda table, field, value: sql.Identifier(table)+sql.SQL(".")+sql.Identifier(field) + sql.SQL(" < ") + sql.Literal(value)
    }
    def __init__(self, config_params):
        super().__init__()  # Blank
        # IMPORTANT: These settings are technically global
        set_json_loads(orjson.loads)
        set_json_dumps(orjson.dumps)

        # Prep config
        _connection_info = {
            "dbname": config_params["DB_NAME"],
            "user": config_params["DB_USER"],
            "password": config_params.get("DB_PASSWORD"),
            "host": config_params.get("DB_HOST"),
            "port": config_params.get("DB_PORT"),
        }

        # Format our config params for the string that ConnectionPools expect
        strconfig = "".join([f"{key}={value} " for key, value in _connection_info.items() if value])
        # This is the floating connection without autocommit.
        self.pool = AsyncConnectionPool(
            strconfig,
            connection_class=_PSQLConnection,
            kwargs=PSQL.GENERAL_CONNECTION_KWARGS,
            min_size=config_params.get("min_pool_size", PSQL.DEFAULT_MIN_POOL_SIZE),
            max_size=config_params.get("max_pool_size", PSQL.DEFAULT_MAX_POOL_SIZE),
            open=False,  # per psycopg documentation
            timeout=config_params.get("queue_timeout", PSQL.DEFAULT_WAIT_TIMEOUT),
            max_waiting=config_params.get("max_queue_size", PSQL.DEFAULT_MAX_QUEUE_SIZE),
            max_lifetime=config_params.get("keep_alive_for", PSQL.DEFAULT_MAX_LIFETIME),
            max_idle=config_params.get("keep_idle_for", PSQL.DEFAULT_IDLE_TIMEOUT)
        )
        execute_immediately(self.pool.open(wait=True))
        self.stage = self.pool.connection  # syntaxical sugar


    #####################################
    ###########  Connection  ############
    #####################################
    async def _run_command(self, command, params=None, return_style="multi"):
        """
        Execute a (dirty) command.
        TODO fully migrate to prepared statements.
        """
        try:
            async with self.stage() as conn:
                cur = await conn.execute(command, params)
                match return_style:
                    case "multi":
                        values = await cur.fetchall()
                    case "solo":
                        values = await cur.fetchone()
                    case _:
                        # Operation not expected to produce anything.
                        return
        except Exception as e:
            raise

        return values


    #####################################
    ###### Practical Accessability ######
    #####################################
class _PSQLConnection(AsyncConnection):
    """
    The connection object is wrapped to allow functionality calls directly to the connection object that's dragged everywhere once staged.
    TODO not sure how to abstractify this prettily, but I'm fairly certain it's practically generalizable
    """
    ### SCHEMA ###
    async def create_table(self, table_name):
        """
        Create a new DB table, including default fields.
        Validation is assumed to be done by StellarStellar.

        :param str table_name: the name of the actual table to create (entity code)
        :param AsyncConnection conn: open DB connection
        """
        COMMAND = sql.SQL("""
        CREATE TABLE {table_name} (
            uid INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            code TEXT NOT NULL,
            _ss_archived BOOLEAN NOT NULL DEFAULT false
        )
        """).format(table_name=sql.Identifier(table_name))
        return await self.execute(COMMAND)


    async def update_table(self, table_name):
        """
        Nothing to do.
        """
        raise NotImplementedError


    async def delete_table(self, table_name):
        """
        Remove a DB table.
        Validation and archival management is assumed to be done by StellarStellar.

        :param str table_name: the name of the actual table to drop (entity code)
        :param AsyncConnection conn: open DB connection
        """
        COMMAND = sql.SQL("""
            DROP TABLE {table} CASCADE;
        """).format(table=sql.Identifier(table_name))
        return await self.execute(COMMAND)


    async def create_field(self, table_name, field_name, field_type, nullable=True, default=None):
        """
        Create a column in the physical DB.
        Validation is assumed to be done by Stellar Stellar.

        :param str table_name: table to create column in
        :param str field_name: name of column to create
        :param str field_type: type of field to create
        :param AsyncConnection conn: open DB connection
        :param bool nullable: switch to determine if the field can be null, default True
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
        await self.execute(COMMAND)
        return True


    async def delete_field(self, table_name, field_name):
        """
        Drop a column from a table.
        Validation and archival management is assumed to be done by Stellar Stellar.

        :param str table_name: table of the column
        :param str field_name: name of column to delete
        :param AsyncConnection conn: open DB connection

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
        await self.execute(COMMAND)
        return True


    ### DATA ###
    async def query(self, table, fields, filters=[], pagination=0, page=1, order="uid"):
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
        print(COMMAND.as_string(self))  # TODO log
        return await (await self.execute(COMMAND,  (pagination, (page*pagination)-pagination))).fetchall()


    async def count(self, table, filters):
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
        print(COMMAND.as_string(self))  # TODO log
        return await (await self.execute(COMMAND)).fetchone()


    async def create(self, op):
        """
        Create a record of a certain type, using column values found in the requested operation.
        Relations are handled by Railgun.

        :param dict op: insert operation to perform
        :param sql.Connection conn: active psycopg connection

        :returns: type-uid dict of the created record
        :rtype: dict
        """
        # TODO secure this, params is not safe
        params = ", ".join("(%s)" for _ in op['data'].values())
        COMMAND = sql.SQL("INSERT INTO {table} ({fields}) VALUES ("+params+") RETURNING {nicetype} as type, uid").format(
            table=sql.Identifier(op["table"]),
            fields=sql.SQL(", ").join([sql.Identifier(field) for field in op["data"].keys()]),
            nicetype=sql.Literal(op["entity"])
        )
        print(COMMAND.as_string(self))
        return await (await self.execute(COMMAND, tuple(op["data"].values()))).fetchone()


    async def update(self, op):
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
        print(COMMAND.as_string(self))
        return await (await self.execute(COMMAND)).fetchone()


    async def delete(self, op):
        """
        Delete a record. It's assumed that archival management is done elsewhere.
        Boom goes the dynamite.

        :param dict op: delete operation to perform
        :param sql.Connection conn: active psycopg connection

        :returns: the entity dict of the deleted record (without the display_col_name), even if it's gone forever ;_;
        :rtype: dict
        """
        COMMAND = sql.SQL(
            "DELETE FROM {table} WHERE uid={uid}"
        ).format(
            table=sql.Identifier(op["table"]),
            uid=sql.Literal(op["entity_id"])
        )
        print(COMMAND.as_string(self))
        await self.execute(COMMAND)
        return {"type": op["entity"], "uid": op["entity_id"]}


    async def delete_relation(self, rtable, tableA, s_col, s_uid):
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
        print(COMMAND.as_string(self))
        await self.execute(COMMAND)


    async def create_relation(self, rtable, tableA, tableB, values):
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
        print(COMMAND.as_string(self))
        await self.execute(COMMAND, values)




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
    print(return_fields)
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
            for subentfield in field:
                mjoins[field.name+"_"+subentfield.join["relation"]] = subentfield
            # Append the top-level return reference
            # TODO json_agg should be done here, and formatted to accept multi-types
            multitypesubsql = []
            for ftype in field._return_fields:
                multitypesubsql.append(sql.SQL("{table}.{field}::jsonb").format(
                        table=sql.Identifier(field.join[ftype]["relation"]),
                        field=sql.Identifier(field.name+"_"+field._return_fields[ftype].join["relation"])
                    )
                )
            rts.append(
                sql.SQL("COALESCE({mergedsublists}, {sublists}) AS {field}").format(
                    mergedsublists=sql.SQL(" || ").join(multitypesubsql),
                    sublists=sql.SQL(",").join(multitypesubsql),
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
