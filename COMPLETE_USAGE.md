# Complete Usage Guide
This document goes over the endpoints exposed by Railgun. How to call them and any particular implementation notes.

## String Standard
In general, the following arbitrary standard should be followed:
- **schema names**: if a schema is required, provide the schema's `code`
- **entity names**: if an entity is required, provide the entity's `soloname`
- **field names**: if a field is required, provide the field's `code`

And don't ask why, it just works.

## /heartbeat
Returns True if the server is alive. (Only) unauthenticated endpoint.

## /login
Endpoint used to initially authenticate a user and provide an access token for further use.  
Railgun authentication follows OAuth2 Password Bearer standard. The provided HTTP request should be of type `x-www-form-urlencoded` with a body of `grant_type=password&username={username}&password={password}`.

Railgun's response will include both a authorization token for use in future request headers, as well as an HTTP-only cookie inluding this token. This is done in order to allow web-based tools to interact with statically served files without the need for middleware, while keeping pure API access simple and standardized. Only one method of authentication is required for future calls.

Python example (requests):
```python
requests.post(
    "https://railgun.aigis.dev/login",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data=f"grant_type=password&username={username}&password={password}"
)
# Response.json()
{
    "access_token": "<token>",
    "token_type": "bearer"
}
```
JS Example (react):
```js
fetch("https://railgun.aigis.dev/login", {
    mode:"cors",
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    method: "POST",
    body: `grant_type=password&username=${username}&password=${password}`,
    credentials: "include"
})
// Cookie is stored to browser automatically.
```

## /create
Endpoint used to create a new record of a specific type, in a specific schema.  
Python example (requests):
```python
requests.post(
    "https://railgun.aigis.dev/create",
    headers={"Authorization": "Bearer <token>"},
    json={
        "schema": f"{schema}",
        "entity": f"{entity}",
        "data": {
            f"{field}": f"{value}",
            # Repeat for each field to set
        }
    }
)
# Response.json()
{"type": "<entity>", "uid": 1}
```

## /read
Endpoint for fetching information from a Railgun-controled DB.  
In order to accomodate complex querying and filtering patterns, `/read` can be given multiple parameters.
Simple python example (requests):
```python
requests.post(
    "https://railgun.aigis.dev/read",
    headers={"Authorization": "Bearer <token>"},
    json={
        "schema": "railgun_internal",
        "entity": "Schema",
        "read": {
            "return_fields": ["code", "host", "name"],
            "page": 1,
            "pagination": 25,
            "order": "uid",
            "filters": {"filter_operator": "AND", "filters": [["uid", "less_than", 3]]},
            "include_count": False,
            "show_archived": False
        }
    }
)
# Response.json()
[
    {
        "type": "Schema",
        "uid": 1,
        "code": "railgun_internal",
        "name": "Railgun Internal",
        "host": "stellardb"
    },
    {
        "type": "Schema",
        "uid": 2,
        "code": "archive",
        "name": "Archive",
        "host": "192.168.X.X"
    }
]
```
### Read/Filter Syntax
Syntax of the `/read` functionality is a bit more complex to allow for fitlering, grouping, paging, etc. Following is a breakdown of the parameters.
```
"read" options:
    filters={"filter_operator": "AND", "filters": [["uid", "is", 42]]},
    return_fields=["type", "uid"],
    pagination=25,
    page=1,
    show_archived=False,
    include_count=False,
```
- **filters**  
(dedicated section below)

- **return_fields**  
(dedicated section below)

- **pagination**  
Specify the number of results per page to return. Default is 25.

- **page**  
Page to return, default 1

- **show_archived**  
Toggle whether or not to query only archived data, default False.

- **include_count**  
If True, the last element returned in the list will be a simple dictionary of `{"count": <int>}` defining the total number of records matching your query.  
this requires making two full calls to the DB (one to fetch within pagination constraints and one to query the COUNT). Default is False.


### Filter Syntax
Filters are represented as a resursive JSON of conditions, with a supplied filter operator ("AND"/"OR") at each level. For example:
```python
{
    "filter_operator": "AND",
    "filters": [
        ["video_type", "is", "Feature Film"],
        {
            "filter_operator": "OR",
            "filters": [
                ["code", "contains", "Big"],
                ["code", "contains", "Small"]
            ]
        }
    ]
}
```
This will return all entities where the field `video_type` is "Feature Film" and where the field `code` *either* contains "Big" *or* "Small". This format of filter can be repeated ad-nauseam


#### Filter Options
The following filter options are currently defined:
- `is`
- `is_not`
- `contains`
- `not_contains`
- `starts_with`
- `ends_with`
- `greater_than`
- `less_than`


#### Filter Discrepancies to Note (TODO)
- Filtering based on entities is not supported yet.
- Filtering based on linked fields is not supported yet.


### Return Fields
Specify here a list of the fields you wish to retrieve from the record.

***Important!***  
By default, `type`, `uid` and the field defined as the entity's display name column (`code`, by default) are always returned! The display name column for an entity type is defined in the `display_name_col` field on the entity itself.  
There's nothing preventing you from specifying them manually, but it is not necessary. `return_fields` is not a required parameter
```python
requests.post(
    "https://railgun.aigis.dev/read",
    headers={"Authorization": "Bearer <token>"},
    json={
        "schema": "railgun_internal",
        "entity": "schema",
        "read": {
            "filters": {"filter_operator": "AND", "filters": [["uid", "is", 1]]}
        }
    }
)
# Response.json()
{
    "type": "Schema",
    "uid": 1,
    "code": "railgun_internal"
}
```

### Linked Return Fields
Railgun's return structure varies slightly from that of other similar commercial software. The syntax for specifying a linked return field is standard:
```python
["field.Linked Entity Type.linked_field"]
# For example
["page_settings.Page Setting.entity.Entity.schema.Schema.code"]
```
There is no limit on how deep the linkage can go. Bear in mind that the deeper you go, the slower the query will be (more JOINS required on the DB).

The exact string set in the `return_fields` parameter will *not* be included in the response. Instead, sub-objects, *including default fields (type, uid, display_name_col)*, are included under the linked field's code.
```python
# Request JSON body:
{
    "schema": "railgun_internal",
    "entity": "Page",
    "read": {
        "filters": {"filter_operator": "AND", "filters": [["uid", "is", 1]]},
        "return_fields": ["page_settings.Page Setting.entity.Entity.schema.Schema.code"]
    }
}
# Response entity object:
[{
    "type": "Page",
    "uid": 1,
    "code": "Archive Browser",
    "page_settings": [
        {
            "type": "Page Setting", 
            "uid": 76,
            "code": "Archived Videos",
            "entity": {
                "type": "Entity",
                "uid": 7,
                "soloname": "Video",
                "schema": {
                    "type": "Schema",
                    "uid": 2,
                    "code": "Archive"
                }
            }
        }
        {
            "type": "Page Setting", 
            "uid": 77,
            "code": "Archived Books",
            "entity": {
                "type": "Entity",
                "uid": 6,
                "soloname": "Book",
                "schema": {
                    "type": "Schema",
                    "uid": 2,
                    "code": "Archive"
                }
            }
        }
    ]
}]
```
**Note as well the ability to fetch linked fields from Multi-Entity sources without issue.**

## /update
Endpoint used to update an existing record of a specific type, in a specific schema.  
Python example (requests):
```python
requests.post(
    "https://railgun.aigis.dev/update",
    headers={"Authorization": "Bearer <token>"},
    json={
        "schema": f"{schema}",
        "entity": f"{entity}",
        "entity_id": f"{entity_id}"
        "data": {
            f"{field}": f"{value}",
            # Repeat for each field to set
        }
    }
)
# Response.json()
{"type": "<entity>", "uid": 1}
```

## /delete
Endpoint used to delete an existing record of a specific type, in a specific schema.  
Python example (requests):
```python
requests.post(
    "https://railgun.aigis.dev/delete",
    headers={"Authorization": "Bearer <token>"},
    json={
        "schema": f"{schema}",
        "entity": f"{entity}",
        "entity_id": f"{entity_id}"
    }
)
# Response.json()
{"type": "<entity>", "uid": 1}
```


## /batch
Endpoint used to submit multiple requests of multiple different types (CUD) so long as they are on the same schema (DB). All requests will be validated, and all requests will be rolled back if a single one is invalid. A list of operations is required, with slightly different parameters depending on the operation type.
```python
requests.post(
    "https://railgun.aigis.dev/batch",
    headers={"Authorization": "Bearer <token>"},
    json={
        "schema": f"{schema}",  # All batched operations must be on one schema
        "batch": [
            {
                "request_type": "create" or "update" or "delete",
                "entity": f"{entity}",
                # IF "create"
                "data": {
                    f"{field}": "value",
                    #etc...
                }
                # IF "update"
                "entity_id": f"{entity_id}",
                "data": {
                    f"{field}": "value",
                    #etc...
                }
                # IF "delete"
                "entity_id": f"{entity_id}",
                "permanent": True or False
            },
            # etc...
        ]
    }
)
# Response.json()
[
    {"type": "entity", "uid": 1},
    # etc, per normal response for individual operations
]
```

## /telescope
Endpoint to fetch the schema definition of a particular DB. This can be done at either the general schema level or at a specific entity level.  
TODO - note that currently, this also returns archived fields and entities.
```python
requests.post(
    "https://railgun.aigis.dev/telescope",
    headers={"Authorization": "Bearer <token>"},
    json={
        "schema": f"{schema}",
        "entity": f"{entity}" # OPTIONAL
    }
)
# Response not included here since it can be rather massive.
```

## /stellar
*kira kira*  
***Important!***  
TODO - this doc is subject to heavy change as `/stellar` will be split into specialized endpoints.  
The STELLAR endpoint is used to perform CUD operations on entities and fields. From here, you can create new entities (tables) and new columns (fields), update the optional parameters when available, or archive/delete them.

***Important!***  
TODO - currently, archival/deletion is determined by the number of times a delete request is submitted (archive when requested, delete if already archived). This is subject to change and will be adjusted to function like the regular record operation (`permanent` optional parameter).

These changes in schema will be reflected immediately in the application replica in which they are requested. Updates are propagated out to other replicas via a pub/sub queue in redis. While this usually doesn't take more than a second or so and is done asynchroneously from app queue processsing, replicas may not *immediately* respond with updated information. This should be highly sufficient for any practical purpose however.
```python
# Field operation syntax
requests.post(
    "https://railgun.aigis.dev/stellar",
    headers={"Authorization": "Bearer <token>"},
    json={
        "part": "field",
        "request_type": "create" or "update" or "delete",
        "schema": f"{schema}",
        "entity": f"{entity}",
        "data": {
            # IF request_type == CREATE
            "code": f"{field_code}",
            "name": f"{field_name}",
            "type": f"{field_type}",
            "options": [f"{field_options}"]  # OPTIONAL

            # IF request_type == UPDATE
            "code": f"{field_code}",
            "options": [f"{field_options}"]
            
            # IF request_type == DELETE
            "code": f"{field_code}",
        }
    }
)

# Entity operation syntax
requests.post(
    "https://railgun.aigis.dev/stellar",
    headers={"Authorization": "Bearer <token>"},
    json={
        "part": "entity",
        "request_type": "create" or "delete",
        "schema": f"{schema}",
        "data": {
            # IF request_type == CREATE
            "code": f"{entity}",
            "soloname": f"{entity_soloname}",
            "multiname": f"{entity_multiname}"

            # IF request_type == DELETE
            "code": f"{entity}"
        }
    }
)

# RESPONSE:
# All schema operations return a boolean, depening on the successful completion of the operation.
```

## /upload
Endpoint for uploading files to be stored on the Railgun server. A response including the *relative* internal path is returned, which can be used to download the file in a separate call, or to fetch the statically served file using the `/discharge` endpoint (doc below).  
***Important!***  
Railgun *is not a file server* and should not be treated as one! The ability to store and serve files exists for convenience, as having simple things like thumbnails or config files stored (and served) directly is somewhat more straightforward than storing the binary data in a DB, and can make frontends more simple. Railgun does not perform and kind of compression, transcoding, or other optimization on the files and will serve them directly. Use a real file system service if needed, and populate text fields with paths if you plan on integrating heavy file management.
```python
with open(path, "rb") as infile:
    resp = requests.post(
        "https://railgun.aigis.dev/upload",
        headers={"Authorization": "Bearer <token>"},
        files={"file":infile},
        data={
            "metadata":{
                "schema": f"{schema}",
                "type": f"{entity}",
                "uid": f"{entity_id}",
                "field": f"{file_type_field}"
            }
        }
    )
# Response.json()
{
    "path": "relative/internal/path.ext
}
```

## /download
Endpoint used to access and download files previously uploaded to Railgun. Requires being given the (relative) server internal path of the file.  
TODO - this will be improved to also include accessing files via entity/field.
```python
with requests.post(
    "https://railgun.aigis.dev/download",
    headers={"Authorization": "Bearer <token>"}
    json={"path": "entity/my/file.ext"},
    stream=True
) as resp:
    resp.raise_for_status()
    # Write the file being recieved to disk.
    with open(f"{final_destination}", 'wb+') as outfile:
        for chunk in resp.iter_content(None):
            outfile.write(chunk)
```

## /discharge
This endpoint mounts all uploaded Railgun files statically. This is primarily to simplify browser-based frontend integration. Backend or API calls should probably go through the `/download` endpoint.  
An HTTP-only cookie or authorization header *is still required*.  
Access example would be `https://railgun.aigis.dev/discharge/relative/internal/path.ext`.
