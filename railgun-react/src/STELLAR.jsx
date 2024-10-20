var STELLAR = null
var AUTH_HEADER = {"Authorization": `Bearer ${localStorage.AUTH_TOKEN}`}

const RG_URL = import.meta.env.RG_BACKEND_URL


async function _makeRGCall(endpoint, body, method){
    let response = await fetch(`${RG_URL}/${endpoint}`, {
        mode:"cors",
        method: method || "POST",
        headers: AUTH_HEADER,
        body: body
    })
    if ([401, 405].includes(response.status)){
        // Auth failure, redirect to login
        location.href = "/login/login.html"
        return  // Who cares
    }
    return response
}


async function login(creds) {
    console.log("Attempting to log in")
    console.log(creds)
    let login_response = await fetch(RG_URL+"/login", {
        mode:"cors",
        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
        method: "POST",
        body: `grant_type=password&username=${creds.get("username")}&password=${creds.get("password")}`
    })
    if (!login_response.ok){
        console.log("returning false")
        return false
    }
    localStorage.AUTH_TOKEN = (await login_response.json())["access_token"]
    // Refresh auth token since it's stateless
    AUTH_HEADER = {"Authorization": `Bearer ${localStorage.AUTH_TOKEN}`}

    console.log(localStorage.AUTH_TOKEN)
    console.log(AUTH_HEADER)
    return true
}


async function telescope(schema, entity, setGlobal=true) {
    let response = await _makeRGCall(
        "telescope",
        JSON.stringify(entity ? {schema: schema, entity:entity} : {schema: schema})
    )
    let LOCAL_STELLAR = await response.json()
    if (setGlobal) {
        STELLAR = LOCAL_STELLAR
    }
    return LOCAL_STELLAR
}


async function fetchRGData(entity_type, fields, filters=null, page=1, schema=null) {
    if (schema) {
        await telescope(schema)
    }

    let fetch_data = {
        "schema": schema||STELLAR.code,
        "entity": entity_type,
        "read": {
                "filters": filters ? filters : null,
                "return_fields": fields,
                "page": page,
                "pagination": 100,
                "include_count": true
        }
    }
    let response = await _makeRGCall(
        "read",
        JSON.stringify(fetch_data)
    )
    return await response.json()
}


async function createRGData(CREATE_REQUEST){
    let response = await _makeRGCall(
        "create",
        JSON.stringify(CREATE_REQUEST),
    )
    if (response.ok) {
        console.log("CREATED!!!!")
        return true
    } else {
        console.error(response)
        return false
    }
}


async function updateRGData(UPDATE_REQUEST){
    let response = await _makeRGCall(
        "update",
        JSON.stringify(UPDATE_REQUEST)
    )
    if (response.ok) {
        console.log("UPDATED!!!!")
        return true
    } else {
        console.log(response)
        return false
    }
}


async function batchRGData(batchData){
    // TODO possiblt change return behavior here
    let response = await _makeRGCall(
        "batch",
        JSON.stringify(batchData),
    )
    if (response.ok) {
        return true
    } else {
        console.error(response)
        return false
    }
}


async function fetchAutocompleteOptions(fieldConstraints, input) {
    let allOptions = []
    await Promise.all(Object.keys(fieldConstraints).map(async (possibleType) => {
        let fetchData = {
            "schema": STELLAR.code,
            "entity": possibleType,
            "read": {
                "return_fields": [STELLAR.entities[possibleType].display_name_col],
                "filters": {  // TODO this filter should be defined in the params of the multi/entity field(s)
                    "filter_operator": "AND",
                    "filters": [
                                [STELLAR.entities[possibleType].display_name_col, "starts_with", input],
                            ]
                },
                "pagination": 10
            }
        }
        let response = await _makeRGCall(
            "read",
            JSON.stringify(fetchData)
        )
        if (!response.ok) {
            return  // This possibleEntity will not be displayed.
        }
        response = await response.json()
        let theseOptions = response.map(ent => {
            return {label: ent[STELLAR.entities[possibleType].display_name_col], value: JSON.stringify(ent)}
        })
        allOptions = allOptions.concat(theseOptions)
    }))
    return allOptions
}


export {STELLAR, AUTH_HEADER, telescope, fetchRGData, createRGData, updateRGData, batchRGData, fetchAutocompleteOptions, login};
