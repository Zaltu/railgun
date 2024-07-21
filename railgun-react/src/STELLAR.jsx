var STELLAR = null

async function telescope(schema, entity, setGlobal=true) {
    let LOCAL_STELLAR = await (await fetch("http://127.0.0.1:8888/telescope", {
        mode:"cors",
        method:"POST",
        body: JSON.stringify(entity ? {schema: schema, entity:entity} : {schema: schema})
    })).json()
    if (setGlobal) {
        STELLAR = LOCAL_STELLAR
    }
    return LOCAL_STELLAR
}


async function fetchRGData(entity_type, fields, filters=null, schema=null) {
    if (schema) {
        await telescope(schema)
    }

    let fetch_data = {
        "schema": schema||STELLAR.code,
        "entity": entity_type,
        "read": {
                "filters": filters ? filters : null,
                "return_fields": fields,
                "pagination": 100
        }
    }

    const rg_data = await (await fetch("http://127.0.0.1:8888/read", {
        mode:"cors",
        method: "POST",
        body: JSON.stringify(fetch_data)
    })).json()

    return rg_data
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
        let response = await fetch("http://127.0.0.1:8888/read", {
            mode:"cors",
            method:"POST",
            body: JSON.stringify(fetchData)
        })
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


export {STELLAR, telescope, fetchRGData, fetchAutocompleteOptions};
