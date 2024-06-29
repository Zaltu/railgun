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

export {STELLAR, telescope};
