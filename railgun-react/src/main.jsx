import React from 'react'
import ReactDOM from 'react-dom/client'
import { STELLAR, telescope } from './STELLAR.jsx'

import { GridLayout } from './layouts/gridlayout.jsx'
import { RGHeader } from './components/rgheader.jsx'

import './styles/CONSTANTS.css'

// TEMP GARBAGE
const DEFAULT_ENTITY_TYPE = "Audio"
const DEFAULT_SCHEMA = "archive"


async function setup() {
    let pathchunks = new URL(window.location).pathname.split("/").filter(e => e)
    const SCHEMA = pathchunks[0] || DEFAULT_SCHEMA
    const ENTITY_TYPE = pathchunks[1] || DEFAULT_ENTITY_TYPE

    console.log(STELLAR)
    const STELLAR_STELLAR = await telescope(SCHEMA)
    console.log(STELLAR)

    const stellar = STELLAR_STELLAR.entities[ENTITY_TYPE]

    let fetch_data = {
        "schema": SCHEMA,
        "entity": ENTITY_TYPE,
        "read": {
                "return_fields": Object.keys(stellar.fields),
                "pagination": 100
        }
    }

    const rg_data = await (await fetch("http://127.0.0.1:8888/read", {
        mode:"cors",
        method: "POST",
        body: JSON.stringify(fetch_data)
    })).json()

    const context = {
        schema: SCHEMA,
        entity_type: ENTITY_TYPE
    }

    ReactDOM.createRoot(document.getElementById('root')).render(
        <React.StrictMode>
            <RGHeader context={context}/>
            <GridLayout context={context} fields={stellar.fields} data={rg_data} />
        </React.StrictMode>,
    )
}
setup()
