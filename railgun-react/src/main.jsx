import React from 'react'
import ReactDOM from 'react-dom/client'

import { GridLayout } from './layouts/gridlayout.jsx'

import './styles/CONSTANTS.css'
import { telescope } from './STELLAR.jsx'


// TEMP GARBAGE
const DEFAULT_ENTITY_TYPE = "Audio"
const DEFAULT_SCHEMA = "archive"


async function setup() {
    let pathchunks = new URL(window.location).pathname.split("/").filter(e => e)
    const SCHEMA = pathchunks[0] || DEFAULT_SCHEMA
    const ENTITY_TYPE = pathchunks[1] || DEFAULT_ENTITY_TYPE

    // STELLAR is the minimum data set we need in order to render. Make sure it's available
    await telescope(SCHEMA)

    const context = {
        schema: SCHEMA,
        entity_type: ENTITY_TYPE
    }
    
    ReactDOM.createRoot(document.getElementById('root')).render(
        <React.StrictMode>
            <GridLayout default_context={context} />
        </React.StrictMode>,
    )
}
setup()
