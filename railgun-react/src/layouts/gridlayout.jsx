import { useState } from "react"

import Grid from './grid/grid.jsx'
import Gridtop from './grid/gridtop.jsx'
import { NewFieldWindow } from "/src/components/createFieldBox.jsx"
import { EditFieldWindow } from "../components/editFieldBox.jsx"
import { NewEntityWindow } from "../components/createNewEntity.jsx"

function GridLayout(props) {
    const [fieldCreateVisible, showFieldCreation] = useState(false)
    const [fieldEditVisible, showFieldEdit] = useState(false)
    const [selectedFieldData, setSelectedField] = useState({})
    const [entityCreateVisible, showEntityCreation] = useState(false)

    return (
        <div>
            <Gridtop context={props.context} fields={props.fields} showFieldCreationWindow={showFieldCreation} showEntityCreationWindow={showEntityCreation} />
            <Grid context={props.context} data={props.data} fields={props.fields} showFieldEditWindow={showFieldEdit} setSelectedField={setSelectedField}/>
            <NewFieldWindow context={props.context} displaySelf={showFieldCreation} display={fieldCreateVisible ? 'visible' : 'hidden'} />
            <EditFieldWindow context={props.context} displaySelf={showFieldEdit} display={fieldEditVisible ? 'visible' : 'hidden'} field={selectedFieldData} />
            {entityCreateVisible ? <NewEntityWindow context={props.context} displaySelf={showEntityCreation} /> : null }
        </div>
    )
}

export {GridLayout}
