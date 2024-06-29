import { useState } from 'react'
import {RGDropDown} from '/src/components/dropdown.jsx'

import '/src/styles/gridtop.css'

function NewEntityButton(props) {
    return (
        <button
            className="RG_HIGHLIGHT_BUTTON"
            onClick={() => props.show(true)}
        >
            Add {props.display}
        </button>
    )
}


function setupFieldMenuOptions(fields, showFieldCreationWindow) {
    return [
        {
            label: "",
            options: [{value: "edit", label:"Create New Field", callback: () => showFieldCreationWindow(true)},]
        },
        {
            label: "",
            options: [
                ...Object.values(fields).map((field) => ({
                        value: field.code,
                        label: field.name,
                        callback: () => console.log(field.code)
                }))
            ]
        }
    ]
}


function Gridtop(props) {
    const [field_menu_options, _] = useState(setupFieldMenuOptions(props.fields, props.showFieldCreationWindow))

    return (
        <div className='RG_GRIDTOP_BG'>
            <NewEntityButton context={props.context} display={props.context.entity_type} show={props.showEntityCreationWindow}/>
            <RGDropDown context={props.context} button="Fields" options={field_menu_options}/>
        </div>
    )
}

export default Gridtop;