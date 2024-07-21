import { useEffect, useState } from 'react'
import {RGDropDown} from '/src/components/dropdown.jsx'
import { STELLAR } from '../../STELLAR'

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


function SimpleSearchBox(props) {
    // Page can load before STELLAR is populated...
    let placeholderText = STELLAR.entities[props.context.entity_type] ? STELLAR.entities[props.context.entity_type].multiname:""
    return (
        <input
            type='text'
            className='RG_SIMPLE_SEARCHBOX'
            placeholder={'Search ' + placeholderText}
            onKeyDown={(event) => {
                if (event.key == "Enter"){
                    props.setSearchValue(event.target.value)
                } else if (event.key == "Escape") {
                    event.target.value = ""
                    event.target.blur()
                    props.setSearchValue("")
                }
            }}
        >
            {/* TODO down_arrow */}
            {/* TODO search/x button */}
        </input>
    )
}



function Gridtop(props) {
    const [field_menu_options, setFieldMenuOptions] = useState(setupFieldMenuOptions(props.fields, props.showFieldCreationWindow))

    useEffect(()=>{
        setFieldMenuOptions(setupFieldMenuOptions(props.fields, props.showFieldCreationWindow))
    }, [props.fields])

    return (
        <div style={{...props.style}} className='RG_GRIDTOP_BG'>
            <div className='RG_GRIDTOP_CHUNK'>
                <NewEntityButton context={props.context} display={props.context.entity_type} show={props.showEntityCreationWindow}/>
                <RGDropDown context={props.context} button="Fields" options={field_menu_options}/>
            </div>
            <div className='RG_GRIDTOP_CHUNK'>
                <SimpleSearchBox context={props.context} setSearchValue={props.setSearchValue}/>
            </div>
        </div>
    )
}

export default Gridtop;