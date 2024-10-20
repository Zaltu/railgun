import Select from 'react-select'
import AsyncSelect from 'react-select/async'

import { STELLAR, createRGData, fetchAutocompleteOptions } from '../STELLAR'

import './createNewEntity.css'
import { useRef } from 'react'



const TYPE_DISPLAY_ELEMENTS = {
    "BOOL": RG_CHECKBOX,
    "TEXT": RG_DEFAULTCELL,
    "INT": RG_DEFAULTCELL,
    "FLOAT": RG_DEFAULTCELL,
    "JSON": RG_DEFAULTCELL,  // TODO needs to be formatted textarea...
    "DATE": RG_DEFAULTCELL,  // TODO datepicker
    "LIST": RG_LISTSELECT,
    "MULTIENTITY": RG_MULTIENTITY,
    "ENTITY": RG_ENTITY
}


function RG_CHECKBOX (required, code, options=null) {
    return (
        <input
            type="checkbox"
            className="RG_NEWENTITY_CHECKBOX"
            required={required}
            name={code}
            id={code}
            onChange={(e) => {e.target.value=e.target.checked}}
        />
    )
}


function RG_DEFAULTCELL (required, code, options=null) {
    return (
        <input
            className="RG_NEWENTITY_EDITCELL"
            type='text'
            required={required}
            name={code}
            id={code}
        />
    )
}


function RG_LISTSELECT (required, code, options, refs, extras) {
    return (
        <Select
            unstyled
            ref={(el => refs.current[extras.refi] = el)}
            name={code}
            id={code}
            required={required}
            className='RG_EMBEDDED_LISTFIELD'
            classNames={{
                menuList: () => "RG_EMBEDDED_LISTDROP",
                option: () => "RG_DROPDOWN_LIST_ITEM"
            }}
            styles={{
                control: base => ({
                    ...base,
                    height: 27.5,
                    minHeight: 27.5
                }),
                input: base => ({
                    ...base,
                    color: 'transparent'
                }),
                dropdownIndicator: base => ({
                    ...base,
                    visibility: "hidden"
                })
            }}
            placeholder={null}
            options={options.map(option => ({value: option, label: option }))}
        />
    )
}


function RG_MULTIENTITY (required, code, options, refs, extras) {
    return (
        <AsyncSelect
            unstyled
            isMulti
            required={required}
            ref={(el => refs.current[extras.refi] = el)}
            name={code}
            id={code}
            cacheOptions
            loadOptions={(inputValue) => {
                return fetchAutocompleteOptions(options, inputValue)
            }}
            noOptionsMessage={() => `Find a ${Object.keys(options).join(". ")} by typing its name.`}
            placeholder={null}
            className='RG_MULTIENTITY_LISTFIELD'
            classNames={{
                menuList: () => "RG_MULTIENTITY_LISTDROP",
                option: () => "RG_MULTIENTITY_LIST_ITEM",
                noOptionsMessage: () => "RG_MULTIENTITY_LIST_ITEM",
                multiValue: () => "RG_MULTIENTITY_SELECTION",
                multiValueLabel: () => "RG_MULTIENTITY_SELECTION_LABEL",
                multiValueRemove: () => "RG_MULTIENTITY_SELECTION_LABEL_CLOSE"
            }}
            styles={{
                control: base => ({
                    ...base,
                    height: 'fit-content',
                    minHeight: 27.5
                }),
                dropdownIndicator: base => ({
                    visibility: "hidden",
                    width: 0
                }),
                option: (base, state) => ({
                    ...base,
                    backgroundColor: state.isFocused ? "var(--RG_HIGHLIGHT_GREY)": ""  // Needs to be here to enable keyboard navigation
                })
            }}
        />
    )
}

function RG_ENTITY (required, code, options, refs, extras) {
    return (
        <AsyncSelect
            unstyled
            required={required}
            ref={(el => refs.current[extras.refi] = el)}
            name={code}
            id={code}
            cacheOptions
            loadOptions={(inputValue) => {
                return fetchAutocompleteOptions(options, inputValue)
            }}
            noOptionsMessage={() => `Find a ${Object.keys(options).join(". ")} by typing its name.`}
            placeholder={null}
            className='RG_MULTIENTITY_LISTFIELD'
            classNames={{
                menuList: () => "RG_MULTIENTITY_LISTDROP",
                option: () => "RG_MULTIENTITY_LIST_ITEM",
                noOptionsMessage: () => "RG_MULTIENTITY_LIST_ITEM",
                multiValue: () => "RG_MULTIENTITY_SELECTION",
                multiValueLabel: () => "RG_MULTIENTITY_SELECTION_LABEL",
                multiValueRemove: () => "RG_MULTIENTITY_SELECTION_LABEL_CLOSE"
            }}
            styles={{
                control: base => ({
                    ...base,
                    height: 'fit-content',
                    minHeight: 27.5
                }),
                dropdownIndicator: base => ({
                    visibility: "hidden",
                    width: 0
                }),
                option: (base, state) => ({
                    ...base,
                    backgroundColor: state.isFocused ? "var(--RG_HIGHLIGHT_GREY)": ""  // Needs to be here to enable keyboard navigation
                })
            }}
        />
    )
}

async function createEntity(e, context, displaySelf, resetables, updateData) {
    e.preventDefault()
    let formData = new FormData(e.target)
    let data = {}
    let resetableids = resetables.current.map(resetable => resetable.props.id)
    for (const ent of formData.entries()) {
        if (ent[1] && !(resetableids.includes(ent[0])) ) {
            data[ent[0]] = ent[1]=="true" ? true: ent[1]  // HACK fucking webdevs
        }
    }
    // Manage all the resetables separately because of react-select's value management
    resetables.current.forEach((resetable) => {
        if (STELLAR.entities[context.entity_type].fields[resetable.props.id].type=="MULTIENTITY") {
            let multillist = resetable.getValue().map(ent => multillist.push(JSON.parse(ent.value)))
            if (multillist.length){
                data[resetable.props.id] = multillist
            }
        }
        else if (STELLAR.entities[context.entity_type].fields[resetable.props.id].type=="ENTITY") {
            if (resetable.getValue()[0]) {
                data[resetable.props.id] = JSON.parse(resetable.getValue()[0].value)
            }
        }
        else if (STELLAR.entities[context.entity_type].fields[resetable.props.id].type=="LIST") {
            if (resetable.getValue()[0]) {
                data[resetable.props.id] = resetable.getValue()[0].value
            }
        }
        else {
            console.error(`UNDEFINED BEHAVIOR FOR ${resetable.props.id}, IGNORING`)
        }
    })

    

    let CREATE_REQUEST = {
        schema: context.schema,
        entity: context.entity_type,
        data: data
    }
    console.log(CREATE_REQUEST)

    
    let finished = await(createRGData(CREATE_REQUEST))
    if (finished) {
        updateData()
        hideSelf(e.target, displaySelf, resetables)
    } else {
        console.error(response)
    }
}


function hideSelf(form, displayState, refs) {
    form.reset()
    refs.current.forEach((resetable) => {
        resetable.clearValue()
    })
    displayState(false)
}


function prepFieldElements(entity_type, refs) {
    let labinputs = []
    let i = 0
    Object.values(STELLAR.entities[entity_type].fields).forEach(field => {
        // TODO non/editable fields
        if (field.code=="uid"){return}
        // TODO required fields
        // style={{fontWeight: field.required ? 'bold' : 'normal'}}
        // required={field.code=='code'}
        labinputs.push((
            <div style={{display: "flex", height: 'fit-content', justifyContent: 'right'}}>
                <div className='RG_NEWENTITY_LABELSIDE' style={{fontWeight: field.code=='code' ? 'bold' : 'normal'}}>
                    <label name={field.code} htmlFor={field.code}>{field.name}</label>
                </div>
                <div className='RG_NEWENTITY_INPUTSIDE'>
                    {TYPE_DISPLAY_ELEMENTS[field.type] ? TYPE_DISPLAY_ELEMENTS[field.type](field.code=='code', field.code, field.params.constraints, refs, {entity_type: entity_type, refi: i}) : 'MISSING EDIT CELL'}
                </div>
            </div>
        ))
        i++
    })
    return labinputs
}


function NewEntityWindow(props) {
    const refs = useRef([])
    return (
        <div className='RG_NEWENTITY_WINDOW'>
            <form autoComplete='off' onSubmit={(event) => createEntity(event, props.context, props.displaySelf, refs, props.updateData)}>
                <div name="formChunk" className='RG_NEWENTITY_CHUNK RG_NEWENTITY_FORMCHUNK'>
                    {...prepFieldElements(props.context.entity_type, refs, props.setData)}
                </div>
                <div name="applyChunk" className='RG_NEWENTITY_APPLY_CHUNK RG_NEWENTITY_CHUNK'>
                    <button 
                        className='RG_SUBTLE_BUTTON'
                        type="button"
                        onClick={(event) => hideSelf(event.target.parentNode.parentNode, props.displaySelf, refs)}
                    >
                        Cancel
                    </button>
                    <button className="RG_HIGHLIGHT_BUTTON">
                        Create {props.context.entity_type}
                    </button>
                </div>
            </form>
        </div>
    )
}

export {NewEntityWindow};
