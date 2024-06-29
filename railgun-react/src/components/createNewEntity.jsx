import { STELLAR } from '../STELLAR'

import './createNewEntity.css'



const TYPE_DISPLAY_ELEMENTS = {
    "BOOL": RG_CHECKBOX,
    "TEXT": RG_DEFAULTCELL,
    "INT": RG_DEFAULTCELL,
    "FLOAT": RG_DEFAULTCELL,
    "JSON": RG_DEFAULTCELL,  // TODO needs to be textarea...
    "DATE": RG_DEFAULTCELL,  // TODO datepicker
    "LIST": RG_DEFAULTCELL,  // TODO
    //"MULTIENTITY": RG_MULTIENTITY_DISPLAY,
    //"ENTITY": RG_ENTITY_DISPLAY
}


function RG_CHECKBOX (required, code) {
    return (
        <input
            type="checkbox"
            className="RG_CHECKBOX"
            required={required}
            name={code}
            id={code}
            onChange={(e) => {e.target.value=e.target.checked}}
        />
    )
}

function RG_DEFAULTCELL (required, code) {
    return (
        <input
            className="RG_GRID_EDITCELL"
            type='text'
            required={required}
            name={code}
            id={code}
        />
    )
}



async function createEntity(e, context, displaySelf) {
    e.preventDefault()
    let formData = new FormData(e.target)
    let data = {}
    for (const ent of formData.entries()) {
        console.log(ent)
        if (ent[1]) {
            data[ent[0]] = ent[1]=="true" ? true: ent[1] // HACK fucking webdevs
        }
    }

    let CREATE_REQUEST = {
        schema: context.schema,
        entity: context.entity_type,
        data: data
    }
    console.log(CREATE_REQUEST)

    fetch("http://127.0.0.1:8888/create", {
        mode:"cors",
        method: "POST",
        body: JSON.stringify(CREATE_REQUEST)
    })
        .then((response) => {
            if (response.ok) {
                console.log("CREATED!!!!")
                hideSelf(e.target, displaySelf)
            } else {
                console.log(response)
            }
        })
}


function prepFieldElements(entity_type) {
    let labinputs = []
    Object.values(STELLAR.entities[entity_type].fields).forEach(field => {
        // TODO non/editable fields
        if (field.code=="uid"){return}
        // TODO required fields
        // style={{fontWeight: field.required ? 'bold' : 'normal'}}
        // required={field.code=='code'}
        labinputs.push(
            <div className='RG_NEWENTITY_LABELSIDE' style={{fontWeight: field.code=='code' ? 'bold' : 'normal'}}>
                <label name={field.code} htmlFor={field.code}>{field.name}</label>
            </div>
        )
        labinputs.push(
            <div className='RG_NEWENTITY_INPUTSIDE'>
                {TYPE_DISPLAY_ELEMENTS[field.type] ? TYPE_DISPLAY_ELEMENTS[field.type](field.code=='code', field.code) : 'MISSING EDIT CELL'}
                {/* <input required={field.code=='code'} name={field.code} id={field.code} type='text'/> */}
            </div>
        )
    })
    return labinputs
}


function hideSelf(form, displayState) {
    form.reset()
    displayState(false)
}


function NewEntityWindow(props) {
    return (
        <div className='RG_NEWENTITY_WINDOW' style={{visibility: props.display}}>
            <form autoComplete='off' onSubmit={(event) => createEntity(event, props.context, props.displaySelf)}>
                <div name="formChunk" className='RG_NEWENTITY_CHUNK RG_NEWENTITY_FORMCHUNK'>
                    {...prepFieldElements(props.context.entity_type, props.displaySelf)}
                </div>
                <div name="applyChunk" className='RG_NEWENTITY_APPLY_CHUNK RG_NEWENTITY_CHUNK'>
                    <button 
                        className='RG_SUBTLE_BUTTON'
                        type="button"
                        onClick={(event) => hideSelf(event.target.parentNode.parentNode, props.displaySelf)}
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
