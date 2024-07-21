import {useState} from 'react'
import { STELLAR, telescope } from '../STELLAR'
import './createFieldBox.css'


const VALID_FIELD_TYPES = [  // TODO setup param edit boxes
    {label: "Text", type: "TEXT"},
    {label: "Number", type: "INT"},
    {label: "Float", type:"FLOAT"},
    {label: "Date", type: "DATE"},
    {label: "JSON String", type: "JSON"},
    {label: "Checkbox", type: "BOOL"},
    {label: "List", type: "LIST"},
    {label: "Entity", type: "ENTITY"},
    {label: "Multi-Entity", type: "MULTIENTITY"}
]


function setupFieldTypeRadio(setFieldType) {
    return VALID_FIELD_TYPES.map(rg_type => (
        <div
            style={{
                display: "flex",
                gap: "5px"
            }}
        >
            <input required type='radio' id={rg_type.type} name="fieldtype" onChange={() => setFieldType(rg_type.type)} />
            <label htmlFor={rg_type.type} className='RG_NEWFIELD_RADIOLABEL'>{rg_type.label}</label>
        </div>
    ))
}


function _formattedFieldCode(value, setCode) {
    setCode(value.toLowerCase().replace(/[\W_]+/g, "_").replace(/_+$/, ""))
}


async function createField(e, fieldtype, context, setFieldCode, displayState, addDisplayField) {
    e.preventDefault()
    let formData = e.target
    let CREATE_REQUEST = {
        part: "field",
        request_type: "create",
        schema: context.schema,
        entity: context.entity_type,
        data: {
            code: formData.fieldcode.value,
            name: formData.fieldname.value,
            type: fieldtype,
            options: formData.fieldparams.value && JSON.parse(formData.fieldparams.value)  // lol
        }
    }
    console.log(CREATE_REQUEST)

    fetch("http://127.0.0.1:8888/stellar", {
        mode:"cors",
        method: "POST",
        body: JSON.stringify(CREATE_REQUEST)
    })
        .then((response) => {
            if (response.ok) {
                console.log("CREATED!!!!")
                // Stellar can be too slow here...
                //addDisplayField(formData.fieldcode.value)
                hideSelf(formData, setFieldCode, displayState)
            } else {
                console.log(response)
            }
        })
}


function hideSelf(form, setFieldCode, displayState) {
    form.reset()
    // Field code value doesn't get reset on it's own pepehands
    setFieldCode("")
    displayState(false)
}


function NewFieldWindow(props) {
    const [fieldcode, setPredictedFieldCode] = useState("")
    const [fieldtype, setFieldType] = useState(null)
    
    return (
        <div className='RG_NEW_FIELD_WINDOW'>
            <form autoComplete='off' onSubmit={(event) => createField(event, fieldtype, props.context, setPredictedFieldCode, props.displaySelf, props.addDisplayField)}>
                <div name="identifyChunk" className='RG_NEWFIELD_IDENTIFY_CHUNK RG_NEWFIELD_CHUNK'>
                    <span>New Field Name</span>
                    <input required name="fieldname" type='string' onChange={(e) => _formattedFieldCode(e.target.value, setPredictedFieldCode)} />
                    <span>Field Code</span>
                    <input required name="fieldcode" type='string' value={fieldcode} onChange={(e) => {setPredictedFieldCode(e.target.value)}}/>
                </div>
                <div name="typeOptionsChunk" className='RG_NEWFIELD_OPTIONS_CHUNK RG_NEWFIELD_CHUNK'>
                    <div name="fieldtypeSelect" className='RG_NEWFIELD_FIELDTYPE RG_NEWFIELD_CHUNK'>
                        <span style={{marginBottom: "5px"}}>Field Type</span>
                        {...setupFieldTypeRadio(setFieldType)}
                    </div>
                    <div name="parameterSet" className='RG_NEWFIELD_PARAM_CHUNK RG_NEWFIELD_CHUNK'>
                        <span>Enter parameters:</span>
                        <textarea
                        style={{verticalAlign: "top"}}
                        name="fieldparams" type='string' rows="10" cols="50"/>
                    </div>
                </div>
                <div name="applyChunk" className='RG_NEWFIELD_APPLY_CHUNK RG_NEWFIELD_CHUNK'>
                    <button 
                        className='RG_SUBTLE_BUTTON'
                        type="button"
                        onClick={(event) => hideSelf(event.target.parentNode.parentNode, setPredictedFieldCode, props.displaySelf)}
                    >
                        Cancel
                    </button>
                    <button className="RG_HIGHLIGHT_BUTTON">
                        Create Field
                    </button>
                </div>
            </form>
        </div>
    )
}

export {NewFieldWindow};
