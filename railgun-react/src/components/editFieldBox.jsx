import './editFieldBox.css'


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


function setupFieldTypeRadio() {
    return VALID_FIELD_TYPES.map(rg_type => (
        <div
            style={{
                display: "flex",
                gap: "5px"
            }}
        >
            <input disabled required type='radio' id={rg_type.type} name="fieldtype" />
            <label htmlFor={rg_type.type} className='RG_NEWFIELD_RADIOLABEL'>{rg_type.label}</label>
        </div>
    ))
}


async function createField(e, context, displayState) {
    e.preventDefault()
    let formData = e.target
    let UPDATE_REQUEST = {
        part: "field",
        request_type: "update",
        schema: context.schema,
        entity: context.entity_type,
        data: {
            name: formData.fieldname.value,
            options: formData.fieldparams.value && JSON.parse(formData.fieldparams.value)  // lol
        }
    }
    console.log(UPDATE_REQUEST)

    // fetch("http://127.0.0.1:8888/stellar", {
    //     mode:"cors",
    //     method: "POST",
    //     body: JSON.stringify(CREATE_REQUEST)
    // })
    //     .then((response) => {
    //         if (response.ok) {
    //             console.log("CREATED!!!!")
    //             hideSelf(formData, setFieldCode, displayState)
    //         } else {
    //             console.log(response)
    //         }
    //     })
}


function hideSelf(form, displayState) {
    form.reset()
    displayState(false)
}


function EditFieldWindow(props) {
    return (
        <div className='RG_EDIT_FIELD_WINDOW'>
            <form autoComplete='off' onSubmit={(event) => createField(event, props.context, props.displaySelf)}>
                <div name="identifyChunk" className='RG_NEWFIELD_IDENTIFY_CHUNK RG_NEWFIELD_CHUNK'>
                    <span>New Field Name</span>
                    <input required name="fieldname" type='string' defaultValue={props.field.name}/>
                    <span>Field Code</span>
                    <label name="fieldcode">{props.field.code}</label>
                </div>
                <div name="typeOptionsChunk" className='RG_NEWFIELD_OPTIONS_CHUNK RG_NEWFIELD_CHUNK'>
                    <div name="fieldtypeSelect" className='RG_NEWFIELD_FIELDTYPE RG_NEWFIELD_CHUNK'>
                        <span style={{marginBottom: "5px"}}>Field Type</span>
                        {...setupFieldTypeRadio()}
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
                        onClick={(event) => hideSelf(event.target.parentNode.parentNode, props.displaySelf)}
                    >
                        Cancel
                    </button>
                    <button className="RG_HIGHLIGHT_BUTTON">
                        Update Field
                    </button>
                </div>
            </form>
        </div>
    )
}

export {EditFieldWindow};
