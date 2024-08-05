
import { RGDropDown } from './dropdown.jsx'
import { STELLAR } from '../STELLAR.jsx';
import './rgheader.css'


// TEMP GARBAGE
function getSchemaOptions(setcontext){
    return [{
        label: "",
        options: [
            {
                value: "railgun_internal",
                label:"Railgun Internal",
                callback: () => {
                    history.pushState({}, "rAIlgun", "/railgun_internal/Schema")
                    setcontext({
                        schema:"railgun_internal",
                        entity_type:"Schema"
                    })
                }
            },
            {
                value: "archive",
                label:"Archive",
                callback: () => {
                    history.pushState({}, "rAIlgun", "/archive/Audio")
                    setcontext({
                        schema:"archive",
                        entity_type:"Audio"
                    })
                }
            }
        ]
    }]
}


function getEntityButtons(context, setcontext){
    return Object.keys(STELLAR.entities).map((entname) => {
        return (
            <button
                className='RG_HEADER_ENTBUTTON'
                style={{color: entname==context.entity_type && "orange"}}
                onClick={entname==context.entity_type ? () => void(0) : (() => {
                    // Set Context
                    history.pushState({}, "rAIlgun", "/"+context.schema+"/"+entname)
                    setcontext({
                        schema:context.schema,
                        entity_type:entname
                    })
                })}
            >
                    {entname}
            </button>
        )
    })
}


function RGHeader(props) {
    return (
        <div style={{...props.style}} className="RG_HEADER">
            <div className="RG_HEADER_TOP">
                <div className='RG_HEADER_TOPLEFT'>
                    <img style={{maxHeight: '20px'}} src='/src/assets/railguntemplogo.png' />
                    <RGDropDown button='Schema' options={getSchemaOptions(props.setcontext)} />
                </div>
                <div>
                    {/* TODO INSERT USER ICON HERE */}
                </div>
            </div>
            <div className="RG_HEADER_BOTTOM">
                <div className='RG_PAGENAME'>{STELLAR.name}</div>
                <div className='RG_HEADER_BOTTOM_ENTLIST'>
                    {...getEntityButtons(props.context, props.setcontext)}
                </div>
            </div>
        </div>
    )
}

export {RGHeader};
