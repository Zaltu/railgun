
import { RGDropDown } from './dropdown.jsx'
import { STELLAR } from '../STELLAR.jsx';
import './rgheader.css'


// TEMP GARBAGE
const SCHEMA_OPTIONS = [
    {
        label: "",
        options: [
            {
                value: "railgun_internal",
                label:"Railgun Internal",
                callback: () => window.location.href = "/railgun_internal/Schema"
            },
            {
                value: "archive",
                label:"Archive",
                callback: () => window.location.href = "/archive/Audio"
            }
        ]
    }
]


function getEntityButtons(context){
    return Object.keys(STELLAR.entities).map((entname) => {
        return (
            <button
                className='RG_HEADER_ENTBUTTON'
                style={{color: entname==context.entity_type && "orange"}}
                onClick={entname==context.entity_type ? () => void(0) : (() => window.location.href = "/"+context.schema+"/"+entname)}
            >
                    {entname}
            </button>
        )
    })
}


function RGHeader(props) {
    return (
        <div className="RG_HEADER">
            <div className="RG_HEADER_TOP">
                <div className='RG_HEADER_TOPLEFT'>
                    <img style={{height: '18px'}} src='/src/assets/railguntemplogo.png' />
                    <RGDropDown button='Schema' options={SCHEMA_OPTIONS} />
                </div>
                <div>
                    {/* TODO INSERT USER ICON HERE */}
                </div>
            </div>
            <div className="RG_HEADER_BOTTOM">
                <div className='RG_PAGENAME'>{STELLAR.name}</div>
                <div className='RG_HEADER_BOTTOM_ENTLIST'>
                    {...getEntityButtons(props.context)}
                </div>
            </div>
        </div>
    )
}

export {RGHeader};
