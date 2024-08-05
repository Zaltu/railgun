import Select, {components} from 'react-select'
import '/src/components/dropdown.css'


const Group = (props) => (
    <div>
        <components.Group {...props} />
    </div>
);



function applyCallback(option) {
    option.callback()
}

function RGDropDown (props) {
    // Expected props:
    // props.button: name to display as button
    // props.options: structure of drop-down options as such:
    // [
    //     {label: "",
    //      options: [
    //          {
    //              value: "edit",
    //              label:"Edit Fields",
    //              callback: () => console.log("ROFLMAO")
    //          },
    //      ]},
    //     {label: "Fields",
    //      options:[
    //          {value: "test", label:"SOME REALLY LONG ARBITRARY TEXT LABEL THAT SHOULD STILL DISPLAY LMAO", callback: () => console.log("LEARN2PLAY")}
    //      ]}
    // ]
    return (
        <Select
            unstyled
            className='RG_LIST_BUTTON'
            classNames={{
                menuList: () => "RG_DROPDOWN_LIST",
                group: () => "RG_DROPDOWN_GROUP",
                option: () => "RG_DROPDOWN_LIST_ITEM",
            }}
            styles={{
                control: (base) => ({
                    ...base,
                    minHeight: "fit-content",
                    padding: "0.3rem 0.2rem"
                })
            }}
            isSearchable={false}
            
            defaultValue={{value: "base", label:props.button}}
            value={{value: "base", label:props.button}}

            options={props.options}

            onChange={applyCallback}
            components={{Group}}
            />
    )
}

export {RGDropDown};