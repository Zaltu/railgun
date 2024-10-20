import {useEffect, useRef, useState } from "react"

import Select from 'react-select'
import AsyncSelect from 'react-select/async'

import {RGHeader} from '../components/rgheader.jsx'
import Gridtop from './grid/gridtop.jsx'
import Grid from './grid/grid.jsx'
import {GridBottom} from "./grid/gridbottom.jsx"

import { NewFieldWindow } from "/src/components/createFieldBox.jsx"
import { EditFieldWindow } from "../components/editFieldBox.jsx"
import { NewEntityWindow } from "../components/createNewEntity.jsx"

import {STELLAR, telescope, fetchRGData, updateRGData, fetchAutocompleteOptions} from '/src/STELLAR.jsx';


const TYPE_DISPLAY_ELEMENTS = {
    "BOOL": RG_CHECKBOX,
    "TEXT": RG_DEFAULTCELL,
    "INT": RG_DEFAULTCELL,
    "FLOAT": RG_DEFAULTCELL,
    "JSON": RG_JSON_DISPLAY,
    "DATE": RG_DEFAULTCELL,  // TODO datepicker
    "LIST": RG_GRID_LIST,
    "MULTIENTITY": RG_MULTIENTITY,  // TODO
    "ENTITY": RG_ENTITY  // TODO
}


function RG_CHECKBOX (cell, context) {
    return (
        <input
            type="checkbox"
            className="RG_CHECKBOX"
            checked={cell.getValue()||false}
            onChange={(event) => {
                updateRG(event, cell, event.target.checked, context)
            }}
        />
    )
}

function RG_DEFAULTCELL (cell, context) {
    const [editable, setEditable] = useState(false)

    return editable ?
        <input
            className="RG_GRID_EDITCELL"
            onFocus={(event) => event.target.select()}
            onBlur={() => setEditable(false)}
            style={{width: "100%", outline: "none"}}
            type='text'
            defaultValue={cell.getValue()}
            autoFocus
            onKeyDown={(event) => {
                if (event.key == "Escape"){
                    setEditable(false)
                } else if (event.key == "Enter") {
                    // Submit data update request
                    updateRG(event, cell, event.target.value, context)
                    setEditable(false)
                }
            }}
        />

        :
        //<div> TODO Set edit icon</div>
        <div
            className="RG_GRID_DISPLAYCELL"
            onDoubleClick={() => setEditable(true)}
        >
            {cell.getValue()}
        </div>
}

function RG_JSON_DISPLAY (cell, context) {  // TODO proper formatting/textarea  TODO unescaped characters on edit rerender fsr
    const [editable, setEditable] = useState(false)

    return editable ?
        <input
            className="RG_GRID_EDITCELL"
            onFocus={(event) => event.target.select()}
            onBlur={() => setEditable(false)}
            style={{width: "100%", outline: "none"}}
            type='text'
            defaultValue={JSON.stringify(cell.getValue())}
            autoFocus
            onKeyDown={(event) => {
                if (event.key == "Escape"){
                    setEditable(false)
                } else if (event.key == "Enter") {
                    // Submit data update request
                    updateRG(event, cell, event.target.value, context)
                    setEditable(false)
                }
            }}
        />

        :
        //<div> TODO Set edit icon</div>
        <div
            className="RG_GRID_DISPLAYCELL"
            onDoubleClick={() => setEditable(true)}
        >
            {JSON.stringify(cell.getValue())}
        </div>
}

function RG_GRID_LIST (cell, context) {
    const [editable, setEditable] = useState(false)

    return editable ?
        <Select
            name={"SELECT_GRIDLIST_STELLAR_HACK"}  // HACK
            autoFocus={true}
            openMenuOnFocus={true}
            onBlur={() => {
                setEditable(false)
            }}
            onMenuClose={() => setEditable(false)}
            unstyled
            options={STELLAR.entities[context.entity_type].fields[cell.column.id].params.constraints.map(((option) => {
                return {label: option, value: option}
            }))}
            className='RG_GRID_LISTFIELD'
            classNames={{
                menuList: () => "RG_GRID_LISTDROP",
                option: () => "RG_GRID_LISTITEM"
            }}
            styles={{
                control: base => ({
                    ...base,
                    height: 18,
                    minHeight: 18
                }),
                input: base => ({
                    ...base,
                    color: 'transparent'
                })
            }}
            defaultValue={{value: cell.getValue(), label: cell.getValue()}}
            onKeyDown={(event) => {
                if (event.key == "Escape"){
                    setEditable(false)
                }
            }}
            onChange={(newval) => {
                let fakeEvent = {
                    target: {
                        parentNode: document.getElementsByName("SELECT_GRIDLIST_STELLAR_HACK")[0].parentNode.parentNode
                    }
                }
                updateRG(fakeEvent, cell, newval.value, context)
                setEditable(false)
            }}
        />

        :
        //<div> TODO Set edit icon</div>
        <div
            className="RG_GRID_DISPLAYCELL"
            onDoubleClick={() => {
                setEditable(true)
            }}
        >
            {cell.getValue()}
        </div>
}

function RG_MULTIENTITY (cell, context) {
    const [editable, setEditable] = useState(false)
    const ment = useRef()

    return editable ? (
        <AsyncSelect
            unstyled
            isMulti
            ref={ment}
            name={"SELECT_GRIDMULTIENT_STELLAR_HACK"}  // HACK
            autoFocus={true}
            openMenuOnFocus={true}
            onBlur={() => {
                setEditable(false)
            }}
            cacheOptions
            defaultValue={(cell.getValue()||[]).map(ent => {
                return {label: ent[STELLAR.entities[ent["type"]].display_name_col], value: JSON.stringify(ent)}
            })}
            loadOptions={(inputValue) => {
                return fetchAutocompleteOptions(STELLAR.entities[context.entity_type].fields[cell.column.id].params.constraints, inputValue)
            }}
            noOptionsMessage={() => `Find a ${Object.keys(STELLAR.entities[context.entity_type].fields[cell.column.id].params.constraints).join(". ")} by typing its name.`}
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
                    minHeight: 18
                }),
                dropdownIndicator: () => ({
                    visibility: "hidden",
                    width: 0
                }),
                option: (base, state) => ({
                    ...base,
                    backgroundColor: state.isFocused ? "var(--RG_HIGHLIGHT_GREY)": ""  // Needs to be here to enable keyboard navigation
                })
            }}
            onKeyDown={(event) => {
                if (event.key == "Escape"){
                    setEditable(false)
                }
                else if (event.key == "Enter" && ment.current.getFocusableOptions().length==0){  // TODO Sus
                    let fakeEvent = {
                        target: {
                            parentNode: document.getElementsByName("SELECT_GRIDMULTIENT_STELLAR_HACK")[0].parentNode.parentNode.parentNode
                        }
                    }
                    let newvalue = ment.current.getValue().map(ent => JSON.parse(ent.value))
                    updateRG(fakeEvent, cell, newvalue.length ? newvalue : null, context)
                    setEditable(false)
                }
            }}
        />
    )
    :
    (<div
        className="RG_GRID_DISPLAYCELL"
        onDoubleClick={() => {
            setEditable(true)
        }}
    >
        {(cell.getValue() || []).map(entity => (
            // Gets a little weird when changing headers & data during "potentially" different frames.
            entity.type in STELLAR["entities"] ? entity[STELLAR["entities"][entity.type].display_name_col]: ""
        )).join(', ')}
    </div>)
}

function RG_ENTITY (cell, context) {
    const [editable, setEditable] = useState(false)
    const ment = useRef()

    return editable ? (
        <AsyncSelect
            unstyled
            ref={ment}
            isClearable={true}
            name={"SELECT_GRIDMULTIENT_STELLAR_HACK"}  // HACK
            autoFocus={true}
            openMenuOnFocus={true}
            onBlur={() => {
                setEditable(false)
            }}
            cacheOptions
            defaultValue={
                {label: cell.getValue() ? cell.getValue()[STELLAR["entities"][cell.getValue().type].display_name_col]:"", value: JSON.stringify(cell.getValue()||"")}
            }
            loadOptions={(inputValue) => {
                return fetchAutocompleteOptions(STELLAR.entities[context.entity_type].fields[cell.column.id].params.constraints, inputValue)
            }}
            noOptionsMessage={() => `Find a ${Object.keys(STELLAR.entities[context.entity_type].fields[cell.column.id].params.constraints).join(". ")} by typing its name.`}
            placeholder={null}
            className='RG_MULTIENTITY_LISTFIELD'
            classNames={{
                menuList: () => "RG_MULTIENTITY_LISTDROP",
                option: () => "RG_MULTIENTITY_LIST_ITEM",
                noOptionsMessage: () => "RG_MULTIENTITY_LIST_ITEM",
            }}
            styles={{
                control: base => ({
                    ...base,
                    height: 'fit-content',
                    minHeight: 18
                }),
                dropdownIndicator: () => ({
                    visibility: "hidden",
                    width: 0
                }),
                option: (base, state) => ({
                    ...base,
                    backgroundColor: state.isFocused ? "var(--RG_HIGHLIGHT_GREY)": ""  // Needs to be here to enable keyboard navigation
                })
            }}
            onKeyDown={(event) => {
                if (event.key == "Escape"){
                    setEditable(false)
                }
                else if (event.key == "Enter" && ment.current.getFocusableOptions().length==0){  // TODO Sus
                    let fakeEvent = {
                        target: {
                            parentNode: document.getElementsByName("SELECT_GRIDMULTIENT_STELLAR_HACK")[0].parentNode.parentNode.parentNode
                        }
                    }
                    let newvalue = ment.current.getValue()
                    updateRG(fakeEvent, cell, newvalue.length ? JSON.parse(newvalue[0].value) : null, context)
                    setEditable(false)
                }
            }}
        />
    )
    :
    (<div
        className="RG_GRID_DISPLAYCELL"
        onDoubleClick={() => {
            setEditable(true)
        }}
    >
        {/* Gets a little weird when changing headers & data during "potentially" different frames. */}
        {cell.getValue()&&cell.getValue().type in STELLAR["entities"] ? cell.getValue()[STELLAR["entities"][cell.getValue().type].display_name_col]: ""}
    </div>)
}

function RG_GRID_CELL_HIDDEN ({...rest}) {
    return (
        <div className="RG_GRID_CELL_HIDDEN" />
    )
}

async function updateRG(event, cell, newvalue, context){
    if ((cell.getValue() === newvalue)||(cell.getValue() != 0 && !cell.getValue() && !newvalue)) {
        // No actual data changed
        return
    }
    newvalue = newvalue ? newvalue : null  // Prevent number values from being set to '' instead of empty
    cell.getContext().table.options.meta.updateData(
        cell.row.index,
        cell.column.id,
        newvalue
    )
    let UPDATE_REQUEST = {
        schema: context.schema,
        entity: context.entity_type,
        entity_id: cell.row.original.uid,
        data: {
            [cell.column.id]: newvalue
        }
    }
    let tdNode = event.target.parentNode
    console.log(UPDATE_REQUEST)
    let updated = await updateRGData(UPDATE_REQUEST)
    if (updated) {
        // Flash green
        tdNode.classList.add('flashgood');
        setTimeout(() => {
            tdNode.classList.remove('flashgood');
            }, 1000);
    } else {
        // TODO Flash red
    }
}



const SELECT_HEADER = {
    size: 'fit-content',
    //size: 0,  // fit-content would be the correct style, but react-tables doesn't like it
    //minSize: 0,
    id: 'select-col',
    header: ({ table }) => (
      <input type="checkbox"
        checked={table.getIsAllRowsSelected()}
        onChange={table.getToggleAllRowsSelectedHandler()}
      />
    ),
    cell: ({ row }) => (
      <input type="checkbox"
        className="RG_CHECKBOX"
        checked={row.getIsSelected()}
        disabled={!row.getCanSelect()}
        onChange={row.getToggleSelectedHandler()}
      />
    ),
    enableResizing: false,
}

const ADD_HEADER = {
    size: 'fit-content',
    //size: 0,  // fit-content would be the correct style, but react-tables doesn't like it
    //minSize: 0,
    id: 'add-col',
    header: "+",
    enableResizing: false,
    cell: ({cell}) => (RG_GRID_CELL_HIDDEN(cell))  // TODO doesn't work, hides cell content not cell (td element)
}

function formatHeaders(stellar_fields, context) {
    const headers = []
    headers.push(SELECT_HEADER)
    headers.push(...Object.values(stellar_fields).map(stellar_field => ({
        header: stellar_field.name,
        accessorKey: stellar_field.code,
        cell: ({cell}) => {
            return TYPE_DISPLAY_ELEMENTS[stellar_field.type] ? TYPE_DISPLAY_ELEMENTS[stellar_field.type](cell, context) : 'MISING DISPLAY ELEMENT'
        },
        enableResizing: false,
        // size: 0//'fit-content'//'fit-content'//150  // TODO fit-content somehow or page layout save
    })))
    headers.push(ADD_HEADER)
    return headers
}



async function setFieldsOnContextChange(newcontext, setFields) {
    await telescope(newcontext.schema)
    setFields(STELLAR.entities[newcontext.entity_type].fields)
}

async function updateFieldsOnCreate(context, oldfields, setFields, newfield) {
    await telescope(context.schema)
    setFields(oldfields+[STELLAR.entities[context.entity_type].fields[newfield]])
}

async function updateGridData(context, fields, filters, page, setData, setCount) {
    let rg_data = await fetchRGData(context.entity_type, Object.keys(fields), filters, page)
    setCount(rg_data.pop(-1)["total_count"])  // Count is always fetched in the webui.
    setData(rg_data)
}


function GridLayout(props) {
    const [fieldCreateVisible, showFieldCreation] = useState(false)
    const [fieldEditVisible, showFieldEdit] = useState(false)
    const [selectedFieldData, setSelectedField] = useState({})
    const [entityCreateVisible, showEntityCreation] = useState(false)

    const [context, setContext] = useState(props.default_context)
    const [fields, setFields] = useState({})
    const [data, setData] = useState({})
    const [filters, setFilters] = useState(null)
    const [headers, setHeaders] = useState(formatHeaders(fields, context))  // TODO Show/Hide
    const [searchValue, setSearchValue] = useState(null)
    const [count, setCount] = useState(71)
    const [page, setPage] = useState(1)

    const tableref = useRef()

    // When context changes, update fields
    useEffect(()=>{
        setFieldsOnContextChange(context, setFields)
        tableref.current.resetRowSelection()
    }, [context])

    // When fields change, update headers and data
    // TODO is data needed?
    useEffect(()=>{
        setHeaders(formatHeaders(fields, context))
        updateGridData(context, fields, filters, page, setData, setCount)
        // We update the data in case there's a default field value or something,
        // but the selection doesn't need to be changed purely because a field was added.
        // tableref.current.resetRowSelection()
    }, [fields])

    // When filters change, update data
    useEffect(() => {
        updateGridData(context, fields, filters, page, setData, setCount)
        tableref.current.resetRowSelection()  // Clear selection as it's independent of table indexing
    }, [filters])

    // When page changes, update data
    useEffect(() => {
        // HACK doing like this means that the count gets updated even if all that changes is the page
        // ...but it's convenient
        updateGridData(context, fields, filters, page, setData, setCount)
        tableref.current.resetRowSelection()  // Clear selection as it's independent of table indexing
    }, [page])

    // When defaultSearch changes, change filters
    useEffect(() =>  {
        if (!searchValue) {
            setFilters(null)
            return
        }
        setFilters({
            filter_operator: "AND",
            filters: [
                [STELLAR.entities[context.entity_type].display_name_col, "contains", searchValue]
            ]
        })
    }, [searchValue])

    return STELLAR!=null ? (
        <div>
            <RGHeader style={{minHeight: "8vh", height: "8vh"}} context={context} setcontext={setContext} />
            <Gridtop style={{minHeight: "4.5vh", height: "4.5vh"}} context={context} fields={fields} setSearchValue={setSearchValue} showFieldCreationWindow={showFieldCreation} showEntityCreationWindow={showEntityCreation} />
            <Grid style={{minHeight: "85vh", height: "85vh"}} context={context} data={data} setData={setData} headers={headers} showFieldEditWindow={showFieldEdit} setSelectedField={setSelectedField} tableref={tableref} updateData={() => {updateGridData(context, fields, filters, page, setData, setCount);tableref.current.resetRowSelection()}} />
            <GridBottom style={{minHeight: "2.5vh", height: "2.5vh"}} context={context} count={count} page={page} setPage={setPage} />
            {fieldCreateVisible ? <NewFieldWindow context={context} addDisplayField={(newfield)=> updateFieldsOnCreate(context, fields, setFields, newfield)} displaySelf={showFieldCreation} /> : null }
            {fieldEditVisible ? <EditFieldWindow context={context} displaySelf={showFieldEdit} field={selectedFieldData} /> : null }
            {entityCreateVisible ? <NewEntityWindow context={context} updateData={() => {updateGridData(context, fields, filters, page, setData, setCount);tableref.current.resetRowSelection()}} displaySelf={showEntityCreation} /> : null }
        </div>
    ) : ""
}

export {GridLayout}
