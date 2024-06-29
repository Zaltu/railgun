import {useState, useReducer, useEffect} from "react";
import {useReactTable, getCoreRowModel, flexRender} from '@tanstack/react-table';
import { Menu, Item, useContextMenu } from 'react-contexify';
import {STELLAR} from '/src/STELLAR.jsx';

import 'react-contexify/ReactContexify.css'
import '/src/styles/grid.css'


const TYPE_DISPLAY_ELEMENTS = {
    "BOOL": RG_CHECKBOX,
    "TEXT": RG_DEFAULTCELL,
    "INT": RG_DEFAULTCELL,
    "FLOAT": RG_DEFAULTCELL,
    "JSON": RG_JSON_DISPLAY,  // TODO needs to be escaped, response dejsoning turns the field into an object lmao
    "DATE": RG_DEFAULTCELL,  // TODO datepicker
    "LIST": RG_DEFAULTCELL,  // TODO
    "MULTIENTITY": RG_MULTIENTITY,
    "ENTITY": RG_ENTITY
}


function RG_CHECKBOX (cell, context) {
    return (
        <input
            type="checkbox"
            className="RG_CHECKBOX"
            defaultChecked={cell.getValue()}
            onChange={(event) => updateRG(event, cell, event.target.checked, context)}
        />
    )
}

function RG_DEFAULTCELL (cell, context) {
    const [editable, setEditable] = useState(false)
    cell.editable = editable
    cell.setEditable = setEditable
    return cell.editable ? 
        <input
            className="RG_GRID_EDITCELL"
            onFocus={(event) => event.target.select()}
            onBlur={() => cell.setEditable(false)}
            style={{width: "100%", outline: "none"}}
            type='text'
            defaultValue={cell.getValue()}
            autoFocus
            onKeyDown={(event) => {
                if (event.key == "Escape"){
                    cell.setEditable(false)
                } else if (event.key == "Enter") {
                    // Submit data update request
                    updateRG(event, cell, event.target.value, context)
                    cell.setEditable(false)
                }
            }}
            onSubmit={() => console.log("Submit")}
        />

        :

        <div
            className="RG_GRID_DISPLAYCELL"
        >
            {cell.getValue()}
        </div>
}

function RG_JSON_DISPLAY (cell, context) {
    return JSON.stringify(cell.getValue())
}

function RG_MULTIENTITY (cell, context) {
    return cell.getValue().map(entity => (
        entity[STELLAR["entities"][entity.type].display_name_col]
    )).join(', ')
}

function RG_ENTITY (cell, context) {
    return cell.getValue()[STELLAR["entities"][cell.getValue().type].display_name_col]
}

function RG_GRID_CELL_HIDDEN ({...rest}) {
    return (
        <div className="RG_GRID_CELL_HIDDEN" />
    )
}


function updateRG(event, cell, newvalue, context){
    console.log(newvalue)
    if (cell.getValue() == newvalue) {
        // No actual data changed
        return
    }
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
    fetch("http://127.0.0.1:8888/update", {
        mode:"cors",
        method: "POST",
        body: JSON.stringify(UPDATE_REQUEST)
    })
        .then((response) => {
            if (response.ok) {
                console.log("UPDATED!!!!")
                // tdNode.style.animation = 'flashgood 0.25s linear'
                tdNode.classList.add('flashgood');
                setTimeout(() => {
                    tdNode.classList.remove('flashgood');
                  }, 1000);
                // Flash green
            } else {
                console.log(response)
            }
        })
    // console.log(context.schema)
    // console.log(context.entity_type)
    // console.log(cell.row.original.uid)
    // console.log(cell.column.id)
    // console.log(newvalue)
}


function formatTableRow (row) {
    return (
        <tr
            key={row.id}
            className={row.getIsSelected() ? "RG_GRID_ROW_SELECT" : "RG_GRID_ROW"}
            onClick={row.getToggleSelectedHandler()}
        >
            {row.getVisibleCells().map(cell => (
                formatTableCell(cell)
            ))}
        </tr>
    )
}

function formatTableCell(cell) {
    return (
        <td
            className={"RG_GRID_CELL"}
            key={cell.id}
            onClick={()=> {
                cell.setEditable ? cell.setEditable(true) : null
            }}
        >
            {/* <div> TODO Set edit icon</div> */}
            {flexRender(cell.column.columnDef.cell, cell.getContext())}
        </td>
    )
}


function formatHeaderResizer(header) {
    return (
        <div
            {...{
            onMouseDown: header.getResizeHandler(),
            className: `
                resizer
                ${header.column.getIsResizing() ? 'isResizing' : ''}
            `,
            }}
        />
    )
}

function headerContextMenu(event, header, context, displayHeaderContext, setSelectedField) {
    setSelectedField(STELLAR.entities[context.entity_type].fields[header.id])
    displayHeaderContext({event: event})
}

function getHeaderContextMenu(displayEditField, hideSelf){
    return (
        <Menu className="RG_GRID_HEADER_CONTEXTMENU" animation={null} id="headerContextMenu">
            <Item className="RG_GRID_HEADER_CONTEXTMENU_ITEM" onClick={() => console.log("Edit Field")}>Edit Field</Item>
            <Item className="RG_GRID_HEADER_CONTEXTMENU_ITEM" onClick={() => console.log("Hide Field")}>Hide Field</Item>
        </Menu>
    )
}

function formatTableHeaderRow (headerGroup, context, displayHeaderContext, setSelectedField) {
    return (
        <tr key={headerGroup.id}>
            {headerGroup.headers.map(header => (
                <th
                    onContextMenu={(event) => headerContextMenu(event, header, context, displayHeaderContext, setSelectedField)}
                    key={header.id}
                    {...{
                        className: "RG_GRID_HEADER",
                        style: {
                            width: header.getSize(),
                            minWidth: header.getSize(),
                            maxWidth: header.getSize()
                        },
                    }}
                >
                    {flexRender(
                        header.column.columnDef.header,
                        header.getContext()
                    )}
                    {formatHeaderResizer(header)}
                </th>
            ))}
        </tr>
    )
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
        enableResizing: true,
        size: 150  // TODO fit-content somehow or page layout save
    })))
    headers.push(ADD_HEADER)
    return headers
}


function Grid(props) {
    const [fieldOrder, setFieldOrder] = useState([])  // TODO for DnD
    const [data, setData] = useState(props.data)
    const [headers, setHeaders] = useState(formatHeaders(props.fields, props.context))  // TODO for Show/Hide

    const headerContextMenu = useContextMenu({id: "headerContextMenu"});

    const table = useReactTable({
        data: data,
        columns: headers,
        enableRowSelection: true,
        enableColumnResizing: true,
        columnResizeMode: 'onChange',
        columnResizeDirection: 'ltr',
        onColumnOrderChange: setFieldOrder,  // TODO
        getCoreRowModel: getCoreRowModel(),
        meta: {
            updateData: (rowIndex, columnId, value) => {
                setData(old =>
                    old.map((row, index) => {
                        if (index === rowIndex) {
                            return {
                            ...old[rowIndex],
                            [columnId]: value,
                            }
                        }
                        return row
                    })
                )
            },
          },
    })

    return (
        <div className="RG_GRID_BG">
            <table
                className="RG_GRID_TABLE"
                style={{
                    width: table.getCenterTotalSize()
                }}
            >
                <thead>
                    {table.getHeaderGroups().map(headerGroup => (
                        formatTableHeaderRow(headerGroup, props.context, headerContextMenu.show, props.setSelectedField)
                    ))}
                </thead>
                <tbody>
                    {table.getRowModel().rows.map(row => (
                        formatTableRow(row)
                    ))}
                </tbody>
            </table>
            {getHeaderContextMenu(props.showFieldEditWindow)}
        </div>
    )
}

export default Grid;