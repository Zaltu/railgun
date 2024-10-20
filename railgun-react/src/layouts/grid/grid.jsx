import {useState} from "react";
import {useReactTable, getCoreRowModel, flexRender} from '@tanstack/react-table';
import MenuList from '@mui/material/MenuList';
import MenuItem from '@mui/material/MenuItem';
import { ClickAwayListener } from "@mui/base/ClickAwayListener";
import {STELLAR, batchRGData} from '/src/STELLAR.jsx';

import '/src/styles/grid.css'



function TableHeaderRow (props) {
    return (
        <tr>
            {props.headerGroup.headers.map(header => (
                <TableHeaderCell key={header.id} header={header} displayHeaderContext={props.displayHeaderContext} />
            ) )}
        </tr>
    )
}

function TableHeaderCell(props) {
    return (
        <th
            onContextMenu={(event) => props.displayHeaderContext(event, props.header)}
            // key={props.header.id}
            {...{
                className: "RG_GRID_HEADER",
                style: {
                    width: props.header.column.columnDef.enableResizing ? props.header.getSize() : 'fit-content',
                    minWidth: props.header.column.columnDef.enableResizing ? props.header.getSize() : 'fit-content',//header.getSize(),
                    maxWidth: props.header.column.columnDef.enableResizing ? props.header.getSize() : 100000
                },
            }}
        >
            {flexRender(
                props.header.column.columnDef.header,
                props.header.getContext()
            )}
            <HeaderResizer header={props.header} />
        </th>
    )
}

function TableRow (props) {
    return (
        <tr
            // key={props.row.id}
            className={props.row.getIsSelected() ? "RG_GRID_ROW_SELECT" : "RG_GRID_ROW"}
            onClick={(e) => {
                if (props.row.getIsSelected()) {return}
                //props.row.getAllCells()[0].getContext().table.resetRowSelection()  // HACK - ensures only one selectable on click
                props.row.getToggleSelectedHandler()(e)
            }}
            onContextMenu={(e)=>{
                e.preventDefault()  // TODO Delete context menu...
                if (!props.row.getIsSelected()) {
                    props.row.getAllCells()[0].getContext().table.resetRowSelection()  // HACK - ensures only one selectable on click
                    props.row.getToggleSelectedHandler()(e)
                }
                props.displayEntityContext(e)
            }}
        >
            {props.row.getVisibleCells().map(cell => (
                <TableCell key={cell.id} cell={cell} />
            ))}
        </tr>
    )
}

function TableCell(props) {
    return (
        <td
            className={"RG_GRID_CELL"}
            style={{
                width: props.cell.column.columnDef.enableResizing ? '' : 'fit-content',
                maxWidth: props.cell.column.columnDef.enableResizing ? 0 : ''
            }}
            // key={props.cell.id}
        >
            {flexRender(props.cell.column.columnDef.cell, props.cell.getContext())}
        </td>
    )
}

function HeaderResizer(props) {
    return (
        <div
            key={String(props.header.id)+"_resize"}
            {...{
            onMouseDown:  (event) => {
                // TODO YABAI
                if (!props.header.column.columnDef.enableResizing){
                    props.header.column.columnDef.size = event.target.parentNode.offsetWidth-6.5
                    props.header.column.columnDef.enableResizing = true
                }
                props.header.getResizeHandler()(event)
            },
            className: `
                resizer
                ${props.header.column.getIsResizing() ? 'isResizing' : ''}
            `,
            }}
        />
    )
}


function HeaderContextMenu(props) {
    return (
        <ClickAwayListener
            mouseEvent="onMouseDown"
            onClickAway={()=>props.setContextMenu(null)}
        >
            <MenuList
                id="headerContextMenu"
                sx={{
                    position: "absolute",
                    top: props.contextMenu.mouseY,
                    left: props.contextMenu.mouseX,
                    zIndex: "1300",
                    color: "var(--RG_TEXT_GREY)",
                    backgroundColor: "var(--RG_ITEM_GREY)",
                    padding: 0,
                    boxShadow: "0px 0px 4px 0px black",
                    "& .MuiMenuItem-root:hover": {
                        backgroundColor: "var(--RG_HIGHLIGHT_GREY)"
                    }
                }}
            >
                <MenuItem onClick={() => {props.showFieldEditWindow(true);props.setContextMenu(null)}}>Edit Field</MenuItem>
                <MenuItem onClick={() => console.log("Hide Field")}>Hide Field</MenuItem>
            </MenuList>
        </ClickAwayListener>
    )
}

function EntityContextMenu(props) {
    return (
        <ClickAwayListener 
            mouseEvent="onMouseDown"
            onClickAway={()=>props.setContextMenu(null)}
        >
            <MenuList
                id="entityContextMenu"
                sx={{
                    position: "absolute",
                    top: props.contextMenu.mouseY,
                    left: props.contextMenu.mouseX,
                    zIndex: "1300",
                    // color: "var(--RG_TEXT_GREY)",
                    backgroundColor: "var(--RG_ITEM_GREY)",
                    padding: 0,
                    boxShadow: "0px 0px 4px 0px black",
                    "& .MuiMenuItem-root:hover": {
                        backgroundColor: "var(--RG_HIGHLIGHT_GREY)"
                    }
                }}
            >
                <MenuItem sx={{color:"#ec4a41"}} onClick={() => {console.log("DELETING LMAO");props.batchDeleteRG();props.setContextMenu(null)}}>DELETE THIS</MenuItem>
            </MenuList>
        </ClickAwayListener>
    )
}


function setHeaderContextMenuOpenPosition(event, context, header, setHeaderContextMenu, setSelectedField) {
    event.preventDefault()
    setSelectedField(STELLAR.entities[context.entity_type].fields[header.id])
    setHeaderContextMenu({
            mouseX: event.clientX,
            mouseY: event.clientY
    })
}

function setEntityContextMenuOpenPosition(event, setEntityContextMenu) {
    setEntityContextMenu({
        mouseX: event.clientX,
        mouseY: event.clientY
    })
}


async function batchDeleteRG(context, getSelected, updateData) {
    // batch for general multi-select delete
    let batchData = getSelected().rows.map((row) => {
        return {
            request_type: "delete",
            entity: context.entity_type,
            entity_id: row.original.uid
        }
    })
    let DELETE_REQUEST = {
        schema: context.schema,
        batch: batchData
    }
    console.log(DELETE_REQUEST)
    let finished = await batchRGData(DELETE_REQUEST)
    if (finished){
        updateData()
    } else {
        console.error(response)
    }
}


function Grid(props) {
    const [fieldOrder, setFieldOrder] = useState([])  // TODO for DnD

    const [headerContextMenu, setHeaderContextMenu] = useState(null)
    const [entityContextMenu, setEntityContextMenu] = useState(null)

    const table = useReactTable({
        data: props.data,
        columns: props.headers,
        enableRowSelection: true,
        enableColumnResizing: true,
        columnResizeMode: 'onChange',
        columnResizeDirection: 'ltr',
        onColumnOrderChange: setFieldOrder,  // TODO
        getCoreRowModel: getCoreRowModel(),
        meta: {
            updateData: (rowIndex, columnId, value) => {
                props.setData(old =>
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
    props.tableref.current = table  // HACK

    return (
        <div style={{...props.style}} className="RG_GRID_BG">
            <table
                className="RG_GRID_TABLE"
                style={{
                    width: table.getCenterTotalSize()
                }}
            >
                <thead>
                    {table.getHeaderGroups().map(headerGroup => (
                        <TableHeaderRow key={headerGroup.id} headerGroup={headerGroup} displayHeaderContext={(event, header) => setHeaderContextMenuOpenPosition(event, props.context, header, setHeaderContextMenu, props.setSelectedField)} />
                    ))}
                </thead>
                <tbody>
                    {table.getRowModel().rows.map(row => (
                        <TableRow key={row.id} row={row} displayEntityContext={(event) => setEntityContextMenuOpenPosition(event, setEntityContextMenu)} />
                    ))}
                </tbody>
            </table>
            {headerContextMenu ? 
                <HeaderContextMenu contextMenu={headerContextMenu} setContextMenu={setHeaderContextMenu} showFieldEditWindow={props.showFieldEditWindow}/>
            :null}
            {entityContextMenu ? 
                <EntityContextMenu contextMenu={entityContextMenu} setContextMenu={setEntityContextMenu} batchDeleteRG={()=>batchDeleteRG(props.context, table.getSelectedRowModel, props.updateData)} />
            :null}
        </div>
    )
}

export default Grid;