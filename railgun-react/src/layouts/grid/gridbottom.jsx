import { useRef } from "react"
import {STELLAR} from "../../STELLAR"
import "../../styles/gridbottom.css"


function PageSwitcher(props) {
    const pageInput = useRef()
    return (
        <div className="RG_PAGESWITCHER">
            <button disabled={props.page==1?true:false} className="RG_PAGESWITCHER_BUTTON" onClick={() => {
                // Back button shouldn't be visible anyway, but just in case
                if (props.page > 1) {
                    props.setPage(props.page-1)
                }
            }}>
                <svg className="RG_PAGESWITCHER_SVG" viewBox="0 0 24 24">
                    <path d="M15.41 16.09l-4.58-4.59 4.58-4.59L14 5.5l-6 6 6 6z" />
                </svg>
            </button>
            Page
            <input
                ref={pageInput}
                placeholder={props.page}
                className="RG_PAGESWITCHER_INPUT"
                name="pageselect"
                onKeyDown={(event) => {
                    if (event.key == "Enter") {
                        let requestedPage = parseInt(event.target.value)
                        if (isNaN(requestedPage)) {
                            // Nothing or not-a-number was actually entered.
                            return
                        } else if (requestedPage < 1) {
                            props.setPage(1)
                        } else if (requestedPage > props.totalpage) {
                            props.setPage(props.totalpage)
                        } else {
                            props.setPage(requestedPage)
                        }
                    }
                }}
            />
            / {props.totalpage}
            <button disabled={props.page==props.totalpage?true:false} className="RG_PAGESWITCHER_BUTTON" onClick={() => {
                // Forward button shouldn't be visible anyway, but just in case
                if (props.page < props.totalpage) {
                    props.setPage(props.page+1)
                    pageInput.current.value = ''
                }
            }}>
                <svg className="RG_PAGESWITCHER_SVG" viewBox="0 0 24 24">
                    <path d="M8.59 16.34l4.58-4.59-4.58-4.59L10 5.75l6 6-6 6z" />
                </svg>
            </button>
        </div>
    )
}

function GridBottom(props){
    // 100 and 99 are based on page size. Will need to be adjusted for variable page size.
    let start = ((props.page-1)*100)+1
    let end = start+99
    return (
        <div style={{...props.style}} className="RG_GRID_BOTTOM">
            <span />
            <div name="count">{start}-{props.count > end ? end : props.count} of {props.count||0} {STELLAR.entities[props.context.entity_type]? STELLAR.entities[props.context.entity_type].multiname : ""} </div>
            {props.count > 100 ? <PageSwitcher page={props.page} totalpage={Math.ceil(props.count / 100)} setPage={props.setPage} /> : <span />}
        </div>
    )
}


export {GridBottom}