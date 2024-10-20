import React, { useState } from 'react'
import ReactDOM from 'react-dom/client'

import '/src/styles/CONSTANTS.css'
import '/src/styles/component_styles.css'
import './login.css'
import { login } from '/src/STELLAR.jsx'


async function attemptLogin(event, setDisplayFailed) {
    event.preventDefault()
    if (await login(new FormData(event.currentTarget))) {
        console.log("Login successfull!")
        location.href = "/"
    }
    else {
        console.log("Login failed...")
        setDisplayFailed(true)
    }
}

function LoginLayout() {
    const [displayFailed, setDisplayFailed] = useState(false)
    return (
        <div className='RG_LOGINPAGE'>
            <div className='RG_LOGINBLOCK'>
                <div className='RG_LOGINLOGO'>
                    <img src='/src/assets/railguntemplogo.png'/>
                </div>
                <div>
                    <form className='RG_LOGINAUTH' autoComplete='off' onSubmit={(event) => attemptLogin(event, setDisplayFailed)}>
                        <div style={{display:"flex", flexDirection:"column"}}>
                            <label style={{fontSize:'small'}}>Username</label>
                            <input name="username" className='RG_INDEPENDENT_TEXTINPUT'></input>
                        </div>
                        <div style={{display:"flex", flexDirection:"column"}}>
                            <label style={{fontSize:'small'}}>Password</label>
                            <input name="password" className='RG_INDEPENDENT_TEXTINPUT' type="password"></input>
                        </div>
                        <button className='RG_HIGHLIGHT_BUTTON' style={{fontSize:"18px", height:'2.5rem'}}>Sign In</button>
                        {displayFailed ? (<span style={{alignSelf:"center", color:"red"}}>
                            Incorrect username or password
                        </span>):(null)}
                    </form>
                </div>
            </div>
        </div>
    )
}


async function setup() {
    // Put redirected from logic here
    // let pathchunks = new URL(window.location).pathname.split("/").filter(e => e)
    // const SCHEMA = pathchunks[0] || DEFAULT_SCHEMA
    // const ENTITY_TYPE = pathchunks[1] || DEFAULT_ENTITY_TYPE

    
    ReactDOM.createRoot(document.getElementById('root')).render(
        <React.StrictMode>
            <LoginLayout/>
        </React.StrictMode>,
    )
}
setup()
