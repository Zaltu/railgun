import uvicorn
from fastapi import Request, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pprint import pprint as pp
from json.decoder import JSONDecodeError

from src.railgun import Railgun
from src import railsecure


railgun_app = Railgun()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login", auto_error=False)

@railgun_app.get("/heartbeat")
async def alive():#token=Depends(oauth2_scheme)):
    # TODO auth here?
    # railsecure.authenticate_token(railgun_app, token)
    return True


@railgun_app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):  # This is disgusting
    return railsecure.authenticate_login(railgun_app, form_data)


@railgun_app.post("/create")
async def create(request: Request, token=Depends(oauth2_scheme)):  # Typing... The root of all evil.
    railsecure.authenticate_token(railgun_app, token)
    try:
        request = await request.json()
        response = railgun_app.create(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/read")
async def read(request: Request, token=Depends(oauth2_scheme)):  # Typing... The root of all evil.
    railsecure.authenticate_token(railgun_app, token)
    try:
        request = await railgun_app.validate_request(request)
        response = railgun_app.read(request)
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/update")
async def update(request: Request, token=Depends(oauth2_scheme)):  # Typing... The root of all evil.
    railsecure.authenticate_token(railgun_app, token)
    try:
        request = await request.json()
        response = railgun_app.update(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/delete")
async def delete(request: Request, token=Depends(oauth2_scheme)):  # Typing... The root of all evil.
    railsecure.authenticate_token(railgun_app, token)
    try:
        request = await request.json()
        response = railgun_app.delete(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/batch")
async def delete(request: Request, token=Depends(oauth2_scheme)):  # Typing... The root of all evil.
    railsecure.authenticate_token(railgun_app, token)
    try:
        request = await request.json()
        response = railgun_app.batch(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/telescope")
async def telescope(request: Request, token=Depends(oauth2_scheme)):  # Typing... The root of all evil.
    railsecure.authenticate_token(railgun_app, token)
    try:
        request = await railgun_app.validate_request(request)
        response = railgun_app.telescope(request)
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/stellar")
async def stellar(request: Request, token=Depends(oauth2_scheme)):  # Typing... The root of all evil.
    railsecure.authenticate_token(railgun_app, token)
    try:
        request = await request.json()
        response = railgun_app.stellar(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/api")
async def api(request: Request, token=Depends(oauth2_scheme)):  # Typing... The root of all evil.
    railsecure.authenticate_token(railgun_app, token)
    return "NYI"


if __name__ == "__main__":
    uvicorn.run(
        "main:railgun_app",
        port=8888,
        host='metis',  # DevEnv
        log_level="debug"
    )