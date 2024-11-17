from fastapi import Request, Depends, Cookie
from fastapi.responses import FileResponse, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm, APIKeyCookie

import json
from json.decoder import JSONDecodeError

# Imports for file upload management
import aiofiles
import re
from pathlib import Path
import tempfile

from src.railgun import Railgun
from src.modules import railsecure
from src.modules.railstatic import AuthStaticFiles

from pprint import pprint as pp

railgun_app = Railgun()
# Do some initial setup if needed
(Railgun.FILE_DIR / Railgun.FILE_TEMP_DIR).mkdir(parents=True, exist_ok=True)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login", auto_error=False)
cookieauth_scheme = APIKeyCookie(name="access_token", auto_error=False)

def authentication(access_token=Depends(cookieauth_scheme), token=Depends(oauth2_scheme)):
    """
    Both Authorization Header and cookie authentication are allowed, matching what is
    returned by a call to /login.
    Cookie is prefered when authenticating via web, Header is prefered when authenticating via API.
    """
    railsecure.authenticate_token(railgun_app, access_token or token)


@railgun_app.get("/heartbeat")
async def alive():#token=Depends(oauth2_scheme)):
    # TODO auth here?
    # railsecure.authenticate_token(railgun_app, token)
    return True


@railgun_app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):  # This is disgusting
    authentication = railsecure.authenticate_login(railgun_app, form_data)
    response = Response(json.dumps(authentication))
    response.set_cookie(
        key="access_token",
        value=authentication["access_token"],
        httponly=True,
        secure=True,
        samesite="none",
        max_age=railsecure.TOKENIZER_EXPIRATION_MINS*60,
        expires=railsecure.TOKENIZER_EXPIRATION_MINS*60
    )
    return response


@railgun_app.post("/create", dependencies=[Depends(authentication)])
async def create(request: Request):  # Typing... The root of all evil.
    try:
        request = await request.json()
        response = railgun_app.create(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/read", dependencies=[Depends(authentication)])
async def read(request: Request):  # Typing... The root of all evil.
    try:
        request = await railgun_app.validate_request(request)
        response = railgun_app.read(request)
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/update", dependencies=[Depends(authentication)])
async def update(request: Request):  # Typing... The root of all evil.
    try:
        request = await request.json()
        response = railgun_app.update(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/delete", dependencies=[Depends(authentication)])
async def delete(request: Request):  # Typing... The root of all evil.
    try:
        request = await request.json()
        response = railgun_app.delete(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/batch", dependencies=[Depends(authentication)])
async def delete(request: Request):  # Typing... The root of all evil.
    try:
        request = await request.json()
        response = railgun_app.batch(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/telescope", dependencies=[Depends(authentication)])
async def telescope(request: Request):  # Typing... The root of all evil.
    try:
        request = await railgun_app.validate_request(request)
        response = railgun_app.telescope(request)
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/stellar", dependencies=[Depends(authentication)])
async def stellar(request: Request):  # Typing... The root of all evil.
    try:
        request = await request.json()
        response = railgun_app.stellar(request)
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/upload", dependencies=[Depends(authentication)])
async def upload(request: Request):  # Typing... The root of all evil.
    pp(request.headers)
    boundary = ("--"+request.headers["content-type"].split("boundary=")[1]).encode('ascii')

    metadata = None
    filename = None
    writethis = False

    # HACK using a private function to generate the name is a bit cringe,
    # but it should guarentee we have a unique available temp file name
    temp_file_path = Railgun.FILE_DIR / Railgun.FILE_TEMP_DIR / ("RG_"+next(tempfile._get_candidate_names()))
    print(temp_file_path)

    async with aiofiles.open(temp_file_path, 'wb+') as tempwritefile:
        async for chunk in request.stream():
            if boundary in chunk:
                print(f"Found {chunk.count(boundary)} boundaries in chunk")
                # Multiple parts may arrive in a single str
                singleparts = chunk.split(boundary)
                # print(chunk[:400])
                for part in singleparts:
                    if not part:
                        # We split the OG boundary
                        continue

                    procpart = part.removeprefix(b'\r\n').removesuffix(b'\r\n')
                    print(procpart[:150])

                    if b'name="metadata"' in procpart:
                        # We can do this because json will ignore newlines wrapping the general data
                        metadata = json.loads(part.split(b'name="metadata"\r\n\r\n')[1])
                        print(metadata)
                    elif b'name="file"' in procpart:
                        # Write the actual chunk part to the file
                        filechunk = procpart.split(b'\r\n\r\n')
                        filename = re.split(b'filename="(.*)"', filechunk[0])[1]
                        print(filename)
                        pastmeta = b"".join(filechunk[1:])  # Just in case the file itself contains double return
                        await tempwritefile.write(pastmeta)
                        # We're in a file chunk now
                        writethis = True
                    elif writethis:
                        # This part is the last part of a data chunk
                        await tempwritefile.write(procpart)
                        # We'll need another file definition to start writing again
                        # We won't get one anyway. This is a single-file upload endpoint.
                        writethis=False
                    elif procpart == b'--':
                        # It's stellover. This is the end of the stream.
                        break
                    else:
                        # Invalid part recieved, fire 422
                        print(procpart[:150])
                        print(procpart[-150:])
                        return "422 Unprocessable entity"
            elif writethis:
                print("No boundary, writing chunk")
                await tempwritefile.write(chunk)
    
    print(metadata)
    print(filename)
    print(temp_file_path)
    try:
        return railgun_app.upload_file(temp_file_path, filename, metadata)
    except:
        # TODO
        raise
    finally:
        # Always try to remove the tempfile, just to be safe. Who knows when it may have failed.
        # TODO this should encapsulate the actual HTTP stream management too...
        temp_file_path.unlink(missing_ok=True)


@railgun_app.post("/download", dependencies=[Depends(authentication)])
async def download(request: Request):  # Typing... The root of all evil.
    try:
        req = await request.json()
        if "path" in req:
            filepath = (Railgun.FILE_DIR / Path(req["path"])).absolute().resolve()
            if Railgun.FILE_DIR not in filepath.parents:
                raise Exception()  # TODO
            print("Reading from path: %s" % str(filepath))
            assert filepath.exists()
            return FileResponse(filepath, filename=filepath.name)
        else:
            # TODO fetch file based on entity block w/ field given
            railgun_app.validate_request(request)
            raise NotImplementedError()
    except JSONDecodeError:
        return "Bad request"
    except:
        raise
        response = "Error"  # TODO
    return response

railgun_app.mount(
    "/discharge",
    AuthStaticFiles(railgun_app=railgun_app, directory=Railgun.FILE_DIR),
    name="discharge",
)
