import uvicorn
from fastapi import Request
from pprint import pprint as pp
from json.decoder import JSONDecodeError

from src.railgun import Railgun


railgun_app = Railgun()


@railgun_app.get("/heartbeat")
async def alive():
    return True


@railgun_app.post("/create")
async def create(request: Request):  # Typing... The root of all evil.
    """
    Creates a record according to specifications.
    TODO
    """
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
async def read(request: Request):  # Typing... The root of all evil.
    try:
        request = await railgun_app.validate_request(request)
        response = railgun_app.read(request)
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/update")
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


@railgun_app.post("/delete")
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


@railgun_app.post("/batch")
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


@railgun_app.post("/telescope")
async def telescope(request: Request):  # Typing... The root of all evil.
    try:
        request = await railgun_app.validate_request(request)
        response = railgun_app.telescope(request)
    except:
        raise
        response = "Error"  # TODO
    return response


@railgun_app.post("/stellar")
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


@railgun_app.post("/api")
async def api(request: Request):  # Typing... The root of all evil.
    return "NYI"


if __name__ == "__main__":
    uvicorn.run(
        "main:railgun_app",
        port=8888,
        log_level="debug"
    )