"""
This is really just a convenience wrapper to keep auth stuff self-contained.
It could probably be reimagined as another stellar-like "special" DB connector,
but it seems excessive given how simple the operations are.

BUG There is currently a major flaw in that user logins are not enforced to be
unique (in the running prod instance).
"""
from datetime import datetime, timedelta, timezone

import bcrypt
from fastapi import HTTPException

import jwt
from jwt import InvalidTokenError

from config import CONFIG


async def authenticate_login(railgun_app, form_data):
    """
    Run through the steps of authenticating a new login fetching a new token.
    Check if user exists. TODO check user status?
    Compare provided password to stored hash.
    Generate token.

    :param railgun.Railgun railgun_app: the Railgun app, used to fetch users.
    :param fastapi.security.OAuth2PasswordRequestForm form_data: the authentication request per OAuth2 standards.

    :returns: access token
    :rtype: dict

    :raises HTTPException: 401 if unauthorized
    :raises HTTPException: 500 if internal error
    """
    try:
        requested_user = form_data.username  # this is actually the login
        existing_user = await railgun_app.STELLAR.fetch_user(requested_user)
        if not existing_user:
            raise NoSuchUserException()
        # Check password match
        if not _compare_passwords(form_data.password, existing_user["password"]):
            raise BadPasswordException()
        token = _generate_token(railgun_app, existing_user)
    except (NoSuchUserException, BadPasswordException):
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    # except:  # TODO this distinction only exists for now to help debug
    #     raise HTTPException(status_code=500, detail="Invalid Login Attempt")
    # Here's your token
    return {"access_token": token, "token_type": "bearer"}


async def authenticate_token(railgun_app, incoming_token):
    """
    Run through the steps of authenticating the access token provided in a request.

    :param railgun.Railgun railgun_app: the Railgun app, used to fetch users.
    :param str incoming_token: auth token provided in the request
    """
    try:
        detokened = jwt.decode(incoming_token, CONFIG.TOKENIZER_KEY, algorithms=[CONFIG.TOKENIZER_ALGO])
        requested_login = detokened["sub"]
        if railgun_app.STELLAR.USER_CACHE[requested_login].invalid_before > detokened["exp"]:
            # All tokens with an expiration before the invalidation time should be considered revoked.
            # If the user isn't cached, they've been removed and a KeyError will trigger.
            raise InvalidTokenError()
        return railgun_app.STELLAR.USER_CACHE[requested_login].permission_groups
    except (InvalidTokenError, KeyError):
        raise HTTPException(
            status_code=405,
            detail="Access token expired. Please log in again.",
            headers={"WWW-Authenticate": "Bearer"}
        )


def _compare_passwords(given_password, expected_hash):
    """
    Stab the given password and see if the wounds match.

    :returns: if the password hashes match
    :rtype: bool
    """
    return bcrypt.checkpw(
        given_password.encode(),
        expected_hash.encode()
    )


def _generate_token(railgun_app, user):
    """
    Generate a token with the user (provided param) and expiration time.
    Would add scope here, but scope will not be used :fingers_crossed:

    :param dict to_encode: {"user": <username>}

    :returns: bearer token
    :rtype: str
    """
    to_encode = {
        "iss": f"Railgun Server {railgun_app.STELLAR.COMET_ID}",
        "sub": user["login"],
        "exp": datetime.now(timezone.utc) + timedelta(minutes=CONFIG.TOKENIZER_EXPIRATION_MINS)
    }
    return jwt.encode(to_encode, CONFIG.TOKENIZER_KEY, algorithm=CONFIG.TOKENIZER_ALGO)


class NoSuchUserException(Exception):
    """User does not exist."""

class BadPasswordException(Exception):
    """User entered incorrect password... Sus amogus."""
