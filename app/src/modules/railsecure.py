"""
This is really just a convenience wrapper to keep auth stuff self-contained.
It could probably be reimagined as another stellar-like "special" DB connector,
but it seems excessive given how simple the operations are.
"""
import os
from datetime import datetime, timedelta

import bcrypt
from fastapi import HTTPException

import jwt
from jwt import InvalidTokenError

############################################
######### Read consistant auth key #########
############################################
### This key needs to be consistent ########
### accross deployments or else ############
### authentication tokens will randomly ####
### be invalid when the user connects to ###
### a new host container. ##################
############################################
### Big TODO to make this smart. ###########
############################################
DEFAULT_SECRET_PATH = "/opt/railgun/config/auth.secret"
with open(os.environ.get("RG_SECRET_PATH") or DEFAULT_SECRET_PATH, "r") as authfile:
    TOKENIZER_KEY = authfile.read()
TOKENIZER_ALGO = "HS256"
TOKENIZER_EXPIRATION_MINS = 1440  # 24 hours


def authenticate_login(railgun_app, form_data):
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
        requested_user = form_data.username
        existing_user = _get_user_exists(railgun_app, requested_user)
        # Not sure about multiple users, TODO
        if not existing_user or len(existing_user)>1:
            raise NoSuchUserException()
        existing_user = existing_user[0]
        # Check password match
        if not _compare_passwords(form_data.password, existing_user["password"]):
            raise BadPasswordException()
        # Pop the password so we dont carry it around in the token.
        existing_user.pop("password")
        token = _generate_token({"user": existing_user})
    except (NoSuchUserException, BadPasswordException):
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except:  # TODO this distinction only exists for now to help debug
        raise HTTPException(status_code=500, detail="Invalid Login Attempt")
    # Here's your token
    return {"access_token": token, "token_type": "bearer"}


def authenticate_token(railgun_app, incoming_token):
    """
    Run through the steps of authenticating the access token provided in a request.

    :param railgun.Railgun railgun_app: the Railgun app, used to fetch users.
    :param str incoming_token: auth token provided in the request
    """
    try:
        detokened = jwt.decode(incoming_token, TOKENIZER_KEY, algorithms=[TOKENIZER_ALGO])
        # Maybe user has been removed since token generation... (or disabled, TODO)
        # This has been commented out 'cause I don't this method.
        # if not _get_user_exists(railgun_app, detokened.get('user', {}).get('username')):
        #     raise InvalidTokenError()
    except InvalidTokenError:
        raise HTTPException(
            status_code=405,
            detail="Access token expired. Please log in again.",
            headers={"WWW-Authenticate": "Bearer"}
        )
    


def _get_user_exists(railgun_app, requested_user):
    """
    Fetch any users with the requested username, and get their (salted) password.
    """
    return railgun_app.read({
        "schema": "railgun_internal",
        "entity": "User",
        "read": {
            "filters": {"filters": [["login", "is", requested_user]], "filter_operator": "AND"},
            "return_fields": ["username", "password"],
            "pagination": 1
        }
    })


def _compare_passwords(given_password, expected_hash):
    """
    Stab the given password and see if the wounds match.
    """
    return bcrypt.checkpw(
        given_password.encode(),
        expected_hash.encode()
    )


def _generate_token(to_encode):
    """
    Generate a token with the user (provided param) and expiration time.
    Would add scope here, but scope will not be used :fingers_crossed:

    :param dict to_encode: {"user": <username>}

    :returns: bearer token
    :rtype: str
    """
    expires_delta = timedelta(minutes=TOKENIZER_EXPIRATION_MINS)
    to_encode["exp"] = datetime.now() + expires_delta  # TODO timezone management pepehands
    return jwt.encode(to_encode, TOKENIZER_KEY, algorithm=TOKENIZER_ALGO)


class NoSuchUserException(Exception):
    """User does not exist."""

class BadPasswordException(Exception):
    """User entered incorrect password... Sus amogus."""