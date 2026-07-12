"""
Configuration management for Railgun. Defines constants to be used based on environment variables
and config files.
"""
import os
from uuid import uuid4
from pathlib import Path


_DEFAULT_CONFIG_PATH = "/opt/railgun/config/db_secrets/"
_DEFAULT_FILE_DIR = "/opt/railgun/files"
_DEFAULT_SECRET_PATH = "/opt/railgun/config/auth.secret"

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
with open(os.environ.get("RG_SECRET_PATH") or _DEFAULT_SECRET_PATH, "r") as authfile:
    _TOKENIZER_KEY = authfile.read()

class CONFIG():
    """
    Simple namespace class for convenience. Holds configuration values.
    """
    FILE_DIR = Path(os.environ.get("RG_FILE_DIR") or _DEFAULT_FILE_DIR)
    FILE_TEMP_DIR = FILE_DIR / "_ss_working"

    RG_URLS = os.environ["RG_URL"].split(",")

    DB_CONFIG_PATH = Path(os.environ.get("RG_CONFIG_PATH") or _DEFAULT_CONFIG_PATH)

    TOKENIZER_KEY = _TOKENIZER_KEY
    TOKENIZER_ALGO = "HS256"
    TOKENIZER_EXPIRATION_MINS = 1440  # 24 hours


    ### Stuff that doesn't change ###
    # railgun_internal DB
    RAILGUN_DB_NAME = "railgun_internal"
    RAILGUN_DB_USER = "railgun"
    RAILGUN_DB_HOST = "stellardb"
    RAILGUN_DB_PORT = 5432
    # Redis
    COMET_HOST = "stellar"
    COMET_PORT = 6379
    COMET_DB = 0
    COMET_NAME = "STELLAR"  # Redis channel name
    COMET_ID = str(uuid4())  # To uniquely identify this instance
