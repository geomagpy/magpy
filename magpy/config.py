#!/usr/bin/env python
"""Method for creating and/or reading a configuration file."""
#stdlib imports
import io
import json
import os


#global variables
CONFIG_DICTIONARY = None
DEFAULT_DIRECTORY = os.path.join(os.path.expanduser("~"), '.magpyFiles')
DEFAULT_FILE_NAME = "config.json"


def get_config(path=None):
    """Access configuration file for configurable variables.

    Parameters
    ----------
    path : string
        String representing path to a configuration file.

    Returns
    -------
    dictionary
        Dictionary of configurable variables.
    """
    global CONFIG_DICTIONARY
    if path is None:
        path = get_default_path()
    dictionary = {}
    if CONFIG_DICTIONARY is None:
        if not os.path.exists(path):
            create_default_config(path)
        with open(path, "r") as cfg:
            dictionary = json.load(cfg)
        CONFIG_DICTIONARY = dictionary
    else:
        dictionary = CONFIG_DICTIONARY
    return dictionary

def get_default_path():
    """Create a path to the user's home directory and specific MagPy file.

    Returns
    -------
    string
        String representing path to a configuration file.

    Notes
    -----
    Will read an environment variable called 'MAGPY_DEFAULT_CONFIG' which
        overrides defaults.
    """
    global DEFAULT_FILE_NAME
    global DEFAULT_DIRECTORY
    path = None
    if "MAGPY_DEFAULT_CONFIG" in os.environ:
        path = os.environ['MAGPY_DEFAULT_CONFIG']
    else:
        if not os.path.exists(DEFAULT_DIRECTORY):
            os.makedirs(DEFAULT_DIRECTORY)
        path = os.path.join(DEFAULT_DIRECTORY, DEFAULT_FILE_NAME)
    return path


def create_default_config(path):
    """Create default configuration file.

    Parameters
    ----------
    path : string
        String representing path to a configuration file.
    """
    with open(path, "w") as cfg:
        dictionary = {
            'droppedValue' : 99999,
            'logLocation' : os.path.dirname(path)
        }
        json.dump(dictionary, cfg, ensure_ascii=False)
