#!/usr/bin/env python
import os
import json
import io

config_dictionary = None


def get_config(path=None):
    if path is None:
        home = os.path.expanduser("~")
        directory = os.path.join(home, '.magpyFiles')
        CONFIG_FILE_NAME = "config.json"
        default_path = os.path.join(directory, CONFIG_FILE_NAME)
        if not os.path.exists(directory):
            os.makedirs(directory)
        path = default_path

    dictionary = {}
    global config_dictionary
    if config_dictionary is None:
        if not os.path.exists(path):
            with open(path, "w") as cfg:
                dictionary = {
                    'droppedValue' : 99999,
                    'logLocation' : directory
                }
                json.dump(dictionary, cfg, ensure_ascii=False)
            with open(path, "r") as cfg:
                dictionary = json.load(cfg)
        else:
            with open(path, "r") as cfg:
                dictionary = json.load(cfg)
        config_dictionary = dictionary
    else:
        dictionary = config_dictionary

    return dictionary
