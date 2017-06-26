#!/usr/bin/env python
import os
import json
import io

config_file_name = "config.json"

from os.path import expanduser
home = expanduser("~")
directory = os.path.join(home, '.magpyFiles')

if not os.path.exists(directory):
    os.makedirs(directory)

path_to_config = os.path.join(directory, config_file_name)
if not os.path.exists(path_to_config):
    with open(path_to_config, "w") as cfg:
        data = {}
        data['droppedValue'] = 99999
        json.dump(data, cfg, ensure_ascii=False)
    with open(path_to_config, "r") as cfg:
        data = json.load(cfg)
else:
    with open(path_to_config, "r") as cfg:
        data = json.load(cfg)
