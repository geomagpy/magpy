"""
MagPy
GSM 19 input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *
# absolute data / DI
from lib.format_abs_magpy import *

def isAbsFormat(filename, format_type):
    if (format_type == "MAGPYABS"):
        if (isMAGPYABS(filename)):
            return True
    else:
        return False

def readAbsFormat(filename, format_type, headonly=False, **kwargs):
    if (format_type == "MAGPYABS"):
        return readMAGPYABS(filename, headonly, **kwargs)
    else:
        return AbsoluteData([],{})


