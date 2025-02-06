"""
MagPy
Absolute data summary
Written by Roman Leonhardt June 2012
"""

from magpy.stream import *
# absolute data / DI
from magpy.lib.format_abs_magpy import *
from magpy.lib import format_autodif as autodif

def isAbsFormat(filename, format_type):
    if (format_type == "MAGPYABS"):
        if (isMAGPYABS(filename)):
            return True
    elif (format_type == "MAGPYNEWABS"):
        if (isMAGPYNEWABS(filename)):
            return True
    elif (format_type == "AUTODIFABS"):
        if (isAUTODIFABS(filename)):
            return True
    elif (format_type == "JSONABS"):
        if (isJSONABS(filename)):
            return True
    elif (format_type == "AUTODIF"):
        if (isAUTODIFRAW(filename)):
            return True
    else:
        return False


def readAbsFormat(filename, format_type, headonly=False, **kwargs):
    output = kwargs.get('output')
    if (format_type == "MAGPYABS"):
        return readMAGPYABS(filename, headonly, **kwargs)
    elif (format_type == "MAGPYNEWABS"):
        return readMAGPYNEWABS(filename, headonly, **kwargs)
    elif (format_type == "AUTODIF"):
        return readAUTODIF(filename, headonly, **kwargs)
    elif (format_type == "JSONABS"):
        return readJSONABS(filename, headonly, **kwargs)
    elif (format_type == "AUTODIFABS"):
        return  autodif.readAUTODIFABS(filename, headonly,scaleangle=20, **kwargs)

    else:
        return AbsoluteData([],{})
