"""
MagPy
GSM 19 input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""


from core.magpy_stream import *
# instrument specific
from lib.format_gsm19 import *
from lib.format_didd import *
from lib.format_gdas import *
from lib.format_lemi import *
from lib.format_cr800 import *
# general purpose
from lib.format_iaga02 import *
from lib.format_wdc import *
from lib.format_magpy import *
# observatory/group specific
from lib.format_wik import *
from lib.format_wic import *
from lib.format_sfs import *
from lib.format_bdv import *
from lib.format_dtu import *


def isFormat(filename, format_type):
    if (format_type == "IAGA"):
        if (isIAGA(filename)):
            return True
    elif (format_type == "WDC"):
        if (isWDC(filename)):
            return True
    elif (format_type == "DIDD"):
        if (isDIDD(filename)):
            return True
    elif (format_type == "OPT"):
        if (isOPT(filename)):
            return True
    elif (format_type == "PMAG1"):
        if (isPMAG1(filename)):
            return True
    elif (format_type == "PMAG2"):
        if (isPMAG2(filename)):
            return True
    elif (format_type == "GDASA1"): # Data from the Conrad Observatory GDAS System
        if (isGDASA1(filename)):
            return True
    elif (format_type == "GDASB1"): # Data from the Conrad Observatory GDAS System
        if (isGDASB1(filename)):
            return True
    elif (format_type == "DTU1"): # ASCII Data from the DTU's FGE systems
        if (isDTU1(filename)):
            return True
    elif (format_type == "PYSTR"):
        if (isPYSTR(filename)):
            return True
    elif (format_type == "PYCDF"):
        if (isPYCDF(filename)):
            return True
    elif (format_type == "RMRCS"): # Data from the Conrad Observatory RCS System
        if (isRMRCS(filename)):
            return True
    elif (format_type == "CR800"): # Data from the CR800 datalogger
        if (isCR800(filename)):
            return True
    elif (format_type == "RADON"): # Data from the CR800 datalogger
        if (isRADON(filename)):
            return True
    elif (format_type == "USBLOG"):
        if (isUSBLOG(filename)):
            return True
    elif (format_type == "CS"):
        if (isCS(filename)):
            return True
    elif (format_type == "GSM19"):
        if (isGSM19(filename)):
            return True
    elif (format_type == "LEMIHF"): # High frequency Lemi data (10 Hz)
        if (isLEMIHF(filename)):
            return True
    elif (format_type == "SFDMI"): # San Fernando DMI(FGE) format
        if (isSFDMI(filename)):
            return True
    elif (format_type == "SFGSM"): # San Fernando GSM format
        if (isSFGSM(filename)):
            return True
    elif (format_type == "BDV1"): # Budkov format
        if (isBDV1(filename)):
            return True
    else:
        return False


def readFormat(filename, format_type, headonly=False, **kwargs):
    empty = DataStream()
    if (format_type == "IAGA"):
        return readIAGA(filename, headonly, **kwargs)
    elif (format_type == "WDC"):
        return readWDC(filename, headonly, **kwargs)
    elif (format_type == "DIDD"):
        return readDIDD(filename, headonly, **kwargs)
    elif (format_type == "GDASA1"):
        return readGDASA1(filename, headonly, **kwargs)
    elif (format_type == "GDASB1"):
        return readGDASB1(filename, headonly, **kwargs)
    elif (format_type == "RMRCS"):
        return readRMRCS(filename, headonly, **kwargs)
    elif (format_type == "PYSTR"):
        return readPYSTR(filename, headonly, **kwargs)
    elif (format_type == "PYCDF"):
        return readPYCDF(filename, headonly, **kwargs)
    elif (format_type == "GSM19"):
        return readGSM19(filename, headonly, **kwargs)
    elif (format_type == "LEMIHF"):
        return readLEMIHF(filename, headonly, **kwargs)
    elif (format_type == "USBLOG"):
        return readUSBLOG(filename, headonly, **kwargs)
    elif (format_type == "CR800"):
        return readCR800(filename, headonly, **kwargs)
    elif (format_type == "RADON"):
        return readRADON(filename, headonly, **kwargs)
    elif (format_type == "CS"):
        return readCS(filename, headonly, **kwargs)
    # Observatory specific
    elif (format_type == "OPT"):
        return readOPT(filename, headonly, **kwargs)
    elif (format_type == "PMAG1"):
        return readPMAG1(filename, headonly, **kwargs)
    elif (format_type == "PMAG2"):
        return readPMAG2(filename, headonly, **kwargs)
    elif (format_type == "DTU1"):
        return readDTU1(filename, headonly, **kwargs)
    elif (format_type == "SFDMI"):
        return readSFDMI(filename, headonly, **kwargs)
    elif (format_type == "SFGSM"):
        return readSFGSM(filename, headonly, **kwargs)
    elif (format_type == "BDV1"):
        return readBDV1(filename, headonly, **kwargs)
    else:
        return DataStream(empty,empty.header)


def writeFormat(datastream, filename, format_type, **kwargs):
    """
    calls the format specific write functions
    if the selceted dir is not existing, it is created
    """
    directory = os.path.dirname(filename)
    if not os.path.exists(directory):
        os.makedirs(os.path.normpath(directory))
    if (format_type == "IAGA"):
        return writeIAGA(datastream, filename, **kwargs)
    elif (format_type == "WDC"):
        return writeWDC(datastream, filename, **kwargs)
    elif (format_type == "DIDD"):
        return writeDIDD(datastream, filename, **kwargs)
    elif (format_type == "PMAG1"):
        return writePMAG1(datastream, filename, **kwargs)
    elif (format_type == "PMAG2"):
        return writePMAG2(datastream, filename, **kwargs)
    elif (format_type == "DTU1"):
        return writeDTU1(datastream, filename, **kwargs)
    elif (format_type == "GDASA1"):
        return writeGDASA1(datastream, filename, **kwargs)
    elif (format_type == "RMRCS"):
        return writeRMRCS(datastream, filename, **kwargs)
    elif (format_type == "PYSTR"):
        return writePYSTR(datastream, filename, **kwargs)
    elif (format_type == "PYCDF"):
        return writePYCDF(datastream, filename, **kwargs)
    elif (format_type == "USBLOG"):
        return writeUSBLOG(datastream, filename, **kwargs)
    elif (format_type == "CR800"):
        return writeCR800(datastream, filename, **kwargs)
    else:
        return "Writing not succesful - format not recognized"
