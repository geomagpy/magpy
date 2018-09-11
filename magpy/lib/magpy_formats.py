'''
Path:                   magpy.lib.magpy_formats
Part of package:        stream (read/write)
Type:                   Input tester, call to read library

PURPOSE:
        Tests which format a file has using the read library.

CONTAINS:
        isFormat:       (Func) Runs through datatypes in library to find fitting type.
        readFormat:     (Func) When format is found, reads data file into DataStream.
        writeFormat:    (Func) Writes DataStream object to given format.

DEPENDENCIES:
        magpy.lib...
                .format_gsm19
                .format_didd
                .format_gdas
                .format_lemi
                .format_pos1
                #.format_env05
                .format_cr800
                .format_iono
                .format_iaga02
                .format_wdc
                .format_magpy
                .format_noaa
                .format_latex
                .format_wik
                .format_wic
                .format_sfs
                .format_bdv
                .format_dtu
                .format_gfz
                .format_imf
                .format_rcs
                .format_json
                .format_pha

CALLED BY:
        magpy.stream.read()
        magpy.stream.write()
'''
from __future__ import print_function

from magpy.stream import *

import logging
logger = logging.getLogger(__name__)

# IMPORT INSTRUMENT SPECIFIC DATA FORMATS:
from magpy.lib.format_gsm19 import *
from magpy.lib.format_didd import *
from magpy.lib.format_gdas import *
from magpy.lib.format_lemi import *
from magpy.lib.format_pos1 import *
from magpy.lib.format_qspin import *
#from magpy.lib.format_env05 import *
from magpy.lib.format_cr800 import *
from magpy.lib.format_iono import *

# IMPORT GENERAL PURPOSE FORMATS:
from magpy.lib.format_iaga02 import *
from magpy.lib.format_wdc import *
from magpy.lib.format_magpy import *
from magpy.lib.format_noaa import *
from magpy.lib.format_nc import isNETCDF, readNETCDF
from magpy.lib.format_latex import *
from magpy.lib.format_json import *

# IMPORT OBSERVATORY/GROUP SPECIFIC FORMATS:
from magpy.lib.format_wik import *
from magpy.lib.format_wic import *
from magpy.lib.format_sfs import *
from magpy.lib.format_bdv import *
from magpy.lib.format_dtu import *
from magpy.lib.format_gfz import *
from magpy.lib.format_neic import *
from magpy.lib.format_rcs import *
from magpy.lib.format_pha import *

from magpy.lib.format_imf import *
try:
    from magpy.lib.format_autodif_fread import *
except:
    logging.warning("magpy-formats: Format package autodif-F not available")

IAFMETA = {'StationInstitution':'word', 'StationName':'word', 'StationIAGAcode':'word', 'DataAcquisitionLatitude':'word', 'DataAcquisitionLongitude':'word', 'DataElevation':'word', 'DataFormat':'word', 'DataComponents':'word', 'DataSensorOrientation':'word', 'DataDigitalSampling':'word', 'DataSamplingFilter':'word', 'Data Type':'word', 'DataPublicationLevel':'word', 'DataConversion':'word', 'StationK9':'word', 'DataQuality':'word', 'SensorType':'word', 'StationStreet':'word', 'StationCity':'word', 'StationPostalCode':'word', 'StationCountry':'word', 'StationWebInfo':'word', 'StationEmail':'word'}

IAFBINMETA = {'StationInstitution':'word', 'DataAcquisitionLatitude':'word', 'DataAcquisitionLongitude':'word', 'DataElevation':'word', 'DataComponents':'word', 'DataSensorOrientation':'word', 'DataDigitalSampling':'word', 'DataConversion':'word', 'StationK9':'word', 'DataQuality':'word', 'SensorType':'word', 'StationID':'word'}


IAGAMETA = {'StationInstitution':'word', 'StationName':'word', 'StationIAGAcode':'word', 'DataAcquisitionLatitude':'word', 'DataAcquisitionLongitude':'word', 'DataElevation':'word', 'DataFormat':'word', 'DataComponents':'word', 'DataSensorOrientation':'word', 'DataDigitalSampling':'word', 'DataSamplingFilter':'word', 'DataPublicationLevel':'word'}

IMAGCDFMETA = {'StationInstitution':'word', 'DataPublicationLevel':'number', 'DataStandardLevel':'word', 'StationIAGAcode':'word', 'StationName':'word', 'StationInstitution':'word', 'DataReferences':'word', 'DataTerms':'word', 'DataAcquisitionLatitude':'word', 'DataAcquisitionLongitude':'word', 'DataElevation':'word', 'DataComponents':'word', 'DataSensorOrientation':'word'}


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
    elif (format_type == "PMAG1"): # Data from the ELSEC820 System
        if (isPMAG1(filename)):
            return True
    elif (format_type == "PMAG2"): # Data from the ELSEC820 System via Cobenzl RCS
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
    elif (format_type == "PYASCII"):
        if (isPYASCII(filename)):
            return True
    elif (format_type == "PYCDF"):
        if (isPYCDF(filename)):
            return True
    elif (format_type == "PYBIN"):
        if (isPYBIN(filename)):
            return True
    elif (format_type == "JSON"):
        if (isJSON(filename)):
            return True
    elif (format_type == "RMRCS"): # Data from the Conrad Observatory RCS System
        if (isRMRCS(filename)):
            return True
    elif (format_type == "RCS"): # Direct data from the Conrad Observatory RCS System
        if (isRCS(filename)):
            return True
    elif (format_type == "METEO"): # Conrad Observatory RCS System - METEO files
        if (isMETEO(filename)):
            return True
    elif (format_type == "LNM"): # Conrad Observatory LaserNiederschlagsMonitor - LNM Telegram 5 files
        if (isLNM(filename)):
            return True
    elif (format_type == "GRAVSG"): # Data from the Conrad Observatory SG gravity system
        if (isGRAVSG(filename)):
            return True
    elif (format_type == "IWT"): # Data from the Conrad Observatory tiltmeter system
        if (isIWT(filename)):
            return True
    elif (format_type == "LIPPGRAV"): # Data from the Lippmann tiltmeter system
        if (isLIPPGRAV(filename)):
            return True
    elif (format_type == "CR800"): # Data from the CR800 datalogger
        if (isCR800(filename)):
            return True
    elif (format_type == "IONO"): # Data from the IM806 Ionometer
        if (isIONO(filename)):
            return True
    elif (format_type == "RADON"): # Data from the CR800 datalogger
        if (isRADON(filename)):
            return True
    elif (format_type == "CS"):
        if (isCS(filename)):
            return True
    elif (format_type == "GSM19"): # Data from the GEM GSM 19 Overhauzer sensor
        if (isGSM19(filename)):
            return True
    elif (format_type == "LEMIHF"): # High frequency Lemi data (10 Hz)
        if (isLEMIHF(filename)):
            return True
    elif (format_type == "LEMIBIN"): # Binary Lemi data (10 Hz)
        if (isLEMIBIN(filename)):
            return True
    elif (format_type == "LEMIBIN1"): # Binary Lemi data (10 Hz)
        if (isLEMIBIN1(filename)):
            return True
    elif (format_type == "POS1"): # Binary POS1 data (0.2 Hz)
        if (isPOS1(filename)):
            return True
    elif (format_type == "POS1TXT"): # Text POS1 data (0.2 Hz)
        if (isPOS1TXT(filename)):
            return True
    elif (format_type == "PMB"): # POS PMB data
        if (isPOSPMB(filename)):
            return True
    elif (format_type == "IAF"): # Intermagnet Archive Format
        if (isIAF(filename)):
            return True
    elif (format_type == "IYFV"): # Intermagnet Yearly mean Format
        if (isIYFV(filename)):
            return True
    elif (format_type == "DKA"): # Intermagnet K-value Format
        if (isDKA(filename)):
            return True
    elif (format_type == "IMAGCDF"): # Intermagnet CDF Format
        if (isIMAGCDF(filename)):
            return True
    elif (format_type == "IMF"): # Intermagnet v1.22,v1.23 data (60 sec)
        try:
            if (isIMF(filename)):
                return True
        except:
            pass
    elif (format_type == "BLV"): # Intermagnet IBFV2.00
        try:
            if (isBLV(filename)):
                return True
        except:
            pass
    elif (format_type == "AUTODIF_FREAD"): # Text AUTODIF F for baseline (0.2 Hz, from POS1)
        try:
            if (isAUTODIF_FREAD(filename)):
                return True
        except:
            pass
    #elif (format_type == "ENV05"): # Binary Environmental data (1 Hz)
    #    if (isENV05(filename)):
    #        return True
    elif (format_type == "SFDMI"): # San Fernando DMI(FGE) format
        if (isSFDMI(filename)):
            return True
    elif (format_type == "SFGSM"): # San Fernando GSM format
        if (isSFGSM(filename)):
            return True
    elif (format_type == "BDV1"): # Budkov format
        if (isBDV1(filename)):
            return True
    elif (format_type == "GFZKP"): # GFZ Kp
        if (isGFZKP(filename)):
            return True
    elif (format_type == "NOAAACE"): # NOAA ACE Satellite data
        if (isNOAAACE(filename)):
            return True
    elif (format_type == "NETCDF"): # NetCDF format, NOAA DSCOVR satellite data
        if (isNETCDF(filename)):
            return True
    elif (format_type == "NEIC"): # NEIC USGS data
        if (isNEIC(filename)):
            return True
    elif (format_type == "PHA"): # Potentially Hazardous Objects (This research has made use of data and/or services provided by the International Astronomical Union's Minor Planet Center.)
        if (isPHA(filename)):
            return True
    elif (format_type == "USBLOG"): # Data from the USB temperature logger
        if (isUSBLOG(filename)):
            return True
    elif (format_type == "QSPIN"): # Data from the USB temperature logger
        if (isQSPIN(filename)):
            return True
    elif (format_type in ["PYNC", "AUTODIF", "SERMUL", "SERSIN", "LATEX"]): # Not yet supported
        return False
    elif (format_type == "UNKOWN"): # Unkown
        return False
    else:
        logger.warning("isFormat: Could not identify data format for file {}. Is {} a valid type?".format(filename, format_type))
        return False


def readFormat(filename, format_type, headonly=False, **kwargs):
    empty = DataStream()
    if (format_type == "IAGA"):
        return readIAGA(filename, headonly, **kwargs)
    elif (format_type == "WDC"):
        return readWDC(filename, headonly, **kwargs)
    elif (format_type == "IMF"):
        return readIMF(filename, headonly, **kwargs)
    elif (format_type == "IAF"):
        return readIAF(filename, headonly, **kwargs)
    elif (format_type == "IMAGCDF"):
        return readIMAGCDF(filename, headonly, **kwargs)
    elif (format_type == "BLV"): # Intermagnet IBFV2.00
        return readBLV(filename, headonly, **kwargs)
    elif (format_type == "IYFV"): # Intermagnet IYVF1.01
        return readIYFV(filename, headonly, **kwargs)
    elif (format_type == "DKA"): # Intermagnet DKA
        return readDKA(filename, headonly, **kwargs)
    elif (format_type == "DIDD"):
        return readDIDD(filename, headonly, **kwargs)
    elif (format_type == "GDASA1"):
        return readGDASA1(filename, headonly, **kwargs)
    elif (format_type == "GDASB1"):
        return readGDASB1(filename, headonly, **kwargs)
    elif (format_type == "RMRCS"):
        return readRMRCS(filename, headonly, **kwargs)
    elif (format_type == "RCS"):
        return readRCS(filename, headonly, **kwargs)
    elif (format_type == "METEO"):
        return readMETEO(filename, headonly, **kwargs)
    elif (format_type == "LNM"):
        return readLNM(filename, headonly, **kwargs)
    elif (format_type == "PYSTR"):
        return readPYSTR(filename, headonly, **kwargs)
    elif (format_type == "PYASCII"):
        return readPYASCII(filename, headonly, **kwargs)
    elif (format_type == "PYCDF"):
        return readPYCDF(filename, headonly, **kwargs)
    elif (format_type == "PYBIN"):
        return readPYBIN(filename, headonly, **kwargs)
    elif (format_type == "JSON"):
        return readJSON(filename, headonly, **kwargs)
    elif (format_type == "GSM19"):
        return readGSM19(filename, headonly, **kwargs)
    elif (format_type == "LEMIHF"):
        return readLEMIHF(filename, headonly, **kwargs)
    elif (format_type == "LEMIBIN"):
        return readLEMIBIN(filename, headonly, **kwargs)
    elif (format_type == "LEMIBIN1"):
        return readLEMIBIN1(filename, headonly, **kwargs)
    elif (format_type == "POS1"):
        return readPOS1(filename, headonly, **kwargs)
    elif (format_type == "POS1TXT"):
        return readPOS1TXT(filename, headonly, **kwargs)
    elif (format_type == "PMB"):
        return readPOSPMB(filename, headonly, **kwargs)
    elif (format_type == "QSPIN"):
        return readQSPIN(filename, headonly, **kwargs)
    elif (format_type == "AUTODIF_FREAD"):
        return readAUTODIF_FREAD(filename, headonly, **kwargs)
    #elif (format_type == "ENV05"):
    #    return readENV05(filename, headonly, **kwargs)
    elif (format_type == "USBLOG"):
        return readUSBLOG(filename, headonly, **kwargs)
    elif (format_type == "GRAVSG"):
        return readGRAVSG(filename, headonly, **kwargs)
    elif (format_type == "IWT"):
        return readIWT(filename, headonly, **kwargs)
    elif (format_type == "LIPPGRAV"):
        return readLIPPGRAV(filename, headonly, **kwargs)
    elif (format_type == "CR800"):
        return readCR800(filename, headonly, **kwargs)
    elif (format_type == "IONO"):
        return readIONO(filename, headonly, **kwargs)
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
    elif (format_type == "GFZKP"):
        return readGFZKP(filename, headonly, **kwargs)
    elif (format_type == "NOAAACE"):
        return readNOAAACE(filename, headonly, **kwargs)
    elif (format_type == "NETCDF"):
        return readNETCDF(filename, headonly, **kwargs)
    elif (format_type == "NEIC"):
        return readNEIC(filename, headonly, **kwargs)
    elif (format_type == "PHA"):
        return readPHA(filename, headonly, **kwargs)
    else:
        logger.info("No valid format found ({}). Returning empty stream.".format(format_type))
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
    elif (format_type == "IMF"):
        return writeIMF(datastream, filename, **kwargs)
    elif (format_type == "IAF"):
        return writeIAF(datastream, filename, **kwargs)
    elif (format_type == "IMAGCDF"):
        return writeIMAGCDF(datastream, filename, **kwargs)
    elif (format_type == "BLV"):
        return writeBLV(datastream, filename, **kwargs)
    elif (format_type == "IYFV"):
        return writeIYFV(datastream, filename, **kwargs)
    elif (format_type == "DKA"):
        return writeDKA(datastream, filename, **kwargs)
    elif (format_type == "DIDD"):
        return writeDIDD(datastream, filename, **kwargs)
    elif (format_type == "PYSTR"):
        return writePYSTR(datastream, filename, **kwargs)
    elif (format_type == "PYASCII"):
        return writePYASCII(datastream, filename, **kwargs)
    elif (format_type == "PYCDF"):
        return writePYCDF(datastream, filename, **kwargs)
    elif (format_type == "AUTODIF_FREAD"):
        return writeAUTODIF_FREAD(datastream, filename, **kwargs)
    elif (format_type == "CR800"):
        return writeCR800(datastream, filename, **kwargs)
    elif (format_type == "LATEX"):
        return writeLATEX(datastream, filename, **kwargs)
    else:
        logging.warning("writeFormat: Writing not succesful - format not recognized")
