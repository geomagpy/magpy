"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_absolutes import *


def isMAGPYABS(filename):
    """
    Checks whether a file is ASCII DIDD (Tihany) format.
    """
    try:
        temp = open(filename, 'rt')
        line = temp.readline()
        line = temp.readline()
    except:
        return False
    if not line.startswith('Miren'):
        return False
    return True


def isMAGPYNEWABS(filename):
    """
    Checks whether a file is ASCII DIDD (Tihany) format.
    """
    try:
        temp = open(filename, 'rt')
        line = temp.readline()
    except:
        return False
    if not line.startswith('# MagPy Absolutes'):
        return False
    return True


def readMAGPYABS(filename, headonly=False, **kwargs):
    """
    Reading MAGPY's Absolute format data.
    Berger  T010B  MireChurch  D  ELSEC820
    Miren:
    51.183055555555555	51.183055555555555	231.1830555555556	231.1830555555556	51.18333333333333	51.18333333333333	231.18333333333334	231.18333333333334
    Positions:
    2010-06-11_12:03:00	93.1836111111111	90.	0.
    2010-06-11_12:03:00	93.1836111111111	90.	0.
    2010-06-11_12:08:30	273.25916666666666	90.	0.
    2010-06-11_12:08:30	273.25916666666666	90.	0.
    """
    plog = PyMagLog()
    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = AbsoluteData()
    # Check whether header infromation is already present
    headers = {}
    data,measurement,unit = [],[],[]
    person, f_inst, di_inst = '','',''
    i = 0
    expectedmire,temp = 0.0,0.0
    key = None
    headfound = False
    #print "Reading data ..."
    for line in fh:
        numelements = len(line.split())
        if line.isspace():
            # blank line
            pass
        elif not headfound and numelements > 3:
            # data header
            headfound = True
            colsstr = line.split()
            person =  colsstr[0]
            di_inst = colsstr[1]
            # check whether mire is number or String            
            try:
                expectedmire = float(colsstr[2])
            except:
                try:
                    expectedmire = miredict[colsstr[2]]
                except:
                    logging.warning('ReadAbsolute: Mire not known in file %s' % filename)
                    return stream
            headers['pillar'] = colsstr[3]
            if numelements > 4:
                f_inst = colsstr[4]
            if numelements > 5:
                try:
                    adate= datetime.strptime(colsstr[5],'%Y-%m-%d')
                    headers['analysisdate'] = adate
                except:
                    if colsstr[5].find('C') > 0:
                        temp = float(colsstr[5].strip('C'))
            if numelements > 6:
                temp = float(colsstr[6].strip('C'))
        elif headonly:
            # skip data for option headonly
            continue
        elif numelements == 1:
            # section titles mesurements - last one corresponds to variometer if no :
            pass
        elif numelements == 2:
            # Intensity mesurements
            row = AbsoluteDIStruct()
            fstr = line.split()
            try:
                row.time = date2num(datetime.strptime(fstr[0],"%Y-%m-%d_%H:%M:%S"))
            except:
                logging.warning('ReadAbsolute: Check date format of f measurements in file %s' % filename)
                return stream
            try:
                row.f = float(fstr[1])
            except:
                logging.warning('ReadAbsolute: Check data format in file %s' % filename)
                return stream
            stream.add(row)
        elif numelements == 4:
            # Position mesurements
            row = AbsoluteDIStruct()
            posstr = line.split()
            try:
                row.time = date2num(datetime.strptime(posstr[0],"%Y-%m-%d_%H:%M:%S"))
            except:
                if not posstr[0] == 'Variometer':
                    logging.warning('ReadAbsolute: Check date format of measurements positions in file %s' % filename)
                return stream
            try:
                row.hc = float(posstr[1])
                row.vc = float(posstr[2])
                row.res = float(posstr[3])
                row.mu = mu
                row.md = md
                row.expectedmire = expectedmire
                row.temp = temp
                row.person = person
                row.di_inst = di_inst
                row.f_inst = f_inst
            except:
                logging.warning('ReadAbsolute: Check general format of measurements positions in file %s' % filename)
                return stream
            stream.add(row)
        elif numelements == 8:
            # Miren mesurements
            mirestr = line.split()
            mu = np.mean([float(mirestr[0]),float(mirestr[1]),float(mirestr[4]),float(mirestr[5])])
            md = np.mean([float(mirestr[2]),float(mirestr[3]),float(mirestr[6]),float(mirestr[7])])
            mustd = np.std([float(mirestr[0]),float(mirestr[1]),float(mirestr[4]),float(mirestr[5])])
            mdstd = np.std([float(mirestr[2]),float(mirestr[3]),float(mirestr[6]),float(mirestr[7])])
            maxdev = np.max([mustd, mdstd])
            if abs(maxdev) > 0.01:
                logging.warning('ReadAbsolute: Check miren readings in file %s' % filename)
        else:
            #print line
            pass
    fh.close()

    return stream


def readMAGPYNEWABS(filename, headonly=False, **kwargs):
    """
    Reading MAGPY's Absolute format data.
    Looks like:
    # MagPy Absolutes
    # 
    Miren:
    51.183055555555555	51.183055555555555	231.1830555555556	231.1830555555556	51.18333333333333	51.18333333333333	231.18333333333334	231.18333333333334
    Positions:
    2010-06-11_12:03:00	93.1836111111111	90.	0.
    2010-06-11_12:03:00	93.1836111111111	90.	0.
    2010-06-11_12:08:30	273.25916666666666	90.	0.
    2010-06-11_12:08:30	273.25916666666666	90.	0.
    """
    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = AbsoluteData()
    # Check whether header infromation is already present
    headers = {}
    data,measurement,unit = [],[],[]
    person, f_inst, di_inst = '','',''
    i = 0
    expectedmire,temp = 0.0,0.0
    key = None
    headfound = False
    #print "Reading data ..."
    for line in fh:
        numelements = len(line.split())
        if line.isspace():
            # blank line
            pass
        elif line.startwith('#'):
            # header
            headline = line.split(':')
            if headline[0] == ('# Abs-Obserever'):
                person = headline[1]
            if headline[0] == ('# Abs-Theodolite'):
                di_inst = headline[1].replace(', ','_')
            if headline[0] == ('# Abs-AzimuthMark'):
                expectedmire = float(headline[1])
            if headline[0] == ('# Abs-Obserever'):
                person = headline[1]
            if headline[0] == ('# Abs-Obserever'):
                person = headline[1]
            if headline[0] == ('# Abs-Obserever'):
                person = headline[1]
            if headline[0] == ('# Abs-Obserever'):
                person = headline[1]
        elif not headfound and numelements > 3:
            # data header
            headfound = True
            colsstr = line.split()
            person =  colsstr[0]
            di_inst = colsstr[1]
            # check whether mire is number or String            
            try:
                expectedmire = float(colsstr[2])
            except:
                try:
                    expectedmire = miredict[colsstr[2]]
                except:
                    logging.warning('ReadAbsolute: Mire not known in file %s' % filename)
                    return stream
            headers['pillar'] = colsstr[3]
            if numelements > 4:
                f_inst = colsstr[4]
            if numelements > 5:
                try:
                    adate= datetime.strptime(colsstr[5],'%Y-%m-%d')
                    headers['analysisdate'] = adate
                except:
                    if colsstr[5].find('C') > 0:
                        temp = float(colsstr[5].strip('C'))
            if numelements > 6:
                temp = float(colsstr[6].strip('C'))
        elif headonly:
            # skip data for option headonly
            continue
        elif numelements == 1:
            # section titles mesurements - last one corresponds to variometer if no :
            pass
        elif numelements == 2:
            # Intensity mesurements
            row = AbsoluteDIStruct()
            fstr = line.split()
            try:
                row.time = date2num(datetime.strptime(fstr[0],"%Y-%m-%d_%H:%M:%S"))
            except:
                logging.warning('ReadAbsolute: Check date format of f measurements in file %s' % filename)
                return stream
            try:
                row.f = float(fstr[1])
            except:
                logging.warning('ReadAbsolute: Check data format in file %s' % filename)
                return stream
            stream.add(row)
        elif numelements == 4:
            # Position mesurements
            row = AbsoluteDIStruct()
            posstr = line.split()
            try:
                row.time = date2num(datetime.strptime(posstr[0],"%Y-%m-%d_%H:%M:%S"))
            except:
                if not posstr[0] == 'Variometer':
                    logging.warning('ReadAbsolute: Check date format of measurements positions in file %s' % filename)
                return stream
            try:
                row.hc = float(posstr[1])
                row.vc = float(posstr[2])
                row.res = float(posstr[3])
                row.mu = mu
                row.md = md
                row.expectedmire = expectedmire
                row.temp = temp
                row.person = person
                row.di_inst = di_inst
                row.f_inst = f_inst
            except:
                logging.warning('ReadAbsolute: Check general format of measurements positions in file %s' % filename)
                return stream
            stream.add(row)
        elif numelements == 8:
            # Miren mesurements
            mirestr = line.split()
            mu = np.mean([float(mirestr[0]),float(mirestr[1]),float(mirestr[4]),float(mirestr[5])])
            md = np.mean([float(mirestr[2]),float(mirestr[3]),float(mirestr[6]),float(mirestr[7])])
            mustd = np.std([float(mirestr[0]),float(mirestr[1]),float(mirestr[4]),float(mirestr[5])])
            mdstd = np.std([float(mirestr[2]),float(mirestr[3]),float(mirestr[6]),float(mirestr[7])])
            maxdev = np.max([mustd, mdstd])
            if abs(maxdev) > 0.01:
                logging.warning('ReadAbsolute: Check miren readings in file %s' % filename)
        else:
            #print line
            pass
    fh.close()

    return stream


def writeMAGPYNEWABS(filename, headonly=False, **kwargs):
    """
    Write MAGPY's Absolute format data - text file.
    possible keywords:
    fmt : format (usually txt, maybe cdf)
    dbase : provide database access
    any missing header info
    Abs-Unit, Abs-Theo, etc...
    
    """
    pass
