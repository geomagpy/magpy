"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from magpy.absolutes import *
#import magpy.absolutes as di


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


def isAUTODIF(filename):
    """
    Checks whether a file is AUTODIF format.
    """
    try:
        temp = open(filename, 'rt')
        line = temp.readline()
    except:
        return False
    if not line.startswith('AUTODIF') and not line.startswith('auto'):
        return False
    return True


def isJSONABS(filename):
    """
    Checks whether a file is AUTODIF format.
    """
    try:
        jsonfile = open(filename, 'r')
        dataset = json.load(jsonfile)
    except:
        return False
    try:
        for idx, elem in enumerate(dataset):
            if idx==0:
                datadict = dataset[elem][0]
                if not 'readings' in datadict:
                    return False
    except:
        return False
    return True


def readMAGPYABS(filename, headonly=False, **kwargs):
    """
    Reading MAGPY's Absolute format data.
    Berger  T010B  MireChurch  D  ELSEC820
    Miren:
    51.183055555555555  51.183055555555555      231.1830555555556       231.1830555555556       51.18333333333333       51.18333333333333       231.18333333333334      231.18333333333334
    Positions:
    2010-06-11_12:03:00 93.1836111111111        90.     0.
    2010-06-11_12:03:00 93.1836111111111        90.     0.
    2010-06-11_12:08:30 273.25916666666666      90.     0.
    2010-06-11_12:08:30 273.25916666666666      90.     0.


    IMPORTANT: 
    - in comparison to the new format, azimuth data (Miren) is stored the other way round
    - before 2010-08, Miren were stored in two separate lines of length 4    
    """

    azimuth = kwargs.get('azimuth')
    output = kwargs.get('output')

    if not output:
        output = 'AbsoluteDIStruct' # else 'DILineStruct'

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
    dirow = DILineStruct(25)
    ang_fac = 1.
    count = 4
    currentsection = "None"
    
    for line in fh:
        numelements = len(line.split())
        #print ("NUM", numelements, line)
        if line.isspace():
            # blank line
            pass
        elif not headfound and numelements > 3:
            # data header
            headfound = True
            colsstr = line.split()
            person =  colsstr[0]
            di_inst = colsstr[1]
            dirow.person = person
            dirow.di_inst = di_inst
            # check whether mire is number or String
            try:
                expectedmire = float(colsstr[2])
            except:
                try:
                    expectedmire = miredict[colsstr[2]]
                except:
                    loggerlib.error('%s : ReadAbsolute: Mire not known in this file' % filename)
                    return stream
            dirow.azimuth = expectedmire
            headers['pillar'] = colsstr[3]
            dirow.pier = colsstr[3]
            if numelements > 4:
                f_inst = colsstr[4]
                dirow.f_inst = f_inst
            if numelements > 5:
                try:
                    adate= datetime.strptime(colsstr[5],'%Y-%m-%d')
                    headers['analysisdate'] = adate
                    dirow.inputdate = adate
                except:
                    if colsstr[5].find('C') > 0:
                        temp = float(colsstr[5].strip('C'))
                        dirow.t = temp
            if numelements > 6:
                temp = float(colsstr[6].strip('C'))
                dirow.t = temp
        elif headonly:
            # skip data for option headonly
            continue
        elif numelements == 1:
            # section titles mesurements - last one corresponds to variometer if no :
            currentsection = line.strip(':\n')
        elif numelements == 2:
            # Intensity mesurements
            row = AbsoluteDIStruct()
            fstr = line.split()
            try:
                row.time = date2num(datetime.strptime(fstr[0],"%Y-%m-%d_%H:%M:%S"))
                dirow.ftime.append(row.time)
            except:
                loggerlib.error('%s : ReadAbsolute: Check date format of f measurements in this file' % filename)
                return stream
            try:
                row.f = float(fstr[1])
                dirow.f.append(row.f)
            except:
                loggerlib.error('%s : ReadAbsolute: Check data format in this file' % filename)
                return stream
            stream.add(row)
        elif numelements == 4:
            # Position mesurements
            if currentsection.startswith("Position") or currentsection.startswith("position"): # very old data format
                row = AbsoluteDIStruct()
                posstr = line.split()
                # correct sorting for DIline
                if count == 8:
                    count = 99
                if count == 10:
                    count = 999
                if count == 6:
                    count = 10
                if count == 12:
                    count = 8
                if count == 999:
                    count = 6
                if count == 18:
                    count = 9999
                if count == 14:
                    count = 999999
                if count == 16:
                    count = 99999
                if count == 99:
                    count = 18
                if count == 20:
                    count = 16
                if count == 9999:
                    count = 14
                if count == 99999:
                    count = 12
                if count == 999999:
                    count = 20
                try:
                    dirow.time[count] = date2num(datetime.strptime(posstr[0],"%Y-%m-%d_%H:%M:%S"))
                except:
                    if not posstr[0] == 'Variometer':
                        logging.error('ReadAbsolute: Check date format of measurements positions in file %s (%s)' % (filename,posstr[0]))
                    return stream
                try:
                    row.time = date2num(datetime.strptime(posstr[0],"%Y-%m-%d_%H:%M:%S"))
                except:
                    if not posstr[0] == 'Variometer':
                        loggerlib.error('%s : ReadAbsolute: Check date format of measurements positions in this file' % filename)
                    return stream
                try:
                    row.hc = float(posstr[1])
                    row.vc = float(posstr[2])
                    row.res = float(posstr[3].replace(',','.'))
                    dirow.hc[count] = float(posstr[1])/ang_fac
                    dirow.vc[count] = float(posstr[2])/ang_fac
                    dirow.res[count] = float(posstr[3].replace(',','.'))
                    row.mu = mu
                    row.md = md
                    row.expectedmire = expectedmire
                    row.temp = temp
                    row.person = person
                    row.di_inst = di_inst
                    row.f_inst = f_inst
                except:
                    loggerlib.error('%s : ReadAbsolute: Check general format of measurements positions in this file' % filename)
                    return stream
                count = count +1
                stream.add(row)
        elif numelements == 8:
            # Miren mesurements
            mirestr = line.split()
            dirow.hc[2] = float(mirestr[2])/ang_fac
            dirow.hc[3] = float(mirestr[3])/ang_fac
            dirow.hc[0] = float(mirestr[0])/ang_fac
            dirow.hc[1] = float(mirestr[1])/ang_fac
            mu = np.mean([float(mirestr[0]),float(mirestr[1]),float(mirestr[4]),float(mirestr[5])])
            md = np.mean([float(mirestr[2]),float(mirestr[3]),float(mirestr[6]),float(mirestr[7])])
            mustd = np.std([float(mirestr[0]),float(mirestr[1]),float(mirestr[4]),float(mirestr[5])])
            mdstd = np.std([float(mirestr[2]),float(mirestr[3]),float(mirestr[6]),float(mirestr[7])])
            maxdev = np.max([mustd, mdstd])
            if abs(maxdev) > 0.01:
                loggerlib.error('%s : ReadAbsolute: Check azimuth readings in this file' % filename)
        else:
            #print line
            pass
    fh.close()

    dirow.hc.insert(12,float(mirestr[6])/ang_fac)
    dirow.hc.insert(13,float(mirestr[7])/ang_fac)
    dirow.hc.insert(14,float(mirestr[4])/ang_fac)
    dirow.hc.insert(15,float(mirestr[5])/ang_fac)
    dirow.vc.insert(12,float(nan))
    dirow.vc.insert(13,float(nan))
    dirow.vc.insert(14,float(nan))
    dirow.vc.insert(15,float(nan))
    dirow.time.insert(12,float(nan))
    dirow.time.insert(13,float(nan))
    dirow.time.insert(14,float(nan))
    dirow.time.insert(15,float(nan))
    dirow.res.insert(12,float(nan))
    dirow.res.insert(13,float(nan))
    dirow.res.insert(14,float(nan))
    dirow.res.insert(15,float(nan))

    if output == "DIListStruct":
        # -- Return single row list ---- Works !!!!!!   Further Checks necessary
        #print dirow
        abslst = []
        abslst.append(dirow)
        return abslst
    else:
        return stream


def readMAGPYNEWABS(filename, headonly=False, **kwargs):
    """
    Reading MAGPY's Absolute format data.
    Looks like:
    # MagPy Absolutes
    #
    Miren:
    51.183055555555555  51.183055555555555      231.1830555555556       231.1830555555556       51.18333333333333       51.18333333333333       231.18333333333334      231.18333333333334
    Positions:
    2010-06-11_12:03:00 93.1836111111111        90.     0.
    2010-06-11_12:03:00 93.1836111111111        90.     0.
    2010-06-11_12:08:30 273.25916666666666      90.     0.
    2010-06-11_12:08:30 273.25916666666666      90.     0.
    """
    azimuth = kwargs.get('azimuth')
    output = kwargs.get('output')

    if not output:
        output = 'AbsoluteDIStruct' # else 'DILineStruct'

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = AbsoluteData()
    # Check whether header infromation is already present
    headers = {}
    data,measurement,unit = [],[],[]
    person, f_inst, di_inst = '','',''
    i = 0
    expectedmire,temp = 0.0,0.0
    delf = 0.0
    key = None
    headfound = False
    dirow = DILineStruct(25)
    count = 4
    goon = False

    for line in fh:
        numelements = len(line.split())
        if line.isspace():
            # blank line
            pass
        elif line.startswith('#'):
            # header
            line = line.strip('\n')
            headline = line.split(':')
            if headline[0] == ('# Abs-Observer'):
                person = headline[1].strip().replace(' ','_')
                dirow.person = person
            if headline[0] == ('# Abs-Theodolite'):
                di_inst = headline[1].replace(', ','_').strip().replace(' ','_')
                dirow.di_inst = di_inst
            if headline[0] == ('# Abs-TheoUnit'):
                unit = headline[1].strip().replace(' ','_')
                # Any given unit is transformed to degree
                # Therefor no further angular corrections are necessary
                if unit=='gon':
                    ang_fac = 400./360.
                elif unit == 'rad':
                    ang_fac = np.pi/180.
                else:
                    ang_fac = 1.
                unit = 'deg'
            if headline[0] == ('# Abs-FGSensor'):
                fgsensor = headline[1].strip().replace(' ','_')
                dirow.fluxgatesensor = fgsensor
            if headline[0] == ('# Abs-AzimuthMark'):
                try:
                    expectedmire = float(headline[1].strip())/ang_fac
                    # a given azimuth value overrides the file value
                    if azimuth:
                        expectedmire = azimuth
                    dirow.azimuth = expectedmire
                except:
                    logging.error('ReadAbsolute: Azimuth mark could not be interpreted, please provide it by option - azimuth = xxx.xxxx - %s' % filename)
                    return stream
            if headline[0] == ('# Abs-Pillar'):
                headers['pillar'] = headline[1].strip()
                dirow.pier = headline[1].strip()
            if headline[0] == ('# Abs-Scalar'):
                f_inst = headline[1].strip()
                dirow.f_inst = f_inst
            if headline[0] == ('# Abs-DeltaF'):
                try:
                    delf = float(headline[1].strip())
                except:
                    delf = 0.0
            if headline[0] == ('# Abs-Temperature'):
                tempstring = headline[1].replace('C','').strip()
                if not tempstring == '':
                    ustr = tempstring
                    #ustr = tempstring.decode('utf-8')
                    #ustr = unicode(tempstring, 'utf-8')
                    ustr.encode('ascii','ignore')
                    temp = float(ustr.replace(',','.').strip(u"\u00B0").strip())
                else:
                    temp = float(nan)
                dirow.t = temp
            if headline[0] == ('# Abs-InputDate'):
                adate= datetime.strptime(headline[1].strip(),'%Y-%m-%d')
                headers['analysisdate'] = adate
                dirow.inputdate = adate
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
                dirow.ftime.append(row.time)
            except:
                logging.warning('ReadAbsolute: Check date format of f measurements in file %s' % filename)
            try:
                row.f = float(fstr[1]) + delf
                dirow.f.append(row.f)
            except:
                logging.warning('ReadAbsolute: Check data format in file %s' % filename)
            stream.add(row)
        elif numelements == 4:
            # Position mesurements
            row = AbsoluteDIStruct()
            posstr = line.split()
            # correct sorting for DIline
            if count == 8:
                count = 99
            if count == 10:
                count = 999
            if count == 6:
                count = 10
            if count == 12:
                count = 8
            if count == 999:
                count = 6
            if count == 18:
                count = 9999
            if count == 14:
                count = 999999
            if count == 16:
                count = 99999
            if count == 99:
                count = 18
            if count == 20:
                count = 16
            if count == 9999:
                count = 14
            if count == 99999:
                count = 12
            if count == 999999:
                count = 20
            try:
                dirow.time[count] = date2num(datetime.strptime(posstr[0],"%Y-%m-%d_%H:%M:%S"))
            except:
                logging.error('ReadAbsolute: Check date format of measurements positions in file %s (%s)' % (filename,posstr[0]))
                return stream
            try:
                row.time = date2num(datetime.strptime(posstr[0],"%Y-%m-%d_%H:%M:%S"))
            except:
                if not posstr[0] == 'Variometer':
                    logging.warning('ReadAbsolute: Check date format of measurements positions in file %s (%s)' % (filename,posstr[0]))
                return stream
            try:
                row.hc = float(posstr[1])/ang_fac
                row.vc = float(posstr[2])/ang_fac
                row.res = float(posstr[3].replace(',','.'))
                dirow.hc[count] = float(posstr[1])/ang_fac
                dirow.vc[count] = float(posstr[2])/ang_fac
                dirow.res[count] = float(posstr[3].replace(',','.'))
                #print count, dirow.vc[count], dirow.res[count]
                row.mu = mu
                row.md = md
                row.expectedmire = expectedmire
                row.temp = temp
                row.person = person
                row.di_inst = di_inst+'_'+fgsensor
                row.f_inst = f_inst
            except:
                logging.warning('ReadAbsolute: Check general format of measurements positions in file %s' % filename)
                return stream
            count = count +1
            stream.add(row)
        elif numelements == 8:
            # Miren mesurements
            mirestr = line.split()
            dirow.hc[0] = float(mirestr[2])/ang_fac
            dirow.hc[1] = float(mirestr[3])/ang_fac
            dirow.hc[2] = float(mirestr[0])/ang_fac
            dirow.hc[3] = float(mirestr[1])/ang_fac
            md = np.mean([float(mirestr[0]),float(mirestr[1]),float(mirestr[4]),float(mirestr[5])])/ang_fac
            mu = np.mean([float(mirestr[2]),float(mirestr[3]),float(mirestr[6]),float(mirestr[7])])/ang_fac
            mdstd = np.std([float(mirestr[0]),float(mirestr[1]),float(mirestr[4]),float(mirestr[5])])
            mustd = np.std([float(mirestr[2]),float(mirestr[3]),float(mirestr[6]),float(mirestr[7])])
            maxdev = np.max([mustd, mdstd])
            if abs(maxdev) > 0.01:
                logging.warning('ReadAbsolute: Check miren readings in file %s' % filename)
        else:
            #print line
            pass
    fh.close()

    dirow.hc.insert(12,float(mirestr[6])/ang_fac)
    dirow.hc.insert(13,float(mirestr[7])/ang_fac)
    dirow.hc.insert(14,float(mirestr[4])/ang_fac)
    dirow.hc.insert(15,float(mirestr[5])/ang_fac)
    dirow.vc.insert(12,float(nan))
    dirow.vc.insert(13,float(nan))
    dirow.vc.insert(14,float(nan))
    dirow.vc.insert(15,float(nan))
    dirow.time.insert(12,float(nan))
    dirow.time.insert(13,float(nan))
    dirow.time.insert(14,float(nan))
    dirow.time.insert(15,float(nan))
    dirow.res.insert(12,float(nan))
    dirow.res.insert(13,float(nan))
    dirow.res.insert(14,float(nan))
    dirow.res.insert(15,float(nan))

    #print "Check output - dirow"
    #print "-----------------------------------------------------"
    #print dirow
    #print "Check output - stream"
    #print "-----------------------------------------------------"
    #print stream

    if output == "DIListStruct":
        # -- Return single row list ---- Works !!!!!!   Further Checks necessary
        #print dirow
        abslst = []
        abslst.append(dirow)
        return abslst
    else:
        return stream

def readAUTODIF(filename, headonly=False, **kwargs):
    """
    Reading Autodifs's Absolute format data.
    Take care: File contains horizontal/vertical axis - magpy is using the h/v circles (perpendicular to axis)
    Looks like:
    AUTODIF002  2013-10-17

    Measure ID  Date    Time    Laser   Fluxgate        Level   Haxis   Vaxis

    Laser PU    2013-10-17      00:15:59        0.0471  2047    -192    90.5789 94.871
    Laser PU    2013-10-17      00:16:07        -0.0366 2047    -192    90.5789 94.8727
    Laser PD    2013-10-17      00:16:56        -0.022  2047    -185    271.3414        274.1147
    Laser PD    2013-10-17      00:17:04        0.022   2047    -185    271.3414        274.1157
    Declination 1       2013-10-17      00:17:23        NaN     55      -183    90.0    281.0448
    Declination 1       2013-10-17      00:17:31        NaN     -1      -183    90.0    281.0304
    Declination 2       2013-10-17      00:17:54        NaN     128     -183    269.9991        280.0551
    Declination 2       2013-10-17      00:18:02        NaN     3       -183    269.9991        280.0883
    Declination 3       2013-10-17      00:18:17        NaN     -19     -192    269.9999        100.7341
    Declination 3       2013-10-17      00:18:25        NaN     -15     -192    269.9999        100.7352
    Declination 4       2013-10-17      00:18:49        NaN     193     -194    90.0007 100.9456
    Declination 4       2013-10-17      00:18:59        NaN     12      -194    90.0007 100.9947
    Laser PU    2013-10-17      00:19:14        -0.0053 2047    -191    90.5785 94.8723
    Laser PU    2013-10-17      00:19:22        0.123   2047    -191    90.5785 94.8696
    Laser PD    2013-10-17      00:19:47        0.0     2047    -185    271.341 274.115
    Laser PD    2013-10-17      00:19:55        -0.0659 2047    -185    271.341 274.1134
    Inclination 1       2013-10-17      00:20:16        NaN     400     -179    115.7004        10.7194
    Inclination 1       2013-10-17      00:20:25        NaN     -8      -179    115.7463        10.7194
    Inclination 2       2013-10-17      00:20:44        NaN     51      -179    295.8997        10.7175
    Inclination 2       2013-10-17      00:20:52        NaN     -7      -179    295.8929        10.7175
    Inclination 3       2013-10-17      00:21:10        1.0     -8      -202    244.4002        190.7183
    Inclination 3       2013-10-17      00:21:18        NaN     0       -202    244.4015        190.7183
    Inclination 4       2013-10-17      00:21:37        NaN     -404    -203    64.3002 190.7157
    Inclination 4       2013-10-17      00:21:49        NaN     -11     -202    64.2543 190.7157
    """
    azimuth = kwargs.get('azimuth')
    scaleflux = kwargs.get('scaleflux')
    scaleangle = kwargs.get('scaleangle')
    temperature = kwargs.get('temperature')
    pier = kwargs.get('pier')

    if not azimuth:
        azimuth = float(nan)
    if not scaleflux:
        scaleflux = 0.098
    if not scaleangle:
        scaleangle = 0.00011
    if not temperature:
        temperature = float(nan)
    if not pier:
        pier = ''

    fh = open(filename, 'rt')
    # read file and split text into channels
    abslist = []
    # Check whether header infromation is already present
    headers = {}

    count = 0
    inccount = 0

    newset = False  # To distinguish between different absolute sets within one file

    #print "Reading DI data ..."
    row = DILineStruct(24)

    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif line.startswith('AUTODIF'):
            di_inst = line.split()[0]
            pass
        elif line.startswith('auto'):
            di_inst = line.split()[0]
            pass
        elif line.startswith('Measure'):
            pass
        elif headonly:
            # skip data for option headonly
            continue
        else:
            # Position mesurements
            posstr = line.split()
            try:
                if not line.startswith('Inclination'):
                    if newset == True:
                        count = 0
                        inccount = 0
                        newset = False
                        row.person = 'AutoDIF'
                        row.di_inst = di_inst
                        row.scaleflux = scaleflux
                        row.scaleangle = scaleangle
                        row.t = temperature
                        row.azimuth = azimuth
                        row.pier = pier
                        abslist.append(row)
                        row = DILineStruct(24)
                    try:
                        row.time[count] = date2num(datetime.strptime(posstr[2] + '_' + posstr[3],"%Y-%m-%d_%H:%M:%S"))
                    except:
                        logging.warning('ReadAbsolute: Check date format of measurements positions in file %s (%s)' % (filename,posstr[0]))
                        pass
                    row.laser[count] = float(posstr[4])
                    row.res[count] = float(posstr[5])*scaleflux
                    row.opt[count] = float(posstr[6])
                    row.vc[count] = float(posstr[7])
                    row.hc[count] = float(posstr[8])
                    count = count +1
                else:
                    inccount = inccount +1
                    if inccount == 8:
                        newset = True
                    try:
                        row.time[count] = date2num(datetime.strptime(posstr[2] + '_' + posstr[3],"%Y-%m-%d_%H:%M:%S"))
                    except:
                        logging.warning('ReadAbsolute: Check date format of measurements positions in file %s (%s)' % (filename,posstr[0]))
                        pass
                    row.res[count] = float(posstr[5])*scaleflux
                    row.opt[count] = float(posstr[6])
                    row.vc[count] = float(posstr[7])
                    row.hc[count] = float(posstr[8])+180 # 180 deg are necessary for the magpy routine
                    count = count +1
            except:
                logging.warning('ReadAbsolute: Check general format of measurements positions in file %s' % filename)
                return
    fh.close()

    # add the last input as well
    row.person = 'AutoDIF'
    row.di_inst = di_inst
    row.scaleflux = scaleflux
    row.scaleangle = scaleangle
    row.t = temperature
    row.azimuth = azimuth
    row.pier = pier
    abslist.append(row)

    return abslist


def readJSONABS(filename, headonly=False, **kwargs):
    """
    Reading JSON Absolute format data.
    """

    jsonfile = open(filename, 'r')
    dataset = json.load(jsonfile)
    stream = AbsoluteData()
    #dirow = DILineStruct(25) ## if scale values would be provided
    dirow = DILineStruct(24)

    abslist = []

    disorting = ['WestDown','EastDown','WestUp','EastUp','SouthDown','NorthUp','SouthUp','NorthDown']
    marksorting = ['FirstMarkUp','FirstMarkDown','SecondMarkUp','SecondMarkDown']
    measorder = ['FirstMarkUp','FirstMarkUp','FirstMarkDown','FirstMarkDown', 'EastUp','EastUp','WestUp','WestUp','EastDown','EastDown','WestDown','WestDown', 'SecondMarkUp','SecondMarkUp','SecondMarkDown','SecondMarkDown', 'SouthUp','SouthUp','NorthDown','NorthDown','SouthDown','SouthDown','NorthUp','NorthUp']

    takef = True
    flist = []
    fcorr = 0.0

    ## cycling though data set, metadata is not yet considered
    ## #######################################################
    for idx, elem in enumerate(dataset):
        #print ("New element: {} : {}".format(idx, elem))
        if elem=='data':
            print ("Found {} datasets".format(len(dataset[elem])))
            for datadict in dataset[elem]:
                #print (dataset[elem][0])
                #datadict = dataset[elem][0]
                for el in datadict:
                    if not el == 'readings':
                        # get readings only after all other infos have been initialized
                        # ######################################################## 
                        if el == 'theodolite': #{u'serial': u'109648', u'id': 2}
                            valdict = datadict[el]
                            di_inst = 'theodolite{}_{}_{}'.format(valdict.get('id',''),valdict.get('serial',''),'0001')
                        if el == 'proton_temperature':
                            # not yet contained in dirow
                            pass
                        if el == 'observer': # Teresa
                            person = datadict[el]
                        if el == 'elect_temperature': # None
                            # not yet contained in dirow
                            pass
                        if el == 'outside_temperature': # None
                            # not yet contained in dirow
                            pass
                        if el == 'mark': # {u'id': 1, u'name': u'AZ', u'azimuth': 199.1383}
                            valdict = datadict[el]
                            azimuth = float('{}'.format(valdict.get('azimuth','')))
                        if el == 'time': # 2016-01-29T19:48:52Z
                            inputdate = date2num(datetime.strptime(datadict[el].strip('Z'),"%Y-%m-%dT%H:%M:%S"))
                            #headers['analysisdate'] = adate
                        if el == 'pier_temperature': # 22.7
                            t = datadict[el]
                        if el == 'reviewer': # Jake Morris
                            # not yet contained in dirow
                            pass
                        if el == 'electronics': # {u'serial': u'0110', u'id': 1}
                            valdict = datadict[el]
                            fluxgatesensor = 'electronics{}_{}'.format(valdict.get('id',''),valdict.get('serial',''))
                        if el == 'reviewed': # True
                            # not yet contained in dirow
                            pass
                        if el == 'pier': # {u'correction': -22, u'id': 214, u'name': u'MainPCDCP'}
                            valdict = datadict[el]
                            pier = '{}'.format(valdict.get('name',''))
                            fcorr = valdict.get('correction',0.0)
                        if el == 'id':
                            # not yet contained in dirow
                            pass
                        if el == 'flux_temperature': # None
                            # not yet contained in dirow
                            pass
                for el in datadict: # Now only extract readings
                    if el == 'readings':
                        # get readings now
                        # ######################################################## 
                        valdict = datadict[el]
                        #print ("Length",len(valdict))
                        for setcount, dataset in enumerate(valdict):
                            # for each set add a new di row structure to abslist
                            # ######################################################## 
                            if not setcount==0:
                                abslist.append(dirow)
                            dirow = DILineStruct(24)
                            dirow.pier = pier
                            dirow.di_inst = di_inst
                            dirow.fluxgatesensor = fluxgatesensor
                            dirow.inputdate = inputdate
                            dirow.azimuth = azimuth
                            dirow.person = person
                            dirow.t = t
                            if setcount > -1:   # only for testing purposes 
                                for ds in dataset:
                                    #print (ds, dataset[ds])
                                    if ds == 'measurements':
                                        measurements = dataset[ds]
                                        for meas in measurements:
                                            #print ("Here", meas.get('type',''))
                                            typ = meas.get('type','')
                                            if typ in marksorting:
                                                for idx,check in enumerate(measorder):
                                                    if typ == check:
                                                        dirow.hc[idx] = meas.get('angle','')
                                            if typ in disorting:
                                                angle = meas.get('angle','')
                                                time = meas.get('time','')
                                                dtime = datetime.fromtimestamp(int(time))
                                                for idx,check in enumerate(measorder):
                                                    if typ == check:
                                                        # TODO Get hy and vc correctly
                                                        if typ.startswith('South') or typ.startswith('North'):
                                                            dirow.vc[idx] = float(meas.get('angle',0.0))
                                                            if typ.endswith('Up'):
                                                                dirow.hc[idx] = 90.0
                                                            else:
                                                                dirow.hc[idx] = 270.0
                                                        if typ.startswith('East') or typ.startswith('West'):
                                                            if typ.startswith('South'):
                                                                dirow.vc[idx] = 180.0
                                                            else:
                                                                dirow.vc[idx] = 0.0
                                                            dirow.hc[idx] = float(meas.get('angle',0.0))
                                                        dirow.res[idx] = float(meas.get('residual',0.0))
                                                        dirow.time[idx] = date2num(dtime)
                                                if takef:
                                                    flist.append([date2num(dtime),float(meas.get('f',''))+fcorr])

                            if takef and len(flist) > 0:
                                # sort flist
                                for fl in flist:
                                    dirow.ftime.append(fl[0])
                                    dirow.f.append(fl[1])
                                flist = [] # cleanup

    abslist.append(dirow)

    return abslist


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
