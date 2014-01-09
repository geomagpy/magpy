"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test, read and write function
"""

from stream import *


def isNOAAACE(filename):
    """
    Checks whether a file is NOAA ACE format.
    """
    try:
        tempf = open(filename, 'rt')
        temp1= tempf.readline()
        temp2= tempf.readline()
        temp3= tempf.readline()
    except:
        return False
    if not temp1.startswith(':'):
        return False
    if not 'NOAA' in temp3:
        return False
    return True



def readNOAAACE(filename, headonly=False, **kwargs):
    """
    Reading NOAA Ace format data.
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None

    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    try:
        splitname = splitpath[1].split('_')
	print splitname
	if len(splitname) == 3: # file is current file
	    tmpdaystring = datetime.strftime(datetime.utcnow(),'%Y%m%d')
            datatype = splitpath[1].split('_')[1]
        elif len(splitname) == 4:
            tmpdaystring = splitpath[1].split('_')[0]
            datatype = splitpath[1].split('_')[2]
        elif len(splitname) == 5:
            tmpdaystring = splitpath[1].split('_')[1]
            datatype = splitpath[1].split('_')[2]
        elif len(splitname) == 6:
            tmpdaystring = splitpath[1].split('_')[2]
            datatype = splitpath[1].split('_')[2]
        daystring = re.findall(r'\d+',tmpdaystring)[0]
        day = datetime.strftime(datetime.strptime(daystring, "%Y%m%d"),"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        if splitpath[0] == '/tmp':
            # internet file accessed
            pass 
        else:
            logging.warning("Could not identify typical NOAA date in %s. Reading all ..." % daystring)
        getfile = True

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith(':'):
                # data info
                pass
            elif line.startswith('#'):
                if line.startswith('# Units:'):
                    unitline = line.replace('# Units:','')
                    unitelem = unitline.split()
                    unit = unitelem[-1]
                    colname = ''
                    for i in range(len(unitelem)-1):
                        colname += unitelem[i]
                    if colname == 'Protondensity':
                        stream.header['col-x'] = colname
                        stream.header['unit-col-x'] = unit
                    if colname == 'Iontempeturedegrees':
                        stream.header['col-z'] = 'Ion temperature'
                        stream.header['unit-col-z'] = unit
                    if colname == 'Bulkspeed':
                        stream.header['col-y'] = 'Solar wind speed'
                        stream.header['unit-col-y'] = unit
                    if colname == 'protonflux':
                        stream.header['col-t1'] = 'Proton flux > 10 MeV'
                        stream.header['col-t2'] = 'Proton flux > 30 MeV'
                        stream.header['unit-col-t1'] = unit
                        stream.header['unit-col-t2'] = unit
                    if colname == 'DifferentialFlux':
                        stream.header['col-f'] = 'Electron flux 38-53'
                        stream.header['col-df'] = 'Electron flux 175-315'
                        stream.header['unit-col-f'] = unit
                        stream.header['unit-col-df'] = unit
                        stream.header['col-dx'] = 'Proton flux 47-68'
                        stream.header['col-dy'] = 'Proton flux 115-195'
                        stream.header['col-dz'] = 'Proton flux 310-580'
                        stream.header['col-var3'] = 'Proton flux 795-1193'
                        stream.header['col-var4'] = 'Proton flux 1060-1900'
                        stream.header['unit-col-dx'] = unit
                        stream.header['unit-col-dy'] = unit
                        stream.header['unit-col-dz'] = unit
                        stream.header['unit-col-var3'] = unit
                        stream.header['unit-col-var4'] = unit
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                dataelem = line.split()
                date = datetime(int(dataelem[0]),int(dataelem[1]),int(dataelem[2]),int(dataelem[3][:2]),int(dataelem[3][2:]))
                status = int(dataelem[6])
                row.time = date2num(date)
                if status == 0: # indicates good data
                    if datatype == 'swepam':
                        if (float(dataelem[7]) > -9999): 
                            row.x = float(dataelem[7])
                            #stream.header['col-x'] = headercol[0]
                        if (float(dataelem[8]) > -9999): 
                            row.y = float(dataelem[8])
                        if (float(dataelem[9]) > -9999): 
                            row.z = float(dataelem[9])
                    elif datatype == 'sis':
                        if (float(dataelem[7]) > -9999): 
                            row.t1 = float(dataelem[7])
                        if (float(dataelem[8]) == 0): # status of high energy 
                            if (float(dataelem[9]) > -9999): 
                                row.t2 = float(dataelem[9])
                    elif datatype == 'mag':
                        pass
                    elif datatype == 'epam':
                        if (float(dataelem[7]) > -9999): 
                            row.f = float(dataelem[7])
                        if (float(dataelem[8]) > -9999): 
                            row.df = float(dataelem[8])
                        if (float(dataelem[9]) == 0): 
                            if (float(dataelem[10]) > -9999): 
                                row.dx = float(dataelem[10])
                            if (float(dataelem[11]) > -9999): 
                                row.dy = float(dataelem[11])
                            if (float(dataelem[12]) > -9999): 
                                row.dz = float(dataelem[12])
                            if (float(dataelem[13]) > -9999): 
                                row.var3 = float(dataelem[13])
                            if (float(dataelem[14]) > -9999): 
                                row.var4 = float(dataelem[14])
                stream.add(row)
    fh.close()

    return DataStream(stream, headers)    

