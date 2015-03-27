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
    loggerlib.info("format_noaa: Found ACE file %s" % filename)
    return True


def isOMNIACE(filename):
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
    if not 'BEGIN METADATA' in temp1:
        return False
    if not 'ACE' in temp3:
        return False
    loggerlib.info("format_omni: Found OMNI ACE file %s" % filename)
    return True


def readNOAAACE(filename, headonly=False, **kwargs):
    """
    Reading NOAA Ace format data.
    ACE data streams use following key formats:
	1min Data:
        ----------
	*MAG:
	Bx [nT]					x
	By [nT]					y
	Bz [nT]					z
	Bt [nT]					f
	Lat [degrees +/- 90.0]			t1
	Lon [degrees 0.0 - 360.0]		t2
	*SWEPAM:
	Proton density [p/cc]			var1
	Bulk solar wind speed [km/s]		var2
	Ion temperature [degrees K]		var3

	5min data:
        ----------
	*SIS:
	Integral proton flux > 10 MeV [p/cs2-sec-ster]				x
	Integral proton flux > 30 MeV [p/cs2-sec-ster]				y
	*EPAM:
	Differential Electron 38-53 Flux [particles/cm2-s-ster-MeV]		z
	Differential Electron 175-315 Flux [particles/cm2-s-ster-MeV]		f
	Differential Proton 47-68 keV Flux [particles/cm2-s-ster-MeV]		var1
	Differential Proton 115-195 keV Flux [particles/cm2-s-ster-MeV]		var2
	Differential Proton 310-580 keV Flux [particles/cm2-s-ster-MeV]		var3
	Differential Proton 795-1193 keV Flux [particles/cm2-s-ster-MeV]	var4
	Differential Proton 1060-1900 keV Flux [particles/cm2-s-ster-MeV]	var5
          
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    # Use only clean data (status var S = 0)
    cleandata = kwargs.get('cleandata')
    getfile = True

    if cleandata == None:
        cleandata = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    #stream = DataStream()
    stream = DataStream([],{})
    # Check whether header infromation is already present
    #if stream.header is None:
    #    headers = {}
    #else:
    #    headers = stream.header
    #data = []
    #key = None

    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    try:
        splitname = splitpath[1].split('_')
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
        loggerlib.info('readNOAAACE: Reading %s' % (filename))
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
                    # NOTE: Some keys repeat themselves. They are ordered so that they do not
		    # double in 1min and 5min combined datasets.
                    for i in range(len(unitelem)-1):
                        colname += unitelem[i]
                    if colname == 'Protondensity':
                        stream.header['col-var1'] = colname
                        stream.header['unit-col-var1'] = unit
                    if colname == 'Bulkspeed':
                        stream.header['col-var2'] = 'Solar wind speed'
                        stream.header['unit-col-var2'] = unit
                    if colname == 'Iontempeturedegrees':
                        stream.header['col-var3'] = 'Ion temperature'
                        stream.header['unit-col-var3'] = unit
                    if colname == 'protonflux':
                        stream.header['col-x'] = 'Integral Proton flux > 10 MeV'
                        stream.header['col-y'] = 'Integral Proton flux > 30 MeV'
                        stream.header['unit-col-x'] = unit
                        stream.header['unit-col-y'] = unit
                    if colname == 'DifferentialFlux':
                        stream.header['col-z'] = 'Diff Electron flux 38-53'
                        stream.header['col-f'] = 'Diff Electron flux 175-315'
                        stream.header['unit-col-z'] = unit
                        stream.header['unit-col-f'] = unit
                        stream.header['col-var1'] = 'Diff Proton flux 47-68 keV'
                        stream.header['col-var2'] = 'Diff Proton flux 115-195 keV'
                        stream.header['col-var3'] = 'Diff Proton flux 310-580 keV'
                        stream.header['col-var4'] = 'Diff Proton flux 795-1193 keV'
                        stream.header['col-var5'] = 'Diff Proton flux 1060-1900 keV'
                        stream.header['unit-col-var1'] = unit
                        stream.header['unit-col-var2'] = unit
                        stream.header['unit-col-var3'] = unit
                        stream.header['unit-col-var4'] = unit
                        stream.header['unit-col-var5'] = unit
                    if colname == 'Bx,By,Bz,Btin':
                        stream.header['col-x'] = 'Bx'
                        stream.header['col-y'] = 'By'
                        stream.header['col-z'] = 'Bz'
                        stream.header['col-f'] = 'Bt'
                        stream.header['unit-col-x'] = unit
                        stream.header['unit-col-y'] = unit
                        stream.header['unit-col-z'] = unit
                        stream.header['unit-col-f'] = unit
                        stream.header['col-t1'] = 'Lat'
                        stream.header['col-t2'] = 'Lon'
                        stream.header['unit-col-t1'] = 'degrees'
                        stream.header['unit-col-t2'] = 'degrees'
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                dataelem = line.split()
                date = datetime(int(dataelem[0]),int(dataelem[1]),int(dataelem[2]),int(dataelem[3][:2]),int(dataelem[3][2:]))
                status = int(dataelem[6])
                row.str1 = status
                row.time = date2num(date)
                if cleandata == True:
                    if status == 0: # indicates good data
                        usedata = True
                    else:
                        usedata = False
                elif cleandata == False:
                    usedata = True

                if usedata:
                    if datatype == 'swepam':
                        if (float(dataelem[7]) > -9999): 
                            row.var1 = float(dataelem[7])
                            #stream.header['col-x'] = headercol[0]
                        if (float(dataelem[8]) > -9999): 
                            row.var2 = float(dataelem[8])
                        if (float(dataelem[9]) > -9999): 
                            row.var3 = float(dataelem[9])
                    elif datatype == 'sis':
                        if (float(dataelem[7]) > -9999): 
                            row.x = float(dataelem[7])
                        if (float(dataelem[8]) == 0): # status of high energy 
                            if (float(dataelem[9]) > -9999): 
                                row.y = float(dataelem[9])
                    elif datatype == 'mag':
                        if (float(dataelem[7]) > -9999): 
                            row.x = float(dataelem[7])
                        if (float(dataelem[8]) > -9999): 
                            row.y = float(dataelem[8])
                        if (float(dataelem[9]) > -9999): 
                            row.z = float(dataelem[9])
                        if (float(dataelem[10]) > -9999): 
                            row.f = float(dataelem[10])
                        if (float(dataelem[11]) > -9999): 
                            row.t1 = float(dataelem[11])
                        if (float(dataelem[12]) > -9999): 
                            row.t2 = float(dataelem[12])
                    elif datatype == 'epam':
                        if (float(dataelem[7]) > -9999): 
                            row.z = float(dataelem[7])
                        if (float(dataelem[8]) > -9999): 
                            row.f = float(dataelem[8])
                        if (float(dataelem[9]) == 0): 
                            if (float(dataelem[10]) > -9999): 
                                row.var1 = float(dataelem[10])
                            if (float(dataelem[11]) > -9999): 
                                row.var2 = float(dataelem[11])
                            if (float(dataelem[12]) > -9999): 
                                row.var3 = float(dataelem[12])
                            if (float(dataelem[13]) > -9999): 
                                row.var4 = float(dataelem[13])
                            if (float(dataelem[14]) > -9999): 
                                row.var5 = float(dataelem[14])
                stream.add(row)
    fh.close()

    return stream
    #return DataStream(stream, headers)  

