'''
Path:			magpy.lib.format_lemi
Part of package:	stream (read/write)
Type:			Input filter, part of read library

PURPOSE:
	Auxiliary input filter for Lemi data.

CONTAINS:
        isLEMIBIN:	(Func) Checks if file is LEMI format binary file.
	readLEMIBIN:	(Func) Reads current LEMI data format binary files.
        isLEMIBIN1:	(Func) Checks if file is LEMI format data file.
	readLEMIBIN1:	(Func) Reads outdated LEMI data format binary files.
        isLEMIHF:	(Func) Checks if file is LEMI format data file.
	readLEMIHF:	(Func) Reads outdated LEMI data format text files.

DEPENDENCIES:
	None.

CALLED BY:
	magpy.lib.magpy_formats
'''

from stream import *

def h2d(x):
    '''
    Hexadecimal to decimal (for format LEMIBIN2)
    Because the binary for dates is in binary-decimal, not just binary.
    '''
    y = int(x/16)*10 + x%16
    return y

def isLEMIHF(filename):
    '''
    Checks whether a file is ASCII Lemi txt file format.
    '''
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    elem = temp.split()
    if len(elem) == 13:
        try:
            testtime = datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2],'%Y-%m-%d')
        except:
            return False
    else:
        return False

    loggerlib.info("format_lemi: Found Lemi 10Hz ascii file %s." % filename)
    return True


def isLEMIBIN1(filename):
    '''
    Checks whether a file is Binary Lemi file format.
    '''
    try:
        temp = open(filename, 'rb').read(32)
        data= struct.unpack('<4cb6B11Bcbbhhhb', temp)
    except:
        return False
    try:
        if not data[0] == 'L':
            return False
        if not data[22] in (['A','P']):
            return false
    except:
        return False

    loggerlib.info("format_lemi: Found Lemi 10Hz binary file %s." % filename)
    return True


def isLEMIBIN(filename):
    '''
    Checks whether a file is Binary Lemi025 file format. (2nd format. Used at Conrad Observatory.)
    '''
    try:
        temp = open(filename, 'rb').read(169)
        data= struct.unpack('<4cb6B8hb30f3BcB6hL', temp)
    except:
        return False
    try:
        if not data[0] == 'L':
            return False
        if not data[53] in (['A','P']):
            return false
    except:
        return False

    loggerlib.info("format_lemi: Found Lemi 10Hz binary file %s." % filename)
    return True


def readLEMIHF(filename, headonly=False, **kwargs):
    '''
    Reading IAGA2002 LEMI format data.
    '''

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header information is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None

    # get day from filename (platform independent)
    # --------------------------------------
    splitpath = os.path.split(filename)
    tmpdaystring = splitpath[1].split('.')[0]
    daystring = re.findall(r'\d+',tmpdaystring)[0]
    if len(daystring) >  8:
        daystring = daystring[:8]
    try:
        day = datetime.strftime(datetime.strptime(daystring, '%Y%m%d'),'%Y-%m-%d')
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        loggerlib.warning("readLEMIHF: Wrong dateformat in Filename %s." % filename)
        pass

    if getfile:
        loggerlib.info('readLEMIHF: Reading %s...' % (filename))
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                elem = line.split()
                row.time = date2num(datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2]+'T'+elem[3]+':'+elem[4]+':'+elem[5],'%Y-%m-%dT%H:%M:%S.%f'))
                row.x = float(elem[6])
                row.y = float(elem[7])
                row.z = float(elem[8])
                stream.add(row)         
        headers['col-x'] = 'x'
        headers['unit-col-x'] = 'nT'
        headers['col-y'] = 'y'
        headers['unit-col-y'] = 'nT'
        headers['col-z'] = 'z'
        headers['unit-col-z'] = 'nT'
    else:
        headers = stream.header
        stream =[]

    fh.close()

    return DataStream(stream, headers)


def readLEMIBIN(filename, headonly=False, **kwargs):
    '''
    Function for reading current data format of LEMI data.

    KWARGS:
        tenHz:		(bool) to use 10Hz data
        timeshift:	(float) given time shift of GPS reading

    COMPLETE DATA STRUCTURE:'<4cb6B8hb30f3BcBcc5hL'
     --TAG:            data[0:4]		# L025
     --TIME (LEMI):    2000+h2d(data[5]),h2d(data[6]),h2d(data[7]),h2d(data[8]),h2d(data[9]),h2d(data[10])
     --T (sensor):     data[11]/100.
     --T (electr.):    data[12]/100.
     --BIAS:           data[13],data[14],data[15]
     --BIAS FIELD:     data[16]/400.,data[17]/400.,data[18]/400.
     --(EMPTY)         data[19]
     --DATA1:          data[20]*1000.,data[21]*1000.,data[22]*1000.
     --DATA2:          data[23]*1000.,data[24]*1000.,data[25]*1000.
     --DATA3:          data[26]*1000.,data[27]*1000.,data[28]*1000.
     --DATA4:          data[29]*1000.,data[30]*1000.,data[31]*1000.
     --DATA5:          data[32]*1000.,data[33]*1000.,data[34]*1000.
     --DATA6:          data[35]*1000.,data[36]*1000.,data[37]*1000.
     --DATA7:          data[38]*1000.,data[39]*1000.,data[40]*1000.
     --DATA8:          data[41]*1000.,data[42]*1000.,data[43]*1000.
     --DATA9:          data[44]*1000.,data[45]*1000.,data[46]*1000.
     --DATA10:         data[47]*1000.,data[48]*1000.,data[49]*1000.
     --MODE:           data[50]		# Mode: 1, 2 or 3
     --FLASH % FREE:   data[51]	
     --BATTERY (V):    data[52]	
     --GPS STATUS:     data[53]		# A (active) or P (passive)
     --(EMPTY)         data[54]
     --TIME (PC):      2000+data[55],data[56],data[57],data[58],data[59],data[60],data[61]
    '''

    # Reading Lemi025 Binary format data.
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    tenHz = kwargs.get('tenHz')
    timeshift = kwargs.get('timeshift')
    gpstime = kwargs.get('gpstime')

    # Define frequency of output data:
    if not tenHz:
        tenHz = False # True # 		# Currently gives memory errors for t > 1 day. 10Hz stream too large? TODO  happens only when start and endtime are used.. single files can be imported
    if not timeshift:
        timeshift = -300 #milliseconds
    if not gpstime:
        gpstime = False # if true then PC time will be saved to the sectime column and gps time will occupy the time column 

    # Check whether its the new (with ntp time) or old (without ntp) format
    temp = open(filename, 'rb').read(169)
    data= struct.unpack('<4cb6B8hb30f3BcBcc5hL', temp)

    if data[55] == 'L':
        # old format
	loggerlib.info("readLEMIBIN: Format is the out-dated lemi format.")
        packcode = '<4cb6B8hb30f3BcB'
        linelength = 153
        stime = False
    else:
        # new format
	loggerlib.info("readLEMIBIN: Format is the current lemi format.")
        packcode = '<4cb6B8hb30f3BcB6hL'
        linelength = 169
        stime = True

    fh = open(filename, 'rb')

    stream = DataStream()

    # Check whether header information is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None

    theday = extractDateFromString(filename)
    try:
        if starttime:
            if not theday >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not theday <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        getfile = True 

    if getfile:

        loggerlib.info('readLEMIBIN: Reading %s...' % (filename))
        headers['col-x'] = 'x'
        headers['unit-col-x'] = 'nT'
        headers['col-y'] = 'y'
        headers['unit-col-y'] = 'nT'
        headers['col-z'] = 'z'
        headers['unit-col-z'] = 'nT'
        headers['col-t1'] = 'Ts'
        headers['unit-col-t1'] = 'deg'
        headers['col-t2'] = 'Te'
        headers['unit-col-t2'] = 'deg'
        headers['col-var1'] = 'Voltage'
        headers['unit-col-var1'] = 'V'

        timediff = []

	line = fh.read(linelength)

	while line != '':
            try:
                data= struct.unpack(packcode,line)
            except Exception as e:
                loggerlib.warning('readLEMIBIN: Error reading data. There is probably a broken line.')
                loggerlib.warning('readLEMIBIN: Error string: "%s"' % e)
                loggerlib.warning('readLEMIBIN: Aborting data read.')
                line = ''
            bfx = data[16]/400.
            bfy = data[17]/400.
            bfz = data[18]/400.

            headers['DataCompensationX'] = bfx
            headers['DataCompensationY'] = bfy
            headers['DataCompensationZ'] = bfz
            headers['SensorID'] = line[0:4]

            if gpstime:
                time = datetime(2000+h2d(data[5]),h2d(data[6]),h2d(data[7]),h2d(data[8]),h2d(data[9]),h2d(data[10]))+timedelta(microseconds=timeshift*1000.)  # Lemi GPS time 
                sectime = datetime(2000+data[55],data[56],data[57],data[58],data[59],data[60],data[61])			# PC time
                timediff.append((date2num(time)-date2num(sectime))*24.*3600.) # in seconds 
            else:
                time = datetime(2000+data[55],data[56],data[57],data[58],data[59],data[60],data[61])			# PC time

            if tenHz:
                for i in range(10):
                    row = LineStruct()

                    row.time = date2num(time+timedelta(microseconds=(100000.*i)))
                    row.t1 = data[11]/100.
                    row.t2 = data[12]/100.

                    row.x = (data[20+i*3])*1000.
                    row.y = (data[21+i*3])*1000.
                    row.z = (data[22+i*3])*1000.
                    row.var1 = data[52]/10.	# Voltage information
                    if gpstime:
                        row.sectime = date2num(sectime+timedelta(microseconds=(100000.*i)))

                    stream.add(row)

            else:
    	        newtime = []
                row = LineStruct()   

                row.time = date2num(time)
                row.t1 = data[11]/100.
                row.t2 = data[12]/100.

                row.x = (data[20])*1000.
                row.y = (data[21])*1000.
                row.z = (data[22])*1000.
                row.var1 = data[52]/10.
                if gpstime:
                    row.sectime = date2num(sectime)

                stream.add(row)    

    	    line = fh.read(linelength)

    fh.close()
    if gpstime:
        loggerlib.info("readLEMIBIN2: Time difference between GPS and PC (GPS-PC): %f , %f" % (np.mean(timediff), np.std(timediff)))
        print "Time difference between GPS and PC (GPS-PC):", np.mean(timediff), np.std(timediff) 
   
    return DataStream(stream, headers)    


def readLEMIBIN1(filename, headonly=False, **kwargs):
    '''
    Function for reading LEMI format data.
    NOTE: This function reads an outdated data format.
    Timeshift of ~0.3 seconds must be accounted for.
    (This timeshift is corrected for in current acquisition.lemiprotocol.)
    '''

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rb')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None

    theday = extractDateFromString(filename)
    try:
        if starttime:
            if not theday >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not theday <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True 

    if getfile:
        loggerlib.info('readLEMIBIN1: Reading %s' % (filename))
        headers['col-x'] = 'x'
        headers['unit-col-x'] = 'nT'
        headers['col-y'] = 'y'
        headers['unit-col-y'] = 'nT'
        headers['col-z'] = 'z'
        headers['unit-col-z'] = 'nT'

	line = fh.read(32)
	while line != '':
    	    data= struct.unpack("<4cb6B11Bcbbhhhb", line)
    	    bfx = data[-4]/400.
    	    bfy = data[-3]/400.
    	    bfz = data[-2]/400.
            headers['DataCompensationX'] = bfx
            headers['DataCompensationY'] = bfy
            headers['DataCompensationZ'] = bfz
            headers['SensorID'] = line[0:4]
    	    newtime = []
    	    for i in range (5,11):
        	newtime.append(h2d(data[i]))
    	    currsec = newtime[-1]
    	    newtime.append(0.0)
    	    for i in range (0,30):
        	row = LineStruct()
        	line = fh.read(16)
        	data= struct.unpack('<3f2h', line)
        	microsec = i/10.
        	if microsec >= 2:
            	    secadd = 2.
        	elif microsec >= 1:
            	    secadd = 1.
        	else:
            	    secadd = 0.
       		newtime[-1] = microsec-secadd
  	      	newtime[-2] = currsec+secadd
        	time = datetime(2000+newtime[0],newtime[1],newtime[2],newtime[3],newtime[4],int(newtime[5]),int(newtime[6]*1000000))
                row.time = date2num(time)
                row.x = (data[0])*1000.
                row.y = (data[1])*1000.
                row.z = (data[2])*1000.
                row.t1 = data[3]/100.
                row.t2 = data[4]/100.
                stream.add(row)         
    	    line = fh.read(32)

    fh.close()

    #print "Finished file reading of %s" % filename

    return DataStream(stream, headers) 

