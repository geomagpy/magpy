'''
MagPy
Auxiliary input filter - Lemi data
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
For logging use:
logging.debug(lib - format_lemi: Found Lemi Binary file %s % filename)
'''


from core.magpy_stream import *


def correct_bin_time(time):
    '''
    Used to correct the time reading of the LEMI025 binary type
    The binary type puts any date record in a range from 0 to 90 (except 0 to 60)
    jumps of 6 occur between 9 and 16, 26 and 32, 42 and 48, 60 and 66, 
    '''
    if time < 10:
        tmp = time
    elif 10 < time < 30:
        tmp = time-6
    elif 30 < time < 45:
        tmp = time-12
    elif 45 < time < 60:
        tmp = time-18
    elif 60 < time < 79:
        tmp = time-24
    else:
        tmp = time-30   
    return tmp

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
    logging.debug("lib - format_lemi: Found Lemi 10Hz ascii file %s" % filename)
    return True


def isLEMIBIN(filename):
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
    logging.debug("lib - format_lemi: Found Lemi Binary file %s" % filename)
    return True

def isLEMIBIN2(filename):
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
    logging.debug("lib - format_lemi: Found Lemi Binary file %s" % filename)
    return True

def readLEMIHF(filename, headonly=False, **kwargs):
    '''
    Reading IAGA2002 format data.
    '''
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
        logging.warning("Wrong dateformat in Filename %s" % filename)
        pass

    if getfile:
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

    print "Finished file reading of %s" % filename

    return DataStream(stream, headers)


def readLEMIBIN(filename, headonly=False, **kwargs):
    #Reading Lemi Binary format data.
    # Timeshift of ~0.3 seconds must be regarded for 
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

    if getfile:

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
            headers['Compensation-x'] = bfx
            headers['Compensation-y'] = bfy
            headers['Compensation-z'] = bfz
    	    newtime = []
    	    for i in range (5,11):
        	newtime.append(correct_bin_time(data[i]))
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
                row.x = (data[0]-bfx)*1000.
                row.y = (data[1]-bfy)*1000.
                row.z = (data[2]-bfz)*1000.
                row.t1 = data[3]/100.
                row.t2 = data[4]/100.
                stream.add(row)         
    	    line = fh.read(32)

    fh.close()

    print "Finished file reading of %s" % filename

    return DataStream(stream, headers) 


def h2d(x):		# Hexadecimal to decimal (for format LEMIBIN2)
    y = int(x/16)*10 + x%16		# Because the binary for dates is in binary-decimal, not just binary.
    return y


def readLEMIBIN2(filename, headonly=False, **kwargs):

    '''
    # COMPLETE DATA STRUCTURE:
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

    Data is in 10Hz, currently only putting 1Hz data into stream.
    '''

    #Reading Lemi025 Binary format data.
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

    if getfile:

        headers['col-x'] = 'x'
        headers['unit-col-x'] = 'nT'
        headers['col-y'] = 'y'
        headers['unit-col-y'] = 'nT'
        headers['col-z'] = 'z'
        headers['unit-col-z'] = 'nT'

	line = fh.read(169)

	while line != '':
            data= struct.unpack("<4cb6B8hb30f3BcB6hL",line)

    	    bfx = data[16]/400.
    	    bfy = data[17]/400.
    	    bfz = data[18]/400.
            headers['Compensation-x'] = bfx
            headers['Compensation-y'] = bfy
            headers['Compensation-z'] = bfz
    	    newtime = []

            row = LineStruct()   
            time = datetime(2000+h2d(data[5]),h2d(data[6]),h2d(data[7]),h2d(data[8]),h2d(data[9]),h2d(data[10]),int(0.0*1000000))
            row.time = date2num(time)
            row.x = (data[20]-bfx)*1000.
            row.y = (data[21]-bfy)*1000.
            row.z = (data[22]-bfz)*1000.
            row.t1 = data[11]/100.
            row.t2 = data[12]/100.
            correction = 0.0 # 31.447826372
            row.f = (row.x**2.+row.y**2.+row.z**2.)**.5	+ correction	#TODO

            stream.add(row)    

    	    line = fh.read(169)

    fh.close()

    print "Finished file reading of %s" % filename
   
    return DataStream(stream, headers)    

