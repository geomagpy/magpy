"""
MagPy
Auxiliary input filter - Lemi data
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
For logging use:
logging.debug("lib - format_lemi: Found Lemi Binary file %s" % filename)

"""

from core.magpy_stream import *


def correct_bin_time(time):
    """
    Used to correct the time reading of the LEMI025 binary type
    The binary type puts any date record in a range from 0 to 90 (except 0 to 60)
    jumps of 6 occur between 9 and 16, 26 and 32, 42 and 48, 60 and 66, 
    """ 
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
    """
    Checks whether a file is ASCII Lemi txt file format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    elem = temp.split()
    if len(elem) == 13:
        try:
            testtime = datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2],"%Y-%m-%d")
        except:
            return False
    else:
        return False
    logging.debug("lib - format_lemi: Found Lemi 10Hz ascii file %s" % filename)
    return True


def isLEMIBIN(filename):
    """
    Checks whether a file is Binary Lemi file format.
    """
    try:
        temp = open(filename, 'rb').read(32)
        data= struct.unpack("<4cb6B11Bcbbhhhb", temp)
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

def readLEMIHF(filename, headonly=False, **kwargs):
    """
    Reading IAGA2002 format data.
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
    # --------------------------------------
    splitpath = os.path.split(filename)
    tmpdaystring = splitpath[1].split('.')[0]
    daystring = re.findall(r'\d+',tmpdaystring)[0]
    if len(daystring) >  8:
        daystring = daystring[:8]
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%Y%m%d"),"%Y-%m-%d")
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
                row.time = date2num(datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2]+'T'+elem[3]+':'+elem[4]+':'+elem[5],"%Y-%m-%dT%H:%M:%S.%f"))
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
	while line != "":
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
    	    #print bfx,bfy,bfz
    	    currsec = newtime[-1]
    	    newtime.append(0.0)
    	    for i in range (0,30):
        	row = LineStruct()
        	line = fh.read(16)
        	data= struct.unpack("<3f2h", line)
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
        	#print time, (data[0]-bfx)*1000., (data[1]-bfy)*1000., (data[2]-bfz)*1000., data[3]/100., data[4]/100. 
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

