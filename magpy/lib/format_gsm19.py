"""
MagPy
GSM 19 input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function

from magpy.stream import *

def int_to_roman(input):
   """
   Convert an integer to Roman numerals.

   taken from:
   http://code.activestate.com/recipes/81611-roman-numerals/

   Examples:
   >>> int_to_roman(0)
   Traceback (most recent call last):
   ValueError: Argument must be between 1 and 3999

   >>> int_to_roman(-1)
   Traceback (most recent call last):
   ValueError: Argument must be between 1 and 3999

   >>> int_to_roman(1.5)
   Traceback (most recent call last):
   TypeError: expected integer, got <type 'float'>

   >>> for i in range(1, 21): print int_to_roman(i)
   ...
   I
   II
   III
   IV
   V
   VI
   VII
   VIII
   IX
   X
   XI
   XII
   XIII
   XIV
   XV
   XVI
   XVII
   XVIII
   XIX
   XX
   >>> print int_to_roman(2000)
   MM
   >>> print int_to_roman(1999)
   MCMXCIX
   """
   if type(input) != type(1):
      raise TypeError("expected integer, got %s" % type(input))
   if not 0 < input < 4000:
      raise ValueError("Argument must be between 1 and 3999")
   ints = (1000, 900,  500, 400, 100,  90, 50,  40, 10,  9,   5,  4,   1)
   nums = ('M',  'CM', 'D', 'CD','C', 'XC','L','XL','X','IX','V','IV','I')
   result = ""
   for i in range(len(ints)):
      count = int(input / ints[i])
      result += nums[i] * count
      input -= ints[i] * count
   return result


def roman_to_int(input):
   """
   Convert a roman numeral to an integer.

   http://code.activestate.com/recipes/81611-roman-numerals/

   >>> r = range(1, 4000)
   >>> nums = [int_to_roman(i) for i in r]
   >>> ints = [roman_to_int(n) for n in nums]
   >>> print r == ints
   1

   >>> roman_to_int('VVVIV')
   Traceback (most recent call last):
    ...
   ValueError: input is not a valid roman numeral: VVVIV
   >>> roman_to_int(1)
   Traceback (most recent call last):
    ...
   TypeError: expected string, got <type 'int'>
   >>> roman_to_int('a')
   Traceback (most recent call last):
    ...
   ValueError: input is not a valid roman numeral: A
   >>> roman_to_int('IL')
   Traceback (most recent call last):
    ...
   ValueError: input is not a valid roman numeral: IL
   """
   if type(input) != type(""):
      raise TypeError("expected string, got %s" % type(input))
   input = input.upper()
   nums = ['M', 'D', 'C', 'L', 'X', 'V', 'I']
   ints = [1000, 500, 100, 50,  10,  5,   1]
   places = []
   print("Ckeck: ", input)
   for c in input:
      if not c in nums:
         raise ValueError("input is not a valid roman numeral: %s" % input)
   for i in range(len(input)):
      c = input[i]
      value = ints[nums.index(c)]
      # If the next place holds a larger number, this value is negative.
      try:
         nextvalue = ints[nums.index(input[i +1])]
         if nextvalue > value:
            value *= -1
      except IndexError:
         # there is no next place.
         pass
      places.append(value)
   sum = 0
   for n in places: sum += n
   # Easiest test for validity...
   if int_to_roman(sum) == input:
      return sum
   else:
      raise ValueError('input is not a valid roman numeral: %s' % input)


def isGSM19(filename):
    """
    Checks whether a file is GSM19 format.
    """
    try:
        temp = open(filename, 'rt') #, encoding='utf-8', errors='ignore'
    except:
        return False
    try:
        li = temp.readline()
    except:
        return False
    while li.isspace():
        li = temp.readline()
    if not li.startswith('Gem Systems GSM-19'):
        if not li.startswith('/Gem Systems GSM-19'):
            return False
    return True


def readGSM19(filename, headonly=False, **kwargs):
    """
    Reading GSM19 format.
    Basis looks like:

    Gem Systems GSM-19W 3101329 v6.0 24 X 2003
    ID 1 file 23c-ost .b   09 VII10
    datum  48000.00


    115014.0  48484.12 99
    115015.0  48487.04 99
    115016.0  48489.92 99
    115017.0  48488.16 99
    115018.0  48486.06 99
    115019.0  48487.17 99
    115020.0  48487.44 99

    v7.0:
    /Gem Systems GSM-19GWV 7122568 v7.0 15 III 2018 M ewv10fl.v7vbs                      
    /ID 1 file 01survey.b   05 IX 18
    /datum  48500.00
    / 047.9278532   015.8618024 
    /UTC+01
    /33T
    /no AC filter
    /12.3V
    /tune initialize Y auto-tune Y 048.6uT
    /time nT sq 


    WG looks like:
    /Gem Systems GSM-19GWV 7122568 v7.0 4 V 2011 M ewv10fl.v7vbs
    /ID 1 file 24survey.wg  02VIII12
    /00106 sensor distance cm
    /X Y elevation nT nT/m sq cor-nT sat time picket-x picket-y
    line  000017
    047.9263435  015.8653717  001089  48382.35  0012.61 99  000000.00 07 115950.0  4.00  39.00
    047.9263438  015.8653840  001089  48386.04  0017.18 99  000000.00 07 115951.0 * *
    or like (if no GPS):
   /Gem Systems GSM-19GWV 7122568 v7.0 4 V 2011 M ewv10fl.v7vbs
   /ID 1 file 07survey.wg  30  I 14
   /00100 sensor distance cm
   /X Y nT nT/m sq cor-nT time picket-x picket-y
   0  0  48420.59  0002.90 99 * 110804.0  0  0
   0  0  48420.21  0002.58 99 * 110805.0 * *


    """

    #print "Found GEM format"
    #print "-------------------------------------"

    timestamp = os.path.getmtime(filename)
    creationdate = datetime.fromtimestamp(timestamp)
    daytmp = datetime.strftime(creationdate,"%Y-%m-%d")
    YeT = daytmp[:2]

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header information is already present
    headers = {}
    array = [[] for key in KEYLIST]

    indf = KEYLIST.index('f')
    inddf = KEYLIST.index('df')
    indz = KEYLIST.index('z')
    indvar1 = KEYLIST.index('var1')
    indvar2 = KEYLIST.index('var2')
    indvar3 = KEYLIST.index('var3')
    indvar4 = KEYLIST.index('var4')
    indvar5 = KEYLIST.index('var5')

    data = []
    key = None
    logging.info(' Read: %s Format: GSM19' % (filename))
    toffset = 0

    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif line.startswith('Gem Systems GSM-19') or line.startswith('/Gem Systems GSM-19'):
            head = line.split()
            headers['SensorID'] = "{}_{}_0001".format(head[2],head[3])
            headers['SensorName'] = 'GSM19'
            headers['SensorDataLogger'] =  head[2]+head[4]
            headers['SensorDataLoggerSerNum'] =  head[3]
            headers['SensorGroup'] =  'Magnetism'
            headers['SensorType'] =  'Overhauzer'

            headers['SensorDescription'] = 'Gem Systems ' + head[2]+head[4]
            typus = ['b']
            try:
                dayt = filename.strip('.txt')
                day = dayt[-8:-4]+'-'+dayt[-4:-2]+'-'+dayt[-2:]
            except:
                logging.warning("No date data!")
            if line.find(' .b ') > 0 and line.find('v5.0') > 0: # day in this line
                mo = roman_to_int(head[-2])
                da = head[-3]
                ye = head[-1]
                day = str(YeT)+str(ye)+'-'+str(mo).zfill(2)+'-'+str(da)
                #print "GSM19", day
            # data header
            pass
        elif line.startswith('ID') or line.startswith('/ID'):
            tester = line.split('.')
            typus = tester[1].split()
            #logging.debug(' Read: %s Format: GSM19' % (filename))
            #logger.debug("format gsm19: print typus, len(typus)
            #print typus[0]
            # typus[0] can be b, m, g, wg, ...
            if len(typus) == 2:
                da = typus[1][:2]
                yecnt = len(typus[1])-2
                ye = typus[1][yecnt:]
                inp = typus[1][2:yecnt]
                mo = roman_to_int(inp)
                day = str(YeT)+str(ye)+'-'+str(mo)+'-'+str(da)
            elif len(typus) == 3:
                da = typus[1]
                yecnt = len(typus[2])-2
                ye = typus[2][yecnt:]
                inp = typus[2][:yecnt]
                mo = roman_to_int(inp)
                day = str(YeT)+str(ye)+'-'+str(mo)+'-'+str(da)
            else:
                da = typus[1]
                mo = roman_to_int(typus[2])
                day = str(YeT)+str(typus[3])+'-'+str(mo)+'-'+str(da)
        elif line.startswith('datum'):
            # data header
            pass
        elif line.startswith('/ '):
            try:
                vals = line.replace('/ ','').split()
                headers['DataAcquisitionLatitude'] =  float(vals[0].strip())
                headers['DataAcquisitionLongitude'] =  float(vals[1].strip())
                headers['DataLocationReference'] =  'WGS84, EPSG: 4326'
            except:
                pass
        elif line.startswith('/UTC'):
            try:
                vals = line.split('C')
                toffset = float(vals[1])
            except:
                pass
        elif line.find('sensor distance') > 0:
            diststr = line.split()[0]
            dist = int(diststr.strip('/'))/100
            #print "Distance =", dist
        elif headonly:
            # skip data for option headonly
            continue
        else:
            elem = line.split()
            if typus[0]=='b': # a Baseline file
                try:
                    if len(elem) == 3:
                        hour = elem[0][:2]
                        minute = elem[0][2:4]
                        #second = elem[0][4:6]
                        second = elem[0][4:]
                        strtime = datetime.strptime(day+"T"+str(hour)+":"+str(minute),"%Y-%m-%dT%H:%M") + timedelta(seconds=float(second))
                        if not toffset == 0:
                            strtime = strtime - timedelta(hours=toffset)
                        #strtime = datetime.strptime(day+"T"+str(hour)+":"+str(minute)+":"+str(second),"%Y-%m-%dT%H:%M:%S")
                        array[0].append(date2num(strtime))
                        array[indf].append(float(elem[1]))
                        array[indvar5].append(float(elem[2]))
                except:
                    logging.warning("Error in input data: %s - skipping bad value" % filename)
            elif typus[0] == 'g':
                try:
                    hour = elem[6][:2]
                    minute = elem[6][2:4]
                    second = elem[6][4:]
                    # add day
                    strtime = datetime.strptime(day+"T"+str(hour)+":"+str(minute)+":"+str(second),"%Y-%m-%dT%H:%M:%S.%f")
                    array[0].append(date2num(strtime))
                    array[indf].append(float(elem[2]))
                    array[inddf].append(float(elem[3]))
                    array[indvar5].append(float(elem[4]))
                    array[indvar1].append(float(elem[0]))
                    array[indvar2].append(float(elem[1]))
                except:
                    logging.warning("Error in input data: %s - skipping bad value" % filename)
                    pass
            elif typus[0] == 'wg':
                headers['col-var4'] = 'N satellites'
                headers['col-var1'] = 'Latitude'
                headers['col-var2'] = 'Longitude'
                headers['col-var3'] = 'Elevation'
                headers['col-var5'] = 'Quality'
                headers['col-df'] = 'delta f'
                headers['unit-col-df'] = 'nT/m'
                if dist:
                    headers['col-z'] = 'f upper'
                    headers['unit-col-z'] = 'nT'
                try:
                    if elem[0] == '0' or len(elem) == 9: # No GPS data -> no elevation
                       hour = elem[6][:2]
                       minute = elem[6][2:4]
                       second = elem[6][4:]
                       # add day
                       try:
                           strtime = datetime.strptime(day+"T"+str(hour)+":"+str(minute)+":"+str(second),"%Y-%m-%dT%H:%M:%S.%f")
                       except:
                           strtime = datetime.strptime(day+"T"+str(hour)+":"+str(minute)+":"+str(second),"%Y-%m-%dT%H:%M:%S")
                       valf = float(elem[2])
                       valdf = float(elem[3])
                       if dist:
                           array[indz].append(valf-(valdf*dist))
                       array[0].append(date2num(strtime))
                       array[indf].append(float(elem[2]))
                       array[inddf].append(float(elem[3]))
                       array[indvar5].append(float(elem[4]))
                       array[indvar1].append(float(elem[0]))
                       array[indvar2].append(float(elem[1]))
                    else:
                       #print "Test:", elem
                       hour = elem[8][:2]
                       minute = elem[8][2:4]
                       second = elem[8][4:]
                       # add day
                       try:
                           strtime = datetime.strptime(day+"T"+str(hour)+":"+str(minute)+":"+str(second),"%Y-%m-%dT%H:%M:%S.%f")
                       except:
                           strtime = datetime.strptime(day+"T"+str(hour)+":"+str(minute)+":"+str(second),"%Y-%m-%dT%H:%M:%S")
                       valf = float(elem[3])
                       valdf = float(elem[4])
                       if dist:
                           array[indz].append(valf-(valdf*dist))
                       array[0].append(date2num(strtime))
                       array[indf].append(float(elem[3]))
                       array[inddf].append(float(elem[4]))
                       array[indvar5].append(float(elem[5]))
                       array[indvar1].append(float(elem[0]))
                       array[indvar2].append(float(elem[1]))
                       array[indvar3].append(float(elem[2]))
                       array[indvar4].append(float(elem[7]))
                except:
                    #print "Error"
                    logging.warning("Error in input data: %s - skipping bad value" % filename)
                    pass
            else:
                if not len(elem) == 3:
                    logging.error("GSM 19 file - %s - type %s not yet supported" % (filename, typus[0]))

    if len(array[0]) > 0:
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    logging.info("Loaded GSM19 file of type %s, using creation date of file (%s), %d values" % (typus[0],filename,len(stream)))
    fh.close()

    array = [np.asarray(el) for el in array]

    return DataStream([LineStruct()], headers, np.asarray(array))
