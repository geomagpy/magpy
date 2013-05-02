"""
MagPy
WDC (BGS version) input filter
Written by Roman Leonhardt October 2012
- contains test, read and write function for hour data
ToDo: Filter for minute data
"""

from core.magpy_stream import *


def isWDC(filename):
    """
    Checks whether a file is ASCII PMAG format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp[10:12] == "  ":
        return False
    if not len(temp.strip()) == 120: # strip is important to remove eventual \r\n sequences or \n
        return False
    return True


def readWDC(filename, headonly=False, **kwargs):
    """
    Reading WDC format data.
    Looks like:
    WIK1209F01  20 4355059506150605058505950585053504750395034503850385043504850515049505150545056505750575057506050575052
    WIK1209F02  20 4355056505950565055505750515049504150395036503750405041504550565059506250605058506750565053505250495052
    WIK1209F03  20 4355047505050535054505550525045503950315021502050315053505250605064506750685071506550615067505950625052
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True
    
    fh = open(filename, 'rt')

    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    # read file and split text into channels
    li,ld,lh,lx,ly,lz,lf = [],[],[],[],[],[],[]
    code = ''
    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif headonly:
            # skip data for option headonly
            continue
        elif len(line.strip())==120: # hour file  
            # skip data for option headonly
            code = line[:3]
            ar = line[3:5]
            mo = line[5:7]
            co = line[7:8].lower()
            day = line[8:10]
            year = line[14:16]+ar # use test for old format with quiet and disturbed days
            #print code, year, mo, day, co
            cf= lambda s,p: [ s[i:i+p] for i in range(0,len(s),p) ]
            dailymean = line[116:120]
            base = line[16:20]
            lst = cf(line[20:116],4)
            for i, elem in enumerate(lst):
                try:
                    hour = "%i" % i
                    date = year + '-' + mo + '-' + day + 'T' + hour + ':30:00'
                    time=date2num(datetime.strptime(date,"%Y-%m-%dT%H:%M:%S"))
                    if co=='i':
                        if not elem == "9999":
                            x = float(base) + float(elem)/600
                        else:
                            x = float(NaN)                                
                        li.append([time,x])
                    if co=='d':
                        if not elem == "9999":
                            y = float(base) + float(elem)/600
                        else:
                            y = float(NaN)                                
                        ld.append([time,y])
                    if co=='h':
                        if not elem == "9999":
                            x = float(base)*100 + float(elem)
                        else:
                            x = float(NaN)                                
                        lh.append([time,x])
                    if co=='x':
                        if not elem == "9999":
                            x = float(base)*100 + float(elem)
                        else:
                            x = float(NaN)                                
                        lx.append([time,x])
                    if co=='y':
                        if not elem == "9999":
                            y = float(base)*100 + float(elem)
                        else:
                            y = float(NaN)                                
                        ly.append([time,y])
                    if co=='z':
                        if not elem == "9999":
                            z = float(base)*100 + float(elem)
                        else:
                            z = float(NaN)                                
                        lz.append([time,z])
                    if co=='f':
                        if not elem == "9999":
                            f = float(base)*100 + float(elem)
                        else:
                            f = float(NaN)                                
                        lf.append([time,f])
                except:
                    pass
        elif len(line)==400: # minute file
            # skip data for option headonly
            continue
        else:
            print "Can not open WDC format"
            pass
    fh.close()

    if len(lx) > 0:
        for el in lx:
            for ele in ly:
                if ele[0] == el[0]:
                    el.append(ele[1])
            for ele in lz:
                if ele[0] == el[0]:
                    el.append(ele[1])
            for ele in lf:
                if ele[0] == el[0]:
                    el.append(ele[1])
        for elem in lx:
            row = LineStruct()
            row.time = elem[0]
            row.x = elem[1]
            row.y = elem[2]
            row.z = elem[3]
            row.f = elem[4]
            row.typ = "xyzf"
            stream.add(row)
    elif len(li) > 0:
        for el in li:
            for ele in ld:
                if ele[0] == el[0]:
                    el.append(ele[1])
            for ele in lz:
                if ele[0] == el[0]:
                    el.append(ele[1])
            for ele in lf:
                if ele[0] == el[0]:
                    el.append(ele[1])
        for elem in li:
            row = LineStruct()
            row.time = elem[0]
            row.x = elem[1]
            row.y = elem[2]
            row.z = elem[3]
            row.f = elem[4]
            stream.add(row)
        stream = stream._convertstream('idf2xyz')
    elif len(lh) > 0:
        for el in lh:
            for ele in ld:
                if ele[0] == el[0]:
                    el.append(ele[1])
            for ele in lz:
                if ele[0] == el[0]:
                    el.append(ele[1])
            for ele in lf:
                if ele[0] == el[0]:
                    el.append(ele[1])
        for elem in lh:
            row = LineStruct()
            row.time = elem[0]
            row.x = elem[1]
            row.y = elem[2]
            row.z = elem[3]
            row.f = elem[4]
            stream.add(row)
        stream = stream._convertstream('hdz2xyz')

    # header info
    headers['col-x'] = 'x'
    headers['unit-col-x'] = 'nT'
    headers['col-y'] = 'y'
    headers['unit-col-y'] = 'nT'
    headers['col-z'] = 'z'
    headers['unit-col-z'] = 'nT'
    headers['col-f'] = 'f'
    headers['unit-col-f'] = 'nT'
    headers['StationIAGAcode'] = code
    
    return DataStream(stream, headers)  


def writeWDC(datastream, filename, **kwargs):
    """
    Writing WDC format data.
    """
    
    mode = kwargs.get('mode')
    #createlatex = kwargs.get('createlatex')

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = pmRead(path_or_url=filename)
            datastream = mergeStreams(exst,datastream,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = pmRead(path_or_url=filename)
            datastream = mergeStreams(datastream,exst,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'append':
            myFile= open( filename, "ab" )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )

    # 1.) Test whether min or hourly data are used
    hourly, minute = False, False
    samplinginterval = datastream.get_sampling_period()
    if 0.98 < samplinginterval*24 < 1.02:
        hourly = True
    elif 0.98 < samplinginterval*24*60 < 1.02:
        minute = True
    else:
        print "Wrong sampling interval"

    # 2.) Get Iaga code
    header = datastream.header
    iagacode = header.get('StationIAGAcode'," ").upper()

    # 3.)
    if hourly:
        #try:
        line, textable = [],[]
        rowx, rowy, rowz, rowf = '','','',''
        latexrowx = ''
        for elem in datastream:
            arb = '  '
            for key in KEYLIST:
                if key == 'time':
                    try:
                        year = datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%Y")
                        month = datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%m")
                        day = datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%d")
                        hour = datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%H")
                        ye = year[2:]
                        ar = year[:-2]
                    except:
                        rowx, rowy, rowz, rowf = '','','',''
                        pass
                elif key == 'x':
                    xname = iagacode + ye + month + header['col-x'].upper() + day + '  ' + arb + ar
                    if rowx[:16] == xname:
                        if not isnan(elem.x):
                            xel.append(elem.x)
                            xhourel.append(int(hour))
                    elif rowx == '':
                        rowx = xname
                        if not isnan(elem.x):
                            xel = [elem.x]
                            xhourel = [int(hour)]
                        else:
                            xel = []
                            xhourel = []
                    else:
                        if len(xel)<1:
                            xdailymean = int(9999)
                            xbase = int(9999)
                        else:
                            xmean = round(np.mean(xel),0)
                            xbase = xmean - 5000.0
                            xbase = int(xbase/100)
                            xdailymean = int(xmean - xbase*100)
                        rowx += "%4i" % xbase
                        count = 0
                        for i in range(24):
                            if len(xhourel) > 0 and count < len(xhourel) and xhourel[count] == i:
                                xval = int(xel[count] - xbase*100)
                                count = count+1
                            else:
                                xval = int(9999)
                                xdailymean = int(9999)
                            rowx+='%4i' % xval
                        eol = '\n'
                        rowx+='%4i%s' % (xdailymean,eol)
                        line.append(rowx)
                        rowx = xname
                        xel, xhourel = [], []
                        if not isnan(elem.x):
                            xel.append(elem.x)
                            xhourel.append(int(hour))
                elif key == 'y':
                    yname = iagacode + ye + month + header['col-y'].upper() + day + '  ' + arb  + ar
                    if rowy[:16] == yname:
                        if not isnan(elem.y):
                            yel.append(elem.y)
                            yhourel.append(int(hour))
                    elif rowy == '':
                        rowy = yname
                        if not isnan(elem.y):
                            yel = [elem.y]
                            yhourel = [int(hour)]
                        else:
                            yel = []
                            yhourel = []
                    else:
                        if len(yel)<1:
                            ydailymean = int(9999)
                            ybase = int(9999)
                        else:
                            ymean = round(np.mean(yel),0)
                            ybase = ymean - 5000.0
                            ybase = int(ybase/100)
                            ydailymean = int(ymean - ybase*100)
                        rowy += "%4i" % ybase
                        count = 0
                        for i in range(24):
                            if len(yhourel) > 0 and count < len(yhourel) and yhourel[count] == i:
                                yval = int(yel[count] - ybase*100)
                                count = count+1
                            else:
                                yval = int(9999)
                                ydailymean = int(9999)
                            rowy+='%4i' % yval
                        rowy+='%4i\n' % ydailymean
                        line.append(rowy)
                        rowy = yname
                        yel, yhourel = [], []
                        if not isnan(elem.y):
                            yel.append(elem.y)
                            yhourel.append(int(hour))
                elif key == 'z':
                    zname = iagacode + ye + month + header['col-z'].upper() + day + '  ' + arb  + ar
                    if rowz[:16] == zname:
                        if not isnan(elem.z):
                            zel.append(elem.z)
                            zhourel.append(int(hour))
                    elif rowz == '':
                        rowz = zname
                        if not isnan(elem.z):
                            zel = [elem.z]
                            zhourel = [int(hour)]
                        else:
                            zel = []
                            zhourel = []
                    else:
                        if len(zel)<1:
                            zdailymean = int(9999)
                            zbase = int(9999)
                        else:
                            zmean = round(np.mean(zel),0)
                            zbase = zmean - 5000.0
                            zbase = int(zbase/100)
                            zdailymean = int(zmean - zbase*100)
                        rowz += "%4i" % zbase
                        count = 0
                        for i in range(24):
                            if len(zhourel) > 0 and count < len(zhourel) and zhourel[count] == i:
                                zval = int(zel[count] - zbase*100)
                                count = count+1
                            else:
                                zval = int(9999)
                                zdailymean = int(9999)
                            rowz+='%4i' % zval
                        rowz+='%4i\n' % zdailymean
                        line.append(rowz)
                        rowz = zname
                        zel, zhourel = [], []
                        if not isnan(elem.z):
                            zel.append(elem.z)
                            zhourel.append(int(hour))
                elif key == 'f':
                    fname = iagacode + ye + month + header['col-f'].upper() + day + '  ' + arb  + ar
                    if rowf[:16] == fname:
                        if not isnan(elem.f):
                            fel.append(elem.f)
                            fhourel.append(int(hour))
                    elif rowf == '':
                        rowf = fname
                        if not isnan(elem.f):
                            fel = [elem.f]
                            fhourel = [int(hour)]
                        else:
                            fel = []
                            fhourel = []
                    else:
                        if len(fel)<1:
                            fdailymean = int(9999)
                            fbase = int(9999)
                        else:
                            fmean = round(np.mean(fel),0)
                            fbase = fmean - 5000.0
                            fbase = int(fbase/100)
                            fdailymean = int(fmean - fbase*100)
                        rowf += "%4i" % fbase
                        count = 0
                        for i in range(24):
                            if len(fhourel) > 0 and count < len(fhourel) and fhourel[count] == i:
                                fval = int(fel[count] - fbase*100)
                                count = count+1
                            else:
                                fval = int(9999)
                                fdailymean = int(9999)
                            rowf+='%4i' % fval
                        rowf+='%4i\n' % fdailymean
                        line.append(rowf)
                        rowf = fname
                        fel, fhourel = [], []
                        if not isnan(elem.f):
                            fel.append(elem.f)
                            fhourel.append(int(hour))
        # Finally save data of the last day, which dropped out by above procedure
        for comp in ['x','y','z','f']:
            if len(eval(comp+'el'))<1:
                exec(comp+'dailymean = int(9999)')
                exec(comp+'base = int(9999)') 
            else:
                exec(comp+'mean = round(np.mean(' + comp +'el),0)') 
                exec(comp+'base = ' + comp +'mean - 5000.0') 
                exec(comp+'base = int(' + comp +'base/100)') 
                exec(comp+'dailymean = int(' + comp +'mean - ' + comp +'base*100)') 
            exec('row'+comp+'+= "%4i" % '+comp+'base')
            count = 0
            for i in range(24):
                if len(eval(comp+'hourel')) > 0 and count < len(eval(comp+'hourel')) and eval(comp+'hourel[count]') == i:
                    exec(comp+'val = int(' + comp +'el[count] - ' + comp + 'base*100)')
                    count = count+1
                else:
                    exec(comp+'val = int(9999)')
                    exec(comp+'dailymean = int(9999)')
                exec('row' + comp + '+="%4i" % ' + comp + 'val')
            eol = '\n'
            exec('row' + comp + '+="%4i%s" % (' + comp + 'dailymean,eol)')
            line.append(eval('row'+comp))
        line.sort()
        try:
            myFile.writelines( line )
            pass
        finally:
           myFile.close()
        #except IOError:
        #    pass
    elif minute:
        pass
    else:
        logging.warning("Could not save WDC data. Please provide hour or minute data")

"""
def textable_preamble(fp, kwargs**):
TEX Table generator
written by Chris Burn, http://users.obs.carnegiescience.edu/~cburns/site/?p=22

    :type justs: String 
    :param justs: String defining justifictaions of the table
    :type fontsize: int 
    :param fontsize: fontsize of table
    :type rotate: bool 
    :param rotate: turn the table
    :type tablewidth: int 
    :param tablewidth: width of the table
    :type numcols: int 
    :param numcols: number of columns of the table
    :type caption: String 
    :param caption: String with table caption
    :type label: String 
    :param label: String with table label
"""

