#!/usr/bin/env python

from __future__ import print_function
import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

try: # Necessary for wx2.8.11.0
    from wx.lib.pubsub import setupkwargs
    # later versions: wxPython-phoenix
    # sudo pip3 install -U --pre -f http://wxpython.org/Phoenix/snapshot-builds/ wxPython_Phoenix
    # does not work so far! leon 2016-06-24
except:
    pass
from wx.lib.pubsub import pub
from wx.lib.dialogs import ScrolledMessageDialog

from magpy.stream import read
import magpy.mpplot as mp
#import magpy.absolutes import as di
from magpy.absolutes import *
from magpy.transfer import *
from magpy.database import *
from magpy.version import __version__
from magpy.gui.streampage import *
from magpy.gui.flagpage import *
from magpy.gui.metapage import *
from magpy.gui.dialogclasses import *
from magpy.gui.absolutespage import *
from magpy.gui.reportpage import *
from magpy.gui.developpage import *  # remove this
from magpy.gui.analysispage import *
from magpy.gui.monitorpage import *
from magpy.collector import collectormethods as colsup
import glob, os, pickle, base64
import pylab
import thread, time
import threading

import wx.py


def saveobj(obj, filename):
    with open(filename, 'wb') as f:
        pickle.dump(obj,f,pickle.HIGHEST_PROTOCOL)

def loadobj(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)

def pydate2wxdate(datum):
     assert isinstance(datum, (datetime, datetime.date))
     tt = datum.timetuple()
     dmy = (tt[2], tt[1]-1, tt[0])
     #print (tt, dmy)
     return wx.DateTimeFromDMY(*dmy)

def wxdate2pydate(date):
     assert isinstance(date, wx.DateTime)
     if date.IsValid():
          ymd = map(int, date.FormatISODate().split('-'))
          return datetime.date(*ymd)
     else:
          return None

def saveini(optionsdict): #dbname=None, user=None, passwd=None, host=None, dirname=None, compselect=None, abscompselect=None, basecompselect=None, resolution=None, dipathlist = None, divariopath = None, discalarpath = None, diexpD = None, diexpI = None, stationid = None, diid = None, ditype = None, diazimuth = None, dipier = None, dialpha = None, dideltaF = None, didbadd = None, bookmarks = None):
    """
    Method for initializing deault paremeters credentials
    """

    try:
        normalpath = os.path.expanduser('~')
    except:
        normalpath = os.path('/') # Test that

    # Updating version info in file
    from magpy.version import __version__
    optionsdict['magpyversion'] = __version__

    if optionsdict.get('dbname','') == '':
        optionsdict['dbname'] = 'None'
    if optionsdict.get('user','') == '':
        optionsdict['user'] = 'Max'
    if optionsdict.get('passwd','') == '':
        passwd = 'Secret'
    else:
        passwd = optionsdict.get('passwd','')
    if optionsdict.get('host','') == '':
        optionsdict['host'] = 'localhost'
    if optionsdict.get('dirname','') == '':
        optionsdict['dirname'] = normalpath
    if optionsdict.get('basefilter','') == '':
        optionsdict['basefilter'] = 'spline'
    if optionsdict.get('dipathlist','') == '':
        optionsdict['dipathlist'] = [normalpath]
    if optionsdict.get('divariopath','') == '':
        optionsdict['divariopath'] = os.path.join(normalpath,'*')
    if optionsdict.get('discalarpath','') == '':
        optionsdict['discalarpath'] = os.path.join(normalpath,'*')
    if optionsdict.get('diexpD','') == '':
        optionsdict['diexpD'] = '0.0'
    if optionsdict.get('diexpI','') == '':
        optionsdict['diexpI'] = '0.0'
    if optionsdict.get('stationid','') == '':
        optionsdict['stationid'] = 'WIC'
    if optionsdict.get('diid','') == '':
        optionsdict['diid'] = ''
    if optionsdict.get('ditype','') == '':
        optionsdict['ditype'] = 'manual' #abstype = ''
    if optionsdict.get('diazimuth','') == '':
        optionsdict['diazimuth'] = ''
    if optionsdict.get('dipier','') == '':
        optionsdict['dipier'] = 'A2'
    if optionsdict.get('dialpha','') == '':
        optionsdict['dialpha'] = '0.0'
    if optionsdict.get('dibeta','') == '':
        optionsdict['dibeta'] = '0.0'
    if optionsdict.get('dideltaF','') == '':
        optionsdict['dideltaF'] = '0.0'
    if optionsdict.get('dideltaD','') == '':
        optionsdict['dideltaD'] = '0.0'
    if optionsdict.get('dideltaI','') == '':
        optionsdict['dideltaI'] = '0.0'
    if optionsdict.get('diannualmean','') == '':
        optionsdict['diannualmean'] = ''
    if optionsdict.get('didbadd','') == '':
        optionsdict['didbadd'] = 'False'
    if optionsdict.get('bookmarks','') == '':
        optionsdict['bookmarks'] = ['ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc','ftp://user:passwd@www.zamg.ac.at/data/magnetism/wic/variation/WIC20160627pmin.min','http://www.conrad-observatory.at/zamg/index.php/downloads-en/category/13-definite2015?download=66:wic-2015-0000-pt1m-4','http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab']
    if optionsdict.get('scalevalue','') == '':
        optionsdict['scalevalue'] = 'True'
    if optionsdict.get('double','') == '':
        optionsdict['double'] = 'True'
    if optionsdict.get('order','') == '':
        optionsdict['order'] = 'MU,MD,EU,WU,ED,WD,NU,SD,ND,SU'
    if optionsdict.get('didbadd','') == '':
        optionsdict['didbadd'] = 'False'
    #calculation
    if optionsdict.get('fitfunction','') == '':
        optionsdict['fitfunction'] = 'spline'
    if optionsdict.get('fitdegree','') == '':
        optionsdict['fitdegree'] = '5'
    if optionsdict.get('fitknotstep','') == '':
        optionsdict['fitknotstep'] = '0.3'
    if optionsdict.get('martasscantime','') == '':
        optionsdict['martasscantime'] = '20'
    if optionsdict.get('favoritemartas','') == '':
        optionsdict['favoritemartas'] = ['www.example.com','192.168.178.42']
    if optionsdict.get('experimental','') == '':
        optionsdict['experimental'] = False

    initpath = os.path.join(normalpath,'.magpyguiini')

    pwd = base64.b64encode(passwd)
    optionsdict['passwd'] = pwd

    saveobj(optionsdict, initpath)
    print ("Initialization: Added data ")


def loadini():
    """
    Load initialisation data

    """
    from magpy.version import __version__
    home = os.path.expanduser('~')
    initpath = os.path.join(home,'.magpyguiini')
    print ("Trying to access initialization file:", initpath)

    try:
        initdata = loadobj(initpath)
        magpyversion = __version__
        if not initdata.get('magpyversion','') == magpyversion:
            # version number has changes and thus eventually also the options ini
            print ("MagPy version has changed ({}): inititalization parameters will be updated".format(magpyversion))
            return initdata, True
        print ("... success")
    except:
        print ("Initialization data not found: Setting defaults")
        return {}, False

    #print "Initialization data loaded"
    return initdata, False


class RedirectText(object):
    # Taken from: http://www.blog.pythonlibrary.org/2009/01/01/wxpython-redirecting-stdout-stderr/
    # Used to redirect di results to the multiline textctrl on the DI page
    def __init__(self,aWxTextCtrl):
        self.out=aWxTextCtrl

    def write(self,string):
        self.out.WriteText(string)

class PlotPanel(wx.Panel):
    """
    DESCRIPTION
        comtains all methods for the left plot panel
    """
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.figure = plt.figure()
        self.plt = plt
        scsetmp = ScreenSelections()
        self.canvas = FigureCanvas(self,-1,self.figure)
        self.datavars = {} # for monitoring
        self.array = [[] for key in KEYLIST] # for monitoring
        self.t1_stop= threading.Event()
        self.xlimits = None
        self.ylimits = None
        self.selplt = 0 # Index to the selected plot - used by flagselection
        self.initialPlot()
        self.__do_layout()

    def __do_layout(self):
        # Resize graph and toolbar, create toolbar
        self.vbox = wx.BoxSizer(wx.VERTICAL)
        self.vbox.Add(self.canvas, 1, wx.LEFT | wx.TOP | wx.GROW)
        self.toolbar = NavigationToolbar2Wx(self.canvas)
        self.vbox.Add(self.toolbar, 0, wx.EXPAND)
        self.SetSizer(self.vbox)
        self.vbox.Fit(self)


    def timer(self, arg1, stop_event):
        while(not stop_event.is_set()):
            if arg1 == 1:
                self.update(self.array)
            else:
                self.update_mqtt(arg1,self.array)
            print ("Running ... {}".format(datetime.utcnow()))
            stop_event.wait(self.datavars[7])
        ###
        # Eventually stop client
        if not arg1 == 1:
            #try:
            arg1.loop_stop()
            #except:
            #pass

    def update(self,array):
        """
        DESCRIPTION
            Update array with new data and plot it.
            If log file is chosen the this method makes use of collector.subscribe method:
            storeData to save binary file
        """
        def list_duplicates(seq):
            seen = set()
            seen_add = seen.add
            return [idx for idx,item in enumerate(seq) if item in seen or seen_add(item)]

        db = self.datavars[8]
        parameterstring = 'time,'+self.datavars[1]
        # li should contain a data source of a certain length (can be filled by any reading process)
        li = sorted(dbselect(db, parameterstring, self.datavars[0], expert='ORDER BY time DESC LIMIT {}'.format(int(self.datavars[2]))))
        try:
            tmpdt = [datetime.strptime(elem[0], "%Y-%m-%d %H:%M:%S.%f") for elem in li]
        except:
            tmpdt = [datetime.strptime(elem[0], "%Y-%m-%d %H:%M:%S") for elem in li]
        self.array[0].extend(tmpdt)
        for idx,para in enumerate(parameterstring.split(',')):
            if not para.endswith('time'):
                i = KEYLIST.index(para)
                self.array[i].extend([float(elem[idx]) for elem in li])

        duplicateindicies = list_duplicates(self.array[0])
        array = [[] for key in KEYLIST]
        for idx, elem in enumerate(self.array):
            if len(elem) > 0:
                newelem = np.delete(np.asarray(elem), duplicateindicies)
                array[idx] = list(newelem)

        coverage = int(self.datavars[6])
        try:
           tmp = DataStream([],{},np.asarray(array)).samplingrate()
           coverage = int(coverage/tmp)
        except:
           pass

        array = [ar[-coverage:] if len(ar) > coverage else ar for ar in array ]

        self.monitorPlot(array)

        #if Log2File:
        #    msubs.output = 'file'
        #    #sensorid = row[0]
        #    #module = row[1]
        #    #line = row[2]
        #    #msubs.storeData(li,parameterstring.split(','))

    def update_mqtt(self,client,array):
        """
        DESCRIPTION
            Update array with new data and plot it.
        """
        #from magpy.collector import collectormethods as colsup

        sumtime = 0

        #print (self.datavars[0][:-5])
        coverage = int(self.datavars[6])

        pos = KEYLIST.index('t1')
        posvar1 = KEYLIST.index('var1')
        #OK = True
        #if OK:
        while sumtime<self.datavars[2]:
            client.loop(.1)
            # TODO add reconnection handler here in case that client goes away
            # print (client.connected_flag) -- connected_flag remains True -> can set it to False if no Payload is received ....
            #if not client.connected_flag:
            #    reconnect
            sumtime = sumtime+1
            li = colsup.streamdict.get(self.datavars[0][:-5])   # li is an ndarray
            if len(self.array[0]) > 0 and li[0][0] == self.array[0][-1]:
                pass
            else:
                if not li == None:
                    for idx,el in enumerate(li):
                        self.array[idx].extend(el)

            try:
                tmp = DataStream([],{},np.asarray(self.array)).samplingrate()
                coverage = int(int(self.datavars[6])/tmp)
            except:
                pass
            self.array = [el[-coverage:] for el in self.array]

        if len(self.array[0]) > 2:
            ind = np.argsort(np.asarray(self.array)[0])
            for idx,line in enumerate(self.array):
                if len(line) == len(ind):
                    self.array[idx] = list(np.asarray(line)[ind])
            #self.array = np.asarray(self.array)[:,np.argsort(np.asarray(self.array)[0])]
            array = self.array
            self.monitorPlot(array)


    def startMQTTMonitor(self,**kwargs):
        """
        DEFINITION:
            embbed matplotlib figure in canvas for mointoring

        PARAMETERS:
            kwargs:  - all plot args
        """
        # moved to MARTAS 
        pass


    def startMARCOSMonitor(self,**kwargs):
        """
        DEFINITION:
            embbed matplotlib figure in canvas for mointoring

        PARAMETERS:
            kwargs:  - all plot args
        """

        dataid = self.datavars[0]
        parameter = self.datavars[1]
        period = self.datavars[2]
        pad = self.datavars[3]
        currentdate = self.datavars[4]
        unitlist = self.datavars[5]
        coverage = self.datavars[6]  # coverage
        updatetime = self.datavars[7]
        db = self.datavars[8]

        # convert parameter list to a dbselect sql format
        parameterstring = 'time,'+parameter

        # Test whether data is available at all with selected keys and dataid
        li = sorted(dbselect(db, parameterstring, dataid, expert='ORDER BY time DESC LIMIT {}'.format(int(coverage))))

        if not len(li) > 0:
            print("Parameter", parameterstring, dataid, coverage)
            print("Did not find any data to display - aborting")
            return
        else:
            valkeys = ['time']
            valkeys = parameterstring.split(',')
            for i,elem in enumerate(valkeys):
                idx = KEYLIST.index(elem)
                if elem == 'time':
                    try:
                        self.array[idx] = [datetime.strptime(el[0],"%Y-%m-%d %H:%M:%S.%f") for el in li]
                    except:
                        self.array[idx] = [datetime.strptime(el[0],"%Y-%m-%d %H:%M:%S") for el in li]
                else:
                    self.array[idx] = [float(el[i]) for el in li]

        self.datavars = {0: dataid, 1: parameter, 2: period, 3: pad, 4: currentdate, 5: unitlist, 6: coverage, 7: updatetime, 8: db}

        self.figure.clear()
        t1 = threading.Thread(target=self.timer, args=(1,self.t1_stop))
        t1.start()
        # Display the plot
        self.canvas.draw()


    def startMARTASMonitor(self, protocol, **kwargs):
        """
        DEFINITION:
            embbed matplotlib figure in canvas for mointoring

        PARAMETERS:
            kwargs:  - all plot args
        """

        dataid = self.datavars[0]
        parameter = self.datavars[1]
        period = self.datavars[2]
        pad = self.datavars[3]
        currentdate = self.datavars[4]
        unitlist = self.datavars[5]
        coverage = self.datavars[6]  # coverage
        updatetime = self.datavars[7]
        db = self.datavars[8]
        martasaddress = self.datavars[9]  # coverage
        martasport = self.datavars[10]
        martasdelay = self.datavars[11]
        martasuser = self.datavars[13]
        martaspasswd = self.datavars[14]
        martasstationid = self.datavars[15]
        qos = self.datavars[16]

        # convert unitlist to [[],[]]
        plist = parameter.split(',')
        if len(unitlist) == len(plist):
            for idx, el in enumerate(plist):
                unitlist[idx] = [el,unitlist[idx]]
            self.datavars[5] = unitlist


        if protocol == 'mqtt':
            try:
                import paho.mqtt.client as mqtt
                from magpy.collector import collectormethods as colsup
                mqttimport = True
            except:
                mqttimport = False
                dlg = wx.MessageDialog(self, "Could not import required packages!\n"
                        "Make sure that the python package paho-mqtt is installed\n",
                        "MARTAS monitor failed", wx.OK|wx.ICON_INFORMATION)
                dlg.ShowModal()
                self.changeStatusbar("Using MQTT monitor failed ... Ready")
                dlg.Destroy()
            if mqttimport:
                mqtt.Client.connected_flag=False
                client = mqtt.Client()

                if not martasuser in ['',None,'None','-']: 
                    #client.tls_set(tlspath)  # check http://www.steves-internet-guide.com/mosquitto-tls/
                    client.username_pw_set(martasuser, password=martaspasswd)

                client.on_connect = colsup.on_connect
                client.on_message = colsup.on_message
                try: 
                    client.connect(martasaddress, int(martasport), int(martasdelay))
                except:
                    dlg = wx.MessageDialog(self, "Connection to MQTT broker failed\n"
                            "Check your internet connection or credentials\n",
                            "Connection failed", wx.OK|wx.ICON_INFORMATION)
                    dlg.ShowModal()
                    dlg.Destroy()
                    return

                client.subscribe("{}/#".format(martasstationid), qos)

                self.figure.clear()

                t1 = threading.Thread(target=self.timer, args=(client,self.t1_stop))
                t1.start()
                # Display the plot
                self.canvas.draw()


        """
        # Test whether data is available at all with selected keys and dataid
        li = sorted(dbselect(db, parameterstring, dataid, expert='ORDER BY time DESC LIMIT {}'.format(int(coverage))))

        if not len(li) > 0:
            print("Parameter", parameterstring, dataid, coverage)
            print("Did not find any data to display - aborting")
            return
        else:
            valkeys = ['time']
            valkeys = parameterstring.split(',')
            for i,elem in enumerate(valkeys):
                idx = KEYLIST.index(elem)
                if elem == 'time':
                    self.array[idx] = [datetime.strptime(el[0],"%Y-%m-%d %H:%M:%S.%f") for el in li]
                else:
                    self.array[idx] = [float(el[i]) for el in li]

        self.datavars = {0: dataid, 1: parameter, 2: period, 3: pad, 4: currentdate, 5: unitlist, 6: coverage, 7: updatetime, 8: db, 9: martasaddress, 10: martasport, 11: martasdelay}

        self.figure.clear()
        t1 = threading.Thread(target=self.timer, args=(1,self.t1_stop))
        t1.start()
        # Display the plot
        self.canvas.draw()
        """

        """
        #clientname,clientip,destpath,dest,stationid,sshcredlst,s,o,printdata,dbcredlst
        #dataid = self.datavars[0]
        #parameter = self.datavars[1]
        #period = self.datavars[2]
        #pad = self.datavars[3]
        #currentdate = self.datavars[4]
        #unitlist = self.datavars[5]
        #coverage = self.datavars[6]  # coverage
        #updatetime = self.datavars[7]
        #db = self.datavars[8]

        try:
            from magpy.collector import subscribe2client as msubs
        except:
            print ("MARTAS and LogFile options not available - check dependencies")
            return

        # MARTAS specific
        clientip = self.datavars[9]
        destpath = self.datavars[10]
        sshcredlst = self.datavars[11]
        s = self.datavars[12]
        o = self.datavars[13]
        stationid = self.datavars[14]

        # clientname
        import socket
        clientaddress = socket.getfqdn(clientip)
        clientname = clientaddress.split('.')[0]

        dest = 'file'
        printdata = False
        dbcredlst = []

        print ("Here", clientname,clientip,destpath,dest,stationid,sshcredlst,s,o,printdata,dbcredlst)

        factory = WampClientFactory("ws://"+clientip+":9100", debugWamp = False)
        msubs.sendparameter(clientname,clientip,destpath,dest,stationid,sshcredlst,s,o,printdata,dbcredlst)
        factory.protocol = msubs.PubSubClient
        connectWS(factory)

        reactor.run()
        """


    def monitorPlot(self,array,**kwargs):
        """
        DEFINITION:
            embbed matplotlib figure in canvas for mointoring

        PARAMETERS:
            kwargs:  - all plot args
        """

        # Read persistent data variables
        dataid = self.datavars[0]
        parameter = self.datavars[1]
        period = self.datavars[2]
        pad = self.datavars[3]
        currentdate = self.datavars[4]
        unitlist = self.datavars[5]
        coverage = self.datavars[6]  # coverage
        updatetime = self.datavars[7]
        db = self.datavars[8]
        martasaddress = self.datavars[9]
        martasport = self.datavars[10]
        martasdelay = self.datavars[11]
        martasprotocol = self.datavars[12]
        martasuser = self.datavars[13]
        martaspasswd = self.datavars[14]
        martasstationid = self.datavars[15]
        qos = self.datavars[16]

        # convert parameter list to a dbselect sql format
        parameterstring = 'time,'+parameter

        self.figure.clear()
        try:
            self.axes.clear()
        except:
            pass
        dt = array[0]
        self.figure.suptitle("Live Data of %s - %s" % (dataid, currentdate))
        for idx,para in enumerate(parameterstring.split(',')):
            if not para.endswith('time'):
                i = KEYLIST.index(para)
                subind = int("{}1{}".format(len(parameterstring.split(','))-1,idx))
                #print subind
                self.axes = self.figure.add_subplot(subind)
                self.axes.grid(True)
                rd = array[i]
                #try:
                l, = self.axes.plot_date(dt,rd,'b-')
                #except:
                #    array = [[] for el in KEYLIST]
                #l, = a.plot_date(dt,td,'g-')
                plt.xlabel("Time")
                plt.ylabel(r'{} [{}]'.format(unitlist[idx-1][0],unitlist[idx-1][1]))

                # Get the minimum and maximum temperatures these are
                # used for annotations and scaling the plot of data
                min_val = np.min(rd)
                max_val = np.max(rd)

                # Add annotations for minimum and maximum temperatures
                self.axes.annotate(r'Min: %0.1f' % (min_val),
                    xy=(dt[rd.index(min_val)], min_val),
                    xycoords='data', xytext=(20, -20),
                    textcoords='offset points',
                    bbox=dict(boxstyle="round", fc="0.8"),
                    arrowprops=dict(arrowstyle="->",
                    shrinkA=0, shrinkB=1,
                    connectionstyle="angle,angleA=0,angleB=90,rad=10"))

                self.axes.annotate(r'Max: %0.1f' % (max_val),
                    xy=(dt[rd.index(max_val)], max_val),
                    xycoords='data', xytext=(20, 20),
                    textcoords='offset points',
                    bbox=dict(boxstyle="round", fc="0.8"),
                    arrowprops=dict(arrowstyle="->",
                    shrinkA=0, shrinkB=1,
                    connectionstyle="angle,angleA=0,angleB=90,rad=10"))

        # Set the axis limits to make the data more readable
        #self.axes.axis([0,len(temps), min_t - pad,max_t + pad])

        self.figure.canvas.draw_idle()

        # Repack variables that need to be persistent between
        # executions of this method
        self.datavars = {0: dataid, 1: parameter, 2: period, 3: pad, 4: currentdate, 5: unitlist, 6: coverage, 7: updatetime, 8: db, 9: martasaddress, 10: martasport, 11: martasdelay, 12: martasprotocol, 13: martasuser, 14: martaspasswd, 15: martasstationid, 16: qos}


    def guiPlot(self,streams,keys,plotopt={},**kwargs):
        """
        DEFINITION:
            embbed matplotlib figure in canvas

        PARAMETERS:
        kwargs:  - all plot args
        """

        #print ("GUI plot", plotopt)

        # Declare and register callbacks
        def on_xlims_change(axes):
            self.xlimits = axes.get_xlim()

        def on_ylims_change(axes):
            #print ("updated ylims: ", axes.get_ylim())
            self.ylimits = axes.get_ylim()
            self.selplt = self.axlist.index(axes)

        self.figure.clear()
        try:
            self.axes.clear()
        except:
            pass

        self.axes = mp.plotStreams(streams,keys,figure=self.figure,**plotopt)
        #self.axes = mp.plotStreams(streams,keys,figure=self.figure,**kwargs)
        self.axlist = self.figure.axes

        #get current xlimits:
        for idx, ax in enumerate(self.axlist):
            self.xlimits = ax.get_xlim()
            self.ylimits = ax.get_ylim()
            ax.callbacks.connect('xlim_changed', on_xlims_change)
            ax.callbacks.connect('ylim_changed', on_ylims_change)

        stream = streams[-1]
        key = keys[-1]
        if not len(stream.ndarray[0])>0:
            #print ("Here")
            self.t = [elem.time for elem in stream]
            flag = [elem.flag for elem in stream]
            self.k = [eval("elem."+keys[0]) for elem in stream]
        else:
            self.t = stream.ndarray[0]
            flagpos = KEYLIST.index('flag')
            firstcol = KEYLIST.index(key[0])
            flag = stream.ndarray[flagpos]
            self.k = stream.ndarray[firstcol]
        #self.axes.af2 = self.AnnoteFinder(t,yplt,flag,self.axes)
        #self.axes.af2 = self.AnnoteFinder(t,k,flag,self.axes)
        #af2 = self.AnnoteFinder(t,k,flag,self.axes)
        #self.figure.canvas.mpl_connect('button_press_event', af2)
        self.canvas.draw()

    def initialPlot(self):
        """
        DEFINITION:
            loads an image for the startup screen
        """
        try:
            self.axes = self.figure.add_subplot(111)
            plt.axis("off") # turn off axis
            try:
                script_dir = os.path.dirname(__file__)
                startupimage = os.path.join(script_dir,'magpy.png')
                # TODO add alternative positions
                # either use a walk to locate the image in /usr for linux and installation path on win
                # or put installation path in ini
                img = imread(startupimage)
                self.axes.imshow(img)
            except:
                pass
            self.canvas.draw()
        except:
            pass

    def linkRep(self):
        return ReportPage(self)

    class AnnoteFinder:
        """
        callback for matplotlib to display an annotation when points are clicked on.  The
        point which is closest to the click and within xtol and ytol is identified.

        Register this function like this:

        scatter(xdata, ydata)
        af = AnnoteFinder(xdata, ydata, annotes)
        connect('button_press_event', af)
        """

        def __init__(self, xdata, ydata, annotes, axis=None, xtol=None, ytol=None):
            self.data = zip(xdata, ydata, annotes)
            if xtol is None:
                xtol = ((max(xdata) - min(xdata))/float(len(xdata)))/2
            if ytol is None:
                ytol = ((max(ydata) - min(ydata))/float(len(ydata)))/2
            ymin = min(ydata)
            ymax = max(ydata)
            self.xtol = xtol
            self.ytol = ytol
            if axis is None:
                self.axis = pylab.gca()
            else:
                self.axis= axis
            self.drawnAnnotations = {}
            self.links = []

        def distance(self, x1, x2, y1, y2):
            """
            return the distance between two points
            """
            return math.hypot(x1 - x2, y1 - y2)

        def __call__(self, event):
            if event.inaxes:
                clickX = event.xdata
                clickY = event.ydata
                if self.axis is None or self.axis==event.inaxes:
                    annotes = []
                    for x,y,a in self.data:
                        #if  clickX-self.xtol < x < clickX+self.xtol and clickY-self.ytol < y < clickY+self.ytol:
                        if  clickX-self.xtol < x < clickX+self.xtol :
                            annotes.append((self.distance(x,clickX,y,clickY),x,y, a) )
                    if annotes:
                        annotes.sort()
                        distance, x, y, annote = annotes[0]
                        self.drawAnnote(event.inaxes, x, y, annote)
                        for l in self.links:
                            l.drawSpecificAnnote(annote)

        def drawAnnote(self, axis, x, y, annote):
            """
            Draw the annotation on the plot
            """
            if (x,y) in self.drawnAnnotations:
                markers = self.drawnAnnotations[(x,y)]
                for m in markers:
                    m.set_visible(not m.get_visible())
                self.axis.figure.canvas.draw()
            else:
                #t = axis.text(x,y, "(%3.2f, %3.2f) - %s"%(x,y,annote), )
                datum = datetime.strftime(num2date(x).replace(tzinfo=None),"%Y-%m-%d")
                t = axis.text(x,y, "(%s, %3.2f)"%(datum,y), )
                m = axis.scatter([x],[y], marker='d', c='r', zorder=100)
                scse = ScreenSelections()
                scse.seldatelist.append(x)
                scse.selvallist.append(y)
                scse.updateList()
                #test = MainFrame(parent=None)
                #test.ReportPage.addMsg(str(x))
                #rep_page.logMsg('Datum is %s ' % (datum))
                #l = axis.plot([x,x],[0,y])
                self.drawnAnnotations[(x,y)] =(t,m)
                self.axis.figure.canvas.draw()

        def drawSpecificAnnote(self, annote):
            annotesToDraw = [(x,y,a) for x,y,a in self.data if a==annote]
            for x,y,a in annotesToDraw:
                self.drawAnnote(self.axis, x, y, a)


class MenuPanel(wx.Panel):
    """
    DESCRIPTION
        comtains all methods for the right menu panel and their insets
        All methods are listed in the MainFrame class
    """
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        # Create pages on MenuPanel
        nb = wx.Notebook(self,-1)
        self.str_page = StreamPage(nb)
        self.fla_page = FlagPage(nb)
        self.met_page = MetaPage(nb)
        self.ana_page = AnalysisPage(nb)
        self.abs_page = AbsolutePage(nb)
        self.rep_page = ReportPage(nb)
        self.com_page = MonitorPage(nb)
        nb.AddPage(self.str_page, "Stream")
        nb.AddPage(self.fla_page, "Flags")
        nb.AddPage(self.met_page, "Meta")
        nb.AddPage(self.ana_page, "Analysis")
        nb.AddPage(self.abs_page, "DI")
        nb.AddPage(self.rep_page, "Report")
        nb.AddPage(self.com_page, "Live")

        sizer = wx.BoxSizer()
        sizer.Add(nb, 1, wx.EXPAND)
        self.SetSizer(sizer)


class MainFrame(wx.Frame):
    def __init__(self, *args, **kwds):
        kwds["style"] = wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, *args, **kwds)
        # The Splitted Window
        self.sp = wx.SplitterWindow(self, -1, style=wx.SP_3D|wx.SP_BORDER)
        self.plot_p = PlotPanel(self.sp,-1)
        self.menu_p = MenuPanel(self.sp,-1)
        pub.subscribe(self.changeStatusbar, 'changeStatusbar')

        # The Status Bar
        self.StatusBar = self.CreateStatusBar(2, wx.ST_SIZEGRIP)
        # Update Status Bar with plot values
        self.plot_p.canvas.mpl_connect('motion_notify_event', self.UpdateCursorStatus)
        # Allow flagging with double click
        #self.plot_p.canvas.mpl_connect('button_press_event', self.OnFlagClick)

        self.streamlist = []
        self.headerlist = []
        self.plotoptlist = []
        self.streamkeylist = []
        self.currentstreamindex = 0
        self.stream = DataStream() # used for storing original data
        self.plotstream = DataStream() # used for manipulated data
        self.orgheader = {}
        self.shownkeylist = []
        self.keylist = []
        self.flaglist = []
        self.compselect = 'None'
        self.databaseconnected = False  # Bool for testing whether database has been connected

        self.options = {}
        self.dipathlist = 'None'
        self.baselinedictlst = [] # variable to hold info on loaded DI streams for baselinecorrection
        self.baselineidxlst = []

        self.InitPlotParameter()
        datacheck = True

        # Try to load ini-file
        # located within home directory
        inipara,update = loadini()
        #print ("INIPARA", inipara)
        if inipara == {}:
            saveini(self.options) # initialize defaultvalues
            inipara, test = loadini()
            #print ("INIPARA", inipara)
        if update:
            self.initParameter(inipara)
            saveini(self.options) # initialize defaultvalues
            inipara, test = loadini()

        # Variable initializations
        self.initParameter(inipara)

        # Menu Bar
        # --------------
        # Existing shortcuts: o,d,u,s,e,q,c,b,l,a,r,m,i,v  (a,b,c,d,e,f,(gh),i,(jk),l,m,(n),o
        self.MainMenu = wx.MenuBar()
        # ## File Menu
        self.FileMenu = wx.Menu()
        self.FileOpen = wx.MenuItem(self.FileMenu, 101, "&Open File...\tCtrl+F", "Open file", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.FileOpen)
        self.DirOpen = wx.MenuItem(self.FileMenu, 102, "Select &Directory...\tCtrl+D", "Select an existing directory", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.DirOpen)
        self.WebOpen = wx.MenuItem(self.FileMenu, 103, "Open &URL...\tCtrl+U", "Get data from the internet", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.WebOpen)
        self.DBOpen = wx.MenuItem(self.FileMenu, 104, "&Select DB table...\tCtrl+S", "Select a MySQL database", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.DBOpen)
        self.DBOpen.Enable(False)
        self.FileMenu.AppendSeparator()
        self.ExportData = wx.MenuItem(self.FileMenu, 105, "&Export data...\tCtrl+E", "Export data to a file", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.ExportData)
        self.ExportData.Enable(False)
        self.FileMenu.AppendSeparator()
        self.FileQuitItem = wx.MenuItem(self.FileMenu, wx.ID_EXIT, "&Quit\tCtrl+Q", "Quit the program", wx.ITEM_NORMAL)
        self.FileMenu.AppendItem(self.FileQuitItem)
        self.MainMenu.Append(self.FileMenu, "&File")
        # ## Database Menu
        self.DatabaseMenu = wx.Menu()
        self.DBConnect = wx.MenuItem(self.DatabaseMenu, 201, "&Connect MySQL DB...\tCtrl+O", "Connect Database", wx.ITEM_NORMAL)
        self.DatabaseMenu.AppendItem(self.DBConnect)
        self.DatabaseMenu.AppendSeparator()
        self.DBInit = wx.MenuItem(self.DatabaseMenu, 202, "&Initialize a new MySQL DB...\tCtrl+Z", "Initialize Database", wx.ITEM_NORMAL)
        self.DatabaseMenu.AppendItem(self.DBInit)
        self.MainMenu.Append(self.DatabaseMenu, "Data&base")
        # ## DI Menu
        self.DIMenu = wx.Menu()
        self.DIPath2DI = wx.MenuItem(self.DIMenu, 501, "&Load DI data...\tCtrl+L", "Load DI data...", wx.ITEM_NORMAL)
        self.DIMenu.AppendItem(self.DIPath2DI)
        self.DIPath2Vario = wx.MenuItem(self.DIMenu, 502, "Path to &variometer data...\tCtrl+A", "Variometer data...", wx.ITEM_NORMAL)
        self.DIMenu.AppendItem(self.DIPath2Vario)
        self.DIPath2Scalar = wx.MenuItem(self.DIMenu, 503, "Path to scala&r data...\tCtrl+R", "Scalar data...", wx.ITEM_NORMAL)
        self.DIMenu.AppendItem(self.DIPath2Scalar)
        self.DIMenu.AppendSeparator()
        self.DIInputSheet = wx.MenuItem(self.DIMenu, 504, "O&pen input sheet...\tCtrl+P", "Input sheet...", wx.ITEM_NORMAL)
        self.DIMenu.AppendItem(self.DIInputSheet)
        self.MainMenu.Append(self.DIMenu, "D&I")
        # ## Stream Operations
        self.StreamOperationsMenu = wx.Menu()
        self.StreamAddListSelect = wx.MenuItem(self.StreamOperationsMenu, 601, "Add current &working state to Streamlist...\tCtrl+W", "Add Stream", wx.ITEM_NORMAL)
        self.StreamOperationsMenu.AppendItem(self.StreamAddListSelect)
        self.StreamOperationsMenu.AppendSeparator()
        self.StreamListSelect = wx.MenuItem(self.StreamOperationsMenu, 602, "Select active Strea&m...\tCtrl+M", "Select Stream", wx.ITEM_NORMAL)
        self.StreamOperationsMenu.AppendItem(self.StreamListSelect)
        self.MainMenu.Append(self.StreamOperationsMenu, "StreamO&perations")
        # ## Data Checker
        if datacheck:
            self.CheckDataMenu = wx.Menu()
            self.CheckDefinitiveDataSelect = wx.MenuItem(self.CheckDataMenu, 701, "Check &definitive data...\tCtrl+H", "Check data", wx.ITEM_NORMAL)
            self.CheckDataMenu.AppendItem(self.CheckDefinitiveDataSelect)
            self.OpenLogFileSelect = wx.MenuItem(self.CheckDataMenu, 702, "Open MagP&y log...\tCtrl+Y", "Open log", wx.ITEM_NORMAL)
            self.CheckDataMenu.AppendItem(self.OpenLogFileSelect)
            self.MainMenu.Append(self.CheckDataMenu, "C&heck Data")
        # ## Options Menu
        self.OptionsMenu = wx.Menu()
        self.OptionsInitItem = wx.MenuItem(self.OptionsMenu, 401, "&Basic initialisation parameter\tCtrl+B", "Modify general defaults (e.g. DB, paths)", wx.ITEM_NORMAL)
        self.OptionsMenu.AppendItem(self.OptionsInitItem)
        self.OptionsMenu.AppendSeparator()
        self.OptionsDIItem = wx.MenuItem(self.OptionsMenu, 402, "DI &initialisation parameter\tCtrl+I", "Modify DI related parameters (e.g. thresholds, paths, input sheet layout)", wx.ITEM_NORMAL)
        self.OptionsMenu.AppendItem(self.OptionsDIItem)
        #self.OptionsMenu.AppendSeparator()
        #self.OptionsObsItem = wx.MenuItem(self.OptionsMenu, 403, "Observator&y specifications\tCtrl+Y", "Modify observatory specific meta data (e.g. pears, offsets)", wx.ITEM_NORMAL)
        #self.OptionsMenu.AppendItem(self.OptionsObsItem)
        self.MainMenu.Append(self.OptionsMenu, "&Options")
        self.HelpMenu = wx.Menu()
        self.HelpAboutItem = wx.MenuItem(self.HelpMenu, 301, "&About...", "Display general information about the program", wx.ITEM_NORMAL)
        self.HelpMenu.AppendItem(self.HelpAboutItem)
        self.HelpReadFormatsItem = wx.MenuItem(self.HelpMenu, 302, "Read Formats...", "Supported data formats to read", wx.ITEM_NORMAL)
        self.HelpMenu.AppendItem(self.HelpReadFormatsItem)
        self.HelpWriteFormatsItem = wx.MenuItem(self.HelpMenu, 303, "Write Formats...", "Supported data formats to write", wx.ITEM_NORMAL)
        self.HelpMenu.AppendItem(self.HelpWriteFormatsItem)
        self.MainMenu.Append(self.HelpMenu, "&Help")
        self.SetMenuBar(self.MainMenu)
        # Menu Bar end


        self.__set_properties()

        # BindingControls on the menu
        self.Bind(wx.EVT_MENU, self.OnOpenDir, self.DirOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenFile, self.FileOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenURL, self.WebOpen)
        self.Bind(wx.EVT_MENU, self.OnOpenDB, self.DBOpen)
        self.Bind(wx.EVT_MENU, self.OnExportData, self.ExportData)
        self.Bind(wx.EVT_MENU, self.OnFileQuit, self.FileQuitItem)
        self.Bind(wx.EVT_MENU, self.OnDBConnect, self.DBConnect)
        self.Bind(wx.EVT_MENU, self.OnDBInit, self.DBInit)
        self.Bind(wx.EVT_MENU, self.OnStreamList, self.StreamListSelect)
        self.Bind(wx.EVT_MENU, self.OnStreamAdd, self.StreamAddListSelect)
        self.Bind(wx.EVT_MENU, self.onLoadDI, self.DIPath2DI)
        self.Bind(wx.EVT_MENU, self.onDefineVario, self.DIPath2Vario)
        self.Bind(wx.EVT_MENU, self.onDefineScalar, self.DIPath2Scalar)
        self.Bind(wx.EVT_MENU, self.onInputSheet, self.DIInputSheet)
        self.Bind(wx.EVT_MENU, self.OnOptionsInit, self.OptionsInitItem)
        self.Bind(wx.EVT_MENU, self.OnOptionsDI, self.OptionsDIItem)
        #self.Bind(wx.EVT_MENU, self.OnOptionsObs, self.OptionsObsItem)
        self.Bind(wx.EVT_MENU, self.OnHelpAbout, self.HelpAboutItem)
        self.Bind(wx.EVT_MENU, self.OnHelpReadFormats, self.HelpReadFormatsItem)
        self.Bind(wx.EVT_MENU, self.OnHelpWriteFormats, self.HelpWriteFormatsItem)
        self.Bind(wx.EVT_CLOSE, self.OnFileQuit)  #Bind the EVT_CLOSE event to FileQuit()
        if datacheck:
            self.Bind(wx.EVT_MENU, self.OnCheckDefinitiveData, self.CheckDefinitiveDataSelect)
            self.Bind(wx.EVT_MENU, self.OnCheckOpenLog, self.OpenLogFileSelect)
        # BindingControls on the notebooks
        #       Stream Page
        # ------------------------
        #self.Bind(wx.EVT_BUTTON, self.onOpenStreamButton, self.menu_p.str_page.openStreamButton)
        self.Bind(wx.EVT_BUTTON, self.onTrimStreamButton, self.menu_p.str_page.trimStreamButton)
        self.Bind(wx.EVT_BUTTON, self.onSelectKeys, self.menu_p.str_page.selectKeysButton)
        self.Bind(wx.EVT_BUTTON, self.onExtractData, self.menu_p.str_page.extractValuesButton)
        self.Bind(wx.EVT_BUTTON, self.onChangePlotOptions, self.menu_p.str_page.changePlotButton)
        self.Bind(wx.EVT_BUTTON, self.onRestoreData, self.menu_p.str_page.restoreButton)
        self.Bind(wx.EVT_CHECKBOX, self.onAnnotateCheckBox, self.menu_p.str_page.annotateCheckBox)
        self.Bind(wx.EVT_CHECKBOX, self.onErrorBarCheckBox, self.menu_p.str_page.errorBarsCheckBox)
        self.Bind(wx.EVT_CHECKBOX, self.onConfinexCheckBox, self.menu_p.str_page.confinexCheckBox)
        self.Bind(wx.EVT_BUTTON, self.onDailyMeansButton, self.menu_p.str_page.dailyMeansButton)
        self.Bind(wx.EVT_BUTTON, self.onApplyBCButton, self.menu_p.str_page.applyBCButton)
        self.Bind(wx.EVT_RADIOBOX, self.onChangeComp, self.menu_p.str_page.compRadioBox)
        self.Bind(wx.EVT_RADIOBOX, self.onChangeSymbol, self.menu_p.str_page.symbolRadioBox)
        #        Flags Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.onFlagOutlierButton, self.menu_p.fla_page.flagOutlierButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagSelectionButton, self.menu_p.fla_page.flagSelectionButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagRangeButton, self.menu_p.fla_page.flagRangeButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagLoadButton, self.menu_p.fla_page.flagLoadButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagSaveButton, self.menu_p.fla_page.flagSaveButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagDropButton, self.menu_p.fla_page.flagDropButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagMinButton, self.menu_p.fla_page.flagMinButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagMaxButton, self.menu_p.fla_page.flagMaxButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagClearButton, self.menu_p.fla_page.flagClearButton)

        #        Meta Page
        # --------------------------
        #self.Bind(wx.EVT_BUTTON, self.onFilterButton, self.menu_p.met_page.filterButton)
        # Contains General info on top - previously on analysis page
        # add sensor id, sensor name to general info
        # add button with GetFromDB, WriteToDB (only active when DB connected) - WriteToDB only specific dataID or all sensorsID
        # provide text boxes with data, sensor and station related info
        #     Edit/Review Data related meta data
        #     button
        #     textbox with existing Meta
        #     Edit/Review Sensor related meta data
        #     ....
        #     .... and so on
        self.Bind(wx.EVT_BUTTON, self.onMetaGetDBButton, self.menu_p.met_page.getDBButton)
        self.Bind(wx.EVT_BUTTON, self.onMetaPutDBButton, self.menu_p.met_page.putDBButton)
        self.Bind(wx.EVT_BUTTON, self.onMetaDataButton, self.menu_p.met_page.MetaDataButton)
        self.Bind(wx.EVT_BUTTON, self.onMetaSensorButton, self.menu_p.met_page.MetaSensorButton)
        self.Bind(wx.EVT_BUTTON, self.onMetaStationButton, self.menu_p.met_page.MetaStationButton)
        #        Analysis Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.onDerivativeButton, self.menu_p.ana_page.derivativeButton)
        self.Bind(wx.EVT_BUTTON, self.onRotationButton, self.menu_p.ana_page.rotationButton)
        self.Bind(wx.EVT_BUTTON, self.onFitButton, self.menu_p.ana_page.fitButton)
        self.Bind(wx.EVT_BUTTON, self.onMeanButton, self.menu_p.ana_page.meanButton)
        self.Bind(wx.EVT_BUTTON, self.onMaxButton, self.menu_p.ana_page.maxButton)
        self.Bind(wx.EVT_BUTTON, self.onMinButton, self.menu_p.ana_page.minButton)
        self.Bind(wx.EVT_BUTTON, self.onFlagmodButton, self.menu_p.ana_page.flagmodButton)
        self.Bind(wx.EVT_BUTTON, self.onOffsetButton, self.menu_p.ana_page.offsetButton)
        self.Bind(wx.EVT_BUTTON, self.onFilterButton, self.menu_p.ana_page.filterButton)
        self.Bind(wx.EVT_BUTTON, self.onSmoothButton, self.menu_p.ana_page.smoothButton)
        self.Bind(wx.EVT_BUTTON, self.onResampleButton, self.menu_p.ana_page.resampleButton)
        self.Bind(wx.EVT_BUTTON, self.onActivityButton, self.menu_p.ana_page.activityButton)
        self.Bind(wx.EVT_BUTTON, self.onBaselineButton, self.menu_p.ana_page.baselineButton)
        self.Bind(wx.EVT_BUTTON, self.onDeltafButton, self.menu_p.ana_page.deltafButton)
        self.Bind(wx.EVT_BUTTON, self.onCalcfButton, self.menu_p.ana_page.calcfButton)
        self.Bind(wx.EVT_BUTTON, self.onPowerButton, self.menu_p.ana_page.powerButton)
        self.Bind(wx.EVT_BUTTON, self.onSpectrumButton, self.menu_p.ana_page.spectrumButton)
        #        DI Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.onLoadDI, self.menu_p.abs_page.loadDIButton)
        self.Bind(wx.EVT_BUTTON, self.onDefineVario, self.menu_p.abs_page.defineVarioButton)
        self.Bind(wx.EVT_BUTTON, self.onDefineScalar, self.menu_p.abs_page.defineScalarButton)
        self.Bind(wx.EVT_BUTTON, self.onDIAnalyze, self.menu_p.abs_page.AnalyzeButton)
        self.Bind(wx.EVT_BUTTON, self.onDISetParameter, self.menu_p.abs_page.advancedButton)
        self.Bind(wx.EVT_BUTTON, self.onSaveDIData, self.menu_p.abs_page.SaveLogButton)
        self.Bind(wx.EVT_BUTTON, self.onClearDIData, self.menu_p.abs_page.ClearLogButton)
        #        Report Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.onSaveLogButton, self.menu_p.rep_page.saveLoggerButton)
        self.menu_p.rep_page.logMsg('Begin logging...')
        # Eventually kill this redirection because it might cause problems from other classes
        #redir=RedirectText(self.menu_p.rep_page.logMsg) # Start redirecting stdout to log window
        #sys.stdout=redir
        #        Monitor Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.onConnectMARCOSButton, self.menu_p.com_page.getMARCOSButton)
        self.Bind(wx.EVT_BUTTON, self.onConnectMARTASButton, self.menu_p.com_page.getMARTASButton)
        #self.Bind(wx.EVT_BUTTON, self.onConnectMQTTButton, self.menu_p.com_page.getMQTTButton)
        self.Bind(wx.EVT_BUTTON, self.onStartMonitorButton, self.menu_p.com_page.startMonitorButton)
        self.Bind(wx.EVT_BUTTON, self.onStopMonitorButton, self.menu_p.com_page.stopMonitorButton)
        self.Bind(wx.EVT_BUTTON, self.onLogDataButton, self.menu_p.com_page.saveMonitorButton)

        # Connect to database
        self._db_connect(self.options.get('host',''), self.options.get('user',''), self.options.get('passwd',''), self.options.get('dbname',''))


        # Disable yet unavailbale buttons
        # --------------------------
        self.DeactivateAllControls()

        self.sp.SplitVertically(self.plot_p,self.menu_p,800)

    def __set_properties(self):
        self.SetTitle("MagPy")
        self.SetSize((1200, 800))
        self.SetFocus()
        self.StatusBar.SetStatusWidths([-1, -1])
        # statusbar fields
        StatusBar_fields = ["Ready", ""]
        for i in range(len(StatusBar_fields)):
            self.StatusBar.SetStatusText(StatusBar_fields[i], i)
        self.menu_p.SetMinSize((400, 750))
        self.plot_p.SetMinSize((100, 100))


    def InitPlotParameter(self, keylist = None):
        # Kwargs for plotting
        #self.annotate = True
        self.menu_p.str_page.annotateCheckBox.SetValue(True)
        #self.errorbars = False
        self.menu_p.str_page.errorBarsCheckBox.SetValue(False)
        #self.confinex = False
        self.menu_p.str_page.confinexCheckBox.SetValue(False)
        #self.fullday = False
        #self.includeid = False
        #self.grid = True
        #self.padding = None
        #self.specialdict={}
        self.colorlist = ['b','g','m','c','y','k','b','g','m','c','y','k']
        #self.stormphases=None
        #self.t_stormphases={}
        #self.function=None
        #self.plottype='discontinuous'
        #self.labels=False
        self.resolution=None
        self.monitorSource=None
        #collist=['b','g','m','c','y','k','b','g','m','c','y','k']

        # please note: symbol and colorlists are defined in ActivateControls

        #print ("colorlist", collist[:lenkeys])
        #self.plotopt = {'labels':'None' , 'padding': 'None', 'stormphases': False, 'specialdict': {}, 'bartrange':'None', 'bgcolor': 'white', 'colorlist': ",".join(collist[:lenkeys]) ,'fullday':'False', 'grid':'True','gridcolor':'#316931', 'includeid':'False', 'labelcolor':'0.2', 'legendposition':'upper left', 'plottitle':'', 'plottype':'discontinuous', 'symbollist':",".join(self.symbollist),'t_stormphases':'None', 'opacity':'0.0'}

        self.plotopt = {'labels':None ,
                        'errorbars':False,
                        'confinex':False,
                        'annotate':False,
                        'padding': None,
                        'stormphases': False,
                        'specialdict': {},
                        'bartrange':0.06,
                        'bgcolor': 'white',
                        'colorlist': [],
                        'fullday':False,
                        'grid':True,
                        'gridcolor':'#316931',
                        'includeid':False,
                        'labelcolor':'0.2',
                        'legendposition':'upper left',
                        'plottitle':'',
                        'plottype':'discontinuous',
                        'symbollist': [],
                        't_stormphases':{},
                        'opacity':1.0,
                        'function':None}


    def initParameter(self, dictionary):
        # Variable initializations
        pwd = dictionary.get('passwd')
        #self.passwd = base64.b64decode(pwd)
        self.dirname = dictionary.get('dirname','')
        self.dipathlist = dictionary.get('dipathlist','')
        self.options = dictionary
        self.options['passwd'] = base64.b64decode(pwd)



    # ################
    # Updating and reinitiatzation methods:

    def DeactivateAllControls(self):
        """
        DESCRIPTION
            To be used at start and when an empty stream is loaded
            Deactivates all control elements
        """
        # Menu
        self.ExportData.Enable(False)

        # Stream
        self.menu_p.str_page.pathTextCtrl.Disable()        # remain disabled
        self.menu_p.str_page.fileTextCtrl.Disable()        # remain disabled
        self.menu_p.str_page.startDatePicker.Disable()     # always
        self.menu_p.str_page.startTimePicker.Disable()     # always
        self.menu_p.str_page.endDatePicker.Disable()       # always
        self.menu_p.str_page.endTimePicker.Disable()       # always
        ## TODO Modify method below - when directory/database is selected, automatically open dialog
        ## to modify time range and other read options
        #self.menu_p.str_page.openStreamButton.Disable()
        self.menu_p.str_page.trimStreamButton.Disable()    # always
        self.menu_p.str_page.restoreButton.Disable()       # always
        self.menu_p.str_page.selectKeysButton.Disable()    # always
        self.menu_p.str_page.extractValuesButton.Disable() # always
        self.menu_p.str_page.changePlotButton.Disable()    # always
        self.menu_p.fla_page.flagOutlierButton.Disable()   # always
        self.menu_p.fla_page.flagSelectionButton.Disable() # always
        self.menu_p.fla_page.flagRangeButton.Disable()     # always
        self.menu_p.fla_page.flagLoadButton.Disable()      # always
        self.menu_p.fla_page.flagMinButton.Disable()       # always
        self.menu_p.fla_page.flagMaxButton.Disable()       # always
        self.menu_p.fla_page.flagClearButton.Disable()     # always
        self.menu_p.fla_page.xCheckBox.Disable()           # always
        self.menu_p.fla_page.yCheckBox.Disable()           # always
        self.menu_p.fla_page.zCheckBox.Disable()           # always
        self.menu_p.fla_page.fCheckBox.Disable()           # always
        self.menu_p.fla_page.FlagIDComboBox.Disable()      # always
        self.menu_p.fla_page.flagDropButton.Disable()      # activated if annotation are present
        self.menu_p.fla_page.flagSaveButton.Disable()      # activated if annotation are present
        self.menu_p.str_page.dailyMeansButton.Disable()    # activated for DI data
        self.menu_p.str_page.applyBCButton.Disable()       # activated if DataAbsInfo is present
        self.menu_p.str_page.annotateCheckBox.Disable()    # activated if annotation are present
        self.menu_p.str_page.errorBarsCheckBox.Disable()   # activated delta columns are present and not DI file
        self.menu_p.str_page.confinexCheckBox.Disable()    # always
        self.menu_p.str_page.compRadioBox.Disable()        # activated if xyz,hdz or idf
        self.menu_p.str_page.symbolRadioBox.Disable()      # activated if less then 2000 points, active if DI data

        # Meta
        self.menu_p.met_page.getDBButton.Disable()         # activated when DB is connected
        self.menu_p.met_page.putDBButton.Disable()         # activated when DB is connected
        self.menu_p.met_page.MetaDataButton.Disable()      # remain disabled
        self.menu_p.met_page.MetaSensorButton.Disable()    # remain disabled
        self.menu_p.met_page.MetaStationButton.Disable()   # remain disabled
        #if PLATFORM.startswith('linux'):
        #    self.menu_p.met_page.stationTextCtrl.Disable()     # remain disabled
        #    self.menu_p.met_page.sensorTextCtrl.Disable()      # remain disabled
        #    self.menu_p.met_page.dataTextCtrl.Disable()        # remain disabled
        # DI
        self.menu_p.abs_page.AnalyzeButton.Disable()       # activate if DI data is present i.e. diTextCtrl contains data
        self.menu_p.abs_page.loadDIButton.Enable()         # remain enabled
        self.menu_p.abs_page.defineVarioButton.Enable()    # remain enabled
        self.menu_p.abs_page.defineScalarButton.Enable()   # remain enabled
        #if PLATFORM.startswith('linux'):
        #    self.menu_p.abs_page.dilogTextCtrl.Disable()       # remain disabled -- WINDOWS Prob - scrolling will not work
        #    self.menu_p.abs_page.scalarTextCtrl.Disable()      # remain disabled
        #    self.menu_p.abs_page.varioTextCtrl.Disable()       # remain disabled
        #    self.menu_p.abs_page.diTextCtrl.Disable()          # remain disabled
        self.menu_p.abs_page.ClearLogButton.Disable()      # Activate if log contains text
        self.menu_p.abs_page.SaveLogButton.Disable()      # Activate if log contains text
        self.menu_p.abs_page.varioTextCtrl.SetValue(self.options.get('divariopath',''))
        self.menu_p.abs_page.scalarTextCtrl.SetValue(self.options.get('discalarpath',''))

        # Analysis
        self.menu_p.ana_page.rotationButton.Disable()      # if xyz magnetic data
        self.menu_p.ana_page.derivativeButton.Disable()    # always
        self.menu_p.ana_page.fitButton.Disable()           # always
        self.menu_p.ana_page.meanButton.Disable()          # always
        self.menu_p.ana_page.maxButton.Disable()           # always
        self.menu_p.ana_page.minButton.Disable()           # always
        self.menu_p.ana_page.flagmodButton.Disable()       # always
        self.menu_p.ana_page.offsetButton.Disable()        # always
        self.menu_p.ana_page.resampleButton.Disable()        # always
        self.menu_p.ana_page.filterButton.Disable()        # always
        self.menu_p.ana_page.smoothButton.Disable()        # always
        self.menu_p.ana_page.activityButton.Disable()      # if xyz, hdz magnetic data
        self.menu_p.ana_page.baselineButton.Disable()      # if absstream in streamlist
        self.menu_p.ana_page.deltafButton.Disable()        # if xyzf available
        self.menu_p.ana_page.calcfButton.Disable()        # if xyz available
        self.menu_p.ana_page.powerButton.Disable()         # always
        self.menu_p.ana_page.spectrumButton.Disable()      # always
        #self.menu_p.ana_page.mergeButton.Disable()         # if len(self.streamlist) > 1
        #self.menu_p.ana_page.subtractButton.Disable()      # if len(self.streamlist) > 1
        #self.menu_p.ana_page.stackButton.Disable()         # if len(self.streamlist) > 1

        # Report
        #if PLATFORM.startswith('linux'):
        #    self.menu_p.rep_page.logger.Disable()              # remain disabled

        # Monitor
        #if PLATFORM.startswith('linux'):
        #    self.menu_p.com_page.connectionLogTextCtrl.Disable()  # remain disabled
        self.menu_p.com_page.startMonitorButton.Disable()  # always
        self.menu_p.com_page.stopMonitorButton.Disable()   # always
        self.menu_p.com_page.saveMonitorButton.Disable()   # always
        self.menu_p.com_page.coverageTextCtrl.Disable()    # always
        self.menu_p.com_page.frequSlider.Disable()         # always
        self.menu_p.com_page.marcosLabel.SetBackgroundColour((255,23,23))
        self.menu_p.com_page.martasLabel.SetBackgroundColour((255,23,23))
        #self.menu_p.com_page.mqttLabel.SetBackgroundColour((255,23,23))
        self.menu_p.com_page.marcosLabel.SetValue('not connected')
        self.menu_p.com_page.martasLabel.SetValue('not connected')
        #self.menu_p.com_page.mqttLabel.SetValue('not connected')

    def ActivateControls(self,stream):
        """
        DESCRIPTION
            Checks contents of stream and state of program.
            Activates controls in dependency of the checks
        """
        baselineexists = False
        # initially reset all controls
        self.DeactivateAllControls()
        if not len(stream.ndarray[0]) > 0:
            self.changeStatusbar("No data available")
            return

        # Always part
        # --------------------------------
        # Length
        n = stream.length()[0]
        keys = stream._get_key_headers()
        keystr = ','.join(keys)

        if len(self.shownkeylist) == 0:   ## Initiaize self.shownkeylist if not yet done
            keylist = [elem for elem in keys if elem in NUMKEYLIST]
            self.shownkeylist = keylist[:9]

        # Reset line/point selection
        if n < 2000:
            self.menu_p.str_page.symbolRadioBox.Enable()
        else:
            self.menu_p.str_page.symbolRadioBox.SetStringSelection('line')
            self.menu_p.str_page.symbolRadioBox.Disable()

        if len(self.plotopt.get('symbollist',[])) == len(self.shownkeylist):
            # everything is fine use current symbollist
            pass
        elif self.menu_p.str_page.symbolRadioBox.GetStringSelection() == 'line':
            self.symbollist = ['-'] * len(self.shownkeylist)
            self.plotopt['symbollist'] =  ['-'] * len(self.shownkeylist)
        else:
            self.symbollist = ['o'] * len(self.shownkeylist)
            self.plotopt['symbollist'] =  ['o'] * len(self.shownkeylist)

        # Other plot options, which are related to len(shownkeylist)
        if not len(self.plotopt.get('colorlist',[])) == len(self.shownkeylist):
            self.plotopt['colorlist'] = self.colorlist[:len(self.shownkeylist)]
        self.UpdatePlotOptions(self.shownkeylist)

        # Sampling rate
        try:
            sr = stream.samplingrate()
        except:
            print ("Sampling rate determinations failed - might happen in DI files")
            sr = 9999
        # Coverage
        ind = np.argmin(stream.ndarray[0].astype(float))
        mintime = stream._get_min('time')
        maxtime = stream._get_max('time')
        # Flag column
        commidx = KEYLIST.index('comment')
        commcol = stream.ndarray[commidx]
        commcol = np.asarray([el for el in commcol if not el in ['','-',np.nan]])
        # Delta
        deltas = False
        if 'dx' in keys or 'dy' in keys or 'dz' in keys or 'df' in keys:
            deltas = True
        # Essential header info
        comps = stream.header.get('DataComponents','')[:3]
        sensorid = stream.header.get('SensorID','')
        dataid = self.plotstream.header.get('DataID','')
        formattype = self.plotstream.header.get('DataFormat','')
        absinfo = self.plotstream.header.get('DataAbsInfo',None)
        metadatatext = ''
        metasensortext = ''
        metastationtext = ''
        for key in stream.header:
            #print ("Activate", key)
            if key.startswith('Data'):
                 value = stream.header.get(key,'')
                 #try:  # python 3
                 if not isinstance(value, basestring): # p3: str
                     try:
                         if self.plotstream._is_number(value):
                             pass
                         else:
                             value = 'object - contains complex data'
                     except:
                         value = 'object - contains complex data'
                 #print ("-- ", value)
                 metadatatext += "{}: \t{}\n".format(key.replace('Data',''),value)
            if key.startswith('Sensor'):
                 metasensortext += "{}: \t{}\n".format(key.replace('Sensor',''),stream.header.get(key,'')) # key.replace('Sensor','')+': \t'+stream.header.get(key,'')+'\n'
            if key.startswith('Station'):
                 metastationtext += "{}: \t{}\n".format(key.replace('Station',''),stream.header.get(key,'')) #key.replace('Station','')+': \t'+stream.header.get(key,'')+'\n'

        # Append baselineinfo to baselinedictlist
        if formattype == 'MagPyDI':
            filename = self.menu_p.str_page.fileTextCtrl.GetValue()
            basedict = {'startdate':mintime,'enddate':maxtime, 'filename':filename, 'streamidx':len(self.streamlist)-1}
            self.baselinedictlst.append(basedict)

        def checkbaseline(baselinedictlst, sensorid, mintime, maxtime):
            """
              DESCRIPTION:
                check whether valid baseline info is existing
              PARAMETER:
                use global self.baselinedictlist
                set baselineidxlist
              RETURNS:
                returns baselineidxlst e.g. [1,3,4] which contains currently
            """
            # check self.baseline dictionary
            baselineidxlst  = []
            #print (baselinedictlst)
            for basedict in baselinedictlst:
                startdate = basedict['startdate']
                enddate = basedict['enddate']
                # Take all baslines, let the user choose whether its the correct instrument
                # -- extrapolation needs to be tested
                #if mintime <= startdate <= maxtime or mintime <= enddate <= maxtime or (startdate <= mintime and enddate >= maxtime):
                baselineidxlst.append(basedict['streamidx'])
                """
                if sensorid in basedict['filename']:
                    #print ("found filename")
                    if mintime <= startdate <= maxtime or mintime <= enddate <= maxtime or (startdate <= mintime and enddate >= maxtime):
                        baselineidxlst.append(basedict['streamidx'])
                """
            return baselineidxlst

        # Activate "always" fields
        # ----------------------------------------
        # menu
        self.ExportData.Enable(True)

        # ----------------------------------------
        # stream page
        self.menu_p.str_page.startDatePicker.Enable()     # always
        self.menu_p.str_page.startTimePicker.Enable()     # always
        self.menu_p.str_page.endDatePicker.Enable()       # always
        self.menu_p.str_page.endTimePicker.Enable()       # always
        self.menu_p.str_page.trimStreamButton.Enable()    # always
        self.menu_p.str_page.restoreButton.Enable()       # always
        self.menu_p.str_page.selectKeysButton.Enable()    # always
        self.menu_p.str_page.extractValuesButton.Enable() # always
        self.menu_p.str_page.changePlotButton.Enable()    # always
        self.menu_p.fla_page.flagOutlierButton.Enable()   # always
        self.menu_p.fla_page.flagSelectionButton.Enable() # always
        self.menu_p.fla_page.flagRangeButton.Enable()     # always
        self.menu_p.fla_page.flagLoadButton.Enable()      # always
        self.menu_p.fla_page.flagMinButton.Enable()       # always
        self.menu_p.fla_page.flagMaxButton.Enable()       # always
        self.menu_p.fla_page.flagClearButton.Enable()       # always
        self.menu_p.fla_page.FlagIDComboBox.Enable()      # always
        self.menu_p.str_page.confinexCheckBox.Enable()    # always
        self.menu_p.met_page.MetaDataButton.Enable()      # always
        self.menu_p.met_page.MetaSensorButton.Enable()    # always
        self.menu_p.met_page.MetaStationButton.Enable()   # always

        # ----------------------------------------
        # analysis page
        self.menu_p.ana_page.derivativeButton.Enable()    # always
        self.menu_p.ana_page.fitButton.Enable()           # always
        self.menu_p.ana_page.meanButton.Enable()          # always
        self.menu_p.ana_page.maxButton.Enable()           # always
        self.menu_p.ana_page.minButton.Enable()           # always
        self.menu_p.ana_page.flagmodButton.Enable()       # always
        self.menu_p.ana_page.offsetButton.Enable()        # always
        self.menu_p.ana_page.resampleButton.Enable()        # always
        self.menu_p.ana_page.filterButton.Enable()        # always
        self.menu_p.ana_page.smoothButton.Enable()        # always
        if self.options.get('experimental'):
            self.menu_p.ana_page.powerButton.Enable()         # if experimental
            self.menu_p.ana_page.spectrumButton.Enable()      # if experimental


        # Selective fields
        # ----------------------------------------
        #print ("COMPONENTS", comps)
        if comps in ['xyz','XYZ','hdz','HDZ','idf','IDF','hez','HEZ','DIF','dif']:
            self.menu_p.str_page.compRadioBox.Enable()
            if comps in ['hdz','HDZ']:
                self.menu_p.str_page.compRadioBox.SetStringSelection('hdz')
                self.compselect = 'hdz'
            elif comps in ['idf','IDF','DIF','dif']:
                self.menu_p.str_page.compRadioBox.SetStringSelection('idf')
                self.compselect = 'idf'
            else:
                self.menu_p.str_page.compRadioBox.SetStringSelection('xyz')
                self.compselect = 'xyz'

        if len(commcol) > 0:
            self.menu_p.fla_page.flagDropButton.Enable()     # activated if annotation are present
            self.menu_p.fla_page.flagSaveButton.Enable()      # activated if annotation are present
            self.menu_p.str_page.annotateCheckBox.Enable()    # activated if annotation are present
            if self.menu_p.str_page.annotateCheckBox.GetValue():
                self.menu_p.str_page.annotateCheckBox.SetValue(True)
                self.plotopt['annotate'] = True                   # activate annotation
        if formattype == 'MagPyDI':
            self.menu_p.str_page.dailyMeansButton.Enable()    # activated for DI data
            self.menu_p.str_page.symbolRadioBox.Enable()      # activated for DI data
        if deltas and not formattype == 'MagPyDI' and not sensorid.startswith('GP20S3'):
            self.menu_p.str_page.errorBarsCheckBox.Enable()   # activated if delta columns are present and not DI file
        if not absinfo == None:
            self.menu_p.str_page.applyBCButton.Enable()       # activated if DataAbsInfo is present
        if n < 2000:
            self.menu_p.str_page.symbolRadioBox.Enable()      # activated if less then 2000 points, active if DI data
        if not dataid == '' and self.db:
            self.menu_p.met_page.getDBButton.Enable()         # activated when DB is connected
            self.menu_p.met_page.putDBButton.Enable()         # activated when DB is connected
        if not str(self.menu_p.abs_page.dilogTextCtrl.GetValue()) == '':
            self.menu_p.abs_page.ClearLogButton.Enable()
            self.menu_p.abs_page.SaveLogButton.Enable()

        if 'x' in keys and 'y' in keys and 'z' in keys:
            self.menu_p.ana_page.rotationButton.Enable()      # activate if vector appears to be present
            self.menu_p.ana_page.activityButton.Enable()      # activate if vector appears to be present
            self.menu_p.ana_page.calcfButton.Enable()    # activate if vector present
            if 'f' in keys and not 'df' in keys:
                self.menu_p.ana_page.deltafButton.Enable()    # activate if full vector present
            if not formattype == 'MagPyDI':
                #print ("Checking baseline info")
                self.baselineidxlst = checkbaseline(self.baselinedictlst, sensorid, mintime, maxtime)
                if len(self.baselineidxlst) > 0:
                    self.menu_p.ana_page.baselineButton.Enable()  # activate if baselinedata is existing


        # Update "information" fields
        # ----------------------------------------
        self.menu_p.met_page.amountTextCtrl.SetValue(str(n))
        self.menu_p.met_page.samplingrateTextCtrl.SetValue(str(sr))
        self.menu_p.met_page.keysTextCtrl.SetValue(keystr)
        self.menu_p.met_page.typeTextCtrl.SetValue(formattype)
        self.menu_p.met_page.dataTextCtrl.SetValue(metadatatext)
        self.menu_p.met_page.sensorTextCtrl.SetValue(metasensortext)
        self.menu_p.met_page.stationTextCtrl.SetValue(metastationtext)

        self.menu_p.str_page.startDatePicker.SetValue(pydate2wxdate(num2date(mintime)))
        self.menu_p.str_page.endDatePicker.SetValue(pydate2wxdate(num2date(maxtime)))
        self.menu_p.str_page.startTimePicker.SetValue(num2date(mintime).strftime('%X'))
        self.menu_p.str_page.endTimePicker.SetValue(num2date(maxtime).strftime('%X'))

        self.menu_p.abs_page.varioTextCtrl.SetValue(self.options.get('divariopath',''))
        self.menu_p.abs_page.scalarTextCtrl.SetValue(self.options.get('discalarpath',''))

    def InitialRead(self,stream):
        """
        DESCRIPTION
            Backups stream content and adds current strem and header info to streamlist and headerlist.
            Creates plotstream copy and stores pointer towards lists.
            Checks whether ndarray is resent and whether data is present at all
        """

        if not len(stream.ndarray[0]) > 0:
            stream = stream.linestruct2ndarray()
        if not len(stream.ndarray[0]) > 0:
            self.DeactivateAllControls()
            self.changeStatusbar("No data available")
            return False
        self.stream = stream

        self.plotstream = self.stream.copy()
        currentstreamindex = len(self.streamlist)
        self.streamlist.append(self.stream)
        self.headerlist.append(self.stream.header)
        self.currentstreamindex = currentstreamindex
        # Moved the following to InitialPlot
        #self.streamkeylist.append(self.stream._get_key_headers())
        #self.plotoptlist.append(self.plotopt)

        return True


    def UpdatePlotOptions(self,keylist):
        #print ("Update plot characteristics")
        # check if lists:
        #special = self.plotopt.get('specialdict',None)
        pads = self.plotopt.get('padding',None)
        labs = self.plotopt.get('labels',None)

        if not pads or not len(pads[0]) == len(keylist):
            #print ("Padding length not fitting")
            self.plotopt['padding']= [[0] * len(keylist)]

        if not labs or not len(labs[0]) == len(keylist):
            #print ("Labels length not fitting")
            self.plotopt['labels']= None

        #if not special or not len(special[0]) == len(keylist):
        #    #print ("specialdict length not fitting")
        #    self.plotopt['specialdict']= None


    def UpdatePlotCharacteristics(self,stream):
        """
        DESCRIPTION
            Checks and activates plot options, checks for correct lengths of all list options
        """

        # Some general Checks on Stream
        # ##############################
        # 1. Preslect first nine keys and set up default options
        keylist = []
        keylist = stream._get_key_headers(limit=9)
        # TODO: eventually remove keys with high percentage of nans
        #for key in keylist:
        #    ar = [eval('elem.'+key) for elem in stream if not isnan(eval('elem.'+key))]
        #    div = float(len(ar))/float(len(stream))*100.0
        #    if div <= 5.:
        #        keylist.remove(key)
        keylist = [elem for elem in keylist if elem in NUMKEYLIST]

        # The following will be overwritten by ActivateControls
        self.symbollist = ['-'] * len(keylist)
        self.plotopt['symbollist'] =  ['-'] * len(keylist)
        self.plotopt['colorlist']=self.colorlist[:len(keylist)]
        self.plotopt['plottitle'] = stream.header.get('StationID')

        try:
            tmin,tmax = stream._find_t_limits()
            diff = (tmax.date()-tmin.date()).days
            if diff < 5 and not diff == 0:
                self.plotopt['plottitle'] = "{}: {} to {}".format(stream.header.get('StationID'),tmin.date(),tmax.date())
            elif diff == 0:
                self.plotopt['plottitle'] = "{}: {}".format(stream.header.get('StationID'),tmin.date())
        except:
            pass

        self.menu_p.str_page.symbolRadioBox.SetStringSelection('line')
        self.menu_p.str_page.dailyMeansButton.Disable()

        # 2. If stream too long then don't allow scatter plots -- too slowly
        if stream.length()[0] < 2000:
            self.menu_p.str_page.symbolRadioBox.Enable()
        else:
            self.menu_p.str_page.symbolRadioBox.Disable()

        # 3. If DataFormat = MagPyDI then preselect scatter, and idf and basevalues
        if stream.header.get('DataFormat') == 'MagPyDI':
            self.menu_p.str_page.symbolRadioBox.Enable()
            self.menu_p.str_page.symbolRadioBox.SetStringSelection('point')
            self.shownkeylist = keylist
            if len(stream.ndarray[KEYLIST.index('x')]) > 0:
                keylist = ['x','y','z','dx','dy','dz']
                self.plotopt['padding'] = [[0,0,0,5,0.05,5]]
            else:
                keylist = ['dx','dy','dz']
                self.plotopt['padding'] = [[5,0.05,5]]
            self.symbollist = ['o'] * len(keylist)
            self.plotopt['symbollist'] =  ['o'] * len(keylist)
            self.plotopt['colorlist']=self.colorlist[:len(keylist)]
            # enable daily average button
            self.menu_p.str_page.dailyMeansButton.Enable()

        # 4. If K values are shown: preselect bar chart
        if stream.header.get('DataFormat') == 'MagPyK' or ('var1' in keylist and stream.header.get('col-var1','').startswith('K')):
            #print ("Found K values - apply self.plotopt")
            self.plotopt['specialdict']=[{'var1':[0,9]}]
            pos = keylist.index('var1')
            self.plotopt['symbollist'][pos] = 'z'
            self.plotopt['bartrange'] = 0.06
            self.plotopt['opacity'] = 1.0

        self.shownkeylist = keylist

        """
        # 4. If DataFormat = MagPyDI then preselect scatter, and idf and basevalues
        typus = stream.header.get('DataComponents')
        try:
            typus = typus.lower()[:3]
        except:
            typus = ''
        if typus in ['xyz','hdz','idf']:
            self.compselect = typus
            self.menu_p.str_page.compRadioBox.Enable()
            self.menu_p.str_page.compRadioBox.SetStringSelection(self.compselect)
        else:
            if 'x' in keylist and 'y' in keylist and 'z' in keylist:
                self.compselect = 'xyz'
                self.menu_p.str_page.compRadioBox.Enable()
        """
        # 5. Baseline correction if Object contained in stream
        #if stream.header.get('DataAbsFunctionObject'):
        #    self.menu_p.str_page.applyBCButton.Enable()
        #else:
        #    self.menu_p.str_page.applyBCButton.Disable()

        self.UpdatePlotOptions(keylist)

        return keylist


    def defaultFileDialogOptions(self):
        ''' Return a dictionary with file dialog options that can be
            used in both the save file dialog as well as in the open
            file dialog. '''
        return dict(message='Choose a file', defaultDir=self.dirname,
                    wildcard='*.*')

    def askUserForFilename(self, **dialogOptions):
        dialog = wx.FileDialog(self, **dialogOptions)
        if dialog.ShowModal() == wx.ID_OK:
            userProvidedFilename = True
            self.filename = dialog.GetFilename()
            self.dirname = dialog.GetDirectory()
            #self.SetTitle() # Update the window title with the new filename
        else:
            userProvidedFilename = False
        dialog.Destroy()
        return userProvidedFilename



    def OnInitialPlot(self, stream, restore = False):
        """
        DEFINITION:
            read stream, extract columns with values and display up to three of them by default
            executes guiPlot then
        """

        self.changeStatusbar("Plotting...")

        self.InitPlotParameter()
        # Init Controls
        self.ActivateControls(self.plotstream)

        # Override initial controls: Set setting (like keylist, basic plot options and basevalue selection)
        keylist = self.UpdatePlotCharacteristics(self.plotstream)

        self.menu_p.rep_page.logMsg('- keys: %s' % (', '.join(keylist)))
        #if len(stream) > self.resolution:
        #    self.menu_p.rep_page.logMsg('- warning: resolution of plot reduced by a factor of %i' % (int(len(stream)/self.resolution)))
        # Eventually change symbol as matplotlib reports errors for line plot with many points
        if stream.length()[0] > 200000:
            self.plotopt['symbollist']= ['.'] * len(keylist)

        if not restore:
            self.streamkeylist.append(keylist)
            self.plotoptlist.append(self.plotopt)

        self.plot_p.guiPlot([self.plotstream],[keylist], plotopt=self.plotopt)
        boxes = ['x','y','z','f']
        for box in boxes:
            checkbox = getattr(self.menu_p.fla_page, box + 'CheckBox')
            if box in self.shownkeylist:
                checkbox.Enable()
                colname = self.plotstream.header.get('col-'+box, '')
                if not colname == '':
                    checkbox.SetLabel(colname)
            else:
                checkbox.SetValue(False)
        self.changeStatusbar("Ready")


    def OnPlot(self, stream, keylist, **kwargs):
        """
        DEFINITION:
            read stream and display
        """
        #self.plotopt = {'bgcolor':'green'}

        self.changeStatusbar("Plotting...")
        #print ("ConfineX:", confinex, symbollist)
        """
        self.plot_p.guiPlot([stream],[keylist],padding=padding,specialdict=specialdict,errorbars=errorbars,
                            colorlist=colorlist,symbollist=symbollist,annotate=annotate,
                            includeid=includeid, function=function,plottype=plottype,
                            labels=labels,resolution=resolution,confinex=confinex,plotopt=plotopt)
        """
        #print ("Keys", keylist)
        if stream.length()[0] > 200000:
            self.plotopt['symbollist']= ['.'] * len(keylist)
        # Update Delta F if plotted
        if 'df' in keylist:
            stream = stream.delta_f()

        self.plot_p.guiPlot([stream],[keylist],plotopt=self.plotopt)
        #self.plot_p.guiPlot(stream,keylist,**kwargs)
        if stream.length()[0] > 1 and len(keylist) > 0:
            self.ExportData.Enable(True)
        boxes = ['x','y','z','f']
        for box in boxes:
            checkbox = getattr(self.menu_p.fla_page, box + 'CheckBox')
            if box in self.shownkeylist:
                checkbox.Enable()
                colname = self.plotstream.header.get('col-'+box, '')
                if not colname == '':
                    checkbox.SetLabel(colname)
            else:
                checkbox.SetValue(False)
        self.changeStatusbar("Ready")


    def OnMultiPlot(self, streamlst, keylst, padding=None, specialdict={},errorbars=None,
        colorlist=None,symbollist=None,annotate=None,stormphases=None,
        t_stormphases={},includeid=False,function=None,plottype='discontinuous',
        labels=False,resolution=None, confinex=False, plotopt=None):
        """
        DEFINITION:
            read stream and display
        """
        self.changeStatusbar("Plotting...")

        """
        - labels:       [ (str) ] List of labels for each stream and variable, e.g.:
                        [ ['FGE'], ['POS-1'], ['ENV-T1', 'ENV-T2'] ]
        - padding:      (float/list(list)) List of lists containing paddings for each
                        respective variable, e.g:
                        [ [5], [5], [0.1, 0.2] ]
                        (Enter padding = 5 for all plots to use 5 as padding.)
        - specialdict:  (list(dict)) Same as plot variable, e.g:
                        [ {'z': [100,150]}, {}, {'t1':[7,8]} ]
        """
        #print ("ConfineX:", confinex, symbollist)
        self.plot_p.guiPlot(streamlst,keylst)
        #if stream.length()[0] > 1 and len(keylist) > 0:
        #    self.ExportData.Enable(True)
        self.changeStatusbar("Ready")

    # ################
    # Top menu methods:


    def OnHelpAbout(self, event):

        description = """MagPy is developed for geomagnetic analysis.
Features include a support of many data formats, visualization,
advanced anaylsis routines, url/database accessability, DI analysis,
non-geomagnetic data support and more.
"""

        licence = """MagPy is free software; you can redistribute
it and/or modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; either version 2 of the License,
or any later version.

MagPy is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details. You should have
received a copy of the GNU General Public License along with MagPy;
if not, write to the Free Software Foundation, Inc., 59 Temple Place,
Suite 330, Boston, MA  02111-1307  USA"""


        info = wx.AboutDialogInfo()

        try:
            script_dir = os.path.dirname(__file__)
            iconimage = os.path.join(script_dir,'magpy128.xpm')
            # Alternative:
            #print ("Check", iconimage)
            #if sys.platform.startswith('linux'):
            info.SetIcon(wx.Icon(iconimage, wx.BITMAP_TYPE_XPM))
        except:
            pass
        info.SetName('MagPy')
        info.SetVersion(__version__)
        info.SetDescription(description)
        info.SetCopyright('(C) 2011 - 2017 Roman Leonhardt, Rachel Bailey, Mojca Miklavec, Jeremy Fee, Heather Schovanec')
        info.SetWebSite('http://www.conrad-observatory.at')
        info.SetLicence(licence)
        info.AddDeveloper('Roman Leonhardt, Rachel Bailey, Mojca Miklavec, Jeremey Fee, Heather Schovanec')
        info.AddDocWriter('Leonhardt,Bailey,Miklavec,Matzka')
        info.AddArtist('Leonhardt')
        info.AddTranslator('Bailey')

        wx.AboutBox(info)

    def OnHelpWriteFormats(self, event):

        WriteFormats = [ "{}: \t{}".format(key, PYMAG_SUPPORTED_FORMATS[key][1]) for key in PYMAG_SUPPORTED_FORMATS if 'w' in PYMAG_SUPPORTED_FORMATS[key][0]]

        message = "\n".join(WriteFormats)
        dlg = ScrolledMessageDialog(self, message, 'Write formats:')
        dlg.ShowModal()

    def OnHelpReadFormats(self, event):

        ReadFormats = [ "{}: \t{}".format(key, PYMAG_SUPPORTED_FORMATS[key][1]) for key in PYMAG_SUPPORTED_FORMATS if 'r' in PYMAG_SUPPORTED_FORMATS[key][0]]

        message = "\n".join(ReadFormats)
        dlg = ScrolledMessageDialog(self, message, 'Read formats:')
        dlg.ShowModal()

    """
    def OnExit(self, event):
        print ("Exiting with exit") ### TODO this method is not used at all
        if self.db:
            self.db.close()
        self.Destroy()  # Close the main window.
        sys.exit()
    """

    def OnOpenDir(self, event):
        stream = DataStream()
        success = False
        dialog = wx.DirDialog(None, "Choose a directory:",self.dirname,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            filelist = glob.glob(os.path.join(dialog.GetPath(),'*'))
            self.dirname = dialog.GetPath() # modify self.dirname
            files = sorted(filelist, key=os.path.getmtime)
            try:
                oldest = extractDateFromString(files[0])[0]
                old  = wx.DateTimeFromTimeT(time.mktime(oldest.timetuple()))
                newest = extractDateFromString(files[-1])[0]
                newest = newest+timedelta(days=1)
                new  = wx.DateTimeFromTimeT(time.mktime(newest.timetuple()))
                self.menu_p.str_page.pathTextCtrl.SetValue(dialog.GetPath())
                self.menu_p.str_page.fileTextCtrl.SetValue("*")
                success = True
            except:
                success = False
            #self.changeStatusbar("Loading data ...")
        dialog.Destroy()


        if success:
            stream = self.openStream(path=self.dirname,mintime=old, maxtime=new, extension='*')
            self.menu_p.rep_page.logMsg('{}: found {} data points'.format(self.dirname,len(stream.ndarray[0])))

            if self.InitialRead(stream):
                #self.ActivateControls(self.plotstream)
                self.OnInitialPlot(self.plotstream)
        else:
                    dlg = wx.MessageDialog(self, "Could not identify appropriate files in directory!\n"
                        "please check and/or try OpenFile\n",
                        "OpenDirectory", wx.OK|wx.ICON_INFORMATION)
                    dlg.ShowModal()
                    self.changeStatusbar("Loading from directory failed ... Ready")
                    dlg.Destroy()

    def OnOpenFile(self, event):
        #self.dirname = ''
        stream = DataStream()
        success = False
        stream.header = {}
        filelist = []
        dlg = wx.FileDialog(self, "Choose a file", self.dirname, "", "*.*", wx.MULTIPLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.changeStatusbar("Loading data ...")
            pathlist = dlg.GetPaths()
        dlg.Destroy()

        loadDlg = WaitDialog(None, "Loading...", "Loading data.\nPlease wait....")
        try:
                for path in pathlist:
                    elem = os.path.split(path)
                    self.dirname = elem[0]
                    filelist.append(elem[1])
                    self.changeStatusbar(path)
                    tmp = read(path)
                    self.changeStatusbar("... found {} rows".format(tmp.length()[0]))
                    stream.extend(tmp.container,tmp.header,tmp.ndarray)
                stream=stream.sorting()
                #stream = read(path_or_url=os.path.join(self.dirname, self.filename),tenHz=True,gpstime=True)
                #self.menu_p.str_page.lengthStreamTextCtrl.SetValue(str(len(stream)))
                self.filename = ' ,'.join(filelist)
                self.menu_p.str_page.fileTextCtrl.SetValue(self.filename)
                self.menu_p.str_page.pathTextCtrl.SetValue(self.dirname)
                self.menu_p.rep_page.logMsg('{}: found {} data points'.format(self.filename,len(stream.ndarray[0])))
                success = True
        except:
                sucess = False
        loadDlg.Destroy()

        # plot data
        if success:
            if self.InitialRead(stream):
                #self.ActivateControls(self.plotstream)
                self.OnInitialPlot(self.plotstream)
        else:
            dlg = wx.MessageDialog(self, "Could not identify file!\n"
                "please check and/or try OpenDirectory\n",
                "OpenFile", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Loading file failed ... Ready")
            dlg.Destroy()


    def OnOpenURL(self, event):
        stream = DataStream()
        success = False
        bookmarks = self.options.get('bookmarks',[])
        if bookmarks == []:
            bookmarks = ['ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc','ftp://user:passwd@www.zamg.ac.at/data/magnetism/wic/variation/WIC20160627pmin.min','http://www.conrad-observatory.at/zamg/index.php/downloads-en/category/13-definite2015?download=66:wic-2015-0000-pt1m-4','http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab','http://www.intermagnet.org/test/ws/?id=BOU']

        dlg = OpenWebAddressDialog(None, title='Open URL', favorites=bookmarks)
        if dlg.ShowModal() == wx.ID_OK:
            url = dlg.urlTextCtrl.GetValue()
            self.changeStatusbar("Loading data ... be patient")
            self.options['bookmarks'] = dlg.favorites
        dlg.Destroy()

        try:
                if not url.endswith('/'):
                    loadDlg = WaitDialog(None, "Loading...", "Loading data.\nPlease wait....")
                    self.menu_p.str_page.pathTextCtrl.SetValue(url)
                    self.menu_p.str_page.fileTextCtrl.SetValue(url.split('/')[-1])
                    try:
                        stream = read(path_or_url=url)
                        success = True
                    except:
                        success = False
                    loadDlg.Destroy()
                else:
                    self.menu_p.str_page.pathTextCtrl.SetValue(url)
                    mintime = pydate2wxdate(datetime(1777,4,30))  # Gauss
                    maxtime = pydate2wxdate(datetime(2233,3,22))  # Kirk
                    try:
                        stream = self.openStream(path=url, mintime=mintime, maxtime=maxtime, extension='*')
                        success = True
                    except:
                        success = False
        except:
                pass

        if success:
            self.menu_p.rep_page.logMsg('{}: found {} data points'.format(url,len(stream.ndarray[0])))
            if self.InitialRead(stream):
                #self.ActivateControls(self.plotstream)
                self.OnInitialPlot(self.plotstream)
            #self.options['bookmarks'] = dlg.favorites
            #print ("Here", dlg.favorites)
            #if not bookmarks == dlg.favorites:
            #print ("Favorites have changed ...  can be saved in init")
            saveini(self.options)
            inipara, check = loadini()
            self.initParameter(inipara)
            self.changeStatusbar("Ready")
        else:
            #self.options['bookmarks'] = dlg.favorites
            #print ("Here", dlg.favorites)
            #if not bookmarks == dlg.favorites:
            #print ("Favorites have changed ...  can be saved in init")
            saveini(self.options)
            inipara, check = loadini()
            self.initParameter(inipara)
            dlg = wx.MessageDialog(self, "Could not access URL!\n"
                "please check address or your internet connection\n",
                "OpenWebAddress", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Loading url failed ... Ready")
            dlg.Destroy()


    def OnOpenDB(self, event):
        # a) get all DATAINFO IDs and store them in a list
        # b) disable pathTextCtrl (DB: dbname)
        # c) Open dialog which lets the user select list and time window
        # d) update stream menu

        # Check whether DB still available
        self.checkDB('minimal')

        getdata = False
        stream = DataStream()
        if self.db:
            self.menu_p.rep_page.logMsg('- Accessing database ...')
            cursor = self.db.cursor()
            sql = "SELECT DataID, DataMinTime, DataMaxTime FROM DATAINFO"
            cursor.execute(sql)
            output = cursor.fetchall()
            #print ("Test", output)
            datainfoidlist = [elem[0] for elem in output]
            if len(datainfoidlist) < 1:
                dlg = wx.MessageDialog(self, "No data tables available!\n"
                            "please check your database\n",
                            "OpenDB", wx.OK|wx.ICON_INFORMATION)
                dlg.ShowModal()
                dlg.Destroy()
                return
            dlg = DatabaseContentDialog(None, title='MySQL Database: Get content',datalst=sort(datainfoidlist))
            if dlg.ShowModal() == wx.ID_OK:
                datainfoid = dlg.dataComboBox.GetValue()
                stream = DataStream()
                mintime = stream._testtime([elem[1] for elem in output if elem[0] == datainfoid][0])
                lastupload = stream._testtime([elem[2] for elem in output if elem[0] == datainfoid][0])
                maxtime = stream._testtime(datetime.strftime(lastupload,'%Y-%m-%d'))+timedelta(days=1)
                self.menu_p.str_page.pathTextCtrl.SetValue('MySQL Database')
                self.menu_p.str_page.fileTextCtrl.SetValue(datainfoid)
                getdata = True
            dlg.Destroy()
        else:
            dlg = wx.MessageDialog(self, "Could not access database!\n"
                        "please check your connection\n",
                        "OpenDB", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            return

        if getdata:
            path = [self.db,datainfoid]
            stream = self.openStream(path=path,mintime=pydate2wxdate(mintime), maxtime=pydate2wxdate(maxtime),extension='MySQL Database')
            self.menu_p.rep_page.logMsg('{}: found {} data points'.format(path[1],len(stream.ndarray[0])))
            if self.InitialRead(stream):
                #self.ActivateControls(self.plotstream)
                self.OnInitialPlot(self.plotstream)


    def OnExportData(self, event):

        self.changeStatusbar("Writing data ...")
        dlg = ExportDataDialog(None, title='Export Data',path=self.dirname,stream=self.plotstream,defaultformat='PYCDF')
        if dlg.ShowModal() == wx.ID_OK:
            filenamebegins = dlg.filenamebegins
            filenameends = dlg.filenameends
            dateformat = dlg.dateformat
            coverage = dlg.coverage
            mode = dlg.mode
            """
            datetyp = dlg.dateComboBox.GetValue()
            if datetyp == '2000-11-22':
                dateformat = '%Y-%m-%d'
            elif datetyp == '20001122':
                dateformat = '%Y%m%d'
            else:
                dateformat = '%b%d%y'
            """
            path = dlg.selectedTextCtrl.GetValue()
            fileformat = dlg.formatComboBox.GetValue()
            """
            coverage = dlg.coverageComboBox.GetValue()
            if coverage == 'hour':
                coverage = timedelta(hour=1)
            elif coverage == 'day':
                coverage = timedelta(days=1)
            elif coverage == 'year':
                coverage = timedelta(year=1)
            mode = dlg.modeComboBox.GetValue()
            """
            #print "Stream: ", len(self.stream), len(self.plotstream)
            #print "Data: ", self.stream[0].time, self.stream[-1].time, self.plotstream[0].time, self.plotstream[-1].time
            #print ("Main : ", filenamebegins, filenameends, dateformat, fileformat, coverage, mode)

            checkPath = os.path.join(path, dlg.filenameTextCtrl.GetValue())
            export = False
            if os.path.exists(checkPath):
                msg = wx.MessageDialog(self, "The current export file will overwrite an existing file!\n"
                    "Choose 'Ok' to apply the overwrite or 'Cancel' to stop exporting.\n",
                    "VerifyOverwrite", wx.OK|wx.CANCEL|wx.ICON_QUESTION)
                if msg.ShowModal() == wx.ID_OK:
                    export = True
                msg.Destroy()
            else:
                export = True

            if export == True:
                try:
                    if fileformat == 'BLV':
                        print ("Writing BLV data")  # add function here
                        print ("Function", self.plotopt['function'])
                        year = num2date(np.nanmean(self.plotstream.ndarray[0])).year
                        # use functionlist as kwarg in write method
                        self.plotstream.write(path,
                                    filenamebegins=filenamebegins,
                                    filenameends=filenameends,
                                    dateformat=dateformat,
                                    mode=mode,
                                    year=year,
                                    coverage=coverage,
                                    format_type=fileformat)
                        mode = 'replace'
                    elif fileformat == 'PYCDF':
                        # Open Yes/No message box and to select whether flags should be stored or not
                        print ("Writing PYCDF data")  # add function here
                        addflags = False
                        # Test whether flags are present at all
                        dlg = wx.MessageDialog(self, 'Compress? (selecting "NO" improves compatibility between different operating systems', 'Compression', wx.YES_NO | wx.ICON_QUESTION)
                        compression = 0
                        if dlg.ShowModal() == wx.ID_YES:
                            compression = 5
                        dlg.Destroy()
                        self.plotstream.write(path,
                                    filenamebegins=filenamebegins,
                                    filenameends=filenameends,
                                    dateformat=dateformat,
                                    mode=mode,
                                    compression = compression,
                                    coverage=coverage,
                                    format_type=fileformat)
                    elif fileformat == 'IMAGCDF':
                        # Open Yes/No message box and to select whether flags should be stored or not
                        print ("Writing IMAGCDF data")  # add function here
                        addflags = False
                        # Test whether flags are present at all
                        dlg = wx.MessageDialog(self, 'Save flags?', 'Flags', wx.YES_NO | wx.ICON_QUESTION)
                        if dlg.ShowModal() == wx.ID_YES:
                            addflags = True
                        dlg.Destroy()
                        self.plotstream.write(path,
                                    filenamebegins=filenamebegins,
                                    filenameends=filenameends,
                                    dateformat=dateformat,
                                    mode=mode,
                                    addflags = addflags,
                                    coverage=coverage,
                                    format_type=fileformat)
                    else:
                        self.plotstream.write(path,
                                    filenamebegins=filenamebegins,
                                    filenameends=filenameends,
                                    dateformat=dateformat,
                                    mode=mode,
                                    coverage=coverage,
                                    format_type=fileformat)

                    self.menu_p.rep_page.logMsg("Data written to path: {}".format(path))
                    self.changeStatusbar("Data written ... Ready")
                except:
                    self.menu_p.rep_page.logMsg("Writing failed - Permission?")
        else:
            self.changeStatusbar("Ready")
        dlg.Destroy()

    def _db_connect(self, host, user, passwd, dbname):
        try:
            self.db.close()
        except:
            pass
        # check whether host can be pinged (faster)
        if PLATFORM.startswith('linux'):
            response = os.system("ping -c 1 -w2 {} > /dev/null 2>&1".format(host))
        else:
            response = 0

        if response == 0:
            try:
                self.db = mysql.connect (host=host,user=user,passwd=passwd,db=dbname)
            except:
                self.db = False
        else:
            self.db = False
        if self.db:
            self.DBOpen.Enable(True)
            self.menu_p.rep_page.logMsg('- MySQL Database {} on {} connected.'.format(dbname,host))
            self.changeStatusbar("Database %s successfully connected" % (dbname))
            # Set variable to True
            self.databaseconnected = True
            # enable MARCOS button
            self.menu_p.com_page.getMARCOSButton.Enable()
        else:
            self.menu_p.rep_page.logMsg('- MySQL Database access failed.')
            self.changeStatusbar("Database connection failed")
            # disable MARCOS button
            self.menu_p.com_page.getMARCOSButton.Disable()

    def OnDBConnect(self, event):
        """
        Provide access for local network:
        Open your /etc/mysql/my.cnf file in your editor.
        scroll down to the entry:
        bind-address = 127.0.0.1
        and you can either hash that so it binds to all ip addresses assigned
        #bind-address = 127.0.0.1
        or you can specify an ipaddress to bind to. If your server is using dhcp then just hash it out.
        Then you'll need to create a user that is allowed to connect to your database of choice from the host/ip your connecting from.
        Login to your mysql console:
        milkchunk@milkchunk-desktop:~$ mysql -uroot -p
        GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY 'some_pass' WITH GRANT OPTION;
        You change out the 'user' to whatever user your wanting to use and the '%' is a hostname wildcard. Meaning that you can connect from any hostname with it. You can change it to either specify a hostname or just use the wildcard.
        Then issue the following:
        FLUSH PRIVILEGES;
        Be sure to restart your mysql (because of the config file editing):
        /etc/init.d/mysql restart
        """
        dlg = DatabaseConnectDialog(None, title='MySQL Database: Connect to')
        dlg.hostTextCtrl.SetValue(self.options.get('host',''))
        dlg.userTextCtrl.SetValue(self.options.get('user',''))
        dlg.passwdTextCtrl.SetValue(self.options.get('passwd',''))
        if self.db == None or self.db == 'None' or not self.db:
            dlg.dbTextCtrl.SetValue('None')
        else:
            dlg.dbTextCtrl.SetValue(self.options.get('dbname',''))
        if dlg.ShowModal() == wx.ID_OK:
            self.options['host'] = dlg.hostTextCtrl.GetValue()
            self.options['user'] = dlg.userTextCtrl.GetValue()
            self.options['passwd'] = dlg.passwdTextCtrl.GetValue()
            self.options['dbname'] = dlg.dbTextCtrl.GetValue()
            self._db_connect(self.options.get('host',''), self.options.get('user',''), self.options.get('passwd',''), self.options.get('dbname',''))
            """
            self.db = mysql.connect (host=host,user=user,passwd=passwd,db=mydb)
            if self.db:
                self.DBOpen.Enable(True)
                self.menu_p.rep_page.logMsg('- MySQL Database selected.')
                self.changeStatusbar("Database %s successfully connected" % (self.db))
            else:
                self.menu_p.rep_page.logMsg('- MySQL Database access failed.')
                self.changeStatusbar("Database connection failed")
            """
        dlg.Destroy()


    def OnDBInit(self, event):
        """
        Provide access for local network:
        Open your /etc/mysql/my.cnf file in your editor.
        scroll down to the entry:
        bind-address = 127.0.0.1
        and you can either hash that so it binds to all ip addresses assigned
        #bind-address = 127.0.0.1
        or you can specify an ipaddress to bind to. If your server is using dhcp then just hash it out.
        Then you'll need to create a user that is allowed to connect to your database of choice from the host/ip your connecting from.
        Login to your mysql console:
        milkchunk@milkchunk-desktop:~$ mysql -uroot -p
        GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY 'some_pass' WITH GRANT OPTION;
        You change out the 'user' to whatever user your wanting to use and the '%' is a hostname wildcard. Meaning that you can connect from any hostname with it. You can change it to either specify a hostname or just use the wildcard.
        Then issue the following:
        FLUSH PRIVILEGES;
        Be sure to restart your mysql (because of the config file editing):
        /etc/init.d/mysql restart
        """
        # Open a message box to confirm that you really want to do that and to provide info on prerequisits
        dlg = wx.MessageDialog(self, "Your are going to intialize a new database\n"
                        "Please make sure that the following points are fullfilled:\n"
                        "1) MySQL is installed\n"
                        "2) An empty database has been created:\n"
                        "   $ CREATE DATABASE mydb;\n"
                        "3) A new user has been added and access has been granted:\n"
                        "   $ GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY 'some_pass';\n",
                        "Init database", wx.OK|wx.CANCEL)
        if dlg.ShowModal() == wx.ID_OK:
            dlg.Destroy()
            # open dialog to select empty db or create new db if mysql is existing
            dlg = DatabaseConnectDialog(None, title='MySQL Database: Initialize...')
            dlg.hostTextCtrl.SetValue(self.options.get('host',''))
            dlg.userTextCtrl.SetValue(self.options.get('user',''))
            dlg.passwdTextCtrl.SetValue(self.options.get('passwd',''))
            if self.db == None or self.db == 'None' or not self.db:
                dlg.dbTextCtrl.SetValue('None')
            else:
                dlg.dbTextCtrl.SetValue(self.options.get('dbname',''))
            if dlg.ShowModal() == wx.ID_OK:
                self.options['host'] = dlg.hostTextCtrl.GetValue()
                self.options['user'] = dlg.userTextCtrl.GetValue()
                self.options['passwd'] = dlg.passwdTextCtrl.GetValue()
                self.options['dbname'] = dlg.dbTextCtrl.GetValue()
                self._db_connect(self.options.get('host',''), self.options.get('user',''), self.options.get('passwd',''), self.options.get('dbname',''))
                dbinit(self.db)
                self.changeStatusbar("New database initiated - Ready")
            dlg.Destroy()
        else:
            dlg.Destroy()


    def OnFileQuit(self, event):
        if self.db:
            self.db.close()
        self.Destroy()  # Close the main window.
        sys.exit()


    def OnSave(self, event):
        textfile = open(os.path.join(self.dirname, self.filename), 'w')
        textfile.write(self.control.GetValue())
        textfile.close()

    def OnSaveAs(self, event):
        if self.askUserForFilename(defaultFile=self.filename, style=wx.SAVE,
                                   **self.defaultFileDialogOptions()):
            self.OnSave(event)


    def OnOptionsInit(self, event):
        """
        DEFINITION
            Change options
        """
        dlg = OptionsInitDialog(None, title='Options: Parameter specifications',options=self.options)
        if dlg.ShowModal() == wx.ID_OK:
            self.options['host'] = dlg.hostTextCtrl.GetValue()
            self.options['user'] = dlg.userTextCtrl.GetValue()
            self.options['passwd'] = dlg.passwdTextCtrl.GetValue()
            #print (self.options['passwd'])
            db = dlg.dbTextCtrl.GetValue()
            if db == '':
                self.options['dbname'] = 'None'
            else:
                self.options['dbname'] = db
            self.options['dirname']=dlg.dirnameTextCtrl.GetValue()

            self.options['stationid']=dlg.stationidTextCtrl.GetValue()

            self.options['fitfunction']=dlg.fitfunctionComboBox.GetValue()
            self.options['fitknotstep']=dlg.fitknotstepTextCtrl.GetValue()
            self.options['fitdegree']=dlg.fitdegreeTextCtrl.GetValue()
            # experimental methods
            self.options['experimental']=dlg.experimentalCheckBox.GetValue()
            # mqtt communication
            self.options['martasscantime']=dlg.martasscantimeTextCtrl.GetValue()
            saveini(self.options)

            inipara, check = loadini()
            self.initParameter(inipara)

        dlg.Destroy()

    def OnOptionsDI(self, event):
        """
        DEFINITION
            Change options
        """

        dlg = OptionsDIDialog(None, title='Options: DI Analysis parameters', options=self.options)

        if dlg.ShowModal() == wx.ID_OK:
            self.options['diexpD']=dlg.diexpDTextCtrl.GetValue()
            self.options['diexpI']=dlg.diexpITextCtrl.GetValue()
            self.options['dialpha']=dlg.dialphaTextCtrl.GetValue()
            self.options['dideltaF']=dlg.dideltaFTextCtrl.GetValue()
            self.options['ditype']=dlg.ditypeComboBox.GetValue()
            self.options['divariopath']=dlg.divariopathTextCtrl.GetValue()
            self.options['discalarpath']=dlg.discalarpathTextCtrl.GetValue()
            self.options['diid']=dlg.diidTextCtrl.GetValue()
            self.options['diazimuth']=dlg.diazimuthTextCtrl.GetValue()
            self.options['dipier']=dlg.dipierTextCtrl.GetValue()
            self.options['didbadd']=dlg.didbaddTextCtrl.GetValue()
            # TODO to be added
            self.options['diannualmean']=dlg.diannualmeanTextCtrl.GetValue()
            self.options['dibeta']=dlg.dibetaTextCtrl.GetValue()
            self.options['dideltaD']=dlg.dideltaDTextCtrl.GetValue()
            self.options['dideltaI']=dlg.dideltaITextCtrl.GetValue()

            self.dipathlist = dlg.dipathlistTextCtrl.GetValue().split(',')
            dipathlist = dlg.dipathlistTextCtrl.GetValue().split(',')
            dipath = dipathlist[0]
            if os.path.isfile(dipath):
                dipath = os.path.split(dipath)[0]
            self.options['dipathlist'] = [dipath]
            order=dlg.sheetorderTextCtrl.GetValue()
            double=dlg.sheetdoubleCheckBox.GetValue()
            scalevalue=dlg.sheetscaleCheckBox.GetValue()
            self.options['double'] = 'True'
            self.options['scalevalue'] = 'True'
            if not double:
                self.options['double'] = 'False'
            if not scalevalue:
                self.options['scalevalue'] = 'False'
            self.options['order'] = order

            saveini(self.options)
            inipara, check = loadini()
            self.initParameter(inipara)

        dlg.Destroy()


    """
    def OnOptionsObs(self, event):
        dlg = OptionsObsDialog(None, title='Options: Observatory specifications')
        dlg.ShowModal()
        dlg.Destroy()

        #dlg = wx.MessageDialog(self, "Coming soon:\n"
        #                "Modify observatory specifications\n",
        #                "PyMag by RL", wx.OK|wx.ICON_INFORMATION)
        #dlg.ShowModal()
        #dlg.Destroy()
    """

    def onOpenAuxButton(self, event):
        if self.askUserForFilename(style=wx.OPEN,
                                   **self.defaultFileDialogOptions()):
            #dat = read_general(os.path.join(self.dirname, self.filename), 0)
            textfile = open(os.path.join(self.dirname, self.filename), 'r')
            self.menu_p.gen_page.AuxDataTextCtrl.SetValue(textfile.read())
            textfile.close()
            #print dat

    def changeStatusbar(self,msg):
        self.SetStatusText(msg)

    def UpdateCursorStatus(self, event):
        """Motion event for displaying values under cursor."""
        if not event.inaxes or not self.menu_p.str_page.trimStreamButton.IsEnabled():
            self.changeStatusbar("Ready")
            return
        pickX, pickY = event.xdata, event.ydata
        xdata = self.plot_p.t
        idx = (np.abs(xdata - pickX)).argmin()
        time = self.plotstream.ndarray[KEYLIST.index('time')][idx]
        possible_val = []
        possible_key = []
        try:
            time = datetime.strftime(num2date(time),"%Y-%m-%d %H:%M:%S %Z")
        except:
            time = num2date(time)
        for elem in self.shownkeylist:
            ul = np.nanmax(self.plotstream.ndarray[KEYLIST.index(elem)])
            ll = np.nanmin(self.plotstream.ndarray[KEYLIST.index(elem)])
            if ll < pickY < ul:
                possible_key += elem
                possible_val += [self.plotstream.ndarray[KEYLIST.index(elem)][idx]]
        try:
            idy = (np.abs(possible_val - pickY)).argmin()
            key = possible_key[idy]
            val = possible_val[idy]
            colname = self.plotstream.header.get('col-'+key, '')
            if not colname == '':
                key = colname
            self.changeStatusbar("time: " + str(time) + "  |  " + key + " data value: " + str(val))
        except:
            pass

    def OnCheckOpenLog(self, event):
        """
        Definition:
            open and display magpy log file
        """

        logfile=os.path.join(tempfile.gettempdir(),'magpy.log')
        reportlst = []
        if os.path.exists(logfile):
            with open(logfile) as fobj:
                for line in fobj:
                    reportlst.append(line)
        else:
            # TODO create message box
            pass
        report = ''.join(reportlst)

        dlg = CheckOpenLogDialog(None, title='MagPy Logging data', report = report)
        if dlg.ShowModal() == wx.ID_OK:
            pass
        else:
            dlg.Destroy()
            return


    def OnCheckDefinitiveData(self, event):
        """
        Definition:
            Tool set for data checking. Organized in a step wise application:
            Step 1: directories and existance of files (obligatory)
            Step 2: file access and basic header information
            Step 3: data content and consistency of primary source
            Step 4: checking secondary source and consistency with primary
            Step 5: basevalues and adopted baseline variation
            Step 6: yearly means, consistency of meta information
            Step 7: acitivity indicies
        """
        # 1. open a dialog with two input directories: 1) for IAF minute data and 2) (optional) for IamgCDF sec data
        # 2. radio field with two selections (quick check, full check)
        minutepath = ''
        secondpath = ''
        seconddata = 'None'
        iafpath = ''
        blvpath = ''
        dkapath = ''
        yearmeanpath = ''
        checkchoice = 'quick'
        reportmsg = ''
        errormsg = ''
        warningmsg = ''
        year = 1777

        def saveReport(label, report):
            if label == 'Save':
                savepath = ''
                saveFileDialog = wx.FileDialog(self, "Save As", "", "",
                                       "Report (*.txt)|*.txt",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
                if saveFileDialog.ShowModal() == wx.ID_OK:
                    savepath = saveFileDialog.GetPath()
                saveFileDialog.Destroy()
                if not savepath == '':
                    with open(savepath, "wb") as myfile:
                        myfile.write(report)
                    return True
                    #self.Close(True)
                return False
            else:
                return False

        def createReport(reportmsg, warningmsg, errormsg):
            """
             Definition:
                  create report for dialog
            """
            warninghead = "\n++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
            warninghead += "     Warnings: You need to solve these issues\n"
            warninghead += "++++++++++++++++++++++++++++++++++++++++++++++++++++\n"

            errorhead = "\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
            errorhead += "     Critical errors: please check\n"
            errorhead += "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"

            if not warningmsg == '':
                warnrep = warninghead + warningmsg
            else:
                warnrep = " - no warnings\n"
            if not errormsg == '':
                errorrep = errorhead + errormsg
            else:
                errorrep = " - no errors\n"
            report = reportmsg+'\n'+warnrep+'\n'+errorrep
            return report


        def readSecData(secondpath,seconddata,rmonth,year,dataid=None):
            """
            Definition:
                  reading one second data from ImagCDF or IAGA02 formats (or MagPy)
            """
            starttime=str(year)+'-'+str(rmonth).zfill(2)+'-01'
            endmonth = rmonth+1
            endyear = year
            success = 1
            if endmonth == 13:
                endyear = year+1
                endmonth = 1
            endtime=str(endyear)+'-'+str(endmonth).zfill(2)+'-01'
            if seconddata == 'cdf':
                cdfname = '*'+str(rmonth).zfill(2)+'_000000_PT1S_4.cdf'
                loadpath = os.path.join(secondpath,cdfname)
                try:
                    secdata = read(loadpath,debug=True)
                except:
                    secdata = DataStream()
                    success = 6
                if not secdata.length()[0] > 0:
                    cdfname = '*.cdf'
                    loadpath = os.path.join(secondpath,cdfname)
                    # File name issue !!
                    try:
                        secdata = read(loadpath,debug=True, starttime=starttime, endtime=endtime)
                        success = 3
                    except:
                        secdata = DataStream()
                        success = 6

            elif seconddata == 'pycdf':
                if dataid:
                    cdfname = dataid+'_vario_sec_'+str(year)+str(rmonth).zfill(2)+'.cdf'
                    loadpath = os.path.join(secondpath,cdfname)
                    secdata = read(loadpath,debug=True)
                else:
                    print ("please provide a sensorid")
            elif seconddata == 'iaga':
                loadpath = os.path.join(secondpath,'*.sec')
                try:
                    secdata = read(loadpath,starttime=starttime,endtime=endtime,debug=True)
                except:
                    secdata = DataStream()
                    success = 6

            return secdata, success

        def readMinData(checkchoice,minutedata,minutepath,month,rmonth,year=1900,resolution=None):
            """
            Definition:
                  reading one minute data from IAF formats (and others)
            """
            starttime=str(year)+'-'+str(rmonth).zfill(2)+'-01'
            endmonth = rmonth+1
            endyear = year
            success = 1
            if endmonth == 13:
                endyear = year+1
                endmonth = 1
            endtime=str(endyear)+'-'+str(endmonth).zfill(2)+'-01'

            if minutedata == 'iaf':
                if checkchoice == 'quick':
                    iafpath1 = minutepath.replace('*.','*'+month.lower()+'.')
                    iafpath2 = minutepath.replace('*.','*'+month.upper()+'.')
                    if len(glob.glob(iafpath1)) > 0:
                        path = iafpath1
                    else:
                        path = iafpath2
                else:
                    path = minutepath
                try:
                    if resolution:
                        mindata = read(path, resolution=resolution, debug=True)
                    else:
                        mindata = read(path, debug=True)
                except:
                    mindata = DataStream()
                    success = 6

            return mindata, success



        self.changeStatusbar("Checking data ... ")

        ## get random month for quick check
        from random import randint
        import locale  # to get english month descriptions
        old_loc = locale.getlocale(locale.LC_TIME)
        try:
            locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
        except:
            pass

        rmonth = randint(1,11) ## Not 12 as this would unnecessarily complicate start and endtime selection
        month = datetime(1900, rmonth, 1).strftime('%b')
        if checkchoice == 'quick':
            reportmsg += "Test type: {} . Random check month: {}\n".format(checkchoice, datetime(1900, rmonth, 1).strftime('%B'))
        else:
            reportmsg += "Test type: {} . Header and readability check for month: {}\n".format(checkchoice, datetime(1900, rmonth, 1).strftime('%B'))

        succlst = ['0','0','0','0','0','0','0']
        onlysec = False
        success = 6  # integer from 1 to 6 - 1 everything perfect, 6 bad
        laststep = 7 # used to switch from coninue to save
        dlg = CheckDefinitiveDataDialog(None, title='Checking defintive data')
        if dlg.ShowModal() == wx.ID_OK:
            checkchoice = dlg.checkchoice
            minutepath = dlg.minuteTextCtrl.GetValue()
            secondpath = dlg.secondTextCtrl.GetValue()
            checkparameter = dlg.checkparameter
            laststep = dlg.laststep
            success = 1
        else:
            dlg.Destroy()
            return
        if minutepath =='' and secondpath =='':
            return
        if success == 1:
            succlst[0] = 1
            reportmsg += "#######################################\n"
            reportmsg += "Step 1:\n"
            reportmsg += "#######################################\n"
            reportmsg += "Def.: checking directories and for presence of files\n\n"
            # check whether paths are existing and appropriate data is contained - if failing set success to 6
            # Provide a report dialog with summaries of each test and a continue button
            # You have selected the following options:

            #if checkchoice == 'quick':
            #    print("Randomly selecting month for full data validity checks: Using {}").format(datetime(1900, rmonth, 1).strftime('%B'))
            # Step 1: # test directory structure, and presence of files
            if not minutepath == '' and os.path.isdir(minutepath):
                reportmsg += "Step 1: minute data\n"
                reportmsg += "-----------------------\n"
                reportmsg += "Step 1: Using selected IAF path: {}\n".format(minutepath)
                #import glob
                iafcnt = len(glob.glob(os.path.join(minutepath,"*.BIN")))
                if iafcnt > 0 and iafpath == '':
                    iafpath = os.path.join(minutepath,"*.BIN")
                else:
                    # windows is not case-sensitive ....
                    iafcnt += len(glob.glob(os.path.join(minutepath,"*.bin")))
                    if iafcnt > 0 and iafpath == '':
                        iafpath = os.path.join(minutepath,"*.bin")
                if not iafcnt == 12:
                    succlst[0] = 6
                    errormsg += "Step 1: !!! IAF error: didn't find 12 monthly files\n"
                else:
                    reportmsg += "Step 1: +++ Correct amount of binary files\n"
                blvcnt = len(glob.glob(os.path.join(minutepath,"*.blv")))
                if blvcnt > 0 and blvpath == '':
                    blvpath = os.path.join(minutepath,"*.blv")
                else:
                    blvcnt += len(glob.glob(os.path.join(minutepath,"*.BLV")))
                    if blvcnt > 0 and blvpath == '':
                        blvpath = os.path.join(minutepath,"*.BLV")
                readmecnt = len(glob.glob(os.path.join(minutepath,"README*")))
                readmecnt += len(glob.glob(os.path.join(minutepath,"readme*")))
                readmecnt += len(glob.glob(os.path.join(minutepath,"Readme*")))
                dkacnt = len(glob.glob(os.path.join(minutepath,"*.dka")))
                if dkacnt > 0 and dkapath == '':
                    dkapath = os.path.join(minutepath,"*.dka")
                else:
                    dkacnt += len(glob.glob(os.path.join(minutepath,"*.DKA")))
                    if dkacnt > 0 and dkapath == '':
                        dkapath = os.path.join(minutepath,"*.DKA")
                yearmeancnt = len(glob.glob(os.path.join(minutepath,"YEARMEAN*")))
                if yearmeancnt > 0 and yearmeanpath == '':
                    yearmeanpath = os.path.join(minutepath,"YEARMEAN*")
                else:
                    yearmeancnt += len(glob.glob(os.path.join(minutepath,"yearmean*")))
                    if yearmeancnt > 0 and yearmeanpath == '':
                        yearmeanpath = os.path.join(minutepath,"yearmean*")
                pngcnt = len(glob.glob(os.path.join(minutepath,"*.png")))
                if pngcnt < 1:
                    pngcnt += len(glob.glob(os.path.join(minutepath,"*.PNG")))
                if not blvcnt == 1:
                    warningmsg += "Step 1: (warning)  No BLV data present\n"
                    blvdata = False
                    succlst[0] = 5
                else:
                    blvdata = True
                if not dkacnt == 1:
                    reportmsg += "Step 1: No DKA data present (file is not obligatory, will be extracted from IAF)\n"
                    dkadata = False
                    #succlst[0] = 5   # File is not obligatory - no change to succlst
                else:
                    dkadata = True
                if not yearmeancnt >= 1:
                    warningmsg += "Step 1: (warning)  No YEARMEAN data present\n"
                    yearmeandata = False
                    succlst[0] = 5
                else:
                    yearmeandata = True
                    try:
                        yldat = read(yearmeanpath)
                        year = num2date(yldat.ndarray[0][-1]).year
                    except:
                        pass
                if not readmecnt >= 1:
                    warningmsg += "Step 1: (warning)  No README present\n"
                    readmedata = False
                    succlst[0] = 5
                else:
                    readmedata = True
                if not pngcnt == 1:
                    reportmsg += "Step 1: No PNG data present (file is not obligatory)\n"
                    pngdata = False
                    #succlst[0] = 3   # File is not obligatory - no change to succlst
                else:
                    pngdata = True
                if blvdata and dkadata and readmedata and yearmeandata and pngdata:
                    reportmsg += "Step 1: +++ Auxiliary files are present\n"
            else:
                if not minutepath == '':
                    errormsg = "Step 1: Can not access directory {}\n".format(minutepath)
                    succlst[0] = 6

            reportmsg += "\nStep 1: one second data\n"
            reportmsg += "-----------------------\n"
            if secondpath == '':
                reportmsg += "Step 1: Skipping one second data checks. No path selected.\n"
            elif os.path.isdir(secondpath):
                reportmsg += "Step 1: Using selected path for one second data: {}\n".format(secondpath)
                #import glob
                cdfcnt = len(glob.glob(os.path.join(secondpath,"*.cdf")))
                cdfcnt += len(glob.glob(os.path.join(secondpath,"*.CDF")))
                iagacnt = len(glob.glob(os.path.join(secondpath,"*.sec")))
                iagacnt += len(glob.glob(os.path.join(secondpath,"*.SEC")))
                if cdfcnt >= 12:
                    seconddata = 'cdf'
                elif iagacnt >= 365:
                    seconddata = 'iaga'
                if not seconddata in ['cdf','iaga']:
                    warningmsg += "Step 1: !!! one second error: didn't find either ImagCDF (*.cdf) or IAGA02 (*.sec) files\n"
                else:
                    reportmsg += "Step 1: +++ Found appropriate amount of one second data files\n"
                    #print ("Here", minutepath, succlst)
                    if minutepath == '':
                        onlysec = True
                        #print ("Checkparameter")
                        checkparameter['step3'] = False
                        checkparameter['step5'] = False
                        checkparameter['step7'] = False
                    succlst[0]=1
            else:
                warningmsg += "Step 1: Could not access provided directory of one second data {}\n".format(secondpath)

            if succlst[0] <= 2:
                if onlysec:
                    reportmsg += "\n----> Step 1: successfully passed - found only secondary data however\n"
                else:
                    reportmsg += "\n----> Step 1: successfully passed\n"
            elif succlst[0] <= 4:
                reportmsg += "\n----> Step 1: passed with some issues to be reviewed\n"
        self.changeStatusbar("Step 1: directories and files ... Done")

        report = createReport(reportmsg, warningmsg, errormsg)
        opendlg = True
        if opendlg:
            dlg = CheckDataReportDialog(None, title='Data check step 1 - report', report=report, rating=succlst[0], step=list(map(str,succlst)), laststep=laststep)
            dlg.ShowModal()
            if dlg.moveon:
                saveReport(dlg.contlabel, dlg.report)
            else:
                dlg.Destroy()
                locale.setlocale(locale.LC_TIME, old_loc)
                self.changeStatusbar("Ready")
                return

        if succlst[0] < 6:
            if checkparameter['step2']:

                logger = setup_logger(__name__)
                logger.info("-------------------------------")
                logger.info("Starting Step 2 of Data checker")
                logger.info("-------------------------------")

                succlst[1] = 1
                reportmsg += "\n"
                reportmsg += "#######################################\n"
                reportmsg += "Step 2:\n"
                reportmsg += "#######################################\n"
                reportmsg += "Def.: checking readability of main data files and header information\n\n"
                reportmsg += "Step 2:  IAF data:\n"
                reportmsg += "-----------------------\n"
                self.changeStatusbar("Step 2: Reading minute data ... ")
                mindata, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth)

                #print(log_stream.getvalue())

                logger = setup_logger(__name__)

                if fail == 6 and not onlysec:
                    errormsg += "Step 2: Reading of IAF data failed - check file format\n"
                    succlst[1] = 6
                self.changeStatusbar("Step 2: Reading minute data ... Done")

                if mindata.length()[0] > 1:
                    reportmsg += "Step 2:  +++ Minute data readable - checked example for {}\n".format(month)
                else:
                    if not onlysec:
                        print (" !!! Could not read data - checked example for {}".format(month))
                        errormsg += 'Step 2: Reading of IAF data failed - no data extracted for {} - check file format\n'.format(month)
                        succlst[1] = 6

                reportmsg += "\nStep 2:  IAF data  - checking meta information:\n"
                reportmsg += "-----------------------\n"
                headfailure = False
                #for head in IMAGCDFMETA: # IMAGMETAMETA
                for head in IAFBINMETA: # IMAGMETAMETA ##### need to select only meta information expected in iaf file, not the information needed to create all IAF output files
                    value = mindata.header.get(head,'')
                    if value == '' and not onlysec:
                        warningmsg += "Step 2: (warning) !!! IAF: no meta information for {}\n".format(head)
                        headfailure = True
                if not onlysec:
                    if not headfailure:
                        reportmsg += "Step 2: +++ IAF: each required header is present. IMPORTANT: did not check content !!\n"
                    else:
                        reportmsg += "Step 2: !!! IAF: got meta information warnings\n"
                        succlst[1] = 5

                obscode = mindata.header.get('StationID')
                # get year
                try:
                    year = num2date(mindata.ndarray[0][-1]).year
                except:
                    year = 1777

                if not seconddata == 'None':
                    reportmsg += "\nStep 2:  Secondary data:\n"
                    reportmsg += "-----------------------\n"
                    proDlg = WaitDialog(None, "Processing...", "Checking file structure of one second data.\nPlease wait....")
                    self.changeStatusbar("Step 2: Reading one second data ({})- please be patient ... this may need a few minutes".format(seconddata))
                    secdata, fail = readSecData(secondpath,seconddata,rmonth,year)
                    proDlg.Destroy()
                    if fail == 6:
                        errormsg += "Step 2: Reading of one second data failed - check file format and/or file name convention and/or dates\n"
                        succlst[1] = 5
                    elif fail == 3:
                        errormsg += "Step 2: Reading of one second data - file names do not follow the ImagCDF convention\n"
                        succlst[1] = 3
                    self.changeStatusbar("Step 2: Reading one second data ... Done ")
                    if secdata.length()[0] > 1:
                        reportmsg += "Step 2: +++ Second data readable - checked example for {}\n".format(month)
                    else:
                        print (" !!! Could not read data - checked example for {}".format(month))
                        warningmsg += 'Step 2: !!! Reading of one second data failed - no data extracted for {} - check file format\n'.format(month)
                        warningmsg += 'Step 2: Skipping analysis of one second data\n'.format(month)
                        seconddata = 'None'

                if not seconddata == 'None':
                    headfailure = False
                    reportmsg += "\nStep 2:  Secondary data  - checking meta information:\n"
                    reportmsg += "-----------------------\n"
                    sr = secdata.samplingrate()
                    reportmsg += "Step 2: data period corresponds to {} second(s)\n".format(sr)
                    reportmsg += "Step 2: SamplingFilter: {} \n".format(secdata.header.get('DataSamplingFilter',''))
                    if seconddata == 'cdf':
                        META = IMAGCDFMETA
                    elif seconddata == 'iaga':
                        META = IAGAMETA
                    for head in META:
                        value = secdata.header.get(head,'')
                        if value == '':
                            warningmsg += "Step 2: (warning) !!! Second data: no meta information for {}\n".format(head)
                            headfailure = True
                    if not headfailure:
                        reportmsg += "Step 2: +++ Second data: each required header is present. IMPORTANT: did not check content !!\n"
                    else:
                        reportmsg += "Step 2: !!! Second data: meta information warnings found\n"
                        succlst[1] = 5


                if succlst[1] <= 2:
                    reportmsg += "\n----> Step 2: successfully passed\n"
                elif succlst[1] <= 4:
                    reportmsg += "\n----> Step 2: passed with some issues to be reviewed\n"
                self.changeStatusbar("Step 2: readability check ... Done")

                logger.info("-------------------------------")
                logger.info("Step 2 of Data checker finished")
                logger.info("-------------------------------")

                report = createReport(reportmsg, warningmsg, errormsg)
                opendlg = True
                if opendlg:
                    dlg = CheckDataReportDialog(None, title='Data check step 2 - report', report=report, rating=max(list(map(int,succlst))), step=list(map(str,succlst)), laststep=laststep)
                    dlg.ShowModal()
                    if dlg.moveon:
                        saveReport(dlg.contlabel, dlg.report)
                    else:
                        dlg.Destroy()
                        locale.setlocale(locale.LC_TIME, old_loc)
                        self.changeStatusbar("Ready")
                        return

            if checkparameter['step3']:
                succlst[2] = 1
                reportmsg += "\n"
                reportmsg += "#######################################\n"
                reportmsg += "Step 3:\n"
                reportmsg += "#######################################\n"
                reportmsg += "Def.: checking data content and consistency\n\n"
                reportmsg += "Step 3: consistency of minute data:\n"
                reportmsg += "-----------------------\n"
                self.changeStatusbar("Step 3: Checking data consistency ... ")
                if checkchoice == 'quick':
                    starttime=str(year)+'-'+str(rmonth).zfill(2)+'-01'
                    endtime=str(year)+'-'+str(rmonth+1).zfill(2)+'-01'
                    days = int(date2num(datetime.strptime(endtime,'%Y-%m-%d')) - date2num(datetime.strptime(starttime,'%Y-%m-%d')))
                    #print ("Lenght", mindata.length()[0])
                    if not mindata.length()[0] > 0:
                         mindata, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth)
                else:
                    starttime=str(year)+'-01-01'
                    endtime=str(year+1)+'-01-01'
                    days = int(date2num(datetime.strptime(str(year+1)+'-01-01','%Y-%m-%d')) - date2num(datetime.strptime(str(year)+'-01-01','%Y-%m-%d')))
                    mindata, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth)

                if not days*24*60 - mindata.length()[0] == 0:
                    reportmsg += "Step 3: Checking coverage failed. Expected data points ({}) differs from observerd ({})\n".format(days*24*60, mindata.length()[0])
                    succlst[2] = 5
                else:
                    reportmsg += "Step 3: Checking coverage ... OK\n"

                ## Check f values ( f or df, sampling frequency, if f check delta F, get standard dev)
                if mindata.length()[0] > 0:
                        if self.InitialRead(mindata):
                            self.OnInitialPlot(self.plotstream)
                        """
                        #Test for vector completeness is not working yet
                        colx = mindata.ndarray[1]
                        coly = mindata.ndarray[2]
                        colz = mindata.ndarray[3]
                        colxt = [[mindata.ndarray[0][idx],el] for idx,el in enumerate(colx) if not np.isnan(el)]
                        colyt = [[mindata.ndarray[0][idx],el] for idx,el in enumerate(coly) if not np.isnan(el)]
                        colzt = [[mindata.ndarray[0][idx],el] for idx,el in enumerate(colz) if not np.isnan(el)]
                        t1 = np.transpose(colxt)[0]
                        t2 = np.transpose(colyt)[0]
                        t3 = np.transpose(colzt)[0]
                        s = set(t1)
                        res = []
                        res.extend([x for x in t3 if x not in s])
                        s = set(t3)
                        res.extend([x for x in t1 if x not in s])
                        s = set(t2)
                        res.extend([x for x in t1 if x not in s])
                        s = set(t1)
                        res.extend([x for x in t2 if x not in s])
                        s = set(t2)
                        res.extend([x for x in t3 if x not in s])
                        s = set(t3)
                        res.extend([x for x in t2 if x not in s])
                        print (len(res))
                        res = list(set(res))
                        print (len(res))
                        if not len(res) > 0:
                            reportmsg += "Step 3: Found consistent vector components\n"
                        else:
                            reportmsg += "Step 3: different amount of valid values in vector components: X: {}, Y: {}, Z: {}\n".format(len(t1),len(t2),len(t3))
                            for el in res:
                                warningmsg += "Step 3: found incomplete vector at time step {}\n".format(num2date(el).replace(tzinfo=None))
                            succlst[2] = 3
                        """

                        # create a backup of minutedata
                        mindataback = mindata.copy()
                        reportmsg += "\nStep 3: Analyzing F, dF \n"
                        reportmsg += "-----------------------\n"

                        # Get f data for f criteria check:
                        fcol = mindata._get_column('f')
                        dfcol = mindata._get_column('df')
                        if len(fcol) == 0 and len(dfcol) == 0:
                            reportmsg += "Step 3: minute data: failed to find scalar (F, dF) data ... Failed\n"
                            succlst[2] = 2
                        else:
                            if len(fcol) > 0:
                                scal = 'f'
                            elif len(dfcol) > 0:
                                scal = 'df'
                            ftest = mindata.copy()
                            ftest = ftest._drop_nans(scal)
                            fsamprate = ftest.samplingrate()
                            reportmsg += "Step 3: minute data with {} - sampling period: {} sec ... OK\n".format(scal, fsamprate)
                            if scal=='f':
                                ftest = ftest.delta_f()
                            fmean, fstd = ftest.mean('df',std=True)
                            reportmsg += "Step 3: found an average delta F of {:.3f} +/- {:.3f}\n".format(fmean, fstd)
                            if fmean >= 1.0:
                                reportmsg += "Step 3: large deviation of mean delta F\n"
                                warningmsg += "Step 3: large deviation of mean delta F - might be related to adopted baseline calcuation \n"
                                succlst[2] = 3
                            if fstd >= 3.0:
                                reportmsg += "Step 3: large scatter about mean\n"
                                warningmsg += "Step 3: dF shows a realtively large scatter - please check\n"
                                succlst[2] = 4
                            if fmean < 0.001 and fstd < 0.001:
                                reportmsg += "Step 3: F seems not to be measured independently \n"
                                warningmsg += "Step 3: F seems not to be measured independently\n"
                                succlst[2] = 3

                reportmsg += "\nStep 3: Checking hourly and daily mean data \n"
                reportmsg += "-----------------------\n"
                hourprob = False
                try:
                    iafhour, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth,resolution='hour')
                    #iafhour = read(iafpath,resolution='hour')
                except:
                    errormsg += "Step 3: Could not extract hourly data from IAF file\n"
                    hourprob = True
                    succlst[2] = 6

                if not iafhour.length()[0] > 0:
                    errormsg += "Step 3: Did not find hourly data in IAF file\n"
                    hourprob = True
                    succlst[2] = 6

                if not days*24 - iafhour.length()[0] == 0:
                    errormsg += "Step 3: Did not find expected amount () of hourly data. Found {}.\n".format(days*24,iafhour.length()[0])
                    hourprob = True
                    succlst[2] = 5

                if not hourprob:
                    reportmsg += "Step 3: Extracting hourly data ... OK\n"
                else:
                    reportmsg += "Step 3: Extracting hourly data ... Failed\n"

                if not hourprob:
                    try:
                        minfiltdata = mindata.filter(filter_width=timedelta(minutes=60), resampleoffset=timedelta(minutes=30), filter_type='flat', missingdata='iaga')
                    except:
                        errormsg += "Step 3: Filtering hourly data failed.\n"

                    incon = False
                    faileddiff = False
                    try:
                        diff = subtractStreams(iafhour,minfiltdata)
                        #print ("Here", KEYLIST.index('df'), iafhour.ndarray[KEYLIST.index('df')], minfiltdata.ndarray[KEYLIST.index('df')])
                        if not diff.length()[0] > 0:
                            diff = subtractStreams(iafhour,minfiltdata, keys=['x','y','z'])
                            warningmsg +=  "Step 3: Could not get F/G differences between hourly data and the IAF minute data filtered to hourly means. Please check data file whether hourly means are complete.\n"
                            succlst[2] = 3

                        if not diff.length()[0] > 0:
                            errormsg += "Step 3: Could not calculate difference between hourly mean values and filtered minute data.\n"
                            faileddiff = True
                        incon = False
                        for col in ['x','y','z']:
                            if not diff.amplitude(col) < 0.2:
                                warningmsg += "Step 3: found differences between expected hourly mean values and filtered minute data for component {}\n".format(col)
                                incon = True
                        inconcount = 0
                        if incon:
                            for idx,ts in enumerate(iafhour.ndarray[0]):
                                add = ''
                                for col in ['x','y','z']:
                                    colnum = KEYLIST.index(col)
                                    if not diff.ndarray[colnum][idx] < 0.2:
                                        add += ' -- component {}: expected = {:.1f}; observed = {}'.format(col,minfiltdata.ndarray[colnum][idx], iafhour.ndarray[colnum][idx])
                                if not add == '':
                                    inconcount += 1
                                    warningmsg += 'Step 3: inconsistence at {} {}\n'.format(num2date(ts).replace(tzinfo=None),add)
                    except:
                        errormsg += "Step 3: Failed to obtain difference between hourly data and filtered minute data.\n"
                        faileddiff = True

                    if not faileddiff:
                        if not incon:
                            reportmsg += "Step 3:  +++ IAF: hourly data is consistent\n"
                        else:
                            reportmsg += "Step 3:  !!! IAF: found inconsistencies in hourly data at {} time steps\n".format(inconcount)
                            succlst[2] = 5
                    else:
                            reportmsg += "Step 3:  !!! IAF: failed to obtain differences\n"
                            succlst[2] = 5

                dayprob = False
                try:
                    iafday, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth,resolution='day')
                    #minute, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth)
                    #iafhour = read(iafpath,resolution='hour')
                except:
                    errormsg += "Step 3: Could not extract daily means from IAF file\n"
                    dayprob = True
                    succlst[2] = 6

                if not iafday.length()[0] > 0:
                    errormsg += "Step 3: Did not find daily means in IAF file\n"
                    dayprob = True
                    succlst[2] = 6

                if not dayprob:
                    reportmsg += "Step 3: Extracting daily means ... OK\n"
                else:
                    reportmsg += "Step 3: Extracting daily means ... Failed\n"

                if not dayprob:
                    testmindata = mindataback.copy()
                    testday = testmindata.dailymeans(['x','y','z'],offset=0.0)
                    ddiff = subtractStreams(iafday,testday)
                    try:
                        if not ddiff.length()[0] > 0:
                            errormsg += "Step 3: Could not calculate difference between daily means and average minute data.\n"
                        incon = False
                        for col in ['x','y','z']:
                            if not ddiff.amplitude(col) < 1.0:
                                #print ("Hello", col, ddiff.amplitude(col))
                                warningmsg += "Step 3: found differences between expected daily means and filtered minute data for component {}\n".format(col)
                                incon = True

                        inconcount = 0
                        if incon:
                            for idx,ts in enumerate(ddiff.ndarray[0]):
                                add = ''
                                for col in ['x','y','z']:
                                    colnum = KEYLIST.index(col)
                                    if not np.abs(ddiff.ndarray[colnum][idx]) < 1.0:
                                        add += ' -- component {}: expected = {:.1f}; observed = {}'.format(col,testday.ndarray[colnum][idx], iafday.ndarray[colnum][idx])
                                if not add == '':
                                    inconcount += 1
                                    warningmsg += 'Step 3: inconsistence at {} {}\n'.format(num2date(ts).replace(tzinfo=None),add)
                    except:
                        errormsg += "Step 3: Failed to obtain difference between daily means and filtered minute data.\n"

                    if not incon:
                        reportmsg += "Step 3:  +++ IAF: daily means are consistent\n"
                    else:
                        reportmsg += "Step 3:  !!! IAF: found inconsistencies in daily means at {} time steps\n".format(inconcount)
                        succlst[2] = 5

                if succlst[2] <= 2:
                    reportmsg += "\n----> Step 3: successfully passed\n"
                elif succlst[2] <= 4:
                    reportmsg += "\n----> Step 3: passed with some issues to be reviewed\n"
                self.changeStatusbar("Step 3: Checking data consistency ... Done")

                report = createReport(reportmsg, warningmsg, errormsg)
                opendlg = True
                if opendlg:
                    dlg = CheckDataReportDialog(None, title='Data check step 3 - report', report=report, rating=max(list(map(int,succlst))), step=list(map(str,succlst)), laststep=laststep)
                    dlg.ShowModal()
                    if dlg.moveon:
                        saveReport(dlg.contlabel, dlg.report)
                    else:
                        dlg.Destroy()
                        locale.setlocale(locale.LC_TIME, old_loc)
                        self.changeStatusbar("Ready")
                        return

            if checkparameter['step4']:
                succlst[3] = 1
                reportmsg += "\n"
                reportmsg += "#######################################\n"
                reportmsg += "Step 4:\n"
                reportmsg += "#######################################\n"
                reportmsg += "Def.: checking one second data content and consistency\n\n"
                if seconddata == 'None':
                    reportmsg += "Step 4: No second data available - continue with step 5\n"
                else:
                    proDlg = WaitDialog(None, "Processing...", "Checking consistency of one second data.\nPlease wait ... might need a while")

                    self.changeStatusbar("Step 4: Checking one second data consistency (internally and with primary data) ")
                    # message box - Continuing with step 4 - consistency of one second data with IAF

                    if checkchoice == 'quick':
                        # use already existing data
                        monthlist = [rmonth]
                    else:
                        # read data for each month
                        monthlist = range(1,13)
                    readnew = False # Data needs only to be read again for full analysis
                    for checkmonth in monthlist:
                        if (checkmonth == rmonth) and secdata.length()[0] > 0 and not readnew:
                            secdata = secdata
                        else:
                            self.changeStatusbar("Step 4: Reading one second data for month {} ... be patient".format(checkmonth))
                            secdata, fail = readSecData(secondpath,seconddata,rmonth,year)
                            readnew = True

                        if not onlysec:
                            if (checkmonth == rmonth) and mindataback.length()[0] > 0:
                                mindata = mindataback.copy()
                            else:
                                mindata, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth)
                        else:
                            mindata = DataStream()

                        reportmsg += "\nStep 4: Checking secondary data for month {} \n".format(checkmonth)
                        reportmsg += "-----------------------\n"
                        #reportmsg += "+++++++++++++++++++++++\n"

                        sr = secdata.samplingrate()
                        if sr == 1:
                            reportmsg += "Step 4: Checking coverage - found vectorial 1 second data ... OK\n"
                        else:
                            reportmsg += "Step 4: Checking coverage - found vectorial data every {} second(s)\n".format(sr)

                        #reportmsg += "Step 4: Analyzing F, dF \n"
                        #reportmsg += "-----------------------\n"
                        # Get f data for f criteria check:
                        fcol = secdata._get_column('f')
                        dfcol = secdata._get_column('df')
                        if len(fcol) == 0 and len(dfcol) == 0:
                            reportmsg += "Step 4: !!! One second data: failed to find scalar (F, dF) data ... Failed\n"
                            succlst[3] = 2
                        else:
                            scal=''
                            if len(fcol) > 0:
                                scal = 'f'
                            elif len(dfcol) > 0:
                                scal = 'df'
                            ftest = secdata.copy()
                            ftest = ftest._drop_nans(scal)
                            fsamprate = ftest.samplingrate()
                            reportmsg += "Step 4: +++ found {} in one second data - sampling period: {} sec ... OK\n".format(scal, fsamprate)
                            if scal=='f':
                                ftest = ftest.delta_f()
                            fmean, fstd = ftest.mean('df',std=True)
                            reportmsg += "Step 4: +++ found an average delta F of {:.3f} +/- {:.3f}\n".format(fmean, fstd)
                            if fmean >= 1.0:
                                reportmsg += "Step 4: !!! large deviation of mean delta F - check baseline corr\n"
                                warningmsg += "Step 4: large deviation of mean delta F - check baseline corr\n"
                                succlst[3] = 3
                            if fstd >= 3.0:
                                reportmsg += "Step 4: !!! large scatter about mean\n"
                                warningmsg += "Step 4: dF/G shows large scatter about mean\n"
                                succlst[3] = 4
                            if fmean < 0.001 and fstd < 0.001:
                                reportmsg += "Step 4: !!! F seems not to be measured independently \n"
                                warningmsg += "Step 4: F seems not to be measured independently\n"
                                succlst[2] = 3

                        #reportmsg += "\nStep 4: Filtering second data and comparing it to minute IAF data \n"
                        #reportmsg += "-----------------------\n"

                        self.changeStatusbar("Step 4: Filtering second data ...")
                        highresfilt = secdata.filter(missingdata='iaga')

                        """
                        #Test for vector completeness is not working yet
                        colx = highresfilt._get_column('x')
                        coly = highresfilt._get_column('y')
                        colz = highresfilt._get_column('z')
                        colx = [el for el in colx if not np.isnan(el)]
                        coly = [el for el in coly if not np.isnan(el)]
                        colz = [el for el in colz if not np.isnan(el)]
                        if len(colx) == len(coly) and len(coly) == len(colz):
                            reportmsg += "Step 4: +++ Found consistent vector components\n"
                        else:
                            warningmsg += "Step 4: different amount of valid values in vector components: X: {}, Y: {}, Z: {}\n".format(len(colx),len(coly),len(colz))
                            succlst[3] = 3
                        if not minutepath == '':
                            mcolx = mindata._get_column('x')
                            mcoly = mindata._get_column('y')
                            mcolz = mindata._get_column('z')
                            mcolx = [el for el in mcolx if not np.isnan(el)]
                            mcoly = [el for el in mcoly if not np.isnan(el)]
                            mcolz = [el for el in mcolz if not np.isnan(el)]
                            print (checkmonth, len(colx),len(coly),len(colz),len(mcolx),len(mcoly),len(mcolz))
                        """
                        self.changeStatusbar("Step 4: Filtering second data ... Done")

                        #if not scal == '':
                        #    diff = subtractStreams(highresfilt,mindata,keys=['x','y','z',scal])
                        #else:
                        #    diff = subtractStreams(highresfilt,mindata,keys=['x','y','z'])
                        if not onlysec:
                            diff = subtractStreams(highresfilt,mindata,keys=['x','y','z'])

                            incon = False
                            if not diff.amplitude('x') < 0.2:
                                warningmsg += "Step 4: !!! IAF versus filtered second data: maximum differences in x/h component ({}) significantly exceed numerical uncertainty\n".format(diff.amplitude('x'))
                                incon = True
                            elif not diff.amplitude('x') < 0.11:
                                reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in x/h component ({}) slightly exceed numerical uncertainty\n".format(diff.amplitude('x'))
                            if not diff.amplitude('y') < 0.2:
                                warningmsg += "Step 4: !!! IAF versus filtered second data: maximum differences in y/d component ({}) significantly  exceed numerical uncertainty\n".format(diff.amplitude('y'))
                                incon = True
                            elif not diff.amplitude('y') < 0.11:
                                reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in y/d component ({}) slightly exceed numerical uncertainty\n".format(diff.amplitude('y'))
                            if not diff.amplitude('z') < 0.2:
                                warningmsg += "Step 4: !!! IAF versus filtered second data: maximum differences in z component ({}) significantly  exceed numerical uncertainty\n".format(diff.amplitude('z'))
                                incon = True
                            elif not diff.amplitude('z') < 0.11:
                                reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in z component ({}) slightly exceed numerical uncertainty\n".format(diff.amplitude('z'))
                            if len(diff._get_column(scal)) > 0:
                                if not diff.amplitude(scal) < 0.30: ## uncertainty is larger because of df conversion (2 times rounding error)
                                    warningmsg += "Step 4: IAF versus filtered second data: maximum differences in f component ({}) exceed numerical uncertainty -- PLEASE NOTE: COBS standard procedure is to use mean F for minute and single best F for second\n".format(diff.amplitude('f'))
                                    try:
                                        ttf, ttstd = diff.mean('f',std=True)
                                        warningmsg += "Step 4: !!! IAF versus filtered second data: mean f = {} +/- {}\n".format(ttf,ttstd)
                                        incon = True
                                    except:
                                        pass
                            if not incon:
                                reportmsg += "Step 4: +++ IAF/Filtered(Sec): IAF data and filtered second data is consistent\n"
                            else:
                                reportmsg += "Step 4: !!! IAF versus filtered second data: found inconsistencies. Eventually the one minute data record has been cleaned but not the second data set? \n"
                                succlst[3] = 4
                                if self.InitialRead(diff):
                                    self.OnInitialPlot(self.plotstream)

                    proDlg.Destroy()

                if succlst[3] <= 2:
                    reportmsg += "\n----> Step 4: successfully passed\n"
                elif succlst[3] <= 4:
                    reportmsg += "\n----> Step 4: passed with some issues to be reviewed\n"
                self.changeStatusbar("Step 4: Checking secondary data ... Done")

                report = createReport(reportmsg, warningmsg, errormsg)
                opendlg = True
                if opendlg:
                    dlg = CheckDataReportDialog(None, title='Data check step 4 - report', report=report, rating=max(list(map(int,succlst))), step=list(map(str,succlst)), laststep=laststep)
                    dlg.ShowModal()
                    if dlg.moveon:
                        saveReport(dlg.contlabel, dlg.report)
                    else:
                        dlg.Destroy()
                        locale.setlocale(locale.LC_TIME, old_loc)
                        self.changeStatusbar("Ready")
                        return


            if checkparameter['step5']:
                succlst[4] = 1
                reportmsg += "\n"
                reportmsg += "#######################################\n"
                reportmsg += "Step 5:\n"
                reportmsg += "#######################################\n"
                reportmsg += "Def.: baseline variation and data quality\n\n"

                if not blvpath == '':
                    reportmsg += "Step 5: baseline data \n"
                    reportmsg += "-----------------------\n"
                    blvdata = read(blvpath)
                    if self.InitialRead(blvdata):
                        #self.ActivateControls(self.plotstream)
                        self.OnInitialPlot(self.plotstream)
                    self.changeStatusbar("Step 5: checking basline ...")
                    """
                    self.plotstream = blvdata
                    self.ActivateControls(self.plotstream)
                    # set padding and plotsymbols to points
                    self.OnPlot(self.plotstream,self.shownkeylist)
                    """

                    headx = blvdata.header.get("col-dx","")
                    heady = blvdata.header.get("col-dy","")
                    headz = blvdata.header.get("col-dz","")
                    unitx = blvdata.header.get("unit-col-dx","")
                    unity = blvdata.header.get("unit-col-dy","")
                    unitz = blvdata.header.get("unit-col-dz","")
                    # get average sampling rate
                    means = blvdata.dailymeans(keys=['dx','dy','dz'])
                    srmeans = means.get_sampling_period()
                    reportmsg += "Step 5: basevalues measured on average every {:.1f} days \n".format(srmeans)
                    # Average and maximum standard deviation
                    means = means._drop_nans('dx')
                    #print ("means", means.mean('dx',percentage=1), means.amplitude('dx'))
                    reportmsg += "Step 5: average deviation of repeated measurements is: {:.2f}{} for {}, {:.4f}{} for {} and {:.2f}{} for {}\n".format(means.mean('dx',percentage=1), unitx, headx, means.mean('dy',percentage=1), unity, heady, means.mean('dz',percentage=1), unitz, headz)
                    if means.mean('dx',percentage=1) > 0.5 or means.mean('dz',percentage=1) > 0.5:
                        reportmsg += "Step 5: consistent variations between repeated measurements are present\n"
                        succlst[4] = 2
                    if means.mean('dx',percentage=1) > 3.0 or means.mean('dz',percentage=1) > 3.0:
                        reportmsg += "Step 5: !!! found relatively large variations for repeated measurements\n"
                        warningmsg += "Step 5: check basevalues\n"
                        succlst[4] = 4

                    # analyse residuum between baseline and basevalues
                    func = blvdata.header.get('DataFunction',[])
                    residual = blvdata.func2stream(func,mode='sub',keys=['dx','dy','dz'])
                    #print ("Here", residual.ndarray)
                    resDIx,resDIstdx = residual.mean('dx',std=True,percentage=10)
                    resDIy,resDIstdy = residual.mean('dy',std=True,percentage=10)
                    resDIz,resDIstdz = residual.mean('dz',std=True,percentage=10)
                    reportmsg += "Step 5: average residual between baseline and basevalues is: {}={:.3f}{}, {}={:.5f}{}, {}={:.3f}{} \n".format(headx, resDIx, unitx, heady, resDIy, unity, headz, resDIz, unitz)
                    if resDIx > 0.1 or resDIz > 0.1:
                        reportmsg += "Step 5: -> found minor deviations between baseline and basevalues\n"
                        succlst[4] = 2
                    if resDIx > 0.5 or resDIz > 0.5:
                        reportmsg += "Step 5: !!! found relatively large deviations between baseline and basevalues\n"
                        warningmsg += "Step 5: check deviations between baseline and basevalues\n"
                        succlst[4] = 3

                    # overall baseline variation
                    # get maximum and minimum of the function for x and z
                    amplitude = blvdata.func2stream(func,mode='values',keys=['dx','dy','dz'])
                    #amplitude = amplitude._convertstream('hdz2xyz') ## TODO this is not correct !!!
                    ampx = amplitude.amplitude('dx')
                    ampy = amplitude.amplitude('dy')
                    ampz = amplitude.amplitude('dz')
                    maxamp = np.max([ampx,ampy,ampz])  # TODO declination!!!
                    reportmsg += "Step 5: maximum amplitude of baseline is {:.1f}{} \n".format(maxamp, unitx)
                    amplitude = amplitude._move_column('dx','x')
                    amplitude = amplitude._move_column('dy','y')
                    amplitude = amplitude._move_column('dz','z')
                    # PLEASE note: amplitude test is currently, effectively only testing x and z component
                    #              this is still useful as maximum amplitudes are expected in these components
                    if maxamp > 5:
                        reportmsg += "Step 5: !!! amplitude of adopted baseline exceeds INTERMAGNET threshold of 5 nT\n"
                        warningmsg += "Step 5: adopted baseline shows relatively high variations - could be related to baseline jumps - please review data\n"
                        succlst[4] = 3

                    # check baseline complexity


                else:
                    reportmsg += "Step 5: could not open baseline data \n"
                    errormsg += "Step 5: failed to open baseline data\n"
                    succlst[4] = 6

                if succlst[4] <= 2:
                    reportmsg += "\n----> Step 5: successfully passed\n"
                elif succlst[4] <= 4:
                    reportmsg += "\n----> Step 5: passed with some issues to be reviewed\n"
                self.changeStatusbar("Step 5: measured and adopted basevalues ... Done")

                report = createReport(reportmsg, warningmsg, errormsg)
                opendlg = True
                if opendlg:
                    dlg = CheckDataReportDialog(None, title='Data check step 5 - report', report=report, rating=succlst[4], step=list(map(str,succlst)), laststep=laststep)
                    dlg.ShowModal()
                    if dlg.moveon:
                        saveReport(dlg.contlabel, dlg.report)
                    else:
                        dlg.Destroy()
                        locale.setlocale(locale.LC_TIME, old_loc)
                        self.changeStatusbar("Ready")
                        return

            if checkparameter['step6']:
                succlst[5] = 1
                reportmsg += "\n"
                reportmsg += "#######################################\n"
                reportmsg += "Step 6:\n"
                reportmsg += "#######################################\n"
                reportmsg += "Def.: yearly means, consistency of meta information in all files\n\n"

                # internally check yearmean  (Not yet)
                # check whether all data files contain the same means

                def diffs (success, hmean1,zmean1,hmean2,zmean2,source1='blv',source2='iaf',threshold=0.5):
                    repmsg = ''
                    warnmsg = ''
                    if not np.isnan(hmean1) and not np.isnan(hmean2):
                        diffh = np.abs(hmean1-hmean2)
                        diffz = np.abs(zmean1-zmean2)
                        if diffh < threshold and diffz < threshold:
                            repmsg += "Step 6: yearly means between {} and {} files are consistent\n".format(source1, source2)
                        else:
                            repmsg += "Step 6: yearly means differ between {} and {} files. {}: H={}nT,Z={}nT; {}: H={}nT, Z={}nT \n".format(source1, source2, source1, hmean1,zmean1, source2, hmean2,zmean2)
                            success = 5
                            if source1 == 'yearmean':
                                repmsg += "    ->   difference might be related to data jumps within the Yearmean file, which are considered when reading this file\n"
                                success = 4
                            warnmsg += "Step 6: yearly means differ between {} and {} files\n".format(source1, source2)
                    else:
                        repmsg += "Step 6: did not compare yearly means of {} and {} data - select step 3 and full to perform this check \n".format(source1, source2)
                    return repmsg, warnmsg, success

                reportmsg += "Step 6: yearly means \n"
                reportmsg += "-----------------------\n"
                #blv yearly means
                blvhmean = np.nan
                blvfmean = np.nan
                if not blvpath == '':
                    try:
                        le = blvdata.length()[0]
                    except:
                        blvdata = read(blvpath)
                    blvhmean = blvdata.header.get('DataScaleX')
                    blvfmean = blvdata.header.get('DataScaleZ')

                #iaf yearly means
                minhmean = np.nan
                minfmean = np.nan
                try:
                    if mindataback.header.get('DataComponents','').startswith('hdz'):
                        mindataback = mindataback._convertstream('hdz2xyz')
                    if checkchoice == 'full' and mindataback.length()[0] > 0:
                        minxmean = mindataback.mean('x',percentage=50)
                        minymean = mindataback.mean('y',percentage=50)
                        minzmean = mindataback.mean('z',percentage=50)
                        minhmean = np.sqrt(minxmean*minxmean + minymean*minymean)
                        minfmean = np.sqrt(minxmean*minxmean + minymean*minymean + minzmean*minzmean)

                    rep, warn, succlst[5] = diffs(succlst[5],blvhmean,blvfmean,minhmean,minfmean)
                    reportmsg += rep
                    warningmsg += warn
                except:
                    reportmsg += "Step 6: could not determine yearly means from IAF - data not availbale \n"

                #yearmean yearly means
                yearmeanh = np.nan
                yearmeanf = np.nan
                if not yearmeanpath == '':
                    yearmeandata = read(yearmeanpath, debug=True)
                    if not yearmeandata.length()[0] > 0:
                        warningmsg += "Step 6: !!! Could not read yearmean data. Please check manually!\n"
                        reportmsg += "Step 6: !!! Could not read yearmean data. Please check manually!\n"
                        succlst[5] = 4
                    else:
                        yearmeanx = yearmeandata.ndarray[1][-1]
                        yearmeany = yearmeandata.ndarray[2][-1]
                        yearmeanz = yearmeandata.ndarray[3][-1]
                        yearmeanh = np.sqrt(yearmeanx*yearmeanx + yearmeany*yearmeany)
                        yearmeanf = np.sqrt(yearmeanx*yearmeanx + yearmeany*yearmeany + yearmeanz*yearmeanz)
                        # extract data for year
                        rep, warn, succlst[5] = diffs(succlst[5],yearmeanh,yearmeanf,minhmean,minfmean,source1='yearmean',source2='iaf',threshold=0.5)
                        reportmsg += rep
                        warningmsg += warn
                        rep, warn, succlst[5] = diffs(succlst[5],yearmeanh,yearmeanf,blvhmean,blvfmean,source1='yearmean',source2='blv',threshold=1.0)
                        reportmsg += rep
                        warningmsg += warn
                        reportmsg += "Step 6: yearlmean.imo contains data from {} until {} \n".format(num2date(yearmeandata.ndarray[0][0]).year,num2date(yearmeandata.ndarray[0][-1]).year)

                if not seconddata == 'None':
                    primeheader = secdata.header
                elif mindata:
                    primeheader = mindata.header
                else:
                    primeheader = {}

                reportmsg += "\nStep 6: meta information \n"
                reportmsg += "-----------------------\n"
                excludelist = ['DataComponents','DataSamplingRate','DataPublicationDate','col-f','col-x','col-y','col-z','col-df','unit-col-f','unit-col-x','unit-col-y','unit-col-z','unit-col-df','DataSamplingFilter']
                floatlist = {'DataElevation':0,'DataAcquisitionLongitude':2,'DataAcquisitionLatitude':2}
                secmsg = ''
                minmsg = ''
                yearmsg = ''
                if not primeheader == {}:
                    if not seconddata == 'None' and mindata and yearmeandata:
                        for key in secdata.header:
                            refvalue = secdata.header.get(key)
                            compvalue1 = mindata.header.get(key,'')
                            compvalue2 = yearmeandata.header.get(key,'')
                            if not key.startswith('col') and not key.startswith('unit'):
                                keyname = key.replace('Data','').replace('station','')
                                secmsg += "Step 6: (Secondary (ImagCDF/IAGA) meta) {}: {}\n".format(keyname,refvalue)
                            if key in floatlist:
                                refvalue = np.round(float(refvalue),floatlist.get(key))
                                if not compvalue1 == '':
                                    compvalue1 = np.round(float(compvalue1),floatlist.get(key))
                                if not compvalue2 == '':
                                    compvalue2 = np.round(float(compvalue2),floatlist.get(key))
                            if not compvalue1 == '' and not key in excludelist:
                                if not refvalue == compvalue1:
                                    OK = False
                                    try:
                                        if refvalue.lower()[:3] == compvalue1.lower()[:3]: #check capitals
                                            OK=True
                                    except:
                                        pass
                                    if not OK:
                                        reportmsg += "Step 6: !!! Found differences for {} between {} and {}: {} unequal {}\n".format(key,'sec','min',refvalue,compvalue1)
                                        succlst[5] = 3
                            if not compvalue2 == '' and not key in excludelist:
                                if not refvalue == compvalue2:
                                    reportmsg += "Step 6: !!! Found differences for {} between {} and {}: {} unequal {}\n".format(key,'sec','year',refvalue,compvalue2)
                                    succlst[5] = 3
                    if mindata and yearmeandata:
                        for key in mindata.header:
                            refvalue = mindata.header.get(key)
                            compvalue1 = yearmeandata.header.get(key,'')
                            if not key.startswith('col') and not key.startswith('unit'):
                                keyname = key.replace('Data','').replace('station','')
                                minmsg += "Step 6: (Primary (IAF) meta) {}: {}\n".format(keyname,refvalue)
                                if not compvalue1 == '':
                                    yearmsg += "Step 6: (yearmean meta) {}: {}\n".format(keyname,compvalue1)
                            if key in floatlist:
                                refvalue = np.round(float(refvalue),floatlist.get(key))
                                if not compvalue1 == '':
                                    compvalue1 = np.round(float(compvalue1),floatlist.get(key))
                            if not compvalue1 == '' and not key in excludelist:
                                if not refvalue == compvalue1:
                                    reportmsg += "Step 6: !!! Found differences for {} between {} and {}: {} unequal {}\n".format(key,'min','year',refvalue,compvalue1)
                                    succlst[5] = 3

                    metamsg = secmsg+minmsg+yearmsg
                    reportmsg += metamsg

                if succlst[5] <= 2:
                    reportmsg += "\n----> Step 6: successfully passed\n"
                elif succlst[5] <= 4:
                    reportmsg += "\n----> Step 6: passed with some issues to be reviewed\n"
                self.changeStatusbar("Step 6: yearly means and meta information ... Done")

                report = createReport(reportmsg, warningmsg, errormsg)
                opendlg = True
                if opendlg:
                    dlg = CheckDataReportDialog(None, title='Data check step 6 - report', report=report, rating=succlst[5], step=list(map(str,succlst)), laststep=laststep)
                    dlg.ShowModal()
                    if dlg.moveon:
                        saveReport(dlg.contlabel, dlg.report)
                    else:
                        dlg.Destroy()
                        locale.setlocale(locale.LC_TIME, old_loc)
                        self.changeStatusbar("Ready")
                        return

            if checkparameter['step7']:
                succlst[6] = 1
                reportmsg += "\n"
                reportmsg += "#######################################\n"
                reportmsg += "Step 7:\n"
                reportmsg += "#######################################\n"
                reportmsg += "Def.: checking K values\n\n"

                posk = KEYLIST.index('var1')
                # compare content of dka and iaf
                if dkadata:
                    dkadata = read(dkapath,debug=True)
                    if not dkadata.length()[0] > 0:
                        warningmsg += 'Step 7: Could not read provided dka file !!!\n'
                    else:
                        if dkadata.amplitude('var1') > 9:
                            warningmsg += 'Step 7: k values in DKA file exceed 9 !!!\n'
                            succlst[6] = 4
                try:
                    iafk, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth,resolution='k')
                    if iafk.amplitude('var1') > 9:
                        warningmsg += 'Step 7: k values in IAF exceed 9 !!!\n'
                        succlst[6] = 5
                    if self.InitialRead(iafk):
                        self.OnInitialPlot(self.plotstream)
                except:
                    errormsg += "Step 7: Could not extract k values from IAF file\n"
                    dayprob = True
                    succlst[6] = 5

                if dkadata and succlst[6] < 5:
                    kdiffs = subtractStreams(iafk, dkadata)
                    if kdiffs.length()[0] > 0:
                        posk = KEYLIST.index('var1')
                        for el in kdiffs.ndarray[posk]:
                            if el >= 0.1:
                                warningmsg += 'Step 7: difference between k in IAF and DKA files at {}: IAF: {}, DKA: {}\n'.format(num2date(kdiffs.ndarray[0][idx]).replace(tzinfo=None), iafk.ndarray[posk][idx], dkadata.ndarray[posk][idx])
                                succlst[6] = 4
                    else:
                        warningmsg += 'Step 7: (optional) k value check with DKA not performed.\n'
                        succlst[6] = 2
                else:
                    #warningmsg += 'Step 7: k value check not performed\n'
                    succlst[6] = 2

                if succlst[6] <= 2:
                    reportmsg += "Step 7: k values ... OK\n"
                    reportmsg += "\n----> Step 7: successfully passed\n"
                elif succlst[6] <= 4:
                    reportmsg += "\n----> Step 7: passed with some issues to be reviewed\n"
                self.changeStatusbar("Step 7: k values OK ... Done")

                report = createReport(reportmsg, warningmsg, errormsg)
                opendlg = True
                if opendlg:
                    dlg = CheckDataReportDialog(None, title='Data check step 6 - report', report=report, rating=succlst[6], step=list(map(str,succlst)), laststep=laststep)
                    dlg.ShowModal()
                    if dlg.moveon:
                        saveReport(dlg.contlabel, dlg.report)
                    else:
                        dlg.Destroy()
                        locale.setlocale(locale.LC_TIME, old_loc)
                        self.changeStatusbar("Ready")
                        return


        locale.setlocale(locale.LC_TIME, old_loc)
        self.changeStatusbar("Check data finished - Ready")


    # ################
    # page methods:

    # pages: stream (plot, coordinate), analysis (smooth, filter, fit, baseline etc),
    #          specials(spectrum, power), absolutes (), report (log), monitor (access web socket)

    def checkDB(self, level='minimal'):
        if self.databaseconnected:
            try:
                dbinfo(self.db,destination='stdout',level='minimal')#,level='full') # use level ='minimal'
            except:
                logger.info("Database not connected any more -- reconnecting")
                self._db_connect(self.options.get('host',''), self.options.get('user',''), self.options.get('passwd',''), self.options.get('dbname',''))



    # ------------------------------------------------------------------------------------------
    # ################
    # Analysis functions
    # ################
    # ------------------------------------------------------------------------------------------

    def onFilterButton(self, event):
        """
        Method for filtering
        """
        self.changeStatusbar("Filtering...")

        # open dialog to modify filter parameters
        #keystr = self.menu_p.met_page.keysTextCtrl.GetValue().encode('ascii','ignore')
        #keys = keystr.split(',')
        sr = self.plotstream.samplingrate()

        filter_type = 'gaussian'
        resample_offset = 0.0

        if sr < 0.5: # use 1 second filter with 0.3 Hz cut off as default
                filter_width = timedelta(seconds=3.33333333)
                resample_period = 1.0
        elif sr < 50: # use 1 minute filter with 0.008 Hz cut off as default
                filter_width = timedelta(minutes=2)
                resample_period = 60.0
        else: # use 1 hour flat filter
                filter_width = timedelta(minutes=60)
                resample_period = 3600.0
                resample_offset = 1800.0
                filter_type = 'flat'
        miss = 'conservative'

        dlg = AnalysisFilterDialog(None, title='Analysis: Filter', samplingrate=sr, resample=True, winlen=filter_width.total_seconds(), resint=resample_period, resoff= resample_offset, filtertype=filter_type)
        if sr < 0.5: # use 1 second filter with 0.3 Hz cut off as default
            dlg.methodRadioBox.SetStringSelection('conservative')

        if dlg.ShowModal() == wx.ID_OK:
            filtertype = dlg.filtertypeComboBox.GetValue()
            filterlength = float(dlg.lengthTextCtrl.GetValue())
            resampleinterval = float(dlg.resampleTextCtrl.GetValue())
            resampleoffset = float(dlg.resampleoffsetTextCtrl.GetValue())
            missingdata = dlg.methodRadioBox.GetStringSelection()
            #print (filtertype,filterlength,missingdata,resampleinterval,resampleoffset)
            if missingdata == 'IAGA':
                miss = 'mean'
            elif missingdata == 'interpolate':
                miss = 'interpolate'

            self.plotstream = self.plotstream.filter(keys=self.shownkeylist,filter_type=filtertype,filter_width=timedelta(seconds=filterlength),resample_period=resampleinterval,resample_offset=resampleoffset,missingdata=miss,resample=True)
            self.menu_p.rep_page.logMsg('- data filtered: {} window, {} Hz passband'.format(filtertype,1./filterlength))

            self.ActivateControls(self.plotstream)
            self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")


    def onDerivativeButton(self, event):
        """
        Method for derivative
        """
        self.changeStatusbar("Calculating derivative ...")
        keys = self.shownkeylist

        if len(self.plotstream.ndarray[0]) == 0:
            self.plotstream = self.stream.copy()

        self.menu_p.rep_page.logMsg("- calculating derivative")
        self.plotstream = self.plotstream.differentiate(keys=keys,put2keys=keys)

        self.menu_p.rep_page.logMsg('- derivative calculated')
        self.ActivateControls(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")

    def onFitButton(self, event):
        """
        Method for fitting
        """
        self.changeStatusbar("Fitting ...")
        keys = self.shownkeylist
        if len(self.plotstream.ndarray[0]) == 0:
            self.plotstream = self.stream.copy()
        #fitknots = str(0.5)
        #fitdegree = str(4)
        #fitfunc='spline'
        self.xlimits = self.plot_p.xlimits


        dlg = AnalysisFitDialog(None, title='Analysis: Fit parameter', options=self.options, stream = self.plotstream, shownkeylist=self.shownkeylist, keylist=self.keylist)
        startdate=self.xlimits[0]
        enddate=self.xlimits[1]
        starttime = num2date(startdate).strftime('%X')
        endtime = num2date(enddate).strftime('%X')
        dlg.startFitDatePicker.SetValue(pydate2wxdate(num2date(startdate)))
        dlg.endFitDatePicker.SetValue(pydate2wxdate(num2date(enddate)))
        dlg.startFitTimePicker.SetValue(starttime)
        dlg.endFitTimePicker.SetValue(endtime)
        if dlg.ShowModal() == wx.ID_OK:
            fitfunc = dlg.funcComboBox.GetValue()
            knots = dlg.knotsTextCtrl.GetValue()
            degree = dlg.degreeTextCtrl.GetValue()
            # Getting time information
            stday = dlg.startFitDatePicker.GetValue()
            sttime = str(dlg.startFitTimePicker.GetValue())
            if sttime.endswith('AM') or sttime.endswith('am'):
                sttime = datetime.strftime(datetime.strptime(sttime,"%I:%M:%S %p"),"%H:%M:%S")
            if sttime.endswith('pm') or sttime.endswith('PM'):
                sttime = datetime.strftime(datetime.strptime(sttime,"%I:%M:%S %p"),"%H:%M:%S")
            sd = datetime.strftime(datetime.fromtimestamp(stday.GetTicks()), "%Y-%m-%d")
            starttime= datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
            enday = dlg.endFitDatePicker.GetValue()
            entime = str(dlg.endFitTimePicker.GetValue())
            if entime.endswith('AM') or entime.endswith('am'):
                entime = datetime.strftime(datetime.strptime(entime,"%I:%M:%S %p"),"%H:%M:%S")
            if entime.endswith('pm') or entime.endswith('PM'):
                entime = datetime.strftime(datetime.strptime(entime,"%I:%M:%S %p"),"%H:%M:%S")
            ed = datetime.strftime(datetime.fromtimestamp(enday.GetTicks()), "%Y-%m-%d")
            endtime= datetime.strptime(str(ed)+'_'+entime, "%Y-%m-%d_%H:%M:%S")

            self.options['fitfunction'] = fitfunc
            if fitfunc.startswith('poly'):
                fitfunc = 'poly'

            self.menu_p.rep_page.logMsg('Fitting with %s, %s, %s' % (fitfunc, knots, degree))
            if not 0<float(knots)<1:
                knots = 0.5
            else:
                knots = float(knots)
            if not int(degree)>0:
                degree = 1
            else:
                degree = int(degree)
            self.options['fitknotstep'] = str(knots)
            self.options['fitdegree'] = str(degree)
            if len(self.plotstream.ndarray[0]) > 0:
                func = self.plotstream.fit(keys=keys,fitfunc=fitfunc,fitdegree=degree,knotstep=knots, starttime=starttime, endtime=endtime)
                if isinstance(self.plotopt['function'], list) and len(self.plotopt['function']) > 0:
                    self.plotopt['function'].append(func)
                else:
                    self.plotopt['function'] = [func]
                #self.function = func
                #self.plotopt['function'] = func
                self.ActivateControls(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)
            else:
                # Msgbox to load data first
                pass

        dlg.Destroy()
        self.menu_p.rep_page.logMsg('- data fitted')
        self.changeStatusbar("Ready")


    def onOffsetButton(self, event):
        """
        Method for offset correction
        """
        # Check whether DB still available
        self.checkDB('minimal')

        self.changeStatusbar("Adding offsets ...")
        keys = self.shownkeylist
        offsetdict = {}

        # get currently zoomed time limits and use as timerange
        self.xlimits = self.plot_p.xlimits
        if not self.xlimits:
            self.xlimits = [num2date(self.plotstream.ndarray[0][0]),num2date(self.plotstream.ndarray[0][-1])]
        else:
            self.xlimits = [num2date(self.xlimits[0]),num2date(self.xlimits[-1])]

        # get existing deltas from database
        deltas = self.plotstream.header.get('DataDeltaValues','')

        if deltas == '':
            # Check data compensation values
            try:
                xcorr = float(self.plotstream.header.get('DataCompensationX',''))
                ycorr = float(self.plotstream.header.get('DataCompensationY',''))
                zcorr = float(self.plotstream.header.get('DataCompensationZ',''))
                if not xcorr=='' and not ycorr=='' and not zcorr=='':
                    deltas = 'x_{},y_{},z_{}'.format(-1*xcorr*1000.,-1*ycorr*1000.,-1*zcorr*1000.)
            except:
                pass
        #print ("Delta", deltas)

        dlg = AnalysisOffsetDialog(None, title='Analysis: define offsets', keylst=keys, xlimits=self.xlimits, deltas=deltas)
        if dlg.ShowModal() == wx.ID_OK:
            for key in keys:
                offset = eval('dlg.'+key+'TextCtrl.GetValue()')
                if not offset in ['','0']:
                    if not float(offset) == 0:
                        offsetdict[key] = float(offset)
            val = dlg.offsetRadioBox.GetStringSelection()
            #print ("Offset", val)
            if str(val) == 'all':
                toffset = dlg.timeshiftTextCtrl.GetValue()
                if not self.plotstream._is_number(toffset):
                    toffset = 0
                if not float(toffset) == 0:
                    offsetdict['time'] = timedelta(seconds=float(toffset))
                self.plotstream = self.plotstream.offset(offsetdict)
            else:
                stday = dlg.StartDatePicker.GetValue()
                sttime = str(dlg.StartTimeTextCtrl.GetValue())
                sd = datetime.strftime(datetime.fromtimestamp(stday.GetTicks()), "%Y-%m-%d")
                st= datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
                edday = dlg.EndDatePicker.GetValue()
                edtime = str(dlg.EndTimeTextCtrl.GetValue())
                ed = datetime.strftime(datetime.fromtimestamp(edday.GetTicks()), "%Y-%m-%d")
                et= datetime.strptime(str(ed)+'_'+edtime, "%Y-%m-%d_%H:%M:%S")
                self.plotstream = self.plotstream.offset(offsetdict, starttime=st, endtime=et)

            self.plotstream.header['DataDeltaValuesApplied'] = 1
            self.ActivateControls(self.plotstream)
            self.OnPlot(self.plotstream,self.shownkeylist)

        dlg.Destroy()
        self.changeStatusbar("Ready")


    def onResampleButton(self, event):
        """
        Method for offset correction
        """
        self.changeStatusbar("Resampling ...")
        keys = self.shownkeylist
        sr = self.plotstream.samplingrate()

        dlg = AnalysisResampleDialog(None, title='Analysis: resampling parameters', keylst=keys, period=sr)
        if dlg.ShowModal() == wx.ID_OK:
            newperiod = dlg.periodTextCtrl.GetValue()
            self.plotstream = self.plotstream.resample(keys, period=float(newperiod))
            self.menu_p.rep_page.logMsg('- resampled stream at period {} second'.format(newperiod))
            self.ActivateControls(self.plotstream)
            self.OnPlot(self.plotstream,self.shownkeylist)

        dlg.Destroy()
        self.changeStatusbar("Ready")

    def onActivityButton(self, event):
        """
        Method for offset correction
        """
        self.changeStatusbar("Getting activity (FMI method)...")

        keys = self.shownkeylist
        offsetdict = {}

        #dlg = AnalysisActivityDialog(None, title='Analysis: get k values (FMI)')
        #if dlg.ShowModal() == wx.ID_OK:
        backup = self.plotstream.copy()
        stream = self.plotstream.k_fmi()
        self.streamlist.append(stream)
        self.streamkeylist.append(stream._get_key_headers())
        self.currentstreamindex = len(self.streamlist)-1
        self.plotstream = self.streamlist[-1]
        #self.headerlist.append(self.plotstream.header)
        self.headerlist.append(stream.header)
        self.shownkeylist = self.plotstream._get_key_headers(numerical=True)
        if self.plotstream and len(self.plotstream.ndarray[0]) > 0:
            self.ActivateControls(self.plotstream)
            keylist = self.UpdatePlotCharacteristics(self.plotstream)
            self.plotoptlist.append(self.plotopt)
            self.OnPlot(self.plotstream,self.shownkeylist)
        else:
            self.plotstream = backup.copy()

        self.changeStatusbar("Ready")

    def onRotationButton(self, event):
        """
        Method for offset correction
        """
        self.changeStatusbar("Rotating data ...")

        if len(self.plotstream.ndarray[0]) > 0:
            # XXX Eventually SetValues from init
            dlg = AnalysisRotationDialog(None, title='Analysis: rotate data')
            if dlg.ShowModal() == wx.ID_OK:
                alphat = dlg.alphaTextCtrl.GetValue()
                betat = dlg.betaTextCtrl.GetValue()
                try:
                    alpha = float(alphat)
                except:
                    alpha = 0.0
                try:
                    beta = float(betat)
                except:
                    beta = 0.0

                self.plotstream = self.plotstream.rotation(alpha=alpha, beta=beta)
                self.menu_p.rep_page.logMsg('- rotated stream by alpha = %s and beta = %s' % (alphat,betat))
                self.ActivateControls(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)

        dlg.Destroy()
        self.changeStatusbar("Ready")

    def onMeanButton(self, event):
        """
        DESCRIPTION
             Calculates means values for all keys of shownkeylist
        """
        self.changeStatusbar("Calculating means ...")
        keys = self.shownkeylist
        meanfunc = 'mean'

        teststream = self.plotstream.copy()
        # limits
        self.xlimits = self.plot_p.xlimits

        if not self.xlimits == [self.plotstream.ndarray[0],self.plotstream.ndarray[-1]]:
            testarray = self.plotstream._select_timerange(starttime=self.xlimits[0],endtime=self.xlimits[1])
            teststream = DataStream([LineStruct()],self.plotstream.header,testarray)

        mean = [teststream.mean(key,meanfunction='mean',std=True,percentage=10) for key in keys]
        t_limits = teststream._find_t_limits()
        trange = '- mean - timerange: {} to {}'.format(t_limits[0],t_limits[1])
        self.menu_p.rep_page.logMsg(trange)
        for idx,me in enumerate(mean):
            meanline = '- mean - key: {} = {} +/- {}'.format(keys[idx],me[0],me[1])
            self.menu_p.rep_page.logMsg(meanline)
            trange = trange + '\n' + meanline
        # open message dialog
        dlg = wx.MessageDialog(self, "Means:\n"+
                        str(trange),
                        "Analysis: Mean values", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.changeStatusbar("Ready")

    def onMaxButton(self, event):
        """
        DESCRIPTION
             Calculates max values for all keys of shownkeylist
        """
        self.changeStatusbar("Calculating maxima ...")
        keys = self.shownkeylist

        teststream = self.plotstream.copy()
        # limits
        self.xlimits = self.plot_p.xlimits
        if not self.xlimits == [self.plotstream.ndarray[0],self.plotstream.ndarray[-1]]:
            testarray = self.plotstream._select_timerange(starttime=self.xlimits[0],endtime=self.xlimits[1])
            teststream = DataStream([LineStruct()],self.plotstream.header,testarray)

        maxi = [teststream._get_max(key,returntime=True) for key in keys]
        t_limits = teststream._find_t_limits()
        trange = '- maxima - timerange: {} to {}'.format(t_limits[0],t_limits[1])
        self.menu_p.rep_page.logMsg(trange)
        for idx,me in enumerate(maxi):
            meanline = '- maxima - key: {} = {} at {}'.format(keys[idx],me[0],num2date(me[1]))
            self.menu_p.rep_page.logMsg(meanline)
            trange = trange + '\n' + meanline
        # open message dialog
        dlg = wx.MessageDialog(self, "Maxima:\n"+
                        str(trange),
                        "Analysis: Maximum values", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.changeStatusbar("Ready")

    def onMinButton(self, event):
        """
        DESCRIPTION
             Calculates means values for all keys of shownkeylist
        """
        self.changeStatusbar("Calculating minima ...")
        keys = self.shownkeylist

        teststream = self.plotstream.copy()
        # limits
        self.xlimits = self.plot_p.xlimits
        if not self.xlimits == [self.plotstream.ndarray[0],self.plotstream.ndarray[-1]]:
            testarray = self.plotstream._select_timerange(starttime=self.xlimits[0],endtime=self.xlimits[1])
            teststream = DataStream([LineStruct()],self.plotstream.header,testarray)

        mini = [teststream._get_min(key,returntime=True) for key in keys]
        t_limits = teststream._find_t_limits()
        trange = '- minima - timerange: {} to {}'.format(t_limits[0],t_limits[1])
        self.menu_p.rep_page.logMsg(trange)
        for idx,me in enumerate(mini):
            meanline = '- minima - key: {} = {} at {}'.format(keys[idx],me[0],num2date(me[1]))
            self.menu_p.rep_page.logMsg(meanline)
            trange = trange + '\n' + meanline
        # open message dialog
        dlg = wx.MessageDialog(self, "Minima:\n"+
                        str(trange),
                        "Analysis: Minimum values", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.changeStatusbar("Ready")


    def onFlagmodButton(self, event):
        """
        DESCRIPTION
             Shows Flagilist statistics and allows to change flag contents
        """
        self.changeStatusbar("Flaglist contents ...")
        keys = self.shownkeylist

        if not self.flaglist or not len(self.flaglist) > 0:
            self.changeStatusbar("no flags available ... Ready")
            return

        stats = self.plotstream.flagliststats(self.flaglist, intensive=True, output='string')

        self.menu_p.rep_page.logMsg(stats)
        """
        for idx,me in enumerate(mean):
            meanline = '- mean - key: {} = {} +/- {}'.format(keys[idx],me[0],me[1])
            self.menu_p.rep_page.logMsg(meanline)
            trange = trange + '\n' + meanline
        """
        # open message dialog
        dlg = AnalysisFlagsDialog(None, title='Analysis: Flags', stats=stats, flaglist=self.flaglist, stream=self.plotstream)
        if dlg.ShowModal() == wx.ID_OK:
            if dlg.mod:
                self.changeStatusbar("Applying new flags ...")
                self.menu_p.rep_page.logMsg('Flags have been modified: ')
                self.flaglist = dlg.newfllist
                self.plotstream = self.plotstream._drop_column('flag')
                self.plotstream = self.plotstream._drop_column('comment')
                self.plotstream = self.plotstream.flag(self.flaglist)
                self.menu_p.rep_page.logMsg('- applied {} modified flags'.format(len(self.flaglist)))
                self.ActivateControls(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)        
            else:
                pass
            pass
        dlg.Destroy()
        self.changeStatusbar("Ready")


    def onSmoothButton(self, event):
        """
        DESCRIPTION
             Calculates smoothed curve
        """
        self.changeStatusbar("Smoothing ... be patient")
        sr = self.plotstream.samplingrate()

        filter_type = 'gaussian'
        resample_offset = 0.0
        if sr < 0.2: # use 1 second filter with 0.3 Hz cut off as default
                filter_width = timedelta(seconds=3.33333333)
                resample_period = 1.0
        elif sr < 50: # use 1 minute filter with 0.008 Hz cut off as default
                filter_width = timedelta(minutes=2)
                resample_period = 60.0
        else: # use 1 hour flat filter
                filter_width = timedelta(minutes=60)
                resample_period = 3600.0
                resample_offset = 1800.0
                filter_type = 'flat'
        miss = 'conservative'

        dlg = AnalysisFilterDialog(None, title='Analysis: Filter', samplingrate=sr, resample=False, winlen=filter_width.seconds, resint=resample_period, resoff= resample_offset, filtertype=filter_type)
        if dlg.ShowModal() == wx.ID_OK:
            filtertype = dlg.filtertypeComboBox.GetValue()
            filterlength = float(dlg.lengthTextCtrl.GetValue())
            missingdata = dlg.methodRadioBox.GetStringSelection()
            if missingdata == 'IAGA':
                miss = 'mean'
            elif missingdata == 'interpolate':
                miss = 'interpolate'

            self.plotstream = self.plotstream.filter(keys=self.shownkeylist,filter_type=filtertype,filter_width=timedelta(seconds=filterlength),missingdata=miss,resample=False,noresample=True)
            self.menu_p.rep_page.logMsg('- data filtered: {} window, {} Hz passband'.format(filtertype,1./filterlength))

            self.ActivateControls(self.plotstream)
            self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")

    def onBaselineButton(self, event):
        """
        DESCRIPTION
             Calculates baseline correction
        """
        self.changeStatusbar("Baseline adoption ...")
        dlg = AnalysisBaselineDialog(None, title='Analysis: Baseline adoption', idxlst=self.baselineidxlst, dictlst = self.baselinedictlst, options=self.options, stream = self.plotstream, shownkeylist=self.shownkeylist, keylist=self.keylist)
        # open dlg which allows to choose baseline data stream, function and parameters
        # Drop down for baseline data stream (idx: filename)
        # Text window describing baseline parameter
        # button to modify baseline parameter
        #print ("BASELINEDICT CoNTENTS:", self.baselinedictlst,self.baselineidxlst)

        if dlg.ShowModal() == wx.ID_OK:
            # return active stream idx ()
            #print ("Here", dlg.absstreamComboBox.GetStringSelection())
            #print ("Here2", dlg.absstreamComboBox.GetValue())
            idx = int(dlg.absstreamComboBox.GetValue().split(':')[0])
            self.options = dlg.options
            absstream = self.streamlist[idx]
            tmpbasedict = [el for el in self.baselinedictlst if el['streamidx']==idx]
            basedict = tmpbasedict[0]
            ## TODO extract all baseline parameters here
            fitfunc = self.options.get('fitfunction','spline')
            if fitfunc.startswith('poly'):
                self.options['fitfunction'] = 'poly'
                fitfunc = 'poly'

            baselinefunc = self.plotstream.baseline(absstream,fitfunc=self.options.get('fitfunction','spline'), knotstep=float(self.options.get('fitknotstep','0.3')), fitdegree=int(self.options.get('fitdegree','5')))
            #keys = self.shownkeylist
            self.menu_p.rep_page.logMsg('- baseline adoption performed using DI data from {}. Parameters: function={}, knotsteps(spline)={}, degree(polynomial)={}'.format(basedict['filename'],self.options.get('fitfunction',''),self.options.get('fitknotstep',''),self.options.get('fitdegree','')))
            # add new stream, with baselinecorr
            # BASECORR
            dlg = wx.MessageDialog(self, "Adopted baseline calculated.\n"
                        "Baseline parameters added to meta information and option 'Baseline Corr' on 'Stream' panel now enabled.\n",
                        "Adopted baseline", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            
            self.ActivateControls(self.plotstream)
            self.OnPlot(self.plotstream,self.shownkeylist)
            self.changeStatusbar("BC function available - Ready")
        else:
            self.changeStatusbar("Ready")

    def onDeltafButton(self, event):
        """
        DESCRIPTION
             Calculates delta F values
        """
        self.changeStatusbar("Delta F ...")

        self.plotstream = self.plotstream.delta_f()
        self.streamlist[self.currentstreamindex].delta_f()
        #print (self.plotstream._get_key_headers())
        if 'df' in self.plotstream._get_key_headers() and not 'df' in self.shownkeylist:
            self.shownkeylist.append('df')
        self.menu_p.rep_page.logMsg('- determined delta F between x,y,z and f')
        self.ActivateControls(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")


    def onCalcfButton(self, event):
        """
        DESCRIPTION
             Calculates delta F values
        """
        self.changeStatusbar("Calculating F from components ...")

        self.plotstream = self.plotstream.calc_f()
        self.streamlist[self.currentstreamindex].calc_f()
        #print (self.plotstream._get_key_headers())
        if 'f' in self.plotstream._get_key_headers() and not 'f' in self.shownkeylist:
            self.shownkeylist.append('f')
        self.menu_p.rep_page.logMsg('- determined f from x,y,z')
        self.ActivateControls(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")

    def onPowerButton(self, event):
        """
        DESCRIPTION
             Calculates Power spectrum of one component
        """
        self.changeStatusbar("Power spectrum ...")

        # Open a dialog for paramater selction
        dlg = SelectFromListDialog(None, title='Select sensor', selectlist=self.shownkeylist, name='Component')
        if dlg.ShowModal() == wx.ID_OK:
            comp = dlg.selectComboBox.GetValue()
        else:
            comp = self.compselect[0]

        import magpy.mpplot as mp
        mp.plotPS(self.plotstream, comp)


    def onSpectrumButton(self, event):
        """
        DESCRIPTION
             Calculates Power spectrum of one component
        """
        self.changeStatusbar("Spectral plot ...")

        dlg = SelectFromListDialog(None, title='Select sensor', selectlist=self.shownkeylist, name='Component')
        if dlg.ShowModal() == wx.ID_OK:
            comp = dlg.selectComboBox.GetValue()
        else:
            comp = self.compselect[0]

        # Open a dialog for paramater selction
        import magpy.mpplot as mp
        mp.plotSpectrogram(self.plotstream, comp)


    # ------------------------------------------------------------------------------------------
    # ################
    # Stream page functions
    # ################
    # ------------------------------------------------------------------------------------------

    def onErrorBarCheckBox(self,event):
        """
        DESCRIPTION
            Switch display of error bars.
        RETURNS
            kwarg for OnPlot method
        """

        if not self.menu_p.str_page.errorBarsCheckBox.GetValue():
            self.errorbars=False
            self.plotopt['errorbars'] = [[False]*len(self.shownkeylist)]
            self.menu_p.str_page.errorBarsCheckBox.SetValue(False)
        else:
            self.errorbars=True
            self.plotopt['errorbars'] = [[True]*len(self.shownkeylist)]
            self.menu_p.str_page.errorBarsCheckBox.SetValue(True)
        self.ActivateControls(self.plotstream)
        if self.plotstream.length()[0] > 0:
            self.OnPlot(self.plotstream,self.shownkeylist)
            self.changeStatusbar("Ready")
        else:
            self.changeStatusbar("Failure")


    def onConfinexCheckBox(self,event):
        """
        DESCRIPTION
            Switch display of error bars.
        RETURNS
            kwarg for OnPlot method
        """
        if not self.menu_p.str_page.confinexCheckBox.GetValue():
            self.confinex=False
            self.plotopt['confinex'] = False
            self.menu_p.str_page.confinexCheckBox.SetValue(False)
        else:
            self.confinex=True
            self.plotopt['confinex'] = True
            self.menu_p.str_page.confinexCheckBox.SetValue(True)
        self.ActivateControls(self.plotstream)
        if self.plotstream.length()[0] > 0:
            self.OnPlot(self.plotstream,self.shownkeylist)
            self.changeStatusbar("Ready")
        else:
            self.changeStatusbar("Failure")


    def onTrimStreamButton(self,event):
        """
        DESCRIPTION
        """
        stday = self.menu_p.str_page.startDatePicker.GetValue()
        sttime = str(self.menu_p.str_page.startTimePicker.GetValue())
        if sttime.endswith('AM') or sttime.endswith('am'):
            sttime = datetime.strftime(datetime.strptime(sttime,"%I:%M:%S %p"),"%H:%M:%S")
        if sttime.endswith('pm') or sttime.endswith('PM'):
            sttime = datetime.strftime(datetime.strptime(sttime,"%I:%M:%S %p"),"%H:%M:%S")
        sd = datetime.strftime(datetime.fromtimestamp(stday.GetTicks()), "%Y-%m-%d")
        start= datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
        enday = self.menu_p.str_page.endDatePicker.GetValue()
        entime = str(self.menu_p.str_page.endTimePicker.GetValue())
        if entime.endswith('AM') or entime.endswith('am'):
            entime = datetime.strftime(datetime.strptime(entime,"%I:%M:%S %p"),"%H:%M:%S")
        if entime.endswith('pm') or entime.endswith('PM'):
            #print ("ENDTime", entime, datetime.strptime(entime,"%I:%M:%S %p"))
            entime = datetime.strftime(datetime.strptime(entime,"%I:%M:%S %p"),"%H:%M:%S")
        ed = datetime.strftime(datetime.fromtimestamp(enday.GetTicks()), "%Y-%m-%d")
        end= datetime.strptime(ed+'_'+entime, "%Y-%m-%d_%H:%M:%S")
        #print ("Range", start, end)

        if end > start:
            try:
                self.changeStatusbar("Trimming stream ...")
                newarray = self.plotstream._select_timerange(starttime=start, endtime=end)
                self.plotstream=DataStream([LineStruct()],self.plotstream.header,newarray)
                self.menu_p.rep_page.logMsg('- Stream trimmed: {} to {}'.format(start,end))
            except:
                self.menu_p.rep_page.logMsg('- Trimming failed')

            self.ActivateControls(self.plotstream)
            if self.plotstream.length()[0] > 0:
                self.OnPlot(self.plotstream,self.shownkeylist)
                self.changeStatusbar("Ready")
            else:
                self.changeStatusbar("Failure")
        else:
            dlg = wx.MessageDialog(self, "Could not trim timerange!\n"
                        "Entered dates are out of order.\n",
                        "TrimTimerange", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Trimming timerange failed ... Ready")
            dlg.Destroy()


    def openStream(self,path='',mintime=None,maxtime=None,extension=None):
        # TODO Move this method to section File menu
        """
        DESCRIPTION:
            Opens time range dialog and loads data. Returns stream.
        USED BY:
            OnOpenDir and OnOpenDB , OnOpen
        """
        dlg = LoadDataDialog(None, title='Select timerange:',mintime=mintime,maxtime=maxtime, extension=extension)
        if dlg.ShowModal() == wx.ID_OK:
            stday = dlg.startDatePicker.GetValue()
            sttime = dlg.startTimePicker.GetValue()
            enday = dlg.endDatePicker.GetValue()
            entime = dlg.endTimePicker.GetValue()
            ext = dlg.fileExt.GetValue()

            sd = datetime.fromtimestamp(stday.GetTicks())
            ed = datetime.fromtimestamp(enday.GetTicks())
            st = datetime.strftime(sd, "%Y-%m-%d") + " " + sttime
            start = datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
            et = datetime.strftime(ed, "%Y-%m-%d") + " " + entime
            end = datetime.strptime(et, "%Y-%m-%d %H:%M:%S")

            dlg.Destroy()

            loadDlg = WaitDialog(None, "Loading...", "Loading data.\nPlease wait....")

            if isinstance(path, basestring):
                if not path=='':
                    self.menu_p.str_page.fileTextCtrl.SetValue(ext)
                    self.changeStatusbar("Loading data ... please be patient")
                    if path.find('//') > 0:
                        stream = read(path_or_url=path, starttime=start, endtime=end)
                    else:
                        stream = read(path_or_url=os.path.join(path,ext), starttime=start, endtime=end)
            else:
                # assume Database
                try:
                    self.changeStatusbar("Loading data ... please be patient")
                    stream = readDB(path[0],path[1], starttime=start, endtime=end)
                except:
                    print ("Reading failed")

            loadDlg.Destroy()            
            return stream

        else:
            return DataStream()


    def onSelectKeys(self,event):
        """
        DESCRIPTION
            open dialog to select shown keys (check boxes)
        """

        self.changeStatusbar("Selecting keys ...")

        if len(self.plotstream.ndarray[0]) == 0:
            self.plotstream = self.stream.copy()
        keylist = self.plotstream._get_key_headers(numerical=True)
        self.keylist = keylist
        shownkeylist = [el for el in self.shownkeylist if el in NUMKEYLIST]

        namelist = []
        unitlist = []
        for key in keylist:
            if not len(self.plotstream.ndarray[KEYLIST.index(key)]) == 0:
                value = self.plotstream.header.get('col-'+key)
                unit = self.plotstream.header.get('unit-col-'+key)
                if not value == '':
                    namelist.append(value)
                else:
                    namelist.append(key)
                if not unit == '':
                    unitlist.append(unit)
                else:
                    unitlist.append('')

        if len(self.plotstream.ndarray[0]) > 0:
            dlg = StreamSelectKeysDialog(None, title='Select keys:',keylst=keylist,shownkeys=self.shownkeylist,namelist=namelist)
            for elem in shownkeylist:
                exec('dlg.'+elem+'CheckBox.SetValue(True)')
            if dlg.ShowModal() == wx.ID_OK:
                shownkeylist = []
                for elem in keylist:
                    boolval = eval('dlg.'+elem+'CheckBox.GetValue()')
                    if boolval:
                        shownkeylist.append(elem)
                if len(shownkeylist) == 0:
                    shownkeylist = self.shownkeylist
                else:
                    self.shownkeylist = shownkeylist
                self.symbollist = [self.symbollist[0]]*len(shownkeylist)
                self.plotopt['symbollist'] =  [self.symbollist[0]]*len(shownkeylist)
                self.ActivateControls(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)
                self.changeStatusbar("Ready")
        else:
            self.changeStatusbar("Failure")


    def onExtractData(self,event):
        """
        DESCRIPTION:
            open dialog to choose extract parameter (paramater compare value)
            up to three possibilities
        """

        if len(self.plotstream.ndarray[0]) == 0:
            self.plotstream = self.stream.copy()
        keylist = self.shownkeylist
        if len(self.plotstream.ndarray[0]) > 0:
            dlg = StreamExtractValuesDialog(None, title='Extract:',keylst=keylist)
            if dlg.ShowModal() == wx.ID_OK:
                key1 = dlg.key1ComboBox.GetValue()
                comp1 = dlg.compare1ComboBox.GetValue()
                val1 = dlg.value1TextCtrl.GetValue()
                logic2 = dlg.logic2ComboBox.GetValue()
                logic3 = dlg.logic3ComboBox.GetValue()
                extractedstream = self.plotstream.extract(key1,val1,comp1)
                if len(extractedstream) < 2 and extractedstream.length()[0] < 2:
                    # Empty stream returned -- looks complex because of old LineStruct rubbish
                    self.menu_p.rep_page.logMsg('Extract: criteria would return an empty data stream - skipping')
                    extractedstream = self.plotstream
                val2 = dlg.value2TextCtrl.GetValue()
                if not val2 == '':
                    key2 = dlg.key2ComboBox.GetValue()
                    comp2 = dlg.compare2ComboBox.GetValue()
                    if logic2 == 'and':
                        extractedstream = extractedstream.extract(key2,val2,comp2)
                    else:
                        extractedstream2 = self.plotstream.extract(key2,val2,comp2)
                        extractedstream.extend(extractedstream2.container, extractedstream2.header,extractedstream2.ndarray)
                        extractedstream = extractedstream.removeduplicates()
                        extractedstream = extractedstream.sorting()
                        extractedstream = extractedstream.get_gaps()
                    val3 = dlg.value3TextCtrl.GetValue()
                    if not val3 == '':
                        key3 = dlg.key3ComboBox.GetValue()
                        comp3 = dlg.compare3ComboBox.GetValue()
                        if logic3 == 'and':
                            extractedstream = extractedstream.extract(key3,val3,comp3)
                        else:
                            extractedstream3 = self.plotstream.extract(key3,val3,comp3)
                            extractedstream.extend(extractedstream3.container, extractedstream3.header,extractedstream3.ndarray)
                            extractedstream = extractedstream.removeduplicates()
                            extractedstream = extractedstream.sorting()
                            extractedstream = extractedstream.get_gaps()
                self.plotstream = extractedstream
                self.ActivateControls(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)
                self.changeStatusbar("Ready")
        else:
            self.menu_p.rep_page.logMsg("Extract: No data available so far")
        # specify filters -> allow to define filters Combo with key - Combo with selector (>,<,=) - TextBox with Filter


    def onChangePlotOptions(self,event):
        """
        DESCRIPTION:
            open dialog to modify plot options (general (e.g. bgcolor) and  key
            specific (key: symbol color errorbar etc)
        """

        if len(self.plotstream.ndarray[0]) > 0:
            dlg = StreamPlotOptionsDialog(None, title='Plot Options:',optdict=self.plotopt)
            if dlg.ShowModal() == wx.ID_OK:
                for elem in self.plotopt:
                    if not elem in ['function']:
                        val = eval('dlg.'+elem+'TextCtrl.GetValue()')
                        if val in ['False','True','None'] or val.startswith('[') or val.startswith('{'):
                            val = eval(val)
                        if elem in ['opacity','bartrange']:
                            val = float(val)
                        if not val == self.plotopt[elem]:
                            self.plotopt[elem] = val

                self.ActivateControls(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)

    def onRestoreData(self,event):
        """
        Restore originally loaded data
        """
        self.flaglist = []

        if not len(self.stream.ndarray[0]) > 0:
            self.DeactivateAllControls()
            self.changeStatusbar("No data available")
            return False
        print ("Restoring (works only for latest stream):", self.currentstreamindex)
        #print ("Header", self.headerlist)
        #self.plotstream = self.streamlist[self.currentstreamindex].copy()
        self.plotstream = self.stream.copy()
        self.plotstream.header = self.headerlist[self.currentstreamindex]

        self.menu_p.rep_page.logMsg('Original data restored...')
        #self.InitPlotParameter()
        #self.ActivateControls(self.stream)
        self.OnInitialPlot(self.stream, restore=True)

    def onDailyMeansButton(self,event):
        """
        Restore originally loaded data
        """
        if self.plotstream.header.get('DataFormat') == 'MagPyDI':
            keys=['dx','dy','dz']
        else:
            keys = False
        self.plotstream = self.plotstream.dailymeans(keys)
        self.shownkeylist = self.plotstream._get_key_headers(numerical=True)[:3]
        self.symbollist = self.symbollist[0]*len(self.shownkeylist)

        self.plotopt['symbollist'] = self.symbollist[0]*len(self.shownkeylist)
        self.plotopt['errorbars'] = [[True]*len(self.shownkeylist)]

        self.ActivateControls(self.plotstream)
        self.errorbars = True
        self.OnPlot(self.plotstream,self.shownkeylist)
        self.menu_p.str_page.errorBarsCheckBox.SetValue(True)
        self.menu_p.str_page.errorBarsCheckBox.Enable()
        self.changeStatusbar("Ready")


    def onApplyBCButton(self,event):
        """
        Apply baselinecorrection
        """
        #print ('self.plotstream', self.plotstream.header.get('DataComponents',''))
        self.plotstream = self.plotstream.bc()
        currentstreamindex = len(self.streamlist)
        self.streamlist.append(self.plotstream)
        self.streamkeylist.append(self.shownkeylist)
        self.headerlist.append(self.plotstream.header)
        self.currentstreamindex = currentstreamindex
        self.plotoptlist.append(self.plotopt)

        #print ('self.plotstream', self.plotstream.header.get('DataComponents',''))
        self.ActivateControls(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)


    def onAnnotateCheckBox(self,event):
        """
        Restore originally loaded data
        """
        #### get True or False
        if not self.menu_p.str_page.annotateCheckBox.GetValue():
            #self.annotate=False
            self.plotopt['annotate'] = False
            self.menu_p.str_page.annotateCheckBox.SetValue(False)
        else:
            #self.annotate=True
            self.plotopt['annotate'] = True
            self.menu_p.str_page.annotateCheckBox.SetValue(True)

        #mp.plot(self.plotstream,annotate=True)
        self.ActivateControls(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)

    def onChangeComp(self, event):
        orgcomp = self.compselect
        self.compselect = self.menu_p.str_page.comp[event.GetInt()]
        coordinate = orgcomp+'2'+self.compselect
        self.changeStatusbar("Transforming ... {}".format(coordinate))
        print("Transforming ... {}".format(coordinate))
        self.plotstream = self.plotstream._convertstream(coordinate)
        self.ActivateControls(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)

    def onChangeSymbol(self, event):
        #orgsymbol = self.symbolselect
        symbolselect = self.menu_p.str_page.symbol[event.GetInt()]
        self.changeStatusbar("Transforming ...")
        self.ActivateControls(self.plotstream)
        #if len(self.plotstream.ndarray[0]) == 0:
        #    self.plotstream = self.stream.copy()
        if symbolselect == 'line':
            self.symbollist = ['-' for elem in self.shownkeylist]
            self.plotopt['symbollist'] =  ['-' for elem in self.shownkeylist]
            self.OnPlot(self.plotstream,self.shownkeylist)
        elif symbolselect == 'point':
            self.symbollist = ['o' for elem in self.shownkeylist]
            self.plotopt['symbollist'] =  ['o' for elem in self.shownkeylist]
            self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")

    def OnFlagClick(self, event):
        """Mouse event for flagging with double click."""
        if not event.inaxes or not event.dblclick:
            return
        else:
            sensid = self.plotstream.header.get('SensorID','')
            dataid = self.plotstream.header.get('DataID','')
            if sensid == '' and not dataid == '':
                sensid = dataid[:-5]
            if sensid == '':
                dlg = wx.MessageDialog(self, "No Sensor ID available!\n"
                                "You need to define a unique Sensor ID\nfor the data set in order to use flagging.\nPlease go the tab Meta for this purpose.\n","Undefined Sensor ID", wx.OK|wx.ICON_INFORMATION)
                dlg.ShowModal()
                dlg.Destroy()
            else:
                flaglist = []
                xdata = self.plot_p.t
                xtol = ((max(xdata) - min(xdata))/float(len(xdata)))/2
                pickX = event.xdata
                idx = (np.abs(xdata - pickX)).argmin()
                time = self.plotstream.ndarray[KEYLIST.index('time')][idx]
                starttime = num2date(time - xtol)
                endtime = num2date(time + xtol)
                print ("Double click disabled because of freezing")
                """
                print ("Opening Dialog")
                dlg = StreamFlagSelectionDialog(None, title='Stream: Flag Selection', shownkeylist=self.shownkeylist, keylist=self.keylist)
                print ("Waiting for OK ...")
                if dlg.ShowModal() == wx.ID_OK:
                    keys2flag = dlg.AffectedKeysTextCtrl.GetValue()
                    keys2flag = keys2flag.split(',')
                    keys2flag = [el for el in keys2flag if el in KEYLIST]
                    flagid = dlg.FlagIDComboBox.GetValue()
                    flagid = int(flagid[0])
                    comment = dlg.CommentTextCtrl.GetValue()
                    if comment == '' and flagid != 0:
                        comment = 'Point flagged with unspecified reason'
                    flaglist = self.plotstream.flag_range(keys=self.shownkeylist,flagnum=flagid,text=comment,keystoflag=keys2flag,starttime=starttime,endtime=endtime)
                    self.menu_p.rep_page.logMsg('- flagged time range: added {} flags'.format(len(flaglist)))
                if len(flaglist) > 0:
                    self.flaglist.extend(flaglist)
                    self.plotstream = self.plotstream.flag(flaglist)
                    self.ActivateControls(self.plotstream)
                    self.plotopt['annotate'] = True
                    self.menu_p.str_page.annotateCheckBox.SetValue(True)
                    self.OnPlot(self.plotstream,self.shownkeylist)
                self.changeStatusbar("Ready")
                """

    def onFlagSelectionButton(self,event):
        """
        DESCRIPTION
            Flag all data within the zoomed region
        """

        flaglist = []
        sensid = self.plotstream.header.get('SensorID','')
        dataid = self.plotstream.header.get('DataID','')
        if sensid == '' and not dataid == '':
            sensid = dataid[:-5]

        if self.flaglist and len(self.flaglist)>0:
            dlg = wx.MessageDialog(self, 'Unsaved flagging information in systems memory. If you want to keep and extend this data with new flags select \n YES \n or to discard it starting with fresh flags select \n NO', 'Flags', wx.YES_NO | wx.ICON_QUESTION)
            if dlg.ShowModal() == wx.ID_NO:
                self.flaglist = []
                self.plotstream = self.plotstream._drop_column('flag')
                self.plotstream = self.plotstream._drop_column('comment')
            dlg.Destroy()

        self.xlimits = self.plot_p.xlimits
        self.ylimits = self.plot_p.ylimits
        selplt = self.plot_p.selplt
        selkey=[self.shownkeylist[selplt]] # Get the marked key here

        if sensid == '':
            dlg = wx.MessageDialog(self, "No Sensor ID available!\n"
                            "You need to define a unique Sensor ID\nfor the data set in order to use flagging.\nPlease go the tab Meta for this purpose.\n","Undefined Sensor ID", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
        else:
            self.changeStatusbar("Flagging selection ...")
            dlg = StreamFlagSelectionDialog(None, title='Stream: Flag Selection', shownkeylist=self.shownkeylist, keylist=self.keylist)
            if dlg.ShowModal() == wx.ID_OK:
                keys2flag = dlg.AffectedKeysTextCtrl.GetValue()
                keys2flag = keys2flag.split(',')
                keys2flag = [el for el in keys2flag if el in KEYLIST]
                comment = dlg.CommentTextCtrl.GetValue()
                flagid = dlg.FlagIDComboBox.GetValue()
                flagid = int(flagid[0])

                above = min(self.ylimits)
                below = max(self.ylimits)
                starttime =num2date(min(self.xlimits))
                endtime = num2date(max(self.xlimits))

                print ("FlagID:", flagid)
                flaglist = self.plotstream.flag_range(keys=selkey,flagnum=flagid,text=comment,keystoflag=keys2flag,starttime=starttime,endtime=endtime,above=above,below=below)
                self.menu_p.rep_page.logMsg('- flagged selection: added {} flags'.format(len(flaglist)))

        if len(flaglist) > 0:
            #print ("FlagRange: Please note that the range definition needs an update as only single values are considered")
            #print ("TEst", flaglist)
            self.flaglist.extend(flaglist)
            self.plotstream = self.plotstream.flag(flaglist)

            self.ActivateControls(self.plotstream)
            #self.annotate = True
            self.plotopt['annotate'] = True

            self.menu_p.str_page.annotateCheckBox.SetValue(True)
            self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")

        """
        #dlg = StreamFlagSelectionDialog(None, title='Stream: Flag selection ...')

        #prev_redir = sys.stdout
        #redir=RedirectText(dlg.SelectionTextCtrl)
        #sys.stdout=redir
        ###   commands
        #sys.stdout=prev_redir

        self.changeStatusbar("Opening external data viewer ...")
        self.plot_p.plt.close()
        variables = self.keylist

        #p = subprocess.Popen(['ls', '-a'], stdout = subprocess.PIPE)
        #text = p.stdout.readlines()
        #text = "".join(text)

        self.plotstream, flaglist = mp.plotFlag(self.plotstream,variables)
        self.flaglist.extend(flaglist)

        self.changeStatusbar("Updating plot ...")
        self.menu_p.rep_page.logMsg('- flagged user selection: added {} flags'.format(len(flaglist)))
        self.ActivateControls(self.plotstream)

        #self.annotate = True
        self.plotopt['annotate'] = True

        self.menu_p.str_page.annotateCheckBox.SetValue(True)
        self.OnPlot(self.plotstream,self.shownkeylist)
        """

    def onFlagClearButton(self, event):
        """
        DESCRIPTION
            Clear current flaglist
        """
        self.changeStatusbar("Deleting flaglist ...")
        self.flaglist = []
        self.plotstream = self.plotstream._drop_column('flag')
        self.plotstream = self.plotstream._drop_column('comment')
        self.ActivateControls(self.plotstream)
        self.plotopt['annotate'] = False
        self.menu_p.str_page.annotateCheckBox.SetValue(False)
        self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")


    def onFlagOutlierButton(self, event):
        """
        DESCRIPTION
            Method for Outlier
        """
        self.changeStatusbar("Flagging outliers ...")
        sr = self.menu_p.met_page.samplingrateTextCtrl.GetValue().encode('ascii','ignore')
        keys = self.shownkeylist
        timerange = float(sr)*600.
        threshold=5.0
        markall = False

        if self.flaglist and len(self.flaglist)>0:
            dlg = wx.MessageDialog(self, 'Unsaved flagging information in systems memory. If you want to keep and extend this data with new flags select \n YES \n or to discard it starting with fresh flags select \n NO', 'Flags', wx.YES_NO | wx.ICON_QUESTION)
            if dlg.ShowModal() == wx.ID_NO:
                self.flaglist = []
                self.plotstream = self.plotstream._drop_column('flag')
                self.plotstream = self.plotstream._drop_column('comment')
            dlg.Destroy()

        # Open Dialog and return the parameters threshold, keys, timerange
        dlg = StreamFlagOutlierDialog(None, title='Stream: Flag outlier', threshold=threshold, timerange=timerange)
        if dlg.ShowModal() == wx.ID_OK:
            threshold = dlg.ThresholdTextCtrl.GetValue()
            timerange = dlg.TimerangeTextCtrl.GetValue()
            markall = dlg.MarkAllCheckBox.GetValue()
            try:
                threshold = float(threshold)
                timerange = float(timerange)
                timerange = timedelta(seconds=timerange)
                flaglist = self.plotstream.flag_outlier(stdout=True,returnflaglist=True, keys=keys,threshold=threshold,timerange=timerange,markall=markall)
                self.flaglist.extend(flaglist)
                #self.plotstream = self.plotstream.flag_outlier(stdout=True, keys=keys,threshold=threshold,timerange=timerange)
                self.menu_p.rep_page.logMsg('- flagged outliers: added {} flags'.format(len(flaglist)))
                if markall:
                    self.menu_p.rep_page.logMsg('- flagged outliers: used option markall')
            except:
                print("flag outliers failed: check parameter")
                self.menu_p.rep_page.logMsg('- flag outliers failed: check parameter')

            self.ActivateControls(self.plotstream)
            #self.annotate = True
            self.plotopt['annotate'] = True

            self.menu_p.str_page.annotateCheckBox.SetValue(True)
            self.OnPlot(self.plotstream,self.shownkeylist)

        self.changeStatusbar("Ready")


    def onFlagRangeButton(self,event):
        """
        DESCRIPTION
            Opens a dialog which allows to select the range to be flagged
        """
        flaglist = []
        sensid = self.plotstream.header.get('SensorID','')
        dataid = self.plotstream.header.get('DataID','')
        if sensid == '' and not dataid == '':
            sensid = dataid[:-5]

        if self.flaglist and len(self.flaglist)>0:
            dlg = wx.MessageDialog(self, 'Unsaved flagging information in systems memory. If you want to keep and extend this data with new flags select \n YES \n or to discard it starting with fresh flags select \n NO', 'Flags', wx.YES_NO | wx.ICON_QUESTION)
            if dlg.ShowModal() == wx.ID_NO:
                self.flaglist = []
                self.plotstream = self.plotstream._drop_column('flag')
                self.plotstream = self.plotstream._drop_column('comment')
            dlg.Destroy()

        self.xlimits = self.plot_p.xlimits

        if sensid == '':
            dlg = wx.MessageDialog(self, "No Sensor ID available!\n"
                            "You need to define a unique Sensor ID\nfor the data set in order to use flagging.\nPlease go the tab Meta for this purpose.\n","Undefined Sensor ID", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
        else:
            self.changeStatusbar("Flagging range ...")
            dlg = StreamFlagRangeDialog(None, title='Stream: Flag range', stream = self.plotstream, shownkeylist=self.shownkeylist, keylist=self.keylist)
            startdate=self.xlimits[0]
            enddate=self.xlimits[1]
            starttime = num2date(startdate).strftime('%X')
            endtime = num2date(enddate).strftime('%X')
            dlg.startFlagDatePicker.SetValue(pydate2wxdate(num2date(startdate)))
            dlg.endFlagDatePicker.SetValue(pydate2wxdate(num2date(enddate)))
            dlg.startFlagTimePicker.SetValue(starttime)
            dlg.endFlagTimePicker.SetValue(endtime)
            if dlg.ShowModal() == wx.ID_OK:
                # get values from dlg
                flagtype = dlg.rangeRadioBox.GetStringSelection()
                keys2flag = dlg.AffectedKeysTextCtrl.GetValue()
                keys2flag = keys2flag.split(',')
                keys2flag = [el for el in keys2flag if el in KEYLIST]
                comment = dlg.CommentTextCtrl.GetValue()
                flagid = dlg.FlagIDComboBox.GetValue()
                flagid = int(flagid[0])
                if flagtype == 'value':
                     keys = str(dlg.SelectKeyComboBox.GetValue())
                     above = dlg.LowerLimitTextCtrl.GetValue()
                     below = dlg.UpperLimitTextCtrl.GetValue()
                     flagval = True
                     if not below == '' and not above == '':
                         above = float(above)
                         below = float(below)
                         #below = None
                         self.menu_p.rep_page.logMsg('- flagging values between {} and {}'.format(above, below))
                     elif not below == '':
                         below = float(below)
                         above = None
                         self.menu_p.rep_page.logMsg('- flagging values below {}'.format(below))
                     elif not above == '':
                         above = float(above)
                         below = None
                         self.menu_p.rep_page.logMsg('- flagging values above {}'.format(above))
                     else:
                         flagval = False
                     if flagval:
                         #print ("Above , Below:", above, below)
                         flaglist = self.plotstream.flag_range(keys=[keys],flagnum=flagid,text=comment,keystoflag=keys2flag,above=above,below=below)
                         self.menu_p.rep_page.logMsg('- flagged value range: added {} flags'.format(len(flaglist)))
                elif flagtype == 'time':
                     if comment == '':
                         comment = 'Time range flagged with unspecified reason'
                     stday = dlg.startFlagDatePicker.GetValue()
                     sttime = str(dlg.startFlagTimePicker.GetValue())
                     if sttime.endswith('AM') or sttime.endswith('am'):
                         sttime = datetime.strftime(datetime.strptime(sttime,"%I:%M:%S %p"),"%H:%M:%S")
                     if sttime.endswith('pm') or sttime.endswith('PM'):
                         sttime = datetime.strftime(datetime.strptime(sttime,"%I:%M:%S %p"),"%H:%M:%S")
                     sd = datetime.strftime(datetime.fromtimestamp(stday.GetTicks()), "%Y-%m-%d")
                     starttime= datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
                     enday = dlg.endFlagDatePicker.GetValue()
                     entime = str(dlg.endFlagTimePicker.GetValue())
                     if entime.endswith('AM') or entime.endswith('am'):
                         entime = datetime.strftime(datetime.strptime(entime,"%I:%M:%S %p"),"%H:%M:%S")
                     if entime.endswith('pm') or entime.endswith('PM'):
                         entime = datetime.strftime(datetime.strptime(entime,"%I:%M:%S %p"),"%H:%M:%S")
                     ed = datetime.strftime(datetime.fromtimestamp(enday.GetTicks()), "%Y-%m-%d")
                     endtime= datetime.strptime(str(ed)+'_'+entime, "%Y-%m-%d_%H:%M:%S")
                     #print ("Range", starttime, endtime, keys2flag)
                     flaglist = self.plotstream.flag_range(keys=self.shownkeylist,flagnum=flagid,text=comment,keystoflag=keys2flag,starttime=starttime,endtime=endtime)
                     self.menu_p.rep_page.logMsg('- flagged time range: added {} flags'.format(len(flaglist)))
                else:
                     pass

        if len(flaglist) > 0:
            #print ("FlagRange: Please note that the range definition needs an update as only single values are considered")
            #print ("TEst", flaglist)
            self.flaglist.extend(flaglist)
            self.plotstream = self.plotstream.flag(flaglist)

            self.ActivateControls(self.plotstream)
            #self.annotate = True
            self.plotopt['annotate'] = True

            self.menu_p.str_page.annotateCheckBox.SetValue(True)
            self.OnPlot(self.plotstream,self.shownkeylist)

        self.changeStatusbar("Ready")


    def onFlagLoadButton(self,event):
        """
        DESCRIPTION
            Opens a dialog which allows to load flags either from a DB or from file
        """
        # Check whether DB still available
        self.checkDB('minimal')

        sensorid = self.plotstream.header.get('SensorID','')
        # Open Dialog and return the parameters threshold, keys, timerange
        self.changeStatusbar("Loading flags ... please be patient")
        dlg = StreamLoadFlagDialog(None, title='Load Flags', db = self.db, sensorid=sensorid, start=self.plotstream.start(),end=self.plotstream.end())
        dlg.ShowModal()
        if len(dlg.flaglist) > 0:
            flaglist = dlg.flaglist
            #print ("Loaded flags like", flaglist[0], self.flaglist[0])
            self.flaglist.extend(flaglist)
            #print ("extended flaglist looking like", self.flaglist[0])
            self.changeStatusbar("Applying flags ... please be patient")
            self.plotstream = self.plotstream.flag(flaglist)
            self.menu_p.rep_page.logMsg('- loaded flags: added {} flags'.format(len(flaglist)))

            self.ActivateControls(self.plotstream)
            #self.annotate = True
            self.plotopt['annotate'] = True

            #self.menu_p.str_page.annotateCheckBox.SetValue(False)
            self.OnPlot(self.plotstream,self.shownkeylist)

        self.changeStatusbar("Ready")


    def onFlagSaveButton(self,event):
        """
        DESCRIPTION
            Opens a dialog which allows to save flags either to DB or to file
        """
        # Check whether DB still available
        self.checkDB('minimal')

        currentlen = len(self.flaglist)

        #print ("FlagSave", self.flaglist)

        self.changeStatusbar("Saving flags ...")
        dlg = StreamSaveFlagDialog(None, title='Save Flags', db = self.db, flaglist=self.flaglist)
        if dlg.ShowModal() == wx.ID_OK:
            #flaglist = dlg.flaglist
            pass

        #self.flaglist = []
        self.changeStatusbar("Flaglist saved and reset - Ready")


    def onFlagDropButton(self,event):
        """
        DESCRIPTION
            Drops all flagged data
        """
        self.changeStatusbar("Dropping flagged data ...")

        #dlg = wx.MessageDialog(self, "Please select:\n"
        #       "Yes: drop data from all columns\nNo: drop only selected data\n","Drop", wx.YES_NO |wx.ICON_INFORMATION)
        #if dlg.ShowModal() == wx.ID_YES:
        #    self.plotstream = self.plotstream.flag(self.shownkeylist)
        #else:
        self.plotstream = self.plotstream.remove_flagged()
        flagid = KEYLIST.index('flag')
        check = [el for el in self.plotstream.ndarray[flagid] if '0' in el or '2' in el or '4' in el]
        if not len(check) > 0:
           self.plotstream = self.plotstream._drop_column('flag')
           self.plotstream = self.plotstream._drop_column('comment')
           #self.plotopt['annotate'] = False
        else:
           pass
           #self.plotopt['annotate'] = True

        self.menu_p.rep_page.logMsg('- flagged data removed')

        self.flaglist = []
        self.ActivateControls(self.plotstream)

        self.OnPlot(self.plotstream,self.shownkeylist)

        self.changeStatusbar("Ready")

    def onFlagMinButton(self,event):
        """
        DESCRIPTION
            Flags minimum value in zoomed region
        """
        if self.flaglist and len(self.flaglist)>0:
            dlg = wx.MessageDialog(self, 'Unsaved flagging information in systems memory. If you want to keep and extend this data with new flags select \n YES \n or to discard it starting with fresh flags select \n NO', 'Flags', wx.YES_NO | wx.ICON_QUESTION)
            if dlg.ShowModal() == wx.ID_NO:
                self.flaglist = []
                self.plotstream = self.plotstream._drop_column('flag')
                self.plotstream = self.plotstream._drop_column('comment')
            dlg.Destroy()

        keys = self.shownkeylist
        teststream = self.plotstream.copy()
        # limits
        self.xlimits = self.plot_p.xlimits
        if not self.xlimits == [self.plotstream.ndarray[0],self.plotstream.ndarray[-1]]:
            testarray = self.plotstream._select_timerange(starttime=self.xlimits[0],endtime=self.xlimits[1])
            teststream = DataStream([LineStruct()],self.plotstream.header,testarray)
        xdata = self.plot_p.t
        xtol = ((max(xdata) - min(xdata))/float(len(xdata)))/2
        mini = [teststream._get_min(key,returntime=True) for key in keys]
        flaglist = []
        comment = 'Flagged minimum'
        flagid = self.menu_p.fla_page.FlagIDComboBox.GetValue()
        flagid = int(flagid[0])
        if flagid is 0:
            comment = ''
        for idx,me in enumerate(mini):
            if not keys[idx] == 'df':
                checkbox = getattr(self.menu_p.fla_page, keys[idx] + 'CheckBox')
                if checkbox.IsChecked():
                    starttime = num2date(me[1] - xtol)
                    endtime = num2date(me[1] + xtol)
                    flaglist.extend(self.plotstream.flag_range(keys=self.shownkeylist,flagnum=flagid,text=comment,keystoflag=keys[idx],starttime=starttime,endtime=endtime))
        if len(flaglist) > 0:
            self.menu_p.rep_page.logMsg('- flagged minimum: added {} flags'.format(len(flaglist)))
            self.flaglist.extend(flaglist)
            self.plotstream = self.plotstream.flag(flaglist)
            self.ActivateControls(self.plotstream)
            self.plotopt['annotate'] = True
            self.menu_p.str_page.annotateCheckBox.SetValue(True)
            self.OnPlot(self.plotstream,self.shownkeylist)

    def onFlagMaxButton(self,event):
        """
        DESCRIPTION
            Flags maximum value in zoomed region
        """
        if self.flaglist and len(self.flaglist)>0:
            dlg = wx.MessageDialog(self, 'Unsaved flagging information in systems memory. If you want to keep and extend this data with new flags select \n YES \n or to discard it starting with fresh flags select \n NO', 'Flags', wx.YES_NO | wx.ICON_QUESTION)
            if dlg.ShowModal() == wx.ID_NO:
                self.flaglist = []
                self.plotstream = self.plotstream._drop_column('flag')
                self.plotstream = self.plotstream._drop_column('comment')
            dlg.Destroy()

        keys = self.shownkeylist
        teststream = self.plotstream.copy()
        # limits
        self.xlimits = self.plot_p.xlimits
        if not self.xlimits == [self.plotstream.ndarray[0],self.plotstream.ndarray[-1]]:
            testarray = self.plotstream._select_timerange(starttime=self.xlimits[0],endtime=self.xlimits[1])
            teststream = DataStream([LineStruct()],self.plotstream.header,testarray)
        xdata = self.plot_p.t
        xtol = ((max(xdata) - min(xdata))/float(len(xdata)))/2
        maxi = [teststream._get_max(key,returntime=True) for key in keys]
        flaglist = []
        comment = 'Flagged maximum'
        flagid = self.menu_p.fla_page.FlagIDComboBox.GetValue()
        flagid = int(flagid[0])
        if flagid is 0:
            comment = ''
        for idx,me in enumerate(maxi):
            if not keys[idx] == 'df':
                checkbox = getattr(self.menu_p.fla_page, keys[idx] + 'CheckBox')
                if checkbox.IsChecked():
                    starttime = num2date(me[1] - xtol)
                    endtime = num2date(me[1] + xtol)
                    flaglist.extend(self.plotstream.flag_range(keys=self.shownkeylist,flagnum=flagid,text=comment,keystoflag=keys[idx],starttime=starttime,endtime=endtime))
        if len(flaglist) > 0:
            self.menu_p.rep_page.logMsg('- flagged maximum: added {} flags'.format(len(flaglist)))
            self.flaglist.extend(flaglist)
            self.plotstream = self.plotstream.flag(flaglist)
            self.ActivateControls(self.plotstream)
            self.plotopt['annotate'] = True
            self.menu_p.str_page.annotateCheckBox.SetValue(True)
            self.OnPlot(self.plotstream,self.shownkeylist)

    # ------------------------------------------------------------------------------------------
    # ################
    # Meta page functions
    # ################
    # ------------------------------------------------------------------------------------------


    def onMetaGetDBButton(self,event):
        # TODO Move to Meta page
        """
        DESCRIPTION
            get Meta data for the current sensorid from database
        """
        # Test whether DB is still connected
        self.checkDB('minimal')

        # open dialog with all header info
        dataid = self.plotstream.header.get('DataID','')
        if dataid == '':
            dlg = wx.MessageDialog(self, "No Data ID available!\n"
                            "You need to specify a unique Data ID\nfor which meta information is obtained.\n","Undefined Data ID", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            self.menu_p.rep_page.logMsg(" - failed to add meta information from DB")
        else:
            self.plotstream.header = dbfields2dict(self.db,dataid)
            self.menu_p.rep_page.logMsg(" - added meta information for {} from DB".format(dataid))
            self.ActivateControls(self.plotstream)


    def onMetaPutDBButton(self,event):
        """
        DESCRIPTION
            write meta data to the database
        """
        # Check whether DB still available
        self.checkDB('minimal')

        # open dialog with all header info
        dataid = self.plotstream.header.get('DataID','')
        if dataid == '':
            dlg = wx.MessageDialog(self, "No Data ID available!\n"
                            "You need to specify a unique Data ID\nfor which meta information is stored.\n","Undefined Data ID", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            self.menu_p.rep_page.logMsg(" - failed to add meta information to DB")
        else:
            dlg = wx.MessageDialog(self, "Please confirm!\n"
                            "I want to replace the Meta information\nfrom the DB with data provided.\n","Confirm", wx.YES_NO |wx.ICON_INFORMATION)
            if dlg.ShowModal() == wx.ID_YES:
                dbdict2fields(self.db,self.plotstream.header)
                self.menu_p.rep_page.logMsg(" - added meta information for {} to DB".format(dataid))
            self.ActivateControls(self.plotstream)

    def onMetaDataButton(self,event):
        """
        DESCRIPTION
            open dialog to modify plot options (general (e.g. bgcolor) and  key
            specific (key: symbol color errorbar etc)
        """
        # open dialog with all header info
        if len(self.plotstream.ndarray[0]) > 0:
            dlg = MetaDataDialog(None, title='Meta information:',header=self.plotstream.header,layer='DATAINFO')
            if dlg.ShowModal() == wx.ID_OK:
                for key in DATAINFOKEYLIST:
                    exec('value = dlg.panel.'+key+'TextCtrl.GetValue()')
                    if not value == dlg.header.get(key,''):
                        self.plotstream.header[key] = value
                self.ActivateControls(self.plotstream)
        else:
            self.menu_p.rep_page.logMsg("Meta information: No data available")


    def onMetaSensorButton(self,event):
        # TODO Move to Meta page
        """
        DESCRIPTION
        open dialog to modify plot options (general (e.g. bgcolor) and  key
        specific (key: symbol color errorbar etc)
        """
        # open dialog with all header info
        if len(self.plotstream.ndarray[0]) > 0:
            dlg = MetaDataDialog(None, title='Meta information:',header=self.plotstream.header,layer='SENSORS')
            if dlg.ShowModal() == wx.ID_OK:
                for key in SENSORSKEYLIST:
                    exec('value = dlg.panel.'+key+'TextCtrl.GetValue()')
                    if not value == dlg.header.get(key,''):
                        self.plotstream.header[key] = value
                self.ActivateControls(self.plotstream)

        else:
            self.menu_p.rep_page.logMsg("Meta information: No data available")


    def onMetaStationButton(self,event):
        # TODO Move to Meta page
        """
        DESCRIPTION
        open dialog to modify plot options (general (e.g. bgcolor) and  key
        specific (key: symbol color errorbar etc)
        """
        # open dialog with all header info
        if len(self.plotstream.ndarray[0]) > 0:
            dlg = MetaDataDialog(None, title='Meta information:',header=self.plotstream.header,layer='STATIONS')
            if dlg.ShowModal() == wx.ID_OK:
                for key in STATIONSKEYLIST:
                    exec('value = dlg.panel.'+key+'TextCtrl.GetValue()')
                    if not value == dlg.header.get(key,''):
                        self.plotstream.header[key] = value
                self.ActivateControls(self.plotstream)
        else:
            self.menu_p.rep_page.logMsg("Meta information: No data available")

    # ------------------------------------------------------------------------------------------
    # ####################
    # Stream Operations functions
    # ####################
    # ------------------------------------------------------------------------------------------

    def OnStreamList(self,event):
        """
        DESCRIPTION
        open dialog to select active stream
        """
        plotstreamlist = []
        plotkeylist = []
        self.changeStatusbar("Selecting streams ...")

        dlg = MultiStreamDialog(None, title='Select stream(s):',streamlist=self.streamlist, idx=self.currentstreamindex, streamkeylist=self.streamkeylist)
        if dlg.ShowModal() == wx.ID_OK:
            namelst = dlg.panel.namelst
            for idx, elem in enumerate(self.streamlist):
                val = eval('dlg.panel.'+namelst[idx]+'CheckBox.GetValue()')
                if val:
                    plotstreamlist.append(elem)
                    plotkeylist.append(dlg.panel.streamkeylist[idx])
                    activeidx = idx
            if len(plotstreamlist) > 1:
                #  deactivate all Meta; Analysis methods
                self.DeactivateAllControls()
                self.OnMultiPlot(plotstreamlist,plotkeylist)
            else:
                self.currentstreamindex = activeidx
                self.plotstream = plotstreamlist[0]
                self.shownkeylist = [el for el in plotkeylist[0] if el in NUMKEYLIST]
                #self.shownkeylist = self.streamkeylist[activeidx]
                self.plotopt = self.plotoptlist[activeidx]
                self.ActivateControls(self.plotstream)
                self.OnPlot(self.plotstream,self.shownkeylist)
        else:
            mod = dlg.panel.modify
            if mod == True:
                self.streamlist.append(dlg.panel.result)
                self.streamkeylist.append(dlg.panel.resultkeys)
                self.currentstreamindex = len(self.streamlist)-1
                self.plotstream = self.streamlist[-1]
                self.headerlist.append(self.plotstream.header)
                self.shownkeylist = self.plotstream._get_key_headers(numerical=True)
                self.ActivateControls(self.plotstream)
                self.plotoptlist.append(self.plotopt)
                self.OnPlot(self.plotstream,self.shownkeylist)
        dlg.Destroy()

    def OnStreamAdd(self,event):
        currentstreamindex = len(self.streamlist)
        self.streamlist.append(self.plotstream)
        self.streamkeylist.append(self.shownkeylist)
        self.headerlist.append(self.plotstream.header)
        self.currentstreamindex = currentstreamindex
        self.plotoptlist.append(self.plotopt)


    # ------------------------------------------------------------------------------------------
    # ################
    # Absolute functions
    # ################
    # ------------------------------------------------------------------------------------------


    def onLoadDI(self,event):
        """
        open dialog to load DI data
        """
        if isinstance(self.dipathlist, str):
            dipathlist = self.dipathlist
        else:
            dipathlist = self.dipathlist[0]
        if os.path.isfile(dipathlist):
            dipathlist = os.path.split(dipathlist)[0]

        dlg = LoadDIDialog(None, title='Get DI data', dirname=dipathlist)
        dlg.ShowModal()
        if not dlg.pathlist == 'None' and not len(dlg.pathlist) == 0:
            self.menu_p.rep_page.logMsg("- loaded DI data")
            self.menu_p.abs_page.diTextCtrl.SetValue(','.join(dlg.pathlist))
            self.dipathlist = dlg.pathlist
            if os.path.isfile(dlg.pathlist[0]):
                dlgpath = os.path.split(dlg.pathlist[0])[0]
            else:
                dlgpath = dlg.pathlist[0]
            self.options['dipathlist'] = [dlgpath]
            self.menu_p.abs_page.AnalyzeButton.Enable()
        dlg.Destroy()


    def onDefineVario(self,event):
        """
        open dialog to load DI data
        """
        if len(self.stream) > 0:
            pass
            # send a message box that this data will be erased

        #self.variopath = ''
        divariopath = self.options.get('divariopath','')
        # Open a select path dlg as long as db and remote is not supported
        dialog = wx.DirDialog(None, "Choose a directory with variometer data:",divariopath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            path = dialog.GetPath()
            self.menu_p.abs_page.varioTextCtrl.SetValue(path)
            self.options['divariopath'] = path
        dialog.Destroy()
        # Select an extension as well

    def onDefineScalar(self,event):
        """
        open dialog to load DI data
        """
        if len(self.stream) > 0:
            pass
            # send a message box that this data will be erased
        # Open a select path dlg as long as db and remote is not supported
        discalarpath = self.options.get('discalarpath','')
        dialog = wx.DirDialog(None, "Choose a directory with scalar data:",discalarpath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            path = dialog.GetPath()
            self.menu_p.abs_page.scalarTextCtrl.SetValue(path)
            self.options['discalarpath'] = path
        dialog.Destroy()

    def onDIAnalyze(self,event):
        """
        open dialog to load DI data
        """
        # Get parameters from options
        divariopath = self.options.get('divariopath','')
        vext = self.menu_p.abs_page.varioextTextCtrl.GetValue()
        #if vext not in ['*.*','*.BIN','*.bin','*.sec','*.SEC','*.min','*.MIN','*.cdf','*.CDF']:
        #    vext = '*'
        divariopath = divariopath.replace('*','')
        divariopath = os.path.join(divariopath,vext)
            
        discalarpath = self.options.get('discalarpath','')
        sext = self.menu_p.abs_page.scalarextTextCtrl.GetValue()
        #if sext not in ['*.*','*.BIN','*.bin','*.sec','*.SEC','*.min','*.MIN','*.cdf','*.CDF']:
        #    sext = '*'
        discalarpath = discalarpath.replace('*','')
        discalarpath = os.path.join(discalarpath,sext)

        stationid= self.options.get('stationid','')
        abstype= self.options.get('ditype','')
        azimuth= self.options.get('diazimuth','')
        try:
            expD= float(self.options.get('diexpD','0.0'))
        except:
            expD = 0.0
        try:
            expI= float(self.options.get('diexpI','0.0'))
        except:
            expI = 0.0
        try:
            alpha= float(self.options.get('dialpha','0.0'))
        except:
            alpha = 0.0
        try:
            beta= float(self.options.get('dibeta','0.0'))
        except:
            beta = 0.0
        try:
            deltaF= float(self.options.get('dideltaF','0.0'))
        except:
            deltaF = 0.0
        try:
            deltaD= float(self.options.get('dideltaD','0.0'))
        except:
            deltaD = 0.0
        try:
            deltaI= float(self.options.get('dideltaI','0.0'))
        except:
            deltaI = 0.0

        if len(self.dipathlist) > 0:
            self.changeStatusbar("Processing DI data ... please be patient")
            #absstream = absoluteAnalysis(self.dipathlist,self.divariopath,self.discalarpath, expD=self.diexpD,expI=self.diexpI,diid=self.diid,stationid=self.stationid,abstype=self.ditype, azimuth=self.diazimuth,pier=self.dipier,alpha=self.dialpha,deltaF=self.dideltaF, dbadd=self.didbadd)
            prev_redir = sys.stdout
            redir=RedirectText(self.menu_p.abs_page.dilogTextCtrl)
            sys.stdout=redir

            if not azimuth == '':
                azimuth = float(azimuth)
                absstream = absoluteAnalysis(self.dipathlist,divariopath,discalarpath, expD=expD,expI=expI,stationid=stationid,abstype=abstype, azimuth=azimuth,alpha=alpha,beta=beta,deltaD=deltaD,deltaI=deltaI,deltaF=deltaF)
            else:
                absstream = absoluteAnalysis(self.dipathlist,divariopath,discalarpath, expD=expD,expI=expI,stationid=stationid,alpha=alpha,beta=beta,deltaD=deltaD,deltaI=deltaI,deltaF=deltaF)

            try:
                if not divariopath == '' and not discalapath == '': 
                    variid = absstream.header.get('SensorID').split('_')[1]
                    scalid = absstream.header.get('SensorID').split('_')[2]
                    msgtxt = ''
                    if variid == 'None' or variid == 'Unkown':
                        msgtxt = 'variometer'
                        if scalid == 'None' or scalid == 'Unkown':
                            msgtxt = 'variometer and scalar magnetometer'
                    elif scalid == 'None' or scalid == 'Unkown':
                        msgtxt = 'scalar magnetometer'
                    if not msgtxt == '':
                        fulltxt = "Could not identify {} data.\n Please check paths.".format(msgtxt)
                        dlg = wx.MessageDialog(self, fulltxt, "Data paths", wx.OK|wx.ICON_INFORMATION)
                        dlg.ShowModal()
                        dlg.Destroy()
            except:
               pass

            sys.stdout=prev_redir
            # only if more than one point is selected
            self.changeStatusbar("Ready")
            if absstream and len(absstream.length()) > 1 and absstream.length()[0] > 0:
                # Convert absstream
                array = [[] for el in KEYLIST]
                for idx,el in enumerate(absstream.ndarray):
                    if KEYLIST[idx] in NUMKEYLIST or KEYLIST[idx] == 'time':
                        array[idx] = np.asarray(el).astype(float)
                    else:
                        array[idx] = np.asarray(el)
                absstream.ndarray = np.asarray(array)
                self.stream = absstream
                self.plotstream = absstream
                currentstreamindex = len(self.streamlist)
                self.streamlist.append(self.stream)
                self.streamkeylist.append(absstream._get_key_headers())
                self.headerlist.append(self.stream.header)
                self.currentstreamindex = currentstreamindex
                #self.ActivateControls(self.plotstream)
                self.OnInitialPlot(self.plotstream)
                #self.plotoptlist.append(self.plotopt)
            else:
                if absstream:
                    self.ActivateControls(self.plotstream)
                    if not str(self.menu_p.abs_page.dilogTextCtrl.GetValue()) == '':
                        self.menu_p.abs_page.ClearLogButton.Enable()
                        self.menu_p.abs_page.SaveLogButton.Enable()
                # set load di to something useful (seems to be empty now)


            #redir=RedirectText(self.menu_p.rep_page.logMsg)
            #sys.stdout=prev_redir


    def onDISetParameter(self,event):
        """
        open parameter box for DI analysis
        """

        dlg = DISetParameterDialog(None, title='Set Parameter')
        dlg.expDTextCtrl.SetValue(self.options.get('diexpD',''))
        dlg.deltaFTextCtrl.SetValue(self.options.get('dideltaF',''))
        dlg.azimuthTextCtrl.SetValue(self.options.get('diazimuth',''))
        dlg.alphaTextCtrl.SetValue(self.options.get('dialpha',''))
        dlg.pierTextCtrl.SetValue(self.options.get('dipier',''))
        dlg.abstypeComboBox.SetStringSelection(self.options.get('ditype',''))

        if dlg.ShowModal() == wx.ID_OK:
            if not dlg.expDTextCtrl.GetValue() == '':
                self.options['diexpD'] = dlg.expDTextCtrl.GetValue()
            if not dlg.azimuthTextCtrl.GetValue() == '':
                self.options['diazimuth'] = dlg.azimuthTextCtrl.GetValue()
            if not dlg.pierTextCtrl.GetValue() == '':
                self.options['dipier'] = dlg.pierTextCtrl.GetValue()
            if not dlg.alphaTextCtrl.GetValue() == '':
                self.options['dialpha'] = dlg.alphaTextCtrl.GetValue()
            if not dlg.deltaFTextCtrl.GetValue() == '':
                self.options['dideltaF'] = dlg.deltaFTextCtrl.GetValue()
            self.options['ditype'] =  dlg.abstypeComboBox.GetValue()

        dlg.Destroy()


    def onInputSheet(self,event):
        """
        DESCRITPTION:
            open dialog to input DI data
        """

        if isinstance(self.dipathlist, str):
            dipath = self.dipathlist
        else:
            dipath = self.dipathlist[0]
        if os.path.isfile(dipath):
            dipath = os.path.split(dipath)[0]

        self.dilayout = {}
        self.dilayout['scalevalue'] = self.options['scalevalue']
        self.dilayout['double'] = self.options['double']
        self.dilayout['order'] = self.options['order'].split(',')
        #self.dilayout = {'order':['MU','MD','EU','WU','ED','WD','NU','SD','ND','SU'], 'scalevalue':'True', 'double':'True'}
        #self.dilayout = {'order':['MU','MD','WU','EU','WD','ED','NU','SD','ND','SU'], 'scalevalue':'True', 'double':'False'}
        defaults = self.options
        cdate = pydate2wxdate(datetime.utcnow())
        dlg = InputSheetDialog(None, title='Add DI data',path=dipath,layout=self.dilayout, defaults=defaults, cdate=cdate, db = self.db)
        if dlg.ShowModal() == wx.ID_OK:
            pass
        dlg.Destroy()


    def onSaveDIData(self, event):
        """
        DESCRIPTION
            Save data of the logger to file
        """
        # TODO When starting ANalysis -> stout is redirected .. switch back to normal afterwards
        saveFileDialog = wx.FileDialog(self, "Save As", "", "",
                                       "DI analysis report (*.txt)|*.txt",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        saveFileDialog.ShowModal()
        savepath = saveFileDialog.GetPath()
        text = self.menu_p.abs_page.dilogTextCtrl.GetValue()
        saveFileDialog.Destroy()

        difile = open(savepath, "w")
        difile.write(text)
        difile.close()


    def onClearDIData(self, event):
        self.menu_p.abs_page.dilogTextCtrl.SetValue('')

    # ------------------------------------------------------------------------------------------
    # ################
    # Report page functions
    # ################
    # ------------------------------------------------------------------------------------------


    def onSaveLogButton(self, event):
        saveFileDialog = wx.FileDialog(self, "Save As", "", "",
                                       "Log files (*.log)|*.log",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        saveFileDialog.ShowModal()
        savepath = saveFileDialog.GetPath()
        text = self.menu_p.rep_page.logger.GetValue()
        saveFileDialog.Destroy()

        logfile = open(savepath, "w")
        logfile.write(text)
        logfile.close()


    # ------------------------------------------------------------------------------------------
    # ################
    # Monitor page functions
    # ################
    # ------------------------------------------------------------------------------------------

    """     
    def onConnectMQTTButton(self, event):
        # start a subscribe to client call
        success = True

        # continuously collect data to stream and periodically call monitor plots
        # Open dlg to select MARTAS-address (IP number)
        # and to provide ssh access
        # (favorite dict on MARTAS sheet {'MARTAS':'address','MQTT':'address'})
        dlg = AGetMARTASDialog(None, title='Select MARTAS',options=self.options)
        if dlg.ShowModal() == wx.ID_OK:
            martasaddress = dlg.addressComboBox.GetValue()
            martasuser = dlg.userTextCtrl.GetValue()
            martaspasswd = dlg.pwdTextCtrl.GetValue()
        else:
            dlg.Destroy()
            return

        # If IP selected try to get sensor.txt from MARTAS using ssh
        # If true : start record with sensorid
        # if false: ask for sensorid (windows)
        print ("Getting sensor information from ", martasaddress)
        martaspath = os.path.join('/home',martasuser,'MARTAS')
        print (martaspath)
        sensfile = os.path.join(martaspath,'sensors.txt')
        owfile = os.path.join(martaspath,'owlist.csv')

        import tempfile
        destpath = tempfile.gettempdir()

        destsensfile = os.path.join(destpath,martasaddress+'_sensors.txt')
        destowfile = os.path.join(destpath,martasaddress+'_owlist.csv')

        try:
            scptransfer(martasuser+'@'+martasaddress+':'+sensfile,destsensfile,martaspasswd)
        except:
            print ("Could not connect to/get sensor info of client {} - aborting".format(martasaddress))
            success = False
            #print "Please make sure that you connected at least once to the client by ssh"
            #print " with your defaultuser %s " % martasuser
            #print " This way the essential key data is established."
        print ("Searching for onewire data from {}".format(martasaddress))
        try:
            scptransfer(martasuser+'@'+martasaddress+':'+owfile,destowfile,martaspasswd)
        except:
            print ("No one wire info available on client {} - proceeding".format(martasaddress))

        s,o = [],[]
        if os.path.exists(destsensfile):
            with open(destsensfile,'rb') as f:
                reader = csv.reader(f)
                s = []
                for line in reader:
                    print (line)
                    if len(line) < 2:
                        try:
                            s.append(line[0].split())
                        except:
                            # Empty line for example
                            pass
                    else:
                        s.append(line)
            print (s)
        else:
            print ("Apparently no sensors defined on client {} - aborting".format(martasaddress))
            success = False
            return

        if os.path.exists(destowfile):
            with open(destowfile,'rb') as f:
                reader = csv.reader(f)
                o = [line for line in reader]
            print (o)


        # get all parameters
        pad = 5
        sr = 1.0 # sampling rate
        currentdate = datetime.strftime(datetime.utcnow(),"%Y-%m-%d")
        period = float(self.menu_p.com_page.frequSlider.GetValue())
        covval = float(self.menu_p.com_page.coverageTextCtrl.GetValue())
        coverage = covval/sr
        limit = period/sr

        # start subscribe2client
        #self.plot_p.datavars = {0: datainfoid, 1: parameter, 2: limit, 3: pad, 4: currentdate, 5: unitlist, 6: coverage, 7: period, 8: self.db}
        self.plot_p.datavars = {2: limit, 3: pad, 4: currentdate, 6: coverage, 7: period, 9: martasaddress, 10: destpath, 11: [martasuser,martaspasswd], 12: s, 13: o, 14: self.options.get('stationid','WIC')}

        self.monitorSource='MARTAS'
        success = True
        if success:
            self.menu_p.com_page.startMonitorButton.Enable()
            self.menu_p.com_page.getMARCOSButton.Disable()
            self.menu_p.com_page.getMQTTButton.Disable()
            self.menu_p.com_page.martasLabel.SetBackgroundColour(wx.GREEN)
            self.menu_p.com_page.martasLabel.SetValue('connected to {}'.format(martasaddress))
            self.menu_p.com_page.logMsg('Begin monitoring...')
            self.menu_p.com_page.logMsg(' - Selected MARTAS')
            self.menu_p.com_page.logMsg(' - IP: {}'.format(martasaddress))
            self.menu_p.com_page.coverageTextCtrl.Enable()    # always
            self.menu_p.com_page.frequSlider.Enable()         # always
    """     

    def onConnectMARCOSButton(self, event):
        # active if database is connected
        # open dlg

        # Check whether DB still available
        self.checkDB('minimal')

        self.menu_p.rep_page.logMsg('- Selecting MARCOS table for monitoring ...')
        output = dbselect(self.db,'DataID,DataMinTime,DataMaxTime','DATAINFO')
        datainfoidlist = [elem[0] for elem in output]
        if len(datainfoidlist) < 1:
            dlg = wx.MessageDialog(self, "No data tables available!\n"
                            "please check your database\n",
                            "OpenDB", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            return
        # select table
        sr = 1
        dlg = AGetMARCOSDialog(None, title='Select table',datalst=datainfoidlist)
        if dlg.ShowModal() == wx.ID_OK:
            datainfoid = dlg.dataComboBox.GetValue()
            vals = dbselect(self.db, 'SensorID,DataSamplingRate,ColumnContents,ColumnUnits','DATAINFO', 'DataID = "'+datainfoid+'"')
            vals = vals[0]
            sensid= vals[0]
            sr= float(vals[1].strip('sec'))
            keys= vals[2].split(',')
            units= vals[3].split(',')
        else:
            dlg.Destroy()
            return
        # get all parameters

        pad = 5
        currentdate = datetime.strftime(datetime.utcnow(),"%Y-%m-%d")

        # start monitoring parameters
        period = float(self.menu_p.com_page.frequSlider.GetValue())
        covval = float(self.menu_p.com_page.coverageTextCtrl.GetValue())
        coverage = covval/sr
        limit = period/sr
        unitlist = []
        for idx,key in enumerate(keys):
           if not key == '':
               unitlist.append([key, units[idx]])

        parameter = ','.join([KEYLIST[idx+1] for idx,key in enumerate(keys) if not key=='' and KEYLIST[idx+1] in NUMKEYLIST])

        self.plot_p.datavars = {0: datainfoid, 1: parameter, 2: limit, 3: pad, 4: currentdate, 5: unitlist, 6: coverage, 7: period, 8: self.db}
        self.monitorSource='MARCOS'

        success = True
        if success:
            self.menu_p.com_page.startMonitorButton.Enable()
            self.menu_p.com_page.getMARTASButton.Disable()
            #self.menu_p.com_page.getMQTTButton.Disable()
            self.menu_p.com_page.marcosLabel.SetBackgroundColour(wx.GREEN)
            self.menu_p.com_page.marcosLabel.SetValue('connected to {}'.format(self.options.get('dbname','')))
            self.menu_p.com_page.logMsg('Begin monitoring...')
            self.menu_p.com_page.logMsg(' - Selected MARCOS database')
            self.menu_p.com_page.logMsg(' - Table: {}'.format(datainfoid))
            self.menu_p.com_page.coverageTextCtrl.Enable()    # always
            self.menu_p.com_page.frequSlider.Enable()         # always


    def onConnectMARTASButton(self, event):

        success = False
        oldopt = self.options.get('favoritemartas',[])
        dlg = SelectMARTASDialog(None, title='Select MARTAS',options=self.options)
        if dlg.ShowModal() == wx.ID_OK:
            martasaddress = dlg.addressComboBox.GetValue()
            martasport = dlg.portTextCtrl.GetValue()
            #martasdelay = dlg.delayTextCtrl.GetValue()
            martasstationid = dlg.stationidTextCtrl.GetValue()
            martasuser = dlg.userTextCtrl.GetValue()
            martaspasswd = dlg.pwdTextCtrl.GetValue()
            self.options['favoritemartas'] = dlg.favoritemartas
            martasdelay = 60
            martasprotocol = 'mqtt' # dlg.pwdTextCtrl.GetValue()
            success = True
            #if not oldopt == self.options['favoritemartas']:
            saveini(self.options)
            inipara, check = loadini()
            self.initParameter(inipara)
            dlg.Destroy()
        else:
            self.options['favoritemartas'] = dlg.favoritemartas
            if not oldopt == self.options['favoritemartas']:
                saveini(self.options)
                inipara, check = loadini()
                self.initParameter(inipara)
            dlg.Destroy()
            return

        #if not oldopt == self.options['favoritemartas']:
        #    saveini(self.options)
        #    inipara, check = loadini()
        #    self.initParameter(inipara)
        self.menu_p.rep_page.logMsg('- Selected MARTAS maschine ({},{}) for monitoring ...'.format(martasaddress,martasprotocol))

        pad = 5
        currentdate = datetime.strftime(datetime.utcnow(),"%Y-%m-%d")
        # start monitoring parameters
        unitlist = []

        # get header information from data stream
        try:
            import paho.mqtt.client as mqtt
            from magpy.collector import collectormethods as colsup
            mqttimport = True
        except:
            mqttimport = False
            dlg = wx.MessageDialog(self, "Could not import required packages!\n"
                        "Make sure that the python package paho-mqtt is installed\n",
                        "MARTAS monitor failed", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Using MQTT monitor failed ... Ready")
            dlg.Destroy()
        #print ("TEST", colsup.identifier)

        if mqttimport:
            # TODO stationcode is currently hardcoded in collectorsupport - change that !!
            client = mqtt.Client()

            if not martasuser in ['',None,'None','-']: 
                #client.tls_set(tlspath)  # check http://www.steves-internet-guide.com/mosquitto-tls/
                client.username_pw_set(martasuser, password=martaspasswd)  # defined on broker by mosquitto_passwd -c passwordfile user

            client.on_connect = colsup.on_connect
            client.on_message = colsup.on_message

            #print (martasaddress)
            #client.connect("192.168.178.84", 1883, 60)
            try:
                client.connect(martasaddress, int(martasport), int(martasdelay))
            except:
                dlg = wx.MessageDialog(self, "Connection to MQTT broker failed\n"
                        "Check your internet connection or credentials\n",
                        "Connection failed", wx.OK|wx.ICON_INFORMATION)
                dlg.ShowModal()
                dlg.Destroy()
                return
            qos = 0
            client.subscribe("{}/#".format(martasstationid), qos)

            loopcnt = 0
            success = True
            try:
                maxloop = int(self.options.get('martasscantime'))*10
                print ("Got data from init - remove this message ater 0.3.99", maxloop)
            except:
                print ("Could not get scantime from options - using approx 20 seconds")
                maxloop = 200
            self.changeStatusbar("Scanning for MQTT broadcasts ... approx {} sec".format(int(maxloop/10)))            
            try:
                self.progress = wx.ProgressDialog("Scanning for MQTT broadcasts ...", "please wait", maximum=maxloop, parent=self, style=wx.PD_SMOOTH|wx.PD_AUTO_HIDE)
                while loopcnt < maxloop: #colsup.identifier == {} and loopcnt < 100:
                    loopcnt += 1
                    client.loop(.1) #blocks for 100ms
                    self.progress.Update(loopcnt)
                    if loopcnt > 600:
                        success = False
                        break
                self.progress.Destroy()
            except:  # test fallback for MacOS
                proDlg = WaitDialog(None, "Scanning...", "Scanning for MQTT broadcasts.\nPlease wait....")
                while loopcnt < maxloop: #colsup.identifier == {} and loopcnt < 100:
                    loopcnt += 1
                    client.loop(.1) #blocks for 100ms
                    self.progress.Update(loopcnt)
                    if loopcnt > 600:
                        success = False
                        break
                proDlg.Destroy()

            #print ("here", colsup.identifier)

            if success and len(colsup.identifier) > 0:
                self.changeStatusbar("Scanning for MQTT broadcasts ... found sensor(s)")
                self.menu_p.com_page.startMonitorButton.Enable()
                self.menu_p.com_page.getMARTASButton.Disable()
                #self.menu_p.com_page.getMQTTButton.Disable()
                self.menu_p.com_page.martasLabel.SetBackgroundColour(wx.GREEN)
                self.menu_p.com_page.martasLabel.SetValue('connected to {}'.format(martasaddress))
                self.menu_p.com_page.logMsg('Begin monitoring...')
                self.menu_p.com_page.logMsg(' - Selected MARTAS {} protocol'.format(martasprotocol))
                self.menu_p.com_page.coverageTextCtrl.Enable()    # always
                self.menu_p.com_page.frequSlider.Enable()         # always

                sensorlist = []
                for key in colsup.identifier:
                    print ("key", key)
                    sensorid = key.split(':')[0]
                    if not sensorid in sensorlist:
                        sensorlist.append(sensorid)

                dlg = SelectFromListDialog(None, title='Select sensor', selectlist=sensorlist, name='Sensor')
                if dlg.ShowModal() == wx.ID_OK:
                    sensorid = dlg.selectComboBox.GetValue()
                else:
                    self.menu_p.com_page.getMARTASButton.Enable()
                    self.ActivateControls(self.plotstream)
                    sensorid = sensorlist[0]
                    
                dlg.Destroy()

                self.menu_p.com_page.logMsg(' - selected Sensor: {}'.format(sensorid))

                pad = 5
                currentdate = datetime.strftime(datetime.utcnow(),"%Y-%m-%d")
                parameter = colsup.identifier.get(sensorid+':keylist')
                parameter = ','.join(parameter)
                unitlist = colsup.identifier.get(sensorid+':unitlist')
                period = float(self.menu_p.com_page.frequSlider.GetValue())
                covval = float(self.menu_p.com_page.coverageTextCtrl.GetValue())
                sr = 1
                coverage = covval/sr
                limit = period/sr
                datainfoid = sensorid+'_0001'

                self.plot_p.datavars = {0: datainfoid, 1: parameter, 2: limit, 3: pad, 4: currentdate, 5: unitlist, 6: coverage, 7: period, 8: self.db, 9: martasaddress, 10: martasport, 11: martasdelay, 12: martasprotocol, 13: martasuser, 14: martaspasswd, 15: martasstationid, 16: qos}
                self.monitorSource='MARTAS'
            else:
                self.changeStatusbar("Scanning for MQTT broadcasts ... no sensor found")
                #self.menu_p.com_page.mqttLabel.SetValue('unable to connect to {}'.format(martasaddress))


    def onStartMonitorButton(self, event):
        self.DeactivateAllControls()
        self.menu_p.com_page.getMARTASButton.Disable()
        self.menu_p.com_page.getMARCOSButton.Disable()
        #self.menu_p.com_page.getMQTTButton.Disable()
        self.menu_p.com_page.stopMonitorButton.Enable()
        if self.options.get('experimental'):
            self.menu_p.com_page.saveMonitorButton.Enable()   # if experimental

        # start monitoring parameters
        period = float(self.menu_p.com_page.frequSlider.GetValue())
        covval = float(self.menu_p.com_page.coverageTextCtrl.GetValue())
        sr = self.plot_p.datavars[7]/self.plot_p.datavars[2]
        coverage = covval/sr
        limit = period/sr
        self.plot_p.datavars[2] = limit
        self.plot_p.datavars[6] = coverage
        self.plot_p.datavars[7] = period

        self.changeStatusbar("Running monitor ...")
        # Obtain the last values from the data base with given dataid and limit
        # A DB query for 10 min 10Hz data needs approx 0.3 sec
        if  self.monitorSource=='MARCOS':
            self.plot_p.t1_stop.clear()
            self.menu_p.com_page.logMsg(' > Starting read cycle... {} sec'.format(period))
            self.plot_p.startMARCOSMonitor()
            self.menu_p.com_page.marcosLabel.SetBackgroundColour(wx.GREEN)
            self.menu_p.com_page.marcosLabel.SetValue('connected to {}'.format(self.options.get('dbname','')))
        elif self.monitorSource=='MARTAS':
            self.plot_p.t1_stop.clear()
            self.menu_p.com_page.logMsg(' > Starting read cycle... {} sec'.format(period))
            self.plot_p.startMARTASMonitor(self.plot_p.datavars.get(12))
            # MARTASmonitor calls subscribe2client  - output in temporary file (to start with) and access global array from storeData (move array to global)
            self.menu_p.com_page.martasLabel.SetBackgroundColour(wx.GREEN)
            self.menu_p.com_page.martasLabel.SetValue('connected to {}'.format(self.plot_p.datavars.get(9)))

    def _monitor2stream(self,array, db=None, dataid=None,header = {}):
        """
        DESCRIPTION:
            creates self.plotstream object from monitor data
        """
        #header = {}
        if db:
            header = dbfields2dict(db,dataid)
        if len(array[0]) > 0:
            if isinstance(array[0][-1], datetime):
                array[0] = date2num(array[0])
        stream = DataStream([LineStruct()],header,array)
        stream = stream.sorting()
        return stream

    def onStopMonitorButton(self, event):
        dataid = self.plot_p.datavars[0]
        self.plot_p.t1_stop.set()
        #self.plot_p.client.loop_stop()
        self.menu_p.com_page.logMsg(' > Read cycle stopped')
        self.menu_p.com_page.logMsg(' - {} disconnected'.format(self.monitorSource))
        self.stream = self._monitor2stream(self.plot_p.array,db=self.db,dataid=dataid)
        # delete old array
        self.plot_p.array = [[] for el in KEYLIST]
        self.stream.header['StationID'] = self.plot_p.datavars[15]
        self.plotstream = self.stream.copy()
        currentstreamindex = len(self.streamlist)
        self.streamlist.append(self.plotstream)
        self.streamkeylist.append(self.plotstream._get_key_headers())
        self.headerlist.append(self.plotstream.header)
        self.currentstreamindex = currentstreamindex

        self.menu_p.com_page.stopMonitorButton.Disable()
        self.menu_p.com_page.saveMonitorButton.Disable()
        self.ActivateControls(self.plotstream)
        self.shownkeylist = self.UpdatePlotCharacteristics(self.plotstream)
        self.plotoptlist.append(self.plotopt)
        self.OnPlot(self.plotstream,self.shownkeylist)

        self.menu_p.com_page.getMARTASButton.Enable()
        self.menu_p.com_page.getMARCOSButton.Enable()
        #self.menu_p.com_page.getMQTTButton.Enable()
        self.menu_p.com_page.marcosLabel.SetBackgroundColour((255,23,23))
        self.menu_p.com_page.martasLabel.SetBackgroundColour((255,23,23))
        #self.menu_p.com_page.mqttLabel.SetBackgroundColour((255,23,23))
        self.menu_p.com_page.marcosLabel.SetValue('not connected')
        self.menu_p.com_page.martasLabel.SetValue('not connected')
        #self.menu_p.com_page.mqttLabel.SetValue('not connected')
        self.changeStatusbar("Ready")


    def onLogDataButton(self, event):
        # open dialog with pathname
        # then use data_2_file method for binary writing
        pass

class MagPyApp(wx.App):
    # wxWindows calls this method to initialize the application
    def OnInit(self):
        # Create an instance of our customized Frame class
        frame = MainFrame(None,-1,"")
        frame.Show(True)
        # Tell wxWindows that this is our main window
        self.SetTopWindow(frame)
        # Return a success flag
        return True

'''
# To run:
app = MagPyApp(0)
app.MainLoop()
'''
