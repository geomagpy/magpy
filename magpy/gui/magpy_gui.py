#!/usr/bin/env python

# TODO - remove the following two line as soon as new packages are updated (only required for testruns (python stream.py)
import sys

sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

from pubsub import pub

# wx 4.x
from wx.lib.dialogs import ScrolledMessageDialog
import wx.lib.scrolledpanel as scrolled
from wx import STB_SIZEGRIP as wxSTB_SIZEGRIP
from wx.adv import AboutDialogInfo as wxAboutDialogInfo
from wx.adv import AboutBox as wxAboutBox
from wx import FD_MULTIPLE as wxMULTIPLE


#from magpy.stream import read
import magpy.core.plot as mp
#import magpy.absolutes as di
from magpy.absolutes import *
from magpy.core import methods
from magpy.core import database
from magpy.core import activity
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

#from magpy.collector import collectormethods as colsup

from magpy.gui.statisticspage import StatisticsPanel
import glob, os, pickle, base64
import platform # for system check to import WXAgg on Mac
import pylab
import time #,thread
import threading
import hashlib
import pathlib
import locale

import wx.py


"""
The magpy_gui module

Some conventions as used in all modules
--------------------------------------
Basic Python naming convention:
Classes:                    MyClass
Basic internal methods:     _my_internal_method
Menu/panel items:           incredibleButton
Methods bound to elements:  on_incredible_button
Major methods:              major_method


| class          |  method  |  since version  |  until vers |  runtime test  |  calls  | manual  |  used by |
| -------------- |  ------  |  -------------  |  ---------- |  ------------  |  -----  |---------|  ---------- |
|  RedirectText  |          |          2.0.0  |             |            |             | -       |  core.activity |
|  PlotPanel     |  __init__  |        2.0.0  |             | level 1    |             |         | |
|  PlotPanel     |  __do_layout  |     2.0.0  |             | level 1    |             |         | |
|  PlotPanel     |  timer   |          2.0.0  |             |            |             |      | |
|  PlotPanel     |  update  |          2.0.0  |             |            |             |      | |
|  PlotPanel     |  update_mqtt  |     2.0.0  |             |            |             |      | |
|  PlotPanel     |  start_martas_monitor |  2.0.0  |        |            |             |      | |
|  PlotPanel     |  start_marcos_monitor |  2.0.0  |        |            |             |      | |
|  PlotPanel     |  monitor_plot  |    2.0.0  |             |            |             |      | |
|  PlotPanel     |  gui_plot  |        2.0.0  |             | level 1    |               |      | |
|  PlotPanel     |  initial_plot  |    2.0.0  |             | level 1    |               |      | |
|  PlotPanel     |  link_rep  |        2.0.0  |             |            |               |      | |
|  PlotPanel     |  link_rep  |        2.0.0  |             |            |               |      | |
|  PlotPanel     |  AnnoteFinder  |    2.0.0  |             |            |               |      | |
|  PlotPanel     |  AF.__init__  |     2.0.0  |             |            |               |      | |
|  PlotPanel     |  AF.distance  |     2.0.0  |             |            |               |      | |
|  PlotPanel     |  AF.__call__  |     2.0.0  |             |            |               |      | |
|  PlotPanel     |  AF.finder  |       2.0.0  |             |            |               |      | |
|  PlotPanel     |  AF.draw  |         2.0.0  |             |            |               |      | |
|  MenuPanel     | __init__  |         2.0.0  |             |            |               |      | |
|  MainFrame     | __init__  |         2.0.0  |             | level 1    |               |        |    |
|  MainFrame     | __set_properties |  2.0.0  |             | level 1    |               |        | init   |
|  MainFrame     | _get_default_initialization |  2.0.0  |  | level 1    |               |        | init   |
|  MainFrame     | _set_plot_parameter |  2.0.0  |          | level 1    |               |        | init   |
|  MainFrame     | _create_menu_bar |  2.0.0  |             | level 1    |               |        | init   |
|  MainFrame     | _bind_controls  |   2.0.0  |             | level 1    |               |        | init   |
|  MainFrame     | _db_connect  |      2.0.0  |             | level 1    |               |        | init, db_, file_db |
|  MainFrame     | _determine_decimals  | 2.0.0  |          | level 2    |               |        | onMean, onMax, onMin |
|  MainFrame     | _deactivate_controls |  2.0.0  |         | level 1    |               |        | init, file_on_open |
|  MainFrame     | _activate_controls |  2.0.0  |           | level 1    |               |        | init, file_on_open |
|  MainFrame     | _initial_read  |    2.0.0  |             | level 1    |               |        | file_on_open  |
|  MainFrame     | _initial_plot  |    2.0.0  |             | level 1    |               |        | file_on_open  |
|  MainFrame     | _update_plot  |     2.0.0  |             | level 1    |               |        | file_on_open  |
|  MainFrame     | _do_plot  |         2.0.0  |             | level 1    |               |        | file_on_open  |
|  MainFrame     | _update_cursor_status |  2.0.0  |        | level 1    |               |        |   |
|  MainFrame     | _open_stream  |     2.0.0  |             | level 0    |               |        | file_on_open  |
|  MainFrame     | _update_statistics | 2.0.0  |            | level 0    |               |        | _do_plot  |
|  MainFrame     | changeStatusbar  |  2.0.0  |             | level 2    |               |        | everywhere  |
|  MainFrame     | file_on_open_file  | 2.0.0  |            | level 2    |               |        |   |
|  MainFrame     | file_on_open_dir  | 1.0.0  |  removed    | level 1    |               |        |   |
|  MainFrame     | file_on_open_url  | 2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | file_on_open_webservice | 2.0.0  |       | level 1    |               |        |   |
|  MainFrame     | file_on_open_db  |  2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | file_on_export |    2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | file_on_quit  |     2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | db_on_connect  |    2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | db_on_init  |       2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | di_input_sheet |    2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | memory_select |     2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | options_init |      2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | options_plot   |    2.0.0  |             |            |               |        |   |
|  MainFrame     | options_di   |      2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | help_about  |       2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | help_read_formats | 2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | help_write_formats | 2.0.0  |            | level 2    |               |        |   |
|  MainFrame     | help_open_log     | 2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | d_get_adjacent_stream | 2.0.0  |         | level 2    |               |        |   |
|  MainFrame     | d_onNextButton |    2.0.0  |             | level 2    |               |        | get_adjacent  |
|  MainFrame     | d_onPreviousButton |  2.0.0  |           | level 2    |               |        | get_adjacent  |
|  MainFrame     | d_onTrimButton |    2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | d_onSelectButton |  2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | d_onDropButton |    2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | d_onExtrcatButton | 2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | d_onGetGapsButton | 2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | d_onStatusButton |  2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | m_onGetDBButton |   2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | m_onPutDBButton |   2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | m_onDataButton |    2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | m_onSensorButton |  2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | m_onStationButton | 2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | a_onDerivativeButton | 2.0.0  |          | level 2    |               |        |   |
|  MainFrame     | a_onDeltaFButton  | 2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | a_onRotationButton | 2.0.0  |            | level 2    |               |        |   |
|  MainFrame     | a_onMeanButton   |  2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | a_onMaxButton |     2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | a_onMinButton |     2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | a_onFitButton    |  2.0.0  |             | level 2    |               |        |   |
|  MainFrame     | a_onFilterButton |  2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | a_onSmoothButton |  2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | a_onOffsetButton |  2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | a_onResampleButton |  2.0.0  |           | level 1    |               |        |   |
|  MainFrame     | a_onActivityButton |  2.0.0  |           | level 1    |               |        |   |
|  MainFrame     | a_onCalcFButton |   2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | a_onDailyMeansButton | 2.0.0  |          | level 2    |               |        |   |
|  MainFrame     | a_onBaselineButton | 2.0.0  |            | level 1    |               |        |   |
|  MainFrame     | a_onApplyBCButton | 2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | a_onPowerButton |   2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | a_onSpectrogramButton | 2.0.0  |         | level 0    |               |        |   |
|  MainFrame     | di_onLoadDIButton |  2.0.0  |            | level 2    |               |        |   |
|  MainFrame     | di_onDefineVarioScalar |  2.0.0  |       | level 1    |               |        |   |
|  MainFrame     | di_onDIAnalysis |   2.0.0  |             | level 1    |               |        |   |
|  MainFrame     | di_onDIParameter |   2.0.0  |            | level 1    |               |        |   |
|  MainFrame     | xxxx |   2.0.0  |             | level 0    |               |        |   |
|  MainFrame     | xxxx |   2.0.0  |             | level 0    |               |        |   |
|  MainFrame     | xxxx |   2.0.0  |             | level 0    |               |        |   |
|  MainFrame     | r_onSaveLogButton |  2.0.0  |            | level 2    |               |        |   |
|  -          |  read_dict  |          2.0.0  |             | level 1    |               |        |   |
|  -          |  save_dict  |          2.0.0  |             | level 1    |               |        |   |
|  -          |  saveobj    |          1.0.0  |             |            |               |        |   |
|  -          |  loadobj    |          1.0.0  |             |            |               |        |   |
|  -          |  pydate2wxdate  |      2.0.0  |             | level 1    |               |        |   |
|  -          |  wxdate2pydate  |      2.0.0  |             | level 1    |               |        |   |
|             |    |  2.0.0  |               |            |               |        |   |
|             |    |  2.0.0  |               |            |               |        |   |
|             |    |  2.0.0  |               |            |               |        |   |


runtime test:
- : not tested
level 0 : runs on linux without throwing an error
level 1 : basic working tests
level 2 : all options tested
level 3 : level 2 also on Mac and Windows (level2w or level2m as temporary)

* all tests are performed with the suggested configuration of the install recommendation

Basic Processing:
__init__ calls read and save_dict to get stored config, _get_default if empty, _set_plot_parameter, _create_menu_bar
               _bind_controls,  _db_connect and _deactivate_controls
file_on_open_...  reads data and calls _initial_read to set datadict, (with _deactivate_controls()), then _initial_plot 
               (with _activate_controls, _update_plot and _do_plot)

# TESTING:
file_on_open_file:   single file, all types, empty columns, multiple files, wrong files
file_on_open_url:    single urls, url paths
file_on_open_webserive: all webservices and selections
file_on_open_db:     database tables 
file_on_export:      all types

"""

def read_dict(path=None, debug=False):
    """
    DESCRIPTION
         read memory
    USED BY
         MainFrame __init__
    """
    mydict = {}
    if not path:
        return {}
    if os.path.isfile(path):
        if debug:
            print("Reading memory: {}".format(path))
        with open(path, 'r') as file:
            mydict = json.load(file)
    else:
        print("Memory path ({}) not found - please check (first run?)".format(path))
    if debug:
        print("Found in Memory: {}".format([el for el in mydict]))
    return mydict


def save_dict(mydict, path=None, debug=False):
    """
    DESCRIPTION
        save dictionary to json
    USED BY
         MainFrame __init__
    """
    if not path:
        return False
    if debug:
        print("magpy: saving to {}".format(path))
    try:
        dirpath = pathlib.Path(path).parent.absolute()
        pathlib.Path(dirpath).mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(mydict, f, ensure_ascii=False, indent=4)
    except:
        print("magpy: saving dictionary to {} failed".format(path))
        return False
    return True


def saveobj(obj, filename):
    """
    DESCRIPTION
        save full object (i.e. function)
    USED BY
    """
    with open(filename, 'wb') as f:
        pickle.dump(obj,f,pickle.HIGHEST_PROTOCOL)


def loadobj(filename):
    """
    DESCRIPTION
        load full object (i.e. function)
    USED BY
    """
    with open(filename, 'rb') as f:
        return pickle.load(f)


def pydate2wxdate(datum):
     assert isinstance(datum, (datetime, datetime.date))
     tt = datum.timetuple()
     dmy = (tt[2], tt[1]-1, tt[0])
     #print (tt, dmy)
     try:
         return wx.DateTime.FromDMY(*dmy)
     except:
         return wx.DateTimeFromDMY(*dmy)

def wxdate2pydate(date):
     assert isinstance(date, wx.DateTime)
     if date.IsValid():
          ymd = map(int, date.FormatISODate().split('-'))
          return datetime.date(*ymd)
     else:
          return None


class RedirectText(object):
    # Taken from: http://www.blog.pythonlibrary.org/2009/01/01/wxpython-redirecting-stdout-stderr/
    # Used to redirect di results to the multiline textctrl on the DI page
    def __init__(self,aWxTextCtrl):
        self.out=aWxTextCtrl

    def write(self,string):
        self.out.WriteText(string)

class PlotPanel(scrolled.ScrolledPanel):
    """
    DESCRIPTION
        contains all methods for the left plot panel
    """
    def __init__(self, *args, **kwds):
        scrolled.ScrolledPanel.__init__(self, *args, **kwds)
        # switch to WXAgg (required for MacOS)
        if platform.system() == "Darwin":
            matplotlib.use('WXAgg')
        #width = 10
        #hght = 3
        #self.figure = plt.figure(figsize=(width, hght))
        self.figure = plt.figure()
        self.plt = plt
        scsetmp = ScreenSelections()
        self.canvas = FigureCanvas(self,-1,self.figure)
        self.datavars = {} # for monitoring
        self.array = [[] for key in DataStream().KEYLIST] # for monitoring
        self.t1_stop= threading.Event()
        self.xlimits = None
        self.ylimits = None
        self.selplt = 0 # Index to the selected plot - used by flagselection
        self.initial_plot()
        self.__do_layout()

    def __do_layout(self):
        # Resize graph and toolbar, create toolbar
        self.vbox = wx.BoxSizer(wx.VERTICAL)
        self.vbox.Add(self.canvas, 1, wx.LEFT | wx.TOP | wx.GROW)
        self.toolbar = NavigationToolbar2Wx(self.canvas)
        self.vbox.Add(self.toolbar, 0, wx.EXPAND)
        self.SetSizer(self.vbox)
        self.vbox.Fit(self)
        self.SetupScrolling()


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
            If log file is chosen then this method makes use of collector.subscribe method:
            storeData to save binary file
        """
        def list_duplicates(seq):
            seen = set()
            seen_add = seen.add
            return [idx for idx,item in enumerate(seq) if item in seen or seen_add(item)]

        db = self.datavars[8]
        parameterstring = 'time,'+self.datavars[1]
        # li should contain a data source of a certain length (can be filled by any reading process)
        li = sorted(db.select(parameterstring, self.datavars[0], expert='ORDER BY time DESC LIMIT {}'.format(int(self.datavars[2]))))
        try:
            tmpdt = [datetime.strptime(elem[0], "%Y-%m-%d %H:%M:%S.%f") for elem in li]
        except:
            tmpdt = [datetime.strptime(elem[0], "%Y-%m-%d %H:%M:%S") for elem in li]
        self.array[0].extend(tmpdt)
        for idx,para in enumerate(parameterstring.split(',')):
            if not para.endswith('time'):
                i = DataStream().KEYLIST.index(para)
                try:
                    self.array[i].extend([float(elem[idx]) for elem in li])
                except:
                    # None, NAN or string
                    pass

        duplicateindicies = list_duplicates(self.array[0])
        array = [[] for key in DataStream().KEYLIST]
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

        print ("CHECK:", self.datavars)
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

        debug = False ## PLEASE NOTE Oct 2019: MARTAS is receiving data only at the defined interval, not inbetween

        #print (self.datavars[0][:-5])
        coverage = int(self.datavars[6])

        pos = DataStream().KEYLIST.index('t1')
        posvar1 = DataStream().KEYLIST.index('var1')
        if debug:
            print ("updating ...")
            print ("data:", self.datavars)
        #OK = True
        #if OK:
        while sumtime<self.datavars[2]:
            #client.loop(.1)
            # TODO add reconnection handler here in case that client goes away
            # print (client.connected_flag) -- connected_flag remains True -> can set it to False if no Payload is received ....
            #if not client.connected_flag:
            #    reconnect
            sumtime = sumtime+1
            # TODO
            #li = colsup.streamdict.get(self.datavars[0][:-5])   # li is an ndarray
            li = []
            if debug:
                print (li)
                print (self.datavars[0][:-5])
            tester = True
            try:
                if li == None:
                    tester = False
            except:
                try:
                    if li.all() == None:
                        tester = False
                except:
                    pass

            if len(self.array[0]) > 0 and li[0][0] == self.array[0][-1]:
                pass
            else:
                if tester:
                    for idx,el in enumerate(li):
                        self.array[idx].extend(el)

            try:
                tmp = DataStream([],{},np.asarray(self.array,dtype=object)).samplingrate()
                coverage = int(int(self.datavars[6])/tmp)
            except:
                pass
            self.array = [el[-coverage:] for el in self.array]

        if len(self.array[0]) > 2:
            ind = np.argsort(np.asarray(self.array,dtype=object)[0])
            for idx,line in enumerate(self.array):
                if len(line) == len(ind):
                    self.array[idx] = list(np.asarray(line)[ind])
            #self.array = np.asarray(self.array)[:,np.argsort(np.asarray(self.array)[0])]
            array = self.array
            self.monitorPlot(array)

    @deprecated("Replaced by start_marcos_monitor")
    def startMARCOSMonitor(self,**kwargs):
        self.start_marcos_monitor(**kwargs)


    def start_marcos_monitor(self,**kwargs):
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
        stationid = self.datavars.get(15)

        # convert parameter list to a dbselect sql format
        parameterstring = 'time,'+parameter

        # Test whether data is available at all with selected keys and dataid
        li = sorted(db.select(parameterstring, dataid, expert='ORDER BY time DESC LIMIT {}'.format(int(coverage))))

        if not len(li) > 0:
            print("Parameter", parameterstring, dataid, coverage)
            print("Did not find any data to display - aborting")
            return
        else:
            valkeys = ['time']
            valkeys = parameterstring.split(',')
            for i,elem in enumerate(valkeys):
                idx = DataStream().KEYLIST.index(elem)
                if elem == 'time':
                    try:
                        self.array[idx] = [datetime.strptime(el[0],"%Y-%m-%d %H:%M:%S.%f") for el in li]
                    except:
                        self.array[idx] = [datetime.strptime(el[0],"%Y-%m-%d %H:%M:%S") for el in li]
                else:
                    try:
                        self.array[idx] = [float(el[i]) for el in li]
                    except:
                        # non valid inputs or no numbers
                        pass

        self.datavars = {0: dataid, 1: parameter, 2: period, 3: pad, 4: currentdate, 5: unitlist, 6: coverage, 7: updatetime, 8: db, 15: stationid}

        self.figure.clear()
        t1 = threading.Thread(target=self.timer, args=(1,self.t1_stop))
        t1.start()
        # Display the plot
        self.canvas.draw()


    @deprecated("Replaced by start_marcos_monitor")
    def startMARTASMonitor(self, protocol, **kwargs):
        self.start_martas_monitor(protocol, **kwargs)


    def start_martas_monitor(self, protocol, **kwargs):
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

        """
        if protocol == 'mqtt':
            try:
                import paho.mqtt.client as mqtt
                #from magpy.collector import collectormethods as colsup
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
                client.loop_start()
                # Display the plot
                self.canvas.draw()
        """


    @deprecated("Replaced by monitor_plot")
    def monitorPlot(self,array,**kwargs):
        self.monitor_plot(array, **kwargs)

    def monitor_plot(self,array,**kwargs):
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
        martasaddress = self.datavars.get(9)
        martasport = self.datavars.get(10)
        martasdelay = self.datavars.get(11)
        martasprotocol = self.datavars.get(12)
        martasuser = self.datavars.get(13)
        martaspasswd = self.datavars.get(14)
        martasstationid = self.datavars.get(15)
        qos = self.datavars.get(16)

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
            i = DataStream().KEYLIST.index(para)
            if not para.endswith('time') and len(array[i]) > 0:
                #print (para, len(array[i]))
                subind = int("{}1{}".format(len(parameterstring.split(','))-1,idx))
                self.axes = self.figure.add_subplot(subind)
                self.axes.grid(True)
                rd = array[i]
                #try:
                l, = self.axes.plot_date(dt,rd,'b-')
                #except:
                #    array = [[] for el in DataStream().KEYLIST]
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


    @deprecated("Replaced by gui_plot")
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

        self.axes = mp.tsplot(streams,keys,figure=self.figure,**plotopt)

        self.axlist = self.figure.axes

        #get current xlimits:
        for idx, ax in enumerate(self.axlist):
            self.xlimits = ax.get_xlim()
            self.ylimits = ax.get_ylim()
            ax.callbacks.connect('xlim_changed', on_xlims_change)
            ax.callbacks.connect('ylim_changed', on_ylims_change)

        stream = streams[-1]
        key = keys[-1]

        self.t = stream.ndarray[0]
        flagpos = DataStream().KEYLIST.index('flag')
        firstcol = DataStream().KEYLIST.index(key[0])
        flag = stream.ndarray[flagpos]
        self.k = stream.ndarray[firstcol]

        self.canvas.draw()


    def gui_plot(self,streamids, datadict, plotdict, sharey=False):
        """
        DEFINITION:
            embbed matplotlib figure in canvas

        PARAMETERS:
            streamids : list of ids to be plotted
            datadict : all data relevant information
            plotdict : all visualization parameter
            sharey : limit shownkeys to a single input to share y axis for multiple diagrams
        """
        debug = False

        streams = []
        keys = []
        colors = []
        symbols = []
        timecolumn = []
        errorbars = []
        yranges = []
        fill = []
        padding = []
        showpatch = []
        functions = []
        title = ''
        legend = {}
        grid = False
        patch = {}
        annotate = False
        alpha = 0.5
        ylabelposition = None
        yscale = None
        functionfmt = "r-"

        for streamid in streamids:
            datacont = datadict.get(streamid)
            streams.append(datacont.get('dataset'))
            plotcont = plotdict.get(streamid)
            keys.append(plotcont.get('shownkeys'))
            colors.append(plotcont.get('colors'))
            symbols.append(plotcont.get('symbols'))
            padding.append(plotcont.get('padding'))
            timecolumn.append(plotcont.get('timecolumn'))
            yranges.append(plotcont.get('yranges'))
            fill.append(plotcont.get('fill'))
            showpatch.append(plotcont.get('showpatch'))
            errorbars.append(plotcont.get('errorbars'))
            functions.append(plotcont.get('functions'))

            title=plotcont.get('title')
            legend=plotcont.get('legend')
            grid=plotcont.get('grid')
            patch=plotcont.get('patch')
            annotate=plotcont.get('annotate')
            alpha=plotcont.get('alpha')
            ylabelposition=None
            yscale=None
            functionfmt="r-"
            #xinds=[None]
            #xlabelposition=None,
            #dateformatter=None
            #force=False
            #width=10
            #height=4

        if sharey and len(keys) > 0:
            keys = [keys[0]]

        if debug:
            print (keys,colors,symbols,timecolumn,errorbars,yranges,fill,padding,showpatch,functions)
            print (title,legend,grid,patch,annotate,alpha,ylabelposition,yscale,functionfmt)

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

        self.figure, self.axes = mp.tsplot(data=streams, keys=keys, timecolumn=timecolumn, yranges=yranges, padding=padding,
              symbols=symbols, colors=colors, title=title, legend=legend, grid=grid, patch=patch, annotate=annotate,
              fill=fill, showpatch=showpatch, errorbars=errorbars, functions=functions, functionfmt=functionfmt,
              ylabelposition=ylabelposition, yscale=yscale, alpha=alpha, figure=self.figure)
        #xrange=None, xinds=[None],  xlabelposition=None, dateformatter=None, force=False, width=10, height=4,

        self.axlist = self.figure.axes

        #get current xlimits:
        for idx, ax in enumerate(self.axlist):
            self.xlimits = ax.get_xlim()
            self.ylimits = ax.get_ylim()
            ax.callbacks.connect('xlim_changed', on_xlims_change)
            ax.callbacks.connect('ylim_changed', on_ylims_change)

        stream = streams[-1]
        key = keys[-1]

        self.t = stream.ndarray[0]
        flagpos = DataStream().KEYLIST.index('flag')
        firstcol = DataStream().KEYLIST.index(key[0])
        flag = stream.ndarray[flagpos]
        self.k = stream.ndarray[firstcol]

        self.canvas.draw()


    def power_plot(self, streamid, datadict, plotdict, sharey=False):
        """
        DEFINITION:
            embbed matplotlib figure in canvas

        PARAMETERS:
            streamids : list of ids to be plotted
            datadict : all data relevant information
            plotdict : all visualization parameter
            sharey : limit shownkeys to a single input to share y axis for multiple diagrams
        """
        debug = True

        colors = []
        symbols = []
        timecolumn = []
        errorbars = []
        yranges = []
        fill = []
        padding = []
        showpatch = []
        functions = []
        title = ''
        legend = {}
        grid = False
        patch = {}
        annotate = False
        alpha = 0.5
        ylabelposition = None
        yscale = None
        functionfmt = "r-"
        separate = True

        datacont = datadict.get(streamid)
        stream = datacont.get('dataset')
        plotcont = plotdict.get(streamid)
        keys = plotcont.get('shownkeys')
        colors = plotcont.get('colors')
        fill = plotcont.get('fill')
        title=plotcont.get('title')
        legend=plotcont.get('legend')
        grid=plotcont.get('grid')
        alpha=plotcont.get('alpha')
        yscale=None

        if sharey and len(keys) > 0:
            separate = False

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

        self.figure, self.axes = mp.psplot(data=stream, keys=keys, colors=colors, title=title, legend=legend, grid=grid,
              ylabelposition=ylabelposition, alpha=alpha, figure=self.figure, separate=separate)

        self.axlist = self.figure.axes

        #get current xlimits:
        for idx, ax in enumerate(self.axlist):
            self.xlimits = ax.get_xlim()
            self.ylimits = ax.get_ylim()
            ax.callbacks.connect('xlim_changed', on_xlims_change)
            ax.callbacks.connect('ylim_changed', on_ylims_change)

        self.canvas.draw()


    def spec_plot(self, streamid, datadict, plotdict, sharey=False):
        """
        DEFINITION:
            embbed matplotlib spectrogram in canvas
            TODO: add the additional specgram parameters to plotdict

        PARAMETERS:
            streamids : list of ids to be plotted
            datadict : all data relevant information
            plotdict : all visualization parameter
        """
        yrange = []
        title = ''
        grid = False
        ylabelposition = None
        yscale = ['log']

        datacont = datadict.get(streamid)
        stream = datacont.get('dataset')
        plotcont = plotdict.get(streamid)
        keys = plotcont.get('shownkeys')

        title=plotcont.get('title')
        grid=plotcont.get('grid')

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

        print ("NOT YET WORKING:", stream, keys, grid, ylabelposition, yscale)
        self.figure, self.axes = mp.spplot(data=stream, keys=keys, title=title, grid=grid,
              ylabelposition=ylabelposition, yscale=yscale, figure=self.figure)
        print ("HERE")

        self.axlist = self.figure.axes

        #get current xlimits:
        for idx, ax in enumerate(self.axlist):
            self.xlimits = ax.get_xlim()
            self.ylimits = ax.get_ylim()
            ax.callbacks.connect('xlim_changed', on_xlims_change)
            ax.callbacks.connect('ylim_changed', on_ylims_change)

        self.canvas.draw()


    def initial_plot(self):
        """
        DEFINITION:
            loads an image for the startup screen
        """
        try:
            self.axes = self.figure.add_subplot(111)
            plt.axis("off") # turn off axis
            try:
                script_dir = os.path.dirname(__file__)
                startupimage = os.path.join(script_dir,'magpy2.png')
                img = imread(startupimage)
                self.axes.imshow(img)
            except:
                pass
            self.canvas.draw()
        except:
            pass

    def link_rep(self):
        return ReportPage(self)

    @deprecated("Replaced by link_rep")
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


class MenuPanel(scrolled.ScrolledPanel):
    """
    DESCRIPTION
        contains all methods for the right menu panel and their insets
        All methods are listed in the MainFrame class
    """
    def __init__(self, *args, **kwds):
        scrolled.ScrolledPanel.__init__(self, *args, **kwds)
        # Create pages on MenuPanel
        nb = wx.Notebook(self,-1)
        self.str_page = StreamPage(nb)
        self.fla_page = FlagPage(nb)
        self.met_page = MetaPage(nb)
        self.ana_page = AnalysisPage(nb)
        self.abs_page = AbsolutePage(nb)
        self.rep_page = ReportPage(nb)
        self.com_page = MonitorPage(nb)
        nb.AddPage(self.str_page, "Data")
        nb.AddPage(self.fla_page, "Flags")
        nb.AddPage(self.met_page, "Meta")
        nb.AddPage(self.ana_page, "Analysis")
        nb.AddPage(self.abs_page, "DI")
        nb.AddPage(self.rep_page, "Report")
        nb.AddPage(self.com_page, "Live")

        sizer = wx.BoxSizer()
        sizer.Add(nb, 1, wx.EXPAND)
        self.SetSizer(sizer)
        self.SetupScrolling()
        #self.nb.RemovePage(4)
        #self.stats_page.Hide()


class MainFrame(wx.Frame):
    def __init__(self, *args, **kwds):
        kwds["style"] = wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, *args, **kwds)
        # The Splitted Window
        self.sp = wx.SplitterWindow(self, -1, style=wx.SP_3D|wx.SP_BORDER)
        self.sp2 = wx.SplitterWindow(self.sp, -1, style=wx.SP_3D|wx.SP_BORDER)
        self.plot_p = PlotPanel(self.sp2,-1)
        self.menu_p = MenuPanel(self.sp2,-1)
        self.sp2.SplitVertically(self.plot_p, self.menu_p, 800)
        self.stats_p = StatisticsPanel(self.sp)
        self.sp.SplitHorizontally(self.sp2, self.stats_p, 800)
        self.sp.Unsplit(self.stats_p)

        # status bar
        # ----------------------------
        pub.subscribe(self.changeStatusbar, 'changeStatusbar')
        self.StatusBar = self.CreateStatusBar(2, wxSTB_SIZEGRIP)
        # Update Status Bar with plot values
        self.plot_p.canvas.mpl_connect('motion_notify_event', self._update_cursor_status)
        # Allow flagging with double click
        #self.plot_p.canvas.mpl_connect('button_press_event', self.OnFlagClick)

        # basic configuration
        # ----------------------------
        # New in Version 2.0 onwards
        # use dictionaries for
        # a) current data stream data and characteristics (add an ID using hashlib)
        #     - datastream, availablekeys, sampling rate, coverage, amount, start and end time
        #       components, shownkeys, paths, all data specific options in GUI
        #self.datadict = {}
        # b) general settings, db and all button and selections (refer to stream dicts ID)
        #     - everything from original ini, magpyversion, db
        #self.guidict
        # c) analysis specific settings, DI default parameters etc
        #     - DI analysis, default function fitting values, default station code, mqtt settings, preferred
        #     webservices, magpyversion for updates, etc
        #self.analysisdict
        # d) current plotting options (again referring to stream ID)
        #     - all options of tsplot
        #self.plotdict = {}

        # read any existing guidict from configuration file
        # if not existing fill with default values
        basepath = os.path.expanduser('~')
        cfgpath = os.path.join(basepath, '.magpy')
        self.guicfg = os.path.join(cfgpath, 'xmagpy_gui.cfg')
        self.analysiscfg = os.path.join(cfgpath, 'xmagpy_analysis.cfg')
        guid, anald = self._set_default_initialization()
        loaded_guidict = read_dict(self.guicfg)            # stored as config
        loaded_analysisdict = read_dict(self.analysiscfg) # stored as config
        if loaded_guidict:
            # Replace elements from default by loaded data (this will easily allow future updates)
            for sub in guid:
                # checking if key present in other dictionary
                if sub in loaded_guidict:
                    guid[sub] = loaded_guidict[sub]
        self.guidict = guid
        if loaded_analysisdict:
            # Replace elements from default by loaded data (this will easily allow future updates)
            for sub in anald:
                # checking if key present in other dictionary
                if sub in loaded_analysisdict:
                    anald[sub] = loaded_analysisdict[sub]
        self.analysisdict = anald

        # create empty dictionaries for holding data is its visualization parameters
        # ----------------------------
        self.datadict = {}
        self.baselinedict = {}
        self.plotdict = {}

        # set some general (data independent) state variables to be changed
        # ----------------------------
        self.magpystate = {}
        self.magpystate['dbtuple'] = (self.guidict.get('dbhost', ''), self.guidict.get('dbuser', ''),
                                       base64.b64decode(self.guidict.get('dbpwd', '')),
                                       self.guidict.get('dbname', ''))
        self.magpystate['databaseconnected'] = False
        self.magpystate['currentpath'] = self.guidict.get('dirname')
        self.magpystate['filename'] = ''
        self.magpystate['source'] = ''

        # REMOVE
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
        self.baselinedictlst = [] # variable to hold info on loaded DI streams for baselinecorrection
        self.baselineidxlst = []


        self.active_id = 0
        self.active_baseid = 0
        self.active_didata = {}

        # Menu Bar
        # --------------
        self._create_menu_bar()
        self.__set_properties()

        # bind menu controls to methods
        # --------------
        self._bind_controls()

        # Connect to database
        db, success = self._db_connect(*self.magpystate.get('dbtuple'))
        self.magpystate['db'] = db
        self.magpystate['databaseconnected'] = success

        # Disable yet unavailable buttons and items
        # --------------------------
        self._deactivate_controls()


    def _check_db(self, level='minimal'):
        """
        DESCRIPTION
            Checks if the database is still connected and eventually reconnect
            TODO: check if really necessary
        USED BY
            many database methods
        """
        if not self.magpystate.get('databaseconnected'):
            try:
                dbank, connect = self._db_connect(*self.magpystate.get('dbtuple'))
                dbank.db.info(destination='stdout',level='minimal')#,level='full') # use level ='minimal'
            except:
                logger.info("Database not connected any more -- reconnecting")
                dbank, connect = self._db_connect(*self.magpystate.get('dbtuple'))


    def _set_default_initialization(self):
        """
        DESCRIPTION
            Default definition of important analysis parameters. These defaults will be used when firstly starting XMagPy.
            Parameters can be changed and saved. If a new version of MagPy, eventually including new or altered parameters
            is installed, then new inputs will be updated without erasing the already modified initialization data
        USED BY
            MainFrame.__init__
        RETURNS
            two dictionaries
        """
        guidict = {}
        analysisdict = {}
        basepath = os.path.expanduser('~')
        # Updating version info in file
        from magpy.version import __version__
        guidict['magpyversion'] = __version__
        guidict['dbname'] = ''
        guidict['dbuser'] = 'Max'
        passwd = 'secret'
        passwd = passwd.encode()
        pwd = base64.b64encode(passwd)
        guidict['dbpwd'] = pwd.decode()
        guidict['dbhost'] = 'localhost'
        guidict['dirname'] = basepath
        guidict['exportpath'] = basepath
        guidict['experimental'] = False
        defaultstation = 'WIC'
        analysisdict['defaultstation'] = defaultstation
        analysisdict['magpyversion'] = __version__
        # calculation
        analysisdict['basefilter'] = 'gaussian'
        analysisdict['fitfunction'] = 'spline'
        analysisdict['fitdegree'] = '5'
        analysisdict['fitknotstep'] = '0.3'
        # monitor
        analysisdict['martasscantime'] = '20'
        favoritemartas = {}
        favoritemartas['conrad'] = 'https://cobs.zamg.ac.at'
        favoritemartas['example'] = 'https://www.example.com'
        analysisdict['favoritemartas'] = favoritemartas
        # DI analysis
        analysisdict['baselinedirect'] = False
        content = {}
        content['divariopath'] = os.path.join(basepath, '*')
        content['discalarpath'] = os.path.join(basepath, '*')
        content['diexpD'] = 0.0
        content['diexpI'] = 0.0
        content['didatapath'] = basepath
        content['divariourl'] = ''
        content['discalarurl'] = ''
        content['divarioDBinst'] = '1'
        content['discalarDBinst'] = '4'
        content['divariosource'] = 0
        content['discalarsource'] = 0
        content['diusedb'] = False
        content['divariocorr'] = False
        content['diid'] = ''
        content['ditype'] = 'manual'
        content['diazimuth'] = 0.0
        content['dipier'] = 'A2'
        content['dialpha'] = 0.0
        content['dibeta'] = 0.0
        content['dideltaF'] = 0.0
        content['dideltaD'] = 0.0
        content['dideltaI'] = 0.0
        content['blvoutput'] = 'HDZ'
        content['fluxgateorientation'] = 'inline'
        content['diannualmean'] = []
        content['didbadd'] = False
        content['scalevalue'] = True
        content['double'] = True
        content['order'] = 'MU,MD,EU,WU,ED,WD,NU,SD,ND,SU'
        stationcontent = {}
        stationcontent[defaultstation] = content
        analysisdict['stations'] = stationcontent
        bookmarks = {}
        bookmarks['WDC'] = 'ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc'
        bookmarks[
            'Conrad'] = 'http://www.conrad-observatory.at/zamg/index.php/downloads-en/category/13-definite2015?download=66:wic-2015-0000-pt1m-4'
        bookmarks['GFZ Kp'] = 'http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab'
        analysisdict['bookmarks'] = bookmarks
        webservices = {}
        cobsws = {'magnetism': {'address': 'https://cobs.zamg.ac.at/gsa/webservice/query.php',
                                'format': ['iaga2002', 'json', 'csv'],
                                'ids': ['WIC', 'GAM', 'SWZ'],
                                'elements': 'X,Y,Z,F',
                                'sampling': ['60'],
                                'type': ['adjusted']
                                },
                  'meteorology': {'address': 'https://cobs.zamg.ac.at/gsa/webservice/query.php',
                                  'format': ['ascii', 'json', 'csv'],
                                  'ids': ['WIC', 'SGO'],
                                  'sampling': ['60'],
                                  'type': ['adjusted']
                                  },
                  'commands': {'format': 'of'}
                  }
        usgsws = {'magnetism': {'address': 'https://geomag.usgs.gov/ws/data/',
                                'format': ['iaga2002', 'json'],
                                'ids': ['BOU', 'BDT', 'TST', 'BRW', 'BRT', 'BSL', 'CMO', 'CMT', 'DED', 'DHT', 'FRD',
                                        'FRN', 'GUA', 'HON', 'NEW', 'SHU', 'SIT', 'SJG', 'TUC', 'USGS', 'BLC', 'BRD',
                                        'CBB', 'EUA', 'FCC', 'IQA', 'MEA', 'OTT', 'RES', 'SNK', 'STJ', 'VIC', 'YKC',
                                        'HAD', 'HER', 'KAK'],
                                'elements': 'X,Y,Z,F',
                                'sampling': ['60', '1', '3600'],
                                'type': ['variation', 'adjusted', 'quasi-definitive', 'definitive']
                                },
                  'basevalues': {'address': 'https://geomag.usgs.gov/baselines/observation.json.php',
                                 'format': ['json'],
                                 'ids': ['BOU', 'BDT', 'TST', 'BRW', 'BRT', 'BSL', 'CMO', 'CMT', 'DED', 'DHT', 'FRD',
                                         'FRN', 'GUA', 'HON', 'NEW', 'SHU', 'SIT', 'SJG', 'TUC', 'USGS', 'BLC', 'BRD',
                                         'CBB', 'EUA', 'FCC', 'IQA', 'MEA', 'OTT', 'RES', 'SNK', 'STJ', 'VIC', 'YKC',
                                         'HAD', 'HER', 'KAK']
                                 },
                  'commands': {}
                  }
        imws = {'magnetism': {'address': 'https://imag-data-staging.bgs.ac.uk/GIN_V1/GINServices',
                              'format': ['iaga2002'],
                              'ids': ['WIC', 'ABK', 'AIA', 'API', 'ARS', 'ASC', 'ASP', 'BDV', 'BEL', 'BFE', 'BFO',
                                      'CKI', 'CNB', 'CNH', 'CPL', 'CSY', 'CTA', 'CYG', 'DOU', 'ESK', 'EY2', 'EYR',
                                      'FUR', 'GAN', 'GCK', 'GNA', 'GNG', 'GZH', 'HAD', 'HBK', 'HER', 'HLP', 'HRN',
                                      'HUA', 'HYB', 'IRT', 'ISK', 'IZN', 'JCO', 'KDU', 'KEP', 'KHB', 'KIV', 'KMH',
                                      'LER', 'LON', 'LRM', 'LVV', 'LYC', 'MAB', 'MAW', 'MCQ', 'MGD', 'MZL', 'NCK',
                                      'NGK', 'NUR', 'NVS', 'ORC', 'PAG', 'PEG', 'PET', 'PIL', 'PST', 'SBA', 'SBL',
                                      'SOD', 'SON', 'THY', 'TSU', 'UPS', 'VAL', 'WMQ', 'WNG', 'YAK'],
                              'elements': 'X,Y,Z,F',
                              'sampling': ['minute', 'second'],
                              'type': ['adj-or-rep']
                              },
                'extra': {'baseextension': '',
                          'additionalelements': 'request=GetData',
                          'displaytype': 'download',
                          'mintime': 'day'
                          },
                'commands': {'format': 'Format',
                             'id': 'observatoryIagaCode',
                             'starttime': 'dataStartDate',
                             'endtime': 'dataEndDate',
                             'type': 'publicationState',
                             'sampling_period': 'samplesPerDay'
                             }
                }
        webservices['conrad'] = cobsws
        webservices['usgs'] = usgsws
        webservices['imws'] = imws
        analysisdict['webservices'] = webservices
        analysisdict['defaultwebservice'] = 'imws'

        return guidict, analysisdict

    def __set_properties(self):
        """
        DESCRIPTION
            Setting some general panel properties like starting size and minimum sizes of panels
        USED BY
            MainFrame.__init__
        """
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
        self.stats_p.SetMinSize((100, 100))


    def _create_menu_bar(self):
        """
        DESCRIPTION
            Creating all items of the main menu bar
        USED BY
            MainFrame.__init__
        """
        # Existing shortcuts: o,d,u,s,e,q,c,b,l,a,r,m,i,v  (a,b,c,d,e,f,(gh),i,(jk),l,m,(n),o
        self.MainMenu = wx.MenuBar()
        # ## File Menu
        self.fileMenu = wx.Menu()
        self.fileOpen = wx.MenuItem(self.fileMenu, 101, "&Open File...\tCtrl+F", "Open file", wx.ITEM_NORMAL)
        self.fileMenu.Append(self.fileOpen)
        self.dirOpen = wx.MenuItem(self.fileMenu, 102, "Select &Directory...\tCtrl+D", "Select an existing directory", wx.ITEM_NORMAL)
        self.fileMenu.Append(self.dirOpen)
        self.fileMenu.AppendSeparator()
        self.webServiceOpen = wx.MenuItem(self.fileMenu, 103, "Open &WebService...\tCtrl+W", "Get webservice data", wx.ITEM_NORMAL)
        self.fileMenu.Append(self.webServiceOpen)
        self.fileMenu.AppendSeparator()
        self.webOpen = wx.MenuItem(self.fileMenu, 104, "Open general &URL...\tCtrl+U", "Get data from the internet", wx.ITEM_NORMAL)
        self.fileMenu.Append(self.webOpen)
        self.fileMenu.AppendSeparator()
        self.dbOpen = wx.MenuItem(self.fileMenu, 105, "&Open DB table...\tCtrl+S", "Select a MySQL database", wx.ITEM_NORMAL)
        self.fileMenu.Append(self.dbOpen)
        self.dbOpen.Enable(False)
        self.fileMenu.AppendSeparator()
        self.exportData = wx.MenuItem(self.fileMenu, 106, "&Export data...\tCtrl+E", "Export data to a file", wx.ITEM_NORMAL)
        self.fileMenu.Append(self.exportData)
        self.exportData.Enable(False)
        self.fileMenu.AppendSeparator()
        self.fileQuitItem = wx.MenuItem(self.fileMenu, wx.ID_EXIT, "&Quit\tCtrl+Q", "Quit the program", wx.ITEM_NORMAL)
        self.fileMenu.Append(self.fileQuitItem)
        self.MainMenu.Append(self.fileMenu, "&File")
        # ## Database Menu
        self.DatabaseMenu = wx.Menu()
        self.DBConnect = wx.MenuItem(self.DatabaseMenu, 201, "&Connect MySQL DB...\tCtrl+O", "Connect Database", wx.ITEM_NORMAL)
        self.DatabaseMenu.Append(self.DBConnect)
        self.DatabaseMenu.AppendSeparator()
        self.DBInit = wx.MenuItem(self.DatabaseMenu, 202, "&Initialize a new MySQL DB...\tCtrl+Z", "Initialize Database", wx.ITEM_NORMAL)
        self.DatabaseMenu.Append(self.DBInit)
        self.MainMenu.Append(self.DatabaseMenu, "Data&base")
        # ## DI Menu
        self.DIMenu = wx.Menu()
        self.DIInputSheet = wx.MenuItem(self.DIMenu, 501, "O&pen DI input sheet...\tCtrl+P", "Input sheet...", wx.ITEM_NORMAL)
        self.DIMenu.Append(self.DIInputSheet)
        self.MainMenu.Append(self.DIMenu, "D&I")
        # ## Stream Operations
        self.StreamOperationsMenu = wx.Menu()
        self.StreamAddListSelect = wx.MenuItem(self.StreamOperationsMenu, 601, "Store &working state...\tCtrl+W", "Add data set", wx.ITEM_NORMAL)
        self.StreamOperationsMenu.Append(self.StreamAddListSelect)
        self.StreamOperationsMenu.AppendSeparator()
        self.StreamListSelect = wx.MenuItem(self.StreamOperationsMenu, 602, "Access data &memory...\tCtrl+M", "Select data set(s)", wx.ITEM_NORMAL)
        self.StreamOperationsMenu.Append(self.StreamListSelect)
        self.MainMenu.Append(self.StreamOperationsMenu, "Memo&ry")
        # ## Data Checker
        self.CheckDataMenu = wx.Menu()
        self.CheckDefinitiveDataSelect = wx.MenuItem(self.CheckDataMenu, 701, "Check &definitive data...\tCtrl+H", "Check data", wx.ITEM_NORMAL)
        self.CheckDataMenu.Append(self.CheckDefinitiveDataSelect)
        self.MainMenu.Append(self.CheckDataMenu, "Spe&cials")
        # ## Options Menu
        self.OptionsMenu = wx.Menu()
        self.OptionsInitItem = wx.MenuItem(self.OptionsMenu, 401, "&Basic parameter\tCtrl+B", "Modify general defaults (e.g. DB, paths)", wx.ITEM_NORMAL)
        self.OptionsMenu.Append(self.OptionsInitItem)
        self.OptionsMenu.AppendSeparator()
        self.OptionsPlotItem = wx.MenuItem(self.OptionsMenu, 402, "Plotting parameter\tCtrl+I", "Modify plotting parameters", wx.ITEM_NORMAL)
        self.OptionsMenu.Append(self.OptionsPlotItem)
        self.OptionsMenu.AppendSeparator()
        self.OptionsDIItem = wx.MenuItem(self.OptionsMenu, 403, "D&I parameter\tCtrl+I", "Modify DI related parameters (e.g. thresholds, paths, input sheet layout)", wx.ITEM_NORMAL)
        self.OptionsMenu.Append(self.OptionsDIItem)
        self.MainMenu.Append(self.OptionsMenu, "&Options")
        self.HelpMenu = wx.Menu()
        self.HelpAboutItem = wx.MenuItem(self.HelpMenu, 301, "&About...", "Display general information about the program", wx.ITEM_NORMAL)
        self.HelpMenu.Append(self.HelpAboutItem)
        self.HelpReadFormatsItem = wx.MenuItem(self.HelpMenu, 302, "Read Formats...", "Supported data formats to read", wx.ITEM_NORMAL)
        self.HelpMenu.Append(self.HelpReadFormatsItem)
        self.HelpWriteFormatsItem = wx.MenuItem(self.HelpMenu, 303, "Write Formats...", "Supported data formats to write", wx.ITEM_NORMAL)
        self.HelpMenu.Append(self.HelpWriteFormatsItem)
        self.HelpLogFileSelect = wx.MenuItem(self.HelpMenu, 304, "Open MagP&y log...\tCtrl+Y", "Open log", wx.ITEM_NORMAL)
        self.HelpMenu.Append(self.HelpLogFileSelect)
        self.MainMenu.Append(self.HelpMenu, "&Help")
        self.SetMenuBar(self.MainMenu)
        # Menu Bar end


    def _set_plot_parameter(self, keylist=None):
        """
        DESCRIPTION
            Create the default dictionary with plotting parameters
            IMPORTANT:
            The plotting parameters are "single plot" typs: ['x','y'] instead of [['x','y']]
        USED BY
            MainFrame.__init__
        """
        self.menu_p.fla_page.annotateCheckBox.SetValue(True)

        shownkeys = []
        colors = ['gray']*15
        symbols = ['-']*15

        # TODO Remove the following
        self.colors = [colors]
        self.resolution=None
        self.monitorSource=None

        # please note: symbol and colorlists are defined in ActivateControls
        # TODO:  add options for psd and specgram
        plotopt = {'yranges' : [],
                        'padding' : [],
                        'shownkeys' : shownkeys,
                        'symbols' : symbols,
                        'colors' : colors,
                        'title' : None,
                        'legend' : {},
                        'grid' : True,
                        'patch' : {},
                        'timecolumn' : 'time',
                        'annotate' : False,
                        'fill' : [],
                        'showpatch' : [],
                        'errorbars' : [],
                        'functions' : [],
                        'functionfmt' : "r-",
                        'xlabelposition' : None,
                        'ylabelposition' : None,
                        'yscale' : None,
                        'dateformatter' : None,
                        'force' : False,
                        'alpha' : 0.5
                        }
        return plotopt


    def _bind_controls(self):
        """
        DESCRIPTION
            Bind the menu bar items to control methods
        # USED BY
            MainFrame.__init__
        """
        # BindingControls on the menu
        # File menu
        self.Bind(wx.EVT_MENU, self.file_on_open_dir, self.dirOpen)
        self.Bind(wx.EVT_MENU, self.file_on_open_file, self.fileOpen)
        self.Bind(wx.EVT_MENU, self.file_on_open_url, self.webOpen)
        self.Bind(wx.EVT_MENU, self.file_on_open_webservice, self.webServiceOpen)
        self.Bind(wx.EVT_MENU, self.file_on_open_db, self.dbOpen)
        self.Bind(wx.EVT_MENU, self.file_export_data, self.exportData)
        self.Bind(wx.EVT_MENU, self.file_on_quit, self.fileQuitItem)
        self.Bind(wx.EVT_CLOSE, self.file_on_quit)  #Bind the EVT_CLOSE event to FileQuit()
        # Database Menu
        self.Bind(wx.EVT_MENU, self.db_on_connect, self.DBConnect)
        self.Bind(wx.EVT_MENU, self.db_on_init, self.DBInit)
        # Memory Menu
        self.Bind(wx.EVT_MENU, self.memory_select, self.StreamListSelect)
        self.Bind(wx.EVT_MENU, self.memory_add, self.StreamAddListSelect) # remove
        # DI Menu
        self.Bind(wx.EVT_MENU, self.di_input_sheet, self.DIInputSheet)
        # Options Menu
        self.Bind(wx.EVT_MENU, self.options_init, self.OptionsInitItem)
        self.Bind(wx.EVT_MENU, self.options_plot, self.OptionsPlotItem)
        self.Bind(wx.EVT_MENU, self.options_di, self.OptionsDIItem)
        # Help Menu
        self.Bind(wx.EVT_MENU, self.help_about, self.HelpAboutItem)
        self.Bind(wx.EVT_MENU, self.help_read_formats, self.HelpReadFormatsItem)
        self.Bind(wx.EVT_MENU, self.help_write_formats, self.HelpWriteFormatsItem)
        self.Bind(wx.EVT_MENU, self.help_open_log, self.HelpLogFileSelect)
        # Specials menu
        self.Bind(wx.EVT_MENU, self.OnCheckDefinitiveData, self.CheckDefinitiveDataSelect)
        # BindingControls on the panels
        #       Stream Page
        # ------------------------
        self.Bind(wx.EVT_BUTTON, self.data_onPreviousButton, self.menu_p.str_page.previousButton)
        self.Bind(wx.EVT_BUTTON, self.data_onNextButton, self.menu_p.str_page.nextButton)
        self.Bind(wx.EVT_BUTTON, self.data_onTrimButton, self.menu_p.str_page.trimStreamButton)
        self.Bind(wx.EVT_BUTTON, self.data_onSelectKeys, self.menu_p.str_page.selectKeysButton)
        self.Bind(wx.EVT_BUTTON, self.data_onDropKeys, self.menu_p.str_page.dropKeysButton)
        self.Bind(wx.EVT_BUTTON, self.data_onExtractData, self.menu_p.str_page.extractValuesButton)
        self.Bind(wx.EVT_BUTTON, self.data_onGetGapsButton, self.menu_p.str_page.getGapsButton)
        self.Bind(wx.EVT_RADIOBOX, self.data_onChangeComp, self.menu_p.str_page.compRadioBox)
        self.Bind(wx.EVT_RADIOBOX, self.data_onChangeSymbol, self.menu_p.str_page.symbolRadioBox)
        self.Bind(wx.EVT_CHECKBOX, self.data_onStatsCheckBox, self.menu_p.str_page.activateStatsCheckBox)
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
        self.Bind(wx.EVT_CHECKBOX, self.onAnnotateCheckBox, self.menu_p.fla_page.annotateCheckBox)
        self.Bind(wx.EVT_BUTTON, self.onFlagmodButton, self.menu_p.fla_page.flagmodButton)
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
        self.Bind(wx.EVT_BUTTON, self.meta_onGetDBButton, self.menu_p.met_page.getDBButton)
        self.Bind(wx.EVT_BUTTON, self.meta_onPutDBButton, self.menu_p.met_page.putDBButton)
        self.Bind(wx.EVT_BUTTON, self.meta_onDataButton, self.menu_p.met_page.MetaDataButton)
        self.Bind(wx.EVT_BUTTON, self.meta_onSensorButton, self.menu_p.met_page.MetaSensorButton)
        self.Bind(wx.EVT_BUTTON, self.meta_onStationButton, self.menu_p.met_page.MetaStationButton)
        #        Analysis Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.analysis_onDerivativeButton, self.menu_p.ana_page.derivativeButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onRotationButton, self.menu_p.ana_page.rotationButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onFitButton, self.menu_p.ana_page.fitButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onMeanButton, self.menu_p.ana_page.meanButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onMaxButton, self.menu_p.ana_page.maxButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onMinButton, self.menu_p.ana_page.minButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onOffsetButton, self.menu_p.ana_page.offsetButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onFilterButton, self.menu_p.ana_page.filterButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onSmoothButton, self.menu_p.ana_page.smoothButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onResampleButton, self.menu_p.ana_page.resampleButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onActivityButton, self.menu_p.ana_page.activityButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onDeltafButton, self.menu_p.ana_page.deltafButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onCalcfButton, self.menu_p.ana_page.calcfButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onBaselineButton, self.menu_p.ana_page.baselineButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onDailyMeansButton, self.menu_p.ana_page.dailyMeansButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onApplyBCButton, self.menu_p.ana_page.applyBCButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onPowerButton, self.menu_p.ana_page.powerButton)
        self.Bind(wx.EVT_BUTTON, self.analysis_onSpectrumButton, self.menu_p.ana_page.spectrumButton)
        #        DI Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.di_onLoadDI, self.menu_p.abs_page.loadDIButton)
        self.Bind(wx.EVT_BUTTON, self.di_onDefineVarioScalar, self.menu_p.abs_page.defineVarioScalarButton)
        self.Bind(wx.EVT_BUTTON, self.di_onDIParameter, self.menu_p.abs_page.defineParameterButton)
        self.Bind(wx.EVT_BUTTON, self.di_onDIAnalyze, self.menu_p.abs_page.AnalyzeButton)
        self.Bind(wx.EVT_BUTTON, self.onSaveDIData, self.menu_p.abs_page.SaveLogButton)
        self.Bind(wx.EVT_BUTTON, self.onClearDIData, self.menu_p.abs_page.ClearLogButton)
        #        Report Page
        # --------------------------
        self.Bind(wx.EVT_BUTTON, self.report_onSaveLogButton, self.menu_p.rep_page.saveLoggerButton)
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


    def _db_connect(self, host, user, passwd, dbname, debug=False):
        """
        DESCRIPTION
            (re-)connect a MagPy database
        USED BY
            MainFrame.__init__
        OPTIONS
        :param host:
        :param user:
        :param passwd:
        :param dbname:
        :param debug:
        :return:
             a database object and a simple connected bool
        """
        databaseconnected = False
        try:
            #self.db.close()
            db = self.magpystate.get('db')
            db.close()
        except:
            pass
        # check whether host can be pinged (faster)
        plat_form = platform.platform()
        if plat_form.startswith('linux') or plat_form.startswith('Linux'):
            response = os.system("ping -c 1 -w2 {} > /dev/null 2>&1".format(host))
        else:
            response = 0

        if response == 0:
            try:
                db = database.DataBank(host=host, user=user, password=passwd, database=dbname)
            except:
                db = False
        else:
            db = False

        if db:
            self.dbOpen.Enable(True)
            self.menu_p.rep_page.logMsg('- MySQL Database {} on {} connected.'.format(dbname,host))
            self.changeStatusbar("Database {} successfully connected".format(dbname))
            # Set variable to True
            databaseconnected = True
            # enable MARCOS button
            self.menu_p.com_page.getMARCOSButton.Enable()
        else:
            self.menu_p.rep_page.logMsg('- MySQL Database access failed.')
            self.changeStatusbar("Database connection failed")
            # disable MARCOS button
            self.menu_p.com_page.getMARCOSButton.Disable()
        return db, databaseconnected

    def _determine_decimals(self, decimal):
        """
        DESCRIPTION
            mini method to get some decimal number for rounding
        APPLICATION
            decimaldet(0.002) will return 2
        :param decimal:
        :return:
        """
        return 0.0 if decimal == 0 else -floor(np.log10(np.abs(decimal))) - 1


    def _update_dictionary(self, dictionary, type='analysis'):
        """
        DESCRIPTION
            Will update the essential dictionary
            Should replace initParameter
        USED BY
            MainFrame.__init__
        """
        pass

    @deprecated("replaced by commands in _init_")
    def initParameter(self, dictionary):
        # Variable initializations
        pwd = dictionary.get('passwd')
        #self.passwd = base64.b64decode(pwd)
        #contains last visited directory
        self.last_dir = dictionary.get('dirname', '')

        self.dipathlist = dictionary.get('dipathlist','')
        self.options = dictionary
        self.options['passwd'] = base64.b64decode(pwd)


    def _update_cursor_status(self, event):
        """
        DESCRIPTION
            Motion event for displaying values under cursor.
        USED BY
            MainFrame.__init__
        """
        if not event.inaxes or not self.menu_p.str_page.trimStreamButton.IsEnabled():
            self.changeStatusbar("Ready")
            return
        pickX, pickY = event.xdata, event.ydata
        time = num2date(pickX).replace(tzinfo=None)
        possible_val = []
        possible_key = []
        idx = (np.abs(self.plot_p.t - time)).argmin()

        try:
            time = time.strftime("%Y-%m-%d %H:%M:%S %Z")
        except:
            time = time
        try:
            shownkeys = self.plotdict.get(self.active_id).get('shownkeys')
            data = self.datadict.get(self.active_id).get('dataset')
            for elem in shownkeys:
                keydata = data._get_column(elem)
                ul = np.nan
                ll = np.nan
                if not np.all(np.isnan(keydata)):
                    ul = np.nanmax(keydata)
                    ll = np.nanmin(keydata)
                if ll < pickY < ul:
                    possible_key += elem
                    possible_val += [keydata[idx]]
            idy = (np.abs(possible_val - pickY)).argmin()
            key = possible_key[idy]
            val = possible_val[idy]
            colname = data.header.get('col-'+key, '')
            if not colname == '':
                key = colname
            self.changeStatusbar("time: " + str(time) + "  |  " + key + " data value: " + str(val))
        except:
            self.changeStatusbar("time: " + str(time) + "  |  ? data value: ?")


    @deprecated("Will be replaced by _deactivate_controls")
    def DeactivateAllControls(self):
        return self._deactivate_controls()

    def _deactivate_controls(self):
        """
        DESCRIPTION
            To be used at start and when an empty stream is loaded
            Deactivates all control elements before reinitializing dependent on the new data set
        USED BY
            MainFrame.__init__
        """
        def display(value):
            # Used for displaying content of database
            if isinstance(value, list):
                dis = str(value[-1])
            else:
                dis = str(value)
            return dis

        # Menu
        # ---------------------------------
        self.exportData.Enable(False)
        # Stream
        # ---------------------------------
        self.menu_p.str_page.previousButton.Disable()      # always
        self.menu_p.str_page.nextButton.Disable()          # always
        self.menu_p.str_page.pathTextCtrl.Disable()        # remain disabled
        self.menu_p.str_page.fileTextCtrl.Disable()        # remain disabled
        self.menu_p.str_page.startDatePicker.Disable()     # always
        self.menu_p.str_page.startTimePicker.Disable()     # always
        self.menu_p.str_page.endDatePicker.Disable()       # always
        self.menu_p.str_page.endTimePicker.Disable()       # always
        self.menu_p.str_page.trimStreamButton.Disable()    # always
        self.menu_p.str_page.selectKeysButton.Disable()    # always
        self.menu_p.str_page.dropKeysButton.Disable()    # always
        self.menu_p.str_page.extractValuesButton.Disable() # always
        self.menu_p.str_page.getGapsButton.Disable()       # activated if not MagPyDI
        self.menu_p.str_page.compRadioBox.Disable()        # activated if xyz,hdz or idf
        self.menu_p.str_page.symbolRadioBox.Disable()      # activated if less then 2000 points, active if DI data
        self.menu_p.str_page.activateStatsCheckBox.Disable()         # always


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
        self.menu_p.fla_page.annotateCheckBox.Disable()    # activated if annotation are present
        self.menu_p.fla_page.flagmodButton.Disable()       # always


        # Meta
        self.menu_p.met_page.getDBButton.Disable()         # activated when DB is connected
        self.menu_p.met_page.putDBButton.Disable()         # activated when DB is connected
        self.menu_p.met_page.MetaDataButton.Disable()      # remain disabled
        self.menu_p.met_page.MetaSensorButton.Disable()    # remain disabled
        self.menu_p.met_page.MetaStationButton.Disable()   # remain disabled

        # DI
        if not isinstance(self.active_didata,dict):
            self.menu_p.abs_page.AnalyzeButton.Disable()       # activate if DI data is present i.e. diTextCtrl contains data
        self.menu_p.abs_page.loadDIButton.Enable()         # remain enabled
        self.menu_p.abs_page.defineVarioScalarButton.Enable()    # remain enabled
        self.menu_p.abs_page.ClearLogButton.Disable()      # Activate if log contains text
        self.menu_p.abs_page.SaveLogButton.Disable()      # Activate if log contains text

        self.menu_p.abs_page.VarioSourceLabel.SetLabel("Vario: from unknown source")
        self.menu_p.abs_page.ScalarSourceLabel.SetLabel("Scalar: from unknown source")

        # TODO Why are the following lines in _deactivate?
        #sourcelist = ['file','database','webservice']
        #self.menu_p.abs_page.varioTextCtrl.SetValue(display(self.options.get('divariopath','')))
        #self.menu_p.abs_page.scalarTextCtrl.SetValue(display(self.options.get('discalarpath','')))
        #self.menu_p.abs_page.VarioSourceLabel.SetLabel("Vario: from {}".format(sourcelist[self.options.get('didictionary').get('divariosource')]))
        #self.menu_p.abs_page.ScalarSourceLabel.SetLabel("Scalar: from {}".format(sourcelist[self.options.get('didictionary').get('discalarsource')]))

        # Analysis
        self.menu_p.ana_page.rotationButton.Disable()      # if xyz magnetic data
        self.menu_p.ana_page.derivativeButton.Disable()    # always
        self.menu_p.ana_page.fitButton.Disable()           # always
        self.menu_p.ana_page.meanButton.Disable()          # always
        self.menu_p.ana_page.maxButton.Disable()           # always
        self.menu_p.ana_page.minButton.Disable()           # always
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
        self.menu_p.ana_page.dailyMeansButton.Disable()    # activated for DI data
        self.menu_p.ana_page.applyBCButton.Disable()       # activated if DataAbsInfo is present

        # Monitor
        self.menu_p.com_page.startMonitorButton.Disable()  # always
        self.menu_p.com_page.stopMonitorButton.Disable()   # always
        self.menu_p.com_page.saveMonitorButton.Disable()   # always
        self.menu_p.com_page.coverageTextCtrl.Disable()    # always
        self.menu_p.com_page.frequSlider.Disable()         # always
        self.menu_p.com_page.marcosLabel.SetBackgroundColour((255,23,23))
        self.menu_p.com_page.martasLabel.SetBackgroundColour((255,23,23))
        self.menu_p.com_page.marcosLabel.SetValue('not connected')
        self.menu_p.com_page.martasLabel.SetValue('not connected')


    @deprecated("Will be replaced by _activate_controls")
    def ActivateControls(self,stream):
        pass


    def _activate_controls(self, streamid):
        """
        DESCRIPTION
            Checks contents of stream and state of program.
            Activates controls in dependency of the checks
            Contains two submethods
                display
                checkbaseline
        USED BY

        CALLING
            ._deactivate_controls()
            .UpdatePlotOptions(shownkeys)
            .append_baseline

        """

        def display(value):
            """
            Used for displaying content of database
            """
            if isinstance(value, list):
                dis = str(value[-1])
            else:
                dis = str(value)
            return dis

        # get data dict
        datacont = self.datadict.get(streamid)
        stream = datacont.get('dataset')
        sensorid = datacont.get('sensorid')
        stationid = datacont.get('stationid')
        n = datacont.get('amount',0)
        sr = datacont.get('samplingrate',0)
        keys = datacont.get('keys',[])
        keystr = ','.join(keys)
        mintime = datacont.get('start')
        maxtime = datacont.get('end')
        coverage = datacont.get('coverage')
        comps = datacont.get('components')

        # Essential header info
        dataid = stream.header.get('DataID','')
        formattype = stream.header.get('DataFormat','')
        ### Formattype is not ideal for discriminating MagPyDI data contents as they can also be in CDF format
        ### Therefore use DataType in future: beginning with 11/2019
        contenttype = stream.header.get('DataType','')  # e.g. MagPyDI1.0
        if contenttype == '' and formattype == 'MagPyDI':
            contenttype = 'MagPyDI1.0'
            stream.header['DataType'] = contenttype

        # initially reset all controls
        self._deactivate_controls()
        if not n > 0:
            self.changeStatusbar("No data available")
            return

        self.menu_p.str_page.symbolRadioBox.Enable()

        # Delta
        deltas = False
        if 'dx' in keys or 'dy' in keys or 'dz' in keys or 'df' in keys:
            deltas = True

        absinfo = stream.header.get('DataAbsInfo',None)
        metadatatext = ''
        metasensortext = ''
        metastationtext = ''
        for key in stream.header:
            if key.startswith('Data'):
                 value = stream.header.get(key,'')
                 if not isinstance(value, basestring): # p3: str
                     try:
                         if methods.is_number(value):
                             pass
                         else:
                             value = 'object - contains complex data'
                     except:
                         value = 'object - contains complex data'
                 metadatatext += "{}: \t{}\n".format(key.replace('Data',''),value)
            if key.startswith('Sensor'):
                 metasensortext += "{}: \t{}\n".format(key.replace('Sensor',''),stream.header.get(key,'')) # key.replace('Sensor','')+': \t'+stream.header.get(key,'')+'\n'
            if key.startswith('Station'):
                 metastationtext += "{}: \t{}\n".format(key.replace('Station',''),stream.header.get(key,'')) #key.replace('Station','')+': \t'+stream.header.get(key,'')+'\n'

        # Append baselineinfo to baselinedictlist
        # ------------------------------------------
        if formattype == 'MagPyDI' or contenttype.startswith('MagPyDI'):
            if not sensorid and not dataid:
                basename = self.menu_p.str_page.fileTextCtrl.GetValue()
            elif dataid:
                basename = dataid
            else:
                basename = sensorid
            # Get a general single fitting function with default parameters and add this as starting parameter to
            # baselinedict
            functions = self.plotdict.get(streamid).get('functions')
            # clean functions:
            if functions:
                new_functions = []
                for func in functions[0]:
                    new_functions.append([{}, func[1], func[2], func[3], func[4], func[5], func[6], func[7], []])
                res = []
                [res.append(val) for val in new_functions if val not in res]
                functions = res
                # functions = [[ {}, mintime, maxtime, fitfunc, degree, knotstep, mintime, maxtime, [] ]]

                basedict = {'startdate': mintime,
                        'enddate': maxtime, 'filename': basename, 'streamid': streamid,
                        'function': functions}
                basestr = "{}{}{}{}{}".format(mintime,maxtime,basename,streamid,functions)
                m = hashlib.md5()
                m.update(basestr.encode('utf-8'))
                baseid = str(int(m.hexdigest(), 16))[0:12]
                self.baselinedict[baseid] = basedict

        # Check data path/filename for dates
        # ----------------------------------------
        # if the path/filename combination contains a valid date and/or the current source is a database
        # then activate the next/previous file path
        source = datacont.get('source')
        sourcepath = datacont.get('sourcepath')
        sourcename = datacont.get('filename')
        date = None
        if source == 'db':
            date = [self.datadict.get(self.active_id).get('end').date()]
        elif source == 'file':
            date = methods.extract_date_from_string(sourcename)
        elif source == 'url':
            date = methods.extract_date_from_string(sourcepath)
        if date and len(date) > 0 and not date[0] == False:
            if date[-1] < datetime.now().date():
                self.menu_p.str_page.nextButton.Enable()  # always
            self.menu_p.str_page.previousButton.Enable()  # always


        # Activate "always" fields
        # ----------------------------------------
        # menu
        self.exportData.Enable(True)

        # ----------------------------------------
        # stream page
        self.menu_p.str_page.startDatePicker.Enable()     # always
        self.menu_p.str_page.startTimePicker.Enable()     # always
        self.menu_p.str_page.endDatePicker.Enable()       # always
        self.menu_p.str_page.endTimePicker.Enable()       # always
        self.menu_p.str_page.trimStreamButton.Enable()    # always
        self.menu_p.str_page.selectKeysButton.Enable()    # always
        self.menu_p.str_page.dropKeysButton.Enable()      # always
        self.menu_p.str_page.extractValuesButton.Enable() # always
        self.menu_p.str_page.activateStatsCheckBox.Enable()      # always

        # ----------------------------------------
        # flag page
        self.menu_p.fla_page.flagOutlierButton.Enable()   # always
        self.menu_p.fla_page.flagSelectionButton.Enable() # always
        self.menu_p.fla_page.flagRangeButton.Enable()     # always
        self.menu_p.fla_page.flagLoadButton.Enable()      # always
        self.menu_p.fla_page.flagMinButton.Enable()       # always
        self.menu_p.fla_page.flagMaxButton.Enable()       # always
        self.menu_p.fla_page.flagClearButton.Enable()       # always
        self.menu_p.fla_page.FlagIDComboBox.Enable()      # always
        self.menu_p.fla_page.flagmodButton.Enable()       # always

        # ----------------------------------------
        # meta page
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
        self.menu_p.ana_page.offsetButton.Enable()        # always
        self.menu_p.ana_page.resampleButton.Enable()      # always
        self.menu_p.ana_page.filterButton.Enable()        # always
        self.menu_p.ana_page.smoothButton.Enable()        # always
        # baseline related stuff and activity below
        self.menu_p.ana_page.powerButton.Enable()         # if experimental
        if self.guidict.get('experimental'):
            self.menu_p.ana_page.spectrumButton.Enable()      # if experimental

        # ----------------------------------------
        # absolutes page
        #self.menu_p.abs_page.loadUSGSButton.Enable()      # always

        # Selective fields
        # ----------------------------------------
        if comps in ['xyz','XYZ','hdz','HDZ','idf','IDF','hez','HEZ','DIF','dif']:
            self.menu_p.str_page.compRadioBox.Enable()
            if comps in ['hdz','HDZ']:
                self.menu_p.str_page.compRadioBox.SetStringSelection('hdz')
                self.compselect = 'hdz'
                self.magpystate['components_select'] = 'hdz'
            elif comps in ['idf','IDF','DIF','dif']:
                self.menu_p.str_page.compRadioBox.SetStringSelection('idf')
                self.compselect = 'idf'
                self.magpystate['components_select'] = 'idf'
            else:
                self.menu_p.str_page.compRadioBox.SetStringSelection('xyz')
                self.compselect = 'xyz'
                self.magpystate['components_select'] = 'xyz'

        if datacont.get('flags'):
            self.menu_p.fla_page.flagDropButton.Enable()     # activated if annotation are present
            self.menu_p.fla_page.flagSaveButton.Enable()      # activated if annotation are present
            self.menu_p.fla_page.annotateCheckBox.Enable()    # activated if annotation are present
            if self.menu_p.fla_page.annotateCheckBox.GetValue():
                self.menu_p.fla_page.annotateCheckBox.SetValue(True)
        if not formattype == 'MagPyDI' and not contenttype.startswith('MagPyDI'):
            self.menu_p.str_page.getGapsButton.Enable()    # activated if not DI data
        if formattype == 'MagPyDI' or contenttype.startswith('MagPyDI'):
            self.menu_p.ana_page.dailyMeansButton.Enable()    # activated for DI data
            self.menu_p.str_page.symbolRadioBox.Enable()      # activated for DI data
            self.menu_p.str_page.symbolRadioBox.SetStringSelection('point')
        if not absinfo == None:
            self.menu_p.ana_page.applyBCButton.Enable()       # activated if DataAbsInfo is present
        #if n < 2000:
        #    self.menu_p.str_page.symbolRadioBox.Enable()      # activated if less then 2000 points, active if DI data
        if not dataid == '' and self.magpystate.get('db'):
            self.menu_p.met_page.getDBButton.Enable()         # activated when DB is connected
            self.menu_p.met_page.putDBButton.Enable()         # activated when DB is connected
        if not str(self.menu_p.abs_page.dilogTextCtrl.GetValue()) == '':
            self.menu_p.abs_page.ClearLogButton.Enable()
            self.menu_p.abs_page.SaveLogButton.Enable()

        if 'x' in keys and 'y' in keys and 'z' in keys:
            self.menu_p.ana_page.rotationButton.Enable()      # activate if vector appears to be present
            if (coverage >= 3) and sr < 61:  # test if enough time is covered and sampling rate is appropriate
                self.menu_p.ana_page.activityButton.Enable()      # activate if vector appears to be present
            self.menu_p.ana_page.calcfButton.Enable()    # activate if vector present
            if 'f' in keys and not 'df' in keys:
                self.menu_p.ana_page.deltafButton.Enable()    # activate if full vector present

            if not formattype == 'MagPyDI' and not contenttype.startswith('MagPyDI'):
                #print ("Checking baseline info")
                baselinelist = [key for key in self.baselinedict]
                if len(baselinelist) > 0:
                    self.menu_p.ana_page.baselineButton.Enable()  # activate if baselinedata is existing

        if self.analysisdict.get('baselinedirect'):
            self.menu_p.ana_page.applyBCButton.Disable()       # activated if DataAbsInfo is present
        if contenttype == 'BC':
            self.menu_p.ana_page.applyBCButton.Disable()       # disabled if already corrected


        # Update "information" fields
        # ----------------------------------------
        self.menu_p.met_page.amountTextCtrl.SetValue(str(n))
        self.menu_p.met_page.samplingrateTextCtrl.SetValue(str(sr))
        self.menu_p.met_page.keysTextCtrl.SetValue(keystr)
        self.menu_p.met_page.typeTextCtrl.SetValue(formattype)
        self.menu_p.met_page.dataTextCtrl.SetValue(metadatatext)
        self.menu_p.met_page.sensorTextCtrl.SetValue(metasensortext)
        self.menu_p.met_page.stationTextCtrl.SetValue(metastationtext)

        self.menu_p.str_page.startDatePicker.SetValue(pydate2wxdate(mintime))
        self.menu_p.str_page.endDatePicker.SetValue(pydate2wxdate(maxtime))
        self.menu_p.str_page.startTimePicker.SetValue(mintime.strftime('%X'))
        self.menu_p.str_page.endTimePicker.SetValue(maxtime.strftime('%X'))

        sourcelist = ['file','database','webservice']
        station_analysiscont = self.analysisdict.get('stations').get(stationid)
        if station_analysiscont and isinstance(station_analysiscont, dict):
            if int(station_analysiscont.get('divariosource')) == 0:
                self.menu_p.abs_page.varioTextCtrl.SetValue(display(station_analysiscont.get('divariopath','')))
            elif int(station_analysiscont.get('divariosource')) == 1:
                self.menu_p.abs_page.varioTextCtrl.SetValue(display(station_analysiscont.get('divarioDBinst', '')))
            else:
                self.menu_p.abs_page.varioTextCtrl.SetValue(display(station_analysiscont.get('divariourl', '')))
            if int(station_analysiscont.get('discalarsource')) == 0:
                self.menu_p.abs_page.scalarTextCtrl.SetValue(display(station_analysiscont.get('discalarpath','')))
            elif int(station_analysiscont.get('discalarsource')) == 1:
                self.menu_p.abs_page.scalarTextCtrl.SetValue(display(station_analysiscont.get('discalarDBinst','')))
            else:
                self.menu_p.abs_page.scalarTextCtrl.SetValue(display(station_analysiscont.get('discalarurl','')))
            self.menu_p.abs_page.VarioSourceLabel.SetLabel("Vario: from {}".format(sourcelist[int(station_analysiscont.get('divariosource'))]))
            self.menu_p.abs_page.ScalarSourceLabel.SetLabel("Scalar: from {}".format(sourcelist[int(station_analysiscont.get('discalarsource'))]))


    def _initial_read(self, stream):
        """
        DESCRIPTION
            Backups stream content and adds current data and header info to the data dictionary and its stream lists.
            Creates an active stream copy and stores pointer towards lists.
            Checks whether ndarray is resent and whether data is present at all
            Eventually extracts flaglist
        """
        flags = False
        nflags = 0

        if not len(stream) > 0:
            self._deactivate_controls()
            self.changeStatusbar("No data available")
            return 0

        # Create a unique stream ID for storage in data dict
        # datadict = {stream id :  { 'data' : stream, 'sampling rate' : sr, 'coverage', start, end, 'flags': True/False}
        # active_id = stream_id

        datacont={}
        amount = len(stream)
        start, end = stream.timerange()
        sr = stream.samplingrate()
        if stream.header.get("DataFlags",False):
            flags = True
            nflags = len(stream.header.get("DataFlags"))
        sensorid = stream.header.get("SensorID")
        stationid = stream.header.get("StationID","")
        if not stationid:
            stationid = self.analysisdict.get("defaultstation")
        datacont['dataset'] = stream
        datacont['stationid'] = stationid
        datacont['amount'] = amount
        datacont['start'] = start
        datacont['end'] = end
        datacont['coverage'] = (end-start).total_seconds()/86400. # coverage in days i.e. to enable activity stuff
        datacont['samplingrate'] = sr
        datacont['sensorid'] = sensorid
        datacont['keys'] = stream.variables()
        datacont['components'] = stream.header.get('DataComponents', '')[:3]
        datacont['flags'] = flags
        # store current state information within the data dictionary
        datacont['source'] = self.magpystate.get('source')
        datacont['filename'] = self.magpystate.get('filename')
        datacont['sourcepath'] = self.magpystate.get('currentpath')
        stream_id_str = "{}{}{}{}{}{}{}".format(sensorid,start,end,sr,str(flags),",".join(stream.variables()),stream.header)
        # create id from string
        m = hashlib.md5()
        m.update(stream_id_str.encode('utf-8'))
        stream_id = str(int(m.hexdigest(), 16))[0:12]
        self.datadict[stream_id] = datacont
        self.active_id = stream_id

        # return active index
        return stream_id


    def _initial_plot(self, streamid, keepplotdict=False, restore=False, debug=False):
        """
        DEFINITION:
            read stream, extract columns with values and display up to three of them by default
            executes guiPlot then
        """
        debug = False
        if streamid:
            # Get current plot parameters
            if debug:
                print ("plotdict BEFORE _set_plot_parameters", self.plotdict)
            if not keepplotdict:
                self.plotdict[streamid] = self._set_plot_parameter()
            if debug:
                print ("plotdict AFTER _set_plot_parameters", self.plotdict)
            # Init Controls
            self._activate_controls(streamid)

            # Override initial controls: Set setting (like keylist, basic plot options and basevalue selection)
            self.plotdict[streamid] = self._update_plot(streamid)

            self.menu_p.rep_page.logMsg('- keys: %s' % (', '.join(self.plotdict.get(streamid).get('shownkeys'))))

            if debug:
                print ("plotdict AFTER _update_plot", self.plotdict)
            self._do_plot([streamid])


    def _update_plot(self, streamid):
        """
        DESCRIPTION
            Checks and eventually updates plot options, checks for correct lengths of all list options

            very similar to _initial_plot, many cross overs with activate_controls
        USED BY
            _initial_plot
        RETURNS
            updated version of plotcontent
        """

        # 1. Get data relevant parameters from datadict
        # ------------------------------
        datacont = self.datadict.get(streamid)
        stream = datacont.get('dataset')
        sensorid = datacont.get('sensorid')
        stationid = datacont.get('stationid')
        n = datacont.get('amount',0)
        sr = datacont.get('samplingrate',0)
        keys = datacont.get('keys',[])
        keystr = ','.join(keys)
        mintime = datacont.get('start')
        maxtime = datacont.get('end')
        coverage = datacont.get('coverage')
        debug = False

        if debug:
            print ("Plotdict BEFORE _update_plot:")
            print (self.plotdict)

        # 2. get the existing plotdict input
        plotcont = self.plotdict.get(streamid)
        # LIMIT shown keys to numerical ones
        lkeys = [key for key in keys if key in stream.NUMKEYLIST]
        shownkeys = plotcont.get('shownkeys',lkeys)
        if not shownkeys or len(shownkeys) > len(keys):
            shownkeys = lkeys
            plotcont['shownkeys'] = shownkeys
        lenshownkeys = max(np.array(shownkeys).shape) # ignores [[1,2,3]] or [1,2,3]

        # 3. Select symbols, colors and padding
        # ------------------------------
        if plotcont.get('symbols') and len(plotcont.get('symbols',[])) == lenshownkeys:
            # everything is fine use currently selected symbollist, eventually with various different symbols
            pass
        elif self.menu_p.str_page.symbolRadioBox.GetStringSelection() == 'line':
            plotcont['symbols'] =  ['-'] * lenshownkeys
        else:
            plotcont['symbols'] =  ['.'] * lenshownkeys
        if plotcont.get('colors') and not len(plotcont.get('colors',[])) == len(shownkeys):
            colors = plotcont.get('colors')
            if len(colors) > lenshownkeys:
                plotcont['colors'] = colors[:lenshownkeys]
            else:
                plotcont['colors'] = [colors[0]] * lenshownkeys
        pads = plotcont.get('padding',[])
        if not pads or not len(pads) == lenshownkeys:
            plotcont['padding']= []


        # 4. Set title and eventually assign function and patch objects
        # ------------------------------
        if stream.header.get('DataFunctionObject',False):
            plotcont['function'] = [stream.header.get('DataFunctionObject')] * lenshownkeys
        plotcont['title'] = stationid
        if coverage < 5 and coverage > 1:
            plotcont['title'] = "{}: {} to {}".format(stationid,mintime.date(),maxtime.date())
        elif coverage <= 1:
            plotcont['title'] = "{}: {}".format(stationid,mintime.date())
        if datacont.get('flags'):
            plotcont['annotate'] = True                   # activate annotation


        # 4. If DataFormat = MagPyDI then preselect scatter, and idf and basevalues
        # ------------------------------
        if stream.header.get('DataFormat') == 'MagPyDI' or stream.header.get('DataType','').startswith('MagPyDI'):
            shownkeys = plotcont.get('shownkeys')
            # only for initial plot - not if selection or drop is chosen
            if shownkeys == lkeys:
                if len(stream._get_column('x')) > 0 and not stream.header.get('DataFormat') == 'MagPyDailyMean':   # is a PYSTR or PYCDF file with basevalues
                    shownkeys = ['dx','dy','dz']
                    plotcont['padding'] = [5,0.05,5]
                elif not len(stream._get_column('x')) > 0 :                  # is a BLV file with basevalues
                    shownkeys = ['dx','dy','dz']
                    plotcont['padding'] = [5,0.05,5]
                dfcol = stream._get_column('df')
                # check if df contains valid data
                if not np.isnan(dfcol).all():
                    shownkeys.append('df')
                    plotcont['padding'].append(2)
                # If dailymeans were calcluated
                if isinstance(plotcont.get('errorbars'), (list,tuple)) and len(plotcont.get('errorbars')) > 0:
                    shownkeys = ['x','y','z','f']
                    shownkeys = shownkeys[:len(plotcont.get('errorbars'))]
                plotcont['symbols'] =  ['.'] * len(shownkeys)
                plotcont['shownkeys'] = shownkeys
                colors = plotcont['colors']
                plotcont['colors'] = colors[:len(shownkeys)]

        # 5. If K values are shown: preselect bar chart
        # ------------------------------
        if stream.header.get('DataFormat') == 'MagPyK' or stream.header.get('DataType','').startswith('MagPyK') or ('var1' in shownkeys and stream.header.get('col-var1','').startswith('K')):
            if 'var1' in shownkeys:
                pos = shownkeys.index('var1')
                plotcont['symbols'][pos] = 'k'

        return plotcont


    def _do_plot(self, streamids, sharey=False):
        """
        DEFINITION:
            read stream and display
        """
        self.changeStatusbar("Plotting...")

        self.plot_p.gui_plot(streamids, self.datadict, self.plotdict, sharey=sharey)

        if len(streamids) == 1:
            streamid = streamids[0]
            datadict = self.datadict.get(streamid)
            plotstream = datadict.get('dataset')
            shownkeylist = self.plotdict.get(streamid).get('shownkeys')
            boxes = ['x','y','z','f']
            for box in boxes:
                checkbox = getattr(self.menu_p.fla_page, box + 'CheckBox')
                if box in shownkeylist:
                    checkbox.Enable()
                    colname = plotstream.header.get('col-'+box, '')
                    if not colname == '':
                        checkbox.SetLabel(colname)
                else:
                    checkbox.SetValue(False)
            # Connect callback to the initial plot
            for ax in self.plot_p.axlist:
                ax.callbacks.connect('xlim_changed', self._update_statistics)
                ax.callbacks.connect('ylim_changed', self._update_statistics)
            self._update_statistics()

        self.changeStatusbar("Ready")


    def _open_stream(self, path=None, mintime=None, maxtime=None, extension=None):
        """
        DESCRIPTION:
            Opens time range dialog and loads data. Returns stream.
        USED BY:
            OnOpenDir and OnOpenDB , OnOpen
        """
        if not path:
            path = ''
        stream = DataStream()
        dlg = LoadDataDialog(None, title='Select timerange:', mintime=mintime, maxtime=maxtime, extension=extension)
        if dlg.ShowModal() == wx.ID_OK:
            stday = dlg.startDatePicker.GetValue()
            sttime = dlg.startTimePicker.GetValue()
            enday = dlg.endDatePicker.GetValue()
            entime = dlg.endTimePicker.GetValue()
            ext = dlg.fileExt.GetValue()

            sd = datetime.fromtimestamp(stday.GetTicks())
            ed = datetime.fromtimestamp(enday.GetTicks())
            st = sd.strftime("%Y-%m-%d") + " " + sttime
            start = datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
            et = ed.strftime("%Y-%m-%d") + " " + entime
            end = datetime.strptime(et, "%Y-%m-%d %H:%M:%S")

            dlg.Destroy()

            loadDlg = WaitDialog(None, "Loading...", "Loading data.\nPlease wait....")

            if isinstance(path, basestring):
                if path:
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
                    #db = path[0]
                    db, success = self._db_connect(*self.magpystate.get('dbtuple'))
                    #print ("HEEERRRR", db)
                    stream = db.read(path[1], starttime=start, endtime=end)
                except:
                    print ("Reading failed")

            loadDlg.Destroy()
            return stream

        else:
            return DataStream()


    def changeStatusbar(self,msg):
        self.SetStatusText(msg)


    def _update_statistics(self, event=None):
        """
        DESCRIPTION
             Updates and sets the statistics if the statistics page
             is displayed
        """
        if self.menu_p.str_page.activateStatsCheckBox.GetValue():
            datacont = self.datadict.get(self.active_id)
            plotcont = self.plotdict.get(self.active_id)
            self.stats_p.stats_page.setStatistics(keys=plotcont.get('shownkeys'),
                    stream=datacont.get('dataset').copy(),
                    xlimits=self.plot_p.xlimits)


    # ##################################################################################################################
    # ####    File Menu Bar                                    #########################################################
    # ##################################################################################################################

    def file_on_open_file(self, event):
        """
        DESCRIPTION
            Control method:
            fileMenu item -> fileOpen
            Will read data from single/multiple files
        """

        stream = DataStream()
        success = False
        filelist = []
        pathlist = []
        # Testing
        message = "Yeah - working fine\n"
        debug = True
        # Extract currently used directory from magpy state
        current_directory = self.magpystate.get('currentpath')

        # Open the file dialig
        dlg = wx.FileDialog(self, "Choose a file", current_directory, "", "*.*", wxMULTIPLE)
        answer = dlg.ShowModal()
        if answer == wx.ID_OK:
            self.changeStatusbar("Loading data ...")
            pathlist = dlg.GetPaths()
        dlg.Destroy()
        if answer == wx.ID_CANCEL:
            return

        if debug:
            print ("Obtained ", pathlist)
            print (" - loading based on MagPy version", magpyversion)

        # Open a temporary dialog until loading is finished
        loadDlg = WaitDialog(None, "Loading...", "Loading data.\nPlease wait....")
        try:
            for path in pathlist:
                elem = os.path.split(path)
                self.magpystate['currentpath'] = elem[0]
                filelist.append(elem[1])
                self.changeStatusbar(path)
                tmp = read(path)
                self.changeStatusbar("... found {} rows".format(tmp.length()[0]))
                stream.extend(tmp.container,tmp.header,tmp.ndarray)
            stream=stream.sorting()
            self.magpystate['filename'] = ' ,'.join(filelist)
            self.menu_p.str_page.fileTextCtrl.SetValue(self.magpystate.get('filename'))
            self.menu_p.str_page.pathTextCtrl.SetValue(self.magpystate.get('currentpath'))
            self.menu_p.rep_page.logMsg('{}: found {} data points'.format(self.magpystate.get('filename'),len(stream)))
            success = True
        except:
            success = False
            message = "Could not identify file!\nplease check and/or try OpenDirectory\n"
        loadDlg.Destroy()

        if not len(stream) > 0:
            message = "Obtained an empty file structure\nfile format supported?\n"
            success = False

        if success:
            stream = stream._remove_nancolumns()

        # plot data
        if success:
            self.magpystate['source'] = 'file'
            # remember filepath
            self.guidict['dirname'] = self.magpystate['currentpath']
            streamid = self._initial_read(stream)
            if streamid: # will create a new input into datadict
                self._initial_plot(streamid)
        else:
            dlg = wx.MessageDialog(self, message,
                "OpenFile", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Loading file failed ... Ready")
            dlg.Destroy()

    @deprecated("OpenDir will be removed")
    def file_on_open_dir(self, event):
        """
        DESCRIPTION
            Open a directory wioth many files.
            Eventually fremove this method.
            Replace by a "get filelist in directory" during Openfile. And create a open next and open previous
            method which loads the next file (or replaces date in file/url/db with next dates.
        CALLS:
            _open_stream
        :param event:
        :return:
        """
        stream = DataStream()
        success = False
        dialog = wx.DirDialog(None, "Choose a directory:", self.magpystate.get('currentpath'),
                              style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            filelist = glob.glob(os.path.join(dialog.GetPath(), '*'))
            self.magpystate['currentpath'] = dialog.GetPath()  # modify self.last_dir
            files = sorted(filelist)
            try:
                oldest = methods.extract_date_from_string(files[0])[0]
                old = wx.DateTime.FromDMY(day=oldest.day, month=oldest.month - 1, year=oldest.year)
                newest = methods.extract_date_from_string(files[-1])[0]
                newest = newest + timedelta(days=1)
                new = wx.DateTime.FromDMY(day=newest.day, month=newest.month - 1, year=newest.year)
                self.menu_p.str_page.pathTextCtrl.SetValue(dialog.GetPath())
                self.menu_p.str_page.fileTextCtrl.SetValue("*")
                success = True
            except:
                success = False
            # self.changeStatusbar("Loading data ...")
        dialog.Destroy()

        if success:
            stream = self._open_stream(path=self.magpystate.get('currentpath'), mintime=old, maxtime=new, extension='*')
            self.menu_p.rep_page.logMsg(
                '{}: found {} data points'.format(self.magpystate.get('currentpath'), len(stream.ndarray[0])))

            if success:
                stream = stream._remove_nancolumns()

            if success:
                self.magpystate['source'] = 'dir'
                self.magpystate['filename'] = ''
                streamid = self._initial_read(stream)
                if streamid:  # will create a new input into datadict
                    self._initial_plot(streamid)
        else:
            dlg = wx.MessageDialog(self, "Could not identify appropriate files in directory!\n"
                                         "please check and/or try OpenFile\n",
                                   "OpenDirectory", wx.OK | wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Loading from directory failed ... Ready")
            dlg.Destroy()


    def file_on_open_url(self, event):
        """
        DESCRIPTION
            Method to open data from any specified url address
        CALLS:
            eventually _open_stream if path is selected
        :param event:
        :return:
        """
        stream = DataStream()
        success = False
        url = ''
        bookmark_dict = self.analysisdict.get('bookmarks',{})
        orgbookmarks = [bookmark_dict.get(el) for el in bookmark_dict]
        bookmarks = orgbookmarks.copy()
        newbookmarks = []
        newbookmarkdict = {}

        dlg = OpenWebAddressDialog(None, title='Open URL', favorites=bookmarks)
        answer = dlg.ShowModal()
        if answer == wx.ID_OK:
            url = dlg.getFavsComboBox.GetValue()
            self.changeStatusbar("Loading data ... be patient")
            newbookmarks = dlg.favorites
        dlg.Destroy()
        if answer == wx.ID_CANCEL:
            return

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
                        stream = self._open_stream(path=url, mintime=mintime, maxtime=maxtime, extension='*')
                        success = True
                    except:
                        success = False
        except:
                pass

        if success:
            stream = stream._remove_nancolumns()

        if len(stream.length()) < 2 and stream.length()[0] < 2:
            message = "No data found"
            success = False

        if success:
            self.menu_p.rep_page.logMsg('{}: found {} data points'.format(url,len(stream)))
            self.magpystate['source'] = 'url'
            self.magpystate['currentpath'] = url
            self.magpystate['filename'] = ''
            streamid = self._initial_read(stream)
            if streamid: # will create a new input into datadict
                self._initial_plot(streamid)
            self.changeStatusbar("Ready")
        else:
            dlg = wx.MessageDialog(self, "Could not access URL!\n"
                "please check address or your internet connection\n",
                "OpenWebAddress", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Loading url failed ... Ready")
            dlg.Destroy()
        # Update the bookmark dictionary - dictionary for future improvements
        if not sorted(orgbookmarks) == sorted(newbookmarks):
            for el in newbookmarks:
                if el in orgbookmarks:
                    # get the key of this values
                    key = list(bookmark_dict.keys())[list(bookmark_dict.values()).index(el)]
                else:
                    # new key
                    m = hashlib.md5()
                    m.update(el.encode('utf-8'))
                    key = str(int(m.hexdigest(), 16))[0:12]
                newbookmarkdict[key] = el
            self.analysisdict['bookmarks'] = newbookmarkdict



    def file_on_open_webservice(self, event):
        """
        DESCRIPTION
            load data from webservices
        :param event:
        :return:
        """
        defaultcommands = {'id':'id', 'starttime':'starttime', 'endtime':'endtime', 'format':'format', 'elements':'elements', 'type':'type','sampling_period':'sampling_period','group':'group'}
        stream = DataStream()
        success = False
        url = ''
        message = "Awesome - its working"
        services = self.analysisdict.get('webservices',{})
        default = self.analysisdict.get('defaultservice','conrad')
        #print (services, self.options)
        if services == {}:
                print ("OPEN a dialog which informs you on the non-existance of services")
                msg = wx.MessageDialog(self, "No Webservices found!\n"
                    "No webservices defined so far.\n",
                    "OpenWebService", wx.OK|wx.ICON_INFORMATION)
                msg.ShowModal()
                self.changeStatusbar("Connecting to webservice failed ... Ready")
                msg.Destroy()
                return

        def replaceCommands(dictionary, replacedict):
            if replacedict and not replacedict == {}:
                for el in replacedict:
                    if not dictionary.get(el,'') == '':
                        dictionary[el] = replacedict[el]
            return dictionary

        dlg = ConnectWebServiceDialog(None, title='Connecting to a webservice', services=services, default=default, validgroups=['magnetism','meteorology'])
        if dlg.ShowModal() == wx.ID_OK:
            # Create URL from inputs
            stday = dlg.startDatePicker.GetValue()
            sttime = str(dlg.startTimePicker.GetValue())
            if sttime.endswith('AM') or sttime.endswith('am'):
                sttime = datetime.strptime(sttime,"%I:%M:%S %p").strftime("%H:%M:%S")
            if sttime.endswith('pm') or sttime.endswith('PM'):
                sttime = datetime.strptime(sttime,"%I:%M:%S %p").strftime("%H:%M:%S")
            sd = datetime.fromtimestamp(stday.GetTicks()).strftime("%Y-%m-%d")
            start= datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
            enday = dlg.endDatePicker.GetValue()
            entime = str(dlg.endTimePicker.GetValue())
            if entime.endswith('AM') or entime.endswith('am'):
                entime = datetime.strptime(entime,"%I:%M:%S %p").strftime("%H:%M:%S")
            if entime.endswith('pm') or entime.endswith('PM'):
                #print ("ENDTime", entime, datetime.strptime(entime,"%I:%M:%S %p"))
                entime = datetime.strptime(entime,"%I:%M:%S %p").strftime("%H:%M:%S")
            ed = datetime.fromtimestamp(enday.GetTicks()).strftime("%Y-%m-%d")
            end = datetime.strptime(ed+'_'+entime, "%Y-%m-%d_%H:%M:%S")
            if start < end:
                service = dlg.serviceComboBox.GetValue()
                # get service depended commands dictionary
                replacedict = services.get(dlg.serviceComboBox.GetValue(),{}).get('commands',{})
                defaultcommands = replaceCommands(defaultcommands, replacedict)
                additionaloptions = services.get(dlg.serviceComboBox.GetValue(),{}).get('extra',{})
                group = dlg.groupComboBox.GetValue()
                #defaultcommands['group'] = None
                if not group == 'magnetism':
                    addgroup = '&{}={}'.format(defaultcommands.get('group'), dlg.groupComboBox.GetValue())
                else:
                    addgroup = ''
                obs_id = '{}={}'.format( defaultcommands.get('id'), dlg.idComboBox.GetValue())
                start_time = '&{}={}T{}Z'.format(defaultcommands.get('starttime'), sd,sttime)
                file_format = '&{}={}'.format(defaultcommands.get('format'), dlg.formatComboBox.GetValue())
                elements = '&{}={}'.format(defaultcommands.get('elements'), dlg.elementsTextCtrl.GetValue())
                data_type = '&{}={}'.format(defaultcommands.get('type'), dlg.typeComboBox.GetValue())
                period = '&{}={}'.format(defaultcommands.get('sampling_period'), dlg.sampleComboBox.GetValue())
                base = services.get(dlg.serviceComboBox.GetValue(),{}).get(group).get('address')

                add_elem=''
                if additionaloptions:
                    #print ("Found additional options")
                    #'extra':{'baseextension':'','additionalelements':'request=GetData','displaytype':'download','mintime':'day'},
                    add_elem = '{}&'.format(additionaloptions.get('additionalelements',''))
                    mintime = additionaloptions.get('mintime',None)
                    if mintime:
                        if mintime in ['day','1d','DAY']:
                            tdiff = (end-start).total_seconds()
                            if tdiff <  86400:
                                missing = 86400-tdiff
                                newend = end + timedelta(0,missing)
                                # add missing seconds to endtime
                                ed = newend.strftime("%Y-%m-%d")
                                entime = newend.strftime("%H:%M:%S")

                end_time = '&{}={}T{}Z'.format(defaultcommands.get('endtime'), ed,entime)

                url = (base + '?' + add_elem + obs_id + start_time + end_time + file_format +
                      elements + data_type + period + addgroup)
                self.analysisdict['defaultservice'] = service
            else:
                msg = wx.MessageDialog(self, "Invalid time range!\n"
                    "The end time occurs before the start time.\n",
                    "Connect Webservice", wx.OK|wx.ICON_INFORMATION)
                msg.ShowModal()
                self.changeStatusbar("Loading from directory failed ... Ready")
                msg.Destroy()


            self.changeStatusbar("Loading webservice data ... be patient")
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
                        message = "Could not access URL"
                    loadDlg.Destroy()
                else:
                    self.menu_p.str_page.pathTextCtrl.SetValue(url)
                    mintime = pydate2wxdate(datetime(1777,4,30))  # Gauss
                    maxtime = pydate2wxdate(datetime(2233,3,22))  # Kirk
                    try:
                        stream = self._open_stream(path=url, mintime=mintime, maxtime=maxtime, extension='*')
                        success = True
                    except:
                        success = False
                        message = "Could not access URL"
        except:
                pass

        if success:
            stream = stream._remove_nancolumns()
        if len(stream.length()) < 2 and stream.length()[0] < 2:
            message = "No data found"
            success = False

        if success:
            self.menu_p.rep_page.logMsg('{}: found {} data points'.format(url,len(stream.ndarray[0])))
            self.magpystate['source'] = 'url'
            self.magpystate['currentpath'] = url
            self.magpystate['filename'] = ''
            streamid = self._initial_read(stream)
            if streamid: # will create a new input into datadict
                self._initial_plot(streamid)
            self.changeStatusbar("Ready")
        else:
            line = "{}!\nplease check address or your internet connection\n".format(message)
            dlg = wx.MessageDialog(self, line,
                "OpenWebAddress", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Loading url failed ... Ready")
            dlg.Destroy()


    def file_on_open_db(self, event):
        """
        DESCRIPTION
            load data from mysql databases.
            The dataset needs to have an input in DATAINFO
            a) get all DATAINFO IDs and store them in a list
            b) disable pathTextCtrl (DB: dbname)
            c) Open dialog which lets the user select list and time window
            d) update stream menu
        CALLS
            _open_stream
        :param event:
        :return:
        """

        def dataAvailabilityCheck(db, datainfoidlist):
            existinglist = []
            if not len(datainfoidlist) > 0:
                return datainfoidlist
            for dataid in datainfoidlist:
                ar = db.select('time', dataid, expert="ORDER BY time DESC LIMIT 10")
                if len(ar) > 0:
                    existinglist.append(dataid)
            return existinglist

        # Check whether DB still available
        db, success = self._db_connect(*self.magpystate.get('dbtuple'))
        datainfoid = ''
        mintime = None
        maxtime = None
        self._check_db('minimal')

        getdata = False
        stream = DataStream()
        if db:
            self.menu_p.rep_page.logMsg('- Accessing database ...')
            output = db.select('DataID,DataMinTime,DataMaxTime', 'DATAINFO')
            datainfoidlist = [elem[0] for elem in output]
            # Verify datainfoidlist
            datainfoidlist = dataAvailabilityCheck(db, datainfoidlist)
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
                mintime = testtime([elem[1] for elem in output if elem[0] == datainfoid][0])
                lastupload = testtime([elem[2] for elem in output if elem[0] == datainfoid][0])
                maxtime = testtime(lastupload.strftime('%Y-%m-%d'))+timedelta(days=1)
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
            path = [db,datainfoid]
            self.magpystate['source'] = 'db'
            self.magpystate['currentpath'] = ''
            self.magpystate['filename'] = datainfoid
            stream = self._open_stream(path=path, mintime=pydate2wxdate(mintime), maxtime=pydate2wxdate(maxtime),extension='MySQL Database')
            stream = stream._remove_nancolumns()
            self.menu_p.rep_page.logMsg('{}: found {} data points'.format(path[1],len(stream.ndarray[0])))
            if len(stream) > 0:
                streamid = self._initial_read(stream)
                if streamid: # will create a new input into datadict
                    self._initial_plot(streamid)


    def file_export_data(self, event):
        """
        DESCRIPTION
            Export the selected data set towrads the choosen destination
        """
        allstreamids = [streamid for streamid in self.datadict]
        datad = self.datadict.get(self.active_id)
        exportsuccess = False
        debug = True # TODO set False
        # Default write options
        exportparameter = {'format_type': 'PYCDF',
                           'filenamebegins': 'myfile_',
                           'filenameends': '.cdf',
                           'dateformat': '%Y-%m-%d',
                           'coverage': 'day',
                           'mode': 'overwrite',
                           'subdirectory': None,
                           'keys': None,
                           'absinfo': None,
                           'fitfunc': None,
                           'fitdegree': None,
                           'knotstep': None,
                           'extradays': None,
                           'year': None,
                           'meanh': None,
                           'meanf': None,
                           'deltaF': None,
                           'diff': None,
                           'baseparam': None,
                           'version': None,
                           'gin': None,
                           'datatype': None,
                           'kvals': None,
                           'kind': None,
                           'comment': None,
                           'useg': None,
                           'skipcompression': None,
                           'addflags': None,
                           'fillvalue': None,
                           'scalar': None,
                           'environment': None
                           }

        # convert all exportparameters to strings
        exportoptions = {}
        for el in exportparameter:
            if not exportparameter.get(el):
                exportoptions[el] = ''
            else:
                exportoptions[el] = str(exportparameter.get(el))

        self.changeStatusbar("Writing data ...")
        dlg = ExportDataDialog(None, title='Export Data', path=self.guidict.get('exportpath'), datadict=datad, exportoptions=exportoptions, allstreamids=allstreamids)
        if dlg.ShowModal() == wx.ID_OK:
            exportoptions = dlg.exportoptions
            exportpath = dlg.selectedTextCtrl.GetValue()
            fileformat = dlg.formatComboBox.GetValue()
            exportfilepath = os.path.join(exportpath, dlg.filenameTextCtrl.GetValue())
            # remember the save path
            self.guidict['exportpath'] = exportpath
            if debug:
                print ("Chosen parameters for export:", exportparameter)
            export = False
            if os.path.exists(exportfilepath) and exportparameter.get('mode') in ['overwrite','replace']:
                msg = wx.MessageDialog(self, "You will overwrite an existing file!\n"
                    "Choose 'Ok' to overwrite or 'Cancel'.\n",
                    "VerifyOverwrite", wx.OK|wx.CANCEL|wx.ICON_QUESTION)
                if msg.ShowModal() == wx.ID_OK:
                    export = True
                msg.Destroy()
            else:
                export = True

            if export == True:
                stream = datad.get('dataset')
                if stream.header.get('DataFormat') == 'MagPyDI':
                    divers = '1.0'
                    stream.header['DataType'] = "{}{}".format('MagPyDI',divers)
                # convert all exportparameters from strings to desired values
                for el in exportoptions:
                    if not exportoptions.get(el):
                        exportparameter[el] = None
                    elif el == 'year' and exportoptions.get(el):
                        exportparameter[el] = int(exportoptions.get(el))
                    elif el in ['fillvalue', 'meanh', 'meanf', 'deltaf'] and methods.is_number(exportoptions.get(el)):
                        exportparameter[el] = float(exportoptions.get(el))
                    elif el == 'fillvalue' and exportoptions.get(el):
                        exportparameter[el] = None
                    elif el == 'addflags'and exportoptions.get(el) == 'True':
                        exportparameter[el] = True
                    elif el == 'addflags' and not exportoptions.get(el) == 'True':
                        exportparameter[el] = None
                    elif el in ['kvals','scalar','environment','diff'] and exportoptions.get(el):
                        # get the dataset of the selected streamid
                        selstreamid = int(exportoptions.get(el))
                        datacont = self.datadict.get(selstreamid)
                        exportparameter[el] = datacont.get('dataset')
                    else:
                        exportparameter[el] = exportoptions.get(el)

                exportsuccess = stream.write(exportpath,
                                             format_type=exportparameter.get('format_type'),
                                             filenamebegins=exportparameter.get('filenamebegins'),
                                             filenameends=exportparameter.get('filenameends'),
                                             dateformat=exportparameter.get('dateformat'),
                                             coverage=exportparameter.get('coverage'),
                                             mode=exportparameter.get('mode'),
                                             subdirectory=exportparameter.get('subdirectory'),
                                             keys=exportparameter.get('keys'),
                                             absinfo=exportparameter.get('absinfo'),
                                             fitfunc=exportparameter.get('fitfunc'),
                                             fitdegree=exportparameter.get('fitdegree'),
                                             knotstep=exportparameter.get('knotstep'),
                                             extradays=exportparameter.get('extradays'),
                                             year=exportparameter.get('year'),
                                             meanh=exportparameter.get('meanh'),
                                             meanf=exportparameter.get('meanf'),
                                             deltaF=exportparameter.get('deltaF'),
                                             diff=exportparameter.get('diff'),
                                             baseparam=exportparameter.get(' baseparam'),
                                             version=exportparameter.get('version'),
                                             gin=exportparameter.get('gin'),
                                             datatype=exportparameter.get('datatype'),
                                             kvals=exportparameter.get('kvals'),
                                             kind=exportparameter.get('kind'),
                                             comment=exportparameter.get('comment'),
                                             useg=exportparameter.get('useg'),
                                             skipcompression=exportparameter.get('skipcompression'),
                                             addflags=exportparameter.get('addflags'),
                                             fillvalue=exportparameter.get('fillvalue'),
                                             scalar=exportparameter.get('scalar'),
                                             environment=exportparameter.get('environment'))

                # TODO: Eventually get additional options -> better do that with export options in Export Dialog
                # dailymean_blv - calculates meanh, meanf and diff
                # self.plotstream.func2header(self.plotopt['function']) -> generally add function as into the header

                if exportsuccess:
                    self.menu_p.rep_page.logMsg("Data written to path: {}".format(self.guidict.get('exportpath')))
                    self.changeStatusbar("Data written ... Ready")
                else:
                    self.menu_p.rep_page.logMsg(
                            "Exporting data failed. Please check required meta data, resolution and coverage for the selected output format")
                    self.changeStatusbar("Exporting data failed ... Ready")
        else:
            self.changeStatusbar("Ready")
        dlg.Destroy()


    def file_on_quit(self, event):
        # Save configuration data
        save_dict(self.guidict, path=self.guicfg)            # stored as config
        save_dict(self.analysisdict, path=self.analysiscfg) # stored as config
        db = self.magpystate.get('db')
        if db:
            db.close()
        self.Destroy()  # Close the main window.
        sys.exit()


    # ##################################################################################################################
    # ####    Database Menu Bar                                #########################################################
    # ##################################################################################################################

    def db_on_connect(self, event):
        """
        DESCRIPTION
            Control method:
            fileMenu item -> fileOpen
        """

        dlg = DatabaseConnectDialog(None, title='MySQL Database: Connect to')
        dlg.hostTextCtrl.SetValue(self.guidict.get('dbhost',''))
        dlg.userTextCtrl.SetValue(self.guidict.get('dbuser',''))
        dlg.passwdTextCtrl.SetValue(base64.b64decode(self.guidict.get('dbpwd','')))
        dlg.dbTextCtrl.SetValue(self.guidict.get('dbname', ''))
        if dlg.ShowModal() == wx.ID_OK:
            self.guidict['dbhost'] = dlg.hostTextCtrl.GetValue()
            self.guidict['dbuser'] = dlg.userTextCtrl.GetValue()
            self.guidict['dbpwd'] = base64.b64encode(dlg.passwdTextCtrl.GetValue().encode()).decode()
            self.guidict['dbname'] = dlg.dbTextCtrl.GetValue()
            self.magpystate['dbtuple'] = (self.guidict.get('dbhost', ''), self.guidict.get('dbuser', ''),
                                          base64.b64decode(self.guidict.get('dbpwd', '')),
                                          self.guidict.get('dbname', ''))
            db, success = self._db_connect(*self.magpystate.get('dbtuple'))
            # Assign the current state
            self.magpystate['db'] = db
            self.magpystate['databaseconnected'] = success

        dlg.Destroy()


    def db_on_init(self, event):
        """
        DESCRIPTION
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
        dlg = wx.MessageDialog(self, "Your are going to intitialize a new database\n"
                        "Please make sure that the following points are fulfilled:\n"
                        "1) MySQL is installed\n"
                        "2) An empty database has been created:\n"
                        "   $ CREATE DATABASE mydb;\n"
                        "3) A new user has been added and access has been granted:\n"
                        "   $ GRANT ALL PRIVILEGES ON mydb.* TO 'user'@'%' IDENTIFIED BY 'some_pass';\n"
                        "4) $ FLUSH PRIVILEGES;\n",
                        "Init database", wx.OK|wx.CANCEL)
        if dlg.ShowModal() == wx.ID_OK:
            dlg.Destroy()
            # open dialog to select empty db or create new db if mysql is existing
            dlg = DatabaseConnectDialog(None, title='MySQL Database: Initialize...')
            db, success = self._db_connect(*self.magpystate.get('dbtuple'))
            dlg.hostTextCtrl.SetValue(self.guidict.get('dbhost',''))
            dlg.userTextCtrl.SetValue(self.guidict.get('dbuser',''))
            dlg.passwdTextCtrl.SetValue(base64.b64decode(self.guidict.get('dbpwd','')))

            if not db:
                dlg.dbTextCtrl.SetValue('None')
            else:
                dlg.dbTextCtrl.SetValue(self.guidict.get('dbname',''))
            if dlg.ShowModal() == wx.ID_OK:
                self.guidict['dbhost'] = dlg.hostTextCtrl.GetValue()
                self.guidict['dbuser'] = dlg.userTextCtrl.GetValue()
                self.guidict['dbpwd'] = base64.b64encode(dlg.passwdTextCtrl.GetValue().encode()).decode()
                self.guidict['dbname'] = dlg.dbTextCtrl.GetValue()
                self.magpystate['dbtuple'] = (self.guidict.get('dbhost', ''), self.guidict.get('dbuser', ''),
                                              base64.b64decode(self.guidict.get('dbpwd', '')),
                                              self.guidict.get('dbname', ''))
                db, success = self._db_connect(*self.magpystate.get('dbtuple'))
                db.dbinit()
                self.changeStatusbar("New database initiated - Ready")
            dlg.Destroy()
        else:
            dlg.Destroy()


    # ##################################################################################################################
    # ####    DI Menu Bar                                      #########################################################
    # ##################################################################################################################

    def di_input_sheet(self,event):
        """
        DESCRIPTION:
            open dialog to input DI data
        """

        # get the di path from analysisdict
        dstation = self.analysisdict.get('defaultstation')
        stationdict = self.analysisdict.get('stations')
        diparameters = stationdict.get(dstation,{})
        dipath = diparameters.get('didatapath')
        dirname = self.guidict.get('dirname')

        if os.path.isfile(dipath):
            dipath = os.path.split(dipath)[0]

        self.dilayout = {}
        self.dilayout['scalevalue'] = diparameters.get('scalevalue')
        self.dilayout['double'] = diparameters.get('double')
        self.dilayout['order'] = diparameters.get('order').split(',')
        cdate = pydate2wxdate(datetime.now(timezone.utc).replace(tzinfo=None))
        # didict contains already loaded data sets for this observatory code
        dlg = InputSheetDialog(None, title='Add DI data', path=dipath, distation=dstation, diparameters=diparameters, cdate=cdate, datapath=dirname, distruct=self.active_didata)
        if dlg.ShowModal() == wx.ID_OK:
            pass
        dlg.Destroy()


    # ##################################################################################################################
    # ####    Memory Menu Bar                                  #########################################################
    # ##################################################################################################################

    def memory_select(self,event):
        """
        DESCRIPTION:
            Open window for multiple stream selection.
            Will access the data dictionary for data specific information and plot dictionary for projected keys
            Layout of the multiple stream page:

            checkbox    id                data-description               button-with-keys          method-buttons
              [ ]     active_id    sensorid,startdate,enddate,sr           [x,y,z]                    Select
              [ ]       id         sensorid,startdate,enddate,sr           [x,y,z,f]                Plot in same

            Further methods:
            join, merge, subtract

            Merge, join and subtract will generate a new stream_id
        """
        plotids = []
        plotkeys = []
        activeid = self.active_id

        debug = False
        if debug:
            print ("Check contents of datadict and plotdict BEFORE memory select:")
            print (self.datadict)
            print (self.plotdict)

        # all other list like plot parameters
        self.changeStatusbar("Selecting data sets ...")

        dlg = MultiStreamDialog(None, title='Select data set(s):', datadict=self.datadict, active=self.active_id, plotdict=self.plotdict)
        if dlg.ShowModal() == wx.ID_OK:
            streamids = [selid for selid in self.datadict]
            for elem in streamids:
                val = eval('dlg.panel.id{}CheckBox.GetValue()'.format(elem))
                if val:
                    # create lists for plotdict
                    plotcont = self.plotdict.get(elem)
                    plotids.append(elem)
                    plotkeys.append(plotcont.get("shownkeys"))
                    activeid = elem
            if len(plotids) > 1:
                #  deactivate all Meta; Analysis methods
                self._deactivate_controls()
            else:
                self._deactivate_controls()
                self._activate_controls(activeid)
                self.active_id = activeid
            self._do_plot(plotids)
        else:
            mod = dlg.panel.modify
            selids = dlg.panel.selectedids
            if mod == True:
                result = dlg.panel.result
                if len(result) > 0:
                    newid = self._initial_read(result)
                    if debug:
                        print ("Created new id", newid)
                    if newid:  # will create a new input into datadict
                        self._initial_plot(newid)
                elif len(selids) > 0:
                    #if debug:
                    print ("Nested plot has been choosen")
                    for elem in selids:
                         # create lists for plotdict
                        plotids.append(elem)
                    self._deactivate_controls()
                    self._do_plot(plotids, sharey=True) #, single=True)
        dlg.Destroy()

        if debug:
            print ("Check contents of datadict and plotdict AFTER memory select:")
            print (self.datadict)
            print (self.plotdict)


    @deprecated("Not used any more")
    def memory_add(self,event):
        """
        DESCRIPTION
            Will add the current working state to the datadict and create a new ID.
            The is function will also update the plotdict.
        :param event:
        :return:
        """
        active_id = self.active_id
        # get the current stream including its modifications
        # two possibilities:
        # 1. create a new streamid whenever ID relevant parameters are changed
        # done by calling _initial_read (datadict), and _update_plot (plotdict)
        # 2. create a temporary id when chaning anything. Make a new id when this function is called
        """
        currentstreamindex = len(self.streamlist)
        self.streamlist.append(self.plotstream)
        self.streamkeylist.append(self.shownkeylist)
        self.headerlist.append(self.plotstream.header)
        self.currentstreamindex = currentstreamindex
        self.plotoptlist.append(self.plotopt)
        if self.plotstream.header.get('DataFormat','').startswith('MagPyDI'):
            basename = self.plotstream.header.get('DataID','')
            print ("Adding a new baselinestream", currentstreamindex, basename)
            self.append_baseline(self.plotstream._get_min('time'),self.plotstream._get_max('time'),basename,currentstreamindex)
        """



    # ##################################################################################################################
    # ####    Specials Menu Bar                                #########################################################
    # ##################################################################################################################


    # ##################################################################################################################
    # ####    Options Menu Bar                                 #########################################################
    # ##################################################################################################################


    def options_init(self, event):
        """
        DEFINITION
            Change options
        """
        dlg = OptionsInitDialog(None, title='Options: Parameter specifications', guidict=self.guidict, analysisdict=self.analysisdict)
        if dlg.ShowModal() == wx.ID_OK:
            self.guidict['dirname'] = dlg.dirnameTextCtrl.GetValue()
            self.guidict['exportpath'] = dlg.exportTextCtrl.GetValue()
            self.guidict['experimental'] = dlg.experimentalCheckBox.GetValue()
            self.analysisdict['defaultstation'] = dlg.stationidTextCtrl.GetValue()
            self.analysisdict['basefilter'] = 'gaussian'
            self.analysisdict['fitfunction'] = dlg.fitfunctionComboBox.GetValue()
            self.analysisdict['fitdegree'] = dlg.fitdegreeTextCtrl.GetValue()
            self.analysisdict['fitknotstep'] = dlg.fitknotstepTextCtrl.GetValue()
            self.analysisdict['martasscantime'] = dlg.martasscantimeTextCtrl.GetValue()
            self.analysisdict['baselinedirect'] = dlg.baselinedirectCheckBox.GetValue()

            # get stationlist - if defaultstation is not in stationlist then create station content
            stationid = self.analysisdict.get('defaultstation','')
            if stationid:
                stationid = stationid.upper()
                self.analysisdict['defaultstation'] = stationid

            stationdict = self.analysisdict.get('stations',{})
            stationlist = [el for el in stationdict]
            newstationdict = {}
            if not stationid in stationlist:
                oldstationdict = stationdict.get(stationlist[0])
                for el in oldstationdict:
                    newstationdict[el] = oldstationdict[el]
                stationdict[stationid] = newstationdict

            save_dict(self.guidict, path=self.guicfg)            # stored as config
            save_dict(self.analysisdict, path=self.analysiscfg) # stored as config

        dlg.Destroy()


    def options_di(self, event):
        """
        DEFINITION
            Change options
        """

        dlg = OptionsDIDialog(None, title='Options: DI Analysis parameters', analysisdict=self.analysisdict)

        if dlg.ShowModal() == wx.ID_OK:
            station = dlg.stationComboBox.GetValue()
            stations = self.analysisdict.get('stations')
            stationcont = stations.get(station)
            order=dlg.sheetorderTextCtrl.GetValue()
            double=dlg.sheetdoubleCheckBox.GetValue()
            scalevalue=dlg.sheetscaleCheckBox.GetValue()
            stationcont['double'] = True
            stationcont['scalevalue'] = True
            if not double:
                stationcont['double'] = False
            if not scalevalue:
                stationcont['scalevalue'] = False
            stationcont['order'] = order
            stations[station] = stationcont
            self.analysisdict['stations'] = stations

            save_dict(self.analysisdict, path=self.analysiscfg) # stored as config

        dlg.Destroy()


    def options_plot(self, event):
        """
        DEFINITION
            Change options
        """
        if self.active_id:
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
            dlg.Destroy()

            self.ActivateControls(self.plotstream)
            self.OnPlot(self.plotstream,self.shownkeylist)


    # ##################################################################################################################
    # ####    Help Menu Bar                                    #########################################################
    # ##################################################################################################################

    def help_about(self, event):

        description = """MagPy is developed for geomagnetic analysis.
    It supports various data formats, visualization,
    advanced anaylsis routines, url/database accessability, DI analysis,
    non-geomagnetic data and more.
    """

        licence = """Copyright (c) 2010-2020, MagPy developers
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:
        * Redistributions of source code must retain the above copyright
          notice, this list of conditions and the following disclaimer.
        * Redistributions in binary form must reproduce the above copyright
          notice, this list of conditions and the following disclaimer in the
          documentation and/or other materials provided with the distribution.
        * Neither the name of the MagPy developers nor the
          names of its contributors may be used to endorse or promote products
          derived from this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL MAGPY DEVELOPERS BE LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."""

        info = wxAboutDialogInfo()

        try:
            script_dir = os.path.dirname(__file__)
            iconimage = os.path.join(script_dir, 'magpy128.xpm')
            # Alternative:
            # print ("Check", iconimage)
            # if sys.platform.startswith('linux'):
            info.SetIcon(wx.Icon(iconimage, wx.BITMAP_TYPE_XPM))
        except:
            pass
        info.SetName('MagPy')
        info.SetVersion(__version__)
        info.SetDescription(description)
        info.SetCopyright(
            '(C) 2011 - 2022 Roman Leonhardt, Rachel Bailey, Mojca Miklavec, Jeremy Fee, Heather Schovanec, Stephan Bracke, Niko Kompein')
        info.SetWebSite('https://cobs.geosphere.at')
        info.SetLicence(licence)
        info.AddDeveloper(
            'Roman Leonhardt, Rachel Bailey, Mojca Miklavec, Jeremey Fee, Heather Schovanec, Stephan Bracke, Niko Kompein')
        info.AddDocWriter('Leonhardt')
        info.AddArtist('Leonhardt')
        info.AddTranslator('Bailey')

        wxAboutBox(info)


    def help_write_formats(self, event):
        """
        DESCRIPTION
            Extract write formats and show
        """
        WriteFormats = ["{}: \t{}".format(key, SUPPORTED_FORMATS[key][1]) for key in SUPPORTED_FORMATS if
                            'w' in SUPPORTED_FORMATS[key][0]]
        message = "\n".join(WriteFormats)
        dlg = ScrolledMessageDialog(self, message, 'Write formats:')
        dlg.ShowModal()


    def help_read_formats(self, event):
        """
        DESCRIPTION
            Extract read formats and show
        """
        ReadFormats = ["{}: \t{}".format(key, SUPPORTED_FORMATS[key][1]) for key in SUPPORTED_FORMATS if
                       'r' in SUPPORTED_FORMATS[key][0]]
        message = "\n".join(ReadFormats)
        dlg = ScrolledMessageDialog(self, message, 'Read formats:')
        dlg.ShowModal()


    def help_open_log(self, event):
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

    # ##################################################################################################################
    # ####    Data Panel                                       #########################################################
    # ##################################################################################################################

    def data_onPreviousButton(self,event):
        """
        DESCRIPTION
            Open an adjacent data set before the currently opened
        CALLS
            get_adjacent_stream
        """

        self.get_adjacent_stream(mode='previous')


    def data_onNextButton(self,event):
        """
        DESCRIPTION
            Open an adjacent data set after the currently opened
        CALLS
            get_adjacent_stream
        """

        self.get_adjacent_stream(mode='next', debug=True)


    def get_adjacent_stream(self, mode='next', debug=False):
        """
        DESCRIPTION
            Load an adjacent data set prior to or after the current data set from the same directory
            Can read magpystate['currentpath'] and magpystate['source'] plus time range and coverage
            for current stream (self.active_id)
        :param mode:
        :return:
        """
        # 1. extract source and current path
        # 2. get dates from active_id
        # 3. identify dates in path and increase or decrease the range
        # 4. Load the new data set (eventually check if a corresponding streamid is already existing.
        # 5.
        def _replace_first_date_occurrence(name, runtime='a', debug=False):
            """
            DESCRIPTION
                Will replace any date recognized by extract_date_from_string, (exception: APR0218.WIK)
                Inserted are YEAR1, MONTH1, DAY1 if runtime is 1
            """
            skip = False
            today = datetime.now()
            tyear = today.year
            resdate = extract_date_from_string(name)
            coverage = 1
            if debug:
                print(resdate)
            if not resdate or (len(resdate) == 1 and resdate[0] == False):
                if debug:
                    print("No date found:", name)
                skip = True
            elif len(resdate) > 1:
                coverage = (resdate[-1] - resdate[0]).days
            elif len(resdate) == 1:
                # single date found or False
                coverage = 1
            if not skip:
                newname = name
                year = str(resdate[0].year)
                month = str(resdate[0].month).zfill(2)
                day = str(resdate[0].day).zfill(2)
                if debug:
                    print(year, month, day, newname, coverage)
                if coverage <= 366 and int(year) <= tyear:
                    # find date in string and replace by dummy
                    # 1 find year
                    if newname.find(year) >= 0:
                        newname = newname.replace(year, 'YEAR{}'.format(runtime), 1)
                print (newname)
                if coverage <= 31:
                    if newname.find(month) >= 0:
                        newname = newname.replace(month, 'MONTH{}'.format(runtime), 1)
                if coverage <= 2:
                    if newname.find(day) >= 0:
                        newname = newname.replace(day, 'DAY{}'.format(runtime), 1)
                return newname
            else:
                return ''

        stream = DataStream()
        datacont = self.datadict.get(self.active_id)
        source = datacont.get('source')
        sourcepath = datacont.get('sourcepath')
        sourcename = datacont.get('filename')
        newstart = None
        newend = None
        sr = self.datadict.get(self.active_id).get('samplingrate')
        start = self.datadict.get(self.active_id).get('start')
        end = self.datadict.get(self.active_id).get('end') + timedelta(seconds=sr)
        if mode == 'next':
            newstart = end
            newend = end + (end - start)
        if mode == 'previous':
            newstart = start - (end - start)
            newend = start
        if debug:
            print ("oldstart, newstart, oldend, newend:", start, newstart, end, newend)
            print (source, sourcename, sourcepath)
        newstart += timedelta(minutes=5)
        newstart -= timedelta(minutes=newstart.minute % 10,
                                 seconds=newstart.second,
                                 microseconds=newstart.microsecond)
        if debug:
            print ("newstart, newend:", newstart, newend)
        if source == 'db':
            db, success = self._db_connect(*self.magpystate.get('dbtuple'))
            stream = db.read(sourcename, starttime=newstart, endtime=newend)
            self.magpystate['source'] = 'db'
            self.magpystate['filename'] = sourcename
            self.magpystate['currentpath'] = ''
        elif source == 'file':
            sourcename = sourcename.split(',')
            if isinstance(sourcename, (list,tuple)):
                sourcename = sourcename[0].strip()
            newname = _replace_first_date_occurrence(sourcename, runtime='a', debug=debug)
            newname = newname.replace("YEARaMONTHaDAYa", "*").replace(
                "YEARa-MONTHa-DAYa", "*").replace(
                "YEARaMONTHa","*").replace(
                "YEARa-MONTHa", "*").replace(
                "YEARa", "*")
            if debug:
                print ("FILE - next, previous:", newname, newstart.strftime("%Y-%m-%d"), newend)
            stream = read(os.path.join(sourcepath,newname),newstart.strftime("%Y-%m-%d"),newend.strftime("%Y-%m-%d"))
            self.magpystate['source'] = 'file'
            self.magpystate['filename'] = sourcename
            self.magpystate['currentpath'] = sourcepath
        elif source == 'url':
            newname = _replace_first_date_occurrence(sourcepath, runtime='a')
            newname = _replace_first_date_occurrence(newname, runtime='b')
            newname = newname.replace("YEARa-MONTHa-DAYa", newstart.strftime("%Y-%m-%d")).replace(
                "YEARaMONTHaDAYa", newstart.strftime("%Y%m%d")).replace(
                "YEARaMONTHa", newstart.strftime("%Y%m")).replace("YEARa", newstart.strftime("%Y"))
            newname = newname.replace("YEARb-MONTHb-DAYb", newend.strftime("%Y-%m-%d")).replace(
                "YEARbMONTHbDAYb", newend.strftime("%Y%m%d")).replace(
                "YEARbMONTHb", newend.strftime("%Y%m")).replace("YEARb", newend.strftime("%Y"))
            stream = read(sourcepath)
            self.magpystate['source'] = 'url'
            self.magpystate['filename'] = ''
            self.magpystate['currentpath'] = sourcepath

        if len(stream) > 0:
            streamid = self._initial_read(stream)
            self._initial_plot(streamid)



    @deprecated("Restore is not used any more as any significant change will create a new memory input")
    def onRestoreData(self,event):
        """
        DESCRIPTION
            Restore originally loaded data.
        """
        """
        self.flaglist = []
        if not len(self.stream.ndarray[0]) > 0:
            self.DeactivateAllControls()
            self.changeStatusbar("No data available")
            return False
        self.plotstream = self.streamlist[self.currentstreamindex].copy()
        self.plotstream.header = self.headerlist[self.currentstreamindex]
        self.menu_p.rep_page.logMsg('Original data restored...')
        self.OnInitialPlot(self.plotstream, restore=True)
        """
        pass


    def data_onTrimButton(self,event):
        """
        DESCRIPTION
            Trim the data stream to the selected timerange. This will modify the data set and create a new stream input.
            Use the zoom methods if you don't want that.
        CALLS
            stream._select_timerange
        """

        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')

        stday = self.menu_p.str_page.startDatePicker.GetValue()
        sttime = str(self.menu_p.str_page.startTimePicker.GetValue())
        stdate = (datetime.fromtimestamp(stday.GetTicks())).date()
        start = dparser.parse("{} {}".format(stdate,sttime))
        """
        try:
            sttimetmp = datetime.strptime(sttime, "%I:%M:%S %p")
            print ("TEST", sttimetmp)
            if sttime.endswith('AM') or sttime.endswith('am'):
                sttime = sttimetmp.strftime("%H:%M:%S")
            if sttime.endswith('pm') or sttime.endswith('PM'):
                sttime = sttimetmp.strftime("%H:%M:%S")
            print ("HHHHHERE")
            sdtmp = datetime.fromtimestamp(stday.GetTicks())
            sd = sdtmp.strftime("%Y-%m-%d")
        except:
            dlg = wx.MessageDialog(self, "Could not trim timerange!\n"
                        "Entered startdate is not valid.\n",
                        "TrimTimerange", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Trimming timerange failed ... Ready")
            dlg.Destroy()
        start= datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
        """

        enday = self.menu_p.str_page.endDatePicker.GetValue()
        entime = str(self.menu_p.str_page.endTimePicker.GetValue())
        endate = (datetime.fromtimestamp(stday.GetTicks())).date()
        end = dparser.parse("{} {}".format(endate,entime))
        """
        try:
            entimetmp = datetime.strptime(entime,"%I:%M:%S %p")
            if entime.endswith('AM') or entime.endswith('am'):
                entime = entimetmp.strftime("%H:%M:%S")
            if entime.endswith('pm') or entime.endswith('PM'):
                entime = entimetmp.strftime("%H:%M:%S")
            edtmp = datetime.fromtimestamp(enday.GetTicks())
            ed = edtmp.strftime("%Y-%m-%d")
        except:
            dlg = wx.MessageDialog(self, "Could not trim timerange!\n"
                        "Entered enddate is not valid.\n",
                        "TrimTimerange", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            self.changeStatusbar("Trimming timerange failed ... Ready")
            dlg.Destroy()
        end= datetime.strptime(ed+'_'+entime, "%Y-%m-%d_%H:%M:%S")
        """

        if end > start:
            plotstream = stream.copy()
            try:
                self.changeStatusbar("Trimming stream ...")
                plotstream = plotstream.trim(starttime=start, endtime=end)
                #self.plotstream=DataStream([LineStruct()],self.plotstream.header,newarray)
                self.menu_p.rep_page.logMsg('- Stream trimmed: {} to {}'.format(start,end))
            except:
                self.menu_p.rep_page.logMsg('- Trimming failed')

            if len(plotstream) > 0:
                streamid = self._initial_read(plotstream)
                self._initial_plot(streamid)
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


    def data_onSelectKeys(self,event):
        """
        DESCRIPTION
            open dialog to select shown keys (check boxes)
            Will NOT create a new data ID.

        """

        self.changeStatusbar("Selecting keys ...")
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        keys = datacont.get('keys')
        plotcont = self.plotdict.get(self.active_id)
        shownkeys = plotcont.get('shownkeys')

        namelist = []
        unitlist = []
        for key in keys:
            col = stream._get_column(key)
            if len(col) > 0:
                value = stream.header.get('col-'+key)
                unit = stream.header.get('unit-col-'+key)
                if not value == '':
                    namelist.append(value)
                else:
                    namelist.append(key)
                if not unit == '':
                    unitlist.append(unit)
                else:
                    unitlist.append('')

        if len(stream) > 0:
            dlg = StreamSelectKeysDialog(None, title='Select keys:',keylst=keys,shownkeys=shownkeys,namelist=namelist)
            for elem in shownkeys:
                exec('dlg.'+elem+'CheckBox.SetValue(True)')
            if dlg.ShowModal() == wx.ID_OK:
                newshownkeys = []
                for elem in keys:
                    boolval = eval('dlg.'+elem+'CheckBox.GetValue()')
                    if boolval:
                        newshownkeys.append(elem)
                if len(newshownkeys) == 0:
                    print ("Deselecting all elements is not possible")
                else:
                    shownkeys = newshownkeys
                plotcont['shownkeys'] = shownkeys
                self.plotdict[self.active_id] = plotcont
                self._initial_plot(self.active_id, keepplotdict=True)
        else:
            self.changeStatusbar("Failure")


    def data_onDropKeys(self,event):
        """
        DESCRIPTION
            open dialog to select drop keys (check boxes).
            Will create a new data ID.
        """

        self.changeStatusbar("Dropping keys ...")
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        keys = datacont.get('keys')
        plotcont = self.plotdict.get(self.active_id)
        shownkeys = plotcont.get('shownkeys')

        namelist = []
        unitlist = []
        for key in keys:
            col = stream._get_column(key)
            if len(col) > 0:
                value = stream.header.get('col-'+key)
                unit = stream.header.get('unit-col-'+key)
                if not value == '':
                    namelist.append(value)
                else:
                    namelist.append(key)
                if not unit == '':
                    unitlist.append(unit)
                else:
                    unitlist.append('')

        if len(stream) > 0:
            plotstream = stream.copy()
            dlg = StreamSelectKeysDialog(None, title='Select keys:',keylst=keys,shownkeys=shownkeys,namelist=namelist)
            if dlg.ShowModal() == wx.ID_OK:
                dropkeylist = []
                for elem in keys:
                    boolval = eval('dlg.'+elem+'CheckBox.GetValue()')
                    if boolval:
                        dropkeylist.append(elem)
                        plotstream = plotstream._drop_column(elem)
                if len(dropkeylist) == 0:
                    self.changeStatusbar("Ready")
                else:
                    shownkeys = [el for el in shownkeys if not el in dropkeylist]
                    self.plotdict[self.active_id] = plotcont # this line is useless
                    streamid = self._initial_read(plotstream)
                    self._initial_plot(streamid)
        else:
            self.changeStatusbar("Failure")


    def data_onExtractData(self,event):
        """
        DESCRIPTION:
            open dialog to choose extract parameter (paramater compare value)
            up to three possibilities
        """

        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        keys = datacont.get('keys')
        plotcont = self.plotdict.get(self.active_id)
        shownkeys = plotcont.get('shownkeys')

        if len(stream.ndarray[0]) > 0:
            dlg = StreamExtractValuesDialog(None, title='Extract:',keylst=shownkeys)
            if dlg.ShowModal() == wx.ID_OK:
                key1 = dlg.key1ComboBox.GetValue()
                comp1 = dlg.compare1ComboBox.GetValue()
                val1 = dlg.value1TextCtrl.GetValue()
                logic2 = dlg.logic2ComboBox.GetValue()
                logic3 = dlg.logic3ComboBox.GetValue()
                extractedstream = stream.extract(key1,val1,comp1)
                if len(extractedstream) < 2 and extractedstream.length()[0] < 2:
                    # Empty stream returned -- looks complex because of old LineStruct rubbish
                    self.menu_p.rep_page.logMsg('Extract: criteria would return an empty data stream - skipping')
                    extractedstream = stream
                val2 = dlg.value2TextCtrl.GetValue()
                if not val2 == '':
                    key2 = dlg.key2ComboBox.GetValue()
                    comp2 = dlg.compare2ComboBox.GetValue()
                    if logic2 == 'and':
                        extractedstream = extractedstream.extract(key2,val2,comp2)
                    else:
                        extractedstream2 = stream.extract(key2,val2,comp2)
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
                            extractedstream3 = stream.extract(key3,val3,comp3)
                            extractedstream.extend(extractedstream3.container, extractedstream3.header,extractedstream3.ndarray)
                            extractedstream = extractedstream.removeduplicates()
                            extractedstream = extractedstream.sorting()
                            extractedstream = extractedstream.get_gaps()
                extractedstream.header["SensorID"] = "extracted-{}".format(extractedstream.header.get("SensorID"))
                streamid = self._initial_read(extractedstream)
                self._initial_plot(streamid)
                self.changeStatusbar("Ready")
        else:
            self.menu_p.rep_page.logMsg("Extract: No data available so far")
        # specify filters -> allow to define filters Combo with key - Combo with selector (>,<,=) - TextBox with Filter


    def data_onGetGapsButton(self,event):
        """
        get gaps in timeseries (eventually missing data assuming periodic signals
        and add this info (0,1) to var5 key
        """
        self.changeStatusbar("Identifying gaps ...")
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        print (len(stream))
        stream = stream.get_gaps()
        print (len(stream))
        streamid = self._initial_read(stream)
        self._initial_plot(streamid)
        self.changeStatusbar("Ready")


    def data_onChangeComp(self, event):
        """
        DESCRIPTION
            Coordinate transformation between orthogonal, cylindrical and spherical coordinates
        :param event:
        :return:
        """
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        orgcomp = datacont.get('components').lower()
        if orgcomp in ['xyz','hez']:  # orthogonal
            orgcomp = 'xyz'
        #orgcomp = self.compselect
        compselect = self.menu_p.str_page.comp[event.GetInt()]
        coordinate = "{}2{}".format(orgcomp,compselect)
        self.changeStatusbar("Transforming ... {}".format(coordinate))
        print("Transforming ... {}".format(coordinate))
        stream = stream._convertstream(coordinate)
        datacont['dataset'] = stream
        datacont['components'] = compselect
        self.datadict[self.active_id] = datacont
        self._initial_plot(self.active_id, keepplotdict=True)


    def data_onChangeSymbol(self, event):
        #orgsymbol = self.symbolselect
        symbolselect = self.menu_p.str_page.symbol[event.GetInt()]
        self.changeStatusbar("Transforming ...")
        plotcont = self.plotdict.get(self.active_id)
        if symbolselect == 'line':
            plotcont['symbols'] =  ['-' for elem in plotcont.get('shownkeys')]
        elif symbolselect == 'point':
            plotcont['symbols'] =  ['.' for elem in plotcont.get('shownkeys')]
        self.plotdict[self.active_id] = plotcont
        self._initial_plot(self.active_id, keepplotdict=True)

        self.changeStatusbar("Ready")


    def data_onStatsCheckBox(self, event):
        """
        DESCRIPTION
             Creates/Destroys the statistics element below main window
             and sets the statistics
        """
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')

        status = self.menu_p.str_page.activateStatsCheckBox.GetValue()
        if status:
            self.sp.SplitHorizontally(self.sp2, self.stats_p, 800)
            self.stats_p.stats_page.setStatistics(keys=keys,
                    stream=stream.copy(),
                    xlimits=self.plot_p.xlimits)
        else:
            self.sp.Unsplit(self.stats_p)



    def default_file_dialog_options(self):
        """
        DESCRIPTION
            Return a dictionary with file dialog options that can be
            used in both the save file dialog as well as in the open
            file dialog.
        USED BY
            on_save_as
            on_open_aux_button
        """
        default_dir = self.magpystate.get('currentpath')
        return dict(message='Choose a file', defaultDir=default_dir,
                    wildcard='*.*')

    # ################
    # Top menu methods:


    def dailymeans_blv(self, datastream,debug=False):
        """
        DESCRIPTION
            Get dailymean values and nnual means for H and F
            Will return daily mean datastream and annual means for BLV export
        VARIABLES
            :param datastream:
        :return:
        """
        dm = None
        meanh = None
        meanf = None
        ds = datastream.copy()
        if debug:
            print ("Stream looks like", ds.ndarray)
        ds = ds.delta_f()
        comp = ds.header.get("DataComponents")
        if not comp.startswith("HDZ") and not comp.startswith("hdz"):
            if debug:
                print("Converting to HDZ to obtain mean H and F")
            ds = ds.xyz2hdz()
        meanh = ds.mean('x')
        meanf = ds.mean('f')
        if debug:
            print ("  Means:", meanh, meanf)
            print("  Moving delta F column to correct position for BLV scalar diff extraction...")
            print("  Filtering daily means ...")
        df = ds._get_column('df')
        ds = ds._put_column(df, 'f')
        dm = ds.dailymeans(keys=['x', 'y', 'z', 'f', 'df'])
        df = dm._get_column('f')
        dm = dm._put_column(df, 'df')
        if debug:
            print ("Done",dm.ndarray)
        return dm, meanh, meanf


    # ##################################################################################################################
    # ####    Flagging Panel                                   #########################################################
    # ##################################################################################################################


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


    # ##################################################################################################################
    # ####    Meta data Panel                                  #########################################################
    # ##################################################################################################################


    def meta_onGetDBButton(self,event):
        """
        DESCRIPTION
            get Meta data for the current sensorid from database
        """
        # Test whether DB is still connected
        self._check_db('minimal')
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        dataid = stream.header.get('DataID','')

        # open dialog with all header info
        if dataid == '':
            dlg = wx.MessageDialog(self, "No Data ID available!\n"
                            "You need to specify a unique Data ID\nfor which meta information is obtained.\n","Undefined Data ID", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            self.menu_p.rep_page.logMsg(" - failed to add meta information from DB")
        else:
            db, success = self._db_connect(*self.magpystate.get('dbtuple'))
            stream.header = db.fields_to_dict(dataid)
            self.menu_p.rep_page.logMsg(" - added meta information for {} from DB".format(dataid))
            self._activate_controls(self.active_id)


    def meta_onPutDBButton(self,event):
        """
        DESCRIPTION
            write meta data to the database
        """
        # Check whether DB still available
        self._check_db('minimal')
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        dataid = stream.header.get('DataID','')

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
                db, success = self._db_connect(*self.magpystate.get('dbtuple'))
                db.dict_to_fields(stream.header)
                self.menu_p.rep_page.logMsg(" - added meta information for {} to DB".format(dataid))
            self._activate_controls(self.active_id)


    def meta_onDataButton(self,event):
        """
        DESCRIPTION
            open dialog to modify plot options (general (e.g. bgcolor) and  key
            specific (key: symbol color errorbar etc)
        """
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        db, success = self._db_connect(*self.magpystate.get('dbtuple'))
        fields = db.DATAINFOKEYLIST
        # open dialog with all header info
        if len(stream) > 0:
            dlg = MetaDataDialog(None, title='Meta information:',header=stream.header,fields=fields)
            if dlg.ShowModal() == wx.ID_OK:
                d = locals()
                for key in fields:
                    exec('value = dlg.panel.'+key+'TextCtrl.GetValue()')
                    try:
                        if not d['value'] == dlg.header.get(key,''):
                            stream.header[key] = d['value']
                    except:
                        # might fail for arrays
                        pass
                self._activate_controls(self.active_id)
        else:
            self.menu_p.rep_page.logMsg("Meta information: No data available")


    def meta_onSensorButton(self,event):
        """
        DESCRIPTION
            open dialog to modify plot options (general (e.g. bgcolor) and  key
            specific (key: symbol color errorbar etc)
        """
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        db, success = self._db_connect(*self.magpystate.get('dbtuple'))
        fields = db.SENSORSKEYLIST
        # open dialog with all header info
        if len(stream) > 0:
            dlg = MetaDataDialog(None, title='Meta information:',header=stream.header,fields=fields)
            if dlg.ShowModal() == wx.ID_OK:
                d = locals()
                for key in fields:
                    exec('value = dlg.panel.'+key+'TextCtrl.GetValue()')
                    if not d['value'] == dlg.header.get(key,''):
                        stream.header[key] = d['value']
                self._activate_controls(self.active_id)
        else:
            self.menu_p.rep_page.logMsg("Meta information: No data available")


    def meta_onStationButton(self,event):
        """
        DESCRIPTION
            open dialog to modify plot options (general (e.g. bgcolor) and  key
            specific (key: symbol color errorbar etc)
        """
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        db, success = self._db_connect(*self.magpystate.get('dbtuple'))
        fields = db.STATIONSKEYLIST
        # open dialog with all header info
        if len(stream) > 0:
            dlg = MetaDataDialog(None, title='Meta information:',header=stream.header,fields=fields)
            if dlg.ShowModal() == wx.ID_OK:
                d = locals()
                for key in fields:
                    exec('value = dlg.panel.'+key+'TextCtrl.GetValue()')
                    if not d['value'] == dlg.header.get(key,''):
                        stream.header[key] = d['value']
                self._activate_controls(self.active_id)
        else:
            self.menu_p.rep_page.logMsg("Meta information: No data available")


    # ##################################################################################################################
    # ####    Analysis Panel                                   #########################################################
    # ##################################################################################################################


    def analysis_onDerivativeButton(self, event):
        """
        DESCRIPTION
            Calculate and display the derivative
        """
        self.changeStatusbar("Calculating derivative ...")
        stream = self.datadict.get(self.active_id).get('dataset')
        keys = self.plotdict.get(self.active_id).get('shownkeys')

        if len(stream) > 0:
            plotstream = stream.copy()
            self.menu_p.rep_page.logMsg("- calculating derivative")
            plotstream = plotstream.derivative(keys=keys,put2keys=keys)
            # change streamid/sensorid to express derivative calculation
            plotstream.header['SensorID'] = "{}-{}".format("derivative",plotstream.header.get('SensorID'))
            self.menu_p.rep_page.logMsg('- derivative calculated')
            # Create a new stream id after calculating derivative and plot it
            streamid = self._initial_read(plotstream)
            self._initial_plot(streamid)

        self.changeStatusbar("Ready")


    def analysis_onRotationButton(self, event):
        """
        DESCRIPTION
            Method for offset correction
            Will take header alpha and beta as presets for dialog
            If a rotation is applied then the data header "DataComments" will contain
            some information about is
        CALLS
            stream.extract_headerlist
        """
        apply = True
        stream = self.datadict.get(self.active_id).get('dataset')
        headalpha = stream.extract_headerlist('DataRotationAlpha')
        headbeta = stream.extract_headerlist('DataRotationBeta')
        headgamma = stream.extract_headerlist('DataRotationGamma')
        print ("Existing alpha,beta,gamma", headalpha,headbeta,headgamma )
        comment = stream.header.get('DataComments','')
        print ("Existing comment", comment)
        count = comment.count("rotation")
        if count > 0:
            # some rotation has already been applied to this data set
            message = "Some rotation has been applied already"
            mes = [el for el in comment.split(",") if el.find("rotation") >= 0]
            if len(mes) > 0:
                message = mes[-1]
            message = "{}.\nApply an additional rotation?".format(message)
            mdlg = wx.MessageDialog(None, message, "Found previous rotation...", wx.YES_NO | wx.NO_DEFAULT)
            result = mdlg.ShowModal()
            if result == wx.ID_YES:
                pass
            else:
                apply = False
            mdlg.Destroy()

        self.changeStatusbar("Rotating data ...")
        if len(stream) > 0 and apply:
            plotstream = stream.copy()
            alphat, betat, gammat = 0.0, 0.0, 0.0
            alpha, beta, gamma = 0.0, 0.0, 0.0
            invert = False
            dlg = AnalysisRotationDialog(None, title='Analysis: rotate data',orgalpha=headalpha, orgbeta=headbeta, orggamma=headgamma)
            if dlg.ShowModal() == wx.ID_OK:
                alphat = dlg.alphaTextCtrl.GetValue()
                betat = dlg.betaTextCtrl.GetValue()
                gammat = dlg.gammaTextCtrl.GetValue()
                invert = dlg.invertCheckBox.GetValue()
            dlg.Destroy()
            if alphat and methods.is_number(alphat):
                alpha = float(alphat)
            if betat and methods.is_number(betat):
                beta = float(betat)
            if gammat and methods.is_number(gammat):
                gamma = float(gammat)

            # applying rotation
            plotstream = plotstream.rotation(alpha=alpha, beta=beta, gamma=gamma, invert=invert)
            plotstream.header['SensorID'] = "{}{}-{}".format("rotation", count, plotstream.header.get('SensorID'))
            inverttext = ''
            if invert:
                inverttext = 'inverted '
            self.menu_p.rep_page.logMsg('- {}stream rotation by alpha={}, beta={} and gamma={}'.format(inverttext,alphat,betat,gammat))
            # Create a new stream id after calculating derivative and plot it
            streamid = self._initial_read(plotstream)
            self._initial_plot(streamid)
        self.changeStatusbar("Ready")


    def analysis_onDeltafButton(self, event):
        """
        DESCRIPTION
             Calculates delta F values
        """
        self.changeStatusbar("Delta F ...")
        stream = self.datadict.get(self.active_id).get('dataset')

        if len(stream) > 0:
            plotstream = stream.copy()
            plotstream = plotstream.delta_f()
            self.menu_p.rep_page.logMsg('- determined delta F between x,y,z and f')
            streamid = self._initial_read(plotstream)
            self._initial_plot(streamid)
        self.changeStatusbar("Ready")


    def analysis_onFilterButton(self, event):
        """
        Method for filtering
        """
        self.changeStatusbar("Filtering...")

        # open dialog to modify filter parameters
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        sr = datacont.get("samplingrate")
        filter_type = self.analysisdict.get('basefilter')

        resample_offset = 0.0
        plotstream = stream.copy()

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

            plotstream = plotstream.filter(keys=keys,filter_type=filtertype,filter_width=timedelta(seconds=filterlength),resample_period=resampleinterval,resampleoffset=timedelta(seconds=resampleoffset),missingdata=miss,resample=True)
            self.menu_p.rep_page.logMsg('- data filtered: {} window, {} Hz passband'.format(filtertype,1./filterlength))

            streamid = self._initial_read(plotstream)
            self._initial_plot(streamid)
        self.changeStatusbar("Ready")


    def analysis_onFitButton(self, event):
        """
        Method for fitting
        """
        self.changeStatusbar("Fitting ...")
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        dir = self.guidict.get('dirname')
        if len(stream) > 0:
            plotstream = stream.copy()
            # get the currently zoomed time range
            xlimits = self.plot_p.xlimits
            dlg = AnalysisFitDialog(None, title='Analysis: Fit parameter',
                                    datacont=datacont, plotcont=plotcont, analysisdict=self.analysisdict,
                                    hide_file=False, last_dir=dir)
            startdate=xlimits[0]
            enddate=xlimits[1]
            dlg.setTimeRange(startdate, enddate)
            if dlg.ShowModal() == wx.ID_OK:
                params = dlg.getFitParameters()
                fitfunc = params['fitfuncname']
                knotstep = str(params['knotstep'])
                fitdegree = str(params['fitdegree'])
                # Update defaults
                self.analysisdict['fitfunction'] = fitfunc
                self.analysisdict['fitknotstep'] = knotstep
                self.analysisdict['fitdegree'] = fitdegree
                self.menu_p.rep_page.logMsg('Fitting with %s, %s, %s' % (
                        params['fitfuncname'], params['knotstep'], params['fitdegree']))
                funckeys = []
                for elem in keys:
                    if eval('dlg.{}CheckBox.GetValue()'.format(elem)):
                        funckeys.append(elem)
                func = plotstream.fit(keys=funckeys,
                            fitfunc=params['fitfunc'],
                            fitdegree=params['fitdegree'], knotstep=params['knotstep'],
                            starttime=params['starttime'],
                            endtime=params['endtime'])
                funclist = []
                for key in keys:
                    funclist.append([func])
                if params['fitfunc'] == 'none':
                    plotcont['functions'] = []
                elif isinstance(plotcont.get('functions'), list) and len(plotcont.get('functions')) > 0:
                    #[[x,y,z]] -> [[[func1],[func1,func2],[func1]]]
                    # Here: [x,y,z] -> [[func1],[func1,func2],[func1]]
                    oldfunclist = plotcont['functions']
                    newfunclist = []
                    for of in oldfunclist:
                        # of = [func1]
                        of.append(func)
                        newfunclist.append(of)
                    plotcont['functions'] = newfunclist
                else:
                    plotcont['functions'] = funclist

                self.plotdict[self.active_id] = plotcont
                self._initial_plot(self.active_id, keepplotdict=True)
            else:
                parameter = dlg.fitparameter
                if parameter:
                    funclist = []
                    for key in parameter:
                        params=parameter[key]
                        funclist.append(self.plotstream.fit(keys=keys,
                            fitfunc=params['fitfunc'],
                            fitdegree=params['fitdegree'], knotstep=params['knotstep'],
                            starttime=params['starttime'],
                            endtime=params['endtime']))
                    plotcont['functions'] = funclist
                    self.plotdict[self.active_id] = plotcont
                    self._initial_plot(self.active_id, keepplotdict=True)

            dlg.Destroy()
            self.menu_p.rep_page.logMsg('- data fitted')
        self.changeStatusbar("Ready")


    def analysis_onOffsetButton(self, event):
        """
        Method for offset correction
        """
        # Check whether DB still available
        self._check_db('minimal')

        self.changeStatusbar("Adding offsets ...")
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        offsetdict = {}

        if len(stream) > 0:
            plotstream = stream.copy()

            # get currently zoomed time limits and use as timerange
            xlimits = self.plot_p.xlimits
            if not xlimits:
                xlimits = [datacont.get('start'),datacont.get('end')]
            else:
                xlimits = [num2date(xlimits[0]),num2date(xlimits[-1])]

            # get existing deltas from database
            deltas = plotstream.header.get('DataDeltaValues','')

            if deltas == '':
                # Check data compensation values
                try:
                    xcorr = float(plotstream.header.get('DataCompensationX',''))
                    ycorr = float(plotstream.header.get('DataCompensationY',''))
                    zcorr = float(plotstream.header.get('DataCompensationZ',''))
                    if not xcorr=='' and not ycorr=='' and not zcorr=='':
                        deltas = 'x_{},y_{},z_{}'.format(-1*xcorr*1000.,-1*ycorr*1000.,-1*zcorr*1000.)
                except:
                    pass

            dlg = AnalysisOffsetDialog(None, title='Analysis: define offsets', keylst=keys, xlimits=xlimits, deltas=deltas)
            if dlg.ShowModal() == wx.ID_OK:
                for key in keys:
                    offset = eval('dlg.'+key+'TextCtrl.GetValue()')
                    if not offset in ['','0']:
                        if not float(offset) == 0:
                            offsetdict[key] = float(offset)
                val = dlg.offsetRadioBox.GetStringSelection()
                if str(val) == 'all':
                    toffset = dlg.timeshiftTextCtrl.GetValue()
                    if not plotstream._is_number(toffset):
                        toffset = 0
                    if not float(toffset) == 0:
                        offsetdict['time'] = timedelta(seconds=float(toffset))
                    plotstream = plotstream.offset(offsetdict)
                else:
                    stday = dlg.StartDatePicker.GetValue()
                    sttime = str(dlg.StartTimeTextCtrl.GetValue())
                    sd_tmp = datetime.fromtimestamp(stday.GetTicks())
                    sd = sd_tmp.strftime("%Y-%m-%d")
                    st= datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
                    edday = dlg.EndDatePicker.GetValue()
                    edtime = str(dlg.EndTimeTextCtrl.GetValue())
                    ed_tmp = datetime.fromtimestamp(edday.GetTicks())
                    ed = ed_tmp.strftime("%Y-%m-%d")
                    et= datetime.strptime(str(ed)+'_'+edtime, "%Y-%m-%d_%H:%M:%S")
                    plotstream = plotstream.offset(offsetdict, starttime=st, endtime=et)
                self.menu_p.rep_page.logMsg(" - offsets applied")

                plotstream.header['DataDeltaValuesApplied'] = 1
                streamid = self._initial_read(plotstream)
                self._initial_plot(streamid)
            dlg.Destroy()

        self.changeStatusbar("Ready")


    def analysis_onResampleButton(self, event):
        """
        DESCRIPTION
            Resampling the data set with a certain frequency using the linearly interpolated data set
        """
        self.changeStatusbar("Resampling ...")
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        sr = datacont.get("samplingrate")

        if len(stream) > 0:
            plotstream = stream.copy()
            dlg = AnalysisResampleDialog(None, title='Analysis: resampling parameters', keylst=keys, period=sr)
            if dlg.ShowModal() == wx.ID_OK:
                newperiod = dlg.periodTextCtrl.GetValue()
                plotstream = plotstream.resample(keys, period=float(newperiod), debugmode=False)
                self.menu_p.rep_page.logMsg('- resampled stream at period {} second'.format(newperiod))
                streamid = self._initial_read(plotstream)
                self._initial_plot(streamid)

            dlg.Destroy()
        self.changeStatusbar("Ready")


    def analysis_onActivityButton(self, event):
        """
        DESCRIPTION
            Determines K values using the FMI method.
            K9 limit and longitude need to be provided within the header
        """
        self.changeStatusbar("Getting activity (FMI method)...")

        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')

        if len(stream) > 0:
            plotstream = stream.copy()
            k9_limit = plotstream.header.get("StationK9", 500)
            long = plotstream.header.get("DataAcquisitionLongitude", 15.0)
            self.menu_p.rep_page.logMsg(" - determining K with K9 limit {} at lonitude {:.2f}".format(k9_limit, long))
            k = activity.K_fmi(plotstream, K9_limit=k9_limit, longitude=long, debug=False)
            streamid = self._initial_read(k)
            self._initial_plot(streamid)

        self.changeStatusbar("Ready")


    def analysis_onMeanButton(self, event):
        """
        DESCRIPTION
             Calculates means values for all keys of shownkeylist
        """
        self.changeStatusbar("Calculating means ...")

        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        if len(stream) > 0:
            teststream = stream.copy()
            meanfunc = 'mean'
            # limits
            xlimits = self.plot_p.xlimits

            if not xlimits == [stream.ndarray[0],stream.ndarray[-1]]:
                testarray = stream._select_timerange(starttime=xlimits[0],endtime=xlimits[1])
                teststream = DataStream([LineStruct()],stream.header,testarray)

            mean = [teststream.mean(key,meanfunction='mean',std=True,percentage=10) for key in keys]
            t_limits = teststream.timerange()
            self.menu_p.rep_page.logMsg("MEAN:")
            trange = 'for timerange: {} to {}\n'.format(t_limits[0],t_limits[1])
            self.menu_p.rep_page.logMsg(trange)
            for idx,me in enumerate(mean):
                column = stream.header.get('col-{}'.format(keys[idx],''))
                unit = stream.header.get('unit-col-{}'.format(keys[idx],''))
                me = list(me)
                decimals = int(self._determine_decimals(me[0]))
                if decimals < 3:
                    # use a minimum of three decimals (I know, it is arbitary but should fit the needs for most
                    # applications. If you need it correctly use the backend)
                    decimals = 3
                me[0] = np.round(me[0],decimals)
                me[1] = np.round(me[1],decimals)
                meanline = 'Key - {} -  {:>10}[{}]:  {} +/- {}'.format(keys[idx], column, unit, me[0],me[1])
                self.menu_p.rep_page.logMsg(meanline)
                trange = trange + '\n' + meanline
            # open message dialog
            dlg = wx.MessageDialog(self,
                            str(trange),
                            "Analysis: Mean values", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
        self.changeStatusbar("Ready")


    def analysis_onMaxButton(self, event):
        """
        DESCRIPTION
             Calculates max values for all keys of shownkeylist
        """
        self.changeStatusbar("Calculating maxima ...")

        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        if len(stream) > 0:
            teststream = stream.copy()
            meanfunc = 'mean'
            # limits
            xlimits = self.plot_p.xlimits

            if not xlimits == [stream.ndarray[0],stream.ndarray[-1]]:
                testarray = stream._select_timerange(starttime=xlimits[0],endtime=xlimits[1])
                teststream = DataStream([LineStruct()],stream.header,testarray)

            maxi = [teststream._get_max(key, returntime=True) for key in keys]
            t_limits = teststream.timerange()
            self.menu_p.rep_page.logMsg("MAXIMA:")
            trange = 'in timerange: {} to {}\n'.format(t_limits[0],t_limits[1])
            self.menu_p.rep_page.logMsg(trange)
            for idx,me in enumerate(maxi):
                column = stream.header.get('col-{}'.format(keys[idx],''))
                unit = stream.header.get('unit-col-{}'.format(keys[idx],''))
                me = list(me)
                decimals = int(self._determine_decimals(me[0]))
                if decimals < 3:
                    # use a minimum of three decimals (I know, it is arbitary but should fit the needs for most
                    # applications. If you need it correctly use the backend)
                    decimals = 3
                me[0] = np.round(me[0],decimals)
                meanline = 'Key - {} -  {:>10}[{}]:  {:<15} at {}'.format(keys[idx], column, unit, me[0], me[1])
                self.menu_p.rep_page.logMsg(meanline)
                trange = trange + '\n' + meanline
            # open message dialog
            dlg = wx.MessageDialog(self,
                            str(trange),
                            "Analysis: Maximum values", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
        self.changeStatusbar("Ready")


    def analysis_onMinButton(self, event):
        """
        DESCRIPTION
             Calculates minimum values for all keys of shownkeylist
        """
        self.changeStatusbar("Calculating minima ...")

        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        if len(stream) > 0:
            teststream = stream.copy()
            meanfunc = 'mean'
            # limits
            xlimits = self.plot_p.xlimits

            if not xlimits == [stream.ndarray[0],stream.ndarray[-1]]:
                testarray = stream._select_timerange(starttime=xlimits[0],endtime=xlimits[1])
                teststream = DataStream([LineStruct()],stream.header,testarray)

            mini = [teststream._get_min(key, returntime=True) for key in keys]
            t_limits = teststream.timerange()
            self.menu_p.rep_page.logMsg("MINIMA:")
            trange = 'in timerange: {} to {}\n'.format(t_limits[0],t_limits[1])
            self.menu_p.rep_page.logMsg(trange)
            for idx,me in enumerate(mini):
                column = stream.header.get('col-{}'.format(keys[idx],''))
                unit = stream.header.get('unit-col-{}'.format(keys[idx],''))
                me = list(me)
                decimals = int(self._determine_decimals(me[0]))
                if decimals < 3:
                    # use a minimum of three decimals (I know, it is arbitary but should fit the needs for most
                    # applications. If you need it correctly use the backend)
                    decimals = 3
                me[0] = np.round(me[0],decimals)
                meanline = 'Key - {} -  {:>10}[{}]:  {:<15} at {}'.format(keys[idx], column, unit, me[0],me[1])
                self.menu_p.rep_page.logMsg(meanline)
                trange = trange + '\n' + meanline
            # open message dialog
            dlg = wx.MessageDialog(self,
                            str(trange),
                            "Analysis: Minimum values", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
        self.changeStatusbar("Ready")


    def analysis_onSmoothButton(self, event):
        """
        DESCRIPTION
             Calculates smoothed curve based on the filter method without resampling (and not smooth)
        """
        self.changeStatusbar("Smoothing ... be patient")

        # open dialog to modify filter parameters
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        sr = datacont.get("samplingrate")
        filter_type = self.analysisdict.get('basefilter')

        resample_offset = 0.0
        plotstream = stream.copy()

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

        dlg = AnalysisFilterDialog(None, title='Analysis: Filter',  samplingrate=sr, resample=False, winlen=filter_width.seconds, resint=resample_period, resoff= resample_offset, filtertype=filter_type)
        if sr < 0.5: # use 1 second filter with 0.3 Hz cut off as default
            dlg.methodRadioBox.SetStringSelection('conservative')

        if dlg.ShowModal() == wx.ID_OK:
            filtertype = dlg.filtertypeComboBox.GetValue()
            filterlength = float(dlg.lengthTextCtrl.GetValue())
            missingdata = dlg.methodRadioBox.GetStringSelection()
            if missingdata == 'IAGA':
                miss = 'mean'
            elif missingdata == 'interpolate':
                miss = 'interpolate'

            plotstream = plotstream.filter(keys=keys,filter_type=filtertype,filter_width=timedelta(seconds=filterlength),missingdata=miss,resample=False,noresample=True)
            plotstream.header["SensorID"] = "smoothed-{}".format(plotstream.header.get("SensorID"))
            self.menu_p.rep_page.logMsg('- data smoothed: {} window, {} Hz passband'.format(filtertype,1./filterlength))

            streamid = self._initial_read(plotstream)
            self._initial_plot(streamid)

        self.changeStatusbar("Ready")


    def analysis_onBaselineButton(self, event):
        """
        DESCRIPTION
             Calculates baseline correction
        """
        self.changeStatusbar("Baseline adoption ...")
        currentdir = self.guidict.get('dirname')
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        keys = datacont.get('keys')
        plotcont = self.plotdict.get(self.active_id)
        shownkeys = plotcont.get('shownkeys')

        plotstream = stream.copy()

        if stream.header.get('DataAbsInfo'):
            existdlg = wx.MessageDialog(self, "Baseline date already connected to timeseries\n"
                        "Append new data (YES) or replace (No)\n".format(time),
                        "Baseline data existing", wx.YES_NO|wx.ICON_INFORMATION)
            if existdlg.ShowModal() == wx.ID_NO:
                plotstream.header['DataAbsInfo'] = ''
                plotstream.header['DataBaseValues'] = None
            existdlg.Destroy()

        # get all baseline ids which can be used
        baseids =  [baseid for baseid in self.baselinedict]
        if not self.active_baseid:
            self.active_baseid = baseids[-1]

        dlg = AnalysisBaselineDialog(None, title='Analysis: Baseline adoption', baseid=self.active_baseid, baselinedict=self.baselinedict,
                                     stream=plotstream,
                                     shownkeylist=shownkeys, keylist=keys,
                                     path=currentdir)
        # open dlg which allows to choose baseline data stream, function and parameters
        # Drop down for baseline data stream (BaseID: filename)
        # Text window describing baseline parameter
        # button to modify baseline parameter
        if dlg.ShowModal() == wx.ID_OK:
            # returns a pointer to the selected baseline parameters
            fitparameters = dlg.fitparameters
            baseid = dlg.active_baseid
            self.active_baseid = baseid
            absstreamid = self.baselinedict.get(baseid).get("streamid")
            absstream = self.datadict.get(absstreamid).get("dataset")

            baselinefunclist = []   # will hold a list of iundividual functions obtained by stream.baseline
            if not baseid:
                self.menu_p.rep_page.logMsg('- baseline adoption aborted as no fit function defined')
                self.changeStatusbar("Ready")
            else:
                for fitparameter in fitparameters:
                    fitpara = fitparameters.get(fitparameter)
                    baselinefunclist.append(plotstream.baseline(absstream,fitfunc=fitpara.get('fitfunc'), knotstep=float(fitpara.get('knotstep')), fitdegree=int(fitpara.get('fitdegree')), startabs=fitpara.get('starttime'), endabs=fitpara.get('endtime'), extradays=0, debug=False))

                self.menu_p.rep_page.logMsg('- baseline adoption performed using DI data from {}. Parameters: '
                                            'from Baseline ID {}'.format(self.baselinedict.get('filename'),baseid))

                # calc_bc True will directly calulate the baseline corrected values and disable bcCorr
                calc_bc = self.analysisdict.get("baselinedirect")
                if calc_bc:
                    msgtext = "Baseline correction performed - Ready"
                    #self.plotstream = self.plotstream.bc(function=baselinefunclist)
                    plotstream = plotstream.bc(function=baselinefunclist)
                    # Eventually update delta F - recalculate from F as variometer data has been corrected
                    if 'df' in plotstream.variables() and 'f' in plotstream.variables():
                        plotstream = plotstream.delta_f()
                    plotstream.header['DataType'] = 'BC'
                    streamid = self._initial_read(plotstream)
                    self._initial_plot(streamid)
                else:
                    msgtext = "BC function available - Ready"
                    dlg = wx.MessageDialog(self, "Adopted baseline calculated.\n"
                               "Baseline parameters added to meta information and option 'Baseline Corr' on 'Analysis' panel now enabled.\n",
                               "Adopted baseline", wx.OK|wx.ICON_INFORMATION)
                    dlg.ShowModal()
                    dlg.Destroy()
                    # header is changed - apply is possible now
                    streamid = self._initial_read(plotstream)
                    self._initial_plot(streamid)

                self.changeStatusbar(msgtext)
        else:
            self.changeStatusbar("Ready")


    def analysis_onCalcfButton(self, event):
        """
        DESCRIPTION
             Calculates delta F values
        """
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')

        if len(stream) > 0:
            plotstream = stream.copy()
            cont = True
            if 'f' in plotstream.variables():
                existdlg = wx.MessageDialog(self, "F data already existing\n"
                            "Replace with vector sum?\n",
                            "F data existing", wx.YES_NO|wx.ICON_INFORMATION)
                if existdlg.ShowModal() == wx.ID_YES:
                    existdlg.Destroy()
                else:
                    existdlg.Destroy()
                    cont = False

            if cont:
                self.changeStatusbar("Calculating F from components ...")
                plotstream = plotstream.calc_f(skipdelta=True)
                if 'df' in plotstream.variables(): # why?
                    plotstream = plotstream.delta_f()
                self.menu_p.rep_page.logMsg(' - determined f from x,y,z')
                streamid = self._initial_read(plotstream)
                self._initial_plot(streamid)
        self.changeStatusbar("Ready")


    def analysis_onPowerButton(self, event):
        """
        DESCRIPTION
             Calculates Power spectrum of one component
        """
        self.changeStatusbar("Power spectrum ...")

        sharey = False
        self.plot_p.power_plot(self.active_id, self.datadict, self.plotdict, sharey=sharey)


    def analysis_onSpectrumButton(self, event):
        """
        DESCRIPTION
             Calculates Power spectrum of one component
        """
        self.changeStatusbar("Spectral plot ...")
        self.plot_p.spec_plot(self.active_id, self.datadict, self.plotdict)


    def analysis_onDailyMeansButton(self,event):
        """
        DESCRIPTION
            Calculate daily mean values by default from x,y,z columns and put uncertainty data into dx,dy,dz
            This method is used for adopted BLV file creation (INTERMAGNET).
            If basevalue data files are provided then means are calculated from the basevalue data in dx,dy,dz
        """
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')
        plotcont = self.plotdict.get(self.active_id)
        keys = plotcont.get('shownkeys')
        if stream.header.get('DataFormat') == 'MagPyDI' or stream.header.get('DataType','').startswith('MagPyDI'):
            keys=['dx','dy','dz']
        else:
            keys = False
        plotstream = stream.copy()
        plotstream = plotstream.dailymeans(keys)

        e1={'key':'dx'}
        e2={'key':'dy'}
        e3={'key':'dz'}

        plotcont = self.plotdict[self.active_id]
        streamid = self._initial_read(plotstream)
        # activate errobars
        plotcont['errorbars'] = [e1, e2, e3]
        self.plotdict[streamid] = plotcont
        self._initial_plot(streamid, keepplotdict=True)

        self.changeStatusbar("Ready")


    def analysis_onApplyBCButton(self,event):
        """
        Apply baselinecorrection
        """
        self.changeStatusbar("Applying baseline ...")
        datacont = self.datadict.get(self.active_id)
        stream = datacont.get('dataset')

        plotstream = stream.bc()
        plotstream.header['DataType'] = 'BC'
        # Eventually update delta F
        if 'df' in plotstream.variables() and 'f' in plotstream.variables():
            plotstream = plotstream.delta_f()
        streamid = self._initial_read(plotstream)
        self._initial_plot(streamid)
        self.changeStatusbar("Ready")


    # ##################################################################################################################
    # ####    DI Panel                                         #########################################################
    # ##################################################################################################################


    def di_onLoadDI(self,event):
        """
        DESCRIPTION
            Open dialog to load DI data.
            All DI data relevant paranmeters are stored within a station dictionary of the analysisdict.
            Each station can have its own parameters. All DI analysis methods will be performed using the
            selected default station in options.

            Currently actively loaded data is stored in self.active_didata

            A station dictionary has the following contents:

            content['divariopath'] = os.path.join(basepath, '*')
            content['discalarpath'] = os.path.join(basepath, '*')
            content['diexpD'] = 0.0
            content['diexpI'] = 0.0
            content['didatapath'] = basepath             # default path where to find DI measurement files
            content['divariourl'] = ''
            content['discalarurl'] = ''
            content['divarioDBinst'] = '1'
            content['discalarDBinst'] = '4'
            content['divariosource'] = 0
            content['discalarsource'] = 0
            content['diusedb'] = False
            content['divariocorr'] = False
            content['diid'] = ''
            content['ditype'] = 'manual'
            content['diazimuth'] = 0.0
            content['dipier'] = 'A2'
            content['dialpha'] = 0.0
            content['dibeta'] = 0.0
            content['dideltaF'] = 0.0
            content['dideltaD'] = 0.0
            content['dideltaI'] = 0.0
            content['blvoutput'] = 'HDZ'
            content['fluxgateorientation'] = 'inline'
            content['diannualmean'] = []
            content['didbadd'] = False
            content['scalevalue'] = True
            content['double'] = True
            content['order'] = 'MU,MD,EU,WU,ED,WD,NU,SD,ND,SU'
        """
        # get defaultstation
        defaultstation = self.analysisdict.get('defaultstation')
        allstations = self.analysisdict.get('stations')
        dicont = allstations.get(defaultstation,{})
        print ("CHECKING", defaultstation, dicont)
        if not dicont:
            # identify a non-empty dictionary in allstations and get this one
            stats = [s for s in allstations]
            for stat in stats:
                dicont = allstations.get(stat, {})
                if dicont:
                    defaultstation = stat
                    break
        print ("CHECKING again", defaultstation, dicont)
        newdicont = dicont.copy()
        defaultpath = dicont.get('didatapath','')
        debug = False

        if os.path.isfile(defaultpath):
            defaultpath = os.path.split(defaultpath)[0]

        services = self.analysisdict.get('webservices',{})
        default = self.analysisdict.get('defaultwebservice','imws')
        db, success = self._db_connect(*self.magpystate.get('dbtuple'))

        dlg = LoadDIDialog(None, title='Get DI data', dicont=dicont, db=db, services=services, defaultservice=default)
        dlg.databaseTextCtrl.SetValue('Connected: {}'.format(self.magpystate.get('dbtuple',['None'])[-1]))
        dlg.ShowModal()
        if not dlg.pathlist == 'None' and not len(dlg.pathlist) == 0:
            self.menu_p.rep_page.logMsg("- loaded DI data")
            dipathlist = dlg.pathlist
            if isinstance(self.active_didata, dict):
                info = "{}: {} dataset(s)".format(dipathlist.get('station'),len(dipathlist.get('absdata')))
                self.menu_p.abs_page.diTextCtrl.SetValue(info)
                self.menu_p.abs_page.diSourceLabel.SetLabel("Source: {}".format(dipathlist.get('source')))
            elif isinstance(dipathlist,list):
                print ("outdated - should not happen any more")
            # Update di data path
            newdicont['didatapath'] = dlg.dirname
            self.active_didata = dipathlist
            self.menu_p.abs_page.AnalyzeButton.Enable()
        dlg.Destroy()

        if debug:
            print ("Now get parameters from active didata and update analysisdict[stations].get(stationid)")
        stationid = self.active_didata.get('station')

        if stationid and self.active_didata:
            if stationid == defaultstation:
                # that should be the normal case
                pass
            elif stationid in allstations:
                self.analysisdict['defaultstation'] = stationid
                if debug:
                    print ("DI analysis: Switching default station to existing input of {}".format(stationid))
            else:
                # create a new stationinput
                self.analysisdict['defaultstation'] = stationid
                if debug:
                    print("DI analysis: Creating new station input {}".format(stationid))
            newdicont['dipier'] = self.active_didata.get('selectedpier')
            newdicont['diazimuth'] = self.active_didata.get('azimuth')
            allstations[stationid] = newdicont
            self.analysisdict['stations'] = allstations
        else:
            msgtxt = "Could not identify DI data.\n Please check your data source."
            dlg = wx.MessageDialog(self, msgtxt, "DI data failed", wx.OK | wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()

    def di_onDIParameter(self,event):
        """
        open dialog to modify analysis parameters for DI
        """
        defaultstation = self.analysisdict.get('defaultstation','')
        allstations = self.analysisdict.get('stations',{})
        dicont = allstations.get(defaultstation,{})

        # parameters should be updated from loaded data set
        boollist = ['diusedb','divariocorr','didbadd','scalevalue','double']
        numlist = ['diexpD','diexpI','divariosource','discalarsource','diazimuth','dialpha','dibeta','dideltaF','dideltaD','dideltaI']
        listlist = ['diannualmean']

        valuedict = {}
        dlg = ParameterDictDialog(None, title="Modify DI analysis parameter for {}".format(defaultstation), dictionary=dicont, preselect=[defaultstation])
        if dlg.ShowModal() == wx.ID_OK:
            for el in dlg.panel.elementlist:
                if not el[1].GetName() == 'Label':
                    key = el[1].GetName()
                    if key in boollist:
                        val = el[1].GetStringSelection()
                        if val in ['True','true',True]:
                            val = True
                        else:
                            val = False
                        valuedict[key] = val
                    else:
                        try: # there might be other panel elements like StaticText without values
                            val = el[1].GetValue()
                            if key in listlist and val.startswith('['):
                                if val == '[]':
                                    val = []
                                else:
                                    val = val.replace('[','').replace(']','').split(',')
                            elif key in numlist and methods.is_number(val):
                                val = float(val)
                            valuedict[key] = val
                        except:
                            pass
            self.menu_p.abs_page.parameterRadioBox.SetStringSelection('options')
        else:
            valuedict = dicont.copy()
        dlg.Destroy()

        allstations[defaultstation] = valuedict
        self.analysisdict['stations'] = allstations


    def di_onDefineVarioScalar(self,event):
        """
        DESCRIPTION
            open dialog to load variometer and scalar data for DI analysis
        """
        def getPathFromDict(typ, dictionary, db):

            filepath = dictionary.get('di{}path'.format(typ),'')
            selection = dictionary.get('di{}source'.format(typ),0)
            url = dictionary.get('di{}url'.format(typ),'')
            dbtable = dictionary.get('di{}DBinst'.format(typ),'')
            if selection == 0:
                return filepath
            elif selection == 1:
                return [db, dbtable]
            elif selection == 2:
                return url

        def display(value):
            if isinstance(value, list):
                display = str(value[-1])
            else:
                display = str(value)
            return display

        db, success = self._db_connect(*self.magpystate.get('dbtuple'))
        defaultstation = self.analysisdict.get('defaultstation')
        allstations = self.analysisdict.get('stations')
        dicont = allstations.get(defaultstation,{})

        discalarpath = dicont.get('discalarpath','')
        divariopath = dicont.get('divariopath','')
        discalarurl = dicont.get('discalarurl','')
        divariourl = dicont.get('divariourl','')
        vselection = dicont.get('divariosource',0)
        sselection = dicont.get('discalarsource',0)
        varioDB = dicont.get('divarioDBinst','1')
        scalarDB = dicont.get('discalarDBinst','4')
        services = self.analysisdict.get('webservices',{})
        defaultwebservice = self.analysisdict.get('defaultwebservice')
        sourcelist = ['file','database','webservice']

        dialog = LoadVarioScalarDialog(None, title="Choose data source", vselection=vselection, sselection=sselection, defaultvariopath=divariopath, defaultscalarpath=discalarpath, db=db, defaultvariotable=varioDB, defaultscalartable=scalarDB, services=services, defaultservice=defaultwebservice)
        if dialog.ShowModal() == wx.ID_OK:
            # get selected options:
            dicont['divariosource'] = dialog.vchoice
            dicont['discalarsource'] = dialog.schoice
            varioext = dialog.varioExtComboBox.GetValue()
            scalarext = dialog.scalarExtComboBox.GetValue()
            dicont['discalarpath'] = os.path.join(dialog.defaultscalarpath,scalarext)
            dicont['divariopath'] = os.path.join(dialog.defaultvariopath,varioext)
            dicont['divarioDBinst'] = dialog.varioDBComboBox.GetValue()
            dicont['discalarDBinst'] = dialog.scalarDBComboBox.GetValue()
            dicont['divariourl'] = dialog.divariows
            dicont['discalarurl'] = dialog.discalarws
            self.menu_p.abs_page.VarioSourceLabel.SetLabel("Vario: from {}".format(sourcelist[dialog.vchoice]))
            self.menu_p.abs_page.ScalarSourceLabel.SetLabel("Scalar: from {}".format(sourcelist[dialog.schoice]))
            res_divariopath = getPathFromDict("vario", dicont, db)
            res_discalarpath = getPathFromDict("scalar", dicont, db)

            self.menu_p.abs_page.varioTextCtrl.SetValue("{}".format(display(res_divariopath)))
            self.menu_p.abs_page.scalarTextCtrl.SetValue("{}".format(display(res_discalarpath)))

            allstations[defaultstation] = dicont
            self.analysisdict['stations'] = allstations

        dialog.Destroy()


    def di_onDIAnalyze(self,event):
        """
        open dialog to load DI data
        """

        db, success = self._db_connect(*self.magpystate.get('dbtuple'))
        defaultstation = self.analysisdict.get('defaultstation')
        allstations = self.analysisdict.get('stations')
        dicont = allstations.get(defaultstation,{})
        debug = True

        # Select the chosen source for variometer and scalar data
        divariosource = dicont.get('divariosource')
        discalarsource = dicont.get('discalarsource')
        if divariosource in [0,'0']:
            divario = {'file': dicont.get('divariopath','')}
        elif divariosource in [1,'1']:
            divario = {'db': (db,dicont.get('divarioDBinst'))}
        else:
            divario = {'file': dicont.get('divariourl')}
        if discalarsource in [0,'0']:
            discalar = {'file': dicont.get('discalarpath','')}
        elif discalarsource in [1,'1']:
            discalar = {'db': (db,dicont.get('discalarDBinst'))}
        else:
            discalar = {'file': dicont.get('discalarurl')}

        primaryparametersource = self.menu_p.abs_page.parameterRadioBox.GetStringSelection()

        # redefining blv output by orientation of the variometer (i.e. xyz output requires xyz variometer
        variometerorientation = None
        residualsign = 1
        blvoutput = dicont.get('blvoutput')
        if blvoutput.lower().startswith("xyz"):
            print (" Selected Variometerorientation in xyz")
            variometerorientation = "XYZ"

        fluxorient = dicont.get('fluxgateorientation')
        if fluxorient.lower().startswith('opp'):
            print(" Selected opposite fluxgate orientation for DI flux")
            residualsign = -1

        magrotation = dicont.get('divariocorr')

        if self.active_didata:
            # Identify source -> Future version: use absolutClass which contains raw data
            #                    and necessary variation,scalar data
            activatereport = True
            prev_redir = None
            if activatereport:
                if debug:
                    print("sending reports to dialogTextCtrl now...")
                prev_redir = sys.stdout
                redir=RedirectText(self.menu_p.abs_page.dilogTextCtrl)
                sys.stdout=redir
            else:
                if debug:
                    print ("sending reports to stdout by default")

            absstream = DataStream()

            if db:
                print ("Database connected")
            elif dicont.get('usedb') and not db:
                print ("No database connected")
            # no report necessary for all other cases

            if isinstance(self.active_didata, dict):
                # Dictionary is the new default - all processes will return a dictionary
                self.changeStatusbar("Processing DI data ... please be patient")
                stationid = defaultstation  # should already be replaced by some valid station code
                if not self.active_didata.get('station') == defaultstation:
                    print ("WARNING - station differenecs between source and options")
                starttime = self.active_didata.get('mindatetime')
                endtime = self.active_didata.get('maxdatetime')
                pier = self.active_didata.get('selectedpier')
                abstable = "DIDATA_{}".format(stationid.upper())
                absdata = self.active_didata.get('absdata')
                azimuth = self.active_didata.get('azimuth')
                if variometerorientation and variometerorientation.lower().startswith("xyz"):
                    print(" Selected basevalue output in xyz components")
                if not dicont.get('diazimuth') == azimuth:
                    print(" Azimuth given in data source and in options are different")
                    if primaryparametersource == 'options':
                        azimuth = dicont.get('diazimuth')
                if not dicont.get('dipier') == pier:
                    print(" Pier given in data source and in options are different")
                    if primaryparametersource == 'options':
                        pier = dicont.get('dipier')
                # check for valid azimuth and eventually request input
                if not azimuth or np.isnan(azimuth) or azimuth == 'nan' or azimuth == None:
                    print ("no aziumth so far - please define")
                    # open dialog
                    ok = False
                    dlg = SetAzimuthDialog(None, title='Define azimuth', azimuth=0.0)
                    if dlg.ShowModal() == wx.ID_OK:
                        azimuth = float(dlg.AzimuthTextCtrl.GetValue())
                        ok = True
                    dlg.Destroy()
                    if ok:
                        print ("Using manually provided aziumth: {}".format(azimuth))
                if debug:
                    print ("Some variables to test:")

                #TODO diusedb
                print (divario, discalar)
                absstream = di.absolute_analysis(absdata, divario, discalar, db=db, magrotation=magrotation,
                                                 annualmeans=dicont.get('diannualmeans'), expD=dicont.get('diexpD'),
                                                 expI=dicont.get('diexpI'), stationid=stationid,
                                                 pier=pier, alpha=dicont.get('dialpha'), beta=dicont.get('dibeta'),
                                                 deltaF= dicont.get('deltaF'), starttime=starttime,endtime=endtime,
                                                 azimuth=azimuth, variometerorientation=variometerorientation,
                                                 residualsign=residualsign, absstruct=True)

            else:
                print ("Could not identify absolute data")
                absstream = DataStream()

            try:
                if not divario == '' and not discalar == '':
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

            if activatereport:
                sys.stdout=prev_redir

            # only if more than one point is selected
            self.changeStatusbar("Ready")
            if absstream and len(absstream) > 0:
                streamid = self._initial_read(absstream)
                self._initial_plot(streamid)

                if not str(self.menu_p.abs_page.dilogTextCtrl.GetValue()) == '':
                    self.menu_p.abs_page.ClearLogButton.Enable()
                    self.menu_p.abs_page.SaveLogButton.Enable()
            else:
                pass


    def onSaveDIData(self, event):
        """
        DESCRIPTION
            Save data of the logger to file
        """
        dirname = self.guidict.get('dirname')
        saveFileDialog = wx.FileDialog(self, "Save As", "", dirname,
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



    # ##################################################################################################################
    # ####    Report Panel                                     #########################################################
    # ##################################################################################################################


    def report_onSaveLogButton(self, event):
        """
        DESCRIPTION
            Save a report to file
        :param event:
        :return:
        """
        dir = self.guidict.get('dirname')

        saveFileDialog = wx.FileDialog(self, "Save As", "", dir,
                                       "Log files (*.log)|*.log",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        saveFileDialog.ShowModal()
        savepath = saveFileDialog.GetPath()
        text = self.menu_p.rep_page.logger.GetValue()
        saveFileDialog.Destroy()

        logfile = open(savepath, "w")
        logfile.write(text)
        logfile.close()


    @deprecated("currently only used by spectrogram - remove when replacing this method")
    def getComponent(self):
        """
        DESCRIPTION
             Calls a dialog where the user can choose a component
        """
        dlg = SelectFromListDialog(None, title='Select sensor',
                selectlist=self.shownkeylist, name='Component')
        try:
            if dlg.ShowModal() == wx.ID_OK:
                comp = dlg.getComponent()
                return comp
            else:
                return None
        finally:
            dlg.Destroy()



    # ------------------------------------------------------------------------------------------
    # ################
    # Stream page functions
    # ################
    # ------------------------------------------------------------------------------------------

    @deprecated("Error bars are selected in the plot options menu")
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


    @deprecated("use dateformatter for optimzing x labels")
    def onConfinexCheckBox(self,event):
        """
        DESCRIPTION
            Switch display of error bars.
        RETURNS
            kwarg for OnPlot method
        """
        plotcont = self.plotdict.get(self.active_id)
        if not self.menu_p.str_page.confinexCheckBox.GetValue():
            plotcont['confinex'] = False
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


    @deprecated("Remove this and add to options menu")
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


    def onAnnotateCheckBox(self,event):
        """
        Restore originally loaded data
        """
        #### get True or False
        if not self.menu_p.fla_page.annotateCheckBox.GetValue():
            #self.annotate=False
            self.plotopt['annotate'] = False
            self.menu_p.fla_page.annotateCheckBox.SetValue(False)
        else:
            #self.annotate=True
            self.plotopt['annotate'] = True
            self.menu_p.fla_page.annotateCheckBox.SetValue(True)

        #mp.plot(self.plotstream,annotate=True)
        self.ActivateControls(self.plotstream)
        self.OnPlot(self.plotstream,self.shownkeylist)


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
                time = self.plotstream.ndarray[DataStream().KEYLIST.index('time')][idx]
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
                    keys2flag = [el for el in keys2flag if el in DataStream().KEYLIST]
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

        debug = False
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
                keys2flag = [el for el in keys2flag if el in DataStream().KEYLIST]
                comment = dlg.CommentTextCtrl.GetValue()
                flagid = dlg.FlagIDComboBox.GetValue()
                flagid = int(flagid[0])

                above = min(self.ylimits)
                below = max(self.ylimits)
                starttime =num2date(min(self.xlimits))
                endtime = num2date(max(self.xlimits))

                if debug:
                    print ("GUI FlagID:", flagid, starttime, endtime)
                flaglist = self.plotstream.flag_range(keys=selkey,flagnum=flagid,text=comment,keystoflag=keys2flag,starttime=starttime,endtime=endtime,above=above,below=below)
                self.menu_p.rep_page.logMsg('- flagged selection: added {} flags'.format(len(flaglist)))
                if debug:
                    print ("GUI Flaglist", flaglist)

        if len(flaglist) > 0:
            #print ("FlagRange: Please note that the range definition needs an update as only single values are considered")
            #print ("TEst", flaglist)
            self.flaglist.extend(flaglist)
            self.plotstream = self.plotstream.flag(flaglist)

            #print (self.plotstream.ndarray)
            self.ActivateControls(self.plotstream)
            #self.annotate = True
            self.plotopt['annotate'] = True

            self.menu_p.fla_page.annotateCheckBox.SetValue(True)
            self.OnPlot(self.plotstream,self.shownkeylist)
        self.changeStatusbar("Ready")

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
        self.menu_p.fla_page.annotateCheckBox.SetValue(False)
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

            self.menu_p.fla_page.annotateCheckBox.SetValue(True)
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
            try:
                dlg.startFlagDatePicker.SetValue(pydate2wxdate(num2date(startdate)))
                dlg.startFlagTimePicker.SetValue(starttime)
            except:
                pass
            try:
                dlg.endFlagDatePicker.SetValue(pydate2wxdate(num2date(enddate)))
                dlg.endFlagTimePicker.SetValue(endtime)
            except:
                pass
            if dlg.ShowModal() == wx.ID_OK:
                # get values from dlg
                flagtype = dlg.rangeRadioBox.GetStringSelection()
                keys2flag = dlg.AffectedKeysTextCtrl.GetValue()
                keys2flag = keys2flag.split(',')
                keys2flag = [el for el in keys2flag if el in DataStream().KEYLIST]
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

            self.menu_p.fla_page.annotateCheckBox.SetValue(True)
            self.OnPlot(self.plotstream,self.shownkeylist)

        self.changeStatusbar("Ready")


    def onFlagLoadButton(self,event):
        """
        DESCRIPTION
            Opens a dialog which allows to load flags either from a DB or from file
        """
        # Check whether DB still available
        self._check_db('minimal')

        sensorid = self.plotstream.header.get('SensorID','')
        # Open Dialog and return the parameters threshold, keys, timerange
        self.changeStatusbar("Loading flags ... please be patient")
        dlg = StreamLoadFlagDialog(None, title='Load Flags', db = self.db, sensorid=sensorid, start=self.plotstream.start(),
                                   end=self.plotstream.end(),last_dir = self.last_dir)
        dlg.ShowModal()
        if len(dlg.flaglist) > 0:
            flaglist = dlg.flaglist
            #print ("Loaded flags like", flaglist[0], self.flaglist[0])
            self.flaglist.extend(flaglist)
            #print ("extended flaglist looking like", self.flaglist)
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
        self._check_db('minimal')

        currentlen = len(self.flaglist)

        #print ("FlagSave", self.flaglist)

        self.changeStatusbar("Saving flags ...")
        dlg = StreamSaveFlagDialog(None, title='Save Flags', db = self.db, flaglist=self.flaglist,
                                   last_dir=self.last_dir)
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
        flagid = DataStream().KEYLIST.index('flag')
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
        if flagid == 0:
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
            self.menu_p.fla_page.annotateCheckBox.SetValue(True)
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
        if flagid == 0:
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
            self.menu_p.fla_page.annotateCheckBox.SetValue(True)
            self.OnPlot(self.plotstream,self.shownkeylist)

    # ------------------------------------------------------------------------------------------
    # ################
    # Meta page functions
    # ################
    # ------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------
    # ####################
    # Stream Operations functions
    # ####################
    # ------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------
    # ################
    # Absolute functions
    # ################
    # ------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------
    # ################
    # Report page functions
    # ################
    # ------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------
    # ################
    # Monitor page functions
    # ################
    # ------------------------------------------------------------------------------------------


    def onConnectMARCOSButton(self, event):
        # active if database is connected
        # open dlg

        # Check whether DB still available
        self._check_db('minimal')

        def dataAvailabilityCheck(db, datainfoidlist):
            existinglist = []
            if not len(datainfoidlist) > 0:
                return datainfoidlist
            for dataid in datainfoidlist:
                ar = db.select('time', dataid, expert="ORDER BY time DESC LIMIT 10")
                if len(ar) > 0:
                    existinglist.append(dataid)
            return existinglist


        self.menu_p.rep_page.logMsg('- Selecting MARCOS table for monitoring ...')
        output = self.db.select('DataID,DataMinTime,DataMaxTime','DATAINFO')
        datainfoidlist = [elem[0] for elem in output]
        datainfoidlist = dataAvailabilityCheck(self.db, datainfoidlist)

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
            vals = self.db.select('SensorID,DataSamplingRate,ColumnContents,ColumnUnits,StationID','DATAINFO', 'DataID = "'+datainfoid+'"')
            vals = vals[0]
            sensid= vals[0]
            sr= float(vals[1].strip('sec'))
            keys= vals[2].split(',')
            units= vals[3].split(',')
            stationid= vals[4]
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

        parameter = ','.join([DataStream().KEYLIST[idx+1] for idx,key in enumerate(keys) if not key=='' and DataStream().KEYLIST[idx+1] in DataStream().NUMKEYLIST])

        self.plot_p.datavars = {0: datainfoid, 1: parameter, 2: limit, 3: pad, 4: currentdate, 5: unitlist, 6: coverage, 7: period, 8: self.db, 15: stationid}
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
            #from magpy.collector import collectormethods as colsup
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
            client = mqtt.Client()

            if not martasuser in ['',None,'None','-']:
                #client.tls_set(tlspath)  # check http://www.steves-internet-guide.com/mosquitto-tls/
                client.username_pw_set(martasuser, password=martaspasswd)  # defined on broker by mosquitto_passwd -c passwordfile user

            #client.on_connect = colsup.on_connect
            #client.on_message = colsup.on_message

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
                #print ("Got data from init - remove this message ater 0.3.99", maxloop)
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

            if success: # and len(colsup.identifier) > 0:
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
                    #print ("key", key)
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
        self.MainMenu.EnableTop(0, False)
        self.MainMenu.EnableTop(1, False)
        self.MainMenu.EnableTop(2, False)
        self.MainMenu.EnableTop(3, False)
        self.MainMenu.EnableTop(4, False)
        self.MainMenu.EnableTop(5, False)
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
            header = db.fields_to_dict(dataid)
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
        self.plot_p.array = [[] for el in DataStream().KEYLIST]
        self.stream.header['StationID'] = self.plot_p.datavars.get(15)
        self.stream.header['SensorID'] = self.plot_p.datavars.get(0)[:-5]
        self.stream.header['DataID'] = self.plot_p.datavars.get(0)
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

        self.MainMenu.EnableTop(0, True)
        self.MainMenu.EnableTop(1, True)
        self.MainMenu.EnableTop(2, True)
        self.MainMenu.EnableTop(3, True)
        self.MainMenu.EnableTop(4, True)
        self.MainMenu.EnableTop(5, True)

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


    # TODO Clean this method up and make it consisting of modules
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
                    if sys.version_info >= (3,0,0):
                        with open(savepath, "w", newline='') as myfile:
                            myfile.write(report)
                    else:
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


        def readSecData(secondpath,seconddata,rmonth,year,dataid=None,debug=False):
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
                cdfname = '*'+str(year)+str(rmonth).zfill(2)+'_000000_PT1S_4.cdf'
                loadpath = os.path.join(secondpath,cdfname)
                try:
                    secdata = read(loadpath,debug=debug)
                except:
                    secdata = DataStream()
                    success = 6
                if not secdata.length()[0] > 0:
                    print ("did not find an appropriate monthly cdf file")
                    cdfname = '*.cdf'
                    cdfnamelist = ['*_000000_PT1S_4.cdf','*.cdf']
                    success = 1
                    for cdfname in cdfnamelist:
                        loadpath = os.path.join(secondpath,cdfname)
                        success += 1
                        # File name issue !!
                        try:
                            secdata = read(loadpath,debug=debug, starttime=starttime, endtime=endtime)
                            success = success
                        except:
                            secdata = DataStream()
                            success = 6
                        if secdata.length()[0] > 0:
                            break

            elif seconddata == 'pycdf':
                if dataid:
                    cdfname = dataid+'_vario_sec_'+str(year)+str(rmonth).zfill(2)+'.cdf'
                    loadpath = os.path.join(secondpath,cdfname)
                    secdata = read(loadpath,debug=debug)
                else:
                    print ("please provide a sensorid")
            elif seconddata == 'iaga':
                loadpath = os.path.join(secondpath,'*.sec')
                try:
                    secdata = read(loadpath,starttime=starttime,endtime=endtime,debug=debug)
                except:
                    secdata = DataStream()
                    success = 6

            return secdata, success

        def readMinData(checkchoice,minutedata,minutepath,month,rmonth,year=1900,resolution=None,debug=False):
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
                        mindata = read(path, resolution=resolution, debug=debug)
                    else:
                        mindata = read(path, debug=debug)
                except:
                    mindata = DataStream()
                    success = 6

            return mindata, success



        self.changeStatusbar("Checking data ... ")

        ## get random month for quick check
        from random import randint
        #import locale  # to get english month descriptions
        #old_loc = locale.getlocale(locale.LC_TIME)
        #try:
        #    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
        #except:
        #    pass

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
                    else:
                        iafcnt += len(glob.glob(os.path.join(minutepath,"*.zip")))
                        if iafcnt > 0 and iafpath == '':
                            iafpath = os.path.join(minutepath,"*.zip")
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
                    if succlst[0]<4:
                        succlst[0]=4
                else:
                    reportmsg += "Step 1: +++ Found appropriate amount of one second data files\n"
                    #print ("Here", minutepath, succlst)
                    if minutepath == '':
                        onlysec = True
                        #print ("Checkparameter")
                        checkparameter['step3'] = False
                        checkparameter['step5'] = False
                        checkparameter['step7'] = False
                    if succlst[0]<1:
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
                #locale.setlocale(locale.LC_TIME, old_loc)
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
                    frame = wx.Frame(None, -1, 'win.py')
                    frame.SetDimensions(0,0,200,50)
                    dlg = wx.TextEntryDialog(frame, 'Enter the YEAR you are checking','Entry')
                    dlg.SetValue("2021")
                    if dlg.ShowModal() == wx.ID_OK:
                        yearstr = dlg.GetValue()
                    dlg.Destroy()
                    try:
                        year = int(yearstr)
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
                        errormsg += "Step 2: Reading of one second data - file names do not follow the (monthly) ImagCDF convention\n"
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
                    noncriticalheadlist = ['DataReferences']
                    for head in META:
                        value = secdata.header.get(head,'')
                        if head in noncriticalheadlist and value == '':
                            warningmsg += "Step 2: (info) Second data: no meta information for {} (not critical)\n".format(head.replace('Data',''))
                        elif value == '':
                            warningmsg += "Step 2: (warning) !!! Second data: no meta information for {}\n".format(head)
                            headfailure = True
                    if not headfailure:
                        reportmsg += "Step 2: +++ Second data: each required header is present. IMPORTANT: did not check content !!\n"
                    else:
                        reportmsg += "Step 2: !!! Second data: meta information warnings found\n"
                        succlst[1] = 4


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
                        #locale.setlocale(locale.LC_TIME, old_loc)
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
                        streamid = self._initial_read(mindata)
                        if streamid:  # will create a new input into datadict
                            self._initial_plot(streamid)
                        #if self.InitialRead(mindata):
                        #    self.OnInitialPlot(self.plotstream)
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
                        #print ("Here", DataStream().KEYLIST.index('df'), iafhour.ndarray[DataStream().KEYLIST.index('df')], minfiltdata.ndarray[DataStream().KEYLIST.index('df')])
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
                                    colnum = DataStream().KEYLIST.index(col)
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
                                    colnum = DataStream().KEYLIST.index(col)
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
                        #locale.setlocale(locale.LC_TIME, old_loc)
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
                        if secdata.length()[0] > 0:
                            oldsecdata = secdata.copy()
                    for checkmonth in monthlist:
                        print ("Loading month {}".format(checkmonth))
                        if (checkmonth == rmonth) and secdata.length()[0] > 0:
                            self.changeStatusbar("Step 4: Taking one second data (loaded already for data checks) for month {} ... be patient".format(checkmonth))
                            secdata = oldsecdata.copy()
                        else:
                            self.changeStatusbar("Step 4: Reading one second data for month {} ... be patient".format(checkmonth))
                            secdata, fail = readSecData(secondpath,seconddata,checkmonth,year,debug=False)
                            #readnew = True
                        #print (secdata._find_t_limits())

                        if not onlysec:
                            if (checkmonth == rmonth) and mindataback.length()[0] > 0:
                                mindata = mindataback.copy()
                            else:
                                mindata, fail = readMinData(checkchoice,'iaf',iafpath,month,checkmonth)
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
                            reportmsg += "Step 4: !!! One second data (month {}): failed to find scalar (F, dF) data ... Failed\n".format(checkmonth)
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
                                warningmsg += "Step 4: (month {}) large deviation of mean delta F - check baseline corr\n".format(checkmonth)
                                succlst[3] = 3
                            if fstd >= 3.0:
                                reportmsg += "Step 4: !!! large scatter about mean\n"
                                warningmsg += "Step 4: (month {}) dF/G shows large scatter about mean\n".format(checkmonth)
                                succlst[3] = 4
                            if fmean < 0.001 and fstd < 0.001:
                                reportmsg += "Step 4: !!! F seems not to be measured independently \n"
                                warningmsg += "Step 4: (month {}) F seems not to be measured independently\n".format(checkmonth)
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
                            diff = diff.trim(starttime=diff.ndarray[0][0]+0.00069)
                            print ("  -> diff length: {}".format(diff.length()[0]))

                            incon = False
                            if not diff.amplitude('x') < 0.3:
                                warningmsg += "Step 4: (month {}) !!! IAF versus filtered second data: maximum differences in x/h component ({}) significantly exceed numerical uncertainty\n".format(checkmonth,diff.amplitude('x'))
                                incon = True
                            elif not diff.amplitude('x') < 0.12:
                                reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in x/h component ({}) slightly exceed numerical uncertainty\n".format(diff.amplitude('x'))
                            if not diff.amplitude('y') < 0.3:
                                warningmsg += "Step 4: (month {}) !!! IAF versus filtered second data: maximum differences in y/d component ({}) significantly  exceed numerical uncertainty\n".format(checkmonth,diff.amplitude('y'))
                                incon = True
                            elif not diff.amplitude('y') < 0.12:
                                reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in y/d component ({}) slightly exceed numerical uncertainty\n".format(diff.amplitude('y'))
                            if not diff.amplitude('z') < 0.3:
                                warningmsg += "Step 4: (month {}) !!! IAF versus filtered second data: maximum differences in z component ({}) significantly  exceed numerical uncertainty\n".format(checkmonth,diff.amplitude('z'))
                                incon = True
                            elif not diff.amplitude('z') < 0.12:
                                reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in z component ({}) slightly exceed numerical uncertainty\n".format(diff.amplitude('z'))
                            if len(diff._get_column(scal)) > 0:
                                if not diff.amplitude(scal) < 0.30: ## uncertainty is larger because of df conversion (2 times rounding error)
                                    warningmsg += "Step 4: (month {}) IAF versus filtered second data: maximum differences in f component ({}) exceed numerical uncertainty -- PLEASE NOTE: COBS standard procedure is to use mean F for minute and single best F for second\n".format(checkmonth,diff.amplitude('f'))
                                    try:
                                        ttf, ttstd = diff.mean('f',std=True)
                                        warningmsg += "Step 4: (month {}) !!! IAF versus filtered second data: mean f = {} +/- {}\n".format(checkmonth,ttf,ttstd)
                                        incon = True
                                    except:
                                        pass
                            if not incon:
                                reportmsg += "Step 4: +++ IAF/Filtered(Sec): IAF data and filtered second data is consistent\n"
                            else:
                                reportmsg += "Step 4: !!! IAF versus filtered second data: found inconsistencies. Eventually the one minute data record has been cleaned but not the second data set? \n"
                                succlst[3] = 4
                                streamid = self._initial_read(diff)
                                if streamid:  # will create a new input into datadict
                                    self._initial_plot(streamid)
                                #if self.InitialRead(diff):
                                #    self.OnInitialPlot(self.plotstream)

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
                        #locale.setlocale(locale.LC_TIME, old_loc)
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
                    streamid = self._initial_read(blvdata)
                    if streamid:  # will create a new input into datadict
                        self._initial_plot(streamid)
                    #if self.InitialRead(blvdata):
                    #    #self.ActivateControls(self.plotstream)
                    #    self.OnInitialPlot(self.plotstream)
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
                    amplitude = amplitude._copy_column('dx','x')
                    amplitude = amplitude._copy_column('dy','y')
                    amplitude = amplitude._copy_column('dz','z')
                    # PLEASE note: amplitude test is currently, effectively only testing x and z component
                    #              this is still useful as maximum amplitudes are expected in these components
                    if maxamp > 5:
                        reportmsg += "Step 5: !!! amplitude of adopted baseline exceeds INTERMAGNET threshold of 5 nT\n"
                        warningmsg += "Step 5: adopted baseline shows relatively high variations - could be related to baseline jumps - please review data\n"
                        succlst[4] = 3

                    # check baseline complexity


                else:
                    reportmsg += "Step 5: could not open baseline data \n"
                    warningmsg += "Step 5: failed to open baseline data\n"
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
                        #locale.setlocale(locale.LC_TIME, old_loc)
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

                def diffs (success, hmean1,zmean1,hmean2,zmean2,source1='blv',source2='iaf',threshold=0.9):
                    repmsg = ''
                    warnmsg = ''
                    if not np.isnan(hmean1) and not np.isnan(hmean2):
                        diffh = np.abs(hmean1-hmean2)
                        diffz = np.abs(zmean1-zmean2)
                        if diffh < threshold and diffz < threshold:
                            repmsg += "Step 6: yearly means between {} and {} files are consistent within an threshold of {} nT\n".format(source1, source2, threshold)
                        else:
                            repmsg += "Step 6: yearly means differ between {} and {} files. {}: H={:.2f}nT,Z={:.2f}nT; {}: H={:.2f}nT, Z={:.2f}nT \n".format(source1, source2, source1, hmean1,zmean1, source2, hmean2,zmean2)
                            success = 5
                            if source1 == 'yearmean':
                                repmsg += "    ->   difference might be related to data jumps within the Yearmean file, which are considered when reading this file\n"
                                success = 4
                            warnmsg += "Step 6: yearly means differ between {} and {} files\n".format(source1, source2)
                    else:
                        repmsg += "Step 6: did not compare yearly means of {} and {} data \n".format(source1, source2)
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
                else:
                    reportmsg += "Step 6: blv file not available \n"
                    succlst[5] = 6

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
                        rep, warn, succlst[5] = diffs(succlst[5],yearmeanh,yearmeanf,minhmean,minfmean,source1='yearmean',source2='iaf',threshold=0.6)
                        reportmsg += rep
                        warningmsg += warn
                        rep, warn, succlst[5] = diffs(succlst[5],yearmeanh,yearmeanf,blvhmean,blvfmean,source1='yearmean',source2='blv',threshold=1.0)
                        reportmsg += rep
                        warningmsg += warn
                        reportmsg += "Step 6: yearlmean.imo contains data from {} until {} \n".format(num2date(yearmeandata.ndarray[0][0]).year,num2date(yearmeandata.ndarray[0][-1]).year)
                else:
                    reportmsg += "Step 6: yearlmean not available \n"
                    succlst[5] = 6

                if not seconddata == 'None':
                    primeheader = secdata.header
                elif mindata:
                    primeheader = mindata.header
                else:
                    primeheader = {}

                reportmsg += "\nStep 6: meta information \n"
                reportmsg += "-----------------------\n"
                excludelist = ['DataFormat','SensorID','DataComponents','DataSamplingRate','DataPublicationDate','col-f','col-x','col-y','col-z','col-df','unit-col-f','unit-col-x','unit-col-y','unit-col-z','unit-col-df','DataSamplingFilter']
                floatlist = {'DataElevation':0,'DataAcquisitionLongitude':2,'DataAcquisitionLatitude':2}
                secmsg = ''
                minmsg = ''
                yearmsg = ''
                if not primeheader == {}:
                    if not seconddata == 'None' and mindata and yearmeandata:
                        for key in secdata.header:
                            refvalue = str(secdata.header.get(key))
                            compvalue1 = str(mindata.header.get(key,''))
                            compvalue2 = str(yearmeandata.header.get(key,''))
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
                        #locale.setlocale(locale.LC_TIME, old_loc)
                        self.changeStatusbar("Ready")
                        return

            if checkparameter['step7']:
                succlst[6] = 1
                reportmsg += "\n"
                reportmsg += "#######################################\n"
                reportmsg += "Step 7:\n"
                reportmsg += "#######################################\n"
                reportmsg += "Def.: checking K values\n\n"

                posk = DataStream().KEYLIST.index('var1')
                # compare content of dka and iaf
                if dkadata:
                    dkadata = read(dkapath,debug=True)
                    if not dkadata.length()[0] > 0:
                        warningmsg += 'Step 7: Could not read provided dka file !!!\n'
                    else:
                        if dkadata.amplitude('var1') > 9:
                            warningmsg += 'Step 7: k values in DKA file exceed k level 9 !!!\n'
                            succlst[6] = 4
                try:
                    iafk, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth,resolution='k',debug=False)
                    if iafk.amplitude('var1') > 9:
                        warningmsg += 'Step 7: k values in IAF exceed 9 !!!\n'
                        succlst[6] = 5
                    streamid = self._initial_read(iafk)
                    if streamid:  # will create a new input into datadict
                        self._initial_plot(streamid)
                    #if self.InitialRead(iafk):
                    #    self.OnInitialPlot(self.plotstream)
                except:
                    warningmsg += "Step 7: Could not extract k values from IAF file\n"
                    dayprob = True
                    succlst[6] = 5

                if dkadata and succlst[6] < 5:
                    kdiffs = subtractStreams(iafk, dkadata)
                    if kdiffs.length()[0] > 0:
                        posk = DataStream().KEYLIST.index('var1')
                        for el in kdiffs.ndarray[posk]:
                            if el >= 0.1:
                                warningmsg += 'Step 7: difference between k in IAF and DKA files at {}: IAF: {}, DKA: {}\n'.format(num2date(kdiffs.ndarray[0][idx]).replace(tzinfo=None), iafk.ndarray[posk][idx], dkadata.ndarray[posk][idx])
                                succlst[6] = 3
                    else:
                        warningmsg += 'Step 7: (optional) k value check with DKA not performed.\n'
                        succlst[6] = 2
                else:
                    #warningmsg += 'Step 7: k value check not performed\n'
                    #succlst[6] = 2
                    pass

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
                        #locale.setlocale(locale.LC_TIME, old_loc)
                        self.changeStatusbar("Ready")
                        return


        #locale.setlocale(locale.LC_TIME, old_loc)
        self.changeStatusbar("Check data finished - Ready")



    @deprecated("Apperently not used any more")
    def onWebServiceParameter(self,event):
        """
        DESCRIPTION
            open dialog to modify webservices
        """
        saveoptions = False
        ok = False
        ws = self.analysisdict.get('webservices',{})

        dlg = ParameterDictDialog(None, title="Review Webserive analysis parameter", dictionary=ws, preselect=['conrad'])
        if dlg.ShowModal() == wx.ID_OK:
            ok = True

        dlg.Destroy()

    @deprecated("To be removed - _update_plot")
    def UpdatePlotCharacteristics(self, stream):
        """
        DESCRIPTION
            Checks and activates plot options, checks for correct lengths of all list options

            very similar to _initial_plot
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
        keylist = [elem for elem in keylist if elem in DataStream().NUMKEYLIST]

        # The following will be overwritten by ActivateControls
        self.symbols = ['-'] * len(keylist)
        self.plotopt['symbols'] =  [['-'] * len(keylist)]
        self.plotopt['colors'] = [self.colors[:len(keylist)]]
        self.plotopt['title'] = stream.header.get('StationID')

        try:
            tmin,tmax = stream._find_t_limits()
            diff = (tmax.date()-tmin.date()).days
            if diff < 5 and not diff == 0:
                self.plotopt['title'] = "{}: {} to {}".format(stream.header.get('StationID'),tmin.date(),tmax.date())
            elif diff == 0:
                self.plotopt['title'] = "{}: {}".format(stream.header.get('StationID'),tmin.date())
        except:
            pass

        self.menu_p.str_page.symbolRadioBox.SetStringSelection('line')
        self.menu_p.ana_page.dailyMeansButton.Disable()

        # 2. If stream too long then don't allow scatter plots -- too slowly
        if stream.length()[0] < 2000:
            self.menu_p.str_page.symbolRadioBox.Enable()
        else:
            self.menu_p.str_page.symbolRadioBox.Disable()

        # 3. If DataFormat = MagPyDI then preselect scatter, and idf and basevalues
        if stream.header.get('DataFormat') == 'MagPyDI' or stream.header.get('DataType','').startswith('MagPyDI'):
            self.menu_p.str_page.symbolRadioBox.Enable()
            self.menu_p.str_page.symbolRadioBox.SetStringSelection('point')
            self.shownkeylist = keylist
            if len(stream.ndarray[DataStream().KEYLIST.index('x')]) > 0:
                keylist = ['x','y','z','dx','dy','dz']
                self.plotopt['padding'] = [[0,0,0,5,0.05,5]]
                #keylist = ['x','y','z','dx','dy','dz','df']
                #self.plotopt['padding'] = [[0,0,0,5,0.05,5,1]]
            else:
                keylist = ['dx','dy','dz']
                self.plotopt['padding'] = [[5,0.05,5]]
            self.symbols = ['o'] * len(keylist)
            self.plotopt['symbols'] =  [['o'] * len(keylist)]
            self.plotopt['colors'] = [self.colorlist[:len(keylist)]]
            # enable daily average button
            self.menu_p.ana_page.dailyMeansButton.Enable()

        # 4. If K values are shown: preselect bar chart
        if stream.header.get('DataFormat') == 'MagPyK' or stream.header.get('DataType','').startswith('MagPyK') or ('var1' in keylist and stream.header.get('col-var1','').startswith('K')):
            #print ("Found K values - apply self.plotopt")
            self.plotopt['specialdict']=[{'var1':[0,9]}]
            pos = keylist.index('var1')
            self.plotopt['symbols'][pos] = 'z'
            self.plotopt['bartrange'] = 0.06
            self.plotopt['opacity'] = 1.0

        self.shownkeylist = keylist

        pads = self.plotopt.get('padding',None)
        if not pads or not len(pads[0]) == len(keylist):
            self.plotopt['padding']= [[0] * len(keylist)]

        return keylist

    @deprecated("Aux file not used any more")
    def askUserForFilename(self, **dialogOptions):
        dialog = wx.FileDialog(self, **dialogOptions)
        if dialog.ShowModal() == wx.ID_OK:
            user_provided_filename = True
            self.filename = dialog.GetFilename()
            self.guidict['dirname'] = dialog.GetDirectory()
        else:
            user_provided_filename = False
        dialog.Destroy()
        return user_provided_filename


    @deprecated("To be replaced")
    def OnInitialPlot(self, stream, restore = False):
        """
        DEFINITION:
            read stream, extract columns with values and display up to three of them by default
            executes guiPlot then
        """
        self.changeStatusbar("Plotting...")

        self._set_plot_parameter()
        # Init Controls
        self.ActivateControls(self.plotstream)

        if self.plotstream.header.get('DataFunctionObject',False):
            #print ("HERE - found functions:", self.plotstream.header.get('DataFunctionObject') )
            self.plotopt['function'] = self.plotstream.header.get('DataFunctionObject')

        # Override initial controls: Set setting (like keylist, basic plot options and basevalue selection)
        keylist = self.UpdatePlotCharacteristics(self.plotstream)

        self.menu_p.rep_page.logMsg('- keys: %s' % (', '.join(keylist)))
        #if len(stream) > self.resolution:
        #    self.menu_p.rep_page.logMsg('- warning: resolution of plot reduced by a factor of %i' % (int(len(stream)/self.resolution)))
        # Eventually change symbol as matplotlib reports errors for line plot with many points
        if stream.length()[0] > 200000:
            self.plotopt['symbols']= ['.'] * len(keylist)

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
        # Connect callback to the initial plot
        for idx, ax in enumerate(self.plot_p.axlist):
            ax.callbacks.connect('xlim_changed', self._update_statistics)
            ax.callbacks.connect('ylim_changed', self._update_statistics)
        self._update_statistics()
        self.changeStatusbar("Ready")


    @deprecated("Replaced by _do_plot")
    def OnPlot(self, stream, keylist, **kwargs):
        """
        DEFINITION:
            read stream and display
        """
        #self.plotopt = {'bgcolor':'green'}

        self.changeStatusbar("Plotting...")
        if stream.length()[0] > 200000:
            self.plotopt['symbollist']= ['.'] * len(keylist)

        # Update Delta F if plotted  -- this should move to BC corr
        # or any other method it is necessary
        #if 'df' in keylist:
        #    stream = stream.delta_f()

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
        # Connect callback to the new plot
        for idx, ax in enumerate(self.plot_p.axlist):
            ax.callbacks.connect('xlim_changed', self._update_statistics)
            ax.callbacks.connect('ylim_changed', self._update_statistics)
        self._update_statistics()
        self.changeStatusbar("Ready")


    @deprecated("To be replaced")
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


    @deprecated("replaced by db_on_connect")
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
        dlg.Destroy()




    """
    TODO UNUSED?
    def on_save(self, event):
        cdir = self.magpystate.get('currentpath')
        cfile = self.magpystate.get('filename')
        textfile = open(os.path.join(cdir, cfile), 'w')
        textfile.write(self.control.GetValue())
        textfile.close()

    def on_save_as(self, event):
        if self.askUserForFilename(defaultFile=self.filename, style=wx.SAVE,
                                   **self.default_file_dialog_options()):
            self.on_save(event)
    """

    @deprecated("Not used any more")
    def onOpenAuxButton(self, event):
        if self.askUserForFilename(style=wx.OPEN,
                                   **self.default_file_dialog_options()):
            textfile = open(os.path.join(self.last_dir, self.filename), 'r')
            self.menu_p.gen_page.AuxDataTextCtrl.SetValue(textfile.read())
            textfile.close()




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

if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: MAGPY_GUI PACKAGE")
    print("All gui methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print()
    print("----------------------------------------------------------")
    print()

    errors = {}
    try:
        # This will not work
        guid, anald = MainFrame()._set_default_initialization()
    except Exception as excep:
        errors['_set_default_initialization'] = str(excep)
        print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR testing number.")

    print()
    print("----------------------------------------------------------")
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(str(errors.keys()))
        print()
        print("Exceptions thrown:")
        for item in errors:
            print("{} : errormessage = {}".format(item, errors.get(item)))
