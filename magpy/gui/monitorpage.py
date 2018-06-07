#!/usr/bin/env python

from __future__ import print_function
try:
    from magpy.stream import *
    from magpy.absolutes import *
    from magpy.transfer import *
    from magpy.database import *
except:
    from magpy.stream import *
    from magpy.absolutes import *
    from magpy.transfer import *
    from magpy.database import *

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

import multiprocessing



class MonitorPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # all buttons open dlg to add parameters (e.g. IP, 
        self.getMARTASButton = wx.Button(self,-1,"Connect to MARTAS", size=(160,30))
        self.getMARCOSButton = wx.Button(self,-1,"Connect to MARCOS", size=(160,30))
        #self.getMQTTButton = wx.Button(self,-1,"Connect to MQTT", size=(160,30))
        self.martasLabel = wx.TextCtrl(self, value="not connected", size=(160,30), style=wx.TE_RICH)  # red bg
        self.marcosLabel = wx.TextCtrl(self, value="not connected", size=(160,30), style=wx.TE_RICH)  # red bg
        #self.mqttLabel = wx.TextCtrl(self, value="not connected", size=(160,30), style=wx.TE_RICH)  # red bg
        self.marcosLabel.SetEditable(False)
        self.martasLabel.SetEditable(False)
        #self.mqttLabel.SetEditable(False)
        # Parameters if connection is established
        # 
        self.coverageLabel = wx.StaticText(self, label="Plot coverage (sec):", size=(160,30))
        self.coverageTextCtrl = wx.TextCtrl(self, value="600", size=(160,30))

        self.sliderLabel = wx.StaticText(self, label="Update period (sec):", size=(160,30))
        self.frequSlider = wx.Slider(self, -1, 10, 1, 60, (-1, -1), (100, -1),
                wx.SL_AUTOTICKS | wx.SL_HORIZONTAL | wx.SL_LABELS)

        self.startMonitorButton = wx.Button(self,-1,"Start Monitor", size=(160,30))  # if started then everything else will be disabled ..... except save monitor
        self.stopMonitorButton = wx.Button(self,-1,"Stop Monitor", size=(160,30))

        self.saveMonitorButton = wx.Button(self,-1,"Log data*", size=(160,30))  # produces a bin file
        #self.startMonitorButton.Disable()
        self.saveMonitorButton.Disable()
        # Connection Log
        # 
        self.connectionLogLabel = wx.StaticText(self, label="Connection Log:")
        self.connectionLogTextCtrl = wx.TextCtrl(self, wx.ID_ANY, size=(330,300),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        


    def doLayout(self):
        mainSizer = wx.BoxSizer(wx.VERTICAL)
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=20, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.getMARTASButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.martasLabel, noOptions),
                 (self.getMARCOSButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.marcosLabel, noOptions),
                 #(self.getMQTTButton, dict(flag=wx.ALIGN_CENTER)),
                 #(self.mqttLabel, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.coverageLabel, noOptions),
                 (self.coverageTextCtrl, expandOption),
                 (self.sliderLabel, noOptions),
                 (self.frequSlider, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.startMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.stopMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.saveMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        #self.SetSizerAndFit(boxSizer)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        mainSizer.Add(self.connectionLogLabel, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.connectionLogTextCtrl, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        self.SetSizerAndFit(mainSizer)

    def logMsg(self, message):
        ''' Private method to append a string to the logger text
            control. '''
        #print message
        self.connectionLogTextCtrl.AppendText('%s\n'%message)

    def collector(self):
        """
        A copy of the collector moon function
        To be called using multiprocessing
        This way the buffer should be accessible for displaying data
        """
        # To be defined
        clientname = 'europa'
        clientip = '192.168.178.21'
        martaspath = '/home/cobs/MARTAS'

        # To be defined in options
        destpath = "/tmp"
        homedir = '/home/leon/CronScripts/MagPyAnalysis/Subscribing2MARTAS'
        defaultuser = 'cobs'
        stationid = 'MyHome'
        dbname = 'mydb'

        # Select destination (options: 'file' or 'db') - Files are saved in .../MARCOS/MartasFiles/
        dest = 'file'
        # For Testing purposes - Print received data to screen:
        printdata = True
        # Please make sure that the db and scp connection data is stored
        # within the credential file -otherwise provide this data directly
        martasname = 'europa'

        dbhost = mpcred.lc(dbname,'host')
        dbuser = mpcred.lc(dbname,'user')
        dbpasswd = mpcred.lc(dbname,'passwd')
        dbname = mpcred.lc(dbname,'db')
        scpuser = mpcred.lc(martasname,'user')
        scppasswd = mpcred.lc(martasname,'passwd')

        logdir = os.path.join(homedir,'MARCOS','Logs')
        logfile = os.path.join(logdir,'marcos.log')
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        log.startLogging(open(logfile,'a'))
        #log.startLogging(sys.stdout)

        sshcredlst = [scpuser,scppasswd]
        # ----------------------------------------------------------
        # 2. connect to database and check availability and version
        # ----------------------------------------------------------
        # The following should only be required in case of db
        if dest == 'db':
            try:
                db = mysql.connect (host=dbhost,user=dbuser,passwd=dbpasswd,db=dbname)
                dbcredlst = [dbhost,dbuser,dbpasswd,dbname]
            except:
                print("Create a credential file first or provide login info for database directly")
                raise
            cursor = db.cursor ()
            cursor.execute ("SELECT VERSION()")
            row = cursor.fetchone ()
            print("MySQL server version:", row[0])
            cursor.close ()
            db.close ()
        else:
            dbcredlst = []

        # Please note: scp is not workings from root
        # Therefore the following processes are performed as defaultuser (ideally as a subprocess)
        uid=pwd.getpwnam(defaultuser)[2]
        os.setuid(uid)

        sensfile = os.path.join(martaspath,'sensors.txt')
        owfile = os.path.join(martaspath,'owlist.csv')
        if not os.path.exists(os.path.join(destpath,'MartasSensors')):
            os.makedirs(os.path.join(destpath,'MartasSensors'))
        destsensfile = os.path.join(destpath,'MartasSensors',clientname+'_sensors.txt')
        destowfile = os.path.join(destpath,'MartasSensors',clientname+'_owlist.csv')
        print("Getting sensor information from ", clientname)

        try:
            scptransfer(scpuser+'@'+clientip+':'+sensfile,destsensfile,scppasswd)
        except:
            print("Could not connect to/get sensor info of client %s - aborting" % clientname)
            print("Please make sure that you connected at least once to the client by ssh")
            print(" from your defaultuser %s " % defaultuser)
            print(" This way the essential key data is established.")
            sys.exit()
        print("Searching for onewire data from ", clientname)
        try:
            scptransfer(scpuser+'@'+clientip+':'+owfile,destowfile,scppasswd)
        except:
            print("No one wire info available on client %s - proceeding" % clientname)
            pass

        s,o = [],[]
        if os.path.exists(destsensfile):
            with open(destsensfile,'rb') as f:
                reader = csv.reader(f)
                s = []
                for line in reader:
                    print(line)
                    if len(line) < 2:
                        try:
                            s.append(line[0].split())
                        except:
                            # Empty line for example
                            pass
                    else:
                        s.append(line)
            print(s)
        else:
            print("Apparently no sensors defined on client %s - aborting" % clientname)
            sys.exit()

        if os.path.exists(destowfile):
            with open(destowfile,'rb') as f:
                reader = csv.reader(f)
                o = [line for line in reader]
            print(o)

        factory = WampClientFactory("ws://"+clientip+":9100", debugWamp = False)
        cl.sendparameter(clientname,clientip,destpath,dest,stationid,sshcredlst,s,o,printdata,dbcredlst)
        factory.protocol = cl.PubSubClient
        connectWS(factory)

        reactor.run()


    def run(self):
        """
        Now start the collection process
        """
        # Start collection as process one
        p1 = multiprocessing.Process(target=self.collector)
        p1.start()
        p1.join()
        # Start visualization as process two
        # google search, how to access an gradually filling array
