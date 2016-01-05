#!/usr/bin/env python

try:
    from stream import *
    from absolutes import *
    from transfer import *
    from database import *
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



class PortCommunicationPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.selectPortButton = wx.Button(self,-1,"Select MARTAS")
        self.portnameTextCtrl = wx.TextCtrl(self, value="coming soon")
        self.portnameTextCtrl.Disable()
        self.sliderLabel = wx.StaticText(self, label="Update frequency:")
        self.frequSlider = wx.Slider(self, -1, 10, 1, 20, (-1, -1), (100, -1),
                wx.SL_AUTOTICKS | wx.SL_HORIZONTAL | wx.SL_LABELS)
        self.startMonitorButton = wx.Button(self,-1,"Start Monitor")
        self.stopMonitorButton = wx.Button(self,-1,"Stop Monitor")
        self.startMonitorButton.Disable()
        self.stopMonitorButton.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=7, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.selectPortButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.portnameTextCtrl, expandOption),
                 (self.sliderLabel, noOptions),
                 (self.frequSlider, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.startMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.stopMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

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
                db = MySQLdb.connect (host=dbhost,user=dbuser,passwd=dbpasswd,db=dbname)
                dbcredlst = [dbhost,dbuser,dbpasswd,dbname]
            except:
                print "Create a credential file first or provide login info for database directly"
                raise
            cursor = db.cursor ()
            cursor.execute ("SELECT VERSION()")
            row = cursor.fetchone ()
            print "MySQL server version:", row[0]
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
        print "Getting sensor information from ", clientname

        try:
            scptransfer(scpuser+'@'+clientip+':'+sensfile,destsensfile,scppasswd)
        except:
            print "Could not connect to/get sensor info of client %s - aborting" % clientname
            print "Please make sure that you connected at least once to the client by ssh"
            print " from your defaultuser %s " % defaultuser
            print " This way the essential key data is established."
            sys.exit()
        print "Searching for onewire data from ", clientname
        try:
            scptransfer(scpuser+'@'+clientip+':'+owfile,destowfile,scppasswd)
        except:
            print "No one wire info available on client %s - proceeding" % clientname
            pass

        s,o = [],[]
        if os.path.exists(destsensfile):
            with open(destsensfile,'rb') as f:
                reader = csv.reader(f)
                s = []
                for line in reader:
                    print line
                    if len(line) < 2:
                        try:
                            s.append(line[0].split())
                        except:
                            # Empty line for example
                            pass
                    else:
                        s.append(line)
            print s
        else:
            print "Apparently no sensors defined on client %s - aborting" % clientname
            sys.exit()

        if os.path.exists(destowfile):
            with open(destowfile,'rb') as f:
                reader = csv.reader(f)
                o = [line for line in reader]
            print o

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
