#!/usr/bin/env python

from magpy.stream import *
from magpy.absolutes import *
from magpy.transfer import *
from magpy.database import *

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure
import wx.lib.scrolledpanel as scrolledpanel
from io import open



# Subclasses for Dialogs called by magpy gui

class OpenWebAddressDialog(wx.Dialog):
    """
    Dialog for File Menu - Load URL
    """

    def __init__(self, parent, title, favorites):
        super(OpenWebAddressDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.favorites = favorites
        if self.favorites == None or len(self.favorites) == 0:
            self.favorites = ['http://www.intermagnet.org/test/ws/?id=BOU']
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # single anaylsis
        #ftp://user:passwd@www.zamg.ac.at//data/magnetism/wic/variation/WIC20160627pmin.min
        # db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.urlLabel = wx.StaticText(self, label="Insert address (e.g. 'ftp://.../' for all files, or 'ftp://.../data.dat' for a single file)",size=(500,30))
        self.urlTextCtrl = wx.TextCtrl(self, value=self.favorites[0],size=(500,30))
        self.favoritesLabel = wx.StaticText(self, label="Favorites:",size=(160,30))
        self.getFavsComboBox = wx.ComboBox(self, choices=self.favorites,
            style=wx.CB_DROPDOWN, value=self.favorites[0],size=(160,-1))
        self.addFavsButton = wx.Button(self, label='Add to favorites',size=(160,30))
        self.dropFavsButton = wx.Button(self, label='Remove from favorites',size=(160,30))

        self.okButton = wx.Button(self, wx.ID_OK, label='Connect')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=5, cols=3, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.urlLabel, noOptions),
                  emptySpace,
                 (self.favoritesLabel, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.getFavsComboBox, expandOption),
                 (self.urlTextCtrl, expandOption),
                  emptySpace,
                 (self.addFavsButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.dropFavsButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.addFavsButton.Bind(wx.EVT_BUTTON, self.AddFavs)
        self.dropFavsButton.Bind(wx.EVT_BUTTON, self.DropFavs)
        self.getFavsComboBox.Bind(wx.EVT_COMBOBOX, self.GetFavs)

    def GetFavs(self,e):
        """
        http://www.intermagnet.org/test/ws/?id=BOU
        """
        url = self.getFavsComboBox.GetValue()
        self.urlTextCtrl.SetValue(url)


    def AddFavs(self, e):
        url = self.urlTextCtrl.GetValue()
        if not url in self.favorites:
            self.favorites.append(url)
            self.getFavsComboBox.Append(str(url))

    def DropFavs(self, e):
        url = self.urlTextCtrl.GetValue()
        self.favorites = [elem for elem in self.favorites if not elem == url]
        self.getFavsComboBox.Clear()
        for elem in self.favorites:
            self.getFavsComboBox.Append(elem)


class LoadDataDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, mintime, maxtime, extension):
        super(LoadDataDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.mintime = mintime
        self.maxtime = maxtime
        self.extension = extension
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.startdateLabel = wx.StaticText(self, label="Start date:")
        self.startDatePicker = wx.DatePickerCtrl(self, dt=self.mintime)
        # the following line produces error in my win xp installation
        self.startTimePicker = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.enddateLabel = wx.StaticText(self, label="End date:")
        self.endDatePicker = wx.DatePickerCtrl(self, dt=self.maxtime)
        self.endTimePicker = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        if self.extension == 'MySQL Database':
            self.extLabel = wx.StaticText(self, label="DB:")
        else:
            self.extLabel = wx.StaticText(self, label="Files (*.min,*,WIC*):")
        self.fileExt = wx.TextCtrl(self, value=self.extension,size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Load',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        elemlist = ['self.startdateLabel, noOptions',
                  '(0,0), noOptions',
                 'self.startDatePicker, expandOption',
                 'self.startTimePicker, expandOption',
                 'self.enddateLabel, noOptions',
                 '(0,0), noOptions',
                 'self.endDatePicker, expandOption',
                 'self.endTimePicker, expandOption',
                 'self.extLabel, noOptions',
                 'self.fileExt, expandOption',
                 '(0,0), noOptions',
                 '(0,0), noOptions',
                 'self.okButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.closeButton, dict(flag=wx.ALIGN_CENTER)']

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for elem in elemlist:
            control = elem.split(', ')[0]
            options = elem.split(', ')[1]
            gridSizer.Add(eval(control), **eval(options))

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class ExportDataDialog(wx.Dialog):
    """
    Dialog for Exporting data
    """
    def __init__(self, parent, title, path, stream, defaultformat):
        super(ExportDataDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.WriteFormats = [ key for key in PYMAG_SUPPORTED_FORMATS if 'w' in PYMAG_SUPPORTED_FORMATS[key][0]]
        if not defaultformat or not defaultformat in self.WriteFormats:
            defaultformat = 'PYCDF'
        self.default = self.WriteFormats.index(defaultformat)
        # use stream info and defaults file export
        self.filenamebegins = None
        self.filenameends = None
        self.dateformat = None
        self.coverage = None
        self.mode = 'overwrite'
        self.stream = stream
        self.filename = self.GetFilename(stream, defaultformat, self.filenamebegins, self.filenameends,self.coverage,self.dateformat)
        self.path = path
        self.createControls()
        self.doLayout()
        self.bindControls()


    # Widgets
    def createControls(self):
        # single anaylsis
        # db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.selectDirButton = wx.Button(self, label='Change Directory', size=(160,30))
        self.selectedTextCtrl = wx.TextCtrl(self, value=self.path, size=(300,30))
        self.formatLabel = wx.StaticText(self, label="as ...")
        self.formatComboBox = wx.ComboBox(self, choices=self.WriteFormats,
            style=wx.CB_DROPDOWN, value=self.WriteFormats[self.default],size=(160,-1))
        self.selectLabel = wx.StaticText(self, label="Export data to ...")
        self.nameLabel = wx.StaticText(self, label="File name(s) looks like ...")
        self.filenameTextCtrl = wx.TextCtrl(self, value=self.filename, size=(300,30))
        self.modifyButton = wx.Button(self, label='Modify name(s)', size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Write', size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel', size=(160,30))

        self.filenameTextCtrl.Disable()
        self.selectedTextCtrl.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.selectLabel, noOptions),
                  emptySpace,
                 (self.selectedTextCtrl, expandOption),
                 (self.selectDirButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.formatLabel, noOptions),
                 (self.formatComboBox, expandOption),
                 (self.nameLabel, noOptions),
                  emptySpace,
                 (self.filenameTextCtrl, expandOption),
                 (self.modifyButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.selectDirButton.Bind(wx.EVT_BUTTON, self.OnSelectDirButton)
        self.modifyButton.Bind(wx.EVT_BUTTON, self.OnModifyButton)
        self.formatComboBox.Bind(wx.EVT_COMBOBOX, self.OnFormatChange)


    def GetFilename(self, stream, format_type, filenamebegins=None, filenameends=None, coverage=None, dateformat=None, blvyear=None):
        """
        DESCRIPTION:
            Helper method to determine filename from selections
        """
        #print ("Calling GetFilename: if file is MagPyDI - eventually open a message box to define year", filenamebegins, filenameends, coverage, dateformat)
        format_type, self.filenamebegins, self.filenameends, self.coverage, self.dateformat = stream._write_format(format_type, filenamebegins, filenameends, coverage, dateformat , blvyear)
        #print ("obtained:", self.filenamebegins, self.filenameends, self.coverage, self.dateformat)
        if coverage == 'all':
            datelook = ''
        else:
            datelook = datetime.strftime(stream._find_t_limits()[0],self.dateformat)
        if format_type.endswith('PYCDF'):
            self.filenameends = '.cdf'
        filename = self.filenamebegins+datelook+self.filenameends
        return filename


    def OnSelectDirButton(self, event):
        dialog = wx.DirDialog(None, "Choose a directory:",'/srv',style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            #self.ReactivateStreamPage()
            self.selectedTextCtrl.SetValue(dialog.GetPath())
        #self.menu_p.rep_page.logMsg('- Directory for file export defined')
        #dialog.Destroy()

    def OnModifyButton(self, event):
        # open a dialog to select filename specifications
        helpdlg = ExportModifyNameDialog(None, title='File name specifications',filenamebegins=self.filenamebegins, filenameends=self.filenameends,coverage=self.coverage,dateformat=self.dateformat,mode=self.mode, year='unspecified')
        blvyear = None
        if helpdlg.ShowModal() == wx.ID_OK:
            self.filenamebegins = helpdlg.beginTextCtrl.GetValue()
            self.filenameends = helpdlg.endTextCtrl.GetValue()
            self.dateformat = helpdlg.dateTextCtrl.GetValue()
            self.coverage = helpdlg.coverageComboBox.GetValue()
            self.mode = helpdlg.modeComboBox.GetValue()
            year = helpdlg.yearTextCtrl.GetValue()
            if not year == 'unspecified':
                try:
                    blvyear = int(year)
                except:
                    blvyear = None
            else:
                blvyear = None
        selformat = self.formatComboBox.GetValue()
        self.filename = self.GetFilename(self.stream, selformat, self.filenamebegins, self.filenameends,self.coverage,self.dateformat, blvyear = blvyear)
        self.filenameTextCtrl.SetValue(self.filename)


    def OnFormatChange(self, event):
        # call stream._write_format to determine self.filename
        selformat = self.formatComboBox.GetValue()
        self.filenamebegins = None
        self.filenameends = None
        self.coverage = None
        self.dateformat = None
        self.filename = self.GetFilename(self.stream, selformat, self.filenamebegins, self.filenameends,self.coverage,self.dateformat)
        self.filenameTextCtrl.SetValue(self.filename)


class ExportModifyNameDialog(wx.Dialog):
    """
    Helper Dialog for Exporting data
    """
    def __init__(self, parent, title, filenamebegins, filenameends, coverage, dateformat,mode, year):
        super(ExportModifyNameDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.filenamebegins = filenamebegins
        self.filenameends = filenameends
        self.dateformat = dateformat
        self.coverage = coverage
        self.mode = mode
        self.year = year
        self.createControls()
        self.doLayout()


    # Widgets
    def createControls(self):

        self.beginLabel = wx.StaticText(self, label="Name(s) start with ...", size=(160,30))
        self.endLabel = wx.StaticText(self, label="Name(s) end with ...", size=(160,30))
        self.beginTextCtrl = wx.TextCtrl(self, value=self.filenamebegins, size=(160,30))
        self.endTextCtrl = wx.TextCtrl(self, value=self.filenameends, size=(160,30))
        self.dateformatLabel = wx.StaticText(self, label="Date looks like ...")
        self.dateTextCtrl = wx.TextCtrl(self, value=self.dateformat, size=(160,30))
        self.coverageLabel = wx.StaticText(self, label="File covers ...")
        self.coverageComboBox = wx.ComboBox(self, choices=['hour','day','month','year','all'],
            style=wx.CB_DROPDOWN, value=self.coverage,size=(160,-1))
        self.modeLabel = wx.StaticText(self, label="Write mode ...")
        self.modeComboBox = wx.ComboBox(self, choices=['replace','append', 'overwrite', 'skip'],
            style=wx.CB_DROPDOWN, value=self.mode,size=(160,-1))
        self.yearLabel = wx.StaticText(self, label="Year (BLV export):", size=(160,30))
        self.yearTextCtrl = wx.TextCtrl(self, value=self.year, size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply', size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel', size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.beginLabel, noOptions),
                 (self.endLabel, noOptions),
                 (self.beginTextCtrl, expandOption),
                 (self.endTextCtrl, expandOption),
                 (self.dateformatLabel, noOptions),
                 (self.coverageLabel, noOptions),
                 (self.dateTextCtrl, expandOption),
                 (self.coverageComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.modeLabel, noOptions),
                 (self.modeComboBox, expandOption),
                 (self.yearLabel, noOptions),
                 (self.yearTextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

class DatabaseConnectDialog(wx.Dialog):
    """
    Dialog for Database Menu - Connect MySQL
    """

    def __init__(self, parent, title):
        super(DatabaseConnectDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # single anaylsis
        # db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.hostLabel = wx.StaticText(self, label="Host")
        self.hostTextCtrl = wx.TextCtrl(self, value="localhost")
        self.userLabel = wx.StaticText(self, label="User")
        self.userTextCtrl = wx.TextCtrl(self, value="Max")
        self.passwdLabel = wx.StaticText(self, label="Password")
        self.passwdTextCtrl = wx.TextCtrl(self, value="Secret",style=wx.TE_PASSWORD)
        self.dbLabel = wx.StaticText(self, label="Database")
        self.dbTextCtrl = wx.TextCtrl(self, value="MyDB")
        self.okButton = wx.Button(self, wx.ID_OK, label='Connect')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        #gridSizer = wx.FlexGridSizer(rows=3, cols=4, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.hostLabel, noOptions),
                 (self.userLabel, noOptions),
                 (self.passwdLabel, noOptions),
                 (self.dbLabel, noOptions),
                 (self.hostTextCtrl, expandOption),
                 (self.userTextCtrl, expandOption),
                 (self.passwdTextCtrl, expandOption),
                 (self.dbTextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class DatabaseContentDialog(wx.Dialog):
    """
    Dialog for Database Menu - Connect MySQL
    """

    def __init__(self, parent, title, datalst):
        super(DatabaseContentDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        #self.datalst = ['test','jgg']
        self.datalst = datalst
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:",size=(160,30))
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.datalst[0],size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        #gridSizer = wx.FlexGridSizer(rows=7, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist=[(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class OptionsInitDialog(wx.Dialog):
    """
    Dialog for Database Menu - Connect MySQL
    """

    def __init__(self, parent, title, options):
        super(OptionsInitDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.options = options
        self.funclist = ['spline','polynomial']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # single anaylsis
        # db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.dboptLabel = wx.StaticText(self, label="Database:",size=(160,30))
        self.basicLabel = wx.StaticText(self, label="Basics:",size=(160,30))
        self.calcLabel = wx.StaticText(self, label="Calculation:",size=(160,30))
        self.otherLabel = wx.StaticText(self, label="Other:",size=(160,30))
        self.hostLabel = wx.StaticText(self, label="Host",size=(160,30))
        self.hostTextCtrl = wx.TextCtrl(self,value=self.options.get('host','localhost'),size=(160,30))
        self.userLabel = wx.StaticText(self, label="User",size=(160,30))
        self.userTextCtrl = wx.TextCtrl(self, value=self.options.get('user','Max'),size=(160,30))
        self.passwdLabel = wx.StaticText(self, label="Password",size=(160,30))
        self.passwdTextCtrl = wx.TextCtrl(self, value=self.options.get('passwd','Secret'),style=wx.TE_PASSWORD,size=(160,30))
        self.dbLabel = wx.StaticText(self, label="Database",size=(160,30))
        self.dbTextCtrl = wx.TextCtrl(self, value=self.options.get('dbname','MyDB'),size=(160,30))
        self.dirnameLabel = wx.StaticText(self, label="Default directory",size=(160,30))
        self.dirnameTextCtrl = wx.TextCtrl(self, value=self.options.get('dirname',''),size=(160,30))

        self.stationidLabel = wx.StaticText(self, label="Station ID",size=(160,30))
        self.stationidTextCtrl = wx.TextCtrl(self, value=self.options.get('stationid','WIC'),size=(160,30))

        self.experimentalLabel = wx.StaticText(self, label="Experimental methods",size=(160,30))
        self.experimentalCheckBox = wx.CheckBox(self, label="Activate", size=(160,30))
        self.martasscantimeLabel = wx.StaticText(self, label="Scanning MARTAS [sec]",size=(160,30))
        self.martasscantimeTextCtrl = wx.TextCtrl(self, value=self.options.get('martasscantime','20'),size=(160,30))

        self.fitfunctionLabel = wx.StaticText(self, label="Fit function",size=(160,30))
        self.fitfunctionComboBox = wx.ComboBox(self, choices=self.funclist,
                              style=wx.CB_DROPDOWN, value=self.options.get('fitfunction','spline'),size=(160,-1))
        self.fitknotstepLabel = wx.StaticText(self, label="Knotstep (spline)",size=(160,30))
        self.fitknotstepTextCtrl = wx.TextCtrl(self, value=self.options.get('fitknotstep','0.3'),size=(160,30))
        self.fitdegreeLabel = wx.StaticText(self, label="Degree (polynom)",size=(160,30))
        self.fitdegreeTextCtrl = wx.TextCtrl(self, value=self.options.get('fitdegree','5'),size=(160,30))
        self.bookmarksLabel = wx.StaticText(self, label="Favorite URLs",size=(160,30))
        bm = self.options.get('bookmarks',['http://www.intermagnet.org/test/ws/?id=BOU'])
        self.bookmarksComboBox = wx.ComboBox(self, choices=bm,style=wx.CB_DROPDOWN, value=bm[0],size=(160,-1))

        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))
        self.saveButton = wx.Button(self, wx.ID_OK, label='Save',size=(160,30))

        #self.bookmarksComboBox.Disable()

        self.experimentalCheckBox.SetValue(self.options.get('experimental',False))
        f = self.dboptLabel.GetFont()
        newf = wx.Font(14, wx.DECORATIVE, wx.ITALIC, wx.BOLD)
        self.dboptLabel.SetFont(newf)
        self.basicLabel.SetFont(newf)
        self.calcLabel.SetFont(newf)
        self.otherLabel.SetFont(newf)

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.dboptLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.hostLabel, noOptions),
                 (self.userLabel, noOptions),
                 (self.passwdLabel, noOptions),
                 (self.dbLabel, noOptions),
                 (self.hostTextCtrl, expandOption),
                 (self.userTextCtrl, expandOption),
                 (self.passwdTextCtrl, expandOption),
                 (self.dbTextCtrl, expandOption),
                 (self.basicLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.dirnameLabel, noOptions),
                 (self.stationidLabel, noOptions),
                  emptySpace,
                 (self.bookmarksLabel, noOptions),
                 (self.dirnameTextCtrl, expandOption),
                 (self.stationidTextCtrl, expandOption),
                  emptySpace,
                 (self.bookmarksComboBox, noOptions),
                 (self.calcLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.fitfunctionLabel, noOptions),
                 (self.fitknotstepLabel, noOptions),
                 (self.fitdegreeLabel, noOptions),
                  emptySpace,
                 (self.fitfunctionComboBox, noOptions),
                 (self.fitknotstepTextCtrl, expandOption),
                 (self.fitdegreeTextCtrl, expandOption),
                 (self.otherLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.martasscantimeLabel, noOptions),
                 (self.experimentalLabel, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.martasscantimeTextCtrl, noOptions),
                 (self.experimentalCheckBox, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.saveButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class OptionsDIDialog(wx.Dialog):
    """
    Dialog for DI specific options
    """

    def __init__(self, parent, title, options):
        super(OptionsDIDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.options = options

        self.abstypes = ['manual','autodif']
        self.dipathlist = self.options.get('dipathlist','')
        if isinstance(self.dipathlist, str):
            self.dipathlist = self.dipathlist
        else:
            self.dipathlist = self.dipathlist[0]

        self.sheetorder = self.options.get('order','')
        #self.sheetorder = ",".join(self.options.get('order',''))
        self.sheetdouble = False
        self.sheetscale = False
        if self.options.get('double','False') == 'True':
            self.sheetdouble = True
        if self.options.get('scalevalue','False') == 'True':
            self.sheetscale = True
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # General paths
        self.DIPathsLabel = wx.StaticText(self, label="Set paths:",size=(160,30))
        self.dipathlistLabel = wx.StaticText(self, label="Default DI path")
        self.divariopathLabel = wx.StaticText(self, label="DI variometer")
        self.discalarpathLabel = wx.StaticText(self, label="DI scalar")
        self.diselectpathLabel = wx.StaticText(self, label="(select paths in DI panel)")
        self.dipathlistTextCtrl = wx.TextCtrl(self, value=self.dipathlist)
        self.divariopathTextCtrl = wx.TextCtrl(self, value=self.options.get('divariopath',''))
        self.discalarpathTextCtrl = wx.TextCtrl(self, value=self.options.get('discalarpath',''))
        self.dipathlistTextCtrl.Disable()
        self.divariopathTextCtrl.Disable()
        self.discalarpathTextCtrl.Disable()
        # Thresholds and defaults
        self.DIDefaultsLabel = wx.StaticText(self, label="Defaults:",size=(160,30))
        self.diexpDLabel = wx.StaticText(self, label="expected Dec",size=(160,30))
        self.diexpILabel = wx.StaticText(self, label="expected Inc",size=(160,30))
        self.diidLabel = wx.StaticText(self, label="DI ID",size=(160,30))
        self.ditypeLabel = wx.StaticText(self, label="DI Type",size=(160,30)) #abstype
        self.diazimuthLabel = wx.StaticText(self, label="Azimuth",size=(160,30))
        self.dipierLabel = wx.StaticText(self, label="Pier",size=(160,30))
        self.dialphaLabel = wx.StaticText(self, label="Alpha",size=(160,30))
        self.dideltaFLabel = wx.StaticText(self, label="Delta F",size=(160,30))
        self.didbaddLabel = wx.StaticText(self, label="Add to DB",size=(160,30))
        diexpD = str(self.options.get('diexpD',''))
        diexpI = str(self.options.get('diexpI',''))
        diazimuth = str(self.options.get('diazimuth',''))
        dipier = str(self.options.get('dipier',''))
        dialpha = str(self.options.get('dialpha',''))
        dideltaF = str(self.options.get('dideltaF',''))
        self.diannualmeanLabel = wx.StaticText(self, label="AnnualMean (*)",size=(160,30))
        self.dibetaLabel = wx.StaticText(self, label="Beta",size=(160,30))
        self.dideltaDLabel = wx.StaticText(self, label="Delta D",size=(160,30))
        self.dideltaILabel = wx.StaticText(self, label="Delta I",size=(160,30))
        diannualmean = str(self.options.get('diannualmean',''))
        dibeta = str(self.options.get('dibeta',''))
        dideltaD = str(self.options.get('dideltaD',''))
        dideltaI = str(self.options.get('dideltaI',''))
        self.dibetaTextCtrl = wx.TextCtrl(self, value=dibeta,size=(160,30))
        self.dideltaDTextCtrl = wx.TextCtrl(self, value=dideltaD,size=(160,30))
        self.diannualmeanTextCtrl = wx.TextCtrl(self, value=diannualmean,size=(160,30))
        self.dideltaITextCtrl = wx.TextCtrl(self, value=dideltaI,size=(160,30))
        """ add
        - beta:         (float) orientation angle 2 in deg
        - deltaD:       (float) = kwargs.get('deltaD')
        - deltaI:       (float) = kwargs.get('deltaI')
        - outputformat: (string) one of 'idf', 'xyz', 'hdf'
        - annualmeans:  (list) provide annualmean for x,y,z as [x,y,z] with floats
        """
        self.diexpDTextCtrl = wx.TextCtrl(self, value=diexpD,size=(160,30))
        self.diexpITextCtrl = wx.TextCtrl(self, value=diexpI,size=(160,30))
        self.diidTextCtrl = wx.TextCtrl(self, value=self.options.get('diid',''),size=(160,30))
        #self.ditypeTextCtrl = wx.TextCtrl(self, value=,size=(160,30)) #abstype
        self.ditypeComboBox = wx.ComboBox(self, choices=self.abstypes,
                 style=wx.CB_DROPDOWN, value=self.options.get('ditype',''),size=(160,-1))
        self.diazimuthTextCtrl = wx.TextCtrl(self, value=diazimuth,size=(160,30))
        self.dipierTextCtrl = wx.TextCtrl(self, value=dipier,size=(160,30))
        self.dialphaTextCtrl = wx.TextCtrl(self, value=dialpha,size=(160,30))
        self.dideltaFTextCtrl = wx.TextCtrl(self, value=dideltaF,size=(160,30))
        self.didbaddTextCtrl = wx.TextCtrl(self, value=self.options.get('didbadd',''),size=(160,30))
        # Thresholds and defaults
        self.DIInputLabel = wx.StaticText(self, label="Input sheet:",size=(160,30))
        self.sheetorderTextCtrl = wx.TextCtrl(self, value=self.sheetorder,size=(160,30))
        self.sheetdoubleCheckBox = wx.CheckBox(self, label="Repeated positions",size=(160,30))
        self.sheetscaleCheckBox = wx.CheckBox(self, label="Scale value",size=(160,30))

        f = self.DIInputLabel.GetFont()
        newf = wx.Font(14, wx.DECORATIVE, wx.ITALIC, wx.BOLD)
        self.DIInputLabel.SetFont(newf)
        self.DIDefaultsLabel.SetFont(newf)
        self.DIPathsLabel.SetFont(newf)

        self.sheetdoubleCheckBox.SetValue(self.sheetdouble)
        self.sheetscaleCheckBox.SetValue(self.sheetscale)
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        self.saveButton = wx.Button(self, wx.ID_OK, label='Save')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=13, cols=4, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.DIPathsLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.dipathlistLabel, noOptions),
                 (self.divariopathLabel, noOptions),
                 (self.discalarpathLabel, noOptions),
                 (self.diselectpathLabel, noOptions),
                 (self.dipathlistTextCtrl, expandOption),
                 (self.divariopathTextCtrl, expandOption),
                 (self.discalarpathTextCtrl, expandOption),
                 (self.DIDefaultsLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.ditypeLabel, noOptions),
                 (self.diidLabel, noOptions),
                 (self.diexpDLabel, noOptions),
                 (self.diexpILabel, noOptions),
                 (self.ditypeComboBox, noOptions),
                 (self.diidTextCtrl, expandOption),
                 (self.diexpDTextCtrl, expandOption),
                 (self.diexpITextCtrl, expandOption),
                 (self.diazimuthLabel, noOptions),
                 (self.dipierLabel, noOptions),
                 (self.dialphaLabel, noOptions),
                 (self.dideltaFLabel, noOptions),
                 (self.diazimuthTextCtrl, expandOption),
                 (self.dipierTextCtrl, expandOption),
                 (self.dialphaTextCtrl, expandOption),
                 (self.dideltaFTextCtrl, expandOption),
                 (self.dideltaDLabel, noOptions),
                 (self.dideltaILabel, noOptions),
                 (self.dibetaLabel, noOptions),
                 (self.diannualmeanLabel, noOptions),
                 (self.dideltaDTextCtrl, expandOption),
                 (self.dideltaITextCtrl, expandOption),
                 (self.dibetaTextCtrl, expandOption),
                 (self.diannualmeanTextCtrl, expandOption),
                 (self.didbaddLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.didbaddTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.DIInputLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.sheetorderTextCtrl, expandOption),
                 (self.sheetdoubleCheckBox, noOptions),
                 (self.sheetscaleCheckBox, noOptions),
                 (self.saveButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


# ###################################################
#    Stream page
# ###################################################

class StreamExtractValuesDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Extracting defined data
    """

    def __init__(self, parent, title, keylst):
        super(StreamExtractValuesDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.keylst = keylst
        self.comparelst = ['==','<=','<','>','>=','!=']
        self.logic3lst = ['and','or']
        self.logic2lst = ['and','or']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):

        self.keyLabel = wx.StaticText(self, label="Available keys:")
        self.key1ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=self.keylst[0],size=(160,-1))
        self.compare1ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0],size=(160,-1))
        self.value1TextCtrl = wx.TextCtrl(self, value="")
        self.logic2ComboBox = wx.ComboBox(self, choices=self.logic2lst,
            style=wx.CB_DROPDOWN, value=self.logic2lst[0],size=(160,-1))
        if len(self.keylst) > 1:
            val2 =  self.keylst[1]
        else:
            val2 = ''
        self.key2ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=val2,size=(160,-1))
        self.compare2ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0],size=(160,-1))
        self.value2TextCtrl = wx.TextCtrl(self, value="")
        self.logic3ComboBox = wx.ComboBox(self, choices=self.logic3lst,
            style=wx.CB_DROPDOWN, value=self.logic3lst[0],size=(160,-1))
        if len(self.keylst) > 2:
            val3 =  self.keylst[2]
        else:
            val3 = ''
        self.key3ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=val3,size=(160,-1))
        self.compare3ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0],size=(160,-1))
        self.value3TextCtrl = wx.TextCtrl(self, value="")
        self.okButton = wx.Button(self, wx.ID_OK, label='Extract')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [emptySpace,
                 (self.keyLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.key1ComboBox, expandOption),
                 (self.compare1ComboBox, expandOption),
                 (self.value1TextCtrl, expandOption),
                 (self.logic2ComboBox, noOptions),
                 (self.key2ComboBox, expandOption),
                 (self.compare2ComboBox, expandOption),
                 (self.value2TextCtrl, expandOption),
                 (self.logic3ComboBox, noOptions),
                 (self.key3ComboBox, expandOption),
                 (self.compare3ComboBox, expandOption),
                 (self.value3TextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class StreamSelectKeysDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, keylst, shownkeys, namelist):
        super(StreamSelectKeysDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.keylst = keylst
        self.shownkeylst = shownkeys
        self.namelist = namelist
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        for idx,elem in enumerate(self.keylst):
            if len(self.namelist) == len(self.keylst):
                if self.namelist[idx] == None:
                    self.namelist[idx] = 'None'
                colname = self.namelist[idx]
            else:
                colname = elem
            exec('self.'+elem+'CheckBox = wx.CheckBox(self,label="'+colname+'")')
        self.okButton = wx.Button(self, wx.ID_OK, label='Select')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst = [eval('(self.'+elem+'CheckBox, expandOption)') for elem in self.keylst]
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=len(contlst), cols=1, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class StreamPlotOptionsDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Plot options
        modify option
    """

    def __init__(self, parent, title, optdict):
        super(StreamPlotOptionsDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.optdict = optdict
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        for elem in self.optdict:
            #print (elem, self.optdict[elem])
            val = "{}".format(self.optdict[elem])
            exec('self.'+elem+'Text = wx.StaticText(self,label="'+elem+'",size=(160,30))')
            exec('self.'+elem+'TextCtrl = wx.TextCtrl(self, value="'+val+'",size=(160,30))')
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst = [[eval('(self.'+elem+'Text, noOptions)'),eval('(self.'+elem+'TextCtrl, expandOption)')] for elem in self.optdict]
        contlst = [y for x in contlst for y in x]

        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        cols = 4
        rows = int(np.ceil(len(contlst)/float(cols)))

        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class StreamFlagOutlierDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Parameter selection of outlier flagging routine
    USED BY:
        Stream Method: onFlagOutlier()
    """
    def __init__(self, parent, title, threshold, timerange):
        super(StreamFlagOutlierDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        self.threshold=str(threshold)
        self.timerange=str(timerange)
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.ThresholdText = wx.StaticText(self,label="Threshold")
        self.TimerangeText = wx.StaticText(self,label="Window width")
        self.UnitText = wx.StaticText(self,label="seconds")
        self.ThresholdTextCtrl = wx.TextCtrl(self, value=self.threshold)
        self.TimerangeTextCtrl = wx.TextCtrl(self, value=self.timerange)
        self.MarkAllCheckBox = wx.CheckBox(self, label="Mark outliers in",size=(160,30))
        self.MarkText = wx.StaticText(self,label="all components")
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.ThresholdText, noOptions))
        contlst.append((self.TimerangeText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.ThresholdTextCtrl, expandOption))
        contlst.append((self.TimerangeTextCtrl, expandOption))
        contlst.append((self.UnitText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.MarkAllCheckBox, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.MarkText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 3
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class StreamFlagRangeDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Parameter selection of flag range routine
    USED BY:
        Stream Method: onFlagRange()
    """
    def __init__(self, parent, title, stream, shownkeylist, keylist):
        super(StreamFlagRangeDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        self.shownkeys=shownkeylist
        self.selectedkey = shownkeylist[0]
        self.keys2flag = ",".join(shownkeylist)
        self.keys=keylist
        self.stream = stream
        self.mintime = num2date(stream.ndarray[0][0])
        self.maxtime = num2date(stream.ndarray[0][-1])
        self.flagidlist = ['0: normal data', '1: automatically flagged', '2: keep data in any case', '3: remove data', '4: special flag']
        self.comment = ''
        #dt=wx.DateTimeFromTimeT(time.mktime(self.maxtime.timetuple()))
        self.ul = np.nanmax(self.stream.ndarray[KEYLIST.index(self.selectedkey)])
        self.ll = np.nanmin(self.stream.ndarray[KEYLIST.index(self.selectedkey)])
        self.rangetype = ['value', 'time']
        self.createControls()
        self.doLayout()
        self.bindControls()
        self.SetValue()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.TimeRangeText = wx.StaticText(self,label="Flag time range")
        self.ValueRangeText = wx.StaticText(self,label="Flag value range")
        self.LimitKeyText = wx.StaticText(self,label="Select Key:")
        self.UpperLimitText = wx.StaticText(self,label="Flag values below:")
        self.UpperLimitTextCtrl = wx.TextCtrl(self, value=str(self.ul),size=(160,30))
        self.LowerLimitText = wx.StaticText(self,label="Flag values above:")
        self.LowerLimitTextCtrl = wx.TextCtrl(self, value=str(self.ll),size=(160,30))
        self.SelectKeyComboBox = wx.ComboBox(self, choices=self.shownkeys,
            style=wx.CB_DROPDOWN, value=self.shownkeys[self.shownkeys.index(self.selectedkey)],size=(160,-1))
        self.UpperTimeText = wx.StaticText(self,label="Flag data before:")
        self.LowerTimeText = wx.StaticText(self,label="Flag data after:")
        self.startFlagDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(self.mintime.timetuple())),size=(160,30))
        self.startFlagTimePicker = wx.TextCtrl(self, value=self.mintime.strftime('%X'),size=(160,30))
        self.endFlagDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(self.maxtime.timetuple())),size=(160,30))
        self.endFlagTimePicker = wx.TextCtrl(self, value=self.maxtime.strftime('%X'),size=(160,30))
        self.KeyListText = wx.StaticText(self,label="Keys which will be flagged:")
        self.AffectedKeysTextCtrl = wx.TextCtrl(self, value=self.keys2flag,size=(160,30))
        self.FlagIDText = wx.StaticText(self,label="Select Flag ID:")
        self.FlagIDComboBox = wx.ComboBox(self, choices=self.flagidlist,
            style=wx.CB_DROPDOWN, value=self.flagidlist[3],size=(160,-1))
        self.CommentText = wx.StaticText(self,label="Comment:")
        self.CommentTextCtrl = wx.TextCtrl(self, value=self.comment,size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))
        self.rangeRadioBox = wx.RadioBox(self,
            label="Select flagging range type:",
            choices=self.rangetype, majorDimension=2, style=wx.RA_SPECIFY_COLS)

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        # 1 row
        contlst.append((self.rangeRadioBox, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        # 2 row
        contlst.append((self.ValueRangeText, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.LimitKeyText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.LowerLimitText, noOptions))
        contlst.append((self.UpperLimitText, noOptions))
        # 3 row
        contlst.append((self.SelectKeyComboBox, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.LowerLimitTextCtrl, expandOption))
        contlst.append((self.UpperLimitTextCtrl, expandOption))
        # 4 row
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.TimeRangeText, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        # 5 row
        contlst.append((self.LowerTimeText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.UpperTimeText, noOptions))
        contlst.append(emptySpace)
        # 6 row
        contlst.append((self.startFlagDatePicker, expandOption))
        contlst.append((self.startFlagTimePicker, expandOption))
        contlst.append((self.endFlagDatePicker, expandOption))
        contlst.append((self.endFlagTimePicker, expandOption))
        # 7 row
        contlst.append((self.KeyListText, noOptions))
        contlst.append((self.FlagIDText, noOptions))
        contlst.append((self.CommentText, noOptions))
        contlst.append(emptySpace)
        # 8 row
        contlst.append((self.AffectedKeysTextCtrl, expandOption))
        contlst.append((self.FlagIDComboBox, expandOption))
        contlst.append((self.CommentTextCtrl, expandOption))
        contlst.append(emptySpace)
        # 9 row
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.Bind(wx.EVT_RADIOBOX, self.OnChangeGroup, self.rangeRadioBox)
        self.Bind(wx.EVT_COMBOBOX, self.OnChangeSelection, self.SelectKeyComboBox)

    def SetValue(self):
            self.UpperLimitTextCtrl.Enable()
            self.LowerLimitTextCtrl.Enable()
            self.SelectKeyComboBox.Enable()
            self.LowerTimeText.Disable()
            self.UpperTimeText.Disable()
            self.startFlagDatePicker.Disable()
            self.startFlagTimePicker.Disable()
            self.endFlagDatePicker.Disable()
            self.endFlagTimePicker.Disable()
            self.UpperLimitTextCtrl.SetValue(str(self.ul))
            self.LowerLimitTextCtrl.SetValue(str(self.ll))


    def OnChangeGroup(self, e):
        val = self.rangeRadioBox.GetStringSelection()
        print ("Change group", val)
        if str(val) == 'time':
            self.UpperLimitTextCtrl.Disable()
            self.LowerLimitTextCtrl.Disable()
            self.SelectKeyComboBox.Disable()
            self.LowerTimeText.Enable()
            self.UpperTimeText.Enable()
            self.startFlagDatePicker.Enable()
            self.startFlagTimePicker.Enable()
            self.endFlagDatePicker.Enable()
            self.endFlagTimePicker.Enable()
        elif str(val) == 'value':
            self.SetValue()

    def OnChangeSelection(self, e):
        firstkey = self.SelectKeyComboBox.GetValue()
        ind = KEYLIST.index(firstkey)
        self.ul = np.nanmax(self.stream.ndarray[ind])
        self.ll = np.nanmin(self.stream.ndarray[ind])
        self.UpperLimitTextCtrl.SetValue(str(self.ul))
        self.LowerLimitTextCtrl.SetValue(str(self.ll))
        print (str(firstkey),ind, self.ul, self.ll)


class StreamFlagSelectionDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Parameter selection of flag range routine
    USED BY:
        Stream Method: onFlagRange()
    """
    def __init__(self, parent, title, shownkeylist, keylist):
        super(StreamFlagSelectionDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        self.shownkeys=shownkeylist
        self.selectedkey = shownkeylist[0]
        self.keys2flag = ",".join(shownkeylist)
        self.keys=keylist
        self.flagidlist = ['0: normal data', '1: automatically flagged', '2: keep data in any case', '3: remove data', '4: special flag']
        self.comment = ''
        self.createControls()
        self.doLayout()
        print ("Dialog open", shownkeylist, keylist)

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.KeyListText = wx.StaticText(self,label="Keys which will be flagged:")
        self.AffectedKeysTextCtrl = wx.TextCtrl(self, value=self.keys2flag,size=(160,30))
        self.FlagIDText = wx.StaticText(self,label="Select Flag ID:")
        self.FlagIDComboBox = wx.ComboBox(self, choices=self.flagidlist,
            style=wx.CB_DROPDOWN, value=self.flagidlist[3],size=(160,-1))
        self.CommentText = wx.StaticText(self,label="Comment:")
        self.CommentTextCtrl = wx.TextCtrl(self, value=self.comment,size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.KeyListText, noOptions))
        contlst.append((self.FlagIDText, noOptions))
        contlst.append((self.CommentText, noOptions))
        # 8 row
        contlst.append((self.AffectedKeysTextCtrl, expandOption))
        contlst.append((self.FlagIDComboBox, expandOption))
        contlst.append((self.CommentTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 3
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class StreamLoadFlagDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Loading Flagging data from file or DB
    """
    def __init__(self, parent, title, db, sensorid, start, end):
        super(StreamLoadFlagDialog, self).__init__(parent=parent,
            title=title, size=(300, 300))
        self.flaglist = []
        self.sensorid = sensorid
        self.db = db
        self.start = start
        self.end = end
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.loadDBButton = wx.Button(self, label='Load from DB')
        self.loadFileButton = wx.Button(self, label='Load file')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        if not self.db:
            self.loadDBButton.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.loadDBButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.loadFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.loadDBButton.Bind(wx.EVT_BUTTON, self.OnLoadDB)
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnLoadFile)

    def OnLoadDB(self, e):
        self.flaglist = db2flaglist(self.db, self.sensorid, begin=self.start, end=self.end)
        dlg = wx.MessageDialog(self, "Flags for {} loaded from DB!\nFLAGS table contained {} inputs\n".format(self.sensorid,len(self.flaglist)),"FLAGS obtained from DB", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.Close(True)

    def OnLoadFile(self, e):
        openFileDialog = wx.FileDialog(self, "Open", "", "",
                                       "pickle flaglist (*.pkl)|*.pkl|json flaglist (*.json)|*.json",
                                       wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        openFileDialog.ShowModal()
        flagname = openFileDialog.GetPath()
        try:
            self.flaglist = loadflags(flagname,sensorid=self.sensorid, begin=self.start, end=self.end)
        except:
            self.flaglist = []
        openFileDialog.Destroy()
        dlg = wx.MessageDialog(self, "Flags for {} loaded from File!\nFound {} flag inputs\n".format(self.sensorid,len(self.flaglist)),"FLAGS obtained from File", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.Close(True)


class StreamSaveFlagDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Loading Flagging data from file or DB
    """
    def __init__(self, parent, title, db, flaglist):
        super(StreamSaveFlagDialog, self).__init__(parent=parent,
            title=title, size=(300, 300))
        self.flaglist = flaglist
        self.db = db
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.saveDBButton = wx.Button(self, label='Save to DB')
        self.saveFileButton = wx.Button(self, label='Save to file')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        if not self.db:
            self.saveDBButton.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.saveDBButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.saveFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.saveDBButton.Bind(wx.EVT_BUTTON, self.OnSaveDB)
        self.saveFileButton.Bind(wx.EVT_BUTTON, self.OnSaveFile)

    def OnSaveDB(self, e):
        print ("Saving", self.flaglist[0])
        flaglist2db(self.db, self.flaglist)
        dlg = wx.MessageDialog(self, "Flags stored in connected DB!\nFLAGS table extended with {} inputs\n".format(len(self.flaglist)),"FLAGS added to DB", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.Close(True)

    def OnSaveFile(self, e):
        saveFileDialog = wx.FileDialog(self, "Save As", "", "",
                                       "pickle flaglist (*.pkl)|*.pkl|json flaglist (*.json)|*.json",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        saveFileDialog.ShowModal()
        flagname = saveFileDialog.GetPath()
        saveFileDialog.Destroy()
        print (flagname)
        saveflags(self.flaglist,flagname)
        self.Close(True)

# ###################################################
#    Meta page
# ###################################################

class MetaDataDialog(wx.Dialog):
    """
    DESCRITPTION
        InputDialog for DI data
    """

    def __init__(self, parent, title, header, layer):
        style = wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        super(MetaDataDialog, self).__init__(parent=parent,
            title=title, style=style) #, size=(600, 600))
        self.header = header
        self.list = []
        self.layer=layer

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        # Add Settings Panel
        self.panel = MetaDataPanel(self, header, layer)
        self.panel.SetInitialSize((400, 500))
        self.mainSizer.Add(self.panel, 0, wx.EXPAND | wx.ALL, 20)
        # Add Save/Cancel Buttons
        self.createWidgets()
        # Set sizer and window size
        self.SetSizerAndFit(self.mainSizer)
        #self.mainSizer.Fit(self)

    def createWidgets(self):
        """Create and layout the widgets in the dialog"""
        btnSizer = wx.StdDialogButtonSizer()

        saveBtn = wx.Button(self, wx.ID_OK, label="Update",size=(160,30))
        #saveBtn.Bind(wx.EVT_BUTTON, self.OnSave)
        btnSizer.AddButton(saveBtn)

        cancelBtn = wx.Button(self, wx.ID_NO, label="Close",size=(160,30))
        cancelBtn.Bind(wx.EVT_BUTTON, self.OnClose)
        btnSizer.AddButton(cancelBtn)
        btnSizer.Realize()

        self.mainSizer.Add(btnSizer, 0, wx.ALL | wx.ALIGN_RIGHT, 5)

    def OnClose(self, event):
        self.Close(True)


class MetaDataPanel(scrolledpanel.ScrolledPanel):
    """
    Dialog for MetaData panel
    """
    def __init__(self, parent, header, layer):
        scrolledpanel.ScrolledPanel.__init__(self, parent, -1, size=(1000, 800))

        self.header = header
        self.list = []
        self.layer=layer
        self.mainSizer = wx.BoxSizer(wx.VERTICAL)

        self.createControls()
        self.cnts=[0,0]
        self.doLayout()

        self.SetSizer(self.mainSizer)
        self.mainSizer.Fit(self)
        self.SetupScrolling()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        cnt,colcnt = 0,0
        self.list = eval(self.layer.upper() + 'KEYLIST')

        for key in self.list:
            if key.find('-') > 0:
                # Column contents:
                tmplst = key.split('-')
                tmplst[-1] = KEYLIST.index(tmplst[-1])
                if tmplst[0] == 'unit':
                    label = tmplst[1].replace('col','Column') + str(tmplst[-1])+'_unit'
                else:
                    label = tmplst[0].replace('col','Column') + str(tmplst[-1])
                key=key.replace('-','')
                colcnt += 1
            else:
                label = key
                value = str(self.header.get(key,''))
                value = value.replace('\r\n',' ').replace('\n','')
                if not isinstance(value, str) or '[' in value:
                     #print ("not a string")
                     try:
                         try:
                             float(value)
                         except:
                             value = 'object with complex data'
                     except:
                         value = 'object with complex data'
                cnt += 1

                label = self.AppendLabel(key,label)
                # DYNAMICSIZE
                if PLATFORM.startswith('linux'):
                    dynsize = '30'
                else:
                    dynsize = '50'

                exec('self.'+key+'Text = wx.StaticText(self,label="'+label+'")')
                exec('self.'+key+'TextCtrl = wx.TextCtrl(self, value="'+value+'",size=(160,'+dynsize+'),style = wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)')
                if value.startswith('object with complex'):
                    exec('self.'+key+'TextCtrl.Disable()')
        self.cnts = [colcnt, cnt]

        self.legendText = wx.StaticText(self,label="(1: IAF, 2: IAGA, 3: IMAGCDF)")

    def doLayout(self):
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # fill all lines with empty fields to max(cnt)
        # transpose this array
        contlst = []

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        headarray = [[],[],[],[],[]]
        for key in self.list:
            #if key.startswith(self.layer):
            contlst.append(eval('(self.'+key+'Text, noOptions)'))
            contlst.append(eval('(self.'+key+'TextCtrl, expandOption)'))
                #headline.append(key)
                #elif elem == 'col' and key.find('-'):
                #    headline.append(key)
        #headarray[idx] = headline

        #for elem in self.header:
        #    if elem.find('-') > 0:
        #        elem=elem.replace('-','')
        #    contlst.append(eval('(self.'+elem+'Text, expandOption)'))

        contlst.append(emptySpace)
        contlst.append((self.legendText, noOptions))

        # A GridSizer will contain the other controls:
        cols = 6
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        self.mainSizer.Add(gridSizer, 0, wx.EXPAND)


    def AppendLabel(self, key,label):
        from magpy.lib.magpy_formats import IAFMETA, IAGAMETA, IMAGCDFMETA
        #print (IAFMETA, IAGAMETA, IMAGCDFMETA)
        if key in IAFMETA:
            if not label.find('(') > 0:
                label += '(1'
        if key in IAGAMETA:
            if not label.find('(') > 0:
                label += '(2'
            else:
                label += ',2'
        if key in IMAGCDFMETA:
            if not label.find('(') > 0:
                label += '(3'
            else:
                label += ',3'
        if label.find('(') > 0:
            label += ')'

        return label

# ###################################################
#    Analysis page
# ###################################################

class AnalysisFitDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, options, stream, shownkeylist, keylist):
        super(AnalysisFitDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))

        self.shownkeys=shownkeylist
        self.selectedkey = shownkeylist[0]
        self.keys2flag = ",".join(shownkeylist)
        self.keys=keylist
        self.stream = stream
        self.options = options
        self.fitfunc = self.options.get('fitfunction','spline')
        self.funclist = ['spline','polynomial']
        self.fitknots = self.options.get('fitknotstep','0.3')
        self.fitdegree = self.options.get('fitdegree','5')
        self.mintime = num2date(stream.ndarray[0][0])
        self.maxtime = num2date(stream.ndarray[0][-1])
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.funcLabel = wx.StaticText(self, label="Fit function:",size=(160,30))
        self.funcComboBox = wx.ComboBox(self, choices=self.funclist,
            style=wx.CB_DROPDOWN, value=self.fitfunc,size=(160,-1))
        self.knotsLabel = wx.StaticText(self, label="Knots [e.g. 0.5  (0..1)] (spline only):")
        self.knotsTextCtrl = wx.TextCtrl(self, value=self.fitknots,size=(160,30))
        self.degreeLabel = wx.StaticText(self, label="Degree [e.g. 1, 2, 345, etc.] (polynomial only):")
        self.degreeTextCtrl = wx.TextCtrl(self, value=self.fitdegree,size=(160,30))

        self.UpperTimeText = wx.StaticText(self,label="Fit data before:")
        self.LowerTimeText = wx.StaticText(self,label="Fit data after:")
        self.startFitDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(self.mintime.timetuple())),size=(160,30))
        self.startFitTimePicker = wx.TextCtrl(self, value=self.mintime.strftime('%X'),size=(160,30))
        self.endFitDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(self.maxtime.timetuple())),size=(160,30))
        self.endFitTimePicker = wx.TextCtrl(self, value=self.maxtime.strftime('%X'),size=(160,30))

        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.funcLabel, noOptions)]
        contlst.append((self.funcComboBox, expandOption))
        contlst.append((self.knotsLabel, noOptions))
        contlst.append((self.knotsTextCtrl, expandOption))
        contlst.append((self.degreeLabel, noOptions))
        contlst.append((self.degreeTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.LowerTimeText, noOptions))
        contlst.append((self.startFitDatePicker, expandOption))
        contlst.append((self.startFitTimePicker, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.UpperTimeText, noOptions))
        contlst.append((self.endFitDatePicker, expandOption))
        contlst.append((self.endFitTimePicker, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class AnalysisFilterDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, samplingrate, resample, winlen, resint, resoff, filtertype):
        super(AnalysisFilterDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.samplingrate = samplingrate
        self.windowlist = ['flat','barthann','bartlett','blackman','blackmanharris','bohman',
                                'boxcar','cosine','flattop','hamming','hann','nuttall',
                                'parzen','triang','gaussian','wiener','spline','butterworth']
        self.resample = resample
        self.winlen = str(winlen)
        self.resint = str(resint)
        self.filtertype = filtertype
        self.missing = ['IAGA','interpolate','conservative']
        self.off = str(resoff)
        if not resample:
            self.buttonlabel = 'Smooth'
        else:
            self.buttonlabel = 'Filter'
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.filtertypeLabel = wx.StaticText(self, label="Select window:")
        self.filtertypeComboBox = wx.ComboBox(self, choices=self.windowlist,
            style=wx.CB_DROPDOWN, value=self.filtertype,size=(160,-1))
        self.lengthLabel = wx.StaticText(self, label="Window length (sec):")
        self.lengthTextCtrl = wx.TextCtrl(self, value=self.winlen)
        if self.resample:
            self.resampleLabel = wx.StaticText(self, label="Resample interval (sec):")
            self.resampleTextCtrl = wx.TextCtrl(self, value=self.resint)
            self.resampleoffsetLabel = wx.StaticText(self, label="Resample offset (sec):")
            self.resampleoffsetTextCtrl = wx.TextCtrl(self, value=self.off)
        self.methodRadioBox = wx.RadioBox(self,
            label="Missing data:",
            choices=self.missing, majorDimension=1, style=wx.RA_SPECIFY_COLS)
        self.okButton = wx.Button(self, wx.ID_OK, label=self.buttonlabel)
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.filtertypeLabel, noOptions)]
        contlst.append((self.filtertypeComboBox, expandOption))
        contlst.append((self.lengthLabel, noOptions))
        contlst.append((self.lengthTextCtrl, expandOption))
        if self.resample:
            contlst.append((self.resampleLabel, noOptions))
            contlst.append((self.resampleTextCtrl, expandOption))
            contlst.append((self.resampleoffsetLabel, noOptions))
            contlst.append((self.resampleoffsetTextCtrl, expandOption))
        contlst.append((self.methodRadioBox, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class AnalysisOffsetDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, keylst, xlimits, deltas):
        super(AnalysisOffsetDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.keylst = keylst
        self.choices = ['all','timerange']
        self.start = self._pydate2wxdate(xlimits[0])
        self.end = self._pydate2wxdate(xlimits[1])
        self.starttime = datetime.strftime(xlimits[0], "%H:%M:%S")
        self.endtime = datetime.strftime(xlimits[1], "%H:%M:%S")
        self.val = {}
        self.val['time'] = '0'
        mtime = date2num(xlimits[1])
        if not deltas == '':
            try:
                dlist = deltas.split(',')
                for delt in dlist:
                    de = delt.split('_')
                    if not de[0] == 'time':
                        self.val[de[0]] = str(de[1])
                    else:
                        self.val[de[0]] = str(de[1].strip(')').split('=')[-1])
                    if de[0].strip() == 'et' and float(self.val[de[0]].split(';')[0]) >= mtime:
                        break
                    print ("BB", self.val[de[0]], de[0])
            except:
                pass
        self.createControls()
        self.doLayout()
        self.bindControls()


    def _pydate2wxdate(self,date):
        assert isinstance(date, (datetime, datetime.date))
        tt = date.timetuple()
        dmy = (tt[2], tt[1]-1, tt[0])
        return wx.DateTimeFromDMY(*dmy)

    # Widgets
    def createControls(self):
        # Add a radio button on top:
        # Offset certain timerange / all data
        self.offsetRadioBox = wx.RadioBox(self, label="Apply offset to:",
                     choices=self.choices, majorDimension=2, style=wx.RA_SPECIFY_COLS)

        self.timeshiftLabel = wx.StaticText(self, label="Timeshift (sec):",size=(160,30))
        self.timeshiftTextCtrl = wx.TextCtrl(self, value=self.val.get('time','0'),size=(160,30))

        self.StartDateLabel = wx.StaticText(self, label="Starting:",size=(160,30))
        self.StartDatePicker = wx.DatePickerCtrl(self, dt=self.start,size=(160,30))
        self.StartTimeTextCtrl = wx.TextCtrl(self, value=self.starttime,size=(160,30))
        self.EndDateLabel = wx.StaticText(self, label="Ending:",size=(160,30))
        self.EndDatePicker = wx.DatePickerCtrl(self, dt=self.end,size=(160,30))
        self.EndTimeTextCtrl = wx.TextCtrl(self, value=self.endtime,size=(160,30))

        for elem in self.keylst:
            exec('self.'+elem+'Label = wx.StaticText(self,label="'+elem+'")')
            exec('self.'+elem+'TextCtrl = wx.TextCtrl(self,value="'+self.val.get(elem,'')+'")')
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

        self.StartDatePicker.Disable()
        self.StartTimeTextCtrl.Disable()
        self.EndDatePicker.Disable()
        self.EndTimeTextCtrl.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.offsetRadioBox, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.StartDateLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.StartDatePicker, expandOption))
        contlst.append((self.StartTimeTextCtrl, expandOption))
        contlst.append((self.EndDateLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.EndDatePicker, expandOption))
        contlst.append((self.EndTimeTextCtrl, expandOption))
        contlst.append((self.timeshiftLabel, noOptions))
        contlst.append((self.timeshiftTextCtrl, expandOption))
        for elem in self.keylst:
            contlst.append(eval('(self.'+elem+'Label, noOptions)'))
            contlst.append(eval('(self.'+elem+'TextCtrl, expandOption)'))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        #print "Hello:", contlst
        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.Bind(wx.EVT_RADIOBOX, self.OnChangeRange, self.offsetRadioBox)

    def OnChangeRange(self, e):
        val = self.offsetRadioBox.GetStringSelection()
        if str(val) == 'all':
            self.StartDatePicker.Disable()
            self.StartTimeTextCtrl.Disable()
            self.EndDatePicker.Disable()
            self.EndTimeTextCtrl.Disable()
            self.timeshiftTextCtrl.Enable()
        elif str(val) == 'timerange':
            self.StartDatePicker.Enable()
            self.StartTimeTextCtrl.Enable()
            self.EndDatePicker.Enable()
            self.EndTimeTextCtrl.Enable()
            self.timeshiftTextCtrl.Disable()

class AnalysisResampleDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """
    def __init__(self, parent, title,keylst, period):
        super(AnalysisResampleDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.period = period
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.periodLabel = wx.StaticText(self,label="Period")
        self.periodTextCtrl = wx.TextCtrl(self,value=str(self.period))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.periodLabel, noOptions))
        contlst.append((self.periodTextCtrl, expandOption))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

class AnalysisRotationDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """
    def __init__(self, parent, title):
        super(AnalysisRotationDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.alphaLabel = wx.StaticText(self,label="Alpha")
        self.alphaTextCtrl = wx.TextCtrl(self,value="")
        self.betaLabel = wx.StaticText(self,label="Beta")
        self.betaTextCtrl = wx.TextCtrl(self,value="")
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.alphaLabel, noOptions))
        contlst.append((self.alphaTextCtrl, expandOption))
        contlst.append((self.betaLabel, noOptions))
        contlst.append((self.betaTextCtrl, expandOption))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class AnalysisBaselineDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, idxlst, dictlst, options, stream, shownkeylist, keylist):
        super(AnalysisBaselineDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        self.options = options
        self.plotstream = stream
        self.shownkeylist = shownkeylist
        self.keylist = keylist
        self.idxlst = idxlst
        self.dictlst = dictlst
        self.fitlist = ['time1','time2']
        self.absstreamlist = []
        for idx in idxlst:
            currentname = [el['filename'] for el in dictlst if str(el['streamidx']) == str(idx)][0]
            line = str(idx)+': '+currentname
            if not line in self.absstreamlist:
                self.absstreamlist.append(line)
        self.parameterstring = "Function: {}\nKnotstep: {}\nDegree: {}\n".format(self.options.get('fitfunction',''),self.options.get('fitknotstep',''),self.options.get('fitdegree',''))
        self.createControls()
        self.doLayout()
        self.bindControls()

        #self.funclist = ['spline','polynomial']
        #self.fitknots = fitknots
        #self.fitdegree = fitdegree


    def _pydate2wxdate(self,date):
        assert isinstance(date, (datetime, datetime.date))
        tt = date.timetuple()
        dmy = (tt[2], tt[1]-1, tt[0])
        return wx.DateTimeFromDMY(*dmy)

    # Widgets
    def createControls(self):
        self.absstreamLabel = wx.StaticText(self, label="Select basevalue data:",size=(160,30))
        self.absstreamComboBox = wx.ComboBox(self, choices=self.absstreamlist,
            style=wx.CB_DROPDOWN, value=self.absstreamlist[-1],size=(160,-1))

        #self.fitlistLabel = wx.StaticText(self, label="Adoption parameter:",size=(160,30))
        # RadioButton with fitting list (eventually updated from DB)
        #self.fitlistRadioBox = wx.RadioBox(self, label="Adoption parameter:",
        #             choices=self.fitlist, majorDimension=len(fitlist), style=wx.RA_SPECIFY_COLS)
        

        self.parameterLabel = wx.StaticText(self, label="Fit parameter:",size=(160,30))
        self.parameterTextCtrl = wx.TextCtrl(self, value=self.parameterstring,size=(300,90),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.parameterButton = wx.Button(self, label='Change fit ...',size=(160,30))

        self.okButton = wx.Button(self, wx.ID_OK, label='Adopt baseline',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

        if PLATFORM.startswith('linux'):
            self.parameterTextCtrl.Disable()
        #self.funcLabel = wx.StaticText(self, label="Fit function:")
        #self.funcComboBox = wx.ComboBox(self, choices=self.funclist,
        #    style=wx.CB_DROPDOWN, value=self.fitfunc)
        #self.knotsLabel = wx.StaticText(self, label="Knots [e.g. 0.5  (0..1)] (spline only):")
        #self.knotsTextCtrl = wx.TextCtrl(self, value=self.fitknots)
        #self.degreeLabel = wx.StaticText(self, label="Degree [e.g. 1, 2, 345, etc.] (polynomial only):")
        #self.degreeTextCtrl = wx.TextCtrl(self, value=self.fitdegree)

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.absstreamLabel, noOptions)]
        contlst.append((self.absstreamComboBox, expandOption))
        #contlst.append((self.fitlistRadioBox, noOptions))
        contlst.append((self.parameterLabel, noOptions))
        contlst.append((self.parameterTextCtrl, expandOption))
        contlst.append((self.parameterButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.parameterButton.Bind(wx.EVT_BUTTON, self.OnParameter)

    def OnParameter(self, e):
        # open fit dlg
        idx = int(self.absstreamComboBox.GetValue().split(':')[0])

        dlg = AnalysisFitDialog(None, title='Analysis: Fit parameter', options=self.options, stream = self.plotstream, shownkeylist=self.shownkeylist, keylist=self.keylist)
        startdate=self.dictlst[idx].get('startdate')
        enddate=self.dictlst[idx].get('enddate')
        starttime = num2date(startdate).strftime('%X')
        endtime = num2date(enddate).strftime('%X')
        dlg.startFitDatePicker.SetValue(self._pydate2wxdate(num2date(startdate)))
        dlg.endFitDatePicker.SetValue(self._pydate2wxdate(num2date(enddate)))
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

            if fitfunc.startswith('poly'):
                fitfunc = 'poly'
            self.options['fitfunction'] = fitfunc

            #self.menu_p.rep_page.logMsg('Fitting base values with %s, %s, %s' % (fitfunc, knots, degree))
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

            self.parameterstring = "Adopted Baseline 1: \nStarttime: {}, Function: {}, Knotstep: {}, Degree: {}, Endtime: {}\n".format(starttime,self.options.get('fitfunction',''),self.options.get('fitknotstep',''),self.options.get('fitdegree',''),endtime)
            self.parameterTextCtrl.SetValue(self.parameterstring)
        dlg.Destroy()

class AnalysisFlagsDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """
    def __init__(self, parent, title, stats, flaglist, stream):
        super(AnalysisFlagsDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.stats = stats
        self.fllist = flaglist
        self.plotstream = stream
        self.newfllist = []
        self.mod = False
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.statsLabel = wx.StaticText(self,label="Flagging statistics")
        self.statsTextCtrl = wx.TextCtrl(self,value=self.stats,size=(400,300),style=wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)
        self.modifyButton = wx.Button(self, label='Modify Flags')
        self.okButton = wx.Button(self, wx.ID_OK, label='OK')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.statsLabel, noOptions))
        contlst.append((self.statsTextCtrl, expandOption))
        contlst.append((self.modifyButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.modifyButton.Bind(wx.EVT_BUTTON, self.OnModify)

    def OnModify(self, e):
        # open modification dlg
        dlg = AnalysisFlagmodDialog(None, title='Analysis: Modify flags')
        if dlg.ShowModal() == wx.ID_OK:
            select = dlg.selectComboBox.GetValue()
            parameter = dlg.parameterComboBox.GetValue()
            value = dlg.valueTextCtrl.GetValue()
            newvalue = dlg.newvalueTextCtrl.GetValue()
            self.newfllist = self.plotstream.flaglistmod(mode=select, flaglist=self.fllist, parameter=parameter, value=value, newvalue=newvalue) #, starttime=None, endtime=None)
            self.stats = self.plotstream.flagliststats(self.newfllist,intensive=True, output='string')
            self.mod = True
            self.statsTextCtrl.SetValue(self.stats)
        dlg.Destroy()

class AnalysisFlagmodDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """
    def __init__(self, parent, title):
        super(AnalysisFlagmodDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.select = ['select','replace','delete']
        self.parameter = ['key', 'sensorid', 'flagnumber', 'comment']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.selectLabel = wx.StaticText(self,label="modification type")
        self.parameterLabel = wx.StaticText(self,label="flag parameter")
        self.valueLabel = wx.StaticText(self,label="value")
        self.newvalueLabel = wx.StaticText(self,label="new value")
        self.starttimeLabel = wx.StaticText(self,label="start time")
        self.endtimeLabel = wx.StaticText(self,label="end time")
        self.selectComboBox = wx.ComboBox(self, choices=self.select,
                 style=wx.CB_DROPDOWN, value=self.select[0],size=(160,-1))
        self.parameterComboBox = wx.ComboBox(self, choices=self.parameter,
                 style=wx.CB_DROPDOWN, value=self.parameter[0],size=(160,-1))
        self.valueTextCtrl = wx.TextCtrl(self,value="",size=(160,-1))
        self.newvalueTextCtrl = wx.TextCtrl(self,value="",size=(160,-1))
        self.starttimeTextCtrl = wx.TextCtrl(self,value="coming soon",size=(160,-1),style=wx.TE_READONLY)
        self.endtimeTextCtrl = wx.TextCtrl(self,value="coming soon",size=(160,-1),style=wx.TE_READONLY)
        self.okButton = wx.Button(self, wx.ID_OK, label='OK')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.selectLabel, noOptions))
        contlst.append((self.parameterLabel, noOptions))
        contlst.append((self.valueLabel, noOptions))
        contlst.append((self.newvalueLabel, noOptions))
        contlst.append((self.starttimeLabel, noOptions))
        contlst.append((self.endtimeLabel, noOptions))
        contlst.append((self.selectComboBox, noOptions))
        contlst.append((self.parameterComboBox, noOptions))
        contlst.append((self.valueTextCtrl, expandOption))
        contlst.append((self.newvalueTextCtrl, expandOption))
        contlst.append((self.starttimeTextCtrl, expandOption))
        contlst.append((self.endtimeTextCtrl, expandOption))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 6
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


# ###################################################
#    DI page
# ###################################################


class LoadDIDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, dirname):
        super(LoadDIDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.pathlist = []
        self.dirname = dirname
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.loadFileButton = wx.Button(self,-1,"Select File(s)",size=(160,30))
        self.loadDBButton = wx.Button(self,-1,"Use DB Table",size=(160,30))
        self.loadRemoteButton = wx.Button(self,-1,"Get from Remote",size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        self.loadDBButton.Disable()
        self.loadRemoteButton.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[]
        contlst.append((self.loadFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.loadDBButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.loadRemoteButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnLoadDIFiles)
        self.loadDBButton.Bind(wx.EVT_BUTTON, self.OnLoadDIDB)
        self.loadRemoteButton.Bind(wx.EVT_BUTTON, self.OnLoadDIRemote)

    def OnLoadDIFiles(self,e):
        self.difiledirname = ''
        stream = DataStream()
        dlg = wx.FileDialog(self, "Choose file(s)", self.dirname, "", "*.*", wx.MULTIPLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.pathlist = dlg.GetPaths()
        dlg.Destroy()
        self.Close(True)

    def OnLoadDIDB(self,e):
        #self.dirname = ''
        stream = DataStream()
        dlg = wx.FileDialog(self, "Choose a file", self.dirname, "", "*.*", wx.MULTIPLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.pathlist = dlg.GetPaths()
        dlg.Destroy()
        self.Close(True)

    def OnLoadDIRemote(self,e):
        self.dirname = ''
        stream = DataStream()
        dlg = wx.FileDialog(self, "Choose a file", self.dirname, "", "*.*", wx.MULTIPLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.pathlist = dlg.GetPaths()
        dlg.Destroy()
        self.Close(True)


class DefineVarioDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title):
        super(DefineVarioDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.path = ''
        self.variopath = ''
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.sourceLabel = wx.StaticText(self, label="Choose Variometer source:")
        self.loadfileLabel = wx.StaticText(self, label="1) Access local files:")
        self.loadFileButton = wx.Button(self,-1,"Get Path",size=(160,30))
        self.getdbLabel = wx.StaticText(self, label="2) Access database:")
        self.remoteLabel = wx.StaticText(self, label="3) Access remote files:")

        self.okButton = wx.Button(self, wx.ID_OK, label='Use',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.sourceLabel, noOptions)]
        contlst.append((self.loadfileLabel, noOptions))
        contlst.append((self.loadFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.getdbLabel, noOptions))
        contlst.append((self.remoteLabel, noOptions))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnDefineVario)

    def OnDefineVario(self,e):
        dialog = wx.DirDialog(None, "Choose a directory with variometer data:",self.variopath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.path = dialog.GetPath()
            self.variopath = self.path
        dialog.Destroy()


class DefineScalarDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title):
        super(DefineScalarDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.path = ''
        self.scalarpath = ''
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.sourceLabel = wx.StaticText(self, label="Choose Variometer source:")
        self.loadfileLabel = wx.StaticText(self, label="1) Access local files:")
        self.loadFileButton = wx.Button(self,-1,"Get Path",size=(160,30))
        self.getdbLabel = wx.StaticText(self, label="2) Access database:")
        self.remoteLabel = wx.StaticText(self, label="3) Access remote files:")

        self.okButton = wx.Button(self, wx.ID_OK, label='Use',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.sourceLabel, noOptions)]
        contlst.append((self.loadfileLabel, noOptions))
        contlst.append((self.loadFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.getdbLabel, noOptions))
        contlst.append((self.remoteLabel, noOptions))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnDefineScalar)


    def OnDefineScalar(self,e):
        dialog = wx.DirDialog(None, "Choose a directory with scalar data:",self.scalarpath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.path = dialog.GetPath()
            self.scalarpath = self.path
        dialog.Destroy()


class DISaveDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title):
        super(DISaveDialog, self).__init__(parent=parent,
            title=title, size=(200, 300))
        self.choice = ''
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        #self.yesButton = wx.Button(self, wx.ID_OK, label="Overwrite",size=(160,30))
        #self.noButton = wx.Button(self, wx.ID_OK,label="Cancel",size=(160,30))
        #self.alternativeButton = wx.Button(self, wx.ID_OK,label="Write as alternative",size=(160,30))
        self.yesButton = wx.Button(self, wx.ID_YES, label="Overwrite",size=(160,30))
        self.noButton = wx.Button(self, wx.ID_NO,label="Cancel",size=(160,30))
        self.alternativeButton = wx.Button(self, wx.ID_YES,label="Write as alternative",size=(160,30))
        self.sourceLabel = wx.StaticText(self, label="A file with similar name is already existing. You can:")

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        mainSizer = wx.BoxSizer(wx.VERTICAL)

        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[]
        contlst.append((self.noButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.alternativeButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.yesButton, dict(flag=wx.ALIGN_CENTER)))


        mainSizer.Add(self.sourceLabel, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        """
        labellst=[]
        labellst.append((self.sourceLabel, noOptions))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridlabelSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in labellst:
            gridlabelSizer.Add(control, **options)

        for control, options in \
                [(gridlabelSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)
        """

        # A GridSizer will contain the other controls:
        cols = 3
        rows = int(np.ceil(len(contlst)/float(cols)))
        rows = 2
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        self.SetSizerAndFit(mainSizer)

    def bindControls(self):
        self.noButton.Bind(wx.EVT_BUTTON, self.OnNo)
        self.yesButton.Bind(wx.EVT_BUTTON, self.OnYes)
        self.alternativeButton.Bind(wx.EVT_BUTTON, self.OnAlternative)

    def OnNo(self, e):
        self.choice = 'no'
        self.Close(True)

    def OnYes(self, e):
        self.choice = 'yes'
        self.Close(True)

    def OnAlternative(self, e):
        self.choice = 'alternative'
        self.Close(True)


class DISetParameterDialog(wx.Dialog):
    """
    Dialog for Parameter selection - Di analysis
    """

    def __init__(self, parent, title):
        super(DISetParameterDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.abstypes = ['manual', 'autodif']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # single anaylsis
        # db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.azimuthLabel = wx.StaticText(self, label="Azimuth",size=(160,30))
        self.azimuthTextCtrl = wx.TextCtrl(self,value="",size=(160,30))
        self.abstypeLabel = wx.StaticText(self, label="Absolute type",size=(160,30))
        self.abstypeComboBox = wx.ComboBox(self, choices=self.abstypes,
                 style=wx.CB_DROPDOWN, value=self.abstypes[0],size=(160,-1))
        self.pierLabel = wx.StaticText(self, label="Pier",size=(160,30))
        self.pierTextCtrl = wx.TextCtrl(self, value="",size=(160,30))
        self.alphaLabel = wx.StaticText(self, label="Horizontal rotation",size=(160,30))
        self.alphaTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.deltaFLabel = wx.StaticText(self, label="Delta F",size=(160,30))
        self.deltaFTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.expDLabel = wx.StaticText(self, label="Expected D",size=(160,30))
        self.expDTextCtrl = wx.TextCtrl(self, value="2.0",size=(160,30))

        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        self.okButton = wx.Button(self, wx.ID_OK, label='OK')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.azimuthLabel, noOptions),
                 (self.abstypeLabel, noOptions),
                 (self.pierLabel, noOptions),
                 (self.azimuthTextCtrl, expandOption),
                 (self.abstypeComboBox, noOptions),
                 (self.pierTextCtrl, expandOption),
                 (self.alphaLabel, noOptions),
                 (self.deltaFLabel, noOptions),
                 (self.expDLabel, noOptions),
                 (self.alphaTextCtrl, expandOption),
                 (self.deltaFTextCtrl, expandOption),
                 (self.expDTextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 3
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class InputSheetDialog(wx.Dialog):
    """
    DESCRITPTION
        InputDialog for DI data
    """

    def __init__(self, parent, title, layout, path, defaults,cdate, db):
        style = wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        super(InputSheetDialog, self).__init__(parent=parent,
            title=title, style=style) #size=(1000, 800),

        self.path = path
        self.layout = layout
        self.defaults = defaults
        self.cdate = cdate
        self.units = ['degree','gon']

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        # Add Settings Panel
        self.panel = SettingsPanel(self, cdate, path, defaults, layout, db)
        self.panel.SetInitialSize((850, 400))
        self.mainSizer.Add(self.panel, 1, wx.EXPAND | wx.ALL, 10)
        # Add Save/Cancel Buttons
        self.createWidgets()
        # Set sizer and window size
        self.SetSizerAndFit(self.mainSizer)
        #self.mainSizer.Fit(self)

    def createWidgets(self):
        """Create and layout the widgets in the dialog"""
        btnSizer = wx.StdDialogButtonSizer()

        saveBtn = wx.Button(self, wx.ID_YES, label="Save",size=(160,30))
        saveBtn.Bind(wx.EVT_BUTTON, self.OnSave)
        btnSizer.AddButton(saveBtn)

        cancelBtn = wx.Button(self, wx.ID_NO, label='Close',size=(160,30))  # Using ID_NO as ID_CLOSE is not working with StdDialogButtonSizer
        cancelBtn.Bind(wx.EVT_BUTTON, self.OnClose)
        btnSizer.AddButton(cancelBtn)
        btnSizer.Realize()

        self.mainSizer.Add(btnSizer, 0, wx.ALL | wx.ALIGN_RIGHT, 5)

    def degminsec2deg(self, string, back='decimal'):
        """
        DESCRIPTION
            checks input string and returns either deg: or deg.
        PARAMETER
            string (string):   a deg:min:sec of deg.decimal value
            back   (float) :   either decimal or minsec
        """
        string = str(string)
        if string.find(':') > 0:
            val = 0.
            numlist = string.split(':')
            for idx, ele in enumerate(numlist):
                add = float(ele)/float(60.**idx)
                val += float(add)
        else:
            if string.find(',') > 0:
                string = string.replace(',','.')
            try:
                val = float(string)
            except:
                val = 999.0

        if not val >= -180 and not val <= 360:
            return 999.

        def decdeg2dms(dd):
            is_positive = dd >= 0
            dd = abs(dd)
            minutes,seconds = divmod(dd*3600,60)
            degrees,minutes = divmod(minutes,60)
            degrees = degrees if is_positive else -degrees
            return [degrees,minutes,seconds]

        if back == 'minsec':
            return ":".join(decdeg2dms(val))
        else:
            return str(val)


    def OnSave(self, event):
        opstring = []
        saving = True
        angleerror = 0
        timeerror = 0

        def testangle(angle, primary, prevangle=None, anglecount=0):
            mireproblem = False
            tangle = 999
            if angle in ["0.0000 or 00:00:00.0", ""]:
                if primary == 1:
                    mireproblem = True
                if primary == 0:
                    #angle = self.degminsec2deg(prevangle)
                    try:
                        angle = self.panel._degminsec2deg(prevangle)
                    except:
                        mireproblem = True
                        angle = 999
            else:
                #angle = self.degminsec2deg(angle)
                try:
                    angle = self.panel._degminsec2deg(angle)
                except:
                    mireproblem = True
                    angle = 999
            try:
                tangle = float(angle)
            except:
                mireproblem = True
            if tangle == 999:
                mireproblem = True
            if tangle > 400:
                mireproblem = True
            if tangle < -400:
                mireproblem = True

            if mireproblem:
                #if anglecount == 0:
                #    checkdlg = wx.MessageDialog(self, "Provided angles:\n"
                #            "Please check your input data\n",
                #            "Angle checker", wx.OK|wx.ICON_INFORMATION)
                #    checkdlg.ShowModal()
                return 999., 1
            return angle, 0


        def testtime(time, datestring, primary=1, prevtime=None, timecount=0):
            timeproblem = False
            if time in ["00:00:00", ""]:
                if primary == 1:
                    timeproblem = True
                if primary == 0:
                    time = prevtime
            else:
                #2010-06-11_12:03:00
                time = datestring + '_' + time

            try:
                tt = datetime.strptime(time, "%Y-%m-%d_%H:%M:%S")
            except:
                timeproblem = True

            if timeproblem:
                if timecount == 0:
                     checkdlg = wx.MessageDialog(self, "Provided times:\n"
                            "Input data ' {} ' could not be interpreted\n".format(time),
                            "Time checker", wx.OK|wx.ICON_INFORMATION)
                     checkdlg.ShowModal()
                return "2233-12-12_13:21:23", 1

            return datetime.strftime(datetime.strptime(time, "%Y-%m-%d_%H:%M:%S"),"%Y-%m-%d_%H:%M:%S"), 0
            #return time

        # Get header
        opstring.append("# MagPy Absolutes")
        unit = self.panel.UnitComboBox.GetValue()
        date = self.panel.DatePicker.GetValue()
        observer = self.panel.ObserverTextCtrl.GetValue()
        iagacode= self.panel.CodeTextCtrl.GetValue()
        theo = self.panel.TheoTextCtrl.GetValue()
        flux = self.panel.FluxTextCtrl.GetValue()
        azimuth = self.panel.AzimuthTextCtrl.GetValue()
        pillar = self.panel.PillarTextCtrl.GetValue()
        temp = self.panel.TempTextCtrl.GetValue()
        finst = self.panel.FInstTextCtrl.GetValue()
        comm = self.panel.CommentTextCtrl.GetValue()

        try:
            self.panel.PillarTextCtrl.SetBackgroundColour(wx.NullColor)
            self.panel.CodeTextCtrl.SetBackgroundColour(wx.NullColor)
            self.panel.AzimuthTextCtrl.SetBackgroundColour(wx.NullColor)
        except: # for MacOs
            self.panel.PillarTextCtrl.SetBackgroundColour(wx.WHITE)
            self.panel.CodeTextCtrl.SetBackgroundColour(wx.WHITE)
            self.panel.AzimuthTextCtrl.SetBackgroundColour(wx.WHITE)

        fluxorient = self.panel.ressignRadioBox.GetSelection()
        if fluxorient == 0:
            ressign = 1
        else:
            ressign = -1

        if theo == "type_serial_version":
            theo = ''
        if flux == "type_serial_version":
            theo = ''
        if finst == "type_serial_version":
            finst = ''

        opstring.append("# Abs-Observer: {}".format(observer))
        opstring.append("# Abs-Theodolite: {}".format(theo))
        opstring.append("# Abs-TheoUnit: {}".format(unit[:3]))
        opstring.append("# Abs-FGSensor: {}".format(flux))
        azi,err = testangle(azimuth,1)
        if not err == 0:
            self.panel.AzimuthTextCtrl.SetBackgroundColour(wx.RED)
        opstring.append("# Abs-AzimuthMark: {}".format(azi))
        opstring.append("# Abs-Pillar: {}".format(pillar))
        opstring.append("# Abs-Scalar: {}".format(finst))
        opstring.append("# Abs-InputDate: {}".format(datetime.strftime(datetime.utcnow(),"%Y-%m-%d")))
        opstring.append("# Abs-Temperature: {}".format(temp))
        opstring.append("# Abs-Notes: {}".format(comm.replace('\n',' ')))

        # Get Mire
        opstring.append("Miren:")
        amu1 = self.panel.AmireUp1TextCtrl.GetValue()
        amu2 = self.panel.AmireUp2TextCtrl.GetValue()
        amd1 = self.panel.AmireDown1TextCtrl.GetValue()
        amd2 = self.panel.AmireDown2TextCtrl.GetValue()
        bmu1 = self.panel.BmireUp1TextCtrl.GetValue()
        bmu2 = self.panel.BmireUp2TextCtrl.GetValue()
        bmd1 = self.panel.BmireDown1TextCtrl.GetValue()
        bmd2 = self.panel.BmireDown2TextCtrl.GetValue()
        amu1,err = testangle(amu1,1,anglecount=angleerror)
        if err == 0:
            self.panel.AmireUp1TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.AmireUp1TextCtrl.SetForegroundColour(wx.RED)
        angleerror += err
        amu2,err = testangle(amu2,0,amu1,anglecount=angleerror)
        if err == 0:
            self.panel.AmireUp2TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.AmireUp2TextCtrl.SetForegroundColour(wx.RED)
        angleerror += err
        amd1,err = testangle(amd1,1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.AmireDown1TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.AmireDown1TextCtrl.SetForegroundColour(wx.RED)
        amd2,err = testangle(amd2,0, amd1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.AmireDown2TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.AmireDown2TextCtrl.SetForegroundColour(wx.RED)
        bmu1,err = testangle(bmu1,1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.BmireUp1TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.BmireUp1TextCtrl.SetForegroundColour(wx.RED)
        bmu2,err = testangle(bmu2,0, bmu1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.BmireUp2TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.BmireUp2TextCtrl.SetForegroundColour(wx.RED)
        bmd1,err = testangle(bmd1,1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.BmireDown1TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.BmireDown1TextCtrl.SetForegroundColour(wx.RED)
        bmd2,err = testangle(bmd2,0, bmd1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.BmireDown2TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.BmireDown2TextCtrl.SetForegroundColour(wx.RED)
        mline = "{}  {}  {}  {}  {}  {}  {}  {}".format(amu1, amu2, amd1, amd2, bmu1, bmu2, bmd1, bmd2)
        opstring.append("{}".format(mline))

        # Get Horizontals
        ymd = map(int, date.FormatISODate().split('-'))
        datestring = datetime.strftime(datetime(*ymd),"%Y-%m-%d")

        opstring.append("Positions:")
        timelist = []
        for comp in ['EU','WU','ED','WD','NU','SD','ND','SU']:
            val = []
            anglelist = []
            for i in ['1','2']:
                if not comp in ['EU','WU','ED','WD']:
                    ellst = ['Time','GC','Angle','Residual']
                else:
                    ellst = ['Time','Angle','GC','Residual']
                for col in ellst:
                    na = comp+i
                    val.append(eval('self.panel.'+na+col+'TextCtrl.GetValue()'))
                    if col[0] == 'A' and i == '1':
                        val[-1], err = testangle(val[-1],1,anglecount=angleerror)
                        angleerror += err
                        if err == 0:
                            eval('self.panel.{}{}TextCtrl.SetForegroundColour(wx.BLACK)'.format(na,col))
                        else:
                            eval('self.panel.{}{}TextCtrl.SetForegroundColour(wx.RED)'.format(na,col))
                        anglelist.append(val[-1])
                    elif col[0] == 'A' and i == '2':
                        val[-1], err = testangle(val[-1],0,val[-5],anglecount=angleerror)
                        angleerror += err
                        if err == 0:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.BLACK)')
                        else:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.RED)')
                        anglelist.append(val[-1])
                    if col[0] == 'G':
                        if len(val[-1].split('/')) > 1:
                            val[-1] = [el for el in val[-1].split('/') if el.endswith(unit[:3])][0].replace(unit[:3],'')
                        else:
                            val[-1] = val[-1]
                    if col[0] == 'R':
                        val[-1] = ressign*float(val[-1].replace(',','.'))
                    if col[0] == 'T' and i == '1':
                        val[-1], terr = testtime(val[-1], datestring,timecount=timeerror)
                        timeerror += terr
                        if terr == 0:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.BLACK)')
                        else:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.RED)')
                        timelist.append(val[-1])
                    elif col[0] == 'T' and i == '2':
                        val[-1], terr = testtime(val[-1], datestring, 0, val[-5],timecount=timeerror)
                        timeerror += terr
                        if terr == 0:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.BLACK)')
                        else:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.RED)')
                        timelist.append(val[-1])
            l1 = "  ".join(map(str, val[:4]))
            l2 = "  ".join(map(str, val[4:]))
            opstring.append("{}".format(l1))
            opstring.append("{}".format(l2))


        for col in ellst:
            val.append(eval('self.panel.SC'+col+'TextCtrl.GetValue()'))
            if col[0] == 'A':
                val[-1], err = testangle(val[-1],0,val[-5], anglecount=angleerror)
                angleerror += err
                if err == 0:
                    eval('self.panel.SC{}TextCtrl.SetForegroundColour(wx.BLACK)'.format(col))
                else:
                    eval('self.panel.SC{}TextCtrl.SetForegroundColour(wx.RED)'.format(col))
                anglelist.append(val[-1])
            if col[0] == 'G':
                if len(val[-1].split('/')) > 1:
                   val[-1] = [el for el in val[-1].split('/') if el.endswith(unit[:3])][0].replace(unit[:3],'')
                else:
                   val[-1] = val[-1]
            if col[0] == 'R':
                val[-1] = float(val[-1].replace(',','.'))
            if col[0] == 'T':
                val[-1],terr = testtime(val[-1], datestring, 0, val[-5],timecount=timeerror)
                timeerror += terr
                if terr == 0:
                    eval('self.panel.SC{}TextCtrl.SetForegroundColour(wx.BLACK)'.format(col))
                else:
                    eval('self.panel.SC{}TextCtrl.SetForegroundColour(wx.RED)'.format(col))
                timelist.append(val[-1])
        l1 = "  ".join(map(str, val[-4:]))
        #print ("Values", l1, val)
        opstring.append("{}".format(l1))

        # Get F vals
        opstring.append("PPM:")
        fvals = self.panel.FValsTextCtrl.GetValue()
        if not fvals == '' and not fvals.startswith('time,value'):
            fvals = [el.split(',') for el in fvals.split('\n')]
            fbase = self.panel.FBaseTextCtrl.GetValue()

            if not fbase == '':
                try:
                    fbase = float(fbase.replace(',','.'))
                except:
                    fbase = 0.0
            else:
                fbase = 0.0

            if len(fvals) > 0 and len(fvals[0]) == 2:
                for el in fvals:
                    if len(el) > 1 and not el in [' ','  '] and not el[0] == '00:00:00':
                        t = testtime(el[0], datestring)
                        f = float(el[1])+fbase
                        fline = "{} {}".format(t[0],f)
                        opstring.append("{}".format(fline))
            else:
                checkdlg = wx.MessageDialog(self, "F values:\n"
                        "It seems as if you provided F values.\nThe format, however, could not be interpreted.\nInputs should look like: 12:12:00, 23.5\n",
                        "F data checker", wx.OK|wx.ICON_INFORMATION)
                checkdlg.ShowModal()

        opstring.append("Result:")  # For historic reasons: eventually add results again


        # Check block
        if 999 in [amu1, amu2, amd1, amd2, bmu1, bmu2, bmd1, bmd2]:
            saving = False
            #if anglecount == 0:
            checkdlg = wx.MessageDialog(self, "Azimuth measurements:\n"
                            "Error within angles. Please check your input data\n",
                            "Angle checker", wx.OK|wx.ICON_INFORMATION)
            checkdlg.ShowModal()
        if 999 in anglelist:
            saving = False
            #if anglecount == 0:
            checkdlg = wx.MessageDialog(self, "DI measurements:\n"
                            "Error within angles. Please check your input data\n",
                            "Angle checker", wx.OK|wx.ICON_INFORMATION)
            checkdlg.ShowModal()
        if "2233-12-12_13:21:23" in timelist:
            saving = False
            #checkdlg = wx.MessageDialog(self, "Provided times:\n"
            #                "Input data ' {} ' could not be interpreted\n".format(time),
            #                "Time checker", wx.OK|wx.ICON_INFORMATION)
            #checkdlg.ShowModal()
        if pillar == '' or iagacode=='' or azimuth=='':
            saving = False
            checkdlg = wx.MessageDialog(self, "Meta data:\n"
                        "You need to provide a pillar name and a station code\n",
                        "Meta data checker", wx.OK|wx.ICON_INFORMATION)
            checkdlg.ShowModal()
            if pillar=='':
                self.panel.PillarTextCtrl.SetBackgroundColour(wx.RED)
            if iagacode=='':
                self.panel.CodeTextCtrl.SetBackgroundColour(wx.RED)
            if azimuth=='':
                self.panel.AzimuthTextCtrl.SetBackgroundColour(wx.RED)


        filename = timelist[0].replace(':','-')+'_'+pillar+'_'+iagacode+'.txt'

        # Write Block
        if saving:
            opstring = [unicode(el+'\n', "utf-8") for el in opstring]
            didirname = os.path.expanduser("~")
            dialog = wx.DirDialog(None, "Choose directory to write data:",didirname,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
            if dialog.ShowModal() == wx.ID_OK:
                didirname = dialog.GetPath() # modify self.dirname
                out = os.path.join(didirname,filename)
                # Check box if file existing, overwrite cancel
                writefile = True
                if os.path.isfile(out):
                    dlg = DISaveDialog(None, title='File Existing')
                    dlg.ShowModal()
                    if dlg.choice == 'alternative':
                        # creating a new file name with one second plus
                        filealreadyexisting = True
                        newtime0 = timelist[0]
                        while filealreadyexisting:
                            newtime0 = datetime.strftime((datetime.strptime(newtime0,'%Y-%m-%d_%H:%M:%S')+timedelta(seconds=1)),'%Y-%m-%d_%H:%M:%S')
                            filename = newtime0.replace(':','-')+'_'+pillar+'_'+iagacode+'.txt'
                            out = os.path.join(didirname,filename)
                            if not os.path.isfile(out):
                                filealreadyexisting = False
                        writefile = True
                    elif  dlg.choice == 'yes':
                        writefile = True
                    else:
                        writefile = False
                    dlg.Destroy()
                if writefile:
                    fo = open(out, "w+")
                    #print ("Name of the file: ", fo.name)
                    fo.writelines( opstring )
                    fo.close()
                    dlg = wx.MessageDialog(self, "Data set {} successfully written.\n".format(filename),
                                    "File written", wx.OK|wx.ICON_INFORMATION)
                    dlg.ShowModal()
            dialog.Destroy()

    def OnClose(self, event):
        closedlg = wx.MessageDialog(self, "Unsaved data will be lost\n"
                        "Continue?\n".format(time),
                        "Closing DI sheet", wx.YES_NO|wx.ICON_INFORMATION)

        if closedlg.ShowModal() == wx.ID_YES:
            closedlg.Destroy()
            self.Close(True)


class SettingsPanel(scrolledpanel.ScrolledPanel):
    def __init__(self, parent, cdate, path, defaults, layout, db):
        scrolledpanel.ScrolledPanel.__init__(self, parent, -1, size=(-1, -1))  #size=(950, 750)
        #self.ShowFullScreen(True)
        self.cdate = cdate
        self.path = path
        self.db = db
        self.layout = layout
        self.defaults = defaults
        self.units = ['degree','gon']
        self.choices = ['decimal', 'dms']
        self.ressign = ['inline','opposite']

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        self.createWidgets()
        self.SetSizer(self.mainSizer)
        self.mainSizer.Fit(self)
        self.SetupScrolling()
        self.bindControls()

    def createWidgets(self):
        """Create and layout the widgets in the panel"""
        # ##### Header Block (fix - 5 columns)
        # - Load line
        self.loadButton = wx.Button(self,-1,"Open DI data",size=(160,30))
        self.angleRadioBox = wx.RadioBox(self, label="Display angle as:",
                     choices=self.choices, majorDimension=2, style=wx.RA_SPECIFY_COLS)

        # - Header
        self.HeadLabel = wx.StaticText(self, label="Meta data:",size=(160,30))
        self.DateLabel = wx.StaticText(self, label="Date:",size=(160,30))
        self.DatePicker = wx.DatePickerCtrl(self, dt=self.cdate,size=(160,30))
        self.ObserverLabel = wx.StaticText(self, label="Observer:",size=(160,30))
        self.ObserverTextCtrl = wx.TextCtrl(self, value="Max",size=(160,30))
        self.CodeLabel = wx.StaticText(self, label="IAGA code:",size=(160,30))
        self.CodeTextCtrl = wx.TextCtrl(self, value="",size=(160,30))
        self.TheoLabel = wx.StaticText(self, label="Theodolite:",size=(160,30))
        self.TheoTextCtrl = wx.TextCtrl(self, value="type_serial_version",size=(160,30))
        self.FluxLabel = wx.StaticText(self, label="Fluxgate:",size=(160,30))
        self.FluxTextCtrl = wx.TextCtrl(self, value="type_serial_version",size=(160,30))
        self.AzimuthLabel = wx.StaticText(self, label="Azimuth:",size=(160,30))
        self.AzimuthTextCtrl = wx.TextCtrl(self, value="",size=(160,30))
        self.PillarLabel = wx.StaticText(self, label="Pier:",size=(160,30))
        self.PillarTextCtrl = wx.TextCtrl(self, value=self.defaults['dipier'],size=(160,-1))
        self.UnitLabel = wx.StaticText(self, label="Select Units:",size=(160,30))
        self.UnitComboBox = wx.ComboBox(self, choices=self.units,
            style=wx.CB_DROPDOWN, value=self.units[0],size=(160,-1))
        self.TempLabel = wx.StaticText(self, label="Temperature [deg C]:",size=(160,30))
        self.TempTextCtrl = wx.TextCtrl(self, value="",size=(160,-1))
        self.CommentLabel = wx.StaticText(self, label="Optional notes:",size=(160,30))
        self.CommentTextCtrl = wx.TextCtrl(self, value="",size=(160,80), style = wx.TE_MULTILINE)
        self.ressignRadioBox = wx.RadioBox(self, label="Fluxgate orientation:",
                     choices=self.ressign, majorDimension=2, style=wx.RA_SPECIFY_COLS)

        # - Mire A
        self.AmireLabel = wx.StaticText(self, label="Azimuth:",size=(160,30))
        self.AmireDownLabel = wx.StaticText(self, label="Sensor Down:",size=(160,30))
        self.AmireDown1TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.AmireDown2TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.AmireUpLabel = wx.StaticText(self, label="Sensor Up:",size=(160,30))
        self.AmireUp1TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.AmireUp2TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))

        # - Horizonatl Block
        self.HorizontalLabel = wx.StaticText(self, label="Horizontal:",size=(160,30))
        self.TimeLabel = wx.StaticText(self, label="Time:",size=(160,30))
        self.HAngleLabel = wx.StaticText(self, label="Hor. Angle:",size=(160,30))
        self.VAngleLabel = wx.StaticText(self, label="Ver. Angle:",size=(160,30))
        self.ResidualLabel = wx.StaticText(self, label="Residual:",size=(160,30))
        self.EULabel = wx.StaticText(self, label="East(Sensor Up)",size=(160,30))
        self.EU1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.EU1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.EU1GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.EU1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.EU2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.EU2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.EU2GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.EU2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.WULabel = wx.StaticText(self, label="West(Sensor Up)",size=(160,30))
        self.WU1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.WU1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.WU1GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.WU1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.WU2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.WU2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.WU2GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.WU2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.EDLabel = wx.StaticText(self, label="East(Sensor Down)",size=(160,30))
        self.ED1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.ED1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.ED1GCTextCtrl = wx.TextCtrl(self, value="270deg/300gon",size=(160,30))
        self.ED1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.ED2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.ED2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.ED2GCTextCtrl = wx.TextCtrl(self, value="270deg/300gon",size=(160,30))
        self.ED2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.WDLabel = wx.StaticText(self, label="West(Sensor Down)",size=(160,30))
        self.WD1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.WD1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.WD1GCTextCtrl = wx.TextCtrl(self, value="270deg/300gon",size=(160,30))
        self.WD1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.WD2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.WD2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.WD2GCTextCtrl = wx.TextCtrl(self, value="270deg/300gon",size=(160,30))
        self.WD2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.EU1GCTextCtrl.Disable()
        self.EU2GCTextCtrl.Disable()
        self.ED1GCTextCtrl.Disable()
        self.ED2GCTextCtrl.Disable()
        self.WU1GCTextCtrl.Disable()
        self.WU2GCTextCtrl.Disable()
        self.WD1GCTextCtrl.Disable()
        self.WD2GCTextCtrl.Disable()


        # - Mire B
        self.BmireLabel = wx.StaticText(self, label="Azimuth:",size=(160,30))
        self.BmireDownLabel = wx.StaticText(self, label="Sensor Down:",size=(160,30))
        self.BmireDown1TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.BmireDown2TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.BmireUpLabel = wx.StaticText(self, label="Sensor Up:",size=(160,30))
        self.BmireUp1TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.BmireUp2TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.calcButton = wx.Button(self,-1,"Check horiz. angle",size=(160,30))

        # - Vertical Block
        self.VerticalLabel = wx.StaticText(self, label="Vertical:",size=(160,30))
        self.NULabel = wx.StaticText(self, label="North(Sensor Up)",size=(160,30))
        self.NU1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.NU1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.NU1GCTextCtrl = wx.TextCtrl(self, value="0deg/0gon",size=(160,30))
        self.NU1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.NU2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.NU2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.NU2GCTextCtrl = wx.TextCtrl(self, value="0deg/0gon",size=(160,30))
        self.NU2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.SDLabel = wx.StaticText(self, label="South(Sensor Down)",size=(160,30))
        self.SD1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.SD1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.SD1GCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,30))
        self.SD1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.SD2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.SD2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.SD2GCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,30))
        self.SD2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.NDLabel = wx.StaticText(self, label="North(Sensor Down)",size=(160,30))
        self.ND1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.ND1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.ND1GCTextCtrl = wx.TextCtrl(self, value="0deg/0gon",size=(160,30))
        self.ND1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.ND2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.ND2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.ND2GCTextCtrl = wx.TextCtrl(self, value="0deg/0gon",size=(160,30))
        self.ND2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.SULabel = wx.StaticText(self, label="South(Sensor Up)",size=(160,30))
        self.SU1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.SU1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.SU1GCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,30))
        self.SU1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.SU2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.SU2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.SU2GCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,30))
        self.SU2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.SCLabel = wx.StaticText(self, label="Scale Test (SSU + 0.2 gon)",size=(160,30))
        self.SCTimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.SCAngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.SCGCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,30))
        self.SCResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))

        self.NU1GCTextCtrl.Disable()
        self.NU2GCTextCtrl.Disable()
        self.ND1GCTextCtrl.Disable()
        self.ND2GCTextCtrl.Disable()
        self.SU1GCTextCtrl.Disable()
        self.SU2GCTextCtrl.Disable()
        self.SD1GCTextCtrl.Disable()
        self.SD2GCTextCtrl.Disable()
        self.SCGCTextCtrl.Disable()

        if not self.layout['double'] == 'False':
            #self.SD2TimeTextCtrl.Hide()
            #self.SD2AngleTextCtrl.Hide()
            #self.SD2GCTextCtrl.Hide()
            #self.SD2ResidualTextCtrl.Hide()
            pass

        # Add scale check

        self.FLabel = wx.StaticText(self, label="F:",size=(160,30))
        self.FInstLabel = wx.StaticText(self, label="F instrument:",size=(160,30))
        self.FInstTextCtrl = wx.TextCtrl(self, value="type_serial_version",size=(160,30))
        self.FBaseLabel = wx.StaticText(self, label="F base (nT):",size=(160,30))
        self.FBaseTextCtrl = wx.TextCtrl(self, value="48000",size=(160,30))
        self.FValsLabel = wx.StaticText(self, label="Time,Value(+Base):",size=(160,30))
        self.FValsTextCtrl = wx.TextCtrl(self, value="time,value",size=(160,100), style = wx.TE_MULTILINE)
        self.FLoadFromFileButton = wx.Button(self, wx.ID_YES, label="Load F Data",size=(160,30))


        f = self.VerticalLabel.GetFont()
        newf = wx.Font(14, wx.DECORATIVE, wx.ITALIC, wx.BOLD)
        self.VerticalLabel.SetFont(newf)
        self.HorizontalLabel.SetFont(newf)
        self.AmireLabel.SetFont(newf)
        self.BmireLabel.SetFont(newf)
        self.HeadLabel.SetFont(newf)
        self.FLabel.SetFont(newf)


        #self.ln = wx.StaticLine(self, -1, style=wx.LI_HORIZONTAL,size=(800,10))
        #self.okButton = wx.Button(self, wx.ID_OK, label='Use')
        #self.closeButton = wx.Button(self, label='Cancel')

        #settingsSizer = wx.GridSizer(rows=0, cols=5, hgap=5, vgap=0)

        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((160, 0), noOptions)

        # Load elements
        #boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        contlst=[emptySpace]
        contlst.append(emptySpace)
        contlst.append((self.loadButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append((self.angleRadioBox, noOptions))

        # Header elements
        contlst.append((self.HeadLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.DateLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.ObserverLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.CodeLabel, noOptions))
        contlst.append((self.DatePicker, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.ObserverTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.CodeTextCtrl, expandOption))
        contlst.append((self.TheoLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.FluxLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.AzimuthLabel, noOptions))
        contlst.append((self.TheoTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.FluxTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.AzimuthTextCtrl, expandOption))
        contlst.append((self.PillarLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.UnitLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.TempLabel, noOptions))
        contlst.append((self.PillarTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.UnitComboBox, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.TempTextCtrl, expandOption))
        contlst.append((self.CommentLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.CommentTextCtrl, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.ressignRadioBox, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)

        # Mire elements
        contlst.append((self.AmireLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        blMU = []
        blMU.append((self.AmireUpLabel, noOptions))
        blMU.append((self.AmireUp1TextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blMU.append((self.AmireUp2TextCtrl, expandOption))
        else:
            blMU.append(emptySpace)
        blMU.append(emptySpace)
        blMU.append(emptySpace)
        blMD = []
        blMD.append((self.AmireDownLabel, noOptions))
        blMD.append((self.AmireDown1TextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blMD.append((self.AmireDown2TextCtrl, expandOption))
        else:
            blMD.append(emptySpace)
        blMD.append(emptySpace)
        blMD.append(emptySpace)
        for el in self.layout['order'][0:2]:
            contlst.extend(eval('bl'+str(el)))

        miorder = self.layout['order'][0:2]
        if miorder[0] == 'MU':  # default is MD, MU
            self.AmireUp2TextCtrl.MoveBeforeInTabOrder(self.AmireDown1TextCtrl)
            self.AmireUp1TextCtrl.MoveBeforeInTabOrder(self.AmireUp2TextCtrl)

        blEU = []
        blEU.append((self.EULabel, noOptions))
        blEU.append((self.EU1TimeTextCtrl, expandOption))
        blEU.append((self.EU1AngleTextCtrl, expandOption))
        blEU.append((self.EU1GCTextCtrl, expandOption))
        blEU.append((self.EU1ResidualTextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blEU.append(emptySpace)
            blEU.append((self.EU2TimeTextCtrl, expandOption))
            blEU.append((self.EU2AngleTextCtrl, expandOption))
            blEU.append((self.EU2GCTextCtrl, expandOption))
            blEU.append((self.EU2ResidualTextCtrl, expandOption))
        blWU = []
        blWU.append((self.WULabel, noOptions))
        blWU.append((self.WU1TimeTextCtrl, expandOption))
        blWU.append((self.WU1AngleTextCtrl, expandOption))
        blWU.append((self.WU1GCTextCtrl, expandOption))
        blWU.append((self.WU1ResidualTextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blWU.append(emptySpace)
            blWU.append((self.WU2TimeTextCtrl, expandOption))
            blWU.append((self.WU2AngleTextCtrl, expandOption))
            blWU.append((self.WU2GCTextCtrl, expandOption))
            blWU.append((self.WU2ResidualTextCtrl, expandOption))
        blED = []
        blED.append((self.EDLabel, noOptions))
        blED.append((self.ED1TimeTextCtrl, expandOption))
        blED.append((self.ED1AngleTextCtrl, expandOption))
        blED.append((self.ED1GCTextCtrl, expandOption))
        blED.append((self.ED1ResidualTextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blED.append(emptySpace)
            blED.append((self.ED2TimeTextCtrl, expandOption))
            blED.append((self.ED2AngleTextCtrl, expandOption))
            blED.append((self.ED2GCTextCtrl, expandOption))
            blED.append((self.ED2ResidualTextCtrl, expandOption))
        blWD = []
        blWD.append((self.WDLabel, noOptions))
        blWD.append((self.WD1TimeTextCtrl, expandOption))
        blWD.append((self.WD1AngleTextCtrl, expandOption))
        blWD.append((self.WD1GCTextCtrl, expandOption))
        blWD.append((self.WD1ResidualTextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blWD.append(emptySpace)
            blWD.append((self.WD2TimeTextCtrl, expandOption))
            blWD.append((self.WD2AngleTextCtrl, expandOption))
            blWD.append((self.WD2GCTextCtrl, expandOption))
            blWD.append((self.WD2ResidualTextCtrl, expandOption))
        #contlst=[]
        contlst.append((self.HorizontalLabel, noOptions))
        contlst.append((self.TimeLabel, noOptions))
        contlst.append((self.HAngleLabel, noOptions))
        contlst.append((self.VAngleLabel, noOptions))
        contlst.append((self.ResidualLabel, noOptions))
        for el in self.layout['order'][2:6]:
            contlst.extend(eval('bl'+str(el)))

        hororder = self.layout['order'][2:6]  # default is EU,WU,ED,WD
        if not hororder == ['EU','WU','ED','WD']:
             prevel = hororder[0]
             for idx, el in enumerate(reversed(hororder)):  # example WD,ED,WU,EU and EU,WD,ED,WU
                 #print ("Test", el,prevel, idx, hororder)
                 if idx > 0:
                     exec("self.{}2ResidualTextCtrl.MoveBeforeInTabOrder(self.{}1TimeTextCtrl)".format(el,prevel))
                     exec("self.{}2GCTextCtrl.MoveBeforeInTabOrder(self.{}2ResidualTextCtrl)".format(el,el))
                     exec("self.{}2AngleTextCtrl.MoveBeforeInTabOrder(self.{}2GCTextCtrl)".format(el,el))
                     exec("self.{}2TimeTextCtrl.MoveBeforeInTabOrder(self.{}2AngleTextCtrl)".format(el,el))
                     exec("self.{}1ResidualTextCtrl.MoveBeforeInTabOrder(self.{}2TimeTextCtrl)".format(el,el))
                     exec("self.{}1GCTextCtrl.MoveBeforeInTabOrder(self.{}1ResidualTextCtrl)".format(el,el))
                     exec("self.{}1AngleTextCtrl.MoveBeforeInTabOrder(self.{}1GCTextCtrl)".format(el,el))
                     exec("self.{}1TimeTextCtrl.MoveBeforeInTabOrder(self.{}1AngleTextCtrl)".format(el,el))
                 prevel = el


        # Mire elements
        contlst.append((self.BmireLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        blMU = []
        blMU.append((self.BmireUpLabel, noOptions))
        blMU.append((self.BmireUp1TextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blMU.append((self.BmireUp2TextCtrl, expandOption))
        else:
            blMU.append(emptySpace)
        blMU.append(emptySpace)
        blMU.append((self.calcButton, expandOption))
        blMD = []
        blMD.append((self.BmireDownLabel, noOptions))
        blMD.append((self.BmireDown1TextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blMD.append((self.BmireDown2TextCtrl, expandOption))
        else:
            blMD.append(emptySpace)
        blMD.append(emptySpace)
        blMD.append(emptySpace)
        for el in self.layout['order'][0:2]:
            contlst.extend(eval('bl'+str(el)))

        miorder = self.layout['order'][0:2]
        if miorder[0] == 'MU':  # default is MD, MU
            self.BmireUp2TextCtrl.MoveBeforeInTabOrder(self.BmireDown1TextCtrl)
            self.BmireUp1TextCtrl.MoveBeforeInTabOrder(self.BmireUp2TextCtrl)

        # Mire elements
        blNU = []
        blNU.append((self.NULabel, noOptions))
        blNU.append((self.NU1TimeTextCtrl, expandOption))
        blNU.append((self.NU1GCTextCtrl, expandOption))
        blNU.append((self.NU1AngleTextCtrl, expandOption))
        blNU.append((self.NU1ResidualTextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blNU.append(emptySpace)
            blNU.append((self.NU2TimeTextCtrl, expandOption))
            blNU.append((self.NU2GCTextCtrl, expandOption))
            blNU.append((self.NU2AngleTextCtrl, expandOption))
            blNU.append((self.NU2ResidualTextCtrl, expandOption))
        blSU = []
        blSU.append((self.SULabel, noOptions))
        blSU.append((self.SU1TimeTextCtrl, expandOption))
        blSU.append((self.SU1GCTextCtrl, expandOption))
        blSU.append((self.SU1AngleTextCtrl, expandOption))
        blSU.append((self.SU1ResidualTextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blSU.append(emptySpace)
            blSU.append((self.SU2TimeTextCtrl, expandOption))
            blSU.append((self.SU2GCTextCtrl, expandOption))
            blSU.append((self.SU2AngleTextCtrl, expandOption))
            blSU.append((self.SU2ResidualTextCtrl, expandOption))
        blND = []
        blND.append((self.NDLabel, noOptions))
        blND.append((self.ND1TimeTextCtrl, expandOption))
        blND.append((self.ND1GCTextCtrl, expandOption))
        blND.append((self.ND1AngleTextCtrl, expandOption))
        blND.append((self.ND1ResidualTextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blND.append(emptySpace)
            blND.append((self.ND2TimeTextCtrl, expandOption))
            blND.append((self.ND2GCTextCtrl, expandOption))
            blND.append((self.ND2AngleTextCtrl, expandOption))
            blND.append((self.ND2ResidualTextCtrl, expandOption))
        blSD = []
        blSD.append((self.SDLabel, noOptions))
        blSD.append((self.SD1TimeTextCtrl, expandOption))
        blSD.append((self.SD1GCTextCtrl, expandOption))
        blSD.append((self.SD1AngleTextCtrl, expandOption))
        blSD.append((self.SD1ResidualTextCtrl, expandOption))
        if not self.layout['double'] == 'False':
            blSD.append(emptySpace)
            blSD.append((self.SD2TimeTextCtrl, expandOption))
            blSD.append((self.SD2GCTextCtrl, expandOption))
            blSD.append((self.SD2AngleTextCtrl, expandOption))
            blSD.append((self.SD2ResidualTextCtrl, expandOption))
        #contlst=[]
        contlst.append((self.VerticalLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        for el in self.layout['order'][6:10]:
            contlst.extend(eval('bl'+str(el)))

        # Tab order
        verorder = self.layout['order'][6:10]  # default is NU,SD,ND,SU
        if not verorder == ['NU','SD','ND','SU']:
             prevel = verorder[0]
             for idx, el in enumerate(reversed(verorder)):
                 #print ("Test", el,prevel, idx, hororder)
                 if idx > 0:
                     exec("self.{}2ResidualTextCtrl.MoveBeforeInTabOrder(self.{}1TimeTextCtrl)".format(el,prevel))
                     exec("self.{}2GCTextCtrl.MoveBeforeInTabOrder(self.{}2ResidualTextCtrl)".format(el,el))
                     exec("self.{}2AngleTextCtrl.MoveBeforeInTabOrder(self.{}2GCTextCtrl)".format(el,el))
                     exec("self.{}2TimeTextCtrl.MoveBeforeInTabOrder(self.{}2AngleTextCtrl)".format(el,el))
                     exec("self.{}1ResidualTextCtrl.MoveBeforeInTabOrder(self.{}2TimeTextCtrl)".format(el,el))
                     exec("self.{}1GCTextCtrl.MoveBeforeInTabOrder(self.{}1ResidualTextCtrl)".format(el,el))
                     exec("self.{}1AngleTextCtrl.MoveBeforeInTabOrder(self.{}1GCTextCtrl)".format(el,el))
                     exec("self.{}1TimeTextCtrl.MoveBeforeInTabOrder(self.{}1AngleTextCtrl)".format(el,el))
                 prevel = el


        # Scale test
        if not self.layout['scalevalue'] == 'False':
            contlst.append((self.SCLabel, noOptions))
            contlst.append((self.SCTimeTextCtrl, expandOption))
            contlst.append((self.SCGCTextCtrl, expandOption))
            contlst.append((self.SCAngleTextCtrl, expandOption))
            contlst.append((self.SCResidualTextCtrl, expandOption))

        contlst.append((self.FLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.FInstLabel, noOptions))
        contlst.append((self.FBaseLabel, noOptions))
        contlst.append((self.FLoadFromFileButton, expandOption))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.FInstTextCtrl, noOptions))
        contlst.append((self.FBaseTextCtrl, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.FValsLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.FValsTextCtrl, noOptions))
        contlst.append(emptySpace)

        # A GridSizer will contain the other controls:
        cols = 5
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        self.mainSizer.Add(gridSizer, 0, wx.EXPAND)

    def bindControls(self):
        self.loadButton.Bind(wx.EVT_BUTTON, self.OnLoad)
        self.Bind(wx.EVT_RADIOBOX, self.OnFlip, self.angleRadioBox)
        self.calcButton.Bind(wx.EVT_BUTTON, self.OnCalc)
        self.FLoadFromFileButton.Bind(wx.EVT_BUTTON, self.OnLoadF)

    def OnLoadF(self, e):
        self.dirname = os.path.expanduser('~')
        dlg = wx.FileDialog(self, "Choose a data file with", self.dirname, "", "*.*")
        path = ''
        if dlg.ShowModal() == wx.ID_OK:
            path = dlg.GetPath()
        if path == '':
            return

        def wxdate2pydate(date):
            assert isinstance(date, wx.DateTime)
            if date.IsValid():
                ymd = list(map(int, date.FormatISODate().split('-')))
                return datetime(*ymd)
            else:
                return None

        import time
        datet = wxdate2pydate(self.DatePicker.GetValue())
        mintime  = wx.DateTimeFromTimeT(time.mktime(datet.timetuple()))
        maxdate = datet+timedelta(days=1)
        maxtime  = wx.DateTimeFromTimeT(time.mktime(maxdate.timetuple()))
        extension = '*.*'
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

            if isinstance(path, basestring):
                if not path=='':
                    #self.changeStatusbar("Loading data ... please be patient")
                    stream = read(path_or_url=path, starttime=start, endtime=end)
            else:
                # assume Database
                try:
                    #self.changeStatusbar("Loading data ... please be patient")
                    stream = readDB(path[0],path[1], starttime=start, endtime=end)
                except:
                    pass
                    #logger.info ("Reading failed")
        else:
            stream = DataStream()

        #self.changeStatusbar("Ready")
        if stream.length()[0] > 0:
            dataid = stream.header.get('DataID')
            self.FInstTextCtrl.SetValue(dataid)
            basevalue = 0.0
            self.FBaseTextCtrl.SetValue(str(basevalue))
            posf = KEYLIST.index('f')
            ftext = ''
            for idx, elem in enumerate(stream.ndarray[0]):
                time = datetime.strftime(num2date(elem).replace(tzinfo=None), "%H:%M:%S")
                ftext += "{},{:.2f}\n".format(time,stream.ndarray[posf][idx])
            self.FValsTextCtrl.SetValue(ftext)

    def _degminsec2deg(self, string, back='decimal'):
        """
        DESCRIPTION
            checks input string and returns either deg: or deg.
        PARAMETER
            string (string):   a deg:min:sec of deg.decimal value
            back   (float) :   either decimal or dms
        """
        string = str(string)
        if string in ['','0.0000 or 00:00:00.0']:
            return string

        if string.find(':') > 0:
            val = 0.
            numlist = string.split(':')
            for idx, ele in enumerate(numlist):
                add = float(ele)/float(60.**idx)
                val += float(add)
        else:
            if string.find(',') > 0:
                string = string.replace(',','.')
            try:
                val = float(string)
            except:
                val = 999.0

        if not val >= -180 and not val <= 360:
            return 999.

        def _decdeg2dms(dd):
            is_positive = dd >= 0
            dd = abs(dd)
            minutes,seconds = divmod(dd*3600,60)
            degrees,minutes = divmod(minutes,60)
            degrees = degrees if is_positive else -degrees
            return [int(np.round(degrees,0)),int(np.round(minutes,0)),seconds]

        if back == 'dms':
            return ":".join(map(str,_decdeg2dms(val)))
        else:
            return str(val)

    def mean_angle(self, deg):   # rosettacode
        from cmath import rect, phase
        from math import radians, degrees
        return degrees(phase(sum(rect(1, radians(d)) for d in deg)/len(deg)))

    def OnCalc(self, e):
        # Calculate angle if enough values are present:

        message = "Please fill in all horizontal measurements first!\n"
        #message = "You need to specify a unique Data ID\nfor which meta information is obtained.\n"
        vallst = []
        for el in self.layout['order'][2:6]:
            for num in ['1','2']:
                exec("valel = self.{}{}AngleTextCtrl.GetValue()".format(el,num))
                try:
                    valel = float(self._degminsec2deg(valel))
                    if not valel == 0 and not np.isnan(valel):
                        vallst.append(valel)
                except:
                    pass

        deg = self.UnitComboBox.GetValue()
        if deg in ['degree','deg']:
            meanangle = (self.mean_angle(vallst))
        else:
            vallst = [el*360./400. for el in vallst]
            meanangle = np.mean(np.asarray(vallst))*400./360.
        if len(vallst) >= 4:
             typus = self.angleRadioBox.GetStringSelection()
             message = "\n {} {}".format(self._degminsec2deg(meanangle,back=typus),deg)
        dlg = wx.MessageDialog(self, "Average horizontal angle:\n"+message, "Horizontal angle", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()


    def OnFlip(self, e):
        # fields to be converted
        checklist = []
        for comp in ['EU','WU','ED','WD','NU','SD','ND','SU']:
            for i in ['1','2']:
                checklist.append('self.'+comp+i+'AngleTextCtrl')
        for comp in ['A','B']:
            for i in ['Up1','Up2','Down1','Down2']:
                checklist.append('self.'+comp+'mire'+i+'TextCtrl')
        checklist.append('self.SCAngleTextCtrl')
        checklist.append('self.AzimuthTextCtrl')
        #print (checklist)
        # get aim (dms or decimal)
        aim = self.angleRadioBox.GetStringSelection()
        # perform calc for each input
        for el in checklist:
            value = eval(el+'.GetValue()')
            newvalue = self._degminsec2deg(value,aim)
            eval(el+'.SetValue(newvalue)')

    def OnLoad(self, e):
        def _readDI(path):
            datalist = []
            fh = open(path, 'rt')
            for line in fh:
                datalist.append(line.strip('\n'))
            fh.close()
            return datalist

        def _getDI():
            datalist = []
            """
            if self.db:
                cursor = self.db.cursor()
                sql = "SHOW TABLES LIKE 'DIDATA%'"
                cursor.execute(sql)
                output = cursor.fetchall()
                tablelist = [elem[0] for elem in output]
                if len(tablelist) < 1:
                    dlg = wx.MessageDialog(self, "No DI data tables available!\n"
                            "please check your database\n",
                            "OpenDB", wx.OK|wx.ICON_INFORMATION)
                    dlg.ShowModal()
                    return
                # 1 dlg, select table
                if not len(tablelist) == 1:
                    dlg = DITableDialog(None, title='Select DIDATA table', tablelist=tablelist)
                    if dlg.ShowModal() == wx.ID_OK:
                        table = dlg.tableRadioBox.GetStringSelection()
                else:
                    table = tablelist[0]

                print table
                # 2 dlg, select pier
                sql = "SELECT StartTime, Pier FROM "+table
                cursor.execute(sql)
                output = cursor.fetchall()
                resultlist = [elem[0] for elem in output]
                # 3 dlg, select time
                dlg = DISelectionDialog(None, title='Select DI-Dataset',resultlist=resultlist)
                if dlg.ShowModal() == wx.ID_OK:
                    # ComboBox with pier and times
                    pier = dlg.pierComboBox.GetValue()
                    time = dlg.timeComboBox.GetValue()
                sql = "Select * FROM {} WHERE StartTime = '{}' and Pier = '{}'".format(table, time, pier)
                cursor.execute(sql)
                output = cursor.fetchall()
                datalist = [elem[0] for elem in output]
                print ("DATALIST", datalist)
            """
            return datalist


        def _datalist2wx(datalist,iagacode):
            # datalist looks like:
            # string list with lines:
            #['# MagPy Absolutes\n', '# Abs-Observer: Leichter\n', '# Abs-Theodolite: T10B_0619H154167_07-2011\n', '# Abs-TheoUnit: deg\n', '# Abs-FGSensor: MAG01H_SerialSensor_SerialElectronic_07-2011\n', '# Abs-AzimuthMark: 180.1044444\n', '# Abs-Pillar: A4\n', '# Abs-Scalar: /\n', '# Abs-Temperature: 6.7C\n', '# Abs-InputDate: 2016-01-26\n', 'Miren:\n', '0.099166666666667  0.098055555555556  180.09916666667  180.09916666667  0.098055555555556  0.096666666666667  180.09805555556  180.09805555556\n', 'Positions:\n', '2016-01-21_13:22:00  93.870555555556  90  1.1\n', '2016-01-21_13:22:30  93.870555555556  90  1.8\n', '2016-01-21_13:27:00  273.85666666667  90  0.1\n', '2016-01-21_13:27:30  273.85666666667  90  0.2\n', '2016-01-21_13:25:30  273.85666666667  270  0.3\n', '2016-01-21_13:26:00  273.85666666667  270  -0.6\n', '2016-01-21_13:24:00  93.845555555556  270  -0.2\n', '2016-01-21_13:24:30  93.845555555556  270  0.4\n', '2016-01-21_13:39:30  0  64.340555555556  -0.3\n', '2016-01-21_13:40:00  0  64.340555555556  0.1\n', '2016-01-21_13:38:00  0  244.34055555556  0\n', '2016-01-21_13:38:30  0  244.34055555556  -0.4\n', '2016-01-21_13:36:00  180  295.67055555556  1.1\n', '2016-01-21_13:36:30  180  295.67055555556  1.2\n', '2016-01-21_13:34:30  180  115.66916666667  0.3\n', '2016-01-21_13:35:00  180  115.66916666667  0.9\n', '2016-01-21_13:34:30  180  115.66916666667  0\n', 'PPM:\n', 'Result:\n']

            poscnt = 0
            poslst = ['EU','EU','WU','WU','ED','ED','WD','WD','NU','NU','SD','SD','ND','ND','SU','SU']
            posord = ['1','2','1','2','1','2','1','2','1','2','1','2','1','2','1','2']
            ffield = []
            self.CodeTextCtrl.SetValue(iagacode)
            for line in datalist:
                #print ("Here", line)
                numelements = len(line.split())
                if line.isspace():
                    # blank line
                    pass
                elif line.startswith('#'):
                    # header
                    line = line.strip('\n')
                    headline = line.split(':')
                    #self.CodeTextCtrl.SetValue()
                    #self.DatePicker = wx.DatePickerCtrl(self, dt=self.cdate,size=(160,30))

                    if headline[0] == ('# Abs-Observer'):
                        self.ObserverTextCtrl.SetValue(headline[1].strip())
                    if headline[0] == ('# Abs-Theodolite'):
                        self.TheoTextCtrl.SetValue(headline[1].replace(', ','_').strip().replace(' ','_'))
                    if headline[0] == ('# Abs-TheoUnit'):
                        self.UnitComboBox.SetStringSelection(headline[1].strip().replace('deg','degree'))
                    if headline[0] == ('# Abs-FGSensor'):
                        self.FluxTextCtrl.SetValue(headline[1].strip().replace(' ','_'))
                    if headline[0] == ('# Abs-AzimuthMark'):
                        self.AzimuthTextCtrl.SetValue(headline[1].strip())
                    if headline[0] == ('# Abs-Pillar'):
                        self.PillarTextCtrl.SetValue(headline[1].strip())
                    if headline[0] == ('# Abs-Scalar'):
                        self.FInstTextCtrl.SetValue(headline[1].strip())
                    if headline[0] == ('# Abs-Notes'):
                        self.CommentTextCtrl.SetValue(headline[1].strip())
                        #datalist.append(headline[1].strip())
                    #if headline[0] == ('# Abs-DeltaF'):
                    #    datalist.append(headline[1].strip())
                    if headline[0] == ('# Abs-Temperature'):
                        self.TempTextCtrl.SetValue(headline[1].strip().strip('C'))
                elif numelements == 8:
                    # Miren mesurements
                    mirestr = line.split()
                    self.AmireUp1TextCtrl.SetValue(mirestr[0])
                    self.AmireUp2TextCtrl.SetValue(mirestr[1])
                    self.AmireDown1TextCtrl.SetValue(mirestr[2])
                    self.AmireDown2TextCtrl.SetValue(mirestr[3])
                    self.BmireUp1TextCtrl.SetValue(mirestr[4])
                    self.BmireUp2TextCtrl.SetValue(mirestr[5])
                    self.BmireDown1TextCtrl.SetValue(mirestr[6])
                    self.BmireDown2TextCtrl.SetValue(mirestr[7])
                elif numelements == 4:
                    # Position mesurements
                    posstr = line.split()
                    #print ("Cnt", poscnt, posstr)
                    lineel = ['Time','GC','Angle','Residual']
                    if poscnt == 16:
                        na = 'SC'
                    elif poscnt < 16:
                        comp = poslst[poscnt]
                        na = comp+posord[poscnt]
                    if poscnt < 8:
                        lineel = ['Time','Angle','GC','Residual']
                    for idx,el in enumerate(posstr):
                        col = lineel[idx]
                        #print ("Here", el, col)
                        if col == 'Time':
                            try:
                                mdate = datetime.strptime(el,"%Y-%m-%d_%H:%M:%S")
                                el = datetime.strftime(mdate,"%H:%M:%S")
                            except:
                                el = '00:00:00'
                        eval('self.'+na+col+'TextCtrl.SetValue(el)')
                    poscnt = poscnt+1
                elif numelements == 2:
                    # Intensity mesurements
                    fstr = line.split()
                    try:
                        el = datetime.strftime(datetime.strptime(fstr[0],"%Y-%m-%d_%H:%M:%S"),"%H:%M:%S")
                    except:
                        el = '00:00:00'
                    try:
                        f = fstr[1].replace(',','.')
                    except:
                        f = 0.0
                    fline = ','.join([el,f])+'\n'
                    ffield.append(fline)
                else:
                    #print line
                    pass

            self.DatePicker.SetValue(wx.DateTimeFromTimeT(time.mktime(mdate.timetuple())))

            if len(ffield) > 0:
                self.FValsTextCtrl.SetValue("".join(ffield))
                self.FBaseTextCtrl.SetValue("0.0")
            else:
                self.FInstTextCtrl.SetValue("type_serial_version")
                self.FValsTextCtrl.SetValue("time,value")
                self.FBaseTextCtrl.SetValue("")


        loadfile = False
        loadDB = False
        if self.db:
            # Open a selection dlg (database , file)
            loadfile = True
        else:
            loadfile = True

        datalist = []
        self.dirname = os.path.expanduser('~')
        iagacode = 'undefined'

        if loadfile:
            dlg = wx.FileDialog(self, "Choose a DI raw data file", self.dirname, "", "*.*")
            if dlg.ShowModal() == wx.ID_OK:
                path = dlg.GetPath()
                try:
                    iagacode = os.path.split(path)[1].split('.')[0].split('_')[-1]
                except:
                    iagacode = 'undefined'
                datalist = _readDI(path)
        elif loadDB:
            datalist = _getDI()
        if len(datalist) > 0:
            self.angleRadioBox.SetStringSelection("decimal")
            #print ("Datalist", datalist)
            _datalist2wx(datalist, iagacode)



# ###################################################
#    Data Checker page
# ###################################################


class CheckDefinitiveDataDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select directories for data checking
    """

    def __init__(self, parent, title):
        super(CheckDefinitiveDataDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.checkchoices = ['quick','full']
        self.checkchoice = 'quick'
        self.laststep = 7
        self.minutedirname = ''
        self.seconddirname = ''
        self.checkparameter = {'step2':True, 'step3':True, 'step4':True, 'step5':True, 'step6':True, 'step7':True }  # modified by checkOptions
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.checkRadioBox = wx.RadioBox(self, label="Choose check type:",  choices=self.checkchoices,
                       majorDimension=2, style=wx.RA_SPECIFY_COLS ) #,size=(160,-1))
        self.minuteLabel = wx.StaticText(self, label="Select minute data:",size=(160,30))
        self.minuteButton = wx.Button(self, label='Choose IAF directory',size=(160,30))
        self.minuteTextCtrl = wx.TextCtrl(self, value=self.minutedirname ,size=(160,30))
        self.secondLabel = wx.StaticText(self, label="(Optional) Select second data:",size=(160,30))
        self.secondButton = wx.Button(self, label='Choose ImagCDF/IAGA02 directory',size=(160,30))
        self.secondTextCtrl = wx.TextCtrl(self, value=self.seconddirname ,size=(160,30))
        self.checkOptionsButton = wx.Button(self, label='Specify check options',size=(160,30))
        self.checkButton = wx.Button(self, wx.ID_OK, label='Run check', size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))
        self.note1Label = wx.StaticText(self, label="*quick: 4 min with second data",size=(160,30))
        self.note2Label = wx.StaticText(self, label="*full: 50 min with second data",size=(160,30))

        self.minuteTextCtrl.Disable()
        self.secondTextCtrl.Disable()
        #self.checkOptionsButton.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.minuteLabel, noOptions),
                  emptySpace,
                 (self.minuteButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.minuteTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.secondLabel, noOptions),
                  emptySpace,
                 (self.secondButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.secondTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.checkRadioBox, noOptions),
                 (self.checkOptionsButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.note1Label, noOptions),
                  emptySpace,
                 (self.note2Label, noOptions),
                  emptySpace,
                 (self.checkButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        #self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.minuteButton.Bind(wx.EVT_BUTTON, self.OnMinute)
        self.secondButton.Bind(wx.EVT_BUTTON, self.OnSecond)
        self.checkOptionsButton.Bind(wx.EVT_BUTTON, self.OnCheckOptions)
        self.Bind(wx.EVT_RADIOBOX, self.OnDeep, self.checkRadioBox)

    #def OnClose(self, e):
    #    self.Close(True)

    def OnMinute(self, e):
        dialog = wx.DirDialog(None, "Choose IAF directory:",self.minutedirname,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.minutedirname = dialog.GetPath() # modify self.dirname
            self.minuteTextCtrl.SetValue(self.minutedirname)
        dialog.Destroy()

    def OnSecond(self, e):
        dialog = wx.DirDialog(None, "Choose ImagCDF/IAGA02 directory:",self.seconddirname,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.seconddirname = dialog.GetPath() # modify self.dirname
            self.secondTextCtrl.SetValue(self.seconddirname)
        dialog.Destroy()

    def OnCheckOptions(self, e):
        dlg = CheckDataSelectDialog(None, title='Select checking steps', checkparameter=self.checkparameter)
        if dlg.ShowModal() == wx.ID_OK:
            #print ("HEEREE", dlg.step2CheckBox.GetValue())
            for key in self.checkparameter:
                val = eval('dlg.'+key+'CheckBox.GetValue()')
                self.checkparameter[key] = val
        else:
            dlg.Destroy()
        steplist = [key.strip('step') for key in self.checkparameter if self.checkparameter.get(key)]
        if len(steplist) == 0:
            steplist=[1]
        self.laststep = np.max(list(map(int,steplist)))

    def OnDeep(self, e):
        self.checkchoice = self.checkRadioBox.GetStringSelection()


class CheckDataReportDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to show report of data check
    """

    def __init__(self, parent, title, report, rating, step=['0','0','0','0','0','0','0'],laststep=7):
        super(CheckDataReportDialog, self).__init__(parent=parent,
            title=title, size=(600, 400))
        self.rating = np.max(list(map(int,step)))
        self.report = report
        self.laststep = laststep
        self.step = step
        currentstep = (np.max([idx for idx, val in enumerate(step) if not val == '0']))+1
        if laststep == currentstep:
            self.contlabel = 'Save'
        else:
            self.contlabel = 'Continue'
        self.moveon = False
        self.createControls()
        self.doLayout()
        self.bindControls()


    def putColor(self, rating, step):
        if rating in ['1','2']:
            exec("self.step{}TextCtrl.SetBackgroundColour(wx.GREEN)".format(step))
        elif rating in ['3','4']:
            exec("self.step{}TextCtrl.SetBackgroundColour(wx.Colour(255,165,0))".format(step))
        elif rating in ['5','6']:
            exec("self.step{}TextCtrl.SetBackgroundColour(wx.RED)".format(step))
        else:
            pass


    # Widgets
    def createControls(self):
        self.reportLabel = wx.StaticText(self, label="Data checking report:",size=(300,30))
        self.reportTextCtrl = wx.TextCtrl(self, value=self.report ,size=(600,300), style = wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)
        self.ratingTextCtrl = wx.TextCtrl(self, value="Overall rating: {}".format(self.rating), size=(30,30))
        self.continueButton = wx.Button(self, label=self.contlabel, size=(160,30))
        self.closeButton = wx.Button(self, label='Cancel',size=(160,30))

        self.step1TextCtrl = wx.TextCtrl(self, value=self.step[0], size=(30,30))
        self.step2TextCtrl = wx.TextCtrl(self, value=self.step[1], size=(30,30))
        self.step3TextCtrl = wx.TextCtrl(self, value=self.step[2], size=(30,30))
        self.step4TextCtrl = wx.TextCtrl(self, value=self.step[3], size=(30,30))
        self.step5TextCtrl = wx.TextCtrl(self, value=self.step[4], size=(30,30))
        self.step6TextCtrl = wx.TextCtrl(self, value=self.step[5], size=(30,30))
        self.step7TextCtrl = wx.TextCtrl(self, value=self.step[6], size=(30,30))
        self.step1Label = wx.StaticText(self, label="Step 1",size=(80,30))
        self.step2Label = wx.StaticText(self, label="Step 2",size=(80,30))
        self.step3Label = wx.StaticText(self, label="Step 3",size=(80,30))
        self.step4Label = wx.StaticText(self, label="Step 4",size=(80,30))
        self.step5Label = wx.StaticText(self, label="Step 5",size=(80,30))
        self.step6Label = wx.StaticText(self, label="Step 6",size=(80,30))
        self.step7Label = wx.StaticText(self, label="Step 7",size=(80,30))

        #self.reportTextCtrl.Disable()
        #self.step1TextCtrl.Disable()
        #self.step2TextCtrl.Disable()
        #self.step3TextCtrl.Disable()
        #self.step4TextCtrl.Disable()
        #self.step5TextCtrl.Disable()
        #self.step6TextCtrl.Disable()
        #self.step7TextCtrl.Disable()
        #self.ratingTextCtrl.Disable()
        self.ratingTextCtrl.SetEditable(False)
        self.step1TextCtrl.SetEditable(False)
        self.step2TextCtrl.SetEditable(False)
        self.step3TextCtrl.SetEditable(False)
        self.step4TextCtrl.SetEditable(False)
        self.step5TextCtrl.SetEditable(False)
        self.step6TextCtrl.SetEditable(False)
        self.step7TextCtrl.SetEditable(False)

        for idx, rating in enumerate(self.step):
            self.putColor(rating, idx+1)

        if self.rating in ['1','2',1,2]:
            self.ratingTextCtrl.SetBackgroundColour(wx.GREEN)
        elif self.rating in ['3','4',3,4]:
            self.ratingTextCtrl.SetBackgroundColour(wx.Colour(255,165,0))
        elif self.rating in ['5','6',5,6]:
            self.ratingTextCtrl.SetBackgroundColour(wx.RED)
        else:
            pass


    def doLayout(self):

        mainSizer = wx.BoxSizer(wx.VERTICAL)
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlist = [(self.reportLabel, noOptions),
                 (self.reportTextCtrl, expandOption),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.continueButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.ratingTextCtrl, expandOption)]

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        contlist = [(self.step1TextCtrl, expandOption),
                 (self.step2TextCtrl, expandOption),
                 (self.step3TextCtrl, expandOption),
                 (self.step4TextCtrl, expandOption),
                 (self.step5TextCtrl, expandOption),
                 (self.step6TextCtrl, expandOption),
                 (self.step7TextCtrl, expandOption),
                 (self.step1Label, noOptions),
                 (self.step2Label, noOptions),
                 (self.step3Label, noOptions),
                 (self.step4Label, noOptions),
                 (self.step5Label, noOptions),
                 (self.step6Label, noOptions),
                 (self.step7Label, noOptions)]
        cols = 7
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            mainSizer.Add(control, **options)

        #mainSizer.Add(self.sourceLabel, 0, wx.ALIGN_LEFT | wx.ALL, 3)

        self.SetSizerAndFit(mainSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.continueButton.Bind(wx.EVT_BUTTON, self.OnContinue)

    def OnClose(self, e):
        self.moveon = False
        self.Close(True)

    def OnContinue(self, e):
        self.moveon = True
        self.Close(True)


class CheckDataSelectDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select steps for data check
    """

    def __init__(self, parent, title, checkparameter):
        super(CheckDataSelectDialog, self).__init__(parent=parent,
            title=title, size=(600, 400))
        self.checkparameter = checkparameter #{'step2':True, 'step3':True, 'step4':True, 'step5':True, 'step6':True, 'step7':True }
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.selectLabel = wx.StaticText(self, label="Choose steps to be used in data checking:",size=(400,30))

        self.step1CheckBox = wx.CheckBox(self, label="Step 1: directories and existance of files (obligatory)",size=(400,30))
        self.step2CheckBox = wx.CheckBox(self, label="Step 2: file access and basic header information",size=(400,30))
        self.step3CheckBox = wx.CheckBox(self, label="Step 3: data content and consistency of primary source",size=(400,30))
        self.step4CheckBox = wx.CheckBox(self, label="Step 4: secondary source and consistency with primary",size=(400,30))
        self.step5CheckBox = wx.CheckBox(self, label="Step 5: basevalues and adopted baseline variation",size=(400,30))
        self.step6CheckBox = wx.CheckBox(self, label="Step 6: yearly means, consistency of meta information",size=(400,30))
        self.step7CheckBox = wx.CheckBox(self, label="Step 7: activity indicies",size=(400,30))

        self.continueButton = wx.Button(self, wx.ID_OK, label='OK', size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

        self.step1CheckBox.SetValue(True)
        self.step2CheckBox.SetValue(self.checkparameter.get('step2'))
        self.step3CheckBox.SetValue(self.checkparameter.get('step3'))
        self.step4CheckBox.SetValue(self.checkparameter.get('step4'))
        self.step5CheckBox.SetValue(self.checkparameter.get('step5'))
        self.step6CheckBox.SetValue(self.checkparameter.get('step6'))
        self.step7CheckBox.SetValue(self.checkparameter.get('step7'))

        self.step1CheckBox.Disable()


    def doLayout(self):

        mainSizer = wx.BoxSizer(wx.VERTICAL)
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlist = [(self.selectLabel, noOptions),
                 (self.step1CheckBox, noOptions),
                 (self.step2CheckBox, noOptions),
                 (self.step3CheckBox, noOptions),
                 (self.step4CheckBox, noOptions),
                 (self.step5CheckBox, noOptions),
                 (self.step6CheckBox, noOptions),
                 (self.step7CheckBox, noOptions)]
        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        contlist = [(self.closeButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.continueButton, dict(flag=wx.ALIGN_CENTER))]
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            mainSizer.Add(control, **options)

        #mainSizer.Add(self.sourceLabel, 0, wx.ALIGN_LEFT | wx.ALL, 3)

        self.SetSizerAndFit(mainSizer)


    def bindControls(self):
        #self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        pass

    #def OnClose(self, e):
    #    self.Close(True)


class CheckOpenLogDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to show log file content
    """

    def __init__(self, parent, title, report):
        super(CheckOpenLogDialog, self).__init__(parent=parent,
            title=title, size=(600, 400))
        self.report = report
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.reportLabel = wx.StaticText(self, label="Logging content:",size=(300,30))
        self.reportTextCtrl = wx.TextCtrl(self, value=self.report ,size=(600,300), style = wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Close',size=(160,30))

        #self.reportTextCtrl.Disable()


    def doLayout(self):

        mainSizer = wx.BoxSizer(wx.VERTICAL)
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlist = [(self.reportLabel, noOptions),
                 (self.reportTextCtrl, expandOption),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


    def bindControls(self):
        #self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        pass

    #def OnClose(self, e):
    #    self.Close(True)


# ###################################################
#    Monitor page
# ###################################################

class SelectMARTASDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select table for MARCOS monitoring
    """

    def __init__(self, parent, title, options):
        super(SelectMARTASDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.options = options
        self.protocol = 'mqtt'
        self.qos = ['0','1','2']
        self.stationid = 'WIC'               # should be extracted from options
        self.user = 'cobs'                   # should be extracted from options
        if options.get('experimental'):
            self.protocollist = ['mqtt','wamp*']
        else:
            self.protocollist = ['mqtt']
        self.portlist = ['8080','1883']
        self.selector = 1                    # list number of protocol
        self.favoritemartas = self.options.get('favoritemartas')
        if not self.favoritemartas or not len(self.favoritemartas) > 0:
            self.favoritemartas = ['www.example.com','192.168.178.42']
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.qosLabel = wx.StaticText(self, label="Quality of service:",size=(160,30))
        self.qosComboBox = wx.ComboBox(self, choices=self.qos,
                       style=wx.CB_DROPDOWN, value=self.qos[0],size=(160,-1))
        self.protocolRadioBox = wx.RadioBox(self, label="Communication protocol:",  choices=self.protocollist,
                       majorDimension=2, style=wx.RA_SPECIFY_COLS ,size=(160,-1))
        self.addressLabel = wx.StaticText(self, label="Select MARTAS:",size=(160,30))
        self.addressComboBox = wx.ComboBox(self, choices=self.favoritemartas,
                       style=wx.CB_DROPDOWN, value=self.favoritemartas[1],size=(160,-1))
        self.dropButton = wx.Button(self, label='Remove from favorites',size=(160,30))
        self.newButton = wx.Button(self, label='Add to favorites',size=(160,30))
        self.portLabel = wx.StaticText(self, label="Communication port:",size=(160,30))
        self.portTextCtrl = wx.TextCtrl(self, value=self.portlist[self.selector],size=(160,30))
        self.stationidLabel = wx.StaticText(self, label="Station ID (e.g. IAGA code):",size=(160,30))
        self.stationidTextCtrl = wx.TextCtrl(self, value="",size=(160,30))
        self.userLabel = wx.StaticText(self, label="*User:",size=(160,30))
        self.userTextCtrl = wx.TextCtrl(self, value=self.user,size=(160,30))
        self.pwdLabel = wx.StaticText(self, label="*Password:",size=(160,30))
        self.pwdTextCtrl = wx.TextCtrl(self, value="",size=(160,30),style=wx.TE_PASSWORD)
        self.authLabel = wx.StaticText(self, label="* if authentication is required",size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.addressLabel, noOptions),
                 (self.addressComboBox, expandOption),
                 (self.dropButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.newButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.protocolRadioBox, noOptions),
                 (self.portLabel, noOptions),
                 (self.portTextCtrl, expandOption),
                 (self.stationidLabel, noOptions),
                 (self.stationidTextCtrl, expandOption),
                 (self.qosLabel, noOptions),
                 (self.qosComboBox, expandOption),
                 (self.userLabel, noOptions),
                 (self.userTextCtrl, expandOption),
                 (self.pwdLabel, noOptions),
                 (self.pwdTextCtrl, expandOption),
                 (self.authLabel, noOptions),
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.newButton.Bind(wx.EVT_BUTTON, self.OnNew)
        self.dropButton.Bind(wx.EVT_BUTTON, self.OnRemove)
        self.Bind(wx.EVT_RADIOBOX, self.OnProtocol, self.protocolRadioBox)


    def OnNew(self, e):
        # get current value in dropdown and append it to 
        newval = self.addressComboBox.GetValue()
        if not newval in self.favoritemartas:
            self.favoritemartas.append(newval)

    def OnRemove(self, e):
        # get current value in dropdown and append it to 
        #self.favoritemartas = self.options.get('favoritemartas')
        dropval = self.addressComboBox.GetValue()
        if dropval in self.favoritemartas:
            self.favoritemartas = [elem for elem in self.favoritemartas if not elem == dropval]
        #print (dropval, self.favoritemartas)
        #self.Close(True)

    def OnProtocol(self, e):
        self.protocol = self.protocolRadioBox.GetStringSelection()


class AGetMARCOSDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select table for MARCOS monitoring
    """

    def __init__(self, parent, title, datalst):
        super(AGetMARCOSDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.datalst = datalst
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:",size=(160,30))
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.datalst[0],size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

class BGetMARCOSDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select table parameters for MARCOS monitoring
    """

    def __init__(self, parent, title, datalst):
        super(BGetMARCOSDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.datalst = datalst
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:",size=(160,30))
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.datalst[0],size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class AGetMARTASDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select address for MARTAS monitoring
    """

    def __init__(self, parent, title, options):
        super(AGetMARTASDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.options = options
        #martaslist = [el for el in self.options.get('connectlist',[]) if el
        self.martaslist =['192.168.178.21', '138.22.188.181', '138.22.188.183']
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.addressLabel = wx.StaticText(self, label="Open MARTAS:",size=(160,30))
        self.addressComboBox = wx.ComboBox(self, choices=self.martaslist,
                       style=wx.CB_DROPDOWN, value=self.martaslist[0],size=(160,-1))
        self.addButton = wx.Button(self, label='Add MARTAS address',size=(160,30))
        self.userLabel = wx.StaticText(self, label="MARTAS user:",size=(160,30))
        self.userTextCtrl = wx.TextCtrl(self, value="cobs",size=(160,30))
        self.pwdLabel = wx.StaticText(self, label="MARTAS pwd:",size=(160,30))
        self.pwdTextCtrl = wx.TextCtrl(self, value="",size=(160,30),style=wx.TE_PASSWORD)
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.addressLabel, noOptions),
                 (self.addressComboBox, expandOption),
                  emptySpace,
                 (self.addButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.userLabel, noOptions),
                 (self.pwdLabel, noOptions),
                 (self.userTextCtrl, expandOption),
                 (self.pwdTextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.addButton.Bind(wx.EVT_BUTTON, self.OnAdd)

    def OnAdd(self, e):
        self.Close(True)


class SelectFromListDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select data from any provided list
    """

    def __init__(self, parent, title, selectlist, name):
        super(SelectFromListDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.selectlist = selectlist
        self.name = name
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.selectLabel = wx.StaticText(self, label="Choose {}:".format(self.name),size=(160,30))
        self.selectComboBox = wx.ComboBox(self, choices=self.selectlist,
                       style=wx.CB_DROPDOWN, value=self.selectlist[0],size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Select',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.selectLabel, noOptions),
                 (self.selectComboBox, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class MultiStreamDialog(wx.Dialog):
    def __init__(self, parent, title, streamlist, idx, streamkeylist):
        style = wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        super(MultiStreamDialog, self).__init__(parent=parent,
            title=title, style=style) #, size=(400, 700))
        #self.modify = False
        #self.result = DataStream()
        #self.resultkeys = []
        #self.streamlist = streamlist
        #self.namelst = []
        #self.streamkeylist = streamkeylist
        #self.activeidx = idx

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        # Add MultilistPanel
        self.panel = MultiStreamPanel(self, title, streamlist, idx, streamkeylist)
        self.panel.SetInitialSize((850, 400))
        self.mainSizer.Add(self.panel, 1, wx.EXPAND | wx.ALL, 10)
        self.SetSizerAndFit(self.mainSizer)


class MultiStreamPanel(scrolledpanel.ScrolledPanel):
    """
    DESCRIPTION:
    Subclass for Multiple stream selections

    This class accesses the streamlist object which should contain the following info:
    datastream and unique header, keylists
    Layout of the multiple stream page:
    stream1 uses dataid name (or sensorid) - if not available just stream x is written

    [ ]  stream1     Dropdown with checkboxes [ ] key1
         (type)                               [ ] key2

    [ ]  stream2     Dropdown with checkboxes [ ] key1
         (type)                               [ ] key2

    [ Select ]         [ Merge ]

    [ Subtract ]       [ Combine ]

    (All other single stream functions are deactivated as long as multiple streams are selected.
     If merge is used a new stream is generated and all other methods are available again.)
    """

    def __init__(self, parent, title, streamlist, idx, streamkeylist):
        scrolledpanel.ScrolledPanel.__init__(self, parent, -1, size=(-1, -1))  #size=(950, 750)
        self.parent = parent
        self.title = title
        self.modify = False
        self.result = DataStream()
        self.resultkeys = []
        self.streamlist = streamlist
        self.namelst = []
        self.streamkeylist = streamkeylist
        self.activeidx = idx

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        self.createWidgets()  ## adding controls to mainsizer
        self.SetSizer(self.mainSizer)
        self.mainSizer.Fit(self)
        self.SetupScrolling()
        self.bindControls()


    # Widgets
    def createWidgets(self):
        self.head1Label = wx.StaticText(self, label="Available datastreams:")
        self.head2Label = wx.StaticText(self, label="Applications:")
        # 1. Section
        tmpnamelst = []
        for idx, elem in enumerate(self.streamlist):
            #print ("Multi - check this if DI analysis has been conducted before",idx, elem.length())
            name = elem.header.get('DataID','stream'+str(idx)).strip()
            if name == 'stream{}'.format(idx):
                name = elem.header.get('SensorID','stream'+str(idx)).strip()
            try:
                name = "{}_{}".format(name,datetime.strftime(elem.start(),"%Y%m%d"))
            except:
                pass
            name = name.replace('-','_')
            keys = self.streamkeylist[idx]
            oldname = name
            if name in tmpnamelst:
                num = len([el for el in tmpnamelst if name == el])
                name = name+'_'+str(num)
            tmpnamelst.append(oldname)
            self.namelst.append(name)
            exec('self.'+name+'CheckBox = wx.CheckBox(self, label="'+name+'")')
            exec('self.'+name+'KeyButton = wx.Button(self,-1,"Keys: '+",".join(keys)+'", size=(160,30))')
            if idx == self.activeidx:
                exec('self.'+name+'CheckBox.SetValue(True)')

        self.ApplyButton = wx.Button(self, wx.ID_OK,"Plot",size=(160,30))
        self.MergeButton = wx.Button(self,-1,"Merge",size=(160,30))
        self.SubtractButton = wx.Button(self,-1,"Subtract",size=(160,30))
        self.CombineButton = wx.Button(self,-1,"Combine",size=(160,30))
        self.AverageStackButton = wx.Button(self,-1,"Average",size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlst = []
        contlst.append((self.head1Label, noOptions))
        contlst.append(emptySpace)
        for idx, elem in enumerate(self.streamlist):
            name = self.namelst[idx]
            contlst.append(eval('(self.'+name+'CheckBox, noOptions)'))
            contlst.append(eval('(self.'+name+'KeyButton, dict(flag=wx.ALIGN_CENTER))'))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.head2Label, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.ApplyButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.MergeButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.SubtractButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.CombineButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.AverageStackButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        #self.mainSizer.Add(gridSizer, 0, wx.EXPAND)
        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        self.mainSizer.Add(gridSizer, 0, wx.EXPAND)


    def bindControls(self):
        from functools import partial
        self.MergeButton.Bind(wx.EVT_BUTTON, self.OnMergeButton)
        self.SubtractButton.Bind(wx.EVT_BUTTON, self.OnSubtractButton)
        self.AverageStackButton.Bind(wx.EVT_BUTTON, self.OnStackButton)
        self.CombineButton.Bind(wx.EVT_BUTTON, self.OnCombineButton)
        for idx, elem in enumerate(self.streamlist):
            name = self.namelst[idx]
            exec('self.'+name+'KeyButton.Bind(wx.EVT_BUTTON, partial( self.OnGetKeys, name = idx ) )')

    def OnGetKeys(self, e, name):
        shkeylst = self.streamkeylist[name]
        keylst = self.streamlist[name]._get_key_headers()
        namelist = []
        for key in shkeylst:
            colname = self.streamlist[name].header.get('col-'+key, '')
            if not colname == '':
                namelist.append(colname)
            else:
                namelist.append(key)
        dlg = StreamSelectKeysDialog(None, title='Select keys:',keylst=keylst,shownkeys=shkeylst,namelist=namelist)
        for elem in shkeylst:
            exec('dlg.'+elem+'CheckBox.SetValue(True)')
        if dlg.ShowModal() == wx.ID_OK:
            shownkeylist = []
            for elem in keylst:
                boolval = eval('dlg.'+elem+'CheckBox.GetValue()')
                if boolval:
                    shownkeylist.append(elem)
            if len(shownkeylist) == 0:
                shownkeylist = self.streamkeylist[name]
            else:
                self.streamkeylist[name] = shownkeylist

        # update
        buttonname = self.namelst[name]
        exec('self.'+str(buttonname)+'KeyButton.SetLabel("Keys: '+",".join(shownkeylist)+'")')

    def OnMergeButton(self, event):
        """
        DESCRIPTION
             Merges two streams
        """
        mergestreamlist,mergekeylist = [],[]
        for idx, elem in enumerate(self.streamlist):
            val = eval('self.'+self.namelst[idx]+'CheckBox.GetValue()')
            if val:
                elem = elem._select_keys(self.streamkeylist[idx])
                mergestreamlist.append(elem)
                mergekeylist.append(self.streamkeylist[idx])
        if len(mergestreamlist) == 2:
            #print (mergestreamlist[0].length(),mergestreamlist[1].length())
            self.result = mergeStreams(mergestreamlist[0],mergestreamlist[1])
            self.resultkeys = self.result._get_key_headers()
            self.modify = True
            #self.streamlist.append(self.result)
            #self.streamkeylist.append(self.result._get_key_headers())
            #
            self.Close(True)
        else:
            dlg = wx.MessageDialog(self, "Merge requires two records\n"
                            " - not less, not more\n",
                            "Merge error", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()

        self.Close(True)
        self.parent.Destroy()


        #self.changeStatusbar("Ready")


    def OnSubtractButton(self, event):
        """
        DESCRIPTION
             Subtracts two stream
        """
        substreamlist,subkeylist = [],[]
        for idx, elem in enumerate(self.streamlist):
            val = eval('self.'+self.namelst[idx]+'CheckBox.GetValue()')
            if val:
                elem = elem._select_keys(self.streamkeylist[idx])
                substreamlist.append(elem)
                subkeylist.append(self.streamkeylist[idx])
        if len(substreamlist) == 2:
            self.result = subtractStreams(substreamlist[0],substreamlist[1])
            self.resultkeys = self.result._get_key_headers()
            self.modify = True
            self.Close(True)
        else:
            dlg = wx.MessageDialog(self, "Subtract requires two records\n"
                            " - not less, not more\n",
                            "Subtract error", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()

        self.Close(True)
        self.parent.Destroy()


    def OnStackButton(self, event):
        """
        DESCRIPTION
             Stacking/Averaging streams
        """
        substreamlist,subkeylist = [],[]
        for idx, elem in enumerate(self.streamlist):
            val = eval('self.'+self.namelst[idx]+'CheckBox.GetValue()')
            if val:
                elem = elem._select_keys(self.streamkeylist[idx])
                substreamlist.append(elem)
                subkeylist.append(self.streamkeylist[idx])
        self.result = stackStreams(substreamlist,get='mean',uncert='True')
        self.resultkeys = self.result._get_key_headers()
        self.modify = True

        self.Close(True)
        self.parent.Destroy()

    def OnCombineButton(self, event):
        """
        DESCRIPTION
             Joining streams
        """
        substreamlist,subkeylist = [],[]
        for idx, elem in enumerate(self.streamlist):
            val = eval('self.'+self.namelst[idx]+'CheckBox.GetValue()')
            if val:
                elem = elem._select_keys(self.streamkeylist[idx])
                substreamlist.append(elem)
                subkeylist.append(self.streamkeylist[idx])
        if len(substreamlist) == 2:
            print ("Combine streams:", substreamlist[0].length()[0],substreamlist[1].length()[0])
            self.result = joinStreams(substreamlist[0],substreamlist[1])
            self.resultkeys = self.result._get_key_headers()
            self.modify = True
            self.Close(True)
        else:
            dlg = wx.MessageDialog(self, "Subtract requires two records\n"
                            " - not less, not more\n",
                            "Subtract error", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()

        self.Close(True)
        self.parent.Destroy()


class WaitDialog(wx.Dialog):
    """ 
    A popup dialog for to inform users
    that work is in progress 
    """

    def __init__(self, parent, title, msg):
        # Create a dialog
        wx.Dialog.__init__(self, parent, -1, title, size=(350, 150),
style=wx.CAPTION | wx.STAY_ON_TOP)
        # Add sizers
        box = wx.BoxSizer(wx.VERTICAL)
        box2 = wx.BoxSizer(wx.HORIZONTAL)
        # Add an Info graphic
        bitmap = wx.EmptyBitmap(32, 32)
        bitmap = wx.ArtProvider_GetBitmap(wx.ART_INFORMATION,
wx.ART_MESSAGE_BOX, (32, 32))
        graphic = wx.StaticBitmap(self, -1, bitmap)
        box2.Add(graphic, 0, wx.EXPAND | wx.ALIGN_CENTER | wx.ALL, 10)
        # Add the message
        message = wx.StaticText(self, -1, msg)
        box2.Add(message, 0, wx.EXPAND | wx.ALIGN_CENTER |
wx.ALIGN_CENTER_VERTICAL | wx.ALL, 10)
        box.Add(box2, 0, wx.EXPAND)
        # Handle layout
        self.SetAutoLayout(True)
        self.SetSizer(box)
        self.Fit()
        self.Layout()
        self.CentreOnScreen()
        # Display the Dialog
        self.Show()
        # Make sure the screen gets fully drawn before continuing.
        wx.Yield()

