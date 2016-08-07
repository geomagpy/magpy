#!/usr/bin/env python

from magpy.stream import *
from magpy.absolutes import *
from magpy.transfer import *
from magpy.database import *

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

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
        # db = MySQLdb.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.urlLabel = wx.StaticText(self, label="Insert address (e.g. 'ftp://.../' for all files, or 'ftp://.../data.dat' for a single file)",size=(500,30))
        self.urlTextCtrl = wx.TextCtrl(self, value=self.favorites[0],size=(500,30))
        self.favoritesLabel = wx.StaticText(self, label="Favorites:",size=(160,30))
        self.getFavsComboBox = wx.ComboBox(self, choices=self.favorites,
            style=wx.CB_DROPDOWN, value=self.favorites[0],size=(160,30))
        self.addFavsButton = wx.Button(self, label='Add to favorites',size=(160,30))
        self.dropFavsButton = wx.Button(self, label='Remove from favorites',size=(160,30))

        self.okButton = wx.Button(self, wx.ID_OK, label='Connect')
        self.closeButton = wx.Button(self, label='Cancel',size=(160,30))
        

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=4, cols=3, vgap=10, hgap=10)

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
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)


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

    def OnClose(self, e):
        self.Destroy()


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
        self.bindControls()

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
        self.closeButton = wx.Button(self, label='Cancel',size=(160,30))

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

        # Add the controls to the sizers:
        for elem in elemlist:
            control = elem.split(', ')[0]
            options = elem.split(', ')[1]
            gridSizer.Add(eval(control), **eval(options))

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


class ExportDataDialog(wx.Dialog):
    """
    Dialog for Exporting data
    """
    def __init__(self, parent, title):
        super(ExportDataDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # single anaylsis
        # db = MySQLdb.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.selectDirButton = wx.Button(self, label='Select Directory')
        self.selectedTextCtrl = wx.TextCtrl(self, value="")
        self.formatComboBox = wx.ComboBox(self, choices=PYMAG_SUPPORTED_FORMATS,
            style=wx.CB_DROPDOWN, value=PYMAG_SUPPORTED_FORMATS[0])
        self.selectLabel = wx.StaticText(self, label="Export data to ...")
        self.nameLabel = wx.StaticText(self, label="File name ...")
        self.beginTextCtrl = wx.TextCtrl(self, value="MyFile_")
        self.dateComboBox = wx.ComboBox(self, choices=['2000-11-22','20001122','NOV2200'],
            style=wx.CB_DROPDOWN, value='2000-11-22')
        self.endTextCtrl = wx.TextCtrl(self, value=".txt")
        self.coverageLabel = wx.StaticText(self, label="File covers ...")
        self.coverageComboBox = wx.ComboBox(self, choices=['hour','day','month','year','all'],
            style=wx.CB_DROPDOWN, value='day')
        self.modeLabel = wx.StaticText(self, label="Write mode ...")
        self.modeComboBox = wx.ComboBox(self, choices=['replace','append', 'overwrite', 'skip'],
            style=wx.CB_DROPDOWN, value='overwrite')
        self.okButton = wx.Button(self, wx.ID_OK, label='Write')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=3, cols=3, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.selectLabel, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.selectDirButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.selectedTextCtrl, expandOption),
                 (self.formatComboBox, expandOption),
                 (self.nameLabel, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.beginTextCtrl, expandOption),
                 (self.dateComboBox, expandOption),
                 (self.endTextCtrl, expandOption),
                 (self.coverageLabel, noOptions),
                 (self.modeLabel, noOptions),
                  emptySpace,
                 (self.coverageComboBox, expandOption),
                 (self.modeComboBox, expandOption),
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.selectDirButton.Bind(wx.EVT_BUTTON, self.OnSelectDirButton)

    def OnSelectDirButton(self, event):
        dialog = wx.DirDialog(None, "Choose a directory:",'/srv',style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            #self.ReactivateStreamPage()
            self.selectedTextCtrl.SetValue(dialog.GetPath())
        #self.menu_p.rep_page.logMsg('- Directory for file export defined')
        dialog.Destroy()

    def OnClose(self, e):
        self.Destroy()


class DatabaseConnectDialog(wx.Dialog):
    """
    Dialog for Database Menu - Connect MySQL
    """

    def __init__(self, parent, title):
        super(DatabaseConnectDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # single anaylsis
        # db = MySQLdb.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.hostLabel = wx.StaticText(self, label="Host")
        self.hostTextCtrl = wx.TextCtrl(self, value="localhost")
        self.userLabel = wx.StaticText(self, label="User")
        self.userTextCtrl = wx.TextCtrl(self, value="Max")
        self.passwdLabel = wx.StaticText(self, label="Password")
        self.passwdTextCtrl = wx.TextCtrl(self, value="Secret",style=wx.TE_PASSWORD)
        self.dbLabel = wx.StaticText(self, label="Database")
        self.dbTextCtrl = wx.TextCtrl(self, value="MyDB")
        self.okButton = wx.Button(self, wx.ID_OK, label='Connect')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=3, cols=4, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.hostLabel, noOptions),
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
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


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
        self.bindControls()

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:")
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.datalst[0])
        self.okButton = wx.Button(self, wx.ID_OK, label='Open')
        self.closeButton = wx.Button(self, label='Cancel')


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
                [(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


class OptionsInitDialog(wx.Dialog):
    """
    Dialog for Database Menu - Connect MySQL
    """

    def __init__(self, parent, title):
        super(OptionsInitDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # single anaylsis
        # db = MySQLdb.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.hostLabel = wx.StaticText(self, label="Host")
        self.hostTextCtrl = wx.TextCtrl(self,value="localhost")
        self.userLabel = wx.StaticText(self, label="User")
        self.userTextCtrl = wx.TextCtrl(self, value="Max")
        self.passwdLabel = wx.StaticText(self, label="Password")
        self.passwdTextCtrl = wx.TextCtrl(self, value="Secret",style=wx.TE_PASSWORD)
        self.dbLabel = wx.StaticText(self, label="Database")
        self.dbTextCtrl = wx.TextCtrl(self, value="MyDB")
        self.dirnameLabel = wx.StaticText(self, label="Default directory")
        self.dirnameTextCtrl = wx.TextCtrl(self, value=".")

        self.dipathlistLabel = wx.StaticText(self, label="Default DI path")
        self.divariopathLabel = wx.StaticText(self, label="DI variometer")
        self.discalarpathLabel = wx.StaticText(self, label="DI scalar")
        self.diexpDLabel = wx.StaticText(self, label="expected Dec")
        self.diexpILabel = wx.StaticText(self, label="expected Inc")
        self.stationidLabel = wx.StaticText(self, label="Station ID")
        self.diidLabel = wx.StaticText(self, label="DI ID")
        self.ditypeLabel = wx.StaticText(self, label="DI Type") #abstype
        self.diazimuthLabel = wx.StaticText(self, label="Azimuth")
        self.dipierLabel = wx.StaticText(self, label="Pier")
        self.dialphaLabel = wx.StaticText(self, label="Alpha")
        self.dideltaFLabel = wx.StaticText(self, label="Delta F")
        self.didbaddLabel = wx.StaticText(self, label="Add to DB")
        self.dipathlistTextCtrl = wx.TextCtrl(self, value="/srv/archive/")
        self.divariopathTextCtrl = wx.TextCtrl(self, value="/srv/archive/")
        self.discalarpathTextCtrl = wx.TextCtrl(self, value="/srv/archive/")
        self.diexpDTextCtrl = wx.TextCtrl(self, value="3.0")
        self.diexpITextCtrl = wx.TextCtrl(self, value="64.0")
        self.stationidTextCtrl = wx.TextCtrl(self, value="wic")
        self.diidTextCtrl = wx.TextCtrl(self, value="")
        self.ditypeTextCtrl = wx.TextCtrl(self, value="Normal") #abstype
        self.diazimuthTextCtrl = wx.TextCtrl(self, value="")
        self.dipierTextCtrl = wx.TextCtrl(self, value="A2")
        self.dialphaTextCtrl = wx.TextCtrl(self, value="")
        self.dideltaFTextCtrl = wx.TextCtrl(self, value="")
        self.didbaddTextCtrl = wx.TextCtrl(self, value="False")

        self.closeButton = wx.Button(self, label='Cancel')
        self.saveButton = wx.Button(self, wx.ID_OK, label='Save')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=4, cols=4, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.hostLabel, noOptions),
                 (self.userLabel, noOptions),
                 (self.passwdLabel, noOptions),
                 (self.dbLabel, noOptions),
                 (self.hostTextCtrl, expandOption),
                 (self.userTextCtrl, expandOption),
                 (self.passwdTextCtrl, expandOption),
                 (self.dbTextCtrl, expandOption),
                 (self.dirnameLabel, noOptions),
                 (self.dipathlistLabel, noOptions),
                 (self.divariopathLabel, noOptions),
                 (self.discalarpathLabel, noOptions),
                 (self.dirnameTextCtrl, expandOption),
                 (self.dipathlistTextCtrl, expandOption),
                 (self.divariopathTextCtrl, expandOption),
                 (self.discalarpathTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.ditypeLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.ditypeTextCtrl, expandOption),
                 (self.stationidLabel, noOptions),
                 (self.diidLabel, noOptions),
                 (self.diexpDLabel, noOptions),
                 (self.diexpILabel, noOptions),
                 (self.stationidTextCtrl, expandOption),
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
                 (self.didbaddLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.didbaddTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.saveButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


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
        self.bindControls()

    # Widgets
    def createControls(self):

        self.keyLabel = wx.StaticText(self, label="Available keys:")
        self.key1ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=self.keylst[0])
        self.compare1ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0])
        self.value1TextCtrl = wx.TextCtrl(self, value="")
        self.logic2ComboBox = wx.ComboBox(self, choices=self.logic2lst,
            style=wx.CB_DROPDOWN, value=self.logic2lst[0])
        if len(self.keylst) > 1:
            val2 =  self.keylst[1]
        else:
            val2 = ''
        self.key2ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=val2)
        self.compare2ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0])
        self.value2TextCtrl = wx.TextCtrl(self, value="")
        self.logic3ComboBox = wx.ComboBox(self, choices=self.logic3lst,
            style=wx.CB_DROPDOWN, value=self.logic3lst[0])
        if len(self.keylst) > 2:
            val3 =  self.keylst[2]
        else:
            val3 = ''
        self.key3ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=val3)
        self.compare3ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0])
        self.value3TextCtrl = wx.TextCtrl(self, value="")
        self.okButton = wx.Button(self, wx.ID_OK, label='Extract')
        self.closeButton = wx.Button(self, label='Cancel')


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=7, cols=4, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [emptySpace,
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
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


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
        self.bindControls()

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
        self.closeButton = wx.Button(self, label='Cancel')

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

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


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
        self.bindControls()

    # Widgets
    def createControls(self):
        for elem in self.optdict:
            print (elem, self.optdict[elem])
            exec('self.'+elem+'Text = wx.StaticText(self,label="'+elem+'")')
            exec('self.'+elem+'TextCtrl = wx.TextCtrl(self, value="'+self.optdict[elem]+'")')
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=len(self.optdict), cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst = [[eval('(self.'+elem+'Text, noOptions)'),eval('(self.'+elem+'TextCtrl, expandOption)')] for elem in self.optdict]
        contlst = [y for x in contlst for y in x]
  
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


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
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.ThresholdText = wx.StaticText(self,label="Threshold")
        self.TimerangeText = wx.StaticText(self,label="Window width")
        self.UnitText = wx.StaticText(self,label="seconds")
        self.ThresholdTextCtrl = wx.TextCtrl(self, value=self.threshold)
        self.TimerangeTextCtrl = wx.TextCtrl(self, value=self.timerange)
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=4, cols=3, vgap=10, hgap=10)

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
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


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
            style=wx.CB_DROPDOWN, value=self.shownkeys[self.shownkeys.index(self.selectedkey)])
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
            style=wx.CB_DROPDOWN, value=self.flagidlist[3])
        self.CommentText = wx.StaticText(self,label="Comment:")
        self.CommentTextCtrl = wx.TextCtrl(self, value=self.comment,size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(160,30))
        self.closeButton = wx.Button(self, label='Cancel',size=(160,30))
        self.rangeRadioBox = wx.RadioBox(self,
            label="Select flagging range type:",
            choices=self.rangetype, majorDimension=2, style=wx.RA_SPECIFY_COLS)

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=8, cols=4, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        #self.okButton.Bind(wx.EVT_BUTTON, self.OnOK)
        self.Bind(wx.EVT_RADIOBOX, self.OnChangeGroup, self.rangeRadioBox)
        self.Bind(wx.EVT_COMBOBOX, self.OnChangeSelection, self.SelectKeyComboBox)

    def OnOK(self, e):
        if self.comment == '':
            # ask for comment
            pass
        else:
            # send OK
            pass

    def OnClose(self, e):
        self.Destroy()

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
    def __init__(self, parent, title):
        super(StreamFlagSelectionDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.SelectionTextCtrl = wx.TextCtrl(self, value='test',size=(160,160))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(160,30))
        self.closeButton = wx.Button(self, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=8, cols=1, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.SelectionTextCtrl, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


class StreamLoadFlagDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Loading Flagging data from file or DB
    """
    def __init__(self, parent, title, db, sensorid):
        super(StreamLoadFlagDialog, self).__init__(parent=parent,
            title=title, size=(300, 300))
        self.flaglist = []
        self.sensorid = sensorid
        self.db = db
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.loadDBButton = wx.Button(self, label='Load from DB')
        self.loadFileButton = wx.Button(self, label='Load file')
        self.closeButton = wx.Button(self, label='Cancel')
        if not self.db:
            self.loadDBButton.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=3, cols=2, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.loadDBButton.Bind(wx.EVT_BUTTON, self.OnLoadDB)
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnLoadFile)

    def OnClose(self, e):
        self.Destroy()

    def OnLoadDB(self, e):
        self.flaglist = db2flaglist(self.db, self.sensorid)
        dlg = wx.MessageDialog(self, "Flags for {} loaded from DB!\nFLAGS table contained {} inputs\n".format(self.sensorid,len(self.flaglist)),"FLAGS obtained from DB", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.Destroy()

    def OnLoadFile(self, e):
        openFileDialog = wx.FileDialog(self, "Open", "", "", 
                                       "Flaglist (*.pkl)|*.pkl", 
                                       wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        openFileDialog.ShowModal()
        flagname = openFileDialog.GetPath()
        try:
            self.flaglist = loadflags(flagname,sensorid=self.sensorid)
        except:
            self.flaglist = [] 
        openFileDialog.Destroy()
        self.Destroy()


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
        self.closeButton = wx.Button(self, label='Cancel')
        if not self.db:
            self.saveDBButton.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=3, cols=2, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.saveDBButton.Bind(wx.EVT_BUTTON, self.OnSaveDB)
        self.saveFileButton.Bind(wx.EVT_BUTTON, self.OnSaveFile)

    def OnClose(self, e):
        self.Destroy()

    def OnSaveDB(self, e):
        flaglist2db(self.db, self.flaglist)
        dlg = wx.MessageDialog(self, "Flags stored in connected DB!\nFLAGS table extended with {} inputs\n".format(len(self.flaglist)),"FLAGS added to DB", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.Destroy()

    def OnSaveFile(self, e):
        saveFileDialog = wx.FileDialog(self, "Save As", "", "", 
                                       "Flaglist (*.pkl)|*.pkl", 
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        saveFileDialog.ShowModal()
        flagname = saveFileDialog.GetPath()
        saveFileDialog.Destroy()
        print (flagname)
        saveflags(self.flaglist,flagname)
        self.Destroy()

# ###################################################
#    Meta page
# ###################################################

class MetaDataDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, header, layer):
        super(MetaDataDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        self.header = header
        self.list = []
        self.layer=layer
        self.createControls()
        self.cnts=[0,0]
        self.doLayout()
        self.bindControls()

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
                if not isinstance(value, str) or '[' in value:
                     print ("not a string")
                     try:
                         try:
                             float(value)
                         except:
                             value = 'object with complex data'
                     except:
                         value = 'object with complex data'
                cnt += 1

                label = self.AppendLabel(key,label)
                exec('self.'+key+'Text = wx.StaticText(self,label="'+label+'")')
                exec('self.'+key+'TextCtrl = wx.TextCtrl(self, value="'+value+'",size=(160,30))')
                if value.startswith('object with complex'):
                    exec('self.'+key+'TextCtrl.Disable()')
        self.cnts = [colcnt, cnt]

        self.legendText = wx.StaticText(self,label="(1: IAF, 2: IAGA, 3: IMAGCDF)")
        self.okButton = wx.Button(self, wx.ID_OK, label='Update')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=self.cnts[1], cols=6, vgap=10, hgap=10)

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
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()

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

    def __init__(self, parent, title, fitfunc, fitknots, fitdegree):
        super(AnalysisFitDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.fitfunc = fitfunc
        self.funclist = ['spline','polynomial']
        self.fitknots = fitknots
        self.fitdegree = fitdegree
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.funcLabel = wx.StaticText(self, label="Fit function:")
        self.funcComboBox = wx.ComboBox(self, choices=self.funclist,
            style=wx.CB_DROPDOWN, value=self.fitfunc)
        self.knotsLabel = wx.StaticText(self, label="Knots [e.g. 0.5  (0..1)] (spline only):")
        self.knotsTextCtrl = wx.TextCtrl(self, value=self.fitknots)
        self.degreeLabel = wx.StaticText(self, label="Degree [e.g. 1, 2, 345, etc.] (polynomial only):")
        self.degreeTextCtrl = wx.TextCtrl(self, value=self.fitdegree)
        self.okButton = wx.Button(self, wx.ID_OK, label='Fit')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=8, cols=1, vgap=10, hgap=10)

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
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


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
        self.bindControls()

    # Widgets
    def createControls(self):
        self.filtertypeLabel = wx.StaticText(self, label="Select window:")
        self.filtertypeComboBox = wx.ComboBox(self, choices=self.windowlist,
            style=wx.CB_DROPDOWN, value=self.filtertype)
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
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=8, cols=2, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


class AnalysisOffsetDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, keylst, xlimits):
        super(AnalysisOffsetDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.keylst = keylst
        self.choices = ['all','timerange']
        self.start = self._pydate2wxdate(xlimits[0])
        self.end = self._pydate2wxdate(xlimits[1])
        self.starttime = datetime.strftime(xlimits[0], "%H:%M:%S")
        self.endtime = datetime.strftime(xlimits[1], "%H:%M:%S")
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
        self.timeshiftTextCtrl = wx.TextCtrl(self, value='0',size=(160,30))

        self.StartDateLabel = wx.StaticText(self, label="Starting:",size=(160,30))
        self.StartDatePicker = wx.DatePickerCtrl(self, dt=self.start,size=(160,30))
        self.StartTimeTextCtrl = wx.TextCtrl(self, value=self.starttime,size=(160,30))
        self.EndDateLabel = wx.StaticText(self, label="Ending:",size=(160,30))
        self.EndDatePicker = wx.DatePickerCtrl(self, dt=self.end,size=(160,30))
        self.EndTimeTextCtrl = wx.TextCtrl(self, value=self.endtime,size=(160,30))

        for elem in self.keylst:
            exec('self.'+elem+'Label = wx.StaticText(self,label="'+elem+'")')
            exec('self.'+elem+'TextCtrl = wx.TextCtrl(self,value="")')
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, label='Cancel')

        self.StartDatePicker.Disable()
        self.StartTimeTextCtrl.Disable()
        self.EndDatePicker.Disable()
        self.EndTimeTextCtrl.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=2, cols=2, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.Bind(wx.EVT_RADIOBOX, self.OnChangeRange, self.offsetRadioBox)
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()

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
        self.bindControls()

    # Widgets
    def createControls(self):
        self.alphaLabel = wx.StaticText(self,label="Alpha")
        self.alphaTextCtrl = wx.TextCtrl(self,value="")
        self.betaLabel = wx.StaticText(self,label="Beta")
        self.betaTextCtrl = wx.TextCtrl(self,value="")
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=3, cols=2, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


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
        self.closeButton = wx.Button(self, label='Cancel')
        self.loadDBButton.Disable()
        self.loadRemoteButton.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=8, cols=1, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnLoadDIFiles)
        self.loadDBButton.Bind(wx.EVT_BUTTON, self.OnLoadDIDB)
        self.loadRemoteButton.Bind(wx.EVT_BUTTON, self.OnLoadDIRemote)

    def OnClose(self, e):
        self.Destroy()

    def OnLoadDIFiles(self,e):
        self.difiledirname = ''
        stream = DataStream()
        dlg = wx.FileDialog(self, "Choose file(s)", self.dirname, "", "*.*", wx.MULTIPLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.pathlist = dlg.GetPaths()
        dlg.Destroy()
        self.Destroy()

    def OnLoadDIDB(self,e):
        #self.dirname = ''
        stream = DataStream()
        dlg = wx.FileDialog(self, "Choose a file", self.dirname, "", "*.*", wx.MULTIPLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.pathlist = dlg.GetPaths()
        dlg.Destroy()
        self.Destroy()

    def OnLoadDIRemote(self,e):
        self.dirname = ''
        stream = DataStream()
        dlg = wx.FileDialog(self, "Choose a file", self.dirname, "", "*.*", wx.MULTIPLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.pathlist = dlg.GetPaths()
        dlg.Destroy()
        self.Destroy()


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
        self.loadFileButton = wx.Button(self,-1,"Get Path",size=(120,30))
        self.getdbLabel = wx.StaticText(self, label="2) Access database:")
        self.remoteLabel = wx.StaticText(self, label="3) Access remote files:")

        self.okButton = wx.Button(self, wx.ID_OK, label='Use')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=8, cols=1, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnDefineVario)

    def OnClose(self, e):
        self.Destroy()

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
        self.loadFileButton = wx.Button(self,-1,"Get Path",size=(120,30))
        self.getdbLabel = wx.StaticText(self, label="2) Access database:")
        self.remoteLabel = wx.StaticText(self, label="3) Access remote files:")

        self.okButton = wx.Button(self, wx.ID_OK, label='Use')
        self.closeButton = wx.Button(self, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=8, cols=1, vgap=10, hgap=10)

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
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnDefineScalar)

    def OnClose(self, e):
        self.Destroy()


    def OnDefineScalar(self,e):
        dialog = wx.DirDialog(None, "Choose a directory with scalar data:",self.scalarpath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.path = dialog.GetPath()
            self.scalarpath = self.path
        dialog.Destroy()


class DISetParameterDialog(wx.Dialog):
    """
    Dialog for Parameter selection - Di analysis
    """

    def __init__(self, parent, title):
        super(DISetParameterDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # single anaylsis
        # db = MySQLdb.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.azimuthLabel = wx.StaticText(self, label="Azimuth")
        self.azimuthTextCtrl = wx.TextCtrl(self,value="")
        self.abstypeLabel = wx.StaticText(self, label="Absolute type (DI/AutoDIF)")
        self.abstypeTextCtrl = wx.TextCtrl(self, value="DI")
        self.pierLabel = wx.StaticText(self, label="Pier")
        self.pierTextCtrl = wx.TextCtrl(self, value="")
        self.alphaLabel = wx.StaticText(self, label="Horizontal rotation")
        self.alphaTextCtrl = wx.TextCtrl(self, value="0.0")
        self.deltaFLabel = wx.StaticText(self, label="Delta F")
        self.deltaFTextCtrl = wx.TextCtrl(self, value="0.0")
        self.expDLabel = wx.StaticText(self, label="Expected D")
        self.expDTextCtrl = wx.TextCtrl(self, value="2.0")

        self.closeButton = wx.Button(self, label='Cancel')
        self.okButton = wx.Button(self, wx.ID_OK, label='OK')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=4, cols=3, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.azimuthLabel, noOptions),
                 (self.abstypeLabel, noOptions),
                 (self.pierLabel, noOptions),
                 (self.azimuthTextCtrl, expandOption),
                 (self.abstypeTextCtrl, expandOption),
                 (self.pierTextCtrl, expandOption),
                 (self.alphaLabel, noOptions),
                 (self.deltaFLabel, noOptions),
                 (self.expDLabel, noOptions),
                 (self.alphaTextCtrl, expandOption),
                 (self.deltaFTextCtrl, expandOption),
                 (self.expDTextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


class InputSheetDialog(wx.Dialog):
    """
    DESCRITPTION
        InputDialog for DI data
    """

    def __init__(self, parent, title, layout, path, defaults,cdate):
        super(InputSheetDialog, self).__init__(parent=parent,
            title=title, size=(800, 100))
        #self.sw = wx.ScrolledWindow(self)
        #self.sw.SetScrollbars(20,20,55,40)
        self.scroll = wx.ScrollBar(self,-1,style=wx.SB_VERTICAL)
        self.path = path
        self.layout = layout
        self.defaults = defaults
        self.cdate = cdate
        self.units = ['degree','gon']
        #print (self.layout['order'][2:6])
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # ##### Header Block (fix - 5 columns)
        # - Load line
        self.loadButton = wx.Button(self,-1,"Open DI data",size=(160,30))
        # - Header
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
        self.PillarTextCtrl = wx.TextCtrl(self, value=self.defaults['pier'],size=(160,30))
        self.TempLabel = wx.StaticText(self, label="Temperature:",size=(160,30))
        self.TempTextCtrl = wx.TextCtrl(self, value="",size=(160,30))
        self.UnitLabel = wx.StaticText(self, label="Select Units:",size=(160,30))
        #self.UnitLabel = wx.StaticText(self, label="Select Units:",size=(160,30))

        # - Mire A
        self.AmireLabel = wx.StaticText(self, label="Azimuth measurements:",size=(160,30))
        self.AmireUpLabel = wx.StaticText(self, label="Sensor Up:",size=(160,30))
        self.AmireUp1TextCtrl = wx.TextCtrl(self, value="0.000 or 00:00:00.00",size=(160,30))
        self.AmireUp2TextCtrl = wx.TextCtrl(self, value="0.000 or 00:00:00.00",size=(160,30))
        self.AmireDownLabel = wx.StaticText(self, label="Sensor Down:",size=(160,30))
        self.AmireDown1TextCtrl = wx.TextCtrl(self, value="0.000 or 00:00:00.00",size=(160,30))
        self.AmireDown2TextCtrl = wx.TextCtrl(self, value="0.000 or 00:00:00.00",size=(160,30))

        # - Horizonatl Block
        self.HorizontalLabel = wx.StaticText(self, label="Horizontal measurements:",size=(160,30))
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
        self.ED1GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.ED1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.ED2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.ED2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.ED2GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.ED2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.WDLabel = wx.StaticText(self, label="West(Sensor Down)",size=(160,30))
        self.WD1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.WD1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.WD1GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.WD1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.WD2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.WD2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.WD2GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
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
        self.BmireLabel = wx.StaticText(self, label="Azimuth measurements:",size=(160,30))
        self.BmireUpLabel = wx.StaticText(self, label="Sensor Up:",size=(160,30))
        self.BmireUp1TextCtrl = wx.TextCtrl(self, value="0.000 or 00:00:00.00",size=(160,30))
        self.BmireUp2TextCtrl = wx.TextCtrl(self, value="0.000 or 00:00:00.00",size=(160,30))
        self.BmireDownLabel = wx.StaticText(self, label="Sensor Down:",size=(160,30))
        self.BmireDown1TextCtrl = wx.TextCtrl(self, value="0.000 or 00:00:00.00",size=(160,30))
        self.BmireDown2TextCtrl = wx.TextCtrl(self, value="0.000 or 00:00:00.00",size=(160,30))

        # - Vertical Block
        """
        self.VerticalLabel = wx.StaticText(self, label="Vertical measurements:",size=(160,30))
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
        self.ED1GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.ED1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.ED2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.ED2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.ED2GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.ED2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.WDLabel = wx.StaticText(self, label="West(Sensor Down)",size=(160,30))
        self.WD1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.WD1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.WD1GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.WD1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.WD2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.WD2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,30))
        self.WD2GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,30))
        self.WD2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,30))
        self.EU1GCTextCtrl.Disable()
        self.EU2GCTextCtrl.Disable()
        self.ED1GCTextCtrl.Disable()
        self.ED2GCTextCtrl.Disable()
        self.WU1GCTextCtrl.Disable()
        self.WU2GCTextCtrl.Disable()
        self.WD1GCTextCtrl.Disable()
        self.WD2GCTextCtrl.Disable()
        """

        self.ln = wx.StaticLine(self, -1, style=wx.LI_HORIZONTAL,size=(800,10))
        self.okButton = wx.Button(self, wx.ID_OK, label='Use')
        self.closeButton = wx.Button(self, label='Cancel')


    def doLayout(self):
        #mainSizer = wx.BoxSizer(wx.VERTICAL)
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((160, 0), noOptions)

        # Load elements
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        gridSizer = wx.FlexGridSizer(rows=40, cols=5, vgap=10, hgap=10)
        contlst=[emptySpace]
        contlst.append(emptySpace)
        contlst.append((self.loadButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)

        # Header elements
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
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.TempTextCtrl, expandOption))

        # Mire elements
        contlst.append((self.AmireLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        blMU = []
        blMU.append((self.AmireUpLabel, noOptions))
        blMU.append((self.AmireUp1TextCtrl, expandOption))
        blMU.append((self.AmireUp2TextCtrl, expandOption))
        blMU.append(emptySpace)
        blMU.append(emptySpace)
        blMD = []
        blMD.append((self.AmireDownLabel, noOptions))
        blMD.append((self.AmireDown1TextCtrl, expandOption))
        blMD.append((self.AmireDown2TextCtrl, expandOption))
        blMD.append(emptySpace)
        blMD.append(emptySpace)
        for el in self.layout['order'][0:2]:
            contlst.extend(eval('bl'+str(el)))

        blEU = []
        blEU.append((self.EULabel, noOptions))
        blEU.append((self.EU1TimeTextCtrl, expandOption))
        blEU.append((self.EU1AngleTextCtrl, expandOption))
        blEU.append((self.EU1GCTextCtrl, expandOption))
        blEU.append((self.EU1ResidualTextCtrl, expandOption))
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

        # Mire elements
        contlst.append((self.BmireLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        blMU = []
        blMU.append((self.BmireUpLabel, noOptions))
        blMU.append((self.BmireUp1TextCtrl, expandOption))
        blMU.append((self.BmireUp2TextCtrl, expandOption))
        blMU.append(emptySpace)
        blMU.append(emptySpace)
        blMD = []
        blMD.append((self.BmireDownLabel, noOptions))
        blMD.append((self.BmireDown1TextCtrl, expandOption))
        blMD.append((self.BmireDown2TextCtrl, expandOption))
        blMD.append(emptySpace)
        blMD.append(emptySpace)
        for el in self.layout['order'][0:2]:
            contlst.extend(eval('bl'+str(el)))

        """
        for control, options in contlst:
            grid4Sizer.Add(control, **options)
        for control, options in \
                [(grid4Sizer, dict(border=5, flag=wx.ALL))]:
            box4Sizer.Add(control, **options)
        mainSizer.Add(box4Sizer, 1, wx.EXPAND)

        # Bottom elements
        box7Sizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        grid7Sizer = wx.FlexGridSizer(rows=1, cols=2, vgap=10, hgap=10)
        contlst=[]
        """
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        for control, options in contlst:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        boxSizer.Add(self.scroll, 0, wx.EXPAND|wx.GROW)
        self.SetScrollbar(0,1000,1000,1000)
        #boxSizer.Add(self.scroll, 0, wx.EXPAND|wx.GROW)

        """
        background_sizer = wx.BoxSizer(wx.HORIZONTAL)
        background_sizer.AddSizer(boxSizer, 1, wx.EXPAND|wx.GROW|wx.ALL, 2)
        background_sizer.Add(self.scroll, 0, wx.EXPAND|wx.GROW)
        b.SetScrollbar(0,1000,1000,1000)
        self.SetSizerAndFit(background_sizer)
        #background_sizer.Fit(self)
        """
        self.SetSizerAndFit(boxSizer)
        #self.SetSizerAndFit(mainSizer)


    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.loadButton.Bind(wx.EVT_BUTTON, self.OnLoadDI)

    def OnClose(self, e):
        self.Destroy()

    def OnLoadDI(self,e):
        dialog = wx.DirDialog(None, "Choose a directory with scalar data:",self.scalarpath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.path = dialog.GetPath()
            self.scalarpath = self.path
        dialog.Destroy()

# ###################################################
#    Monitor page
# ###################################################


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
        self.bindControls()

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:")
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.datalst[0])
        self.okButton = wx.Button(self, wx.ID_OK, label='Open')
        self.closeButton = wx.Button(self, label='Cancel')

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
                [(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()

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
        self.bindControls()

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:")
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.datalst[0])
        self.okButton = wx.Button(self, wx.ID_OK, label='Open')
        self.closeButton = wx.Button(self, label='Cancel')

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
                [(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()


