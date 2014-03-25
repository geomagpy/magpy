#!/usr/bin/env python

from stream import *
from absolutes import *
from transfer import *
from database import *

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

from wx.lib.pubsub import Publisher
import wx.lib.masked as masked

# Subclasses for Dialogs called by magpy gui     

class OpenWebAddressDialog(wx.Dialog):
    """
    Dialog for File Menu - Load URL
    """
    
    def __init__(self, parent, title):
        super(OpenWebAddressDialog, self).__init__(parent=parent, 
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()
        self.bindControls()
        
    # Widgets
    def createControls(self):
        # single anaylsis
        # db = MySQLdb.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.urlLabel = wx.StaticText(self, label="Insert address (e.g. 'ftp://.../' for all files, or 'ftp://.../data.dat' for a single file)")
        self.urlTextCtrl = wx.TextCtrl(self, value="ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc")
        self.okButton = wx.Button(self, wx.ID_OK, label='Connect')
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
        for control, options in \
                [(self.urlLabel, noOptions),
                  emptySpace,
                 (self.urlTextCtrl, expandOption),
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
        self.passwdTextCtrl = wx.TextCtrl(self, value="Secret")
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

