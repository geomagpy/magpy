#!/usr/bin/env python

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



class AbsolutePage(wx.Panel):
    #def __init__(self, parent):
        #wx.Panel.__init__(self,parent,-1,size=(100,100))
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.comp = ['xyz', 'hdz', 'idf']
        self.choices = ['file','options']
        self.dipathlist = []
        self.extension = '*'
        self.varioext = ['*.*']
        self.scalarext = ['*.*']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.sourceLabel = wx.StaticText(self, label="Data source:")
        self.diLabel = wx.StaticText(self, label="Actions:")
        self.loadDIButton = wx.Button(self,-1,"DI data",size=(160,30))
        self.diSourceLabel = wx.StaticText(self, label="Source: None")
        self.diTextCtrl = wx.TextCtrl(self, value="None",size=(160,40),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.defineVarioScalarButton = wx.Button(self,-1,"Vario/Scalar",size=(160,30))
        self.VarioSourceLabel = wx.StaticText(self, label="Vario: None")
        self.ScalarSourceLabel = wx.StaticText(self, label="Scalar: None")
        self.varioTextCtrl = wx.TextCtrl(self, value="None",size=(160,40),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.defineParameterButton = wx.Button(self,-1,"Analysis parameter",size=(160,30))
        self.parameterRadioBox = wx.RadioBox(self,label="parameter source",choices=self.choices, majorDimension=2, style=wx.RA_SPECIFY_COLS,size=(160,50))
        #self.parameterTextCtrl = wx.TextCtrl(self, value="Default",size=(160,30),
        #                  style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.scalarTextCtrl = wx.TextCtrl(self, value="None",size=(160,40),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.AnalyzeButton = wx.Button(self,-1,"Analyze",size=(160,30))
        self.logLabel = wx.StaticText(self, label="Logging:")
        #self.exportButton = wx.Button(self,-1,"Export...",size=(160,30))
        self.ClearLogButton = wx.Button(self,-1,"Clear Log",size=(160,30))
        self.SaveLogButton = wx.Button(self,-1,"Save Log",size=(160,30))
        self.dilogTextCtrl = wx.TextCtrl(self, wx.ID_ANY, size=(330,200),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        #self.varioExtComboBox = wx.ComboBox(self, choices=self.varioext,
        #         style=wx.CB_DROPDOWN, value=self.varioext[0],size=(160,-1))
        #self.scalarExtComboBox = wx.ComboBox(self, choices=self.scalarext,
        #         style=wx.CB_DROPDOWN, value=self.scalarext[0],size=(160,-1))


    def doLayout(self):

        mainSizer = wx.BoxSizer(wx.VERTICAL)

        # Title
        #self.centred_text = wx.StaticText(self, label="DI Analysis")
        #mainSizer.Add(self.centred_text, 0, wx.ALIGN_CENTRE | wx.ALL, 3)

        # Grids
        #content_sizer = wx.BoxSizer(wx.HORIZONTAL)
        #grid_1 = wx.GridSizer(12, 2, 0, 0)
        #grid_1.AddMany(wx.StaticText(self.panel, label=str(i)) for i in xrange(24))
        #content_sizer.Add(grid_1, 1, wx.EXPAND | wx.ALL, 3)
        #grid_2 = wx.GridSizer(10, 3, 0, 0)
        #grid_2.AddMany(wx.StaticText(self.panel, label=str(i)) for i in xrange(30))
        #content_sizer.Add(grid_2, 1, wx.EXPAND | wx.ALL, 3)

        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=13, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.sourceLabel, noOptions),
                  emptySpace,
                 (self.loadDIButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.diSourceLabel, noOptions),
                 (self.diTextCtrl, expandOption),
                 (self.defineVarioScalarButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.VarioSourceLabel, noOptions),
                 (self.varioTextCtrl, expandOption),
                 (self.ScalarSourceLabel, noOptions),
                 (self.scalarTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.diLabel, noOptions),
                  emptySpace,
                 (self.defineParameterButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.parameterRadioBox, expandOption),
                 (self.AnalyzeButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.logLabel, noOptions),
                  emptySpace,
                 (self.ClearLogButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.SaveLogButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)


        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        self.centred_text = wx.StaticText(self, label="DI Analysis Log:")
        mainSizer.Add(self.centred_text, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.dilogTextCtrl, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        #mainSizer.AddSpacer(60)

        self.SetSizerAndFit(mainSizer)

        #boxSizer.Add(self.dilogTextCtrl, 0, wx.ALL|wx.CENTER, 5)
        #boxSizer.Add(self.dilogTextCtrl,expandOption, dict(border=5, flag=wx.ALL))
        #self.SetSizerAndFit(boxSizer)
