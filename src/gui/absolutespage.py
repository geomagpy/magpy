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



class AbsolutePage(wx.Panel):
    #def __init__(self, parent):
        #wx.Panel.__init__(self,parent,-1,size=(100,100))
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.comp = ['xyz', 'hdz', 'idf']
        self.dipathlist = []
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.diLabel = wx.StaticText(self, label="DI files:")
        self.loadDIButton = wx.Button(self,-1,"Load DI data",size=(130,30))
        self.diTextCtrl = wx.TextCtrl(self, value="--")
        self.varioLabel = wx.StaticText(self, label="Variometer:")
        self.defineVarioButton = wx.Button(self,-1,"Select path",size=(130,30))
        self.varioTextCtrl = wx.TextCtrl(self, value="--")
        self.scalarLabel = wx.StaticText(self, label="Scalar:")
        self.defineScalarButton = wx.Button(self,-1,"Select path",size=(130,30))
        self.scalarTextCtrl = wx.TextCtrl(self, value="--")
        self.AnalyzeButton = wx.Button(self,-1,"Analyze",size=(130,30))
        self.advancedLabel = wx.StaticText(self, label="Advanced:")
        self.advancedButton = wx.Button(self,-1,"Set parameter",size=(130,30))
        self.ClearLogButton = wx.Button(self,-1,"Clear Log",size=(130,30))
        self.dilogTextCtrl = wx.TextCtrl(self, wx.ID_ANY, size=(300,250),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)


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
        gridSizer = wx.FlexGridSizer(rows=10, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.diLabel, noOptions),
                 (self.loadDIButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.diTextCtrl, expandOption),
                 (self.varioLabel, noOptions),
                 (self.defineVarioButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.varioTextCtrl, expandOption),
                 (self.scalarLabel, noOptions),
                 (self.defineScalarButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.scalarTextCtrl, expandOption),
                 (self.AnalyzeButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.advancedLabel, noOptions),
                  emptySpace,
                 (self.advancedButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.ClearLogButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)


        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        self.centred_text = wx.StaticText(self, label="DI Analysis Log:")
        mainSizer.Add(self.centred_text, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.dilogTextCtrl, 0, wx.ALIGN_LEFT | wx.ALL, 3)

        self.SetSizerAndFit(mainSizer)

        #boxSizer.Add(self.dilogTextCtrl, 0, wx.ALL|wx.CENTER, 5)
        #boxSizer.Add(self.dilogTextCtrl,expandOption, dict(border=5, flag=wx.ALL))
        #self.SetSizerAndFit(boxSizer)
