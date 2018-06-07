#!/usr/bin/env python

from magpy.stream import *
from magpy.absolutes import *
from magpy.transfer import *
from magpy.database import *

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

import wx.lib.masked as masked

# Subclasses for Menu pages and their controls

class FlagPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.flagidlist = ['0: normal data',
                        '1: automatically flagged',
                        '2: keep data in any case',
                        '3: remove data',
                        '4: special flag']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.flagOptionsLabel = wx.StaticText(self, label="Flagging methods:")
        self.flagOutlierButton = wx.Button(self,-1,"Flag Outlier",size=(160,30))
        self.flagRangeButton = wx.Button(self,-1,"Flag Range",size=(160,30))
        self.flagMinButton = wx.Button(self,-1,"Flag Minimum",size=(160,30))
        self.flagMaxButton = wx.Button(self,-1,"Flag Maximum",size=(160,30))
        self.xCheckBox = wx.CheckBox(self,label="X             ")
        self.yCheckBox = wx.CheckBox(self,label="Y             ")
        self.zCheckBox = wx.CheckBox(self,label="Z             ")
        self.fCheckBox = wx.CheckBox(self,label="F             ")
        self.FlagIDText = wx.StaticText(self,label="Select Min/Max Flag ID:")
        self.FlagIDComboBox = wx.ComboBox(self, choices=self.flagidlist,
            style=wx.CB_DROPDOWN, value=self.flagidlist[3],size=(160,-1))
        self.flagSelectionButton = wx.Button(self,-1,"Flag Selection",size=(160,30))
        self.flagDropButton = wx.Button(self,-1,"Drop flagged",size=(160,30))
        self.flagLoadButton = wx.Button(self,-1,"Load flags",size=(160,30))
        self.flagSaveButton = wx.Button(self,-1,"Save flags",size=(160,30))
        self.flagClearButton = wx.Button(self,-1,"Clear flags",size=(160,30))


    def doLayout(self):
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = '(0,0), noOptions'

        elemlist = ['self.flagOptionsLabel, noOptions',
                 '(0,0), noOptions',
                 'self.flagOutlierButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagSelectionButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagRangeButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagDropButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagMinButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagMaxButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.xCheckBox, noOptions',
                 'self.yCheckBox, noOptions',
                 'self.zCheckBox, noOptions',
                 'self.fCheckBox, noOptions',
                 'self.FlagIDText, noOptions',
                 'self.FlagIDComboBox, expandOption',
                 'self.flagLoadButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagSaveButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagClearButton, dict(flag=wx.ALIGN_CENTER)',
                 '(0,0), noOptions']

        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        rows = int((len(elemlist)+1)/2.)
        gridSizer = wx.FlexGridSizer(rows=rows, cols=2, vgap=5, hgap=10)

        # modify look: ReDraw connected to radio and check boxes with dates
        # buttons automatically redraw the graph

        #checklist = ['self.'+elem+'CheckBox, noOptions' for elem in KEYLIST]
        #elemlist.extend(checklist)
        #elemlist.append('self.DrawButton, dict(flag=wx.ALIGN_CENTER)')

        # Add the controls to the sizers:
        for elem in elemlist:
            control = elem.split(', ')[0]
            options = elem.split(', ')[1]
            gridSizer.Add(eval(control), **eval(options))

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)
