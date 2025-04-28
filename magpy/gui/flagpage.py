#!/usr/bin/env python

from magpy.stream import *
import magpy.absolutes as di
from magpy.core import database

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

import wx.lib.masked as masked

# Subclasses for Menu pages and their controls

class FlagPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        flagversion = '2.0'
        fl = flagging.Flags()
        cftdict = fl.FLAGTYPE.get(flagversion)
        self.flagidlist = ["{}: {}".format(key,cftdict.get(key)) for key in cftdict]
        self.labels = ["{}: {}".format(key, fl.FLAGLABEL.get(key)) for key in fl.FLAGLABEL]
        self.currentlabelindex = [i for i, el in enumerate(self.labels) if el.startswith('002')][0]
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        font = wx.Font(10, wx.DEFAULT, wx.NORMAL, wx.BOLD)
        self.flagOptionsLabel = wx.StaticText(self, label="Flagging methods:")
        self.flagOutlierButton = wx.Button(self,-1,"Flag Outlier",size=(160,30))
        self.flagRangeButton = wx.Button(self,-1,"Flag Range",size=(160,30))
        self.flagUltraButton = wx.Button(self,-1,"Experimental Flag",size=(160,30))
        self.flagExtremaLabel = wx.StaticText(self, label="Label extrema:")
        self.flagMinButton = wx.Button(self,-1,"Flag Minimum",size=(160,30))
        self.flagMaxButton = wx.Button(self,-1,"Flag Maximum",size=(160,30))
        self.xCheckBox = wx.CheckBox(self,label="X             ")
        self.yCheckBox = wx.CheckBox(self,label="Y             ")
        self.zCheckBox = wx.CheckBox(self,label="Z             ")
        self.fCheckBox = wx.CheckBox(self,label="F             ")
        self.LabelComboBox = wx.ComboBox(self, choices=self.labels,
            style=wx.CB_DROPDOWN, value=self.labels[self.currentlabelindex],size=(160,-1))
        self.FlagIDComboBox = wx.ComboBox(self, choices=self.flagidlist,
            style=wx.CB_DROPDOWN, value=self.flagidlist[3],size=(160,-1))
        self.flagSelectionButton = wx.Button(self,-1,"Flag Selection",size=(160,30))
        self.flagApplyLabel = wx.StaticText(self, label="Apply or cleanup flags:")
        self.flagDropButton = wx.Button(self,-1,"Drop flagged",size=(160,30))
        self.flagClearButton = wx.Button(self, -1, "Clear flags", size=(160, 30))
        self.flagAcceptButton = wx.Button(self,-1,"Accept modifications",size=(160,30))
        self.flagStorageLabel = wx.StaticText(self, label="Storage operations:")
        self.flagLoadButton = wx.Button(self,-1,"Load flags",size=(160,30))
        self.flagSaveButton = wx.Button(self,-1,"Save flags",size=(160,30))
        self.flagExtraLabel = wx.StaticText(self, label="Information:")
        self.flagmodButton = wx.Button(self,-1,"Flag info",size=(160,30))
        self.annotateCheckBox = wx.CheckBox(self,label="annotate")

        self.flagviewTextCtrl = wx.TextCtrl(self, wx.ID_ANY, size=(330,150),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.flagOptionsLabel.SetFont(font)
        self.flagExtremaLabel.SetFont(font)
        self.flagApplyLabel.SetFont(font)
        self.flagStorageLabel.SetFont(font)
        self.flagExtraLabel.SetFont(font)

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
                    'self.flagUltraButton, dict(flag=wx.ALIGN_CENTER)',
                    'self.flagExtremaLabel, noOptions',
                    '(0,0), noOptions',
                 'self.flagMinButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagMaxButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.xCheckBox, noOptions',
                 'self.yCheckBox, noOptions',
                 'self.zCheckBox, noOptions',
                 'self.fCheckBox, noOptions',
                 'self.LabelComboBox, expandOption',
                 'self.FlagIDComboBox, expandOption',
                    'self.flagApplyLabel, noOptions',
                    '(0,0), noOptions',
                    'self.flagDropButton, dict(flag=wx.ALIGN_CENTER)',
                    'self.flagClearButton, dict(flag=wx.ALIGN_CENTER)',
                    'self.flagAcceptButton, dict(flag=wx.ALIGN_CENTER)',
                    '(0,0), noOptions',
                    'self.flagStorageLabel, noOptions',
                    '(0,0), noOptions',
                    'self.flagLoadButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.flagSaveButton, dict(flag=wx.ALIGN_CENTER)',
                    'self.flagExtraLabel, noOptions',
                    '(0,0), noOptions',
                    'self.annotateCheckBox, noOptions',
                    'self.flagmodButton, dict(flag=wx.ALIGN_CENTER)',
                    '(0,0), noOptions',
                 '(0,0), noOptions']

        mainSizer = wx.BoxSizer(wx.VERTICAL)
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

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        self.centred_text = wx.StaticText(self, label="Flags and labels:")
        mainSizer.Add(self.centred_text, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.flagviewTextCtrl, 0, wx.ALIGN_LEFT | wx.ALL, 3)

        self.SetSizerAndFit(mainSizer)

        #self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.LabelComboBox.Bind(wx.EVT_COMBOBOX, self.OnUpdateLabel)

    def OnUpdateLabel(self, event):
        """
        DESCRIPTION
            update flagtype according to labelid
        :param e:
        :return:
        """
        label = self.LabelComboBox.GetStringSelection()
        labelid = label[:3]
        print ("Changed label to", labelid)
        if 10 <= int(labelid) < 50:
            self.FlagIDComboBox.SetValue(self.flagidlist[4])
        else:
            self.FlagIDComboBox.SetValue(self.flagidlist[3])
