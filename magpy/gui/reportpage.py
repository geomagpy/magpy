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

class ReportPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        #self.parent = parent
        self.createControls()
        self.doLayout()

    def createControls(self):
        self.logger = wx.TextCtrl(self, wx.ID_ANY, size=(330,400),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.saveLoggerButton = wx.Button(self,-1,"Save log",size=(160,30))

    def doLayout(self):
        mainSizer = wx.BoxSizer(wx.VERTICAL)
        mainSizer.Add(self.logger, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        mainSizer.Add(self.saveLoggerButton, 0, wx.ALIGN_LEFT | wx.ALL, 3)
        self.SetSizerAndFit(mainSizer)

    def logMsg(self, message):
        ''' Private method to append a string to the logger text
            control. '''
        #print message
        self.logger.AppendText('%s\n'%message)


