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

class FlaggingPage(wx.Panel):
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
        self.flagOutlierButton = wx.Button(self,-1,"Flag Outlier",size=(160,25))
        self.flagRangeButton = wx.Button(self,-1,"Flag Range",size=(160,25))
        self.flagMinButton = wx.Button(self,-1,"Flag Minimum",size=(160,25))
        self.flagMaxButton = wx.Button(self,-1,"Flag Maximum",size=(160,25))
        self.xCheckBox = wx.CheckBox(self,label="X             ")
        self.yCheckBox = wx.CheckBox(self,label="Y             ")
        self.zCheckBox = wx.CheckBox(self,label="Z             ")
        self.fCheckBox = wx.CheckBox(self,label="F             ")
        self.flagIDText = wx.StaticText(self,label="Select Min/Max Flag ID:")
        self.flagIDComboBox = wx.ComboBox(self, choices=self.flagidlist,
            style=wx.CB_DROPDOWN, value=self.flagidlist[3],size=(160,-1))
        self.flagSelectionButton = wx.Button(self,-1,"Flag Selection",size=(160,25))
        self.flagDropButton = wx.Button(self,-1,"Drop flagged",size=(160,25))
        self.flagLoadButton = wx.Button(self,-1,"Load flags",size=(160,25))
        self.flagSaveButton = wx.Button(self,-1,"Save flags",size=(160,25))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        elemlist = [(self.flagOptionsLabel, dict()),
                 ((0,0), dict()),
                 (self.flagOutlierButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.flagSelectionButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.flagRangeButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.flagDropButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.flagMinButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.flagMaxButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.xCheckBox, dict()),
                 (self.yCheckBox, dict()),
                 (self.zCheckBox, dict()),
                 (self.fCheckBox, dict()),
                 (self.flagIDText, dict()),
                 (self.flagIDComboBox, dict(flag=wx.EXPAND)),
                 (self.flagLoadButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.flagSaveButton, dict(flag=wx.ALIGN_CENTER)),]
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
