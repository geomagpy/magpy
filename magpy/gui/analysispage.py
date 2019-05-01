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



class AnalysisPage(wx.Panel):
    #def __init__(self, parent):
    #    wx.Panel.__init__(self,parent,-1,size=(100,100))
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.filterlist = ['flat','barthann','bartlett','blackman','blackmanharris','bohman','boxcar','cosine','flattop','hamming','hann','nuttall','parzen','triang','gaussian','wiener','spline','butterworth']
        self.filterlength = ['second','minute','hour','day','month','year','userdefined']
        self.comp = ['xyz', 'hdz', 'idf']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # TODO Methods:
        # filter, derivative, offset, fit, baseline, k_fmi, get_min, get_max, mean, delta_f, rotation, spectrogam, powerspec, smooth

        self.head1Label = wx.StaticText(self, label="Basic methods:")
        # derivative, fit, rotation
        self.head2Label = wx.StaticText(self, label="Get values:")
        # mean, max, min
        self.head3Label = wx.StaticText(self, label="Manipulation:")
        # filter, smooth, offset
        self.head4Label = wx.StaticText(self, label="Geomagnetic methods:")
        # frequency range
        self.head5Label = wx.StaticText(self, label="Frequency range:")
        #self.head5Label = wx.StaticText(self, label="Multiple streams:")
        # merge, subtract, stack

        # 1 Line
        self.derivativeButton = wx.Button(self,-1,"Derivative",size=(160,30))
        self.rotationButton = wx.Button(self,-1,"Rotation",size=(160,30))
        self.fitButton = wx.Button(self,-1,"Fit",size=(160,30))
        # 2 Line
        self.meanButton = wx.Button(self,-1,"Mean",size=(160,30))
        self.maxButton = wx.Button(self,-1,"Maxima",size=(160,30))
        self.minButton = wx.Button(self,-1,"Minima",size=(160,30))
        self.flagmodButton = wx.Button(self,-1,"Flags",size=(160,30))

        # 3 Line
        self.offsetButton = wx.Button(self,-1,"Offsets",size=(160,30))
        self.filterButton = wx.Button(self,-1,"Filter",size=(160,30))
        self.smoothButton = wx.Button(self,-1,"Smooth",size=(160,30))
        self.resampleButton = wx.Button(self,-1,"Resample",size=(160,30))

        # 4 Line
        self.activityButton = wx.Button(self,-1,"Activity",size=(160,30))
        self.deltafButton = wx.Button(self,-1,"Delta F",size=(160,30))
        self.baselineButton = wx.Button(self,-1,"Baseline",size=(160,30))
        self.calcfButton = wx.Button(self,-1,"Calculate F",size=(160,30))

        # 5 Line
        self.powerButton = wx.Button(self,-1,"Power*",size=(160,30))
        self.spectrumButton = wx.Button(self,-1,"Spectrum*",size=(160,30))

        # 5 Line
        #self.mergeButton = wx.Button(self,-1,"Merge",size=(160,30))
        #self.subtractButton = wx.Button(self,-1,"Subtract",size=(160,30))
        #self.stackButton = wx.Button(self,-1,"Stack/Average",size=(160,30))

        # 3. Section
        #self.selectfilterLabel = wx.StaticText(self, label="Select type:")
        #self.selectfilterComboBox = wx.ComboBox(self, choices=self.filterlist,
        #    style=wx.CB_DROPDOWN, value=self.filterlist[14])
        #self.selectlengthLabel = wx.StaticText(self, label="Select length:")
        #self.selectlengthComboBox = wx.ComboBox(self, choices=self.filterlength,
        #    style=wx.CB_DROPDOWN, value=self.filterlength[0])


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=20, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        """
                 # section 3
                 (self.selectfilterLabel, noOptions),
                 (self.selectfilterComboBox, expandOption),
                 (self.selectlengthLabel, noOptions),
                 (self.selectlengthComboBox, expandOption),
                  emptySpace,
                 (self.filterButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 # end
        """
        # Add the controls to the sizers:
        for control, options in \
                [(self.head1Label, noOptions),
                  emptySpace,
                 (self.derivativeButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.fitButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.rotationButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.head2Label, noOptions),
                  emptySpace,
                 (self.maxButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.minButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.meanButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.flagmodButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.head3Label, noOptions),
                  emptySpace,
                 (self.filterButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.smoothButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.offsetButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.resampleButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.head4Label, noOptions),
                  emptySpace,
                 (self.deltafButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.baselineButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.activityButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.calcfButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.head5Label, noOptions),
                  emptySpace,
                 (self.powerButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.spectrumButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)
