#!/usr/bin/env python

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

from stream import *
from absolutes import *
from transfer import *
from database import *

import wx

from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

from wx.lib.pubsub import Publisher

# Subclasses for Menu pages and their controls     

class GeneralPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        #self.datatype = ['raw', 'reviewed']
        #self.varios = ['didd', 'lemi', 'gdas']
        #self.scalars = ['didd', 'pmag', 'csmag','none']
        #self.resolution = ['intrinsic', 'second', 'minute', 'hour']
        #self.comp = ['xyz', 'hdz', 'idf']
        self.createControls()
        self.doLayout()
        
    # Widgets
    def createControls(self):
        self.DrawAuxButton = wx.Button(self,-1,"Draw/Recalc")
        self.SaveAuxButton = wx.Button(self,-1,"Save data")
        self.AppendAuxButton = wx.Button(self,-1,"Append")
        self.OpenAuxButton = wx.Button(self,-1,"Open Aux file")
        self.AuxDataLabel = wx.StaticText(self, label="Auxiliary data")
        self.AuxDataTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.AuxDataTextCtrl.Disable()
        self.AuxResolutionLabel = wx.StaticText(self, label="Time resolution")
        self.AuxResolutionTextCtrl = wx.TextCtrl(self, value="NaN")
        self.AuxResolutionTextCtrl.Disable()
        self.AuxStartDateTextCtrl = wx.TextCtrl(self, value="--")
        self.AuxStartDateTextCtrl.Disable()
        self.AuxEndDateTextCtrl = wx.TextCtrl(self, value="--")
        self.AuxEndDateTextCtrl.Disable()
        self.funcLabel = wx.StaticText(self, label="Apply fuctions:")
        self.removeOutliersCheckBox = wx.CheckBox(self,
            label="Remove Outliers")
        self.recoveryCheckBox = wx.CheckBox(self,
            label="Show data coverage")
        self.interpolateCheckBox = wx.CheckBox(self,
            label="Interpolate data")
        self.fitCheckBox = wx.CheckBox(self,
            label="Fit function")

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=24, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.OpenAuxButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.AuxDataLabel, noOptions),
                  emptySpace,
                 (self.AuxDataTextCtrl, expandOption),
                  emptySpace,
                 (self.AuxResolutionLabel, noOptions),
                 (self.AuxResolutionTextCtrl, expandOption),
                 (self.AuxStartDateTextCtrl, expandOption),
                 (self.AuxEndDateTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.funcLabel, noOptions),
                  emptySpace,
                 (self.removeOutliersCheckBox, noOptions),
                  emptySpace,
                 (self.recoveryCheckBox, noOptions),
                  emptySpace,
                 (self.interpolateCheckBox, noOptions),
                  emptySpace,
                 (self.fitCheckBox, noOptions),
                  emptySpace,
                 (self.AppendAuxButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                 (self.SaveAuxButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.DrawAuxButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

         
class GraphPage(wx.Panel):
    #def __init__(self, parent):
        #wx.Panel.__init__(self,parent,-1,size=(100,100))
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.datatype = ['raw', 'reviewed']
        # get those data from obsini
        # Load obsini: dfs etc
        self.varios = ['didd', 'lemi', 'optical','gdas']
        self.scalars = ['selected vario','didd', 'pmag', 'csmag']
        self.resolution = ['intrinsic','second', 'minute', 'hour']
        self.comp = ['xyz', 'hdz', 'idf']
        self.createControls()
        self.doLayout()
        
    # Widgets
    def createControls(self):
        self.DrawButton = wx.Button(self,-1,"Draw/Recalc")
        self.SaveVarioButton = wx.Button(self,-1,"Save data")
        self.dateLabel = wx.StaticText(self, label="Insert time range:")
        self.startdateLabel = wx.StaticText(self, label="Start date:")
        self.startDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(datetime.strptime("2011-10-22","%Y-%m-%d").timetuple())))
        self.enddateLabel = wx.StaticText(self, label="End date:")
        self.endDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(datetime.strptime("2011-10-22","%Y-%m-%d").timetuple())))
        self.instLabel = wx.StaticText(self, label="Select variometer:")
        self.resolutionLabel = wx.StaticText(self, label="Select resolution:")
        self.scalarLabel = wx.StaticText(self, label="Select F source:")
        self.scalarReviewedLabel = wx.StaticText(self, label="only reviewed!")
        self.varioComboBox = wx.ComboBox(self, choices=self.varios,
            style=wx.CB_DROPDOWN, value=self.varios[0])
        self.overrideAutoBaselineButton = wx.Button(self,-1,"Manual base.")
        self.baselinefileTextCtrl = wx.TextCtrl(self, value="--")
        self.baselinefileTextCtrl.Disable()
        self.scalarComboBox = wx.ComboBox(self, choices=self.scalars,
            style=wx.CB_DROPDOWN, value=self.scalars[0])
        self.resolutionComboBox = wx.ComboBox(self, choices=self.resolution,
            style=wx.CB_DROPDOWN, value=self.resolution[0])
        self.datatypeLabel = wx.StaticText(self, label="Select datatype:")
        self.datatypeComboBox = wx.ComboBox(self, choices=self.datatype,
            style=wx.CB_DROPDOWN, value=self.datatype[1])
        self.drawRadioBox = wx.RadioBox(self,
            label="Select vector components",
            choices=self.comp, majorDimension=3, style=wx.RA_SPECIFY_COLS)
        self.addoptLabel = wx.StaticText(self, label="Optional graphs:")
        self.baselinecorrCheckBox = wx.CheckBox(self,label="Baseline corr.")
        self.fCheckBox = wx.CheckBox(self,label="Plot F")
        self.dfCheckBox = wx.CheckBox(self,label="calculate dF")
        self.dfCheckBox.Disable()
        self.tCheckBox = wx.CheckBox(self,label="Plot T")
        self.showFlaggedCheckBox = wx.CheckBox(self,
            label="show flagged")
        self.curdateTextCtrl = wx.TextCtrl(self, value="--")
        self.curdateTextCtrl.Disable()
        self.prevdateTextCtrl = wx.TextCtrl(self, value="--")
        self.prevdateTextCtrl.Disable()
        self.GetGraphMarksButton = wx.Button(self,-1,"Get marks")
        self.flagSingleButton = wx.Button(self,-1,"Flag date")
        self.flagRangeButton = wx.Button(self,-1,"Flag range")
        self.curselecteddateLabel = wx.StaticText(self, label="Current sel.")
        self.prevselecteddateLabel = wx.StaticText(self, label="Previous sel.")
        self.dfIniTextCtrl = wx.TextCtrl(self, value="dF(ini): 0 nT")
        self.dfCurTextCtrl = wx.TextCtrl(self, value="dF(cur): --")
        self.dfIniTextCtrl.Disable()
        self.dfCurTextCtrl.Disable()

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
        for control, options in \
                [(self.dateLabel, noOptions),
                  emptySpace,
                 (self.startdateLabel, noOptions),
                 (self.enddateLabel, noOptions),
                 (self.startDatePicker, expandOption),
                 (self.endDatePicker, expandOption),
                 (self.resolutionLabel, noOptions),
                 (self.datatypeLabel, noOptions),
                 (self.resolutionComboBox, expandOption),
                 (self.datatypeComboBox, expandOption),
                 (self.instLabel, noOptions),
                  emptySpace,
                 (self.varioComboBox, expandOption),
                  emptySpace,
                 (self.baselinecorrCheckBox, noOptions),
                 (self.overrideAutoBaselineButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.baselinefileTextCtrl, expandOption),
                  emptySpace,
                 (self.scalarLabel, noOptions),
                  emptySpace,
                 (self.scalarComboBox, expandOption),
                 (self.scalarReviewedLabel, noOptions),
                 (self.fCheckBox, noOptions),
                 (self.dfCheckBox, noOptions),
                 (self.dfIniTextCtrl, expandOption),
                 (self.dfCurTextCtrl, expandOption),
                 (self.addoptLabel, noOptions),
                  emptySpace,
                 (self.drawRadioBox, noOptions),
                 (self.tCheckBox, noOptions),
                  emptySpace,
                 (self.showFlaggedCheckBox, noOptions),
                 (self.GetGraphMarksButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,        
                 (self.curselecteddateLabel, noOptions),
                  emptySpace,
                 (self.curdateTextCtrl, expandOption),
                 (self.flagSingleButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.prevselecteddateLabel, noOptions),
                  emptySpace,
                 (self.prevdateTextCtrl, expandOption),
                 (self.flagRangeButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.SaveVarioButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.DrawButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class AnalysisPage(wx.Panel):
    #def __init__(self, parent):
    #    wx.Panel.__init__(self,parent,-1,size=(100,100))
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.datatype = ['reviewed', 'definitv']
        self.varios = ['didd', 'lemi', 'gdas']
        self.vario2 = ['none','didd', 'lemi', 'gdas']
        self.resolution = ['sec', 'min', 'hour']
        self.comp = ['xyz', 'hdz', 'idf']
        self.kmeth = ['fmi', 'oth', 'mine']
        self.createControls()
        self.doLayout()
        
    # Widgets
    def createControls(self):
        self.DrawButton = wx.Button(self,-1,"Calculate/Draw")
        self.ExportButton = wx.Button(self,-1,"Export data")
        self.dateLabel = wx.StaticText(self, label="Insert time range:")
        self.startdateLabel = wx.StaticText(self, label="Start date:")
        self.startDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(datetime.strptime("2010-11-22","%Y-%m-%d").timetuple())))
        self.enddateLabel = wx.StaticText(self, label="End date:")
        self.endDatePicker = wx.DatePickerCtrl(self, dt=wx.DateTimeFromTimeT(time.mktime(datetime.strptime("2010-11-22","%Y-%m-%d").timetuple())))
        self.resolutionComboBox = wx.ComboBox(self, choices=self.resolution,
            style=wx.CB_DROPDOWN, value=self.resolution[0])
        self.datatypeLabel = wx.StaticText(self, label="Select datatype:")
        self.datatypeComboBox = wx.ComboBox(self, choices=self.datatype,
            style=wx.CB_DROPDOWN, value=self.datatype[0])
        self.resolutionLabel = wx.StaticText(self, label="Select resolution:")
        self.primaryLabel = wx.StaticText(self, label="Primary instrument:")
        self.secondaryLabel = wx.StaticText(self, label="Secondary instrument:")
        self.primaryComboBox = wx.ComboBox(self, choices=self.varios,
            style=wx.CB_DROPDOWN, value=self.varios[0])
        self.secondaryComboBox = wx.ComboBox(self, choices=self.vario2,
            style=wx.CB_DROPDOWN, value=self.vario2[0])
        self.addoptLabel = wx.StaticText(self, label="Select graph options:")
        self.analysisLabel = wx.StaticText(self, label="Select graph options:")
        self.drawRadioBox = wx.RadioBox(self,
            label="Select vector components",
            choices=self.comp, majorDimension=3, style=wx.RA_SPECIFY_COLS)
        self.fCheckBox = wx.CheckBox(self, label="Plot F")
        self.tCheckBox = wx.CheckBox(self, label="Plot T")
        self.kvalLabel = wx.StaticText(self, label="Get k:")
        self.kvalRadioBox = wx.RadioBox(self,
            label="Select k method",
            choices=self.kmeth, majorDimension=3, style=wx.RA_SPECIFY_COLS)
        self.kvalButton = wx.Button(self,-1,"Determine k")
        self.compareLabel = wx.StaticText(self, label="Compare instruments")
        self.fillPrimButton = wx.Button(self,-1,"fill by sec")
        self.stormLabel = wx.StaticText(self, label="Get storm onsets")
        self.otherLabel = wx.StaticText(self, label="Spectrogram")
 
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
        for control, options in \
                [(self.dateLabel, noOptions),
                  emptySpace,
                 (self.startdateLabel, noOptions),
                 (self.enddateLabel, noOptions),
                 (self.startDatePicker, expandOption),
                 (self.endDatePicker, expandOption),
                 (self.resolutionLabel, noOptions),
                 (self.datatypeLabel, noOptions),
                 (self.resolutionComboBox, expandOption),
                 (self.datatypeComboBox, expandOption),
                 (self.primaryLabel, noOptions),
                 (self.secondaryLabel, noOptions),
                 (self.primaryComboBox, expandOption),
                 (self.secondaryComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.addoptLabel, noOptions),
                  emptySpace,
                 (self.drawRadioBox, noOptions),
                  emptySpace,
                 (self.fCheckBox, noOptions),
                  emptySpace,
                 (self.tCheckBox, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.analysisLabel, noOptions),
                  emptySpace,
                 (self.compareLabel, noOptions),
                 (self.fillPrimButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.kvalLabel, noOptions),
                  emptySpace,
                 (self.kvalRadioBox, noOptions),
                 (self.kvalButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.stormLabel, noOptions),
                  emptySpace,
                 (self.otherLabel, noOptions),
                  emptySpace,
                 (self.ExportButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.DrawButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

        
class ReportPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        #self.parent = parent
        self.createControls()
        self.doLayout()

    def createControls(self):
        self.logger = wx.TextCtrl(self, style=wx.TE_MULTILINE|wx.TE_READONLY)

    def doLayout(self):
        self.logger.SetDimensions(x=10, y=20, width=200, height=300)

    def logMsg(self, message):
        ''' Private method to append a string to the logger text
            control. '''
        print message
        self.logger.AppendText('%s\n'%message)

class PortCommunicationPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.createControls()
        self.doLayout()
        
    # Widgets
    def createControls(self):
        self.selectPortButton = wx.Button(self,-1,"Select Port")
        self.portnameTextCtrl = wx.TextCtrl(self, value="--")
        self.portnameTextCtrl.Disable()
        self.sliderLabel = wx.StaticText(self, label="Update frequency:")
        self.frequSlider = wx.Slider(self, -1, 10, 1, 20, (-1, -1), (100, -1), 
		wx.SL_AUTOTICKS | wx.SL_HORIZONTAL | wx.SL_LABELS)
        self.startMonitorButton = wx.Button(self,-1,"Start Monitor")
        self.stopMonitorButton = wx.Button(self,-1,"Stop Monitor")
        self.startMonitorButton.Disable()
        self.stopMonitorButton.Disable()

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
                [(self.selectPortButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.portnameTextCtrl, expandOption),
                 (self.sliderLabel, noOptions),
                 (self.frequSlider, noOptions),
                  emptySpace,
                  emptySpace,
                 (self.startMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.stopMonitorButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

 
