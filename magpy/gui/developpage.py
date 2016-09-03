#!/usr/bin/env python

from __future__ import print_function
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


# Subclasses for Menu pages and their controls

class ScreenSelections(object):
    def __init__(self, seldatelist=[], selvallist=[],shflag = False):
        self.seldatelist = seldatelist
        self.selvallist = selvallist
        self.shflag = shflag

    def clearList(self):
        self.seldatelist = []
        self.selvallist = []

    def updateList(self):
        #print self.seldatelist[len(self.seldatelist)-1]
        pass
        #panel = wx.Panel(self,-1)
        #self.sp = MenuPanel(panel)
        #self.menu_p.rep_page.logMsg


class DataContainer(object):
    def __init__(self, magdatastruct1=[], magdatastruct2=[],struct1res=0,struct2res=0):
        self.magdatastruct1 = magdatastruct1
        self.magdatastruct2 = magdatastruct2
        self.struct1res = struct1res
        self.struct2res = struct2res

    def test(self):
        print(len(self.magdatastruct1))

class FlagDateDialog(wx.Dialog):
    #def __init__(self, parent, title, choices, curflag):
        #super(FlagDateDialog, self).__init__(parent=parent,
        #    title=title, size=(250, 400))
    def __init__(self, parent, choices, comment, title ):
        super(FlagDateDialog, self).__init__( parent=parent, choices=[], comment='', title=title )
        self.choices = choices
        self.createControls()
        self.doLayout()
        self.bindControls()

    #def doLayout(self):
    #    self.logger.SetDimensions(x=10, y=20, width=200, height=300)
    # Widgets
    def createControls(self):
        # single anaylsis
        self.flagLabel = wx.StaticText(self, label="Add a flag to a single date")
        self.flagsRadioBox = wx.RadioBox(self,
            label="Select flag",
            choices=self.choices, majorDimension=2, style=wx.RA_SPECIFY_COLS)
        self.commentText =  wx.TextCtrl(self, style=wx.TE_MULTILINE|wx.TE_READONLY)
        self.okButton = wx.Button(self, label='Ok')
        self.closeButton = wx.Button(self, label='Close')

    def doLayout(self):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        gridSizer = wx.FlexGridSizer(rows=3, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.flagLabel, noOptions),
                  emptySpace,
                 (self.flagsRadioBox, noOptions),
                 (self.commentText, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.okButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()

class OptionsObsDialog(wx.Dialog):

    def __init__(self, parent, title):
        super(OptionsObsDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # single anaylsis
        self.obsnameLabel = wx.StaticText(self, label="Observatory name")
        self.obsnameTextCtrl = wx.TextCtrl(self, value="--")
        self.obscodeLabel = wx.StaticText(self, label="Observatory code")
        self.obscodeTextCtrl = wx.TextCtrl(self, value="--")
        self.obscityLabel = wx.StaticText(self, label="City")
        self.obscityTextCtrl = wx.TextCtrl(self, value="--")
        self.obscountryLabel = wx.StaticText(self, label="Country")
        self.obscountryTextCtrl = wx.TextCtrl(self, value="--")
        self.obslongLabel = wx.StaticText(self, label="Longitude")
        self.obslongTextCtrl = wx.TextCtrl(self, value="--")
        self.obslatLabel = wx.StaticText(self, label="Latitude")
        self.obslatTextCtrl = wx.TextCtrl(self, value="--")
        self.obsaltLabel = wx.StaticText(self, label="Altitude")
        self.obsaltTextCtrl = wx.TextCtrl(self, value="--")
        self.obsmirenLabel = wx.StaticText(self, label="Miren")
        self.obsmirenTextCtrl = wx.TextCtrl(self, value="--")

        self.pilnamesLabel = wx.StaticText(self, label="Pillars")
        self.pilnamesTextCtrl = wx.TextCtrl(self, value="--")
        self.pillocLabel = wx.StaticText(self, label="Pillar locations")
        self.pillocTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pilazimuthLabel = wx.StaticText(self, label="Pillar azimuths")
        self.pilazimuthTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pildfLabel = wx.StaticText(self, label="delta F")
        self.pildfTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pilddLabel = wx.StaticText(self, label="delta D")
        self.pilddTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pildiLabel = wx.StaticText(self, label="delta I")
        self.pildiTextCtrl = wx.TextCtrl(self, style=wx.TE_MULTILINE) # get this value from obsini
        self.pildfepochLabel = wx.StaticText(self, label="Epoch dF")
        self.pildfepochTextCtrl = wx.TextCtrl(self, value="--")
        self.pilddirepochLabel = wx.StaticText(self, label="Epoch dV")
        self.pilddirepochTextCtrl = wx.TextCtrl(self, value="--")

        self.okButton = wx.Button(self, label='Ok')
        self.closeButton = wx.Button(self, label='Close')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=5, cols=8, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        for control, options in \
                [(self.obsnameLabel, noOptions),
                 (self.obscodeLabel, noOptions),
                 (self.obscityLabel, noOptions),
                 (self.obscountryLabel, noOptions),
                 (self.obslongLabel, noOptions),
                 (self.obslatLabel, noOptions),
                 (self.obsaltLabel, noOptions),
                 (self.obsmirenLabel, noOptions),
                 (self.obsnameTextCtrl, expandOption),
                 (self.obscodeTextCtrl, expandOption),
                 (self.obscityTextCtrl, expandOption),
                 (self.obscountryTextCtrl, expandOption),
                 (self.obslongTextCtrl, expandOption),
                 (self.obslatTextCtrl, expandOption),
                 (self.obsaltTextCtrl, expandOption),
                 (self.obsmirenTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.pilnamesLabel, noOptions),
                 (self.pillocLabel, noOptions),
                 (self.pilazimuthLabel, noOptions),
                 (self.pildfLabel, noOptions),
                 (self.pilddLabel, noOptions),
                 (self.pildiLabel, noOptions),
                 (self.pildfepochLabel, noOptions),
                 (self.pilddirepochLabel, noOptions),
                 (self.pilnamesTextCtrl, expandOption),
                 (self.pillocTextCtrl, expandOption),
                 (self.pilazimuthTextCtrl, expandOption),
                 (self.pildfTextCtrl, expandOption),
                 (self.pilddTextCtrl, expandOption),
                 (self.pildiTextCtrl, expandOption),
                 (self.pildfepochTextCtrl, expandOption),
                 (self.pilddirepochTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                  emptySpace,
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
        self.okButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)

    def OnClose(self, e):
        self.Destroy()



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


class PortCommunicationPage(wx.Panel):
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.selectPortButton = wx.Button(self,-1,"Select MARTAS")
        self.portnameTextCtrl = wx.TextCtrl(self, value="coming soon")
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
