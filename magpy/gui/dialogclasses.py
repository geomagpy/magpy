#!/usr/bin/env python

from magpy.stream import *
from magpy.core import methods
from magpy import absolutes as di
from magpy.core import database


import wx
# wx 4.x
from wx.adv import DatePickerCtrl as wxDatePickerCtrl
from wx.adv import DP_DEFAULT as wxDP_DEFAULT
from wx import FD_MULTIPLE as wxMULTIPLE
from wx import Bitmap as wxBitmap
from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure
import wx.lib.scrolledpanel as scrolledpanel
from io import open
import dateutil.parser
import platform


"""

| class          |  since version  |  until version  |  runtime test  |  manual  |  used by |
| -------------- |  -------------  |  -------------  |  ------------  | -------- |  ---------- |
| OpenWebAddressDialog |    2.0.0  |                 |  level 2       |          | file_on_open_url |
| ConnectWebServiceDialog | 2.0.0  |                 |  level 2       |          | file_on_open_webservice |
| LoadDataDialog |          2.0.0  |                 |  level 2       |          | _open_stream      |
| ExportDataDialog |        2.0.0  |                 |  level 2       |          | file_export_data  |
| ExportModifyNameDialog |  2.0.0  |                 |  level 2       |          | ExportDataDialog  |
| DatabaseConnectDialog |   2.0.0  |                 |  level 2       |          | db_on_connect |
| OptionsInitDialog |       2.0.0  |                 |  level 2       |          | options_init |
| OptionsDIDialog |         2.0.0  |                 |  level 2       |          | options_di |
| OptionsPlotDialog |       2.0.0  |                 |  level 2       |          | options_plot |
| MultiStreamPanel |        2.0.0  |                 |  level 2       |          | memory_select |
| InputSheetDialog |       2.0.0   |                 |  level 2       |          | di_input_sheet |
| SettingsPanel |          2.0.0   |                 |  level 2       |          | InputSheetDialog |
| AnalysisBaselineDialog | 2.0.0   |                 |  level 2       |          | ana_onBaselineButton |
| AnalysisRotationDialog | 2.0.0   |                 |  level 2       |          | ana_onRotationButton |
| AnalysisFitDialog      | 2.0.0   |                 |  level 2       |          | ana_onFitButton |
| AnalysisFilterDialog   | 2.0.0   |                 |  level 2       |          | ana_onFilterButton |
| AnalysisOffsetDialog   | 2.0.0   |                 |  level 2       |          | ana_onOffsetButton |
| AnalysisResampleDialog | 2.0.0   |                 |  level 2       |          | ana_onOffsetButton |
| LoadDIDialog           | 2.0.0   |                 |  level 2       |          | dip_onLoadDIButton |
| SetAzimuthDialog       | 2.0.0   |                 |  level 2       |          | dip_onDIAnanlysis |
| SetStationIDDialog     | 2.0.0   |                 |  level 2       |          | LoadDIDialog |
| LoadVarioScalarDialog     | 2.0.0   |              |  level 2       |          | dip_onDIVarioButton |
| DIConnectDatabaseDialog   | 2.0.0   |              |  level 2       |          | LoadDIDialog |
| DefineVarioDialog      | 2.0.0   |                 |  level 2       |          | LoadDIDialog |
| DefineScalarDialog     | 2.0.0   |                 |  level 2       |          | LoadDIDialog |
| DISaveDialog           | 2.0.0   |                 |  level 2       |          | dip_onDISaveButton |
| ParameterDictDialog    | 2.0.0   |                 |  level 2       |          | dip_onDIParameterButton |
| FlaggingGroupsDialog |   2.0.0   |                 |  level 2       |          | FlagOutlier, FlagRange, FlagSelection |
| FlagOutlierDialog    |   2.0.0   |                 |  level 2       |          | flag_onFlagOutlier |
| FlagSelectionDialog  |   2.0.0   |                 |  level 2       |          | flag_onFlagSelection |
| FlagRangeDialog      |   2.0.0   |                 |  level 2       |          | flag_onFlagRange |
| FlagLoadDialog       |   2.0.0   |                 |  level 2       |          | flag_onFlagLoad |
| FlagSaveDialog       |   2.0.0   |                 |  level 2       |          | flag_onFlagSave |
| CheckDefinitiveDataDialog | 2.0.0   |              |  level 2       |          |            |
| CheckDataReportDialog |  2.0.0   |                 |  level 2       |          |            |
| CheckDataSelectDialog |  2.0.0   |                 |  level 2       |          |            |


runtime test:
- : not tested
level 0 : opens in linux
level 1 : all buttons working and return ok
level 2 : all options tested
level 3 : level 2 also on Mac and Windows (level2w or level2m as temporary)

* all tests are performed with the suggested configuration of the install recommendation
"""

# ##################################################################################################################
# ##################################################################################################################
# #############################    Subclasses for Dialogs called by magpy gui   ####################################
# ##################################################################################################################
# ##################################################################################################################


# ##################################################################################################################
# ####    Menu Bar                                         #########################################################
# ##################################################################################################################

class OpenWebAddressDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for File Menu - Load URL
    """

    def __init__(self, parent, title, favorites):
        super(OpenWebAddressDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.favorites = favorites
        if self.favorites == None or len(self.favorites) == 0:
            self.favorites = ['http://www.example.com']
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # single analysis
        self.urlLabel = wx.StaticText(self, label="Open address:",size=(500,-1))
        self.urlHelp = wx.StaticText(self, label="Valid inputs: 'ftp://.../'for all files, or 'ftp://.../data.dat' for a single file",size=(500,-1))
        #self.urlTextCtrl = wx.TextCtrl(self, value=self.favorites[0],size=(500,30))
        self.favoritesLabel = wx.StaticText(self, label="Modify URL memory:",size=(160,-1))
        self.getFavsComboBox = wx.ComboBox(self, choices=self.favorites,
            style=wx.CB_DROPDOWN, value=self.favorites[0],size=(500,-1))
        self.addFavsButton = wx.Button(self, label='Add current input',size=(200,-1))
        self.dropFavsButton = wx.Button(self, label='Remove selected item',size=(200,-1))

        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,-1))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,-1))

    def doLayout(self):
        #mainSizer = wx.BoxSizer(wx.VERTICAL)
        #boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        #gridSizer = wx.FlexGridSizer(rows=3, cols=2, vgap=10, hgap=10)
        # mainSizer contains main selection menua

        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=6, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        for control, options in \
                [(self.urlLabel, noOptions),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.getFavsComboBox, expandOption),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.favoritesLabel, noOptions),
                  emptySpace,
                 (self.addFavsButton, dict(flag=wx.ALIGN_LEFT)),
                 emptySpace,
                 (self.dropFavsButton, dict(flag=wx.ALIGN_LEFT)),
                  emptySpace,
                  (self.urlHelp, noOptions),
                  emptySpace]:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.addFavsButton.Bind(wx.EVT_BUTTON, self.AddFavs)
        self.dropFavsButton.Bind(wx.EVT_BUTTON, self.DropFavs)
        self.getFavsComboBox.Bind(wx.EVT_COMBOBOX, self.GetFavs)

    def GetFavs(self,e):
        """
        http://www.intermagnet.org/test/ws/?id=BOU
        """
        url = self.getFavsComboBox.GetValue()
        #self.urlTextCtrl.SetValue(url)


    def AddFavs(self, e):
        # url = self.urlTextCtrl.GetValue()
        url = self.getFavsComboBox.GetValue()
        if not url in self.favorites:
            self.favorites.append(url)
            self.getFavsComboBox.Append(str(url))

    def DropFavs(self, e):
        url = self.getFavsComboBox.GetValue()
        self.favorites = [elem for elem in self.favorites if not elem == url]
        self.getFavsComboBox.Clear()
        for elem in self.favorites:
            self.getFavsComboBox.Append(elem)
        if self.favorites and len(self.favorites) > 0:
            self.getFavsComboBox.SetValue(self.favorites[0])


class ConnectWebServiceDialog(wx.Dialog):
    """
    Helper method to connect to edge
    Select shown keys
    """
    def __init__(self, parent, title, services, default, validgroups, startdate=wx.DateTime().Today(), enddate=wx.DateTime().Today() ):
        super(ConnectWebServiceDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.services = services
        self.default = default
        self.validgroups = validgroups
        defaultgroup = 'magnetism'
        self.startdate = startdate
        self.enddate = enddate
        self.servicelist = [el for el in services]
        if default in self.servicelist and len(self.servicelist) > 0:
            self.selectedservice = default
        elif len(self.servicelist) > 0:
            self.selectedservice = self.servicelist[0]
        else:
            # Message window that no service has been found
            # Aborting
            print ("Abort")

        self.grouplist = [el for el in self.services.get(self.selectedservice) if el in validgroups and not el == 'commands']

        if defaultgroup in self.grouplist and len(self.grouplist) > 0:
            self.selectedgroup = defaultgroup
        elif len(self.grouplist) > 0:
            self.selectedgroup = self.grouplist[0]

        self.ids = services.get(self.selectedservice).get(self.selectedgroup).get('ids',[])
        self.types = services.get(self.selectedservice).get(self.selectedgroup).get('type',[])
        self.formats = services.get(self.selectedservice).get(self.selectedgroup).get('format')
        self.sampling = services.get(self.selectedservice).get(self.selectedgroup).get('sampling',[])
        self.elements = services.get(self.selectedservice).get(self.selectedgroup).get('elements','')

        self.createControls()

        self.doLayout()
        self.bindControls()

    def createControls(self):
        self.serviceLabel = wx.StaticText(self, label="Webservice source:",size=(400,25))
        self.serviceComboBox = wx.ComboBox(self, choices=self.servicelist,
            style=wx.CB_DROPDOWN, value=self.selectedservice,size=(400,-1))
        self.groupLabel = wx.StaticText(self, label="Source:",size=(400,25))
        self.groupComboBox = wx.ComboBox(self, choices=self.grouplist,
            style=wx.CB_DROPDOWN, value=self.selectedgroup,size=(400,-1))
        self.obsIDLabel = wx.StaticText(self, label="Observatory ID:",size=(400,25))
        self.idComboBox = wx.ComboBox(self, choices=self.ids,
            style=wx.CB_DROPDOWN, value=self.ids[0],size=(400,-1))
        self.formatLabel = wx.StaticText(self, label="Format: ",size=(400,25))
        self.formatComboBox = wx.ComboBox(self, choices=self.formats,
            style=wx.CB_DROPDOWN, value=self.formats[0],size=(400,-1))
        self.typeLabel = wx.StaticText(self, label="Type: ",size=(400,25))
        self.typeComboBox = wx.ComboBox(self, choices=self.types,
            style=wx.CB_DROPDOWN, value=self.types[0],size=(400,-1))
        self.sampleLabel = wx.StaticText(self, label="Sampling Period (seconds):",size=(400,25))
        self.sampleComboBox = wx.ComboBox(self, choices=self.sampling,
            style=wx.CB_DROPDOWN, value=self.sampling[0],size=(400,-1))
        self.startTimeLabel = wx.StaticText(self, label="Start Time: ",size=(400,25))
        self.startDatePicker = wxDatePickerCtrl(self,dt=self.startdate,size=(160,25))
        self.startTimePicker = wx.TextCtrl(self, value='00:00:00',size=(160,25))
        self.endTimeLabel = wx.StaticText(self, label="End Time: ",size=(400,25))
        self.endDatePicker = wxDatePickerCtrl(self,dt=self.enddate, size=(160,25))
        self.endTimePicker = wx.TextCtrl(self, value='23:59:59',size=(160,25))
        self.elementsLabel = wx.StaticText(self, label="Comma separated list of requested elements: ",size=(400,25))
        self.elementsTextCtrl = wx.TextCtrl(self, value='X,Y,Z,F',size=(400,25))
        self.okButton = wx.Button(self, wx.ID_OK, label='Connect')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(400,25))


    def bindControls(self):
        self.serviceComboBox.Bind(wx.EVT_COMBOBOX, self.updateService)
        self.groupComboBox.Bind(wx.EVT_COMBOBOX, self.updateGroup)


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.serviceLabel, noOptions),
                 (self.groupLabel, dict()),
                 (self.serviceComboBox, expandOption),
                 (self.groupComboBox, dict(flag=wx.EXPAND)),
                 (self.obsIDLabel, noOptions),
                 (self.formatLabel, noOptions),
                 (self.idComboBox, expandOption),
                 (self.formatComboBox, expandOption),
                 (self.typeLabel, noOptions),
                 (self.sampleLabel, noOptions),
                 (self.typeComboBox, expandOption),
                 (self.sampleComboBox, expandOption),
                 (self.elementsLabel, noOptions),
                 emptySpace,
                 (self.elementsTextCtrl, expandOption),
                 emptySpace,
                 (self.startTimeLabel, noOptions),
                 emptySpace,
                 (self.startDatePicker, expandOption),
                 (self.startTimePicker, expandOption),
                 (self.endTimeLabel, noOptions),
                 emptySpace,
                 (self.endDatePicker, expandOption),
                 (self.endTimePicker, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=5, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


    def createUrl(self):
        # Not used any more - have chosen alternative to provide appropriate dictionary in init
        # Known webservices can be added to initial dictionary
        # Experimental or test sites can be opened by the "general url" method
        pass

    def getService(self):
        selectn = self.serviceComboBox.GetStringSelection()
        if selectn:
            self.selectedservice = selectn
        self.grouplist = [el for el in self.services.get(self.selectedservice) if el in self.validgroups]

        self.groupComboBox.Clear()
        self.groupComboBox.AppendItems(self.grouplist)
        if not self.selectedgroup in self.grouplist:
            self.selectedgroup = self.grouplist[0]
        self.groupComboBox.SetValue(self.selectedgroup)


    def getGroup(self):
        selectg = self.groupComboBox.GetStringSelection()
        if selectg:
            self.selectedgroup = selectg
        parameter = self.services.get(self.selectedservice).get(self.selectedgroup)

        print (parameter)
        self.ids = parameter.get('ids')
        self.types = parameter.get('type')
        self.formats = parameter.get('format')
        self.sampling = parameter.get('sampling')
        self.elements = parameter.get('elements')

        self.idComboBox.Clear()
        self.idComboBox.AppendItems(self.ids)
        self.idComboBox.SetValue(self.ids[0])
        self.typeComboBox.Clear()
        self.typeComboBox.AppendItems(self.types)
        self.typeComboBox.SetValue(self.types[0])
        self.formatComboBox.Clear()
        self.formatComboBox.AppendItems(self.formats)
        self.formatComboBox.SetValue(self.formats[0])
        self.sampleComboBox.Clear()
        self.sampleComboBox.AppendItems(self.sampling)
        self.sampleComboBox.SetValue(self.sampling[0])
        self.elementsTextCtrl.Clear()
        self.elementsTextCtrl.SetValue(self.elements)

    def updateService(self, event):
        self.getService()
        self.getGroup()

    def updateGroup(self, event):
        self.getGroup()


class LoadSelectDialog(wx.Dialog):
    """
    DESCRIPTION
        Used in case of ImagCDF files and various conteined data resolutions.
        Select the data set of your choice in a Combo Box
    """

    def __init__(self, parent, title, filecontents):
        super(LoadSelectDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.select = ['default: all data with largest amount']
        for f in filecontents:
            self.select.append("{}: Amount={}".format(f[1],f[0]))
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.selectComboBox = wx.ComboBox(self, choices=self.select,
            style=wx.CB_DROPDOWN, value=self.select[0],size=(300,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Load',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        elemlist = [(self.selectComboBox, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]
        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)
        self.SetSizerAndFit(boxSizer)



class LoadDataDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, mintime, maxtime, extension):
        super(LoadDataDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.mintime = mintime
        self.maxtime = maxtime
        self.extension = extension
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.startdateLabel = wx.StaticText(self, label="Start date:")
        self.startDatePicker = wxDatePickerCtrl(self, dt=self.mintime)
        # the following line produces error in my win xp installation
        self.startTimePicker = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        self.enddateLabel = wx.StaticText(self, label="End date:")
        self.endDatePicker = wxDatePickerCtrl(self, dt=self.maxtime)
        self.endTimePicker = wx.TextCtrl(self, value="00:00:00",size=(160,30))
        if self.extension == 'MySQL Database':
            self.extLabel = wx.StaticText(self, label="DB:")
        else:
            self.extLabel = wx.StaticText(self, label="Files (*.min,*,WIC*):")
        self.fileExt = wx.TextCtrl(self, value=self.extension,size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Load',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        elemlist = ['self.startdateLabel, noOptions',
                  '(0,0), noOptions',
                 'self.startDatePicker, expandOption',
                 'self.startTimePicker, expandOption',
                 'self.enddateLabel, noOptions',
                 '(0,0), noOptions',
                 'self.endDatePicker, expandOption',
                 'self.endTimePicker, expandOption',
                 'self.extLabel, noOptions',
                 'self.fileExt, expandOption',
                 '(0,0), noOptions',
                 '(0,0), noOptions',
                 'self.okButton, dict(flag=wx.ALIGN_CENTER)',
                 'self.closeButton, dict(flag=wx.ALIGN_CENTER)']

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for elem in elemlist:
            control = elem.split(', ')[0]
            options = elem.split(', ')[1]
            gridSizer.Add(eval(control), **eval(options))

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class ExportDataDialog(wx.Dialog):
    """
    Dialog for Exporting data
    """
    def __init__(self, parent, title, path, datadict, exportoptions, allstreamids):
        super(ExportDataDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))

        self.WriteFormats = [ key for key in SUPPORTED_FORMATS if 'w' in SUPPORTED_FORMATS[key][0]]
        stream = datadict.get('dataset')
        samplingrate = datadict.get('samplingrate')
        coverage = datadict.get('coverage')
        defaultformat = exportoptions.get('format_type')
        self.streamids = allstreamids

        #print (self.WriteFormats)
        #print (datadict)
        ALL = ['IAGA', 'WDC', 'IMF', 'IAF', 'BLV', 'BLV1_2', 'IYFV', 'DKA', 'DIDD', 'COVJSON', 'PYSTR', 'PYASCII', 'CSV',
         'IMAGCDF', 'PYCDF', 'LATEX']
        LOWRESOLUTIONFORMATS = ['WDC', 'IMF', 'IAF', 'IYFV', 'DKA', 'DIDD']
        EXPERIMENTALFORMATS = ['LATEX']
        BLVFORMATS = ['BLV','PYCDF','PYSTR']
        self.WriteFormats = [el for el in self.WriteFormats if not el in EXPERIMENTALFORMATS]
        if stream.header.get('DataType','').startswith('MagPyDI') or stream.header.get('DataFormat','') == 'MagPyDI':
            self.WriteFormats = [el for el in self.WriteFormats if el in BLVFORMATS]
        else:
            self.WriteFormats = [el for el in self.WriteFormats if not el.startswith('BLV')]
            if samplingrate < 59:
                self.WriteFormats = [el for el in self.WriteFormats if not el in LOWRESOLUTIONFORMATS]
            if samplingrate > 3601:
                self.WriteFormats = [el for el in self.WriteFormats if not el in ['WDC','IMF','IAF','DIDD']]
            if coverage <= 28:
                self.WriteFormats = [el for el in self.WriteFormats if not el in ['IAF']]

        if not defaultformat or not defaultformat in self.WriteFormats:
            defaultformat = 'PYCDF'
        self.default = self.WriteFormats.index(defaultformat)
        # use stream info and defaults file export
        self.mode = 'overwrite'
        self.exportoptions = exportoptions
        self.stream = stream
        self.filename = self.GetFilename(stream, exportoptions=exportoptions)
        self.path = path
        self.createControls()
        self.doLayout()
        self.bindControls()


    # Widgets
    def createControls(self):
        # single anaylsis
        # db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.selectDirButton = wx.Button(self, label='Change directory', size=(190,30))
        self.selectedTextCtrl = wx.TextCtrl(self, value=self.path, size=(300,30))
        self.formatLabel = wx.StaticText(self, label="as ...")
        self.formatComboBox = wx.ComboBox(self, choices=self.WriteFormats,
            style=wx.CB_DROPDOWN, value=self.WriteFormats[self.default],size=(190,-1))
        self.selectLabel = wx.StaticText(self, label="Export data to ...")
        self.nameLabel = wx.StaticText(self, label="File name(s) looks like ...")
        self.modifyLabel = wx.StaticText(self, label="Change name/coverage/...")
        self.filenameTextCtrl = wx.TextCtrl(self, value=self.filename, size=(300,30))
        self.modifyButton = wx.Button(self, label='Export options', size=(190,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Write', size=(190,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel', size=(190,30))

        self.filenameTextCtrl.Disable()
        self.selectedTextCtrl.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.selectLabel, noOptions),
                  emptySpace,
                 (self.selectedTextCtrl, expandOption),
                 (self.selectDirButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.formatLabel, noOptions),
                 (self.formatComboBox, expandOption),
                 (self.nameLabel, noOptions),
                 (self.modifyLabel, noOptions),
                 (self.filenameTextCtrl, expandOption),
                 (self.modifyButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


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

    def bindControls(self):
        self.selectDirButton.Bind(wx.EVT_BUTTON, self.OnSelectDirButton)
        self.modifyButton.Bind(wx.EVT_BUTTON, self.OnModifyButton)
        self.formatComboBox.Bind(wx.EVT_COMBOBOX, self.OnFormatChange)


    def GetFilename(self, stream, exportoptions=None):
        """
        DESCRIPTION:
            Helper method to determine filename from selections
        """
        filenamebegins = exportoptions.get('filenamebegins')
        filenameends = exportoptions.get('filenameends')
        coverage = exportoptions.get('coverage')
        dateformat = exportoptions.get('dateformat')
        format_type  = exportoptions.get('format_type')
        year  = exportoptions.get('year')

        #print ("Calling GetFilename: if file is MagPyDI - eventually open a message box to define year", filenamebegins, filenameends, coverage, dateformat)
        format_type, self.exportoptions['filenamebegins'], self.exportoptions['filenameends'], self.exportoptions['coverage'], self.exportoptions['dateformat'] = stream._write_format(format_type, filenamebegins, filenameends, coverage, dateformat , year)
        if self.exportoptions.get('coverage') == 'all':
            datelook = ''
        else:
            datelooktmp = stream.start()
            datelook = datelooktmp.strftime(self.exportoptions.get('dateformat'))
        if format_type.endswith('PYCDF'):
            self.exportoptions['filenameends'] = '.cdf'
        filename = self.exportoptions.get('filenamebegins')+datelook+self.exportoptions.get('filenameends')
        return filename


    def OnSelectDirButton(self, event):
        dialog = wx.DirDialog(None, "Choose a directory:",self.path,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            #self.ReactivateStreamPage()
            self.path = dialog.GetPath()
            self.selectedTextCtrl.SetValue(self.path)
        #self.menu_p.rep_page.logMsg('- Directory for file export defined')
        #dialog.Destroy()

    def OnModifyButton(self, event):
        # open a dialog to select filename specifications
        helpdlg = ExportModifyNameDialog(None, title='Specify options',exportoptions=self.exportoptions, streamids=self.streamids)
        if helpdlg.ShowModal() == wx.ID_OK:
            self.exportoptions['filenamebegins'] = helpdlg.beginTextCtrl.GetValue()
            self.exportoptions['filenameends'] = helpdlg.endTextCtrl.GetValue()
            self.exportoptions['dateformat'] = helpdlg.dateTextCtrl.GetValue()
            self.exportoptions['coverage'] = helpdlg.coverageComboBox.GetValue()
            self.exportoptions['mode'] = helpdlg.modeComboBox.GetValue()
            self.exportoptions['subdirectory'] = helpdlg.subdirComboBox.GetValue()
            format_type = self.exportoptions.get('format_type')
            if format_type == 'IMF':
                self.exportoptions['version'] = helpdlg.versionTextCtrl.GetValue()
                self.exportoptions['gin'] = helpdlg.ginTextCtrl.GetValue()
                self.exportoptions['datatype'] = helpdlg.datatypeComboBox.GetValue()
            if format_type == 'IAF':
                self.exportoptions['kvals'] = helpdlg.kvalsComboBox.GetValue()
            if format_type == 'IYFV':
                self.exportoptions['comment'] = helpdlg.commentTextCtrl.GetValue()
                self.exportoptions['kind'] = helpdlg.kindComboBox.GetValue()
            if format_type == 'IMAGCDF':
                self.exportoptions['addflags'] = helpdlg.addflagsComboBox.GetValue()
                self.exportoptions['fillvalue'] = helpdlg.fillvalueTextCtrl.GetValue()
                self.exportoptions['scalar'] = helpdlg.scalarComboBox.GetValue()
                self.exportoptions['environment'] = helpdlg.environmentComboBox.GetValue()
            if format_type == 'BLV':
                self.exportoptions['absinfo'] = helpdlg.absinfoTextCtrl.GetValue()
                self.exportoptions['year'] = helpdlg.yearTextCtrl.GetValue()
                self.exportoptions['meanh'] = helpdlg.meanhTextCtrl.GetValue()
                self.exportoptions['meanf'] = helpdlg.meanfTextCtrl.GetValue()
                self.exportoptions['deltaf'] = helpdlg.deltafTextCtrl.GetValue()
                self.exportoptions['diff'] = helpdlg.diffComboBox.GetValue()
        self.filename = self.GetFilename(self.stream, exportoptions=self.exportoptions)
        self.filenameTextCtrl.SetValue(self.filename)


    def OnFormatChange(self, event):
        # call stream._write_format to determine self.filename
        selformat = self.formatComboBox.GetValue()
        self.exportoptions['format_type'] = selformat
        self.exportoptions['filenamebegins'] = None
        self.exportoptions['filenameends'] = None
        self.exportoptions['coverage'] = None
        self.exportoptions['dateformat'] = None
        self.filename = self.GetFilename(self.stream, exportoptions=self.exportoptions)
        self.filenameTextCtrl.SetValue(self.filename)


class ExportModifyNameDialog(wx.Dialog):
    """
    Helper Dialog for Exporting data
    """
    def __init__(self, parent, title, exportoptions, streamids):
        super(ExportModifyNameDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.exportoptions = exportoptions
        self.streamids = streamids
        self.elemlist = []
        self.createControls()
        self.doLayout()


    # Widgets
    def createControls(self):
        """
        DESCRIPTION
            Create controls in dependency of the selected format type
        :return:
        """
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)
        elemlist = []

        # General fiels:
        self.beginLabel = wx.StaticText(self, label="Name(s) start with.", size=(160,-1))
        elemlist.append((self.beginLabel, noOptions))
        self.beginTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('filenamebegins'), size=(160,-1))
        elemlist.append((self.beginTextCtrl, expandOption))
        self.endLabel = wx.StaticText(self, label="Name(s) end with:", size=(160,-1))
        elemlist.append((self.endLabel, noOptions))
        self.endTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('filenameends'), size=(160,-1))
        elemlist.append((self.endTextCtrl, expandOption))
        self.dateformatLabel = wx.StaticText(self, label="Date format:")
        elemlist.append((self.dateformatLabel, noOptions))
        self.dateTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('dateformat'), size=(160,-1))
        elemlist.append((self.dateTextCtrl, expandOption))
        self.coverageLabel = wx.StaticText(self, label="File coverage:")
        elemlist.append((self.coverageLabel, noOptions))
        self.coverageComboBox = wx.ComboBox(self, choices=['hour','day','month','year','all'],
            style=wx.CB_DROPDOWN, value=self.exportoptions.get('coverage'),size=(160,-1))
        elemlist.append((self.coverageComboBox, expandOption))
        self.modeLabel = wx.StaticText(self, label="Write mode:")
        elemlist.append((self.modeLabel, noOptions))
        self.modeComboBox = wx.ComboBox(self, choices=['replace','append', 'overwrite', 'skip'],
            style=wx.CB_DROPDOWN, value=self.exportoptions.get('mode'),size=(160,-1))
        elemlist.append((self.modeComboBox, expandOption))
        self.subdirLabel = wx.StaticText(self, label="Create subdirectories:")
        elemlist.append((self.subdirLabel, noOptions))
        self.subdirComboBox = wx.ComboBox(self, choices=['','Y', 'Ym', 'Yj'],
            style=wx.CB_DROPDOWN, value=self.exportoptions['subdirectory'],size=(160,-1))
        elemlist.append((self.subdirComboBox, expandOption))

        format_type=self.exportoptions.get('format_type')
        if format_type == 'IMF':
            """
            - version       (str) file version
            - gin           (gin) information node code
            - datatype      (str) R: reported, A: adjusted, Q: quasi-definit, D: definite
            """
            self.versionLabel = wx.StaticText(self, label="IMF version:")
            self.versionTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('version'), size=(160, -1))
            elemlist.append((self.versionLabel, noOptions))
            elemlist.append((self.versionTextCtrl, expandOption))
            self.ginLabel = wx.StaticText(self, label="Geomagnetic information node (GIN):")
            elemlist.append((self.ginLabel, noOptions))
            self.ginTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('gin'), size=(160, -1))
            elemlist.append((self.ginTextCtrl, expandOption))
            self.datatypeLabel = wx.StaticText(self, label="Data type (Reported, Adjusted, Quasi, Definitive:")
            elemlist.append((self.datatypeLabel, noOptions))
            self.datatypeComboBox = wx.ComboBox(self, choices=['', 'R', 'A', 'G', 'D'],
                                                style=wx.CB_DROPDOWN, value=self.exportoptions.get('datatype'),
                                                size=(160, -1))
            elemlist.append((self.datatypeComboBox, expandOption))
            self.beginTextCtrl.Disable()
            self.endTextCtrl.Disable()

        if format_type == 'IAF':
            """
            - kvals         (Datastream) contains K value for iaf storage
            """
            self.kvalsLabel = wx.StaticText(self, label="Select data set containing K values:")
            elemlist.append((self.kvalsLabel, noOptions))
            self.kvalsComboBox = wx.ComboBox(self, choices=self.streamids,
                                                style=wx.CB_DROPDOWN, value=self.exportoptions.get('kvals'),
                                                size=(160, -1))
            elemlist.append((self.kvalsComboBox, expandOption))
            self.beginTextCtrl.Disable()
            self.endTextCtrl.Disable()
            self.coverageComboBox.Disable()

        if format_type == 'IYFV':
            """
            - comment       (string) some comment, currently used in IYFV
            - kind          (string) one of 'A' (all), 'Q' quiet days, 'D' disturbed days,
                                 currently used in IYFV
            """
            self.commentLabel = wx.StaticText(self, label="IYFV comment:")
            elemlist.append((self.commentLabel, noOptions))
            self.commentTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('comment'), size=(160, -1))
            elemlist.append((self.commentTextCtrl, expandOption))
            self.kindLabel = wx.StaticText(self, label="Data type (All, Quiet, Disturded")
            elemlist.append((self.kindLabel, noOptions))
            self.kindComboBox = wx.ComboBox(self, choices=['', 'A', 'Q', 'D'],
                                                style=wx.CB_DROPDOWN, value=self.exportoptions.get('kind'),
                                                size=(160, -1))
            elemlist.append((self.kindComboBox, expandOption))

        if format_type == 'IMAGCDF':
            """
            *Specific parameters:
            - addflags      (BOOL) add flags to IMAGCDF output if True
            - fillvalue     (float) define a fill value for non-existing data (default is np.nan)
            - scalar        (DataStream) provide scalar data when sampling rate is different to vector data
            - environment   (DataStream) provide environment data when sampling rate is different to vector data
            """
            self.addflagsLabel = wx.StaticText(self, label="Add flags:")
            elemlist.append((self.addflagsLabel, noOptions))
            self.addflagsComboBox = wx.ComboBox(self, choices=['False','True'],
                                                style=wx.CB_DROPDOWN, value=self.exportoptions.get('addflags'),
                                                size=(160, -1))
            elemlist.append((self.addflagsComboBox, expandOption))
            self.fillvalueLabel = wx.StaticText(self, label="Define missing data value (NaN if empty):")
            elemlist.append((self.fillvalueLabel, noOptions))
            self.fillvalueTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('fillvalue'), size=(160, -1))
            elemlist.append((self.fillvalueTextCtrl, expandOption))
            self.scalarLabel = wx.StaticText(self, label="Select separate scalar data set:")
            elemlist.append((self.scalarLabel, noOptions))
            self.scalarComboBox = wx.ComboBox(self, choices=self.streamids,
                                                style=wx.CB_DROPDOWN, value=self.exportoptions.get('scalar'),
                                                size=(160, -1))
            elemlist.append((self.scalarComboBox, expandOption))
            self.environmentLabel = wx.StaticText(self, label="Select separate temperature data set:")
            elemlist.append((self.environmentLabel, noOptions))
            self.environmentComboBox = wx.ComboBox(self, choices=self.streamids,
                                                style=wx.CB_DROPDOWN, value=self.exportoptions.get('environment'),
                                                size=(160, -1))
            elemlist.append((self.environmentComboBox, expandOption))
            self.beginTextCtrl.Disable()
            self.endTextCtrl.Disable()

        if format_type == 'BLV':
            """
            *Specific parameters:
            - absinfo       (str) parameter of DataAbsInfo
            - fitfunc       
            - fitdegree
            - knotstep
            - extradays
            - year          (int) year
            - meanh         (float) annual mean of H component
            - meanf         (float) annual mean of F component
            - deltaF        (float) given deltaF value between pier and f position
            - diff          (DataStream) diff (deltaF) between vario and scalar
            """
            self.absinfoLabel = wx.StaticText(self, label="DataAbsoluteInfo:", size=(160, -1))
            elemlist.append((self.absinfoLabel, noOptions))
            self.absinfoTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('absinfo'), size=(160, -1))
            elemlist.append((self.absinfoTextCtrl, expandOption))
            self.yearLabel = wx.StaticText(self, label="Year (BLV export):", size=(160, -1))
            elemlist.append((self.yearLabel, noOptions))
            self.yearTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('year'), size=(160, -1))
            elemlist.append((self.yearTextCtrl, expandOption))
            self.meanhLabel = wx.StaticText(self, label="Define starting value for H:")
            elemlist.append((self.meanhLabel, noOptions))
            self.meanhTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('meanh',""), size=(160, -1))
            elemlist.append((self.meanhTextCtrl, expandOption))
            self.meanfLabel = wx.StaticText(self, label="Define starting value for F:")
            elemlist.append((self.meanfLabel, noOptions))
            self.meanfTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('meanf',""), size=(160, -1))
            elemlist.append((self.meanfTextCtrl, expandOption))
            self.deltafLabel = wx.StaticText(self, label="Define pier difference deltaF:")
            elemlist.append((self.deltafLabel, noOptions))
            self.deltafTextCtrl = wx.TextCtrl(self, value=self.exportoptions.get('deltaf',""), size=(160, -1))
            elemlist.append((self.deltafTextCtrl, expandOption))
            self.diffLabel = wx.StaticText(self, label="Select data set with daily mean F differences:")
            elemlist.append((self.diffLabel, noOptions))
            self.diffComboBox = wx.ComboBox(self, choices=self.streamids,
                                               style=wx.CB_DROPDOWN, value=self.exportoptions.get('diff'),
                                               size=(160, -1))
            elemlist.append((self.diffComboBox, expandOption))

        self.okButton = wx.Button(self, wx.ID_OK, label='Apply', size=(160, -1))
        elemlist.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel', size=(160, -1))
        elemlist.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        self.elemlist = elemlist

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)


        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(self.elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in self.elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class DatabaseConnectDialog(wx.Dialog):
    """
    Dialog for Database Menu - Connect MySQL
    """

    def __init__(self, parent, title):
        super(DatabaseConnectDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # single anaylsis
        # db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysqldb")
        self.hostLabel = wx.StaticText(self, label="Host")
        self.hostTextCtrl = wx.TextCtrl(self, value="localhost")
        self.userLabel = wx.StaticText(self, label="User")
        self.userTextCtrl = wx.TextCtrl(self, value="Max")
        self.passwdLabel = wx.StaticText(self, label="Password")
        self.passwdTextCtrl = wx.TextCtrl(self, value="Secret",style=wx.TE_PASSWORD)
        self.dbLabel = wx.StaticText(self, label="Database")
        self.dbTextCtrl = wx.TextCtrl(self, value="MyDB")
        self.okButton = wx.Button(self, wx.ID_OK, label='Connect')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        #gridSizer = wx.FlexGridSizer(rows=3, cols=4, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.hostLabel, noOptions),
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
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class OptionsInitDialog(wx.Dialog):
    """
    Dialog for Database Menu - Connect MySQL
    """

    def __init__(self, parent, title, guidict, analysisdict):
        super(OptionsInitDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.guidict = guidict
        self.analysisdict = analysisdict
        self.flaglabels = json.dumps(analysisdict.get('flaglabel'), indent=4)
        self.funclist = ['spline', 'polynomial', 'harmonic', 'least-squares', 'mean']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.basicLabel = wx.StaticText(self, label="Basics:",size=(160,30))
        self.calcLabel = wx.StaticText(self, label="Calculation:",size=(160,30))
        self.otherLabel = wx.StaticText(self, label="Other:",size=(160,30))
        self.dirnameLabel = wx.StaticText(self, label="Default load directory:",size=(160,30))
        self.dirnameTextCtrl = wx.TextCtrl(self, value=self.guidict.get('dirname',''),size=(160,30))
        self.exportLabel = wx.StaticText(self, label="Default export dir.:",size=(160,30))
        self.exportTextCtrl = wx.TextCtrl(self, value=self.guidict.get('exportpath',''),size=(160,30))
        self.stationidLabel = wx.StaticText(self, label="Default station code:",size=(160,30))
        self.stationidTextCtrl = wx.TextCtrl(self, value=self.analysisdict.get('defaultstation','WIC'),size=(160,30))
        self.experimentalLabel = wx.StaticText(self, label="Experimental methods:",size=(160,30))
        self.experimentalCheckBox = wx.CheckBox(self, label="activate", size=(160,30))
        self.baselinedirectLabel = wx.StaticText(self, label="Apply baseline:",size=(160,30))
        self.baselinedirectCheckBox = wx.CheckBox(self, label="directly", size=(160,30))
        self.FadoptionLabel = wx.StaticText(self, label='F-baseline:',size=(160,30))
        self.FadoptionCheckBox = wx.CheckBox(self, label='adopt when available',size=(160,30))
        self.baselinedirectCheckBox.SetValue(self.analysisdict.get('baselinedirect',False))
        self.FadoptionCheckBox.SetValue(self.analysisdict.get('fadoption',False))
        self.fitfunctionLabel = wx.StaticText(self, label="Default fit function:",size=(160,30))
        self.fitfunctionComboBox = wx.ComboBox(self, choices=self.funclist,
                              style=wx.CB_DROPDOWN, value=self.analysisdict.get('fitfunction','spline'),size=(160,-1))
        self.fitknotstepLabel = wx.StaticText(self, label="Knotstep (spline):",size=(160,30))
        self.fitknotstepTextCtrl = wx.TextCtrl(self, value=self.analysisdict.get('fitknotstep','0.3'),size=(160,30))
        self.fitdegreeLabel = wx.StaticText(self, label="Degree (polynom):",size=(160,30))
        self.fitdegreeTextCtrl = wx.TextCtrl(self, value=self.analysisdict.get('fitdegree','5'),size=(160,30))
        self.flaglabelLabel = wx.StaticText(self, label="Flag labels:",size=(160,30))
        self.flaglabelTextCtrl = wx.TextCtrl(self, wx.ID_ANY, value=self.flaglabels, size=(160,150),
                          style = wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))
        self.saveButton = wx.Button(self, wx.ID_OK, label='Save',size=(160,30))

        self.experimentalCheckBox.SetValue(self.guidict.get('experimental',False))
        newf = wx.Font(14, wx.DECORATIVE, wx.ITALIC, wx.BOLD)
        self.basicLabel.SetFont(newf)
        self.calcLabel.SetFont(newf)
        self.otherLabel.SetFont(newf)

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.basicLabel, noOptions),
                    emptySpace,
                    (self.calcLabel, noOptions),
                    emptySpace,
                    (self.stationidLabel, noOptions),
                    (self.stationidTextCtrl, expandOption),
                    (self.fitfunctionLabel, noOptions),
                    (self.fitfunctionComboBox, noOptions),
                    (self.dirnameLabel, noOptions),
                    (self.dirnameTextCtrl, expandOption),
                    (self.fitknotstepLabel, noOptions),
                    (self.fitknotstepTextCtrl, expandOption),
                    (self.exportLabel, noOptions),
                    (self.exportTextCtrl, expandOption),
                    (self.fitdegreeLabel, noOptions),
                    (self.fitdegreeTextCtrl, expandOption),
                    (self.otherLabel, noOptions),
                    emptySpace,
                    emptySpace,
                    emptySpace,
                    (self.baselinedirectLabel, noOptions),
                    (self.baselinedirectCheckBox, noOptions),
                    (self.experimentalLabel, noOptions),
                    (self.experimentalCheckBox, noOptions),
                    (self.FadoptionLabel, noOptions),
                    (self.FadoptionCheckBox, noOptions),
                    emptySpace,
                    emptySpace,
                    (self.flaglabelLabel, noOptions),
                    emptySpace,
                    emptySpace,
                    emptySpace,
                    (self.flaglabelTextCtrl, expandOption),
                    emptySpace,
                    emptySpace,
                    emptySpace,
                    (self.saveButton, dict(flag=wx.ALIGN_CENTER)),
                    emptySpace,
                    emptySpace,
                    (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class OptionsDIDialog(wx.Dialog):
    """
    Dialog for DI specific options
    """

    def __init__(self, parent, title, analysisdict):
        super(OptionsDIDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.analysisdict = analysisdict
        defaultstation = self.analysisdict.get('defaultstation')
        self.currentstation = defaultstation
        self.stations = self.analysisdict.get('stations')
        self.stationlist = [el for el in self.stations]
        self.dicontent = self.stations.get(defaultstation)
        self.newdicontent = self.dicontent.copy()

        self.sheetorder = self.dicontent.get('order','')
        self.sheetdouble = self.dicontent.get('double',False)
        self.sheetscale = self.dicontent.get('scalevalue',False)
        if self.dicontent.get('double',False) in [True,'True']:
            self.sheetdouble = True
        if self.dicontent.get('scalevalue',False) in [True,'True']:
            self.sheetscale = True
        self.createControls()
        self.doLayout()
        self.bindControls()


    # Widgets
    def createControls(self):
        # General paths
        self.stationLabel = wx.StaticText(self, label="Select station code",size=(160,-1))
        self.stationComboBox = wx.ComboBox(self, choices=self.stationlist,
                              style=wx.CB_DROPDOWN, value=self.currentstation,size=(240,-1))
        # Thresholds and defaults
        self.diinputLabel = wx.StaticText(self, label="Input sheet:",size=(160,-1))
        self.sheetorderTextCtrl = wx.TextCtrl(self, value=self.sheetorder,size=(240,-1))
        self.sheetdoubleCheckBox = wx.CheckBox(self, label="Repeated positions",size=(160,-1))
        self.sheetscaleCheckBox = wx.CheckBox(self, label="Scale value",size=(160,-1))

        self.sheetdoubleCheckBox.SetValue(self.sheetdouble)
        self.sheetscaleCheckBox.SetValue(self.sheetscale)
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        self.saveButton = wx.Button(self, wx.ID_OK, label='Save')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=13, cols=4, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.stationLabel, noOptions),
                    (self.stationComboBox, expandOption),
                    (self.diinputLabel, noOptions),
                    (self.sheetorderTextCtrl, expandOption),
                    (self.sheetdoubleCheckBox, noOptions),
                    (self.sheetscaleCheckBox, noOptions),
                    (self.saveButton, dict(flag=wx.ALIGN_CENTER)),
                    (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


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

    def bindControls(self):
        self.stationComboBox.Bind(wx.EVT_COMBOBOX, self.OnStationChange)

    def OnStationChange(self, event):
        # call stream._write_format to determine self.filename
        self.currentstation = self.stationComboBox.GetValue()
        self.dicontent = self.stations.get(self.currentstation)
        print (self.currentstation)
        print (self.dicontent)
        self.sheetorderTextCtrl.SetValue(self.dicontent.get('order',''))
        self.sheetdoubleCheckBox.SetValue(self.dicontent.get('double',False))
        self.sheetscaleCheckBox.SetValue(self.dicontent.get('scalevalue',False))


class OptionsPlotDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Plot options
        modify option
    """

    def __init__(self, parent, title, optdict):
        super(OptionsPlotDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.optdict = optdict
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        for elem in self.optdict:
            #print (elem, self.optdict[elem])
            val = "{}".format(self.optdict[elem])
            exec('self.'+elem+'Text = wx.StaticText(self,label="'+elem+'",size=(160,30))')
            exec('self.'+elem+'TextCtrl = wx.TextCtrl(self, value="'+val+'",size=(160,30))')
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        def applyBox(self,key,noOptions, expandOption):
            return [eval('(self.{}Text, noOptions)'.format(key)),eval('(self.{}TextCtrl, expandOption)'.format(key))]
        contlst = []
        for elem in self.optdict:
            contlst.append(applyBox(self,elem,noOptions,expandOption))

        #contlst = [[eval('(self.'+elem+'Text, noOptions)'),eval('(self.'+elem+'TextCtrl, expandOption)')] for elem in self.optdict]
        contlst = [y for x in contlst for y in x]

        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        cols = 4
        rows = int(np.ceil(len(contlst)/float(cols)))

        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class CheckDefinitiveDataDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select directories for data checking
    """

    def __init__(self, parent, title):
        super(CheckDefinitiveDataDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.checkchoices = ['quick','full']
        self.checkchoice = 'quick'
        self.laststep = 5
        self.minutedirname = ''
        self.seconddirname = ''
        self.checkparameter = {'step2':True, 'step3':True, 'step4':True, 'step5':True }  # modified by checkOptions
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.checkRadioBox = wx.RadioBox(self, label="Choose check type:",  choices=self.checkchoices,
                       majorDimension=2, style=wx.RA_SPECIFY_COLS ) #,size=(160,-1))
        self.minuteLabel = wx.StaticText(self, label="Select minute data:",size=(240,30))
        self.minuteButton = wx.Button(self, label='Choose IAF directory',size=(240,30))
        self.minuteTextCtrl = wx.TextCtrl(self, value=self.minutedirname ,size=(240,30))
        self.secondLabel = wx.StaticText(self, label="(Optional) Select second data:",size=(240,30))
        self.secondButton = wx.Button(self, label='Choose ImagCDF/IAGA02 directory',size=(240,30))
        self.secondTextCtrl = wx.TextCtrl(self, value=self.seconddirname ,size=(240,30))
        self.checkOptionsButton = wx.Button(self, label='Specify check options',size=(240,30))
        self.checkButton = wx.Button(self, wx.ID_OK, label='Run check', size=(240,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(240,30))
        self.note1Label = wx.StaticText(self, label="*quick: ~40 secs with 1second data",size=(240,30))
        self.note2Label = wx.StaticText(self, label="*full: ~7 min with 1second data",size=(240,30))

        self.minuteTextCtrl.Disable()
        self.secondTextCtrl.Disable()
        #self.checkOptionsButton.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.minuteLabel, noOptions),
                  emptySpace,
                 (self.minuteButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.minuteTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.secondLabel, noOptions),
                  emptySpace,
                 (self.secondButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.secondTextCtrl, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.checkRadioBox, noOptions),
                 (self.checkOptionsButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.note1Label, noOptions),
                  emptySpace,
                 (self.note2Label, noOptions),
                  emptySpace,
                 (self.checkButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        #self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.minuteButton.Bind(wx.EVT_BUTTON, self.OnMinute)
        self.secondButton.Bind(wx.EVT_BUTTON, self.OnSecond)
        self.checkOptionsButton.Bind(wx.EVT_BUTTON, self.OnCheckOptions)
        self.Bind(wx.EVT_RADIOBOX, self.OnDeep, self.checkRadioBox)

    #def OnClose(self, e):
    #    self.Close(True)

    def OnMinute(self, e):
        dialog = wx.DirDialog(None, "Choose IAF directory:",self.minutedirname,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.minutedirname = dialog.GetPath() # modify self.dirname
            self.minuteTextCtrl.SetValue(self.minutedirname)
        dialog.Destroy()

    def OnSecond(self, e):
        dialog = wx.DirDialog(None, "Choose ImagCDF/IAGA02 directory:",self.seconddirname,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.seconddirname = dialog.GetPath() # modify self.dirname
            self.secondTextCtrl.SetValue(self.seconddirname)
        dialog.Destroy()

    def OnCheckOptions(self, e):
        dlg = CheckDataSelectDialog(None, title='Select checking steps', checkparameter=self.checkparameter)
        if dlg.ShowModal() == wx.ID_OK:
            #print ("HEEREE", dlg.step2CheckBox.GetValue())
            for key in self.checkparameter:
                val = eval('dlg.'+key+'CheckBox.GetValue()')
                self.checkparameter[key] = val
        else:
            dlg.Destroy()
        steplist = [key.strip('step') for key in self.checkparameter if self.checkparameter.get(key)]
        if len(steplist) == 0:
            steplist=[1]
        self.laststep = np.max(list(map(int,steplist)))

    def OnDeep(self, e):
        self.checkchoice = self.checkRadioBox.GetStringSelection()


class CheckDataReportDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to show report of data check
    """

    def __init__(self, parent, title, config, results, laststep=5):
        super(CheckDataReportDialog, self).__init__(parent=parent,
            title=title, size=(600, 400))
        # Construct report from results
        replist = [results.get("report")]
        replist.append("\n## Errors\n")
        replist.extend(results.get("errors"))
        replist.append("\n## Warnings\n")
        replist.extend(results.get("warnings"))
        replist.append("\n## 1. Files and directories")
        replist.extend(results.get("minute-data-directory",{}).get('report'))
        replist.extend(results.get("second-data-directory",{}).get('report'))
        replist.append("\n## 2. Contents and consistency - monthly report")
        for month in config.get('months'):
            monthdict = results.get(month,{})
            replist.append("### 2.{a} Details for month {a}".format(a=month))
            minlist = monthdict.get('minute',{}).get('report',[])
            seclist = monthdict.get('second',{}).get('report',[])
            replist.extend(minlist)
            replist.extend(seclist)
        replist.extend(results.get("baseline-analysis",{}).get('report',[]))
        replist.extend(results.get("header-analysis",{}).get('report',[]))
        replist.extend(results.get("k-value-analysis",{}).get('report',[]))

        self.report = "\n".join(replist)
        # Construct ratings from results

        self.laststep = laststep
        grades = results.get("grades")
        step = []
        for el in grades:
            step.append(str(grades.get(el,0)))
        self.step = step
        self.rating = np.max(list(map(int,step)))
        currentstep = 5
        #currentstep = (np.max([idx for idx, val in enumerate(step) if not val == '0']))+1
        self.contlabel = 'Save'
        self.moveon = False
        self.createControls()
        self.doLayout()
        self.bindControls()


    def putColor(self, rating, step):
        if rating in ['1']:
            exec("self.step{}TextCtrl.SetBackgroundColour(wx.GREEN)".format(step))
        elif rating in ['2']:
            exec("self.step{}TextCtrl.SetBackgroundColour(wx.Colour(255,165,0))".format(step))
        elif rating in ['3']:
            exec("self.step{}TextCtrl.SetBackgroundColour(wx.RED)".format(step))
        else:
            exec("self.step{}TextCtrl.SetBackgroundColour(wx.BLUE)".format(step))


    # Widgets
    def createControls(self):
        self.reportLabel = wx.StaticText(self, label="Data checking report:",size=(300,30))
        self.reportTextCtrl = wx.TextCtrl(self, value=self.report ,size=(600,300), style = wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)
        self.ratingTextCtrl = wx.TextCtrl(self, value="Overall rating: {}".format(self.rating), size=(30,30))
        self.continueButton = wx.Button(self, label=self.contlabel, size=(160,30))
        self.closeButton = wx.Button(self, label='Cancel',size=(160,30))

        self.step1TextCtrl = wx.TextCtrl(self, value=self.step[0], size=(30,30))
        self.step2TextCtrl = wx.TextCtrl(self, value=self.step[1], size=(30,30))
        self.step3TextCtrl = wx.TextCtrl(self, value=self.step[2], size=(30,30))
        self.step4TextCtrl = wx.TextCtrl(self, value=self.step[3], size=(30,30))
        self.step5TextCtrl = wx.TextCtrl(self, value=self.step[4], size=(30,30))
        self.step1Label = wx.StaticText(self, label="Files",size=(110,30))
        self.step2Label = wx.StaticText(self, label="Contents",size=(110,30))
        self.step3Label = wx.StaticText(self, label="Baseline",size=(110,30))
        self.step4Label = wx.StaticText(self, label="Header",size=(110,30))
        self.step5Label = wx.StaticText(self, label="K values",size=(110,30))

        self.ratingTextCtrl.SetEditable(False)
        self.step1TextCtrl.SetEditable(False)
        self.step2TextCtrl.SetEditable(False)
        self.step3TextCtrl.SetEditable(False)
        self.step4TextCtrl.SetEditable(False)
        self.step5TextCtrl.SetEditable(False)

        for idx, rating in enumerate(self.step):
            self.putColor(rating, idx+1)

        if self.rating in ['1',1]:
            self.ratingTextCtrl.SetBackgroundColour(wx.GREEN)
        elif self.rating in ['2',2]:
            self.ratingTextCtrl.SetBackgroundColour(wx.Colour(255,165,0))
        elif self.rating in ['3',3]:
            self.ratingTextCtrl.SetBackgroundColour(wx.RED)
        else:
            pass


    def doLayout(self):

        mainSizer = wx.BoxSizer(wx.VERTICAL)
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlist = [(self.reportLabel, noOptions),
                 (self.reportTextCtrl, expandOption),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.continueButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.ratingTextCtrl, expandOption)]

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        contlist = [(self.step1TextCtrl, expandOption),
                 (self.step2TextCtrl, expandOption),
                 (self.step3TextCtrl, expandOption),
                 (self.step4TextCtrl, expandOption),
                 (self.step5TextCtrl, expandOption),
                 (self.step1Label, noOptions),
                 (self.step2Label, noOptions),
                 (self.step3Label, noOptions),
                 (self.step4Label, noOptions),
                 (self.step5Label, noOptions)]
        cols = 5
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            mainSizer.Add(control, **options)

        #mainSizer.Add(self.sourceLabel, 0, wx.ALIGN_LEFT | wx.ALL, 3)

        self.SetSizerAndFit(mainSizer)

    def bindControls(self):
        self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        self.continueButton.Bind(wx.EVT_BUTTON, self.OnContinue)

    def OnClose(self, e):
        self.moveon = False
        self.Close(True)

    def OnContinue(self, e):
        self.moveon = True
        self.Close(True)


class CheckDataSelectDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select steps for data check
    """

    def __init__(self, parent, title, checkparameter):
        super(CheckDataSelectDialog, self).__init__(parent=parent,
            title=title, size=(600, 400))
        self.checkparameter = checkparameter #{'step2':True, 'step3':True, 'step4':True, 'step5':True, 'step6':True, 'step7':True }
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.selectLabel = wx.StaticText(self, label="Choose steps to be used in data checking:",size=(400,30))
        self.step1CheckBox = wx.CheckBox(self, label="Step 1: directories and existance of files (obligatory)",size=(400,30))
        self.step2CheckBox = wx.CheckBox(self, label="Step 2: file access, consistency and data contents",size=(400,30))
        self.step3CheckBox = wx.CheckBox(self, label="Step 3: basevalues and adopted baseline variation",size=(400,30))
        self.step4CheckBox = wx.CheckBox(self, label="Step 4: yearly means, consistency of meta information",size=(400,30))
        self.step5CheckBox = wx.CheckBox(self, label="Step 5: activity indicies",size=(400,30))
        self.continueButton = wx.Button(self, wx.ID_OK, label='OK', size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

        self.step1CheckBox.SetValue(True)
        self.step2CheckBox.SetValue(self.checkparameter.get('step2'))
        self.step3CheckBox.SetValue(self.checkparameter.get('step3'))
        self.step4CheckBox.SetValue(self.checkparameter.get('step4'))
        self.step5CheckBox.SetValue(self.checkparameter.get('step5'))

        self.step1CheckBox.Disable()
        self.step2CheckBox.Disable()
        self.step3CheckBox.Disable()
        self.step4CheckBox.Disable()
        self.bindControls()


    def doLayout(self):

        mainSizer = wx.BoxSizer(wx.VERTICAL)
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlist = [(self.selectLabel, noOptions),
                 (self.step1CheckBox, noOptions),
                 (self.step2CheckBox, noOptions),
                 (self.step3CheckBox, noOptions),
                 (self.step4CheckBox, noOptions),
                 (self.step5CheckBox, noOptions)]
        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        contlist = [(self.closeButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.continueButton, dict(flag=wx.ALIGN_CENTER))]
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)
        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            mainSizer.Add(control, **options)

        self.SetSizerAndFit(mainSizer)


    def bindControls(self):
        self.step5CheckBox.Bind(wx.EVT_CHECKBOX, self.on_step5)
        self.step4CheckBox.Bind(wx.EVT_CHECKBOX, self.on_step4)
        self.step3CheckBox.Bind(wx.EVT_CHECKBOX, self.on_step3)

    def on_step5(self, e):
        self.step2CheckBox.Disable()
        self.step3CheckBox.Disable()
        self.step4CheckBox.Disable()
        self.step2CheckBox.SetValue(True)
        self.step3CheckBox.SetValue(True)
        self.step4CheckBox.SetValue(True)
        if self.step5CheckBox.GetValue():
            self.step4CheckBox.Disable()
        else:
            self.step4CheckBox.Enable()

    def on_step4(self, e):
        self.step2CheckBox.Disable()
        self.step3CheckBox.Disable()
        self.step5CheckBox.Enable()
        self.step2CheckBox.SetValue(True)
        self.step3CheckBox.SetValue(True)
        if self.step4CheckBox.GetValue():
            self.step3CheckBox.Disable()
        else:
            self.step3CheckBox.Enable()

    def on_step3(self, e):
        self.step2CheckBox.Disable()
        self.step4CheckBox.Enable()
        self.step5CheckBox.Enable()
        self.step2CheckBox.SetValue(True)
        if self.step3CheckBox.GetValue():
            self.step2CheckBox.Disable()
        else:
            self.step2CheckBox.Enable()


class CheckOpenLogDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to show log file content
    """

    def __init__(self, parent, title, report):
        super(CheckOpenLogDialog, self).__init__(parent=parent,
            title=title, size=(600, 400))
        self.report = report
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.reportLabel = wx.StaticText(self, label="Logging content:",size=(300,30))
        self.reportTextCtrl = wx.TextCtrl(self, value=self.report ,size=(600,300), style = wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Close',size=(160,30))

        #self.reportTextCtrl.Disable()


    def doLayout(self):

        mainSizer = wx.BoxSizer(wx.VERTICAL)
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlist = [(self.reportLabel, noOptions),
                 (self.reportTextCtrl, expandOption),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


    def bindControls(self):
        #self.closeButton.Bind(wx.EVT_BUTTON, self.OnClose)
        pass



# ##################################################################################################################
# ####    Data Panel                                       #########################################################
# ##################################################################################################################

class StreamExtractValuesDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Extracting defined data
    """

    def __init__(self, parent, title, keylst):
        super(StreamExtractValuesDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.keylst = keylst
        self.comparelst = ['==','<=','<','>','>=','!=']
        self.logic3lst = ['and','or']
        self.logic2lst = ['and','or']
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):

        self.keyLabel = wx.StaticText(self, label="Available keys:")
        self.key1ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=self.keylst[0],size=(160,-1))
        self.compare1ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0],size=(160,-1))
        self.value1TextCtrl = wx.TextCtrl(self, value="")
        self.logic2ComboBox = wx.ComboBox(self, choices=self.logic2lst,
            style=wx.CB_DROPDOWN, value=self.logic2lst[0],size=(160,-1))
        if len(self.keylst) > 1:
            val2 =  self.keylst[1]
        else:
            val2 = ''
        self.key2ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=val2,size=(160,-1))
        self.compare2ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0],size=(160,-1))
        self.value2TextCtrl = wx.TextCtrl(self, value="")
        self.logic3ComboBox = wx.ComboBox(self, choices=self.logic3lst,
            style=wx.CB_DROPDOWN, value=self.logic3lst[0],size=(160,-1))
        if len(self.keylst) > 2:
            val3 =  self.keylst[2]
        else:
            val3 = ''
        self.key3ComboBox = wx.ComboBox(self, choices=self.keylst,
            style=wx.CB_DROPDOWN, value=val3,size=(160,-1))
        self.compare3ComboBox = wx.ComboBox(self, choices=self.comparelst,
            style=wx.CB_DROPDOWN, value=self.comparelst[0],size=(160,-1))
        self.value3TextCtrl = wx.TextCtrl(self, value="")
        self.okButton = wx.Button(self, wx.ID_OK, label='Extract')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [emptySpace,
                 (self.keyLabel, noOptions),
                  emptySpace,
                  emptySpace,
                  emptySpace,
                 (self.key1ComboBox, expandOption),
                 (self.compare1ComboBox, expandOption),
                 (self.value1TextCtrl, expandOption),
                 (self.logic2ComboBox, noOptions),
                 (self.key2ComboBox, expandOption),
                 (self.compare2ComboBox, expandOption),
                 (self.value2TextCtrl, expandOption),
                 (self.logic3ComboBox, noOptions),
                 (self.key3ComboBox, expandOption),
                 (self.compare3ComboBox, expandOption),
                 (self.value3TextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                  emptySpace,
                  emptySpace,
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class StreamSelectKeysDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, keylst, shownkeys, namelist):
        super(StreamSelectKeysDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.keylst = keylst
        self.shownkeylst = shownkeys
        self.namelist = namelist
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        for idx,elem in enumerate(self.keylst):
            if len(self.namelist) == len(self.keylst):
                if self.namelist[idx] == None:
                    self.namelist[idx] = 'None'
                colname = self.namelist[idx]
            else:
                colname = elem
            exec('self.'+elem+'CheckBox = wx.CheckBox(self,label="'+colname+'")')
        self.okButton = wx.Button(self, wx.ID_OK, label='Select')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:   ### list comprehension with eval not working in py3
        def applyBox(self,key,expandOption):
            return eval('(self.{}CheckBox, expandOption)'.format(key))
        contlst = []
        for elem in self.keylst:
            contlst.append(applyBox(self,elem,expandOption))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        # A GridSizer will contain the other controls:
        gridSizer = wx.FlexGridSizer(rows=len(contlst), cols=1, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


# ##################################################################################################################
# ####    Meta Panel                                       #########################################################
# ##################################################################################################################

class MetaDataDialog(wx.Dialog):
    """
    DESCRITPTION
        InputDialog for DI data
    """

    def __init__(self, parent, title, header, fields):
        style = wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        super(MetaDataDialog, self).__init__(parent=parent,
            title=title, style=style) #, size=(600, 600))
        self.header = header
        self.list = []
        self.fields=fields

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        # Add Settings Panel
        self.panel = MetaDataPanel(self, header, fields)
        self.panel.SetInitialSize((900, 500))
        self.mainSizer.Add(self.panel, 0, wx.EXPAND | wx.ALL, 20)
        # Add Save/Cancel Buttons
        self.createWidgets()
        # Set sizer and window size
        self.SetSizerAndFit(self.mainSizer)
        #self.mainSizer.Fit(self)

    def createWidgets(self):
        """Create and layout the widgets in the dialog"""
        btnSizer = wx.StdDialogButtonSizer()

        saveBtn = wx.Button(self, wx.ID_OK, label="Update",size=(160,30))
        #saveBtn.Bind(wx.EVT_BUTTON, self.OnSave)
        btnSizer.AddButton(saveBtn)

        cancelBtn = wx.Button(self, wx.ID_NO, label="Close",size=(160,30))
        cancelBtn.Bind(wx.EVT_BUTTON, self.OnClose)
        btnSizer.AddButton(cancelBtn)
        btnSizer.Realize()

        self.mainSizer.Add(btnSizer, 0, wx.ALL | wx.ALIGN_RIGHT, 5)

    def OnClose(self, event):
        self.Close(True)


class MetaDataPanel(scrolledpanel.ScrolledPanel):
    """
    Dialog for MetaData panel
    """
    def __init__(self, parent, header, fields):
        scrolledpanel.ScrolledPanel.__init__(self, parent, -1, size=(1000, 800))

        self.header = header
        self.fields=fields
        self.mainSizer = wx.BoxSizer(wx.VERTICAL)

        self.createControls()
        self.cnts=[0,0]
        self.doLayout()

        self.SetSizer(self.mainSizer)
        self.mainSizer.Fit(self)
        self.SetupScrolling()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        cnt,colcnt = 0,0

        # DYNAMICSIZE
        plat_form = platform.platform()
        if plat_form.startswith('linux') or plat_form.startswith('Linux'):
            dynsize = '30'
        else:
            dynsize = '50'

        for key in self.fields:
            if key.find('-') > 0:
                # Column contents:
                tmplst = key.split('-')
                tmplst[-1] = KEYLIST.index(tmplst[-1])
                if tmplst[0] == 'unit':
                    label = tmplst[1].replace('col','Column') + str(tmplst[-1])+'_unit'
                else:
                    label = tmplst[0].replace('col','Column') + str(tmplst[-1])
                key=key.replace('-','')
                colcnt += 1
            else:
                label = key
                value = str(self.header.get(key,''))
                value = value.replace('\r\n',' ').replace('\n','')
                if not isinstance(value, str) or '[' in value:
                     #print ("not a string")
                     try:
                         try:
                             float(value)
                         except:
                             value = 'object with complex data'
                     except:
                         value = 'object with complex data'
                cnt += 1
                label = self.AppendLabel(key,label)
                exec('self.'+key+'Text = wx.StaticText(self,label="'+label+'")')
                exec('self.'+key+'TextCtrl = wx.TextCtrl(self, value="'+value+'",size=(160,'+dynsize+'),style = wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)')
                if value.startswith('object with complex'):
                    exec('self.'+key+'TextCtrl.Disable()')
        self.cnts = [colcnt, cnt]
        self.legendText = wx.StaticText(self,label="(1: IAF, 2: IAGA, 3: IMAGCDF)")

    def doLayout(self):
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # fill all lines with empty fields to max(cnt)
        # transpose this array
        contlst = []

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        headarray = [[],[],[],[],[]]
        for key in self.fields:
            contlst.append(eval('(self.'+key+'Text, noOptions)'))
            contlst.append(eval('(self.'+key+'TextCtrl, expandOption)'))

        contlst.append(emptySpace)
        contlst.append((self.legendText, noOptions))

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        self.mainSizer.Add(gridSizer, 0, wx.EXPAND)


    def AppendLabel(self, key,label):
        from magpy.lib.magpy_formats import IAFMETA, IAGAMETA, IMAGCDFMETA
        #print (IAFMETA, IAGAMETA, IMAGCDFMETA)
        if key in IAFMETA:
            if not label.find('(') > 0:
                label += '(1'
        if key in IAGAMETA:
            if not label.find('(') > 0:
                label += '(2'
            else:
                label += ',2'
        if key in IMAGCDFMETA:
            if not label.find('(') > 0:
                label += '(3'
            else:
                label += ',3'
        if label.find('(') > 0:
            label += ')'

        return label


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

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:",size=(160,30))
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.datalst[0],size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        # A GridSizer will contain the other controls:
        #gridSizer = wx.FlexGridSizer(rows=7, cols=2, vgap=10, hgap=10)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist=[(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

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



# ##################################################################################################################
# ####    Flagging Panel                                   #########################################################
# ##################################################################################################################

class FlaggingGroupsDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for adding flag groups to the specific data flags
    USED BY:
        Stream Method: onFlagOutlier()
    """
    def __init__(self, parent, title, groups):
        super(FlaggingGroupsDialog, self).__init__(parent=parent,
            title=title, size=(600, 700))
        fl = flagging.Flags()
        self.groups = groups
        self.existinggroups = []
        if groups:
            self.existinggroups = ["{} : {}".format(key,",".join(groups.get(key))) for key in groups]
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.addgroupText = wx.StaticText(self,label="SensorID or SensorGroup")
        self.addvariablesText = wx.StaticText(self,label="Variables")
        self.addgroupTextCtrl = wx.TextCtrl(self, value="", size=(160,-1))
        self.addvariablesTextCtrl = wx.TextCtrl(self, value="x,y,z,f", size=(160,-1))
        self.groupsListBox = wx.ListBox(self, 26, wx.DefaultPosition, (160, 130), self.existinggroups, wx.LB_SINGLE)
        self.addButton = wx.Button(self, label='Add')
        self.removeButton = wx.Button(self, label='Remove')
        self.okButton = wx.Button(self, wx.ID_OK, label='Finished')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.addgroupText, noOptions))
        contlst.append((self.addvariablesText, noOptions))
        contlst.append((self.addgroupTextCtrl, expandOption))
        contlst.append((self.addvariablesTextCtrl, expandOption))
        contlst.append((self.groupsListBox, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.addButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.removeButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.addButton.Bind(wx.EVT_BUTTON, self.OnAddGroup)
        self.removeButton.Bind(wx.EVT_BUTTON, self.OnRemoveGroups)

    def OnAddGroup(self, e):
        sens = self.addgroupTextCtrl.GetValue()
        vars = self.addvariablesTextCtrl.GetValue()
        if sens and vars:
            newgroup = "{} : {}".format(sens, vars)
            self.existinggroups.append(newgroup)
            self.groupsListBox.Set(self.existinggroups)

    def OnRemoveGroups(self, e):
        idx = self.groupsListBox.GetSelection()
        self.existinggroups = [el for i, el in enumerate(self.existinggroups) if not i == idx]
        self.groupsListBox.Set(self.existinggroups)

class FlagModificationDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog with all flagging parameters for the selected flag.
        Allows for editing the parameters
        sensorid=None, starttime=None, endtime=None, components=None, flagtype=0, labelid='000', label='',
            comment='', groups=None, probabilities=None, stationid='', validity='', operator='', color='',
            modificationtime=None, flagversion='2.0', minimumtimediff
    USED BY:
        Stream Method: _update_flags_onclick()
    """
    def __init__(self, parent, title, flagobject, flag):
        super(FlagModificationDialog, self).__init__(parent=parent, title=title, size=(600, 700))
        self.validityselect = ['','h : hide','d : delete']
        labelid = flag.get('labelid')
        validity = flag.get('validity')
        valsel = self.validityselect[0]
        if validity == 'd':
            valsel = self.validityselect[2]
        elif validity == 'h':
            valsel = self.validityselect[1]
        self.valsel = valsel
        self.groups = flag.get('groups')
        self.grouplabel = 'Define groups'
        if self.groups:
            self.grouplabel = ",".join([k for k in self.groups])
        self.flag = flag
        self.labels = ["{}: {}".format(key, flagobject.FLAGLABEL.get(key)) for key in flagobject.FLAGLABEL]
        self.currentlabelindex = [i for i, el in enumerate(self.labels) if el.startswith(labelid)][0]
        #print (flagobject.FLAGTYPE)
        cftdict = flagobject.FLAGTYPE.get(flag.get('flagversion'))
        self.flagidlist = ["{}: {}".format(key,cftdict.get(key)) for key in cftdict]
        flagtype = self.flag.get('flagtype')
        self.flagtypeselect = [el for el in  self.flagidlist if el.startswith(str(flagtype))][0]
        self.stationid = self.flag.get('stationid')
        if not self.stationid:
            self.stationid = ''

        self.createControls()
        self.doLayout()
        self.bindControls()

    def createControls(self):
        try:
            stda = wx.DateTime.FromDMY(day=self.flag.get('starttime').day,month=self.flag.get('starttime').month-1,year=self.flag.get('starttime').year)
            edda = wx.DateTime.FromDMY(day=self.flag.get('endtime').day,month=self.flag.get('endtime').month-1,year=self.flag.get('endtime').year)
        except:
            stda = wx.DateTimeFromDMY(day=self.flag.get('starttime').day,month=self.flag.get('starttime').month-1,year=self.flag.get('starttime').year)
            edda = wx.DateTimeFromDMY(day=self.flag.get('endtime').day,month=self.flag.get('endtime').month-1,year=self.flag.get('endtime').year)
        # countvariables for specific header blocks
        self.sensoridText = wx.StaticText(self,label="SensorID:")
        self.starttimeText = wx.StaticText(self,label="Starttime:")
        self.endtimeText = wx.StaticText(self,label="Endtime:")
        self.componentsText = wx.StaticText(self,label="Components:")
        self.flagidText = wx.StaticText(self,label="Flagtype:")
        self.labelText = wx.StaticText(self,label="Label:")
        self.commentText = wx.StaticText(self,label="Comment:")
        self.operatorText = wx.StaticText(self,label="Operator:")
        self.groupsText = wx.StaticText(self,label="Groups:")
        self.validityText = wx.StaticText(self,label="Validity:")
        self.stationidText = wx.StaticText(self,label="StationID:")
        self.colorText = wx.StaticText(self,label="Color:")
        self.probabilitiesText = wx.StaticText(self,label="Probabilities:")
        self.sensoridTextCtrl = wx.TextCtrl(self, value=self.flag.get('sensorid'),size=(160,30))
        self.startFlagDatePicker = wxDatePickerCtrl(self, dt=stda,size=(160,30))
        self.startFlagTimePicker = wx.TextCtrl(self, value=self.flag.get('starttime').strftime('%X'),size=(160,30))
        self.endFlagDatePicker = wxDatePickerCtrl(self, dt=edda,size=(160,30))
        self.endFlagTimePicker = wx.TextCtrl(self, value=self.flag.get('endtime').strftime('%X'),size=(160,30))
        self.componentsTextCtrl = wx.TextCtrl(self, value=",".join(self.flag.get('components',[])),size=(160,30))
        self.commentTextCtrl = wx.TextCtrl(self, value=self.flag.get('comment',''), size=(160, 30))
        self.operatorTextCtrl = wx.TextCtrl(self, value=self.flag.get('operator',''), size=(160, 30))
        self.stationidTextCtrl = wx.TextCtrl(self, value=self.stationid, size=(160, 30))
        self.colorTextCtrl = wx.TextCtrl(self, value=self.flag.get('color',''), size=(160, 30))
        self.probabilitiesTextCtrl = wx.TextCtrl(self, value=self.flag.get('probability',''), size=(160, 30))
        self.flagidComboBox = wx.ComboBox(self, choices=self.flagidlist,
            style=wx.CB_DROPDOWN, value=self.flagtypeselect,size=(160,-1))
        self.labelComboBox = wx.ComboBox(self, choices=self.labels,
            style=wx.CB_DROPDOWN, value=self.labels[self.currentlabelindex],size=(160,-1))
        self.validityComboBox = wx.ComboBox(self, choices=self.validityselect,
            style=wx.CB_DROPDOWN, value=self.valsel,size=(160,-1))
        self.groupsButton = wx.Button(self, label=self.grouplabel,size=(200,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Update flag')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        self.sensoridTextCtrl.Disable()
        self.probabilitiesTextCtrl.Disable()

    def doLayout(self):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)
        contlst = []
        contlst.append((self.sensoridText, noOptions))
        contlst.append((self.stationidText, noOptions))
        contlst.append((self.componentsText, noOptions))
        contlst.append((self.commentText, noOptions))
        contlst.append((self.sensoridTextCtrl, expandOption))
        contlst.append((self.stationidTextCtrl, expandOption))
        contlst.append((self.componentsTextCtrl, expandOption))
        contlst.append((self.commentTextCtrl, expandOption))
        #
        contlst.append((self.starttimeText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.endtimeText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.startFlagDatePicker, expandOption))
        contlst.append((self.startFlagTimePicker, expandOption))
        contlst.append((self.endFlagDatePicker, expandOption))
        contlst.append((self.endFlagTimePicker, expandOption))
        #
        contlst.append((self.labelText, noOptions))
        contlst.append((self.flagidText, noOptions))
        contlst.append((self.operatorText, noOptions))
        contlst.append((self.groupsText, noOptions))
        contlst.append((self.labelComboBox, expandOption))
        contlst.append((self.flagidComboBox, expandOption))
        contlst.append((self.operatorTextCtrl, expandOption))
        contlst.append((self.groupsButton, dict(flag=wx.ALIGN_CENTER)))
        #
        contlst.append((self.validityText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.colorText, noOptions))
        contlst.append((self.probabilitiesText, noOptions))
        contlst.append((self.validityComboBox, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.colorTextCtrl, expandOption))
        contlst.append((self.probabilitiesTextCtrl, expandOption))
        #
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.groupsButton.Bind(wx.EVT_BUTTON, self.OnSelectGroups)
        self.labelComboBox.Bind(wx.EVT_COMBOBOX, self.OnUpdateLabel)

    def OnSelectGroups(self, e):
        dlg = FlaggingGroupsDialog(None, title='Define flagging groups', groups=self.groups)
        if dlg.ShowModal() == wx.ID_OK:
            # get values from dlg
            grouplist = dlg.existinggroups
            for g in dlg.existinggroups:
                gl = g.split(' : ')
                self.groups[gl[0]] = gl[1].split(',')
        dlg.Destroy()
        self.grouplabel = 'Define groups'
        if self.groups:
            self.grouplabel = ",".join([k for k in self.groups])
        self.groupsButton.SetLabel(self.grouplabel)

    def OnUpdateLabel(self, event):
        """
        DESCRIPTION
            update flagtype according to labelid
        :param e:
        :return:
        """
        label = self.labelComboBox.GetStringSelection()
        labelid = label[:3]
        #print ("Changed label to", labelid)
        if 10 <= int(labelid) < 50:
            self.flagidComboBox.SetValue(self.flagidlist[4])
        else:
            self.flagidComboBox.SetValue(self.flagidlist[3])




class FlagOutlierDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Parameter selection of outlier flagging routine
    USED BY:
        Stream Method: onFlagOutlier()
    """
    def __init__(self, parent, title, threshold, timerange, markall, labelid, operator, groups, flaglabel):
        super(FlagOutlierDialog, self).__init__(parent=parent,
            title=title, size=(600, 700))
        fl = flagging.Flags(None,flaglabel)
        self.threshold = str(threshold)
        self.timerange = str(timerange)
        self.markall = markall
        self.labelid = labelid
        self.operator = operator
        if isinstance(groups, dict):
            self.groups = groups
        else:
            self.groups = {}
        self.labels = ["{}: {}".format(key, fl.FLAGLABEL.get(key)) for key in fl.FLAGLABEL]
        self.currentlabelindex = [i for i, el in enumerate(self.labels) if el.startswith(labelid)][0]
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.ThresholdText = wx.StaticText(self,label="Threshold")
        self.TimerangeText = wx.StaticText(self,label="Window width (sec)")
        self.OperatorText = wx.StaticText(self,label="Operator")
        self.ThresholdTextCtrl = wx.TextCtrl(self, value=self.threshold, size=(160,-1))
        self.TimerangeTextCtrl = wx.TextCtrl(self, value=self.timerange, size=(160,-1))
        self.OperatorTextCtrl = wx.TextCtrl(self, value=self.operator, size=(160,-1))
        self.MarkAllCheckBox = wx.CheckBox(self, label="other components",size=(160,-1))
        self.MarkText = wx.StaticText(self,label="Mark outliers also in")
        self.LabelText = wx.StaticText(self,label="Select label")
        self.LabelComboBox = wx.ComboBox(self, choices=self.labels,
            style=wx.CB_DROPDOWN, value=self.labels[self.currentlabelindex],size=(160,-1))
        self.GroupButton = wx.Button(self, label='Groups',size=(200,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        self.MarkAllCheckBox.SetValue(self.markall)
        self.OperatorTextCtrl.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.ThresholdText, noOptions))
        contlst.append((self.TimerangeText, noOptions))
        contlst.append((self.OperatorText, noOptions))
        contlst.append((self.ThresholdTextCtrl, expandOption))
        contlst.append((self.TimerangeTextCtrl, expandOption))
        contlst.append((self.OperatorTextCtrl, expandOption))
        contlst.append((self.LabelText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.MarkText, noOptions))
        contlst.append((self.LabelComboBox, expandOption))
        contlst.append((self.GroupButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.MarkAllCheckBox, noOptions))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 3
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.GroupButton.Bind(wx.EVT_BUTTON, self.OnSelectGroups)

    def OnSelectGroups(self, e):
        dlg = FlaggingGroupsDialog(None, title='Define flagging groups', groups=self.groups)
        if dlg.ShowModal() == wx.ID_OK:
            # get values from dlg
            grouplist = dlg.existinggroups
            for g in dlg.existinggroups:
                gl = g.split(' : ')
                self.groups[gl[0]] = gl[1].split(',')
        dlg.Destroy()


class FlagRangeDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Parameter selection of flag range routine
    USED BY:
        Stream Method: onFlagRange()
    """
    def __init__(self, parent, title, stream, shownkeylist, keylist, labelid, operator, groups, flagversion, flaglabel):
        super(FlagRangeDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        fl = flagging.Flags(None,flaglabel)
        self.shownkeys=shownkeylist
        self.selectedkey = shownkeylist[0]
        self.keys2flag = ",".join(shownkeylist)
        self.keys=keylist
        self.stream = stream
        self.mintime = stream.start()
        self.maxtime = stream.end()
        self.labelid = labelid
        self.operator = operator
        self.labels = ["{}: {}".format(key, fl.FLAGLABEL.get(key)) for key in fl.FLAGLABEL]
        self.currentlabelindex = [i for i, el in enumerate(self.labels) if el.startswith(labelid)][0]
        cftdict = fl.FLAGTYPE.get(flagversion)
        self.flagidlist = ["{}: {}".format(key,cftdict.get(key)) for key in cftdict]
        self.comment = ''
        if isinstance(groups, dict):
            self.groups = groups
        else:
            self.groups = {}
        #dt=wx.DateTimeFromTimeT(time.mktime(self.maxtime.timetuple()))
        self.ul = np.nanmax(self.stream.ndarray[KEYLIST.index(self.selectedkey)])
        self.ll = np.nanmin(self.stream.ndarray[KEYLIST.index(self.selectedkey)])
        self.rangetype = ['time', 'value']
        self.createControls()
        self.doLayout()
        self.bindControls()
        self.SetValue()

    # Widgets
    def createControls(self):
        try:
            stda = wx.DateTime.FromDMY(day=self.mintime.day,month=self.mintime.month-1,year=self.mintime.year)
            edda = wx.DateTime.FromDMY(day=self.maxtime.day,month=self.maxtime.month-1,year=self.maxtime.year)
        except:
            stda = wx.DateTimeFromDMY(day=self.mintime.day,month=self.mintime.month-1,year=self.mintime.year)
            edda = wx.DateTimeFromDMY(day=self.maxtime.day,month=self.maxtime.month-1,year=self.maxtime.year)
        # countvariables for specific header blocks
        self.TimeRangeText = wx.StaticText(self,label="Flag time range")
        self.ValueRangeText = wx.StaticText(self,label="Flag value range")
        self.LimitKeyText = wx.StaticText(self,label="Select Key:")
        self.UpperLimitText = wx.StaticText(self,label="Flag values below:")
        self.UpperLimitTextCtrl = wx.TextCtrl(self, value=str(self.ul),size=(160,30))
        self.LowerLimitText = wx.StaticText(self,label="Flag values above:")
        self.LowerLimitTextCtrl = wx.TextCtrl(self, value=str(self.ll),size=(160,30))
        self.SelectKeyComboBox = wx.ComboBox(self, choices=self.shownkeys,
            style=wx.CB_DROPDOWN, value=self.shownkeys[self.shownkeys.index(self.selectedkey)],size=(160,-1))
        self.UpperTimeText = wx.StaticText(self,label="Flag data before:")
        self.LowerTimeText = wx.StaticText(self,label="Flag data after:")
        self.startFlagDatePicker = wxDatePickerCtrl(self, dt=stda,size=(160,30))
        self.startFlagTimePicker = wx.TextCtrl(self, value=self.mintime.strftime('%X'),size=(160,30))
        self.endFlagDatePicker = wxDatePickerCtrl(self, dt=edda,size=(160,30))
        self.endFlagTimePicker = wx.TextCtrl(self, value=self.maxtime.strftime('%X'),size=(160,30))
        self.KeyListText = wx.StaticText(self,label="Keys to flag:")
        self.AffectedKeysTextCtrl = wx.TextCtrl(self, value=self.keys2flag,size=(160,30))
        self.FlagIDText = wx.StaticText(self,label="Select Flagtype:")
        self.FlagIDComboBox = wx.ComboBox(self, choices=self.flagidlist,
            style=wx.CB_DROPDOWN, value=self.flagidlist[3],size=(160,-1))
        self.LabelText = wx.StaticText(self,label="Select label")
        self.LabelComboBox = wx.ComboBox(self, choices=self.labels,
            style=wx.CB_DROPDOWN, value=self.labels[self.currentlabelindex],size=(200,-1))
        self.OperatorText = wx.StaticText(self,label="Operator:")
        self.OperatorTextCtrl = wx.TextCtrl(self, value=self.operator,size=(200,-1))
        self.GroupText = wx.StaticText(self,label="Group:")
        self.GroupButton = wx.Button(self, label='Groups',size=(200,30))
        self.CommentText = wx.StaticText(self,label="Comment:")
        self.CommentTextCtrl = wx.TextCtrl(self, value=self.comment,size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))
        self.rangeRadioBox = wx.RadioBox(self,
            label="Select range type:",
            choices=self.rangetype, majorDimension=2, style=wx.RA_SPECIFY_COLS, size=(160,-1))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        #
        contlst.append((self.rangeRadioBox, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        #
        contlst.append((self.TimeRangeText, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        #
        contlst.append((self.LowerTimeText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.UpperTimeText, noOptions))
        contlst.append(emptySpace)
        #
        contlst.append((self.startFlagDatePicker, expandOption))
        contlst.append((self.startFlagTimePicker, expandOption))
        contlst.append((self.endFlagDatePicker, expandOption))
        contlst.append((self.endFlagTimePicker, expandOption))
        #
        contlst.append((self.ValueRangeText, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        #
        contlst.append((self.LimitKeyText, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.LowerLimitText, noOptions))
        contlst.append((self.UpperLimitText, noOptions))
        #
        contlst.append((self.SelectKeyComboBox, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.LowerLimitTextCtrl, expandOption))
        contlst.append((self.UpperLimitTextCtrl, expandOption))
        #
        contlst.append((self.KeyListText, noOptions))
        contlst.append((self.LabelText, noOptions))
        contlst.append((self.FlagIDText, noOptions))
        contlst.append((self.OperatorText, noOptions))
        #
        contlst.append((self.AffectedKeysTextCtrl, expandOption))
        contlst.append((self.LabelComboBox, expandOption))
        contlst.append((self.FlagIDComboBox, expandOption))
        contlst.append((self.OperatorTextCtrl, expandOption))
        #
        contlst.append((self.CommentText, noOptions))
        contlst.append((self.GroupText, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        #
        contlst.append((self.CommentTextCtrl, expandOption))
        contlst.append((self.GroupButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        #
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.Bind(wx.EVT_RADIOBOX, self.OnChangeGroup, self.rangeRadioBox)
        self.Bind(wx.EVT_COMBOBOX, self.OnChangeSelection, self.SelectKeyComboBox)
        self.GroupButton.Bind(wx.EVT_BUTTON, self.OnSelectGroups)
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
        if 10 <= int(labelid) < 50:
            self.FlagIDComboBox.SetValue(self.flagidlist[4])
        else:
            self.FlagIDComboBox.SetValue(self.flagidlist[3])

    def SetValue(self):
        self.UpperLimitTextCtrl.Disable()
        self.LowerLimitTextCtrl.Disable()
        self.SelectKeyComboBox.Disable()
        self.LowerTimeText.Enable()
        self.UpperTimeText.Enable()
        self.startFlagDatePicker.Enable()
        self.startFlagTimePicker.Enable()
        self.endFlagDatePicker.Enable()
        self.endFlagTimePicker.Enable()
        self.UpperLimitTextCtrl.SetValue(str(self.ul))
        self.LowerLimitTextCtrl.SetValue(str(self.ll))


    def OnChangeGroup(self, e):
        val = self.rangeRadioBox.GetStringSelection()
        if str(val) == 'time':
            self.UpperLimitTextCtrl.Disable()
            self.LowerLimitTextCtrl.Disable()
            self.SelectKeyComboBox.Disable()
            self.LowerTimeText.Enable()
            self.UpperTimeText.Enable()
            self.startFlagDatePicker.Enable()
            self.startFlagTimePicker.Enable()
            self.endFlagDatePicker.Enable()
            self.endFlagTimePicker.Enable()
        elif str(val) == 'value':
            self.UpperLimitTextCtrl.Enable()
            self.LowerLimitTextCtrl.Enable()
            self.SelectKeyComboBox.Enable()
            self.LowerTimeText.Disable()
            self.UpperTimeText.Disable()
            self.startFlagDatePicker.Disable()
            self.startFlagTimePicker.Disable()
            self.endFlagDatePicker.Disable()
            self.endFlagTimePicker.Disable()
            self.UpperLimitTextCtrl.SetValue(str(self.ul))
            self.LowerLimitTextCtrl.SetValue(str(self.ll))

    def OnChangeSelection(self, e):
        firstkey = self.SelectKeyComboBox.GetValue()
        ind = KEYLIST.index(firstkey)
        self.ul = np.nanmax(self.stream.ndarray[ind])
        self.ll = np.nanmin(self.stream.ndarray[ind])
        self.UpperLimitTextCtrl.SetValue(str(self.ul))
        self.LowerLimitTextCtrl.SetValue(str(self.ll))

    def OnSelectGroups(self, e):
        #print ("Groups look like:", self.groups)
        dlg = FlaggingGroupsDialog(None, title='Define flagging groups', groups=self.groups)
        if dlg.ShowModal() == wx.ID_OK:
            # get values from dlg
            grouplist = dlg.existinggroups
            for g in dlg.existinggroups:
                gl = g.split(' : ')
                self.groups[gl[0]] = gl[1].split(',')
        dlg.Destroy()
        #print ("Groups now look like:", self.groups)


class FlagSelectionDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Parameter selection of flag range routine
    USED BY:
        Stream Method: onFlagSelection()
    """
    def __init__(self, parent, title, shownkeylist, keylist, labelid, operator, groups, flagversion, flaglabel):
        super(FlagSelectionDialog, self).__init__(parent=parent,
            title=title, size=(600, 800))
        fl = flagging.Flags(None,flaglabel)
        self.shownkeys=shownkeylist
        self.selectedkey = shownkeylist[0]
        self.keys2flag = ",".join(shownkeylist)
        self.keys=keylist
        self.labelid = labelid
        self.operator = operator
        self.labels = ["{}: {}".format(key, fl.FLAGLABEL.get(key)) for key in fl.FLAGLABEL]
        self.currentlabelindex = [i for i, el in enumerate(self.labels) if el.startswith(labelid)][0]
        cftdict = fl.FLAGTYPE.get(flagversion)
        self.flagidlist = ["{}: {}".format(key,cftdict.get(key)) for key in cftdict]
        self.comment = ''
        self.operator = operator
        if isinstance(groups, dict):
            self.groups = groups
        else:
            self.groups = {}
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.KeyListText = wx.StaticText(self,label="Keys which will be flagged:")
        self.AffectedKeysTextCtrl = wx.TextCtrl(self, value=self.keys2flag,size=(200,-1))
        self.FlagIDText = wx.StaticText(self,label="Select Flag ID:")
        self.FlagIDComboBox = wx.ComboBox(self, choices=self.flagidlist,
            style=wx.CB_DROPDOWN, value=self.flagidlist[3],size=(200,-1))
        self.LabelText = wx.StaticText(self,label="Select label")
        self.LabelComboBox = wx.ComboBox(self, choices=self.labels,
            style=wx.CB_DROPDOWN, value=self.labels[self.currentlabelindex],size=(200,-1))
        self.OperatorText = wx.StaticText(self,label="Operator:")
        self.OperatorTextCtrl = wx.TextCtrl(self, value=self.operator,size=(200,-1))
        self.GroupText = wx.StaticText(self,label="Group:")
        self.GroupButton = wx.Button(self, label='Groups',size=(200,30))
        self.CommentText = wx.StaticText(self,label="Comment:")
        self.CommentTextCtrl = wx.TextCtrl(self, value=self.comment,size=(200,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(200,-1))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(200,-1))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = [(self.KeyListText, noOptions)]
        #contlst.append((self.KeyListText, noOptions))
        contlst.append((self.LabelText, noOptions))
        contlst.append((self.FlagIDText, noOptions))
        contlst.append((self.AffectedKeysTextCtrl, expandOption))
        contlst.append((self.LabelComboBox, expandOption))
        contlst.append((self.FlagIDComboBox, expandOption))
        contlst.append((self.CommentText, noOptions))
        contlst.append((self.GroupText, noOptions))
        contlst.append((self.OperatorText, noOptions))
        contlst.append((self.CommentTextCtrl, expandOption))
        contlst.append((self.GroupButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.OperatorTextCtrl, expandOption))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 3
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.LabelComboBox.Bind(wx.EVT_COMBOBOX, self.OnUpdateLabel)
        self.GroupButton.Bind(wx.EVT_BUTTON, self.OnSelectGroups)

    def OnUpdateLabel(self, event):
        """
        DESCRIPTION
            update flagtype according to labelid
        :param e:
        :return:
        """
        label = self.LabelComboBox.GetStringSelection()
        labelid = label[:3]
        #print ("Changed label to", labelid)
        if 10 <= int(labelid) < 50:
            self.FlagIDComboBox.SetValue(self.flagidlist[4])
        else:
            self.FlagIDComboBox.SetValue(self.flagidlist[3])

    def OnSelectGroups(self, e):
        dlg = FlaggingGroupsDialog(None, title='Define flagging groups', groups=self.groups)
        if dlg.ShowModal() == wx.ID_OK:
            # get values from dlg
            grouplist = dlg.existinggroups
            for g in dlg.existinggroups:
                gl = g.split(' : ')
                self.groups[gl[0]] = gl[1].split(',')
        dlg.Destroy()


class FlagLoadDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Loading Flagging data from file or DB
    """
    def __init__(self, parent, title, db, sensorid, start, end, header, last_dir: string =''):
        super(FlagLoadDialog, self).__init__(parent=parent,
            title=title, size=(300, 300))
        self.fl = flagging.Flags()
        self.sensorid = sensorid
        self.header = header
        self.db = db
        self.start = start
        self.end = end
        self.createControls()
        self.doLayout()
        self.bindControls()
        self.last_dir = last_dir

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.loadDBButton = wx.Button(self, label='Load from DB')
        self.loadFileButton = wx.Button(self, label='Load file')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        if not self.db:
            self.loadDBButton.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.loadDBButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.loadFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.loadDBButton.Bind(wx.EVT_BUTTON, self.OnLoadDB)
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnLoadFile)

    def OnLoadDB(self, e):
        fl = self.db.flags_from_db(starttime=self.start, endtime=self.end) # sensorid=self.sensorid,
        # keep only flags matching sensorid and group
        newflagdict = {}
        for d in fl.flagdict:
            flagcont = fl.flagdict[d]
            # test, if sensorid is fitting or sensorid/group is part of groups
            valid, comps = fl._match_groups(self.header, flagcont.get('sensorid'),
                                            flag_keys=flagcont.get('components'),
                                            flag_groups=flagcont.get('groups'))
            # test validity parameter for d or h
            if flagcont.get('validity') in ['d', 'h']:
                valid = False
            if valid:
                newflagdict[d] = flagcont
        fl.flagdict = newflagdict
        self.fl = fl

        dlg = wx.MessageDialog(self, "Flags loaded from DB!\nFLAGS table contained {} inputs for this sensor and its group\n".format(len(self.fl)),"FLAGS obtained from DB", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.Close(True)

    def OnLoadFile(self, e):
        openFileDialog = wx.FileDialog(self, "Open", self.last_dir, "",
                                       "json flaglist (*.json)|*.json|pickle flaglist (*.pkl)|*.pkl|all files (*.*)|*.*",
                                       wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        if openFileDialog.ShowModal() == wx.ID_OK:
            flagname = openFileDialog.GetPath()
            try:
                fl = flagging.load(flagname, begin=self.start, end=self.end) #,sensorid=self.sensorid - removed sensorid for correct application of groups
            except:
                fl = flagging.Flags()
            openFileDialog.Destroy()
            # keep only flags matching sensorid and group
            newflagdict = {}
            for d in fl.flagdict:
                flagcont = fl.flagdict[d]
                # test, if sensorid is fitting or sensorid/group is part of groups
                valid, comps = fl._match_groups(self.header, flagcont.get('sensorid'),
                                                flag_keys=flagcont.get('components'),
                                                flag_groups=flagcont.get('groups'))
                # test validity parameter for d or h
                if flagcont.get('validity') in ['d', 'h']:
                    valid = False
                if valid:
                    newflagdict[d] = flagcont
            fl.flagdict = newflagdict
            self.fl = fl

            dlg = wx.MessageDialog(self, "Flags loaded from File!\nFound a total of {} flag inputs for this sensor and its group\n".format(len(self.fl)),"FLAGS obtained from File", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
        else:
            openFileDialog.Destroy()
        self.Close(True)


class FlagSaveDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for Loading Flagging data from file or DB
    """
    def __init__(self, parent, title, db, flaglist, last_dir: string =''):
        super(FlagSaveDialog, self).__init__(parent=parent,
            title=title, size=(300, 300))
        self.fl = flaglist
        self.db = db
        self.last_dir = last_dir
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.saveDBButton = wx.Button(self, label='Save to DB')
        self.saveFileButton = wx.Button(self, label='Save to file')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        if not self.db:
            self.saveDBButton.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.saveDBButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.saveFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.saveDBButton.Bind(wx.EVT_BUTTON, self.OnSaveDB)
        self.saveFileButton.Bind(wx.EVT_BUTTON, self.OnSaveFile)

    def OnSaveDB(self, e):
        #print ("Saving", self.flaglist[0])
        self.db.flags_to_db(self.fl)
        dlg = wx.MessageDialog(self, "Flags stored in connected DB!\nFLAGS table extended with {} inputs\n".format(len(self.fl)),"FLAGS added to DB", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()
        self.Close(True)

    def OnSaveFile(self, e):
        saveFileDialog = wx.FileDialog(self, "Save As", self.last_dir,"",
                                       "json flaglist (*.json)|*.json|pickle flaglist (*.pkl)|*.pkl",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        if saveFileDialog.ShowModal() == wx.ID_OK:
            extensions = ['.json','.pkl']
            extind = saveFileDialog.GetFilterIndex()

            flagname = saveFileDialog.GetPath()
            if not flagname.endswith(extensions[extind]):
                flagname = flagname+extensions[extind]

            saveFileDialog.Destroy()
            self.fl.save(flagname)
        self.Close(True)


class FlagDetailsDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """
    def __init__(self, parent, title, stats, flags, stream):
        super(FlagDetailsDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.stats = stats
        self.fl = flags
        self.plotstream = stream
        self.newfl = flagging.Flags()
        self.mod = False
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.statsLabel = wx.StaticText(self,label="Flagging statistics")
        self.statsTextCtrl = wx.TextCtrl(self,value=self.stats,size=(400,300),style=wx.TE_MULTILINE|wx.HSCROLL|wx.VSCROLL)
        self.modifyButton = wx.Button(self, label='Modify Flags')
        self.okButton = wx.Button(self, wx.ID_OK, label='OK')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.statsLabel, noOptions))
        contlst.append((self.statsTextCtrl, expandOption))
        contlst.append((self.modifyButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.modifyButton.Bind(wx.EVT_BUTTON, self.OnModify)

    def OnModify(self, e):
        # open modification dlg
        dlg = FlagModDialog(None, title='Modify flags')
        if dlg.ShowModal() == wx.ID_OK:
            select = dlg.selectComboBox.GetValue()
            parameter = dlg.parameterComboBox.GetValue()
            value = dlg.valueTextCtrl.GetValue()
            newvalue = dlg.newvalueTextCtrl.GetValue()
            if select == 'select':
                newfl = self.fl.select(parameter=parameter, values=newvalue)
            elif select == 'replace':
                newfl = self.fl.replace(parameter=parameter, values=value, newvalue=newvalue)
            else:
                newfl = self.fl.drop(parameter=parameter, values=newvalue)
            self.newfl = newfl
            self.stats = self.newfl.stats(intensive=True, output='string')
            self.mod = True
            self.statsTextCtrl.SetValue(self.stats)
        dlg.Destroy()


class FlagModDialog(wx.Dialog):
    """
    DESCRIPTION
        Modify flagging data
        three pulldowns: 1) (select, drop, replace)  2) key (operator etc) 3) value
        value is either a pulldown (label, labelid) or textctrl
        'sensorid', 'components', 'flagtype', 'labelid', 'label', 'comment', 'groups', 'probabilities', 'stationid',
        'validity', 'operator'
    """
    def __init__(self, parent, title):
        super(FlagModDialog, self).__init__(parent=parent, title=title, size=(400, 600))
        self.select = ['select','replace','drop']
        self.parameter = ['sensorid', 'components', 'flagtype', 'labelid', 'label', 'comment', 'groups',
                          'probabilities', 'stationid', 'validity', 'operator']
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.selectLabel = wx.StaticText(self,label="modification type")
        self.parameterLabel = wx.StaticText(self,label="flagging key")
        self.valueLabel = wx.StaticText(self,label="old value")
        self.newvalueLabel = wx.StaticText(self,label="desired value")
        self.selectComboBox = wx.ComboBox(self, choices=self.select,
                 style=wx.CB_DROPDOWN, value=self.select[0],size=(160,-1))
        self.parameterComboBox = wx.ComboBox(self, choices=self.parameter,
                 style=wx.CB_DROPDOWN, value=self.parameter[0],size=(160,-1))
        self.valueTextCtrl = wx.TextCtrl(self,size=(160,-1))
        self.newvalueTextCtrl = wx.TextCtrl(self,size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='OK')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        self.valueTextCtrl.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.selectLabel, noOptions))
        contlst.append((self.parameterLabel, noOptions))
        contlst.append((self.valueLabel, noOptions))
        contlst.append((self.newvalueLabel, noOptions))
        contlst.append((self.selectComboBox, noOptions))
        contlst.append((self.parameterComboBox, noOptions))
        contlst.append((self.valueTextCtrl, expandOption))
        contlst.append((self.newvalueTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.selectComboBox.Bind(wx.EVT_COMBOBOX, self.onUpdateSelect)

    def onUpdateSelect(self, event):
        """
        DESCRIPTION
            update fields accoring to select
        """
        select = self.selectComboBox.GetStringSelection()
        if select == 'replace':
            self.valueTextCtrl.Enable()



# ##################################################################################################################
# ####    Analysis Panel                                   #########################################################
# ##################################################################################################################


class AnalysisFitDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for analysis panels fit button
    """

    def __init__(self, parent, title, datacont, plotcont, analysisdict, hide_file, last_dir : string = ''):
        super(AnalysisFitDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))

        self.plotcont = plotcont
        self.datacont = datacont
        self.analysisdict = analysisdict
        self.shownkeys = plotcont.get('shownkeys')
        self.selectedkey = self.shownkeys[0]
        self.keys2flag = ",".join(self.shownkeys)
        self.keys = datacont.get('keys')
        self.last_dir = last_dir
        self.mintime = datacont.get('start')
        self.maxtime = datacont.get('end')
        self.fitfunc = self.analysisdict.get('fitfunction','spline')
        self.funclist = ['spline','polynomial', 'linear least-squares', 'mean', 'none']
        self.fitknots = self.analysisdict.get('fitknotstep','0.3')
        self.fitdegree = self.analysisdict.get('fitdegree','5')

        self.fitparameter = {}
        self.hide_file = hide_file
        self.createControls()
        self.doLayout()
        self.modifyWindows(self.fitfunc)

    # Widgets
    def createControls(self):
        try:
            stfit = wx.DateTime.FromDMY(self.mintime.day,self.mintime.month-1,self.mintime.year)
            etfit = wx.DateTime.FromDMY(self.maxtime.day,self.maxtime.month-1,self.maxtime.year)
        except:
            stfit = wx.DateTimeFromDMY(self.mintime.day,self.mintime.month-1,self.mintime.year)
            etfit = wx.DateTimeFromDMY(self.maxtime.day,self.maxtime.month-1,self.maxtime.year)
        self.funcLabel = wx.StaticText(self, label="Fit function:",size=(200,30))
        self.funcComboBox = wx.ComboBox(self, choices=self.funclist,
            style=wx.CB_DROPDOWN, value=self.fitfunc,size=(200,-1))
        self.knotsLabel = wx.StaticText(self, label="Knots (0 - 1) (spline only):")
        self.knotsTextCtrl = wx.TextCtrl(self, value=self.fitknots,size=(200,30))
        self.degreeLabel = wx.StaticText(self, label="Degree (1 - ..) (polynom only):")
        self.degreeTextCtrl = wx.TextCtrl(self, value=self.fitdegree,size=(200,30))

        self.UpperTimeText = wx.StaticText(self,label="Fit data before:")
        self.LowerTimeText = wx.StaticText(self,label="Fit data after:")
        self.startFitDatePicker = wxDatePickerCtrl(self, dt=stfit,size=(200,30))
        self.startFitTimePicker = wx.TextCtrl(self, value=self.mintime.strftime('%X'),size=(200,30))
        self.endFitDatePicker = wxDatePickerCtrl(self, dt=etfit,size=(200,30))
        self.endFitTimePicker = wx.TextCtrl(self, value=self.maxtime.strftime('%X'),size=(200,30))
        self.extrapolateCheckBox = wx.CheckBox(self, label="extrapolate", size=(200,30))
        self.loadButton = wx.Button(self, label='Load fit',size=(200,30))
        self.saveButton = wx.Button(self, label='Save fit(s)',size=(200,30))

        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(200,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(200,30))

        for elem in self.shownkeys:
            exec('self.{}CheckBox = wx.CheckBox(self, label="{}", size=(160,30))'.format(elem, elem))
            exec('self.{}CheckBox.SetValue(True)'.format(elem))

        self.funcComboBox.Bind(wx.EVT_COMBOBOX, self.onUpdate)
        self.loadButton.Bind(wx.EVT_BUTTON, self.on_load)
        self.saveButton.Bind(wx.EVT_BUTTON, self.on_save)


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.funcLabel, noOptions)]
        contlst.append((self.funcComboBox, expandOption))
        contlst.append((self.knotsLabel, noOptions))
        contlst.append((self.knotsTextCtrl, expandOption))
        contlst.append((self.degreeLabel, noOptions))
        contlst.append((self.degreeTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.LowerTimeText, noOptions))
        contlst.append((self.UpperTimeText, noOptions))
        contlst.append((self.startFitDatePicker, expandOption))
        contlst.append((self.endFitDatePicker, expandOption))
        contlst.append((self.startFitTimePicker, expandOption))
        contlst.append((self.endFitTimePicker, expandOption))
        contlst.append((self.extrapolateCheckBox, expandOption))
        contlst.append(emptySpace)
        maxrange = max([len(self.shownkeys), 4])
        for i in range(0,maxrange):
            if i < len(self.shownkeys):
                contlst.append((eval('self.{}CheckBox'.format(self.shownkeys[i])), expandOption))
            else:
                contlst.append(emptySpace)
            if i == 0:
                contlst.append((self.loadButton, dict(flag=wx.ALIGN_CENTER)))
            elif i == 1:
                contlst.append((self.saveButton, dict(flag=wx.ALIGN_CENTER)))
            elif i == 2:
                contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
            elif i == 3:
                contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
            else:
                contlst.append(emptySpace)

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


    def on_load(self,e):
        openFileDialog = wx.FileDialog(self, "Open", self.last_dir, "",
                                       "json fit parameter (*.json)|*.json|all files (*.*)|*.*",
                                       wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        openFileDialog.ShowModal()
        fitname = openFileDialog.GetPath()
        self.fitparameter = methods.func_from_file(fitname,debug=False)
        openFileDialog.Destroy()
        self.Close(True)
        self.Destroy()


    def on_save(self,e):
        saveFileDialog = wx.FileDialog(self, "Save As", self.last_dir, "",
                                       "json fit parameter (*.json)|*.json",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        saveFileDialog.ShowModal()
        extensions = ['.json']
        extind = saveFileDialog.GetFilterIndex()

        savename = saveFileDialog.GetPath()
        if not savename.endswith(extensions[extind]):
            savename = savename+extensions[extind]

        saveFileDialog.Destroy()
        if len(self.plotcont.get('functions',[])) > 0:
            # plotcont functions contain all functions asociated to each plotted key
            # for saveing we only store one layer
            savefunc = self.plotcont.get('functions')[0] # only for first key
            methods.func_to_file(savefunc,savename,debug=False)
        self.Close(True)
        self.Destroy()


    def getFitParameters(self):
        params = {}
        params['starttime'], params['endtime'] = self.getTimeRange()
        fitfunc = self.funcComboBox.GetValue()
        knots = self.knotsTextCtrl.GetValue()
        degree = self.degreeTextCtrl.GetValue()
        params['fitfuncname'] = fitfunc
        if fitfunc.startswith('poly'):
            fitfunc = 'poly'
        elif fitfunc.startswith('linear'):
            fitfunc = 'least-squares'
        params['fitfunc'] = fitfunc
        if not 0<float(knots)<1:
            knots = 0.5
        else:
            knots = float(knots)
        params['knotstep'] = knots
        if not int(degree)>0:
            degree = 1
        else:
            degree = int(degree)
        params['fitdegree'] = degree
        return params


    def getTimeRange(self):
        # Get start time
        stday = self.startFitDatePicker.GetValue()
        sttime = str(self.startFitTimePicker.GetValue())
        if sttime.endswith('AM') or sttime.endswith('am'):
            sttime_tmp = datetime.strptime(sttime,"%I:%M:%S %p")
            sttime = sttime_tmp.strftime("%H:%M:%S")
        if sttime.endswith('pm') or sttime.endswith('PM'):
            sttime_tmp = datetime.strptime(sttime,"%I:%M:%S %p")
            sttime = sttime_tmp.strftime("%H:%M:%S")
        sd_tmp = datetime.fromtimestamp(stday.GetTicks())
        sd = sd_tmp.strftime("%Y-%m-%d")
        starttime = datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
        # Get end time
        enday = self.endFitDatePicker.GetValue()
        entime = str(self.endFitTimePicker.GetValue())
        if entime.endswith('AM') or entime.endswith('am'):
            entime_tmp = datetime.strptime(entime, "%I:%M:%S %p")
            entime = entime_tmp.strftime("%H:%M:%S")
        if entime.endswith('pm') or entime.endswith('PM'):
            entime_tmp = datetime.strptime(entime, "%I:%M:%S %p")
            entime = entime_tmp.strftime("%H:%M:%S")
        ed_tmp = datetime.fromtimestamp(enday.GetTicks())
        ed = ed_tmp.strftime("%Y-%m-%d")
        endtime = datetime.strptime(str(ed)+'_'+entime, "%Y-%m-%d_%H:%M:%S")
        return starttime, endtime


    def setTimeRange(self, startdate, enddate):
        from magpy.gui.magpy_gui import pydate2wxdate
        starttime = num2date(startdate).strftime('%X')
        endtime = num2date(enddate).strftime('%X')
        self.startFitDatePicker.SetValue(pydate2wxdate(num2date(startdate)))
        self.endFitDatePicker.SetValue(pydate2wxdate(num2date(enddate)))
        self.startFitTimePicker.SetValue(starttime)


    def modifyWindows(self, select):
        if select == 'spline':
            self.knotsTextCtrl.Enable()
            self.degreeTextCtrl.Disable()
        elif select == 'mean':
            self.knotsTextCtrl.Disable()
            self.degreeTextCtrl.Disable()
        elif select.startswith('poly'):
            self.knotsTextCtrl.Disable()
            self.degreeTextCtrl.Enable()
        elif select.startswith('linear'):
            self.knotsTextCtrl.Disable()
            self.degreeTextCtrl.Disable()
        else:
            self.knotsTextCtrl.Enable()
            self.degreeTextCtrl.Enable()

    def onUpdate(self, event):
        select = self.funcComboBox.GetStringSelection()
        self.modifyWindows(select)


class AnalysisFilterDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, samplingrate, resample, winlen, resint, resoff, filtertype):
        super(AnalysisFilterDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.samplingrate = samplingrate
        self.windowlist = ['flat','barthann','bartlett','blackman','blackmanharris','bohman',
                                'boxcar','cosine','flattop','hamming','hann','nuttall',
                                'parzen','triang','gaussian','wiener','spline','butterworth']
        self.resample = resample
        self.winlen = str(winlen)
        self.resint = str(resint)
        self.filtertype = filtertype
        self.missing = ['IAGA','interpolate','conservative']
        self.off = str(resoff)
        if not resample:
            self.buttonlabel = 'Smooth'
        else:
            self.buttonlabel = 'Filter'
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.filtertypeLabel = wx.StaticText(self, label="Select window:")
        self.filtertypeComboBox = wx.ComboBox(self, choices=self.windowlist,
            style=wx.CB_DROPDOWN, value=self.filtertype,size=(160,-1))
        self.lengthLabel = wx.StaticText(self, label="Window length (sec):")
        self.lengthTextCtrl = wx.TextCtrl(self, value=self.winlen)
        if self.resample:
            self.resampleLabel = wx.StaticText(self, label="Resample interval (sec):")
            self.resampleTextCtrl = wx.TextCtrl(self, value=self.resint)
            self.resampleoffsetLabel = wx.StaticText(self, label="Resample offset (sec):")
            self.resampleoffsetTextCtrl = wx.TextCtrl(self, value=self.off)
        self.methodRadioBox = wx.RadioBox(self,
            label="Missing data:",
            choices=self.missing, majorDimension=1, style=wx.RA_SPECIFY_COLS)
        self.okButton = wx.Button(self, wx.ID_OK, label=self.buttonlabel)
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.filtertypeLabel, noOptions)]
        contlst.append((self.filtertypeComboBox, expandOption))
        contlst.append((self.lengthLabel, noOptions))
        contlst.append((self.lengthTextCtrl, expandOption))
        if self.resample:
            contlst.append((self.resampleLabel, noOptions))
            contlst.append((self.resampleTextCtrl, expandOption))
            contlst.append((self.resampleoffsetLabel, noOptions))
            contlst.append((self.resampleoffsetTextCtrl, expandOption))
        contlst.append((self.methodRadioBox, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class AnalysisOffsetDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title, keylst, xlimits, deltas):
        super(AnalysisOffsetDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.keylst = keylst
        self.choices = ['all','timerange']
        self.start = self._pydate2wxdate(xlimits[0])
        self.end = self._pydate2wxdate(xlimits[1])
        self.starttime = xlimits[0].strftime("%H:%M:%S")
        self.endtime = xlimits[1].strftime("%H:%M:%S")
        self.val = {}
        self.val['time'] = '0'
        mtime = date2num(xlimits[1])
        # Parsetime function
        def parse_time(timestring):
            #timestring can either be isotime or a string containing numerical datetime in old or new format
            #returns a numerical time value within the current systems date2num matplotlib version
            timefloat = 0
            try:
                # first check whether timestring can converted to numerical value
                timefloat = float(timestring)
                # Assume a valid date after 1830:
                mavers = matplotlib.__version__
                MATPLOTLIB_VERSION = [int(a) for a in mavers.split('.')]
                if MATPLOTLIB_VERSION[0] >= 3 and MATPLOTLIB_VERSION[1] >= 3 and timefloat > 660770:
                    timefloat = timefloat-719163.0
                elif MATPLOTLIB_VERSION[0] == 3 and MATPLOTLIB_VERSION[1] < 3 and  timefloat < 20000:
                    timefloat = timefloat+719163.0
                elif MATPLOTLIB_VERSION[0] < 3 and timefloat < 20000:
                    timefloat = timefloat+719163.0
                #730120, and a 64-bit floating point number has a resolution of 2^{-52}, or approximately 14 microseconds, so microsecond precision was lost. With the new default epoch "2020-01-01" is 10957.0,
            except:
                try:
                     dt = dateutil.parser.parse(timestring)
                     timefloat = date2num(dt).replace(tzinfo=None)
                     #parse isotime
                except:
                    print ("ERROR: given endtime in DataDeltaValus could not be interpreted correctly")
                    pass
            return timefloat

        if not deltas == '':
            try:
                dlist = deltas.split(',')
                for delt in dlist:
                    de = delt.split('_')
                    if not de[0] == 'time':
                        self.val[de[0]] = str(de[1])
                    else:
                        self.val[de[0]] = str(de[1].strip(')').split('=')[-1])
                    #if de[0].strip() == 'et' and float(self.val[de[0]].split(';')[0]) >= mtime:
                    if de[0].strip() == 'et' and parse_time(self.val[de[0]].split(';')[0]) >= mtime:
                        break
                    #print ("BB", self.val[de[0]], de[0])
            except:
                pass
        self.createControls()
        self.doLayout()
        self.bindControls()


    def _pydate2wxdate(self,date):
        assert isinstance(date, (datetime, datetime.date))
        tt = date.timetuple()
        dmy = (tt[2], tt[1]-1, tt[0])
        try:
            return wx.DateTime.FromDMY(*dmy)
        except:
            return wx.DateTimeFromDMY(*dmy)

    # Widgets
    def createControls(self):
        # Add a radio button on top:
        # Offset certain timerange / all data
        self.offsetRadioBox = wx.RadioBox(self, label="Apply offset to:",
                     choices=self.choices, majorDimension=2, style=wx.RA_SPECIFY_COLS)

        self.timeshiftLabel = wx.StaticText(self, label="Timeshift (sec):",size=(160,30))
        self.timeshiftTextCtrl = wx.TextCtrl(self, value=self.val.get('time','0'),size=(160,30))

        self.StartDateLabel = wx.StaticText(self, label="Starting:",size=(160,30))
        self.StartDatePicker = wxDatePickerCtrl(self, dt=self.start,size=(160,30))
        self.StartTimeTextCtrl = wx.TextCtrl(self, value=self.starttime,size=(160,30))
        self.EndDateLabel = wx.StaticText(self, label="Ending:",size=(160,30))
        self.EndDatePicker = wxDatePickerCtrl(self, dt=self.end,size=(160,30))
        self.EndTimeTextCtrl = wx.TextCtrl(self, value=self.endtime,size=(160,30))

        for elem in self.keylst:
            exec('self.'+elem+'Label = wx.StaticText(self,label="'+elem+'")')
            exec('self.'+elem+'TextCtrl = wx.TextCtrl(self,value="'+self.val.get(elem,'')+'")')
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

        self.StartDatePicker.Disable()
        self.StartTimeTextCtrl.Disable()
        self.EndDatePicker.Disable()
        self.EndTimeTextCtrl.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.offsetRadioBox, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.StartDateLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.StartDatePicker, expandOption))
        contlst.append((self.StartTimeTextCtrl, expandOption))
        contlst.append((self.EndDateLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.EndDatePicker, expandOption))
        contlst.append((self.EndTimeTextCtrl, expandOption))
        contlst.append((self.timeshiftLabel, noOptions))
        contlst.append((self.timeshiftTextCtrl, expandOption))
        for elem in self.keylst:
            contlst.append(eval('(self.'+elem+'Label, noOptions)'))
            contlst.append(eval('(self.'+elem+'TextCtrl, expandOption)'))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        #print "Hello:", contlst
        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.Bind(wx.EVT_RADIOBOX, self.OnChangeRange, self.offsetRadioBox)

    def OnChangeRange(self, e):
        val = self.offsetRadioBox.GetStringSelection()
        if str(val) == 'all':
            self.StartDatePicker.Disable()
            self.StartTimeTextCtrl.Disable()
            self.EndDatePicker.Disable()
            self.EndTimeTextCtrl.Disable()
            self.timeshiftTextCtrl.Enable()
        elif str(val) == 'timerange':
            self.StartDatePicker.Enable()
            self.StartTimeTextCtrl.Enable()
            self.EndDatePicker.Enable()
            self.EndTimeTextCtrl.Enable()
            self.timeshiftTextCtrl.Disable()


class AnalysisResampleDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """
    def __init__(self, parent, title,keylst, period):
        super(AnalysisResampleDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.period = period
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.periodLabel = wx.StaticText(self,label="Period")
        self.periodTextCtrl = wx.TextCtrl(self,value=str(self.period))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.periodLabel, noOptions))
        contlst.append((self.periodTextCtrl, expandOption))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)



class AnalysisRotationDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for providing rotation values
        Will take data from header as orgalpha, orgbeta and orggamma
    """
    def __init__(self, parent, title, orgalpha, orgbeta, orggamma):
        super(AnalysisRotationDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        if orgalpha:
            self.orgalpha = str(orgalpha)
        else:
            self.orgalpha = ''
        if orgbeta:
            self.orgbeta = str(orgbeta)
        else:
            self.orgbeta = ''
        if orggamma:
            self.orggamma = str(orggamma)
        else:
            self.orggamma = ''
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.alphaLabel = wx.StaticText(self,label="z-axix rotation: alpha in degree")
        self.alphaTextCtrl = wx.TextCtrl(self,value=self.orgalpha)
        self.betaLabel = wx.StaticText(self,label="y-axix rotation: beta in degree")
        self.betaTextCtrl = wx.TextCtrl(self,value=self.orgbeta)
        self.gammaLabel = wx.StaticText(self,label="x-axix rotation: gamma in degree")
        self.gammaTextCtrl = wx.TextCtrl(self,value=self.orggamma)
        self.invertLabel = wx.StaticText(self,label="Invert Euler rotation:")
        self.invertCheckBox = wx.CheckBox(self, label='', size=(160, -1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply')
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # (self.'+elem+'Label, noOptions),
        contlst = []
        contlst.append((self.alphaLabel, noOptions))
        contlst.append((self.alphaTextCtrl, expandOption))
        contlst.append((self.betaLabel, noOptions))
        contlst.append((self.betaTextCtrl, expandOption))
        contlst.append((self.gammaLabel, noOptions))
        contlst.append((self.gammaTextCtrl, expandOption))
        contlst.append((self.invertLabel, noOptions))
        contlst.append((self.invertCheckBox, expandOption))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class AnalysisBaselineDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog will be opened when choosing the Baseline button on the Analysis panel.
    """

    def __init__(self, parent, title, baseid, baselinedict, stream, shownkeylist, keylist, path: string = ''):
        super(AnalysisBaselineDialog, self).__init__(parent=parent,
            title=title, size=(600, 900))
        self.absstreamlist = []
        self.last_dir = path
        self.baselinedict = baselinedict
        self.plotstream = stream
        self.shownkeylist = shownkeylist
        self.keylist = keylist
        self.active_baseid = baseid
        self.starttime = baselinedict.get(baseid).get("startdate")
        self.endtime = baselinedict.get(baseid).get("enddate")

        baseids = [bid for bid in baselinedict]

        # Create selection and information lines for all baseline inputs
        for bid in baseids:
            basecont = baselinedict.get(bid)
            # coverage of all basevalue data
            st = basecont.get('startdate')
            et = basecont.get('enddate')
            line = "{}: {}_{}_{}".format(str(bid), basecont.get('filename'), st.strftime("%Y%m%d"), et.strftime("%Y%m%d"))
            self.absstreamlist.append(line)

        # Select the fitting parameter lists for currently active baseline
        self.fitparameters = self.get_fitpara(self.active_baseid, starttime=self.starttime,endtime=self.endtime)

        # Create an information string for the currently active baseid
        self.parameterstring = self.create_fitparameterstring(self.active_baseid)

        self.createControls()
        self.doLayout()
        self.bindControls()


    def get_fitpara(self, baseid, starttime=None,endtime=None):
        fitparameters = {}

        bd = self.baselinedict.get(baseid)
        functlist = bd.get('function',[])

        if functlist and len(functlist) > 0:
            for idx,func in enumerate(functlist):
                funcdict = {"keys":func[8], "fitfunc":func[3],"fitdegree":func[4], "knotstep":func[5], "starttime":func[6],"endtime":func[7], "sv":func[1], "ev":func[2]}
                fitparameters[idx] = funcdict
        return fitparameters


    def create_fitparameterstring(self, baseid):
        activeparameters = self.baselinedict.get(baseid)
        st = activeparameters.get('startdate')
        et = activeparameters.get('enddate')
        ps = "Adopted Baseline (ID: {}):\n\n Starttime: {}\n".format(baseid, st.strftime("%Y-%m-%d"))
        functlist = activeparameters.get('function')
        if functlist and len(functlist) > 0:
            for idx,func in enumerate(functlist):
                place = ''
                fitfunc = func[3]
                if fitfunc == 'spline':
                    place = " Knotstep: {},".format(func[5])
                elif fitfunc == 'poly':
                    place = " Degree: {},".format(func[4])
                line = " - Fitfunc: {} ,{} between {} and {}\n".format(fitfunc, place, func[6], func[7])
                ps += line
        ps += " Endtime: {}\n".format(et.strftime("%Y-%m-%d"))
        return ps

    def _pydate2wxdate(self,date):
        assert isinstance(date, (datetime, datetime.date))
        tt = date.timetuple()
        dmy = (tt[2], tt[1]-1, tt[0])
        try:
            return wx.DateTime.FromDMY(*dmy)
        except:
            return wx.DateTimeFromDMY(*dmy)

    # Widgets
    def createControls(self):
        self.absstreamLabel = wx.StaticText(self, label="Select basevalue data:",size=(190,30))
        self.absstreamComboBox = wx.ComboBox(self, choices=self.absstreamlist,
            style=wx.CB_DROPDOWN, value=self.absstreamlist[-1],size=(550,-1))
        self.parameterLabel = wx.StaticText(self, label="Fit parameter:",size=(190,30))
        self.parameterTextCtrl = wx.TextCtrl(self, value=self.parameterstring,size=(550,120),
                          style = wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.clearButton = wx.Button(self, label='Delete all',size=(190,30))
        self.loadButton = wx.Button(self, label='Load ...',size=(190,30))
        self.saveButton = wx.Button(self, label='Save ...',size=(190,30))

        self.okButton = wx.Button(self, wx.ID_OK, label='Adopt baseline',size=(190,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(190,30))

        plat_form = platform.platform()
        if plat_form.startswith('linux') or plat_form.startswith('Linux'):
            self.parameterTextCtrl.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.absstreamLabel, noOptions)]
        contlst.append((self.absstreamComboBox, expandOption))
        contlst.append((self.parameterLabel, noOptions))
        contlst.append((self.parameterTextCtrl, expandOption))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.loadButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.saveButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.clearButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


    def bindControls(self):
        self.clearButton.Bind(wx.EVT_BUTTON, self.OnClear)
        self.loadButton.Bind(wx.EVT_BUTTON, self.OnLoad)
        self.saveButton.Bind(wx.EVT_BUTTON, self.OnSave)
        self.absstreamComboBox.Bind(wx.EVT_TEXT, self.OnUpdate)


    def OnLoad(self, e):
        # Load will load from file and replace current fit parameters
        # extradays = 1
        # if endtime == "now":
        #     endtime = datetime.utcnow()+timedelta(days=extradays)
        openFileDialog = wx.FileDialog(self, "Open", self.last_dir, "",
                                       "json fit parameter (*.json)|*.json|all files (*.*)|*.*",
                                       wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        if openFileDialog.ShowModal() == wx.ID_OK:
            fitname = openFileDialog.GetPath()
            basedict = func_from_file(fitname,debug=False)
            openFileDialog.Destroy()
            for bid in basedict:
                basecont = basedict.get(bid)
                self.baselinedict[bid] = basecont
                # coverage of all basevalue data
                st = basecont.get('startdate')
                et = basecont.get('enddate')
                line = "{}: {}_{}_{}".format(str(bid), basecont.get('filename'), st.strftime("%Y%m%d"),
                                             et.strftime("%Y%m%d"))
                self.absstreamlist.append(line)

            self.parameterTextCtrl.Clear()
            #print ("LOADED", basedict)
            keys = [key for key in basedict]
            if keys and len(keys) > 0:
                mainkey = keys[0]
                choice = [el for el in self.absstreamlist if el.startswith(str(mainkey))][0]
                self.active_baseid = mainkey
                self.absstreamComboBox.SetItems(self.absstreamlist)
                self.absstreamComboBox.SetValue(choice)
                self.parameterstring = self.create_fitparameterstring(self.active_baseid)
                self.parameterTextCtrl.SetValue(self.parameterstring)
                self.clearButton.Enable()
                self.saveButton.Enable()
        else:
            openFileDialog.Destroy()
        self.OnUpdate(e)


    def OnSave(self, e):

        savedict = {}
        bd = self.baselinedict.get(self.active_baseid)
        savedict[self.active_baseid] = bd

        saveFileDialog = wx.FileDialog(self, "Save As", self.last_dir, "",
                                       "json fit parameter (*.json)|*.json",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        if saveFileDialog.ShowModal() == wx.ID_OK:
            extensions = ['.json']
            extind = saveFileDialog.GetFilterIndex()

            savename = saveFileDialog.GetPath()
            if not savename.endswith(extensions[extind]):
                savename = savename+extensions[extind]

            saveFileDialog.Destroy()
            func_to_file(savedict,savename,debug=False)
            self.Close(True)
        else:
             saveFileDialog.Destroy()


    def OnClear(self, e):
        # delete current fitparameter
        self.baselinedict = {}
        self.parameterTextCtrl.Clear()
        self.parameterTextCtrl.SetValue("")
        self.clearButton.Disable()
        self.saveButton.Disable()
        self.absstreamComboBox.Clear()
        self.okButton.Disable()


    def OnUpdate(self, e):
        """
        DESCRIPTION
            will be called by an update event in the selection ComboBox
        :param e:
        :return:
        """
        # open fit dlg
        self.parameterstring = ""
        self.active_baseid = self.absstreamComboBox.GetValue().split(':')[0]
        activeparameters = self.baselinedict.get(self.active_baseid)
        if activeparameters:
            self.starttime = activeparameters.get('startdate')
            self.endtime = activeparameters.get('enddate')
            # get the selected data from plotoptlist - and then update the data here
            self.fitparameters = self.get_fitpara(self.active_baseid, starttime=self.starttime,endtime=self.endtime)
            self.parameterstring = self.create_fitparameterstring(self.active_baseid)
            self.okButton.Enable()
        ## also add funtional parameters to the dictionary
        self.parameterTextCtrl.Clear()
        self.parameterTextCtrl.SetValue(self.parameterstring)


# ##################################################################################################################
# ####    DI Panel                                         #########################################################
# ##################################################################################################################

class SetStationIDDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog fto define an eventually missing stationid
    USED BY:
        Stream Method: LoadDIDialog()
    """
    def __init__(self, parent, title, stationid):
        super(SetStationIDDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        self.stationid=stationid
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.StationText = wx.StaticText(self,label="StationID",size=(160,-1))
        self.StationTextCtrl = wx.TextCtrl(self, value=self.stationid,size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(160,-1))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,-1))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.StationText, noOptions))
        contlst.append((self.StationTextCtrl, expandOption))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class SetAzimuthDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog fto define an eventually missing azimuth
    USED BY:
        Stream Method: DIAnalyse()
    """
    def __init__(self, parent, title, azimuth):
        super(SetAzimuthDialog, self).__init__(parent=parent,
            title=title, size=(600, 600))
        self.azimuth=str(azimuth)
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        # countvariables for specific header blocks
        self.AzimuthText = wx.StaticText(self,label="Azimuth",size=(160,-1))
        self.AzimuthTextCtrl = wx.TextCtrl(self, value=self.azimuth,size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Apply',size=(160,-1))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,-1))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        # transform headerlist to an array with lines like cnts
        contlst = []
        contlst.append((self.AzimuthText, noOptions))
        contlst.append((self.AzimuthTextCtrl, expandOption))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


class LoadDIDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog for loading di data

        Essential parameters for DI analysis:
        self.dipathlist                :  contains the obtained dictionary from the Load process with a diline structure
        self.divariopath               :  the sourcepath
        self.discalarpath              :  the sourcepath
        self.dirname                   :  initial path for vario, scalar and di data
        self.options['didictionary']   :  basically all options and defauts for variometer and scalar
        self.options['diparameter']    :  parameter for analysis
    """

    def __init__(self, parent, title, dicont, db, services, defaultservice):
        super(LoadDIDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.pathlist = []
        self.dicont = dicont   # content of the station specific DI dictionary - will be updated here
        self.dirname = dicont.get('didatapath','')
        self.db = db
        self.absolutes = []
        self.services =  self.get_basevalue_service(services)
        self.serviceitems = list(self.services.keys())
        if not defaultservice in self.serviceitems:
            self.mainsource = self.serviceitems[0]
        else:
            self.mainsource = defaultservice
        self.createControls()
        self.doLayout()
        self.bindControls()

    def get_basevalue_service(self,services):
        """
        DESCRIPTION
          Obtain only services providing basevalues
          Return a service dictionary and a service list for preselection in ComboBox
        """
        basevaluedict = {}
        for service in services:
            cont = services[service]
            for el in cont:
                if el == 'basevalues':
                    basecont = {}
                    if not cont[el].get('type'):
                        cont[el]['type'] = ['basevalue']
                    if not cont[el].get('sampling'):
                        cont[el]['sampling'] = ['']
                    cont[el]['elements'] = ''
                    basecont[el] = cont[el]
                    basevaluedict[service] = basecont
        return basevaluedict

    # Widgets
    def createControls(self):
        self.loadFileButton = wx.Button(self,-1,"Select File(s)",size=(210,30))
        self.loadDBButton = wx.Button(self,-1,"Select Database",size=(210,30))
        self.loadRemoteButton = wx.Button(self,-1,"Select Webservice/Remote",size=(210,30))
        self.remoteComboBox = wx.ComboBox(self, choices=self.serviceitems,
                 style=wx.CB_DROPDOWN, value=self.mainsource,size=(160,-1))
        self.fileTextCtrl = wx.TextCtrl(self,value=self.dirname,size=(160,-1)) # manual, autodif -> if autudif request azimuth
        self.databaseTextCtrl = wx.TextCtrl(self,value="",size=(160,-1)) # currently conected to

        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel')
        if not self.db:
            self.loadDBButton.Disable()


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[]
        contlst.append((self.loadFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.fileTextCtrl, expandOption))
        contlst.append((self.loadDBButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.databaseTextCtrl, expandOption))
        contlst.append((self.loadRemoteButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.remoteComboBox, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)


    def bindControls(self):
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnLoadDIFiles)
        self.loadDBButton.Bind(wx.EVT_BUTTON, self.OnLoadDIDB)
        self.loadRemoteButton.Bind(wx.EVT_BUTTON, self.OnLoadDIRemote)


    def LoadFiles(self, pathlist, stationid=None, azimuth=None, pier=None, source='file'):
        """
        DESCRIPTION
            internal method to load all files from a list
            and create a absdata list
        RETURNS
            a didict dictionary will be assigned to self.pathlist
                    didict['mindatetime'] = datetime.strptime(min(datelist),"%Y-%m-%d")
                    didict['maxdatetime'] = datetime.strptime(max(datelist),"%Y-%m-%d")
                    didict['selectedpier'] = pierlist[0]
                    didict['azimuth'] = azimuthlist[0]
                    didict['station'] = stationlist[0]
                    didict['source'] = source
                    didict['absdata'] = abslist

        """
        didict = {}
        abslist = []
        datelist, pierlist,stationlist = [], [], []
        azimuthlist = []
        for elem in pathlist:
            absst = di.abs_read(elem, output='DIListStruct')
            try:

                for a in absst:
                    stream =a.get_abs_distruct()
                    abslist.append(a)
                    tempdate = num2date(stream[0].time).replace(tzinfo=None)
                    datelist.append(tempdate.strftime("%Y-%m-%d"))
                    pierlist.append(a.pier)
                    azimuthlist.append(a.azimuth)
                    stationlist.append(a.stationid)
            except:
                print("absoluteAnalysis: Failed to analyse %s - problem of filestructure" % elem)
                pass

        pierlist = list(set(pierlist))
        azimuthlist = list(set(azimuthlist))
        if len(pierlist) > 1:
            print ("Multiple piers selected - TODO")
            # TODO do something here TODO stationid not extracted from filename in old AUTODIF files
        if len(abslist) == 0:
            raise Exception("DI File has no valid measurements")
        stationid = stationlist[0]
        didict['mindatetime'] = datetime.strptime(min(datelist),"%Y-%m-%d")
        didict['maxdatetime'] = datetime.strptime(max(datelist),"%Y-%m-%d")+timedelta(days=1)
        didict['selectedpier'] = pierlist[0]
        didict['azimuth'] = azimuthlist[0]
        didict['station'] = stationid
        didict['source'] = source
        didict['absdata'] = abslist

        #check stationlist[0]
        if not stationid is None and not len(stationid) == 3:
            stationid = None

        # stationid needs to be defined !!!!
        if stationid is None:
            # Open a dialog to set the stationid
            stationid = 'invalid'
            dlg = SetStationIDDialog(self, title='Define a StationID (e.g. IAGA code)', stationid=stationid)
            if dlg.ShowModal() == wx.ID_OK:
                stationid = dlg.StationTextCtrl.GetValue()
            dlg.Destroy()
            didict['station'] =  stationid

        return didict


    def OnLoadDIFiles(self,e):
        """
        CALLS
            LoadFiles
        """
        try:
            self.difiledirname = ''
            stream = DataStream()
            dlg = wx.FileDialog(self, "Choose file(s)", self.dirname, "", "*.*", wxMULTIPLE)
            if dlg.ShowModal() == wx.ID_OK:
                try:
                    self.dirname = os.path.split(dlg.GetPaths()[0])[0]
                except Exception as exc:
                    logger.error(exc)

                self.pathlist = self.LoadFiles(dlg.GetPaths())
            #dlg.Destroy()
        except Exception as exc:
            logger.error(exc)
        finally:
            dlg.Destroy()
            self.Close(True)


    def OnLoadDIDB(self,e):
        """
        DESCRIPTION
            internal method to load DI files from a database
            and create a absdata list
        RETURNS
            self.pathlist will be set to a current content dictionary
                    content['mindatetime'] = midate
                    content['maxdatetime'] = madate
                    content['source'] = 'db'
                    self.absolutes = self.db.diline_from_db(starttime=midate,endtime=madate,sql=sql)
                    content['absdata'] = self.absolutes
                    content['azimuth'] = self.absolutes[0].azimuth
                    (missing are 'station', 'selectedpier')

        """
        # 1. check whether data is accessible
        ditables = []
        if not self.db:
            dlg = wx.MessageDialog(self, "Could not access database!\n"
                        "please check your connection\n",
                        "Get DI data from database", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            self.Close(True)
            return
        # 2. Identify all tables with DIDATA_xxx
        if self.db:
            sql = "SHOW TABLES LIKE 'DIDATA\_%'"
            cursor = self.db.db.cursor()
            message = self.db._executesql(cursor, sql)
            #cursor.execute(sql)
            tablelist = cursor.fetchall()
            ditables = [el[0] for el in tablelist]
            print ("Test", ditables)
            if len(ditables) < 1:
                dlg = wx.MessageDialog(self, "No DI tables available!\n"
                            "please check your database\n",
                            "Get DI data from database", wx.OK|wx.ICON_INFORMATION)
                dlg.ShowModal()
                dlg.Destroy()
                self.Close(True)
                return
        # 3. check contents of DIDATA_Obscode
        didatadict = {}
        if self.db and len(ditables) > 0:
            cursor = self.db.db.cursor()
            # cycle through all tables
            didatadict = {}
            for table in ditables:
                dicont = {}
                try:
                    stationid = table.split('_')[1]
                except:
                    stationid = 'None'
                sql = "SELECT DIID, Pier, Observer, StartTime FROM {}".format(table)
                message = self.db._executesql(cursor, sql)
                output = cursor.fetchall()
                piers = list(set([el[1] for el in output]))
                observers = list(set([el[2] for el in output]))
                output = [[el[0],el[1],el[2],el[3]] for el in output]
                #
                # get unique list of piers and observers
                #
                dicont['piers'] = piers
                dicont['observers'] = observers
                dicont['data'] = output
                didatadict[stationid] = dicont

        #4. if didatadict existing
        # open a new selection window with station id, pier and observer combos
        # update start and endtime plus aoumt of available data upon selection

        # option 1: create temporary files from each selected DIID and return a pointer to this temporary filelist
        # option 2: obtain diline structure directly
        # option 3: obtain link to DB and provide that to absoluteAnalysis

        dlg = DIConnectDatabaseDialog(None, title='Obtaining DI data from database', db=self.db, didict=didatadict, options={})
        if dlg.ShowModal() == wx.ID_OK:
            # Obtain selection dictionary
            #stday = dlg.startDatePicker.GetValue()

            content = dlg.stationdict
            midate = dlg.dt(content.get('mindate'),content.get('mintime'))
            madate = dlg.dt(content.get('maxdate'),content.get('maxtime'))
            # Obtain DiLineSruct of all selected DI ata
            sql = 'Pier="{}"'.format(content.get('selectedpier'))
            if not content.get('selectedobserver') == 'all':
                sql += ' AND Observer="{}"'.format(content.get('selectedpier'))

            content['mindatetime'] = midate
            content['maxdatetime'] = madate+timedelta(days=1)
            content['source'] = 'db'
            # add one hour to the timerange to make sure that data sets are all loaded even if start time lightly differs
            absolutes = self.db.diline_from_db(starttime=midate-timedelta(hours=1), endtime=madate+timedelta(hours=1), tablename=ditables[0], sql=sql)
            content['absdata'] = absolutes
            content['azimuth'] = absolutes[0].azimuth
            self.pathlist = content

        dlg.Destroy()
        self.Close(True)

    def OnLoadDIRemote(self,e):
        """
        DESCRIPTION
            internal method to load DI files from a database
            and create a absdata list
        RETURNS
            self.pathlist will be set to a didict dictionary (see LoadFiles for general dictionary contents)
                in here are set:
                    didict['mindatetime'] = datetime.strptime(min(datelist),"%Y-%m-%d")
                    didict['maxdatetime'] = datetime.strptime(max(datelist),"%Y-%m-%d")
                    didict['selectedpier'] = pierlist[0]
                    didict['source'] = source
                    didict['absdata'] = abslist
                    didict['station'] =  stationid
                    didict['azimuth'] = azimuthlist[0]

        """
        url = ''
        stationid = 'None'
        source = 'webservice'
        abslist = []
        datelist, pierlist, azimuthlist = [], [],  []
        services = self.services
        default = self.remoteComboBox.GetValue()
        dlg = ConnectWebServiceDialog(None, title='Connecting to a webservice', services=services, default=default, validgroups=['basevalues'], startdate = wx.DateTime().Today()-wx.TimeSpan(24*14))
        # Disable items
        dlg.groupComboBox.Disable()
        dlg.typeComboBox.Disable()
        dlg.sampleComboBox.Disable()
        dlg.elementsTextCtrl.Disable()

        if dlg.ShowModal() == wx.ID_OK:
            # Create URL from inputs
            stday = dlg.startDatePicker.GetValue()
            sttime = str(dlg.startTimePicker.GetValue())
            if sttime.endswith('AM') or sttime.endswith('am'):
                sttime_tmp = datetime.strptime(sttime,"%I:%M:%S %p")
                sttime = sttime_tmp.strftime("%H:%M:%S")
            if sttime.endswith('pm') or sttime.endswith('PM'):
                sttime_tmp = datetime.strptime(sttime, "%I:%M:%S %p")
                sttime = sttime_tmp.strftime("%H:%M:%S")
            sd_tmp = datetime.fromtimestamp(stday.GetTicks())
            sd = sd_tmp.strftime("%Y-%m-%d")
            start= datetime.strptime(str(sd)+'_'+sttime, "%Y-%m-%d_%H:%M:%S")
            enday = dlg.endDatePicker.GetValue()
            entime = str(dlg.endTimePicker.GetValue())
            if entime.endswith('AM') or entime.endswith('am'):
                entime_tmp = datetime.strptime(entime,"%I:%M:%S %p")
                entime = entime_tmp.strftime("%H:%M:%S")
            if entime.endswith('pm') or entime.endswith('PM'):
                entime_tmp = datetime.strptime(entime,"%I:%M:%S %p")
                entime = entime_tmp.strftime("%H:%M:%S")
            ed_tmp = datetime.fromtimestamp(enday.GetTicks())
            ed = ed_tmp.strftime("%Y-%m-%d")
            end = datetime.strptime(ed+'_'+entime, "%Y-%m-%d_%H:%M:%S")
            if start < end:
                stationid = dlg.idComboBox.GetValue()
                # Should that be changed to "id" ?
                obs_id = 'observatory=' + stationid
                start_time = '&starttime=' + sd + 'T' + sttime + 'Z'
                end_time = '&endtime=' + ed + 'T' + entime + 'Z'
                raw = '&includemeasurements=true'
                base = services.get(dlg.serviceComboBox.GetValue()).get('basevalues').get('address')
                url = (base + '?' + obs_id + start_time + end_time + raw)
                #print ("Constructed url:", url)
            else:
                msg = wx.MessageDialog(self, "Invalid time range!\n"
                    "The end time occurs before the start time.\n",
                    "Connect Webservice", wx.OK|wx.ICON_INFORMATION)
                msg.ShowModal()
                msg.Destroy()

        didict = {}
        if not url == '':
            # get data from url and read using readJSONABS
            # eventually move that to readJSONABS
            content = urlopen(url).read()
            suffix = '.json'
            date = os.path.basename(url).partition('.')[0] # append the full filename to the temporary file
            fname = date+suffix
            fname = fname.strip('?').strip(':')      ## Necessary for windows
            fh = NamedTemporaryFile(suffix=fname,delete=False)
            fh.write(content)
            fh.close()
            # create temporary file??
            absst = di.abs_read(fh.name, output='DIListStruct')
            #absst = readJSONABS(fh.name)
            # JSONABS returns a DILIST
            for a in absst:
                stream = a.get_abs_distruct()
                abslist.append(a)
                tempdate = num2date(stream[0].time).replace(tzinfo=None)
                datelist.append(tempdate.strftime("%Y-%m-%d"))
                pierlist.append(a.pier)
                azimuthlist.append(a.azimuth)

            didict['mindatetime'] = datetime.strptime(min(datelist),"%Y-%m-%d")
            didict['maxdatetime'] = datetime.strptime(max(datelist),"%Y-%m-%d")+timedelta(days=1) # add one day as rounded to full days
            didict['selectedpier'] = pierlist[0]
            didict['source'] = source
            didict['absdata'] = abslist
            didict['station'] =  stationid
            didict['azimuth'] = azimuthlist[0]

            self.pathlist = didict
        else:
            # set some info parameters on DI panel
            pass

        dlg.Destroy()
        self.Close(True)


class LoadVarioScalarDialog(wx.Dialog):
    """
    Dialog for Absolute panel
    Select data source for variometer and scalar data
    """

    def __init__(self, parent, title, vselection, sselection, defaultvariopath, defaultscalarpath, db, defaultvariotable, defaultscalartable, services, defaultservice):
        super(LoadVarioScalarDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.vchoicesselection = [False,False,False]
        self.vchoice = vselection
        self.vchoicesselection[self.vchoice] = True
        self.sourcechoices = ['file','DB','webservice']
        self.schoicesselection = [False,False,False]
        self.schoice = sselection
        self.schoicesselection[self.schoice] = True
        self.defaultvariopath = defaultvariopath.split('*.')[0]
        self.defaultscalarpath = defaultscalarpath.split('*.')[0]
        try:
            vext = os.path.split(defaultvariopath)[-1]
            sext = os.path.split(defaultscalarpath)[-1]
        except:
            vext = '*.*'
            sext = '*.*'
        self.varioext = [vext]
        self.scalarext = [sext]
        self.db = db
        if self.db:
            self.variotables = self.checkDB(search='x,y,z')
            self.scalartables = self.checkDB(search='f')
            if len(self.variotables) > 0:
                self.defaultvariotable = self.variotables[0]
            else:
                self.variotables = ['1', '2']
            if len(self.scalartables) > 0:
                self.defaultscalartable = self.scalartables[0]
            else:
                self.scalartables = ['3','4']
        else:
            self.variotables = ['1','2']
            self.scalartables = ['3','4']
        self.defaultvariotable = defaultvariotable
        self.defaultscalartable = defaultscalartable
        self.services = services
        self.serviceitems = list(self.services.keys())
        if not defaultservice in self.serviceitems:
            self.mainsource = self.serviceitems[0]
        else:
            self.mainsource = defaultservice
        self.divariows = 'url'
        self.discalarws = 'url'
        self.variopath_short = self.getShort(self.defaultvariopath)
        self.scalarpath_short = self.getShort(self.defaultscalarpath)

        # the following variables contain the resulting source information for absoluteAnalysis
        self.variosource = ''
        self.scalarsource = ''


        self.createControls()
        self.doLayout()
        self.bindControls()

    def getShort(self,path,slen=20):
        """
        Get short version of directory name
        """
        if not path:
            path = ''
        elif len(path) < slen:
            return path
        else:
            return "...{}".format(path[-slen:])

    def checkDB(self, search='f'):
        cursor = self.db.db.cursor()
        sql = "SELECT DataID, ColumnContents, ColumnUnits FROM DATAINFO"
        cursor.execute(sql)
        output = cursor.fetchall()
        datainfoidlist = []
        if search == 'f':
            search = [',f',',s']
        else:
            search = ['x,y,z','h,e,z']
        for se in search:
            datainfoidlist += [elem[0] for elem in output if se in elem[1].lower() and 'nT' in elem[2]]
        return datainfoidlist

    # Widgets
    def createControls(self):
        self.variosourceLabel = wx.StaticText(self, label="Variometer:",size=(160,30))
        self.scalarsourceLabel = wx.StaticText(self, label="Scalar instr.:",size=(160,30))
        self.scalarLabel = wx.StaticText(self, label="Source:",size=(120,30))

        self.vsource1CheckBox = wx.CheckBox(self, label='files',size=(160,30))
        self.vsource2CheckBox = wx.CheckBox(self, label='DB',size=(160,30))
        self.vsource3CheckBox = wx.CheckBox(self, label='webservice',size=(160,30))

        self.ssource1CheckBox = wx.CheckBox(self, label='files',size=(160,30))
        self.ssource2CheckBox = wx.CheckBox(self, label='DB',size=(160,30))
        self.ssource3CheckBox = wx.CheckBox(self, label='webservice',size=(160,30))

        self.varioButton = wx.Button(self, -1, label=self.variopath_short, size=(210,30))
        self.scalarButton = wx.Button(self, -1, label=self.scalarpath_short, size=(210,30))

        self.varioDBComboBox = wx.ComboBox(self, choices=self.variotables,
                 style=wx.CB_DROPDOWN, value=self.defaultvariotable,size=(210,-1))
        self.scalarDBComboBox = wx.ComboBox(self, choices=self.scalartables,
                 style=wx.CB_DROPDOWN, value=self.defaultscalartable,size=(210,-1))
        self.varioWSButton = wx.Button(self, -1, label=self.mainsource ,size=(210,30))
        self.scalarWSButton = wx.Button(self, -1, label=self.mainsource ,size=(210,30))

        self.varioExtComboBox = wx.ComboBox(self, choices=self.varioext,
                 style=wx.CB_DROPDOWN, value=self.varioext[0],size=(80,-1))
        self.scalarExtComboBox = wx.ComboBox(self, choices=self.scalarext,
                 style=wx.CB_DROPDOWN, value=self.scalarext[0],size=(80,-1))

        self.okButton = wx.Button(self, wx.ID_OK, label='Continue',size=(210,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(210,30))
        if not self.db:
            self.varioDBComboBox.Disable()
            self.scalarDBComboBox.Disable()

        self.vsource1CheckBox.SetValue(True)
        self.vsource2CheckBox.SetValue(True)
        self.vsource3CheckBox.SetValue(True)
        self.ssource1CheckBox.SetValue(True)
        self.ssource2CheckBox.SetValue(True)
        self.ssource3CheckBox.SetValue(True)
        self.EnDis(self.vchoice,self.schoice)


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[]
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.scalarLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.variosourceLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.scalarsourceLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.vsource1CheckBox, noOptions))
        contlst.append((self.varioButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.varioExtComboBox, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.ssource1CheckBox, noOptions))
        contlst.append((self.scalarButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.scalarExtComboBox, noOptions))
        contlst.append((self.vsource2CheckBox, noOptions))
        contlst.append((self.varioDBComboBox, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.ssource2CheckBox, noOptions))
        contlst.append((self.scalarDBComboBox, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.vsource3CheckBox, noOptions))
        contlst.append((self.varioWSButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.ssource3CheckBox, noOptions))
        contlst.append((self.scalarWSButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)

        # A GridSizer will contain the other controls:
        cols = 7
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.vsource1CheckBox.Bind(wx.EVT_CHECKBOX, self.OnCb1)
        self.vsource2CheckBox.Bind(wx.EVT_CHECKBOX, self.OnCb2)
        self.vsource3CheckBox.Bind(wx.EVT_CHECKBOX, self.OnCb3)
        self.ssource1CheckBox.Bind(wx.EVT_CHECKBOX, self.OnCbs1)
        self.ssource2CheckBox.Bind(wx.EVT_CHECKBOX, self.OnCbs2)
        self.ssource3CheckBox.Bind(wx.EVT_CHECKBOX, self.OnCbs3)
        self.varioButton.Bind(wx.EVT_BUTTON, self.OnVario)
        self.scalarButton.Bind(wx.EVT_BUTTON, self.OnScalar)
        self.varioWSButton.Bind(wx.EVT_BUTTON, self.OnVarioWS)
        self.scalarWSButton.Bind(wx.EVT_BUTTON, self.OnScalarWS)

    def EnDis(self,vchoice,schoice):
        self.varioButton.Disable()
        self.scalarButton.Disable()
        self.varioDBComboBox.Disable()
        self.scalarDBComboBox.Disable()
        self.varioWSButton.Disable()
        self.scalarWSButton.Disable()
        self.varioExtComboBox.Disable()
        self.scalarExtComboBox.Disable()
        if vchoice == 0:
            self.vsource2CheckBox.SetValue(False)
            self.vsource3CheckBox.SetValue(False)
            self.varioButton.Enable()
            self.varioExtComboBox.Enable()
        if vchoice == 1:
            self.vsource1CheckBox.SetValue(False)
            self.vsource3CheckBox.SetValue(False)
            if self.db:
                self.varioDBComboBox.Enable()
        if vchoice == 2:
            self.vsource1CheckBox.SetValue(False)
            self.vsource2CheckBox.SetValue(False)
            self.varioWSButton.Enable()
        if schoice == 0:
            self.ssource2CheckBox.SetValue(False)
            self.ssource3CheckBox.SetValue(False)
            self.scalarButton.Enable()
            self.scalarExtComboBox.Enable()
        if schoice == 1:
            self.ssource1CheckBox.SetValue(False)
            self.ssource3CheckBox.SetValue(False)
            if self.db:
                self.scalarDBComboBox.Enable()
        if schoice == 2:
            self.ssource1CheckBox.SetValue(False)
            self.ssource2CheckBox.SetValue(False)
            self.scalarWSButton.Enable()


    def OnCb1(self, evt):
        self.vchoice = 0
        self.EnDis(self.vchoice,self.schoice)
        self.variosource = self.defaultvariopath
        self.scalarsource = self.defaultscalarpath

    def OnCb2(self, evt):
        self.vchoice = 1
        self.EnDis(self.vchoice,self.schoice)

    def OnCb3(self, evt):
        self.vchoice = 2
        self.EnDis(self.vchoice,self.schoice)

    def OnCbs1(self, evt):
        self.schoice = 0
        self.EnDis(self.vchoice,self.schoice)
        self.variosource = self.defaultvariopath
        self.scalarsource = self.defaultscalarpath

    def OnCbs2(self, evt):
        self.schoice = 1
        self.EnDis(self.vchoice,self.schoice)

    def OnCbs3(self, evt):
        self.schoice = 2
        self.EnDis(self.vchoice,self.schoice)

    def getExtensionList(self,path):
            import collections
            # returns a sorted extension list for all file in the directory
            # sorted by abunandce
            ext = []
            for f in os.listdir(path):
                sp = f.split('.')
                if len(sp) > 1:
                    ext.append(sp[-1])
            counter=collections.Counter(ext)
            sortlist = counter.most_common()
            sortlist.append(('*',1))
            #print (["*.{}".format(el[0]) for el in sortlist])
            return ["*.{}".format(el[0]) for el in sortlist]

    def OnVario(self, event):
        # Open a select path dlg as long as db and remote is not supported
        dialog = wx.DirDialog(None, "Choose a directory with variometer data:",self.defaultvariopath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            path = dialog.GetPath()
            varioext = self.getExtensionList(path)
            self.varioExtComboBox.Clear()
            try:
                self.varioExtComboBox.Append(varioext)
            except:
                self.varioExtComboBox.AppendItems(varioext)
            self.varioExtComboBox.SetValue(varioext[0])
            label = self.getShort(path)
            self.varioButton.SetLabel(label)
            self.defaultvariopath = path
        dialog.Destroy()

    def OnScalar(self, event):
        # Open a select path dlg as long as db and remote is not supported
        dialog = wx.DirDialog(None, "Choose a directory with scalar data:",self.defaultvariopath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            path = dialog.GetPath()
            scalarext = self.getExtensionList(path)
            self.scalarExtComboBox.Clear()
            try:
                self.scalarExtComboBox.Append(scalarext)
            except:
                self.scalarExtComboBox.AppendItems(scalarext)
            self.scalarExtComboBox.SetValue(scalarext[0])
            label = self.getShort(path)
            self.scalarButton.SetLabel(label)
            self.defaultscalarpath = path
        dialog.Destroy()

    def replaceCommands(self, dictionary, replacedict):
            if replacedict and not replacedict == {}:
                for el in replacedict:
                    if not dictionary.get(el,'') == '':
                        dictionary[el] = replacedict[el]
            return dictionary

    def OnVarioWS(self, event):
        defaultcommands = {'id':'id', 'starttime':'starttime', 'endtime':'endtime', 'format':'format', 'elements':'elements', 'type':'type','sampling_period':'sampling_period'}

        dlg = ConnectWebServiceDialog(None, title='Connecting to a webservice', services=self.services, default=self.mainsource, validgroups=['magnetism'])
        if dlg.ShowModal() == wx.ID_OK:
            # Create URL from inputs ignoring times
            service = dlg.serviceComboBox.GetValue()
            # get service depended commands dictionary
            replacedict = self.services.get(dlg.serviceComboBox.GetValue()).get('commands',{})
            defaultcommands = self.replaceCommands(defaultcommands, replacedict)
            group = dlg.groupComboBox.GetValue()
            obs_id = '{}={}'.format( defaultcommands.get('id'), dlg.idComboBox.GetValue())
            file_format = '&{}={}'.format(defaultcommands.get('format'), dlg.formatComboBox.GetValue())
            elements = '&{}={}'.format(defaultcommands.get('elements'), dlg.elementsTextCtrl.GetValue())
            data_type = '&{}={}'.format(defaultcommands.get('type'), dlg.typeComboBox.GetValue())
            period = '&{}={}'.format(defaultcommands.get('sampling_period'), dlg.sampleComboBox.GetValue())
            base = self.services.get(dlg.serviceComboBox.GetValue()).get(group).get('address')
            url = (base + '?' + obs_id + file_format +
                      elements + data_type + period)
            self.divariows = url
            self.varioWSButton.SetLabel(service)
            self.mainsource = service

    def OnScalarWS(self, event):
        defaultcommands = {'id':'id', 'starttime':'starttime', 'endtime':'endtime', 'format':'format', 'elements':'elements', 'type':'type','sampling_period':'sampling_period'}

        dlg = ConnectWebServiceDialog(None, title='Connecting to a webservice', services=self.services, default=self.mainsource, validgroups=['magnetism'])
        if dlg.ShowModal() == wx.ID_OK:
            # Create URL from inputs ignoring times
            service = dlg.serviceComboBox.GetValue()
            # get service depended commands dictionary
            replacedict = self.services.get(dlg.serviceComboBox.GetValue()).get('commands',{})
            defaultcommands = self.replaceCommands(defaultcommands, replacedict)
            group = dlg.groupComboBox.GetValue()
            obs_id = '{}={}'.format( defaultcommands.get('id'), dlg.idComboBox.GetValue())
            file_format = '&{}={}'.format(defaultcommands.get('format'), dlg.formatComboBox.GetValue())
            elements = '&{}={}'.format(defaultcommands.get('elements'), dlg.elementsTextCtrl.GetValue())
            data_type = '&{}={}'.format(defaultcommands.get('type'), dlg.typeComboBox.GetValue())
            period = '&{}={}'.format(defaultcommands.get('sampling_period'), dlg.sampleComboBox.GetValue())
            base = self.services.get(dlg.serviceComboBox.GetValue()).get(group).get('address')
            url = (base + '?' + obs_id + file_format +
                      elements + data_type + period)
            self.discalarws = url
            self.scalarWSButton.SetLabel(service)
            self.mainsource = service


class DIConnectDatabaseDialog(wx.Dialog):
    """
    Helper method to connect to edge
    Select shown keys
    """
    def __init__(self, parent, title, db, didict, options):
        super(DIConnectDatabaseDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.db = db
        self.didict = didict
        self.opt = options # get default pier etc from this list
        self.stations = [el for el in didict]
        defaultstation = 'WIC'
        if defaultstation in self.stations:
            self.defaultstation = defaultstation
        else:
            self.defaultstation = self.stations[0]
        defaultpier = 'A2'
        self.stationdict = self.getStationData(didict.get(self.defaultstation), pier=defaultpier, station= self.defaultstation)
        self.createControls()
        self.doLayout()
        self.bindControls()

    def createControls(self):
        self.stationsLabel = wx.StaticText(self, label="Available stations:",size=(400,25))
        self.stationsComboBox = wx.ComboBox(self, choices=self.stations,
            style=wx.CB_DROPDOWN, value=self.defaultstation,size=(400,-1))
        self.piersLabel = wx.StaticText(self, label="Piers:",size=(400,25))
        self.piersComboBox = wx.ComboBox(self, choices=self.stationdict.get('piers'),
            style=wx.CB_DROPDOWN, value=self.stationdict.get('selectedpier'),size=(400,-1))
        self.observersLabel = wx.StaticText(self, label="Observers:",size=(400,25))
        self.observersComboBox = wx.ComboBox(self, choices=self.stationdict.get('observers'),
            style=wx.CB_DROPDOWN, value=self.stationdict.get('selectedobserver'),size=(400,-1))

        self.amountLabel = wx.StaticText(self, label="Selected DI datasets:",size=(400,25))
        self.amountTextCtrl = wx.TextCtrl(self,value=self.stationdict.get('amount'),size=(160,-1))

        self.startTimeLabel = wx.StaticText(self, label="Start Time: ",size=(400,25))
        self.startDatePicker = wxDatePickerCtrl(self,dt=self.stationdict.get('mindate'),size=(160,25)) #wx.DateTime().Today()
        self.startTimePicker = wx.TextCtrl(self, value=self.stationdict.get('mintime'),size=(160,25))

        self.endTimeLabel = wx.StaticText(self, label="End Time: ",size=(400,25))
        self.endDatePicker = wxDatePickerCtrl(self,dt=self.stationdict.get('maxdate'), size=(160,25))
        self.endTimePicker = wx.TextCtrl(self, value=self.stationdict.get('maxtime'),size=(160,25))

        self.okButton = wx.Button(self, wx.ID_OK, label='Continue',size=(160,25))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,25))


    def bindControls(self):
        self.stationsComboBox.Bind(wx.EVT_TEXT, self.update)
        self.piersComboBox.Bind(wx.EVT_TEXT, self.update)
        self.observersComboBox.Bind(wx.EVT_TEXT, self.update)
        self.startDatePicker.Bind(wx.EVT_TEXT, self.update)
        self.endDatePicker.Bind(wx.EVT_TEXT, self.update)


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        elemlist = [(self.stationsLabel, noOptions),
                 (self.stationsComboBox, expandOption),
                 (self.piersLabel, dict()),
                 (self.piersComboBox, dict(flag=wx.EXPAND)),
                 (self.observersLabel, noOptions),
                 (self.observersComboBox, expandOption),
                 (self.amountLabel, noOptions),
                 (self.amountTextCtrl, expandOption),
                 (self.startTimeLabel, noOptions),
                 emptySpace,
                 (self.startDatePicker, expandOption),
                 (self.startTimePicker, expandOption),
                 (self.endTimeLabel, noOptions),
                 emptySpace,
                 (self.endDatePicker, expandOption),
                 (self.endTimePicker, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(elemlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=5, hgap=10)

        # Add the controls to the sizers:
        for control, options in elemlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def pydate2wxdate(self,date):
        assert isinstance(date, (datetime, date))
        tt = date.timetuple()
        dmy = (tt[2], tt[1]-1, tt[0])
        try:
            return wx.DateTime.FromDMY(*dmy)
        except:
            return wx.DateTimeFromDMY(*dmy)

    def wxdate2pydate(self,date):
        assert isinstance(date, wx.DateTime)
        if date.IsValid():
             ymd = map(int, date.FormatISODate().split('-'))
             return datetime.date(*ymd)
        else:
             return None

    def getLimits(self, data):
        timecol = [el[3] for el in data]
        mindatetime = min(timecol)
        maxdatetime = max(timecol)
        mintime = mindatetime.strftime("%H:%M:%S")
        mindate = self.pydate2wxdate(mindatetime)
        maxtime = maxdatetime.strftime("%H:%M:%S")
        maxdate = self.pydate2wxdate(maxdatetime)
        return mintime, maxtime, mindate, maxdate


    def getStationData(self, content, pier=None, observer=None, mindatetime=None, maxdatetime=None, station=None):
        # returns a stationdict with data, mintime, maxtime, amount
        stationdict = {}
        data = content.get('data')
        orgdata = data
        stationdict['piers'] = content.get('piers')
        observerlist = ['all']
        observerlist.extend(content.get('observers'))
        stationdict['observers'] = observerlist
        stationdict['amount'] = str(len(content.get('data')))
        stationdict['selectedobserver'] = 'all'
        stationdict['station'] = station

        if pier:
             if pier in stationdict['piers']:
                 stationdict['selectedpier'] = pier
             else:
                 stationdict['selectedpier'] = stationdict.get('piers')[0]
             data = [el for el in data if el[1] == stationdict.get('selectedpier')]
             stationdict['observers'] = list(set([el[2] for el in data]))
        else:
             stationdict['selectedpier'] = stationdict['piers'][0]
        if observer and not observer == 'all':
             if observer in stationdict['observers']:
                 stationdict['selectedobserver'] = observer
                 data = [el for el in data if el[2] == stationdict.get('selectedobserver')]
                 stationdict['piers'] = list(set([el[1] for el in orgdata if el[2] == observer]))
        else:
             stationdict['selectedobserver'] = 'all'
        if mindatetime:
             data = [el for el in data if el[3] >= mindatetime]
        if maxdatetime:
             data = [el for el in data if el[3] <= maxdatetime]

        stationdict['amount'] = str(len(data))
        if len(data) > 0:
            stationdict['id'] = [el[0] for el in data]
            stationdict['mintime'], stationdict['maxtime'], stationdict['mindate'], stationdict['maxdate'] = self.getLimits(data)
        else:
            stationdict['mintime'] = self.startTimePicker.GetValue()
            stationdict['mindate'] = self.startDatePicker.GetValue()
            stationdict['maxtime'] = self.endTimePicker.GetValue()
            stationdict['maxdate'] = self.endDatePicker.GetValue()
        return stationdict

    def dt(self, wxval,wxstr):
        tl = wxstr.split(':')
        if wxval and len(tl) == 3:
            return datetime(wxval.GetYear(), wxval.GetMonth()+1, wxval.GetDay(), int(tl[0]), int(tl[1]), int(tl[2]))
        elif wxval:
            return datetime(wxval.GetYear(), wxval.GetMonth()+1, wxval.GetDay())
        else:
            return None

    def update(self, event):
        station = self.stationsComboBox.GetValue()
        if not station:
            station = self.defaultstation
        pier = self.piersComboBox.GetValue()
        obs = self.observersComboBox.GetValue()
        content = self.didict.get(station)
        midate = self.dt(self.startDatePicker.GetValue(),self.startTimePicker.GetValue())
        madate = self.dt(self.endDatePicker.GetValue(),self.endTimePicker.GetValue())

        self.stationdict = self.getStationData(content, pier=pier, observer=obs, mindatetime=midate, maxdatetime=madate, station=station)

        self.amountTextCtrl.Clear()
        self.amountTextCtrl.SetValue(self.stationdict.get('amount'))
        self.endTimePicker.Clear()
        self.endTimePicker.SetValue(self.stationdict.get('maxtime'))
        self.endDatePicker.SetValue(self.stationdict.get('maxdate'))
        self.startTimePicker.Clear()
        self.startTimePicker.SetValue(self.stationdict.get('mintime'))
        self.startDatePicker.SetValue(self.stationdict.get('mindate'))
        self.piersComboBox.Clear()
        self.piersComboBox.AppendItems(self.stationdict.get('piers'))
        self.observersComboBox.Clear()
        self.observersComboBox.AppendItems(self.stationdict.get('observers'))


class DefineVarioDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title):
        super(DefineVarioDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.path = ''
        self.variopath = ''
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.sourceLabel = wx.StaticText(self, label="Choose Variometer source:")
        self.loadfileLabel = wx.StaticText(self, label="1) Access local files:")
        self.loadFileButton = wx.Button(self,-1,"Get Path",size=(160,30))
        self.getdbLabel = wx.StaticText(self, label="2) Access database:")
        self.remoteLabel = wx.StaticText(self, label="3) Access remote files:")

        self.okButton = wx.Button(self, wx.ID_OK, label='Use',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.sourceLabel, noOptions)]
        contlst.append((self.loadfileLabel, noOptions))
        contlst.append((self.loadFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.getdbLabel, noOptions))
        contlst.append((self.remoteLabel, noOptions))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnDefineVario)

    def OnDefineVario(self,e):
        dialog = wx.DirDialog(None, "Choose a directory with variometer data:",self.variopath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.path = dialog.GetPath()
            self.variopath = self.path
        dialog.Destroy()


class DefineScalarDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title):
        super(DefineScalarDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.path = ''
        self.scalarpath = ''
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.sourceLabel = wx.StaticText(self, label="Choose Variometer source:")
        self.loadfileLabel = wx.StaticText(self, label="1) Access local files:")
        self.loadFileButton = wx.Button(self,-1,"Get Path",size=(160,30))
        self.getdbLabel = wx.StaticText(self, label="2) Access database:")
        self.remoteLabel = wx.StaticText(self, label="3) Access remote files:")

        self.okButton = wx.Button(self, wx.ID_OK, label='Use',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[(self.sourceLabel, noOptions)]
        contlst.append((self.loadfileLabel, noOptions))
        contlst.append((self.loadFileButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.getdbLabel, noOptions))
        contlst.append((self.remoteLabel, noOptions))
        contlst.append((self.okButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.closeButton, dict(flag=wx.ALIGN_CENTER)))

        # A GridSizer will contain the other controls:
        cols = 1
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.loadFileButton.Bind(wx.EVT_BUTTON, self.OnDefineScalar)


    def OnDefineScalar(self,e):
        dialog = wx.DirDialog(None, "Choose a directory with scalar data:",self.scalarpath,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
        if dialog.ShowModal() == wx.ID_OK:
            self.path = dialog.GetPath()
            self.scalarpath = self.path
        dialog.Destroy()


class DISaveDialog(wx.Dialog):
    """
    Dialog for Stream panel
    Select shown keys
    """

    def __init__(self, parent, title):
        super(DISaveDialog, self).__init__(parent=parent,
            title=title, size=(200, 300))
        self.choice = ''
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        #self.yesButton = wx.Button(self, wx.ID_OK, label="Overwrite",size=(160,30))
        #self.noButton = wx.Button(self, wx.ID_OK,label="Cancel",size=(160,30))
        #self.alternativeButton = wx.Button(self, wx.ID_OK,label="Write as alternative",size=(160,30))
        self.yesButton = wx.Button(self, wx.ID_YES, label="Overwrite",size=(160,30))
        self.noButton = wx.Button(self, wx.ID_NO,label="Cancel",size=(160,30))
        self.alternativeButton = wx.Button(self, wx.ID_YES,label="Write as alternative",size=(160,30))
        self.sourceLabel = wx.StaticText(self, label="A file with similar name is already existing. You can:")

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        mainSizer = wx.BoxSizer(wx.VERTICAL)

        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        # Add the controls to the sizers:
        contlst=[]
        contlst.append((self.noButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.alternativeButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append((self.yesButton, dict(flag=wx.ALIGN_CENTER)))


        mainSizer.Add(self.sourceLabel, 0, wx.ALIGN_LEFT | wx.ALL, 3)

        # A GridSizer will contain the other controls:
        cols = 3
        rows = int(np.ceil(len(contlst)/float(cols)))
        rows = 2
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)
        for control, options in contlst:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        mainSizer.Add(boxSizer, 1, wx.EXPAND)

        self.SetSizerAndFit(mainSizer)

    def bindControls(self):
        self.noButton.Bind(wx.EVT_BUTTON, self.OnNo)
        self.yesButton.Bind(wx.EVT_BUTTON, self.OnYes)
        self.alternativeButton.Bind(wx.EVT_BUTTON, self.OnAlternative)

    def OnNo(self, e):
        self.choice = 'no'
        self.Close(True)

    def OnYes(self, e):
        self.choice = 'yes'
        self.Close(True)

    def OnAlternative(self, e):
        self.choice = 'alternative'
        self.Close(True)


class ParameterDictDialog(wx.Dialog):
    """
    Dialog for general Parameter selection - based on a dictionary
    """

    def __init__(self, parent, title, dictionary, preselect):
        super(ParameterDictDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.dict = dictionary
        self.depth = len(list(self.iter_leafs(dictionary))[0][0])
        self.elementlist = []
        selected = []
        self.widgcnt = 0

        self.lastlayer, self.layhead, self.selected = self.getHeadsAndLast(self.depth, self.dict,preselect)

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        # Add ComboBoxes
        self.createTopWidgets()
        # Add Ok/Cancel Buttons
        self.createBottomWidgets()
        self.setupPanel(self.lastlayer, self.selected)
        # Set sizer and window size
        self.SetSizerAndFit(self.mainSizer)


    def setupPanel(self, lastlayer, selected):
        # Add Settings Panel
        self.panel = ParameterDictPanel(self, lastlayer, selected)
        self.panel.SetInitialSize((450, 400))
        self.mainSizer.Insert(1, self.panel, 1, wx.EXPAND | wx.ALL, 10)

    def iter_leafs(self, d, keys=None):
        if not keys:
            keys = []
        for key, val in d.items():
            if isinstance(val, dict):
                #try:
                #     yield from self.iter_leafs(val, keys + [key])
                #except:
                for el in self.iter_leafs(val, keys + [key]):
                    yield el
            else:
                yield keys + [key], val

    def getHeadsAndLast(self, depth, d, preselect=None):
        if not preselect:
            preselect = []
        #print ("PreselectioN", preselect)
        if self.depth > 1:
            lay = []
            last=d
            selection = []
            for i in range(depth-1):
                lay.append([])
                for el in last:
                    lay[i].append(el)
                if len(preselect) >= depth-1 and preselect[i] in lay[i]:
                    selection.append(preselect[i])
                else:
                    selection.append(lay[i][0])
                #print ("CHOOSING", selection[-1])
                last=last.get(selection[-1])
            #print ("Layer list", lay, len(lay))
            headlist = lay
            return last, headlist, selection
        else:
            return d, [], []


    def createTopWidgets(self):
        """Create and layout the widgets in the dialog"""
        self.widgcnt = 0

        if len(self.layhead) > 0:
            for idx,el in enumerate(self.layhead):
                self.elementlist.append(['Combo', wx.ComboBox(self, choices=el,
                            style=wx.CB_DROPDOWN, value=self.selected[idx],size=(110,-1))])
                self.elementlist[-1][1].Bind(wx.EVT_COMBOBOX, self.OnUpdate)

        contlist = []
        for el in self.elementlist:
            contlist.append((el[1],dict()))

        for control, options in contlist:
            self.mainSizer.Add(control, **options)
            self.widgcnt += 1

    def createBottomWidgets(self):
        """Create and layout the widgets in the dialog"""
        btnSizer = wx.StdDialogButtonSizer()

        okBtn = wx.Button(self, wx.ID_OK, label="OK",size=(160,30))
        btnSizer.AddButton(okBtn)

        cancelBtn = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))
        btnSizer.AddButton(cancelBtn)
        btnSizer.Realize()

        self.mainSizer.Add(btnSizer, 0, wx.ALL | wx.ALIGN_RIGHT, 5)


    def OnUpdate(self, event):
        #print ("YESS")
        #print (self.elementlist)
        selected = []
        for idx,el in enumerate(self.layhead):
            pos = idx*2
            selected.append(self.elementlist[pos][1].GetValue())
            #print ( "SEL", self.elementlist[pos][1].GetValue() )

        print ("Selected", selected)
        self.lastlayer, self.layhead, self.selected = self.getHeadsAndLast(self.depth, self.dict, selected)
        self.panel.Destroy()
        self.setupPanel(self.lastlayer,self.selected)
        self.mainSizer.Layout()


class ParameterDictPanel(scrolledpanel.ScrolledPanel):
    def __init__(self, parent, lastlayer, selected):
        scrolledpanel.ScrolledPanel.__init__(self, parent, -1, size=(-1, -1))  #size=(950, 750)
        #self.ShowFullScreen(True)
        self.lastlayer = lastlayer
        self.selected = selected
        self.elementlist = []

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        self.createControls(selected)
        self.doLayout()
        self.SetSizer(self.mainSizer)
        self.mainSizer.Fit(self)
        self.SetupScrolling()

    def createControls(self, selected):
        """
        selected is a list of same length as self.layhead containing selected items
        """

        lastlayer= self.lastlayer
        for el in lastlayer:
            #print (el, lastlayer[el])
            self.elementlist.append(['Label',wx.StaticText(self, label=el,size=(110,30))])
            if not isinstance(lastlayer[el], bool):
                self.elementlist.append(['Text', wx.TextCtrl(self,name=el,value="{}".format(lastlayer[el]),size=(210,30))])
            else:
                choices=['True','False']
                self.elementlist.append(['Radio', wx.RadioBox(self,name=el,label="",choices=choices, majorDimension=2, style=wx.RA_SPECIFY_COLS,size=(210,50))])
                if lastlayer[el]:
                    self.elementlist[-1][1].SetSelection(0)
                else:
                    self.elementlist[-1][1].SetSelection(1)

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        self._nb_control = 0

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = []
        opt=None
        for el in self.elementlist:
            if el[0] == 'Label':
                opt = noOptions
            elif el[0] == 'Text':
                opt = expandOption
            elif el[0] == 'Radio':
                opt = noOptions
            elif el[0] == 'Combo':
                opt = noOptions
            elif el[0] == 'Button':
                opt = dict(flag=wx.ALIGN_CENTER)
            contlist.append((el[1],opt))

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlist:
            gridSizer.Add(control, **options)

        self.mainSizer.Add(gridSizer, 0, wx.EXPAND)


class InputSheetDialog(wx.Dialog):
    """
    DESCRITPTION
        InputDialog for DI data
    """

    def __init__(self, parent, title, path, distation, diparameters, cdate, datapath, distruct, height, width):
        style = wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        super(InputSheetDialog, self).__init__(parent=parent,
            title=title, style=style) #size=(1000, 800),

        self.path = path
        self.diparameters = diparameters
        layout = {}
        layout['scalevalue'] = diparameters.get('scalevalue')
        layout['double'] = diparameters.get('double')
        layout['order'] = diparameters.get('order').split(',')
        # diparameters contains a list of all opened/available DI files for this stationID
        #print ("LAYOUT", layout)

        self.cdate = cdate
        self.units = ['degree','gon']

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        # Add Settings Panel
        self.panel = SettingsPanel(self, cdate, path, distation, diparameters, layout, datapath, distruct )
        self.panel.SetInitialSize((height, width))
        self.mainSizer.Add(self.panel, 1, wx.EXPAND | wx.ALL, 10)
        # Add Save/Cancel Buttons
        self.createWidgets()
        # Set sizer and window size
        self.SetSizerAndFit(self.mainSizer)
        #self.mainSizer.Fit(self)

    def createWidgets(self):
        """Create and layout the widgets in the dialog"""
        btnSizer = wx.StdDialogButtonSizer()

        saveBtn = wx.Button(self, wx.ID_YES, label="Save",size=(160,30))
        saveBtn.Bind(wx.EVT_BUTTON, self.onSave)
        btnSizer.AddButton(saveBtn)

        cancelBtn = wx.Button(self, wx.ID_NO, label='Close',size=(160,30))  # Using ID_NO as ID_CLOSE is not working with StdDialogButtonSizer
        cancelBtn.Bind(wx.EVT_BUTTON, self.onClose)
        btnSizer.AddButton(cancelBtn)
        btnSizer.Realize()

        self.mainSizer.Add(btnSizer, 0, wx.ALL | wx.ALIGN_RIGHT, 5)

    def degminsec2deg(self, string, back='decimal'):
        """
        DESCRIPTION
            checks input string and returns either deg: or deg.
        PARAMETER
            string (string):   a deg:min:sec of deg.decimal value
            back   (float) :   either decimal or minsec
        """
        string = str(string)
        if string.find(':') > 0:
            val = 0.
            numlist = string.split(':')
            for idx, ele in enumerate(numlist):
                add = float(ele)/float(60.**idx)
                val += float(add)
        else:
            if string.find(',') > 0:
                string = string.replace(',','.')
            try:
                val = float(string)
            except:
                val = 999.0

        if not val >= -180 and not val <= 360:
            return 999.

        def decdeg2dms(dd):
            is_positive = dd >= 0
            dd = abs(dd)
            minutes,seconds = divmod(dd*3600,60)
            degrees,minutes = divmod(minutes,60)
            degrees = degrees if is_positive else -degrees
            return [degrees,minutes,seconds]

        if back == 'minsec':
            return ":".join(decdeg2dms(val))
        else:
            return str(val)


    def onSave(self, event):
        opstring = []
        saving = True
        angleerror = 0
        timeerror = 0

        def testangle(angle, primary, prevangle=None, anglecount=0):
            mireproblem = False
            tangle = 999
            if angle in ["0.0000 or 00:00:00.0", ""]:
                if primary == 1:
                    mireproblem = True
                if primary == 0:
                    #angle = self.degminsec2deg(prevangle)
                    try:
                        angle = self.panel._degminsec2deg(prevangle)
                    except:
                        mireproblem = True
                        angle = 999
            else:
                #angle = self.degminsec2deg(angle)
                try:
                    angle = self.panel._degminsec2deg(angle)
                except:
                    mireproblem = True
                    angle = 999
            try:
                tangle = float(angle)
            except:
                mireproblem = True
            if tangle == 999:
                mireproblem = True
            if tangle > 400:
                mireproblem = True
            if tangle < -400:
                mireproblem = True

            if mireproblem:
                #if anglecount == 0:
                #    checkdlg = wx.MessageDialog(self, "Provided angles:\n"
                #            "Please check your input data\n",
                #            "Angle checker", wx.OK|wx.ICON_INFORMATION)
                #    checkdlg.ShowModal()
                return 999., 1
            return angle, 0


        def testtime(time, datestring, primary=1, prevtime=None, timecount=0):
            timeproblem = False
            if time in ["00:00:00", ""]:
                if primary == 1:
                    timeproblem = True
                if primary == 0:
                    time = prevtime
            else:
                #2010-06-11_12:03:00
                time = datestring + '_' + time

            try:
                tt = datetime.strptime(time, "%Y-%m-%d_%H:%M:%S")
            except:
                timeproblem = True

            if timeproblem:
                if timecount == 0:
                     checkdlg = wx.MessageDialog(self, "Provided times:\n"
                            "Input data ' {} ' could not be interpreted\n".format(time),
                            "Time checker", wx.OK|wx.ICON_INFORMATION)
                     checkdlg.ShowModal()
                return "2233-12-12_13:21:23", 1

            resultt = datetime.strptime(time, "%Y-%m-%d_%H:%M:%S")
            return resultt.strftime("%Y-%m-%d_%H:%M:%S"), 0
            #return time

        # Get header
        opstring.append("# MagPy Absolutes")
        unit = self.panel.UnitComboBox.GetValue()
        date = self.panel.DatePicker.GetValue()
        observer = self.panel.ObserverTextCtrl.GetValue()
        iagacode= self.panel.CodeTextCtrl.GetValue()
        theo = self.panel.TheoTextCtrl.GetValue()
        flux = self.panel.FluxTextCtrl.GetValue()
        azimuth = self.panel.AzimuthTextCtrl.GetValue()
        pillar = self.panel.PillarTextCtrl.GetValue()
        temp = self.panel.TempTextCtrl.GetValue()
        finst = self.panel.FInstTextCtrl.GetValue()
        comm = self.panel.CommentTextCtrl.GetValue()

        try:
            self.panel.PillarTextCtrl.SetBackgroundColour(wx.NullColor)
            self.panel.CodeTextCtrl.SetBackgroundColour(wx.NullColor)
            self.panel.AzimuthTextCtrl.SetBackgroundColour(wx.NullColor)
        except: # for MacOs
            self.panel.PillarTextCtrl.SetBackgroundColour(wx.WHITE)
            self.panel.CodeTextCtrl.SetBackgroundColour(wx.WHITE)
            self.panel.AzimuthTextCtrl.SetBackgroundColour(wx.WHITE)

        fluxorient = self.panel.ressignRadioBox.GetSelection()
        if fluxorient == 0:
            ressign = 1
        else:
            ressign = -1

        if theo == "type_serial_version":
            theo = ''
        if flux == "type_serial_version":
            theo = ''
        if finst == "type_serial_version":
            finst = ''

        opstring.append("# Abs-Observer: {}".format(observer))
        opstring.append("# Abs-Theodolite: {}".format(theo))
        opstring.append("# Abs-TheoUnit: {}".format(unit[:3]))
        opstring.append("# Abs-FGSensor: {}".format(flux))
        azi,err = testangle(azimuth,1)
        if not err == 0:
            self.panel.AzimuthTextCtrl.SetBackgroundColour(wx.RED)
        opstring.append("# Abs-AzimuthMark: {}".format(azi))
        opstring.append("# Abs-Pillar: {}".format(pillar))
        opstring.append("# Abs-Scalar: {}".format(finst))
        now = datetime.now(timezone.utc)
        opstring.append("# Abs-InputDate: {}".format(now.strftime("%Y-%m-%d")))
        opstring.append("# Abs-Temperature: {}".format(temp))
        opstring.append("# Abs-Notes: {}".format(comm.replace('\n',' ')))

        # Get Mire
        opstring.append("Miren:")
        amu1 = self.panel.AmireUp1TextCtrl.GetValue()
        amu2 = self.panel.AmireUp2TextCtrl.GetValue()
        amd1 = self.panel.AmireDown1TextCtrl.GetValue()
        amd2 = self.panel.AmireDown2TextCtrl.GetValue()
        bmu1 = self.panel.BmireUp1TextCtrl.GetValue()
        bmu2 = self.panel.BmireUp2TextCtrl.GetValue()
        bmd1 = self.panel.BmireDown1TextCtrl.GetValue()
        bmd2 = self.panel.BmireDown2TextCtrl.GetValue()
        amu1,err = testangle(amu1,1,anglecount=angleerror)
        if err == 0:
            self.panel.AmireUp1TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.AmireUp1TextCtrl.SetForegroundColour(wx.RED)
        angleerror += err
        amu2,err = testangle(amu2,0,amu1,anglecount=angleerror)
        if err == 0:
            self.panel.AmireUp2TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.AmireUp2TextCtrl.SetForegroundColour(wx.RED)
        angleerror += err
        amd1,err = testangle(amd1,1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.AmireDown1TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.AmireDown1TextCtrl.SetForegroundColour(wx.RED)
        amd2,err = testangle(amd2,0, amd1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.AmireDown2TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.AmireDown2TextCtrl.SetForegroundColour(wx.RED)
        bmu1,err = testangle(bmu1,1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.BmireUp1TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.BmireUp1TextCtrl.SetForegroundColour(wx.RED)
        bmu2,err = testangle(bmu2,0, bmu1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.BmireUp2TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.BmireUp2TextCtrl.SetForegroundColour(wx.RED)
        bmd1,err = testangle(bmd1,1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.BmireDown1TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.BmireDown1TextCtrl.SetForegroundColour(wx.RED)
        bmd2,err = testangle(bmd2,0, bmd1,anglecount=angleerror)
        angleerror += err
        if err == 0:
            self.panel.BmireDown2TextCtrl.SetForegroundColour(wx.BLACK)
        else:
            self.panel.BmireDown2TextCtrl.SetForegroundColour(wx.RED)
        mline = "{}  {}  {}  {}  {}  {}  {}  {}".format(amu1, amu2, amd1, amd2, bmu1, bmu2, bmd1, bmd2)
        opstring.append("{}".format(mline))

        # Get Horizontals
        ymd = map(int, date.FormatISODate().split('-'))
        datestring = datetime(*ymd).strftime("%Y-%m-%d")

        opstring.append("Positions:")
        timelist = []
        for comp in ['EU','WU','ED','WD','NU','SD','ND','SU']:
            val = []
            anglelist = []
            for i in ['1','2']:
                if not comp in ['EU','WU','ED','WD']:
                    ellst = ['Time','GC','Angle','Residual']
                else:
                    ellst = ['Time','Angle','GC','Residual']
                for col in ellst:
                    na = comp+i
                    val.append(eval('self.panel.'+na+col+'TextCtrl.GetValue()'))
                    if col[0] == 'A' and i == '1':
                        val[-1], err = testangle(val[-1],1,anglecount=angleerror)
                        angleerror += err
                        if err == 0:
                            eval('self.panel.{}{}TextCtrl.SetForegroundColour(wx.BLACK)'.format(na,col))
                        else:
                            eval('self.panel.{}{}TextCtrl.SetForegroundColour(wx.RED)'.format(na,col))
                        anglelist.append(val[-1])
                    elif col[0] == 'A' and i == '2':
                        val[-1], err = testangle(val[-1],0,val[-5],anglecount=angleerror)
                        angleerror += err
                        if err == 0:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.BLACK)')
                        else:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.RED)')
                        anglelist.append(val[-1])
                    if col[0] == 'G':
                        if len(val[-1].split('/')) > 1:
                            val[-1] = [el for el in val[-1].split('/') if el.endswith(unit[:3])][0].replace(unit[:3],'')
                        else:
                            val[-1] = val[-1]
                    if col[0] == 'R':
                        val[-1] = ressign*float(val[-1].replace(',','.'))
                    if col[0] == 'T' and i == '1':
                        val[-1], terr = testtime(val[-1], datestring,timecount=timeerror)
                        timeerror += terr
                        if terr == 0:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.BLACK)')
                        else:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.RED)')
                        timelist.append(val[-1])
                    elif col[0] == 'T' and i == '2':
                        val[-1], terr = testtime(val[-1], datestring, 0, val[-5],timecount=timeerror)
                        timeerror += terr
                        if terr == 0:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.BLACK)')
                        else:
                            eval('self.panel.'+na+col+'TextCtrl.SetForegroundColour(wx.RED)')
                        timelist.append(val[-1])
            l1 = "  ".join(map(str, val[:4]))
            l2 = "  ".join(map(str, val[4:]))
            opstring.append("{}".format(l1))
            opstring.append("{}".format(l2))


        for col in ellst:
            val.append(eval('self.panel.SC'+col+'TextCtrl.GetValue()'))
            if col[0] == 'A':
                val[-1], err = testangle(val[-1],0,val[-5], anglecount=angleerror)
                angleerror += err
                if err == 0:
                    eval('self.panel.SC{}TextCtrl.SetForegroundColour(wx.BLACK)'.format(col))
                else:
                    eval('self.panel.SC{}TextCtrl.SetForegroundColour(wx.RED)'.format(col))
                anglelist.append(val[-1])
            if col[0] == 'G':
                if len(val[-1].split('/')) > 1:
                   val[-1] = [el for el in val[-1].split('/') if el.endswith(unit[:3])][0].replace(unit[:3],'')
                else:
                   val[-1] = val[-1]
            if col[0] == 'R':
                val[-1] = float(val[-1].replace(',','.'))
            if col[0] == 'T':
                val[-1],terr = testtime(val[-1], datestring, 0, val[-5],timecount=timeerror)
                timeerror += terr
                if terr == 0:
                    eval('self.panel.SC{}TextCtrl.SetForegroundColour(wx.BLACK)'.format(col))
                else:
                    eval('self.panel.SC{}TextCtrl.SetForegroundColour(wx.RED)'.format(col))
                timelist.append(val[-1])
        l1 = "  ".join(map(str, val[-4:]))
        #print ("Values", l1, val)
        opstring.append("{}".format(l1))

        # Get F vals
        opstring.append("PPM:")
        fvals = self.panel.FValsTextCtrl.GetValue()
        if not fvals == '' and not fvals.startswith('time,value'):
            fvals = [el.split(',') for el in fvals.split('\n')]
            fbase = self.panel.FBaseTextCtrl.GetValue()

            if not fbase == '':
                try:
                    fbase = float(fbase.replace(',','.'))
                except:
                    fbase = 0.0
            else:
                fbase = 0.0

            if len(fvals) > 0 and len(fvals[0]) == 2:
                for el in fvals:
                    if len(el) > 1 and not el in [' ','  '] and not el[0] == '00:00:00':
                        t = testtime(el[0], datestring)
                        f = float(el[1])+fbase
                        fline = "{} {}".format(t[0],f)
                        opstring.append("{}".format(fline))
            else:
                checkdlg = wx.MessageDialog(self, "F values:\n"
                        "It seems as if you provided F values.\nThe format, however, could not be interpreted.\nInputs should look like: 12:12:00, 23.5\n",
                        "F data checker", wx.OK|wx.ICON_INFORMATION)
                checkdlg.ShowModal()

        opstring.append("Result:")  # For historic reasons: eventually add results again


        # Check block
        if 999 in [amu1, amu2, amd1, amd2, bmu1, bmu2, bmd1, bmd2]:
            saving = False
            #if anglecount == 0:
            checkdlg = wx.MessageDialog(self, "Azimuth measurements:\n"
                            "Error within angles. Please check your input data\n",
                            "Angle checker", wx.OK|wx.ICON_INFORMATION)
            checkdlg.ShowModal()
        if 999 in anglelist:
            saving = False
            #if anglecount == 0:
            checkdlg = wx.MessageDialog(self, "DI measurements:\n"
                            "Error within angles. Please check your input data\n",
                            "Angle checker", wx.OK|wx.ICON_INFORMATION)
            checkdlg.ShowModal()
        if "2233-12-12_13:21:23" in timelist:
            saving = False
            #checkdlg = wx.MessageDialog(self, "Provided times:\n"
            #                "Input data ' {} ' could not be interpreted\n".format(time),
            #                "Time checker", wx.OK|wx.ICON_INFORMATION)
            #checkdlg.ShowModal()
        if pillar == '' or iagacode=='' or azimuth=='':
            saving = False
            checkdlg = wx.MessageDialog(self, "Meta data:\n"
                        "You need to provide a pillar name and a station code\n",
                        "Meta data checker", wx.OK|wx.ICON_INFORMATION)
            checkdlg.ShowModal()
            if pillar=='':
                self.panel.PillarTextCtrl.SetBackgroundColour(wx.RED)
            if iagacode=='':
                self.panel.CodeTextCtrl.SetBackgroundColour(wx.RED)
            if azimuth=='':
                self.panel.AzimuthTextCtrl.SetBackgroundColour(wx.RED)


        filename = timelist[0].replace(':','-')+'_'+pillar+'_'+iagacode+'.txt'

        # Write Block
        if saving:
            try:
                # python2 - will fail in py3
                opstring = [unicode(el+'\n', "utf-8") for el in opstring]
            except:
                # python3
                opstring = ["{}\n".format(el) for el in opstring]
            didirname = os.path.expanduser("~")
            dialog = wx.DirDialog(None, "Choose directory to write data:",didirname,style=wx.DD_DEFAULT_STYLE | wx.DD_NEW_DIR_BUTTON)
            if dialog.ShowModal() == wx.ID_OK:
                didirname = dialog.GetPath() # modify self.dirname
                out = os.path.join(didirname,filename)
                # Check box if file existing, overwrite cancel
                writefile = True
                if os.path.isfile(out):
                    dlg = DISaveDialog(None, title='File Existing')
                    dlg.ShowModal()
                    if dlg.choice == 'alternative':
                        # creating a new file name with one second plus
                        filealreadyexisting = True
                        newtime0 = timelist[0]
                        while filealreadyexisting:
                            newtimetemp = datetime.strptime(newtime0, '%Y-%m-%d_%H:%M:%S') + timedelta(seconds=1)
                            newtime0 = newtimetemp.strftime('%Y-%m-%d_%H:%M:%S')
                            filename = newtime0.replace(':','-')+'_'+pillar+'_'+iagacode+'.txt'
                            out = os.path.join(didirname,filename)
                            if not os.path.isfile(out):
                                filealreadyexisting = False
                        writefile = True
                    elif  dlg.choice == 'yes':
                        writefile = True
                    else:
                        writefile = False
                    dlg.Destroy()
                if writefile:
                    fo = open(out, "w+")
                    #print ("Name of the file: ", fo.name)
                    fo.writelines( opstring )
                    fo.close()
                    dlg = wx.MessageDialog(self, "Data set {} successfully written.\n".format(filename),
                                    "File written", wx.OK|wx.ICON_INFORMATION)
                    dlg.ShowModal()
            dialog.Destroy()

    def onClose(self, event):
        closedlg = wx.MessageDialog(self, "Unsaved data will be lost\n"
                        "Continue?\n".format(time),
                        "Closing DI sheet", wx.YES_NO|wx.ICON_INFORMATION)

        if closedlg.ShowModal() == wx.ID_YES:
            closedlg.Destroy()
            self.Close(True)


class SettingsPanel(scrolledpanel.ScrolledPanel):
    """
    DESCRIPTION
        contains the layout and structure of the input sheet within a scrolled panel
    VARIABLES
        cdate
        path
        diparameters
        layout
        datapath : default datapath from self.guidict
        distruct
    INCLUDED METHODS
        OnLoadF : method to load scalar data connected to the DI meaurement
        OnCalc : method to calculate the mean declination value
        OnFlip
        OnLoad
        _degminsec2deg : transform between deg:min:sec und decimal deg
        mean_angle
        dataline2wx
    """
    def __init__(self, parent, cdate, path, distation, diparameters, layout, datapath, distruct):
        scrolledpanel.ScrolledPanel.__init__(self, parent, -1, size=(-1, -1))  #size=(950, 750)
        #self.ShowFullScreen(True)
        debug = False
        self.cdate = cdate
        self.path = path
        self.layout = layout
        self.diparameters = diparameters
        self.station = distation

        self.units = ['degree','gon']
        self.choices = ['decimal', 'dms']
        self.ressign = ['inline','opposite']

        self.dichoices = []
        self.didatalists = []
        self.distruct = distruct
        self.datapath = datapath

        self.didict = distruct
        self.diline2datalist(distruct)
        if debug:
            print ("distruct", distruct)
            print ("distruct", self.dichoices)
            print ("SETTING LAYOUT", layout)
        if self.layout.get('double', True) in ["False", False]:
            self.layout['double'] = False
        else:
            self.layout['double'] = True
        if self.layout.get('scalevalue', True) in ["False", False]:
            self.layout['scalevalue'] = False
        else:
            self.layout['scalevalue'] = True

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        self.createWidgets()
        self.SetSizer(self.mainSizer)
        self.mainSizer.Fit(self)
        self.SetupScrolling()
        self.bindControls()

    def createWidgets(self):
        """Create and layout the widgets in the panel"""
        # ##### Header Block (fix - 5 columns)
        # - Load line
        self.memdataComboBox = wx.ComboBox(self, choices=self.dichoices,
                     style=wx.CB_DROPDOWN,size=(160,-1))
        self.loadButton = wx.Button(self,-1,"Open DI data",size=(160,-1))
        self.angleRadioBox = wx.RadioBox(self, label="Display angle as:",
                     choices=self.choices, majorDimension=2, style=wx.RA_SPECIFY_COLS)

        # - Header
        self.HeadLabel = wx.StaticText(self, label="Meta data:",size=(160,-1))
        self.DateLabel = wx.StaticText(self, label="Date:",size=(160,-1))
        self.DatePicker = wxDatePickerCtrl(self, dt=self.cdate,size=(160,-1))
        self.ObserverLabel = wx.StaticText(self, label="Observer:",size=(160,-1))
        self.ObserverTextCtrl = wx.TextCtrl(self, value="Max",size=(160,-1))
        self.CodeLabel = wx.StaticText(self, label="IAGA code:",size=(160,-1))
        self.CodeTextCtrl = wx.TextCtrl(self, value=self.station,size=(160,-1))
        self.TheoLabel = wx.StaticText(self, label="Theodolite:",size=(160,-1))
        self.TheoTextCtrl = wx.TextCtrl(self, value="type_serial_version",size=(160,-1))
        self.FluxLabel = wx.StaticText(self, label="Fluxgate:",size=(160,-1))
        self.FluxTextCtrl = wx.TextCtrl(self, value="type_serial_version",size=(160,-1))
        self.AzimuthLabel = wx.StaticText(self, label="Azimuth:",size=(160,-1))
        self.AzimuthTextCtrl = wx.TextCtrl(self, value="",size=(160,-1))
        self.PillarLabel = wx.StaticText(self, label="Pier:",size=(160,-1))
        self.PillarTextCtrl = wx.TextCtrl(self, value=self.diparameters.get('dipier','P'),size=(160,-1))
        self.UnitLabel = wx.StaticText(self, label="Select Units:",size=(160,-1))
        self.UnitComboBox = wx.ComboBox(self, choices=self.units,
            style=wx.CB_DROPDOWN, value=self.units[0],size=(160,-1))
        self.TempLabel = wx.StaticText(self, label="Temperature [deg C]:",size=(160,-1))
        self.TempTextCtrl = wx.TextCtrl(self, value="",size=(160,-1))
        self.CommentLabel = wx.StaticText(self, label="Optional notes:",size=(160,-1))
        self.CommentTextCtrl = wx.TextCtrl(self, value="",size=(160,80), style = wx.TE_MULTILINE)
        self.ressignRadioBox = wx.RadioBox(self, label="Fluxgate orientation:",
                     choices=self.ressign, majorDimension=2, style=wx.RA_SPECIFY_COLS)

        # - Mire A
        self.AmireLabel = wx.StaticText(self, label="Azimuth:",size=(160,-1))
        self.AmireDownLabel = wx.StaticText(self, label="Sensor Down:",size=(160,-1))
        self.AmireDown1TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.AmireDown2TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.AmireUpLabel = wx.StaticText(self, label="Sensor Up:",size=(160,-1))
        self.AmireUp1TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.AmireUp2TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))

        # - Horizonatl Block
        self.HorizontalLabel = wx.StaticText(self, label="Horizontal:",size=(160,-1))
        self.TimeLabel = wx.StaticText(self, label="Time:",size=(160,-1))
        self.HAngleLabel = wx.StaticText(self, label="Hor. Angle:",size=(160,-1))
        self.VAngleLabel = wx.StaticText(self, label="Ver. Angle:",size=(160,-1))
        self.ResidualLabel = wx.StaticText(self, label="Residual:",size=(160,-1))
        self.EULabel = wx.StaticText(self, label="East(Sensor Up)",size=(160,-1))
        self.EU1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.EU1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.EU1GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,-1))
        self.EU1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.EU2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.EU2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.EU2GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,-1))
        self.EU2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.WULabel = wx.StaticText(self, label="West(Sensor Up)",size=(160,-1))
        self.WU1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.WU1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.WU1GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,-1))
        self.WU1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.WU2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.WU2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.WU2GCTextCtrl = wx.TextCtrl(self, value="90deg/100gon",size=(160,-1))
        self.WU2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.EDLabel = wx.StaticText(self, label="East(Sensor Down)",size=(160,-1))
        self.ED1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.ED1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.ED1GCTextCtrl = wx.TextCtrl(self, value="270deg/300gon",size=(160,-1))
        self.ED1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.ED2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.ED2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.ED2GCTextCtrl = wx.TextCtrl(self, value="270deg/300gon",size=(160,-1))
        self.ED2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.WDLabel = wx.StaticText(self, label="West(Sensor Down)",size=(160,-1))
        self.WD1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.WD1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.WD1GCTextCtrl = wx.TextCtrl(self, value="270deg/300gon",size=(160,-1))
        self.WD1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.WD2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.WD2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.WD2GCTextCtrl = wx.TextCtrl(self, value="270deg/300gon",size=(160,-1))
        self.WD2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.EU1GCTextCtrl.Disable()
        self.EU2GCTextCtrl.Disable()
        self.ED1GCTextCtrl.Disable()
        self.ED2GCTextCtrl.Disable()
        self.WU1GCTextCtrl.Disable()
        self.WU2GCTextCtrl.Disable()
        self.WD1GCTextCtrl.Disable()
        self.WD2GCTextCtrl.Disable()

        # - Mire B
        self.BmireLabel = wx.StaticText(self, label="Azimuth:",size=(160,-1))
        self.BmireDownLabel = wx.StaticText(self, label="Sensor Down:",size=(160,-1))
        self.BmireDown1TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.BmireDown2TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.BmireUpLabel = wx.StaticText(self, label="Sensor Up:",size=(160,-1))
        self.BmireUp1TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.BmireUp2TextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.calcButton = wx.Button(self,-1,"Check horiz. angle",size=(160,-1))

        # - Vertical Block
        self.VerticalLabel = wx.StaticText(self, label="Vertical:",size=(160,-1))
        self.NULabel = wx.StaticText(self, label="North(Sensor Up)",size=(160,-1))
        self.NU1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.NU1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.NU1GCTextCtrl = wx.TextCtrl(self, value="0deg/0gon",size=(160,-1))
        self.NU1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.NU2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.NU2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.NU2GCTextCtrl = wx.TextCtrl(self, value="0deg/0gon",size=(160,-1))
        self.NU2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.SDLabel = wx.StaticText(self, label="South(Sensor Down)",size=(160,-1))
        self.SD1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.SD1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.SD1GCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,-1))
        self.SD1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.SD2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.SD2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.SD2GCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,-1))
        self.SD2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.NDLabel = wx.StaticText(self, label="North(Sensor Down)",size=(160,-1))
        self.ND1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.ND1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.ND1GCTextCtrl = wx.TextCtrl(self, value="0deg/0gon",size=(160,-1))
        self.ND1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.ND2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.ND2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.ND2GCTextCtrl = wx.TextCtrl(self, value="0deg/0gon",size=(160,-1))
        self.ND2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.SULabel = wx.StaticText(self, label="South(Sensor Up)",size=(160,-1))
        self.SU1TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.SU1AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.SU1GCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,-1))
        self.SU1ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.SU2TimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.SU2AngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.SU2GCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,-1))
        self.SU2ResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))
        self.SCLabel = wx.StaticText(self, label="Scale Test (SSU + 0.2 gon)",size=(160,-1))
        self.SCTimeTextCtrl = wx.TextCtrl(self, value="00:00:00",size=(160,-1))
        self.SCAngleTextCtrl = wx.TextCtrl(self, value="0.0000 or 00:00:00.0",size=(160,-1))
        self.SCGCTextCtrl = wx.TextCtrl(self, value="180deg/200gon",size=(160,-1))
        self.SCResidualTextCtrl = wx.TextCtrl(self, value="0.0",size=(160,-1))

        self.NU1GCTextCtrl.Disable()
        self.NU2GCTextCtrl.Disable()
        self.ND1GCTextCtrl.Disable()
        self.ND2GCTextCtrl.Disable()
        self.SU1GCTextCtrl.Disable()
        self.SU2GCTextCtrl.Disable()
        self.SD1GCTextCtrl.Disable()
        self.SD2GCTextCtrl.Disable()
        self.SCGCTextCtrl.Disable()

        if not len(self.dichoices) > 0:
            self.memdataComboBox.Hide()
        else:
            self.memdataComboBox.SetValue(self.dichoices[0])

        if not self.layout.get('scalevalue', True):
            self.SCLabel.Hide()
            self.SCTimeTextCtrl.Hide()
            self.SCGCTextCtrl.Hide()
            self.SCAngleTextCtrl.Hide()
            self.SCResidualTextCtrl.Hide()

        if not self.layout.get('double', True):
            self.AmireDown2TextCtrl.Hide()
            self.AmireUp2TextCtrl.Hide()
            self.BmireUp2TextCtrl.Hide()
            self.BmireDown2TextCtrl.Hide()
            self.EU2TimeTextCtrl.Hide()
            self.EU2AngleTextCtrl.Hide()
            self.EU2GCTextCtrl.Hide()
            self.EU2ResidualTextCtrl.Hide()
            self.WU2TimeTextCtrl.Hide()
            self.WU2AngleTextCtrl.Hide()
            self.WU2GCTextCtrl.Hide()
            self.WU2ResidualTextCtrl.Hide()
            self.ED2TimeTextCtrl.Hide()
            self.ED2AngleTextCtrl.Hide()
            self.ED2GCTextCtrl.Hide()
            self.ED2ResidualTextCtrl.Hide()
            self.WD2TimeTextCtrl.Hide()
            self.WD2AngleTextCtrl.Hide()
            self.WD2GCTextCtrl.Hide()
            self.WD2ResidualTextCtrl.Hide()
            self.NU2TimeTextCtrl.Hide()
            self.NU2GCTextCtrl.Hide()
            self.NU2AngleTextCtrl.Hide()
            self.NU2ResidualTextCtrl.Hide()
            self.SU2TimeTextCtrl.Hide()
            self.SU2GCTextCtrl.Hide()
            self.SU2AngleTextCtrl.Hide()
            self.SU2ResidualTextCtrl.Hide()
            self.ND2TimeTextCtrl.Hide()
            self.ND2GCTextCtrl.Hide()
            self.ND2AngleTextCtrl.Hide()
            self.ND2ResidualTextCtrl.Hide()
            self.SD2TimeTextCtrl.Hide()
            self.SD2GCTextCtrl.Hide()
            self.SD2AngleTextCtrl.Hide()
            self.SD2ResidualTextCtrl.Hide()

        # Add scale check
        self.FLabel = wx.StaticText(self, label="F:",size=(160,-1))
        self.FInstLabel = wx.StaticText(self, label="F instrument:",size=(160,-1))
        self.FInstTextCtrl = wx.TextCtrl(self, value="type_serial_version",size=(160,-1))
        self.FBaseLabel = wx.StaticText(self, label="F base (nT):",size=(160,-1))
        self.FBaseTextCtrl = wx.TextCtrl(self, value="48000",size=(160,-1))
        self.FValsLabel = wx.StaticText(self, label="Time,Value(+Base):",size=(160,-1))
        self.FValsTextCtrl = wx.TextCtrl(self, value="time,value",size=(160,100), style = wx.TE_MULTILINE)
        self.FLoadFromFileButton = wx.Button(self, wx.ID_YES, label="Load F Data",size=(160,-1))

        f = self.VerticalLabel.GetFont()
        newf = wx.Font(12, wx.FONTFAMILY_DEFAULT, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD)
        #newf = wx.Font(14, wx.DECORATIVE, wx.ITALIC, wx.BOLD)
        self.VerticalLabel.SetFont(newf)
        self.HorizontalLabel.SetFont(newf)
        self.AmireLabel.SetFont(newf)
        self.BmireLabel.SetFont(newf)
        self.HeadLabel.SetFont(newf)
        self.FLabel.SetFont(newf)

        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((160, 0), noOptions)

        # Load elements
        #boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        contlst=[emptySpace]
        contlst.append(emptySpace)
        contlst.append((self.loadButton, dict(flag=wx.ALIGN_CENTER)))
        contlst.append(emptySpace)
        contlst.append((self.angleRadioBox, noOptions))

        # Header elements
        contlst.append((self.HeadLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.DateLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.ObserverLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.CodeLabel, noOptions))
        contlst.append((self.DatePicker, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.ObserverTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.CodeTextCtrl, expandOption))
        contlst.append((self.TheoLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.FluxLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.AzimuthLabel, noOptions))
        contlst.append((self.TheoTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.FluxTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.AzimuthTextCtrl, expandOption))
        contlst.append((self.PillarLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.UnitLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.TempLabel, noOptions))
        contlst.append((self.PillarTextCtrl, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.UnitComboBox, expandOption))
        contlst.append(emptySpace)
        contlst.append((self.TempTextCtrl, expandOption))
        contlst.append((self.CommentLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.CommentTextCtrl, noOptions))
        contlst.append(emptySpace)
        contlst.append((self.ressignRadioBox, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)

        # Mire elements
        contlst.append((self.AmireLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        blMU = []
        blMU.append((self.AmireUpLabel, noOptions))
        blMU.append((self.AmireUp1TextCtrl, expandOption))
        if self.layout.get('double', True):
            blMU.append((self.AmireUp2TextCtrl, expandOption))
        else:
            blMU.append(emptySpace)
        blMU.append(emptySpace)
        blMU.append(emptySpace)
        blMD = []
        blMD.append((self.AmireDownLabel, noOptions))
        blMD.append((self.AmireDown1TextCtrl, expandOption))
        if self.layout.get('double', True):
            blMD.append((self.AmireDown2TextCtrl, expandOption))
        else:
            blMD.append(emptySpace)
        blMD.append(emptySpace)
        blMD.append(emptySpace)
        for el in self.layout['order'][0:2]:
            contlst.extend(eval('bl'+str(el)))

        miorder = self.layout['order'][0:2]
        if miorder[0] == 'MU':  # default is MD, MU
            self.AmireUp2TextCtrl.MoveBeforeInTabOrder(self.AmireDown1TextCtrl)
            self.AmireUp1TextCtrl.MoveBeforeInTabOrder(self.AmireUp2TextCtrl)

        blEU = []
        blEU.append((self.EULabel, noOptions))
        blEU.append((self.EU1TimeTextCtrl, expandOption))
        blEU.append((self.EU1AngleTextCtrl, expandOption))
        blEU.append((self.EU1GCTextCtrl, expandOption))
        blEU.append((self.EU1ResidualTextCtrl, expandOption))
        if self.layout.get('double', True):
            blEU.append(emptySpace)
            blEU.append((self.EU2TimeTextCtrl, expandOption))
            blEU.append((self.EU2AngleTextCtrl, expandOption))
            blEU.append((self.EU2GCTextCtrl, expandOption))
            blEU.append((self.EU2ResidualTextCtrl, expandOption))
        blWU = []
        blWU.append((self.WULabel, noOptions))
        blWU.append((self.WU1TimeTextCtrl, expandOption))
        blWU.append((self.WU1AngleTextCtrl, expandOption))
        blWU.append((self.WU1GCTextCtrl, expandOption))
        blWU.append((self.WU1ResidualTextCtrl, expandOption))
        if self.layout.get('double', True):
            blWU.append(emptySpace)
            blWU.append((self.WU2TimeTextCtrl, expandOption))
            blWU.append((self.WU2AngleTextCtrl, expandOption))
            blWU.append((self.WU2GCTextCtrl, expandOption))
            blWU.append((self.WU2ResidualTextCtrl, expandOption))
        blED = []
        blED.append((self.EDLabel, noOptions))
        blED.append((self.ED1TimeTextCtrl, expandOption))
        blED.append((self.ED1AngleTextCtrl, expandOption))
        blED.append((self.ED1GCTextCtrl, expandOption))
        blED.append((self.ED1ResidualTextCtrl, expandOption))
        if self.layout.get('double', True):
            blED.append(emptySpace)
            blED.append((self.ED2TimeTextCtrl, expandOption))
            blED.append((self.ED2AngleTextCtrl, expandOption))
            blED.append((self.ED2GCTextCtrl, expandOption))
            blED.append((self.ED2ResidualTextCtrl, expandOption))
        blWD = []
        blWD.append((self.WDLabel, noOptions))
        blWD.append((self.WD1TimeTextCtrl, expandOption))
        blWD.append((self.WD1AngleTextCtrl, expandOption))
        blWD.append((self.WD1GCTextCtrl, expandOption))
        blWD.append((self.WD1ResidualTextCtrl, expandOption))
        if self.layout.get('double', True):
            blWD.append(emptySpace)
            blWD.append((self.WD2TimeTextCtrl, expandOption))
            blWD.append((self.WD2AngleTextCtrl, expandOption))
            blWD.append((self.WD2GCTextCtrl, expandOption))
            blWD.append((self.WD2ResidualTextCtrl, expandOption))
        contlst.append((self.HorizontalLabel, noOptions))
        contlst.append((self.TimeLabel, noOptions))
        contlst.append((self.HAngleLabel, noOptions))
        contlst.append((self.VAngleLabel, noOptions))
        contlst.append((self.ResidualLabel, noOptions))
        for el in self.layout['order'][2:6]:
            contlst.extend(eval('bl'+str(el)))

        hororder = self.layout['order'][2:6]  # default is EU,WU,ED,WD
        if not hororder == ['EU','WU','ED','WD']:
             prevel = hororder[0]
             for idx, el in enumerate(reversed(hororder)):  # example WD,ED,WU,EU and EU,WD,ED,WU
                 #print ("Test", el,prevel, idx, hororder)
                 if idx > 0:
                     exec("self.{}2ResidualTextCtrl.MoveBeforeInTabOrder(self.{}1TimeTextCtrl)".format(el,prevel))
                     exec("self.{}2GCTextCtrl.MoveBeforeInTabOrder(self.{}2ResidualTextCtrl)".format(el,el))
                     exec("self.{}2AngleTextCtrl.MoveBeforeInTabOrder(self.{}2GCTextCtrl)".format(el,el))
                     exec("self.{}2TimeTextCtrl.MoveBeforeInTabOrder(self.{}2AngleTextCtrl)".format(el,el))
                     exec("self.{}1ResidualTextCtrl.MoveBeforeInTabOrder(self.{}2TimeTextCtrl)".format(el,el))
                     exec("self.{}1GCTextCtrl.MoveBeforeInTabOrder(self.{}1ResidualTextCtrl)".format(el,el))
                     exec("self.{}1AngleTextCtrl.MoveBeforeInTabOrder(self.{}1GCTextCtrl)".format(el,el))
                     exec("self.{}1TimeTextCtrl.MoveBeforeInTabOrder(self.{}1AngleTextCtrl)".format(el,el))
                 prevel = el


        # Mire elements
        contlst.append((self.BmireLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        blMU = []
        blMU.append((self.BmireUpLabel, noOptions))
        blMU.append((self.BmireUp1TextCtrl, expandOption))
        if self.layout.get('double', True):
            blMU.append((self.BmireUp2TextCtrl, expandOption))
        else:
            blMU.append(emptySpace)
        blMU.append(emptySpace)
        blMU.append((self.calcButton, expandOption))
        blMD = []
        blMD.append((self.BmireDownLabel, noOptions))
        blMD.append((self.BmireDown1TextCtrl, expandOption))
        if self.layout.get('double', True):
            blMD.append((self.BmireDown2TextCtrl, expandOption))
        else:
            blMD.append(emptySpace)
        blMD.append(emptySpace)
        blMD.append(emptySpace)
        for el in self.layout['order'][0:2]:
            contlst.extend(eval('bl'+str(el)))

        miorder = self.layout['order'][0:2]
        if miorder[0] == 'MU':  # default is MD, MU
            self.BmireUp2TextCtrl.MoveBeforeInTabOrder(self.BmireDown1TextCtrl)
            self.BmireUp1TextCtrl.MoveBeforeInTabOrder(self.BmireUp2TextCtrl)

        # Mire elements
        blNU = []
        blNU.append((self.NULabel, noOptions))
        blNU.append((self.NU1TimeTextCtrl, expandOption))
        blNU.append((self.NU1GCTextCtrl, expandOption))
        blNU.append((self.NU1AngleTextCtrl, expandOption))
        blNU.append((self.NU1ResidualTextCtrl, expandOption))
        if self.layout.get('double', True):
            blNU.append(emptySpace)
            blNU.append((self.NU2TimeTextCtrl, expandOption))
            blNU.append((self.NU2GCTextCtrl, expandOption))
            blNU.append((self.NU2AngleTextCtrl, expandOption))
            blNU.append((self.NU2ResidualTextCtrl, expandOption))
        blSU = []
        blSU.append((self.SULabel, noOptions))
        blSU.append((self.SU1TimeTextCtrl, expandOption))
        blSU.append((self.SU1GCTextCtrl, expandOption))
        blSU.append((self.SU1AngleTextCtrl, expandOption))
        blSU.append((self.SU1ResidualTextCtrl, expandOption))
        if self.layout.get('double', True):
            blSU.append(emptySpace)
            blSU.append((self.SU2TimeTextCtrl, expandOption))
            blSU.append((self.SU2GCTextCtrl, expandOption))
            blSU.append((self.SU2AngleTextCtrl, expandOption))
            blSU.append((self.SU2ResidualTextCtrl, expandOption))
        blND = []
        blND.append((self.NDLabel, noOptions))
        blND.append((self.ND1TimeTextCtrl, expandOption))
        blND.append((self.ND1GCTextCtrl, expandOption))
        blND.append((self.ND1AngleTextCtrl, expandOption))
        blND.append((self.ND1ResidualTextCtrl, expandOption))
        if self.layout.get('double', True):
            blND.append(emptySpace)
            blND.append((self.ND2TimeTextCtrl, expandOption))
            blND.append((self.ND2GCTextCtrl, expandOption))
            blND.append((self.ND2AngleTextCtrl, expandOption))
            blND.append((self.ND2ResidualTextCtrl, expandOption))
        blSD = []
        blSD.append((self.SDLabel, noOptions))
        blSD.append((self.SD1TimeTextCtrl, expandOption))
        blSD.append((self.SD1GCTextCtrl, expandOption))
        blSD.append((self.SD1AngleTextCtrl, expandOption))
        blSD.append((self.SD1ResidualTextCtrl, expandOption))
        if self.layout.get('double', True):
            blSD.append(emptySpace)
            blSD.append((self.SD2TimeTextCtrl, expandOption))
            blSD.append((self.SD2GCTextCtrl, expandOption))
            blSD.append((self.SD2AngleTextCtrl, expandOption))
            blSD.append((self.SD2ResidualTextCtrl, expandOption))
        #contlst=[]
        contlst.append((self.VerticalLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        for el in self.layout['order'][6:10]:
            contlst.extend(eval('bl'+str(el)))

        # Tab order
        verorder = self.layout['order'][6:10]  # default is NU,SD,ND,SU
        if not verorder == ['NU','SD','ND','SU']:
             prevel = verorder[0]
             for idx, el in enumerate(reversed(verorder)):
                 #print ("Test", el,prevel, idx, hororder)
                 if idx > 0:
                     exec("self.{}2ResidualTextCtrl.MoveBeforeInTabOrder(self.{}1TimeTextCtrl)".format(el,prevel))
                     exec("self.{}2GCTextCtrl.MoveBeforeInTabOrder(self.{}2ResidualTextCtrl)".format(el,el))
                     exec("self.{}2AngleTextCtrl.MoveBeforeInTabOrder(self.{}2GCTextCtrl)".format(el,el))
                     exec("self.{}2TimeTextCtrl.MoveBeforeInTabOrder(self.{}2AngleTextCtrl)".format(el,el))
                     exec("self.{}1ResidualTextCtrl.MoveBeforeInTabOrder(self.{}2TimeTextCtrl)".format(el,el))
                     exec("self.{}1GCTextCtrl.MoveBeforeInTabOrder(self.{}1ResidualTextCtrl)".format(el,el))
                     exec("self.{}1AngleTextCtrl.MoveBeforeInTabOrder(self.{}1GCTextCtrl)".format(el,el))
                     exec("self.{}1TimeTextCtrl.MoveBeforeInTabOrder(self.{}1AngleTextCtrl)".format(el,el))
                 prevel = el


        # Scale test
        if self.layout.get('scalevalue', True):
            contlst.append((self.SCLabel, noOptions))
            contlst.append((self.SCTimeTextCtrl, expandOption))
            contlst.append((self.SCGCTextCtrl, expandOption))
            contlst.append((self.SCAngleTextCtrl, expandOption))
            contlst.append((self.SCResidualTextCtrl, expandOption))

        contlst.append((self.FLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.FInstLabel, noOptions))
        contlst.append((self.FBaseLabel, noOptions))
        contlst.append((self.FLoadFromFileButton, expandOption))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.FInstTextCtrl, noOptions))
        contlst.append((self.FBaseTextCtrl, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.FValsLabel, noOptions))
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append(emptySpace)
        contlst.append((self.FValsTextCtrl, noOptions))
        contlst.append(emptySpace)

        # A GridSizer will contain the other controls:
        cols = 5
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        self.mainSizer.Add(gridSizer, 0, wx.EXPAND)

    def bindControls(self):
        self.loadButton.Bind(wx.EVT_BUTTON, self.OnLoad)
        self.Bind(wx.EVT_RADIOBOX, self.OnFlip, self.angleRadioBox)
        self.calcButton.Bind(wx.EVT_BUTTON, self.OnCalc)
        self.FLoadFromFileButton.Bind(wx.EVT_BUTTON, self.OnLoadF)
        self.memdataComboBox.Bind(wx.EVT_COMBOBOX, self.OnUpdateCombo)

    def OnLoadF(self, e):

        stream = DataStream()
        dlg = wx.FileDialog(self, "Choose a data file with", self.datapath, "", "*.*")
        path = ''
        if dlg.ShowModal() == wx.ID_OK:
            path = dlg.GetPath()
        if path == '':
            return

        def wxdate2pydate(date):
            assert isinstance(date, wx.DateTime)
            if date.IsValid():
                ymd = list(map(int, date.FormatISODate().split('-')))
                return datetime(*ymd)
            else:
                return None

        datet = wxdate2pydate(self.DatePicker.GetValue())
        maxdate = datet+timedelta(days=1)
        mintime = wx.DateTime.FromDMY(datet.day, datet.month - 1, datet.year)
        maxtime = wx.DateTime.FromDMY(maxdate.day, maxdate.month - 1, maxdate.year)

        extension = '*.*'
        dlg = LoadDataDialog(None, title='Select timerange:',mintime=mintime,maxtime=maxtime, extension=extension)
        if dlg.ShowModal() == wx.ID_OK:
            stday = dlg.startDatePicker.GetValue()
            sttime = dlg.startTimePicker.GetValue()
            enday = dlg.endDatePicker.GetValue()
            entime = dlg.endTimePicker.GetValue()
            ext = dlg.fileExt.GetValue()

            sd = datetime.fromtimestamp(stday.GetTicks())
            ed = datetime.fromtimestamp(enday.GetTicks())
            st = sd.strftime("%Y-%m-%d") + " " + sttime
            start = datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
            et = ed.strftime("%Y-%m-%d") + " " + entime
            end = datetime.strptime(et, "%Y-%m-%d %H:%M:%S")

            if isinstance(path, basestring):
                if not path=='':
                    try:
                        stream = read(path_or_url=path, starttime=start, endtime=end)
                    except:
                        stream = DataStream()
            """ no database option here
            else:
                # assume Database
                try:
                    stream = db.read(path[1], starttime=start, endtime=end)
                except:
                    pass
            """
        dlg.Destroy()

        #self.changeStatusbar("Ready")
        if len(stream) > 0:
            dataid = stream.header.get('DataID')
            if not dataid:
                dataid = stream.header.get('SensorID')
                if not dataid:
                    dataid = 'NoF***ingInstrumentName'
            self.FInstTextCtrl.SetValue(dataid)
            basevalue = 0.0
            self.FBaseTextCtrl.SetValue(str(basevalue))
            posf = KEYLIST.index('f')
            ftext = ''
            for idx, elem in enumerate(stream.ndarray[0]):
                time = elem.strftime("%H:%M:%S")
                ftext += "{},{:.2f}\n".format(time,stream.ndarray[posf][idx])
            self.FValsTextCtrl.SetValue(ftext)

    def _degminsec2deg(self, string, back='decimal'):
        """
        DESCRIPTION
            checks input string and returns either deg: or deg.
        PARAMETER
            string (string):   a deg:min:sec of deg.decimal value
            back   (float) :   either decimal or dms
        """
        def _decdeg2dms(dd):
            is_positive = dd >= 0
            dd = abs(dd)
            minutes,seconds = divmod(dd*3600,60)
            degrees,minutes = divmod(minutes,60)
            degrees = degrees if is_positive else -degrees
            return [int(np.round(degrees,0)),int(np.round(minutes,0)),np.round(seconds,2)]


        string = str(string)
        if string in ['','0.0000 or 00:00:00.0']:
            return string

        if string.find(':') > 0:
            val = 0.
            numlist = string.split(':')
            for idx, ele in enumerate(numlist):
                add = float(ele)/float(60.**idx)
                val += float(add)
        else:
            if string.find(',') > 0:
                string = string.replace(',','.')
            try:
                val = float(string)
            except:
                val = 999.0

        if not val >= -180 and not val <= 360:
            return 999.

        if back == 'dms':
            return ":".join(map(str,_decdeg2dms(val)))
        else:
            return str(np.round(val,6))

    def mean_angle(self, deg):   # rosettacode
        from cmath import rect, phase
        from math import radians, degrees
        # spilt up in first 4 and second four an d then average those
        # why: a poor mean of angles is definitle not correct (i.e. [359,0,1] would result in 120 instead of 0
        # treatment as unit vectors as done here however does not seem to be perfectly correct either if angles strongly
        # vary
        deg1 = deg[:int(len(deg)/2)] # extract 90degree measurements
        deg2 = deg[-int(len(deg)/2):] # extract 270degree measurements
        m1 = degrees(phase(sum(rect(1, radians(d)) for d in deg1)/len(deg1)))
        m2 = degrees(phase(sum(rect(1, radians(d)) for d in deg2)/len(deg2)))
        fin = [m1,m2]
        return degrees(phase(sum(rect(1, radians(d)) for d in fin)/len(fin)))

    def OnCalc(self, e):
        # Calculate angle if enough values are present:

        message = "Please fill in all horizontal measurements first!\n"
        #message = "You need to specify a unique Data ID\nfor which meta information is obtained.\n"
        vallst = []
        for el in self.layout['order'][2:6]:
            for num in ['1','2']:
                valel = eval("self.{}{}AngleTextCtrl.GetValue()".format(el,num))
                try:
                    valel = float(self._degminsec2deg(valel))
                    if not valel == 0 and not np.isnan(valel):
                        vallst.append(valel)
                except:
                    pass

        deg = self.UnitComboBox.GetValue()
        if deg in ['degree','deg']:
            meanangle = (self.mean_angle(vallst))
        else:
            vallst = [el*360./400. for el in vallst]
            meanangle = np.mean(np.asarray(vallst))*400./360.
        if len(vallst) >= 4:
             typus = self.angleRadioBox.GetStringSelection()
             message = "\n {} {}".format(self._degminsec2deg(meanangle,back=typus),deg)
        dlg = wx.MessageDialog(self, "Average horizontal angle:\n"+message, "Horizontal angle", wx.OK|wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()


    def OnFlip(self, e):
        # fields to be converted
        checklist = []
        for comp in ['EU','WU','ED','WD','NU','SD','ND','SU']:
            for i in ['1','2']:
                checklist.append('self.'+comp+i+'AngleTextCtrl')
        for comp in ['A','B']:
            for i in ['Up1','Up2','Down1','Down2']:
                checklist.append('self.'+comp+'mire'+i+'TextCtrl')
        checklist.append('self.SCAngleTextCtrl')
        checklist.append('self.AzimuthTextCtrl')
        #print (checklist)
        # get aim (dms or decimal)
        aim = self.angleRadioBox.GetStringSelection()
        # perform calc for each input
        for el in checklist:
            value = eval(el+'.GetValue()')
            newvalue = self._degminsec2deg(value,aim)
            eval(el+'.SetValue(newvalue)')

    def OnLoad(self, e):
        def _readDI(path):
            datalist = []
            with open(path, "rt", encoding="latin-1") as fh:
                for line in fh:
                    datalist.append(line.strip('\n'))
            return datalist

        def _getDI():
            datalist = []
            return datalist

        datalist = []
        iagacode = 'undefined'

        # If Open DI data then loadfile=True
        loadfile = True

        if loadfile:
            dlg = wx.FileDialog(self, "Choose a DI raw data file", self.path, "", "*.*")
            if dlg.ShowModal() == wx.ID_OK:
                path = dlg.GetPath()
                try:
                    iagacode = os.path.split(path)[1].split('.')[0].split('_')[-1]
                except:
                    iagacode = 'undefined'
                datalist = _readDI(path)
        #elif loadDB:
        #    datalist = _getDI()

        if not len(iagacode) == 3:
            iagacode = self.station

        if len(datalist) > 0:
            self.angleRadioBox.SetStringSelection("decimal")
            self.datalist2wx(datalist, iagacode)


    def datalist2wx(self,datalist,iagacode):
            # datalist looks like:
            # string list with lines:
            #['# MagPy Absolutes\n', '# Abs-Observer: Leichter\n', '# Abs-Theodolite: T10B_0619H154167_07-2011\n', '# Abs-TheoUnit: deg\n', '# Abs-FGSensor: MAG01H_SerialSensor_SerialElectronic_07-2011\n', '# Abs-AzimuthMark: 180.1044444\n', '# Abs-Pillar: A4\n', '# Abs-Scalar: /\n', '# Abs-Temperature: 6.7C\n', '# Abs-InputDate: 2016-01-26\n', 'Miren:\n', '0.099166666666667  0.098055555555556  180.09916666667  180.09916666667  0.098055555555556  0.096666666666667  180.09805555556  180.09805555556\n', 'Positions:\n', '2016-01-21_13:22:00  93.870555555556  90  1.1\n', '2016-01-21_13:22:30  93.870555555556  90  1.8\n', '2016-01-21_13:27:00  273.85666666667  90  0.1\n', '2016-01-21_13:27:30  273.85666666667  90  0.2\n', '2016-01-21_13:25:30  273.85666666667  270  0.3\n', '2016-01-21_13:26:00  273.85666666667  270  -0.6\n', '2016-01-21_13:24:00  93.845555555556  270  -0.2\n', '2016-01-21_13:24:30  93.845555555556  270  0.4\n', '2016-01-21_13:39:30  0  64.340555555556  -0.3\n', '2016-01-21_13:40:00  0  64.340555555556  0.1\n', '2016-01-21_13:38:00  0  244.34055555556  0\n', '2016-01-21_13:38:30  0  244.34055555556  -0.4\n', '2016-01-21_13:36:00  180  295.67055555556  1.1\n', '2016-01-21_13:36:30  180  295.67055555556  1.2\n', '2016-01-21_13:34:30  180  115.66916666667  0.3\n', '2016-01-21_13:35:00  180  115.66916666667  0.9\n', '2016-01-21_13:34:30  180  115.66916666667  0\n', 'PPM:\n', 'Result:\n']

            mdate = None
            na = ''
            poscnt = 0
            poslst = ['EU','EU','WU','WU','ED','ED','WD','WD','NU','NU','SD','SD','ND','ND','SU','SU']
            posord = ['1','2','1','2','1','2','1','2','1','2','1','2','1','2','1','2']
            ffield = []
            self.CodeTextCtrl.SetValue(iagacode)
            for line in datalist:
                #print ("Here", line)
                numelements = len(line.split())
                if line.isspace():
                    # blank line
                    pass
                elif line.startswith('#'):
                    # header
                    line = line.strip('\n')
                    headline = line.split(':')
                    #self.CodeTextCtrl.SetValue()
                    #self.DatePicker = wxDatePickerCtrl(self, dt=self.cdate,size=(160,30))

                    if headline[0] == ('# Abs-Observer'):
                        self.ObserverTextCtrl.SetValue(headline[1].strip())
                    if headline[0] == ('# Abs-Theodolite'):
                        self.TheoTextCtrl.SetValue(headline[1].replace(', ','_').strip().replace(' ','_'))
                    if headline[0] == ('# Abs-TheoUnit'):
                        self.UnitComboBox.SetStringSelection(headline[1].strip().replace('deg','degree'))
                    if headline[0] == ('# Abs-FGSensor'):
                        self.FluxTextCtrl.SetValue(headline[1].strip().replace(' ','_'))
                    if headline[0] == ('# Abs-AzimuthMark'):
                        self.AzimuthTextCtrl.SetValue(headline[1].strip())
                    if headline[0] == ('# Abs-Pillar'):
                        self.PillarTextCtrl.SetValue(headline[1].strip())
                    if headline[0] == ('# Abs-Scalar'):
                        self.FInstTextCtrl.SetValue(headline[1].strip())
                    if headline[0] == ('# Abs-Notes'):
                        self.CommentTextCtrl.SetValue(headline[1].strip())
                        #datalist.append(headline[1].strip())
                    #if headline[0] == ('# Abs-DeltaF'):
                    #    datalist.append(headline[1].strip())
                    if headline[0] == ('# Abs-Temperature'):
                        self.TempTextCtrl.SetValue(headline[1].strip().strip('C'))
                elif numelements == 8:
                    # Miren mesurements
                    mirestr = line.split()
                    self.AmireUp1TextCtrl.SetValue(mirestr[0])
                    self.AmireUp2TextCtrl.SetValue(mirestr[1])
                    self.AmireDown1TextCtrl.SetValue(mirestr[2])
                    self.AmireDown2TextCtrl.SetValue(mirestr[3])
                    self.BmireUp1TextCtrl.SetValue(mirestr[4])
                    self.BmireUp2TextCtrl.SetValue(mirestr[5])
                    self.BmireDown1TextCtrl.SetValue(mirestr[6])
                    self.BmireDown2TextCtrl.SetValue(mirestr[7])
                elif numelements == 4:
                    # Position mesurements
                    posstr = line.split()
                    #print ("Cnt", poscnt, posstr)
                    lineel = ['Time','GC','Angle','Residual']
                    if poscnt == 16:
                        na = 'SC'
                    elif poscnt < 16:
                        comp = poslst[poscnt]
                        na = comp+posord[poscnt]
                    if poscnt < 8:
                        lineel = ['Time','Angle','GC','Residual']
                    for idx,el in enumerate(posstr):
                        col = lineel[idx]
                        #print ("Here", el, col)
                        if col == 'Time':
                            try:
                                mdate = datetime.strptime(el,"%Y-%m-%d_%H:%M:%S")
                                el = mdate.strftime("%H:%M:%S")
                            except:
                                el = '00:00:00'
                        eval('self.'+na+col+'TextCtrl.SetValue(el)')
                    poscnt = poscnt+1
                elif numelements == 2:
                    # Intensity mesurements
                    fstr = line.split()
                    try:
                        tmmm = datetime.strptime(fstr[0], "%Y-%m-%d_%H:%M:%S")
                        el = tmmm.strftime("%H:%M:%S")
                    except:
                        el = '00:00:00'
                    try:
                        f = fstr[1].replace(',','.')
                    except:
                        f = 0.0
                    fline = ','.join([el,f])+'\n'
                    ffield.append(fline)
                else:
                    #print line
                    pass
            try:
                new = wx.DateTime.FromDMY(day=mdate.day,month=mdate.month-1,year=mdate.year)
                self.DatePicker.SetValue(wx.DateTime.FromDMY(day=mdate.day,month=mdate.month-1,year=mdate.year))
            except:
                self.DatePicker.SetValue(wx.DateTimeFromDMY(day=mdate.day,month=mdate.month-1,year=mdate.year))

            if len(ffield) > 0:
                self.FValsTextCtrl.SetValue("".join(ffield))
                self.FBaseTextCtrl.SetValue("0.0")
            else:
                self.FInstTextCtrl.SetValue("type_serial_version")
                self.FValsTextCtrl.SetValue("time,value")
                self.FBaseTextCtrl.SetValue("")


    def diline2datalist(self, didict):
        """
        DESCRIPTION:
           converts a diline structure into a datalist string used for displaying and saving
           and a name list for the ComboBox selection
        """
        if didict == {}:
            return
        for struc in didict.get('absdata'):
            datalist = struc.get_data_list()
            #try:
            positionsindex = datalist.index('Positions:\n')
            datadate = datalist[positionsindex+1].split()[0]
            dataname = "{}_{}_{}".format(datadate.replace(':','-'), struc.pier, didict.get('station'))
            self.dichoices.append(dataname)
            self.didatalists.append(datalist)

    def OnUpdateCombo(self, event):
        # Get choice
        idx = self.dichoices.index(self.memdataComboBox.GetValue())
        # Get corresponding datalist
        datalist = self.didatalists[idx]
        # Get iagacode
        iagacode = self.didict.get('station')
        # Update Screen
        if len(datalist) > 0:
            self.angleRadioBox.SetStringSelection("decimal")
            #print ("Datalist", datalist)
            self.datalist2wx(datalist, iagacode)



# ##################################################################################################################
# ####    Monitor  Panel                                   #########################################################
# ##################################################################################################################

class LiveSelectMARTASDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select table for MARCOS monitoring
    """

    def __init__(self, parent, title, analysisdict):
        super(LiveSelectMARTASDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.favoritemartas = analysisdict.get('favoritemartas')
        self.favmartas = [key for key in self.favoritemartas]
        self.qoslist = ['0','1','2']
        self.stationid = analysisdict.get('defaultstation')
        if not self.favmartas or not len(self.favmartas) > 0:
            self.favoritemartas['example'] = {'address' : 'www.example.com',
                                    'scantime' : 20,
                                    'qos' : 1,
                                    'topic' : 'all',
                                    'port' : 1883,
                                    'auth' : True,
                                    'user' : 'cobs'}
            self.favmartas = ['example']
        self.favorite = self.favoritemartas.get(self.favmartas[0])
        self.selectedmartas = self.favmartas[0]
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.qosLabel = wx.StaticText(self, label="Quality of service:",size=(160,30))
        self.qosComboBox = wx.ComboBox(self, choices=self.qoslist,
                       style=wx.CB_DROPDOWN, value=str(self.favorite.get('qos')),size=(160,-1))
        self.protocolLabel = wx.StaticText(self, label="Protocol:",size=(160,30))
        self.protocolTextCtrl = wx.TextCtrl(self, value='MQTT', size=(160,30))
        self.martasLabel = wx.StaticText(self, label="Select MARTAS:",size=(160,30))
        self.martasComboBox = wx.ComboBox(self, choices=self.favmartas,
                       style=wx.CB_DROPDOWN, value=self.selectedmartas,size=(160,-1))
        self.dropButton = wx.Button(self, label='Remove from favorites',size=(160,30))
        self.newButton = wx.Button(self, label='Add to favorites',size=(160,30))
        self.addressLabel = wx.StaticText(self, label="URL/IP:",size=(160,30))
        self.addressTextCtrl = wx.TextCtrl(self, value=self.favorite.get('address'),size=(160,30))
        self.portLabel = wx.StaticText(self, label="Communication port:",size=(160,30))
        self.portTextCtrl = wx.TextCtrl(self, value=str(self.favorite.get('port')),size=(160,30))
        self.topicLabel = wx.StaticText(self, label="Topic:", size=(160,30)) #(will extended by /#)
        self.topicTextCtrl = wx.TextCtrl(self, value=self.favorite.get('topic','all'), size=(160,30))
        self.scanLabel = wx.StaticText(self, label="Scantime (sec):",size=(160,30))
        self.scanTextCtrl = wx.TextCtrl(self, value=str(self.favorite.get('scantime',20)),size=(160,30))
        self.userLabel = wx.StaticText(self, label="*User:",size=(160,30))
        self.userTextCtrl = wx.TextCtrl(self, value=self.favorite.get('user',''),size=(160,30))
        self.pwdLabel = wx.StaticText(self, label="*Password:",size=(160,30))
        self.pwdTextCtrl = wx.TextCtrl(self, value="",size=(160,30),style=wx.TE_PASSWORD)
        self.authLabel = wx.StaticText(self, label="* if authentication is required",size=(160,30))
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

        self.protocolTextCtrl.Disable()

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.martasLabel, noOptions),
                 (self.martasComboBox, expandOption),
                 (self.dropButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.newButton, dict(flag=wx.ALIGN_CENTER)),
                    (self.protocolLabel, noOptions),
                    (self.protocolTextCtrl, expandOption),
                 (self.addressLabel, noOptions),
                 (self.addressTextCtrl, expandOption),
                 (self.portLabel, noOptions),
                 (self.portTextCtrl, expandOption),
                 (self.topicLabel, noOptions),
                 (self.topicTextCtrl, expandOption),
                 (self.qosLabel, noOptions),
                 (self.qosComboBox, expandOption),
                 (self.scanLabel, noOptions),
                 (self.scanTextCtrl, expandOption),
                 (self.userLabel, noOptions),
                 (self.userTextCtrl, expandOption),
                 (self.pwdLabel, noOptions),
                 (self.pwdTextCtrl, expandOption),
                 (self.authLabel, noOptions),
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.newButton.Bind(wx.EVT_BUTTON, self.OnNew)
        self.dropButton.Bind(wx.EVT_BUTTON, self.OnRemove)
        self.Bind(wx.EVT_COMBOBOX, self.OnUpdate, self.martasComboBox)


    def OnNew(self, e):
        # get current value in dropdown and append it to

        auth = False
        user = self.userTextCtrl.GetValue()
        if user:
            auth = True
        newcontent = {'address': self.addressTextCtrl.GetValue(),
                      'scantime':  int(self.scanTextCtrl.GetValue()),
                      'qos': int(self.qosComboBox.GetValue()),
                      'topic': self.topicTextCtrl.GetValue(),
                      'port': int(self.portTextCtrl.GetValue()),
                      'auth': auth,
                      'user': user,
                      'password': self.pwdTextCtrl.GetValue()
                      }
        # Select a name for the new input
        #existingnames =
        newname = self.addressTextCtrl.GetValue()
        dlg = LiveGetMARTASNameDialog(None, title='Input for favorite MARTAS list', address=newname, existing_names=self.favmartas)
        if dlg.ShowModal() == wx.ID_OK:
            newname = dlg.nameTextCtrl.GetValue()
            if newname in self.favmartas:
                print ("name already existing  - adding some random numnber to it")
                # add some random number at the end
                rannum = np.random.randint(100,999)
                newname = "{}_{}".format(newname,rannum)
            self.favoritemartas[newname] = newcontent


    def OnRemove(self, e):
        """
        DECSRIPTION
            Will remove MARTAS inputs from favorite list.
            'example' cannot be removed.
        :param e:
        :return:
        """
        if self.selectedmartas == 'example':
            print ("Cannot remove dummy example")
            dlg = wx.MessageDialog(self, "Cannot remove the example!\n",
                                   "Not possible", wx.OK | wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()

        else:
            cleanedmartasdict = {}
            for key in self.favoritemartas:
                if not key == self.selectedmartas:
                    cleanedmartasdict[key] = self.favoritemartas[key]
            self.martasComboBox.SetValue('example')
            self.favmartas = [el for el in cleanedmartasdict]
            #self.martasComboBox.SetChoices(self.favmartas)
            self.favoritemartas = cleanedmartasdict.copy()
            self.favorite = self.favoritemartas.get('example')
            self.qosComboBox.SetValue(str(self.favorite.get('qos')))
            self.portTextCtrl.SetValue(str(self.favorite.get('port')))
            self.topicTextCtrl.SetValue(self.favorite.get('topic','all'))
            self.userTextCtrl.SetValue(self.favorite.get('user',''))
            self.pwdTextCtrl.SetValue(self.favorite.get('password',''))
            self.addressTextCtrl.SetValue(self.favorite.get('address',''))
            self.scanTextCtrl.SetValue(str(self.favorite.get('scantime','')))


    def OnUpdate(self, e):
        self.selectedmartas = self.martasComboBox.GetStringSelection()
        self.favorite = self.favoritemartas.get(self.selectedmartas)
        self.qosComboBox.SetValue(str(self.favorite.get('qos')))
        self.portTextCtrl.SetValue(str(self.favorite.get('port')))
        self.topicTextCtrl.SetValue(self.favorite.get('topic','all'))
        self.userTextCtrl.SetValue(self.favorite.get('user',''))
        self.pwdTextCtrl.SetValue(self.favorite.get('password',''))
        self.addressTextCtrl.SetValue(self.favorite.get('address',''))
        self.scanTextCtrl.SetValue(str(self.favorite.get('scantime','')))


class LiveGetMARTASNameDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select a name for a new MARTAS input
    USED BY:
        LiveSelectMARTASDialog in dialogclasses
    RETURNS:
        a valid, yet non-existing name for the MARTAS favorite list
    """

    def __init__(self, parent, title, address, existing_names):
        super(LiveGetMARTASNameDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.address = address
        self.existing_names = existing_names
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.nameLabel = wx.StaticText(self, label="New shortcut:",size=(160,30))
        self.nameTextCtrl = wx.TextCtrl(self, value=self.address, size=(160, 30))
        self.okButton = wx.Button(self, wx.ID_OK, label='OK',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.nameLabel, noOptions),
                 (self.nameTextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)



class LiveGetMARCOSDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select table for MARCOS monitoring
    """

    def __init__(self, parent, title, datadict):
        super(LiveGetMARCOSDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.datalst = [key for key in datadict if datadict.get(key).get('valid')]
        self.value = 'No valid data'
        if len(self.datalst) > 0:
            self.value = self.datalst[0]
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:",size=(200,30))
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.value, size=(200,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(200,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(200,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

@deprecated("not used any more")
class BGetMARCOSDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select table parameters for MARCOS monitoring
    """

    def __init__(self, parent, title, datalst):
        super(BGetMARCOSDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.datalst = datalst
        self.createControls()
        self.doLayout()

    # Widgets
    def createControls(self):
        self.dataLabel = wx.StaticText(self, label="Data tables:",size=(160,30))
        self.dataComboBox = wx.ComboBox(self, choices=self.datalst,
            style=wx.CB_DROPDOWN, value=self.datalst[0],size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))

    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.dataLabel, noOptions),
                 (self.dataComboBox, expandOption),
                  emptySpace,
                  emptySpace,
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]


        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

@deprecated("not used any more")
class AGetMARTASDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select address for MARTAS monitoring
    """

    def __init__(self, parent, title, options):
        super(AGetMARTASDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.options = options
        #martaslist = [el for el in self.options.get('connectlist',[]) if el
        self.martaslist =['192.168.178.21', '138.22.188.181', '138.22.188.183']
        self.createControls()
        self.doLayout()
        self.bindControls()

    # Widgets
    def createControls(self):
        self.addressLabel = wx.StaticText(self, label="Open MARTAS:",size=(160,30))
        self.addressComboBox = wx.ComboBox(self, choices=self.martaslist,
                       style=wx.CB_DROPDOWN, value=self.martaslist[0],size=(160,-1))
        self.addButton = wx.Button(self, label='Add MARTAS address',size=(160,30))
        self.userLabel = wx.StaticText(self, label="MARTAS user:",size=(160,30))
        self.userTextCtrl = wx.TextCtrl(self, value="cobs",size=(160,30))
        self.pwdLabel = wx.StaticText(self, label="MARTAS pwd:",size=(160,30))
        self.pwdTextCtrl = wx.TextCtrl(self, value="",size=(160,30),style=wx.TE_PASSWORD)
        self.okButton = wx.Button(self, wx.ID_OK, label='Open',size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.addressLabel, noOptions),
                 (self.addressComboBox, expandOption),
                  emptySpace,
                 (self.addButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.userLabel, noOptions),
                 (self.pwdLabel, noOptions),
                 (self.userTextCtrl, expandOption),
                 (self.pwdTextCtrl, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.addButton.Bind(wx.EVT_BUTTON, self.OnAdd)

    def OnAdd(self, e):
        self.Close(True)


class SelectFromListDialog(wx.Dialog):
    """
    DESCRIPTION
        Dialog to select data from any provided list
    """

    def __init__(self, parent, title, selectlist, name):
        super(SelectFromListDialog, self).__init__(parent=parent,
            title=title, size=(400, 600))
        self.selectlist = selectlist
        self.name = name
        self.createControls()
        self.bindControls()
        self.doLayout()
        self.setComponent(idx=0)

    # Widgets
    def createControls(self):
        self.selectLabel = wx.StaticText(self,
                label="Choose {}:".format(self.name),size=(160,30))
        self.selectComboBox = wx.ComboBox(self, choices=self.selectlist,
                style=wx.CB_DROPDOWN, value=self.selectlist[0],
                size=(160,-1))
        self.okButton = wx.Button(self, wx.ID_OK, label='Select',
                size=(160,30))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',
                size=(160,30))


    def doLayout(self):
        # A horizontal BoxSizer will contain the GridSizer (on the left)
        # and the logger text control (on the right):
        boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)

        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        contlist = [(self.selectLabel, noOptions),
                 (self.selectComboBox, expandOption),
                 (self.okButton, dict(flag=wx.ALIGN_CENTER)),
                 (self.closeButton, dict(flag=wx.ALIGN_CENTER))]

        # A GridSizer will contain the other controls:
        cols = 2
        rows = int(np.ceil(len(contlist)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        # Add the controls to the sizers:
        for control, options in contlist:
            gridSizer.Add(control, **options)

        for control, options in \
                [(gridSizer, dict(border=5, flag=wx.ALL))]:
            boxSizer.Add(control, **options)

        self.SetSizerAndFit(boxSizer)

    def bindControls(self):
        self.selectComboBox.Bind(wx.EVT_COMBOBOX, self.setComponent)
        self.selectComboBox.Bind(wx.EVT_TEXT, self.setComponent)

    def setComponent(self, event=None, idx=None):
        """
        DESCRIPTION
            Method to set the component variable when the combo box selection
            is changed or the method is called
        """
        if idx is not None:
            self.component = self.selectlist[idx]
        else:
            idx = self.selectComboBox.GetSelection()
            self.component = self.selectlist[idx]

    def getComponent(self):
        """
        DESCRIPTION
            Method to return the component variable
        """
        component = self.component
        return component


class MultiStreamDialog(wx.Dialog):
    """
    DESCRIPTION:
        Main Dialog for showing data set memory
    CALLS:
        MultiStreamPanel, which is a scrolled panel
    """
    def __init__(self, parent, title, datadict, active, plotdict):
        style = wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        super(MultiStreamDialog, self).__init__(parent=parent,
            title=title, style=style) #, size=(400, 700))

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        # Add MultilistPanel
        self.panel = MultiStreamPanel(self, title, datadict, active, plotdict)
        self.panel.SetInitialSize((850, 400))
        self.mainSizer.Add(self.panel, 1, wx.EXPAND | wx.ALL, 10)
        self.SetSizerAndFit(self.mainSizer)


class MultiStreamPanel(scrolledpanel.ScrolledPanel):
    """
    DESCRIPTION:
        Subclass for Multiple stream selections
        This class accesses the data dictionary for data specific information and plot dictionary for projected keys
        Layout of the multiple stream page:

        checkbox    id                data-description               button-with-keys          method-buttons
          [ ]     active_id    sensorid,startdate,enddate,sr           [x,y,z]                  plot vertically
          [ ]       id         sensorid,startdate,enddate,sr           [x,y,z,f]                plot nested

        Further methods:
        join, merge, subtract

        Merge, join and subtract will generate a new stream_id
    CALL:
        used by memory_select
    IMPROVEMENTS:
        remove the evaluating and execution calls
    """

    def __init__(self, parent, title, datadict, active, plotdict):
        scrolledpanel.ScrolledPanel.__init__(self, parent, -1, size=(-1, -1))  #size=(950, 750)
        self.parent = parent
        self.title = title
        self.modify = False
        self.bindkeys = []
        self.datadict = datadict
        self.plotdict = plotdict
        self.active = active
        self.result = DataStream()
        self.selectedids = []

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        self.createWidgets()  ## adding controls to mainsizer
        self.SetSizer(self.mainSizer)
        self.mainSizer.Fit(self)
        self.SetupScrolling()
        self.bindControls()

    # Widgets
    def createWidgets(self):
        self.head1Label = wx.StaticText(self, label="Select data ID:")
        self.head2Label = wx.StaticText(self, label="Data set contents:")
        self.head3Label = wx.StaticText(self, label="Selected keys:")
        self.head4Label = wx.StaticText(self, label="Operation:")
        # 1. Section
        layoutcheckids = []
        layouttextids = []
        layoutbuttonids = []
        count = 0
        for selid in self.datadict:
            keys = self.datadict.get(selid).get('keys')
            shownkeys = self.plotdict.get(selid).get('shownkeys', keys)
            label = "{},{},{},{}".format(self.datadict.get(selid).get('sensorid'), self.datadict.get(selid).get('start'),
                                         self.datadict.get(selid).get('end'), self.datadict.get(selid).get('samplingrate'))
            if not shownkeys:
                shownkeys = keys
            layoutcheckids.append('(self.id{}CheckBox, noOptions)'.format(selid))
            layouttextids.append('(self.id{}TextCtrl, expandOption)'.format(selid))
            layoutbuttonids.append('(self.id{}KeyButton, expandOption)'.format(selid))
            exec('self.id{}CheckBox = wx.CheckBox(self, label="{}")'.format(selid,selid))
            exec('self.id{}TextCtrl = wx.TextCtrl(self, value="{}", size=(320,-1))'.format(selid,label))
            exec('self.id{}KeyButton = wx.Button(self,-1,"Keys: {}", size=(160,-1))'.format(selid, ",".join(shownkeys)))
            if selid == self.active:
                checkbox = getattr(self, "id{}CheckBox".format(selid))
                checkbox.SetValue(True)
                #exec('self.id{}CheckBox.SetValue(True)'.format(selid))
            keybutton = getattr(self, "id{}KeyButton".format(selid))
            keybutton.Bind(wx.EVT_BUTTON, lambda evt, activeid=selid: self.on_get_keys(evt, activeid))
            count += 1
        self.applyButton = wx.Button(self, wx.ID_OK,"Plot",size=(160,-1))
        self.plotButton = wx.Button(self, -1,"Plot nested",size=(160,-1))
        self.mergeButton = wx.Button(self,-1,"Merge",size=(160,-1))
        self.subtractButton = wx.Button(self,-1,"Subtract",size=(160,-1))
        self.joinButton = wx.Button(self,-1,"Join",size=(160,-1))
        self.closeButton = wx.Button(self, wx.ID_CANCEL, label='Cancel',size=(160,-1))

        if count < 2:
            self.plotButton.Disable()
            self.mergeButton.Disable()
            self.subtractButton.Disable()
            self.joinButton.Disable()
        # Prepare some reusable arguments for calling sizer.Add():
        expandOption = dict(flag=wx.EXPAND)
        noOptions = dict()
        emptySpace = ((0, 0), noOptions)

        amount = len(layoutcheckids)
        if amount <= 7:
            amount = 7
        buttonlist = [(self.applyButton, dict(flag=wx.ALIGN_CENTER)),
                      (self.plotButton, dict(flag=wx.ALIGN_CENTER)),
                      (self.joinButton, dict(flag=wx.ALIGN_CENTER)),
                      (self.mergeButton, dict(flag=wx.ALIGN_CENTER)),
                      (self.subtractButton, dict(flag=wx.ALIGN_CENTER)),
                      emptySpace,
                      (self.closeButton, dict(flag=wx.ALIGN_CENTER))]
        if amount > 7:
            buttonlist = [buttonlist[idx] if idx < len(buttonlist) else emptySpace for idx in list(range(0,amount))]

        contlst = []
        contlst.append((self.head1Label, noOptions))
        contlst.append((self.head2Label, noOptions))
        contlst.append((self.head3Label, noOptions))
        contlst.append((self.head4Label, noOptions))
        for idx in list(range(0,amount)):
            if idx < len(layoutcheckids):
                contlst.append(eval(layoutcheckids[idx]))
                contlst.append(eval(layouttextids[idx]))
                contlst.append(eval(layoutbuttonids[idx]))
                contlst.append(buttonlist[idx])
            else:
                contlst.append(emptySpace)
                contlst.append(emptySpace)
                contlst.append(emptySpace)
                contlst.append(buttonlist[idx])

        #self.mainSizer.Add(gridSizer, 0, wx.EXPAND)
        # A GridSizer will contain the other controls:
        cols = 4
        rows = int(np.ceil(len(contlst)/float(cols)))
        gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=10, hgap=10)

        for control, options in contlst:
            gridSizer.Add(control, **options)

        self.mainSizer.Add(gridSizer, 0, wx.EXPAND)


    def bindControls(self):
        self.mergeButton.Bind(wx.EVT_BUTTON, self.onMergeButton)
        self.subtractButton.Bind(wx.EVT_BUTTON, self.onSubtractButton)
        self.joinButton.Bind(wx.EVT_BUTTON, self.onJoinButton)
        self.plotButton.Bind(wx.EVT_BUTTON, self.onPlotButton)
        for elem in self.bindkeys:
            selid = elem[0]
            print (elem[1])
            exec(elem[1])


    def on_get_keys(self, e, activeid):
        keys = self.datadict.get(activeid).get('keys')
        dataset = self.datadict.get(activeid).get('dataset')
        plotcont = self.plotdict.get(activeid)
        shownkeys = plotcont.get('shownkeys', keys)

        namelist = []
        for key in shownkeys:
            colname = dataset.header.get('col-'+key, '')
            if not colname == '':
                namelist.append(colname)
            else:
                namelist.append(key)
        dlg = StreamSelectKeysDialog(None, title='Select keys:',keylst=keys,shownkeys=shownkeys,namelist=namelist)
        for elem in shownkeys:
            exec('dlg.{}CheckBox.SetValue(True)'.format(elem))
        if dlg.ShowModal() == wx.ID_OK:
            shownkeylist = []
            for elem in keys:
                boolval = eval('dlg.{}CheckBox.GetValue()'.format(elem))
                if boolval:
                    shownkeylist.append(elem)
            if len(shownkeylist) == 0:
                shownkeylist = shownkeys
            else:
                plotcont['shownkeys'] = shownkeylist
                self.plotdict[activeid] = plotcont

            # update
            exec('self.id{}KeyButton.SetLabel("Keys: {}")'.format(activeid, ",".join(shownkeylist)))


    def onMergeButton(self, event):
        """
        DESCRIPTION
             Merges two streams
        """
        selectedids = []
        # Get checked items
        streamids = [selid for selid in self.datadict]
        for elem in streamids:
            val = eval('self.id{}CheckBox.GetValue()'.format(elem))
            if val:
                selectedids.append(elem)
        if len(selectedids) == 2:
            dlg = wx.ProgressDialog("Merging...", "message", maximum=100, parent=None, style=wx.PD_APP_MODAL|wx.PD_ELAPSED_TIME|wx.PD_AUTO_HIDE)
            dlg.Update(0, "please wait ... and ignore the progress bar")
            # get the two data sets
            dataset1 = self.datadict.get(selectedids[0]).get('dataset')
            dataset2 = self.datadict.get(selectedids[1]).get('dataset')
            s1 = self.datadict.get(selectedids[0]).get('start')
            e1 = self.datadict.get(selectedids[0]).get('end')
            s2 = self.datadict.get(selectedids[1]).get('start')
            e2 = self.datadict.get(selectedids[1]).get('end')
            sr1 = self.datadict.get(selectedids[0]).get('samplingrate')
            sr2 = self.datadict.get(selectedids[1]).get('samplingrate')
            st = max(s1,s2)
            et = min(e1,e2)+timedelta(seconds=sr1)
            if not s1 == s2 or not e1 == e2:
                dataset2 = dataset2.trim(st,et)
                dataset1 = dataset1.trim(st,et)
            self.result = merge_streams(dataset1,dataset2, debug=False)
            dlg.Update(100, "done")
            dlg.Destroy()
            # Return the new datastream self.result and call _init_read plus -update_plot
            self.selectedids = selectedids
            self.modify = True
            self.Close(True)
            self.parent.Close(True)
        else:
            dlg = wx.MessageDialog(self, "Merge requires two records\n"
                            " - not less, not more\n",
                            "Merge error", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            self.Close(True)
        #self.parent.Destroy()  # Old Version ... only working on linux and windows, not macos


    def onSubtractButton(self, event):
        """
        DESCRIPTION
             Subtracts two stream
        """
        selectedids = []
        # Get checked items
        streamids = [selid for selid in self.datadict]
        for elem in streamids:
            val = eval('self.id{}CheckBox.GetValue()'.format(elem))
            if val:
                selectedids.append(elem)
        if len(selectedids) == 2:
            dlg = wx.ProgressDialog("Subtracting...", "message", maximum=100, parent=None, style=wx.PD_APP_MODAL|wx.PD_ELAPSED_TIME|wx.PD_AUTO_HIDE)
            dlg.Update(0, "please wait ... and ignore the progress bar")
            dataset1 = self.datadict.get(selectedids[0]).get('dataset')
            dataset2 = self.datadict.get(selectedids[1]).get('dataset')
            s1 = self.datadict.get(selectedids[0]).get('start')
            e1 = self.datadict.get(selectedids[0]).get('end')
            s2 = self.datadict.get(selectedids[1]).get('start')
            e2 = self.datadict.get(selectedids[1]).get('end')
            sr1 = self.datadict.get(selectedids[0]).get('samplingrate')
            sr2 = self.datadict.get(selectedids[1]).get('samplingrate')
            st = max(s1,s2)
            et = min(e1,e2)+timedelta(seconds=sr1)
            if not s1 == s2 or not e1 == e2:
                dataset2 = dataset2.trim(st,et)
                dataset1 = dataset1.trim(st,et)
            #if not (s1 == s2 and e1 == e2):
            #    e1 = e1+timedelta(seconds=sr1)
            #    e2 = e2+timedelta(seconds=sr2)
            #    dataset2 = dataset2.trim(s1,e1)
            #    dataset1 = dataset1.trim(s2,e2)
            self.result = subtract_streams(dataset1, dataset2)
            dlg.Update(100, "done")
            dlg.Destroy()
            self.selectedids = selectedids
            self.modify = True
            self.Close(True)
            self.parent.Close(True)
        else:
            dlg = wx.MessageDialog(self, "Subtract requires two records\n"
                            " - not less, not more\n",
                            "Subtract error", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            self.Close(True)
        #self.parent.Destroy()


    def onPlotButton(self, event):
        """
        DESCRIPTION
             Stacking/Averaging streams
        """
        selectedids = []
        keylist = []
        startlist = []
        endlist = []
        # Get checked items
        streamids = [selid for selid in self.datadict]
        for elem in streamids:
            val = eval('self.id{}CheckBox.GetValue()'.format(elem))
            if val:
                selectedids.append(elem)
                # get time ranges and keys
                keylist.append(self.plotdict.get(elem).get('shownkeys'))
                startlist.append(self.datadict.get(elem).get('start'))
                endlist.append(self.datadict.get(elem).get('end'))
        startinvalid = any([1 if el > ref else 0 for el in startlist for ref in endlist])
        endinvalid = any([1 if el > ref else 0 for el in startlist for ref in endlist])
        # if time ranges are overlapping and shownkeys keys are identical then
        # all starttime < min(endtime) and all endtime > max(starttime)
        if all(i == keylist[0] for i in keylist) and not startinvalid and not endinvalid:
            self.selectedids = selectedids
            self.modify = True
            self.Close(True)
            self.parent.Close(True)
        else:
            dlg = wx.MessageDialog(self, "Plotting data on a single diagram requires identical keys\n"
                            "and at least overlapping time ranges\n",
                            "Plotting error", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
        #self.parent.Destroy()

    def onJoinButton(self, event):
        """
        DESCRIPTION
             Joining streams
        """
        selectedids = []
        # Get checked items
        streamids = [selid for selid in self.datadict]
        for elem in streamids:
            val = eval('self.id{}CheckBox.GetValue()'.format(elem))
            if val:
                selectedids.append(elem)
        if len(selectedids) == 2:
            dlg = wx.ProgressDialog("Joining...", "message", maximum=100, parent=None, style=wx.PD_APP_MODAL|wx.PD_ELAPSED_TIME|wx.PD_AUTO_HIDE)
            dlg.Update(0, "please wait ... and ignore the progress bar")
            dataset1 = self.datadict.get(selectedids[0]).get('dataset')
            dataset2 = self.datadict.get(selectedids[1]).get('dataset')
            self.result = join_streams(dataset1, dataset2)
            dlg.Update(100, "done")
            dlg.Destroy()
            self.resultkeys = self.result._get_key_headers()
            self.selectedids = selectedids
            self.modify = True
            self.Close(True)
            self.parent.Close(True)
        else:
            dlg = wx.MessageDialog(self, "Join requires two records\n"
                            " - not less, not more\n",
                            "Join error", wx.OK|wx.ICON_INFORMATION)
            dlg.ShowModal()
            dlg.Destroy()
            self.Close(True)
        #self.parent.Destroy()

@deprecated("removed the wait dialog as information is shown in status bar and not working in wxpython 4.2")
class WaitDialog(wx.Dialog):
    """
    A popup dialog for to inform users
    that work is in progress
    """

    def __init__(self, parent, title, msg):
        # Create a dialog
        wx.Dialog.__init__(self, parent, -1, title, size=(350, 150),
style=wx.CAPTION | wx.STAY_ON_TOP)
        # Add sizers
        box = wx.BoxSizer(wx.VERTICAL)
        box2 = wx.BoxSizer(wx.HORIZONTAL)
        # Add an Info graphic
        bitmap = wxBitmap(32, 32)
        try:
            bitmap = wx.ArtProvider_GetBitmap(wx.ART_INFORMATION,
wx.ART_MESSAGE_BOX, (32, 32))
        except:
            bitmap = wx.ArtProvider.GetBitmap(wx.ART_INFORMATION,
wx.ART_MESSAGE_BOX, (32, 32))
        graphic = wx.StaticBitmap(self, -1, bitmap)
        box2.Add(graphic, 0, wx.EXPAND | wx.ALL, 10)
        # Add the message
        message = wx.StaticText(self, -1, msg)
        box2.Add(message, 0, wx.EXPAND | wx.ALL, 10)
        box.Add(box2, 0, wx.EXPAND)
        # Handle layout
        self.SetAutoLayout(True)
        self.SetSizer(box)
        self.Fit()
        self.Layout()
        self.CentreOnScreen()
        # Display the Dialog
        self.Show()
        # Make sure the screen gets fully drawn before continuing.
        wx.GetApp().Yield()
        #wx.Yield()
