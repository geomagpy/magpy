from magpy.stream import *

import wx
import wx.lib.scrolledpanel as scrolled


class StatisticsPanel(scrolled.ScrolledPanel):
    """
    DESCRIPTION
        Scrolled Panel that contains all methods for the lower statistics.
        panel and its insets
    """
    def __init__(self, *args, **kwds):
        scrolled.ScrolledPanel.__init__(self, *args, **kwds)
        # Create pages on MenuPanel
        nb = wx.Notebook(self,-1)
        self.stats_page = StatisticsPage(nb)
        nb.AddPage(self.stats_page, "Continuous Statistics")
        sizer = wx.BoxSizer()
        sizer.Add(nb, 1, wx.EXPAND)
        self.SetSizer(sizer)
        self.SetupScrolling()


class StatisticsPage(wx.Panel):
    """
    DESCRIPTION
        Panel that contains methods for displaying continuous statistics.
    """
    def __init__(self, *args, **kwds):
        wx.Panel.__init__(self, *args, **kwds)
        self.createControls()
        self.doLayout()

    def createControls(self):
        self.statisticsLabel = wx.StaticText(self, label="Statistics:")
        self.timeLabel = wx.StaticText(self, label="")
        self.statisticsTextCtrl = wx.TextCtrl(self, wx.ID_ANY,
                style=wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        self.valuesLabel = wx.StaticText(self,
                label="Individual Values (For ten or less data points):")
        self.valuesTextCtrl = wx.TextCtrl(self, wx.ID_ANY,
                style=wx.TE_MULTILINE|wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)

    def doLayout(self):
      mainSizer = wx.BoxSizer(wx.HORIZONTAL)
      gridSizer = wx.FlexGridSizer(3, 2, 5,10)
      gridSizer.AddMany([(self.timeLabel),
            (0, 0),
            (self.statisticsLabel),
            (self.valuesLabel),
            (self.statisticsTextCtrl, 1, wx.EXPAND),
            (self.valuesTextCtrl, 1, wx.EXPAND)])
      gridSizer.AddGrowableRow(2, 1)
      gridSizer.AddGrowableCol(1, 1)
      gridSizer.AddGrowableCol(0, 1)
      mainSizer.Add(gridSizer, proportion = 2,
            flag = wx.ALIGN_LEFT | wx.ALL |wx.EXPAND, border=5)
      self.SetSizer(mainSizer)

    def getDataPoints(self, keys, teststream):
        """
        DESCRIPTION:
            Function to display values of a time series.
        PARAMETERS:
            keys            (list) list of key(s)
            teststream      (DataStream object) stream containing data
        RETURNS:
            A string displaying the values in a time series
        """
        valueText = ''
        times = teststream.ndarray[KEYLIST.index('time')]
        for key in keys:
            valueText += '|-----------------------{}'.format(key) + \
                    '-----------------------|\n'.format(key)
            for t, val in zip(times, teststream.ndarray[KEYLIST.index(key)]):
                line = '     {}: {}\n'.format(num2date(t), val)
                valueText += line
        return valueText

    def getMin(self, key, teststream):
        """
        DESCRIPTION:
            Function to display minimum value of a time series.
        PARAMETERS:
            keys            (list) list of key(s)
            teststream      (DataStream object) stream containing data
        RETURNS:
            A string displaying the minimum value in a time series
        """
        try:
            mini = teststream._get_min(key,returntime=True)
            minText = '       Minimum: {} at {}\n'.format(mini[0],
                    num2date(mini[1]))
        except:
            minText = '       Unable to calculate minimum.\n'
        return minText

    def getMax(self, key, teststream):
        """
        DESCRIPTION:
            Function to display maximum value of a time series.
        PARAMETERS:
            keys            (list) list of key(s)
            teststream      (DataStream object) stream containing data
        RETURNS:
            A string displaying the maximum value in a time series
        """
        try:
            maxi = teststream._get_max(key,returntime=True)
            maxText = '       Maximum: {} at {}\n'.format(maxi[0],
                    num2date(maxi[1]))
        except:
            maxText = '       Unable to calculate maximum.\n'
        return maxText

    def getMean(self, key, teststream):
        """
        DESCRIPTION:
            Function to display mean value of a time series.
        PARAMETERS:
            keys            (list) list of key(s)
            teststream      (DataStream object) stream containing data
        RETURNS:
            A string displaying the mean value of a time series
        """
        try:
            mean = teststream.mean(key, meanfunction='mean',
                    std=True,percentage=10)
            line = '       Mean: {}\n'.format(mean[0])
            line += '       Standard Deviation: {}\n'.format(mean[1])
            meanText = line
        except:
            meanText = '       Unable to calculate mean.\n'
        return meanText

    def getStatistics(self, keys, teststream):
        """
        DESCRIPTION:
            Function to format statistics.
        PARAMETERS:
            keys            (list) list of key(s)
            teststream      (DataStream object) stream containing data
        RETURNS:
            A string displaying statistics of a time series
        """
        statisticText = ''
        for key in keys:
            header = '|------------------------------{}'.format(key) + \
                    '------------------------------|\n'
            minText = self.getMin(key, teststream)
            maxText = self.getMax(key, teststream)
            meanText = self.getMean(key, teststream)
            varText = self.getVariance(key, teststream)
            statisticText += header + minText + maxText + meanText + \
                    varText
        return statisticText

    def getVariance(self, key, teststream):
        """
        DESCRIPTION:
            Function to display variance value of a time series.
        PARAMETERS:
            keys            (list) list of key(s)
            teststream      (DataStream object) stream containing data
        RETURNS:
            A string displaying the variance value of a time series
        """
        try:
            var = teststream._get_variance(key)
            varText = '       Variance: {}\n'.format(var)
        except:
            varText = '       Unable to calculate variance.\n'
        return varText

    def setStatistics(self, keys, stream, xlimits):
        """
        DESCRIPTION:
            Function to set and display statistics and values in text
            controls.
        PARAMETERS:
            keys            (list) list of key(s)
            stream          (DataStream object) stream containing data
            xlimits         (list) list with limits of a time series
        RETURNS:
            A string displaying the variance value of a time series
        """
        try:
            testarray = stream._select_timerange(starttime=xlimits[0],
                    endtime=xlimits[1])
            teststream = DataStream([LineStruct()], stream, testarray)
            t_limits = teststream._find_t_limits()
            trange = 'Timerange: {} to {}'.format(t_limits[0],
                    t_limits[1])
            self.timeLabel.SetLabel(trange)
            stats = self.getStatistics(keys, teststream)
            self.statisticsTextCtrl.SetValue(stats)
            if len(teststream.ndarray[KEYLIST.index('time')]) <= 10:
                dataPoints = self.getDataPoints(keys, teststream)
                self.valuesTextCtrl.SetValue(dataPoints)
            else:
                self.valuesTextCtrl.SetValue('Too many data points...')
        except:
            message = 'Cannot get statistics for time range.'
            self.statisticsTextCtrl.SetValue(message)
            self.valuesTextCtrl.SetValue(message)

