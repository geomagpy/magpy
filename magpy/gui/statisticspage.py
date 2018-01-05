from magpy.stream import *

import wx
import wx.lib.scrolledpanel as scrolled


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
        self.timeLabel = wx.StaticText(self, -1, label="", size=(330,20))
        self.compLabel0 = wx.StaticText(self, -1, label="", size=(330,20))
        self.compLabel1 = wx.StaticText(self, -1, label="", size=(330,20))
        self.compLabel2 = wx.StaticText(self, -1, label="", size=(330,20))
        self.compLabel3 = wx.StaticText(self, -1, label="", size=(330,20))
        self.compLabel4 = wx.StaticText(self, -1, label="", size=(330,20))
        self.compLabel5 = wx.StaticText(self, -1, label="", size=(330,20))
        self.compStats0 = wx.StaticText(self, -1, label="", size=(375,90))
        self.compStats1 = wx.StaticText(self, -1, label="", size=(375,90))
        self.compStats2 = wx.StaticText(self, -1, label="", size=(375,90))
        self.compStats3 = wx.StaticText(self, -1, label="", size=(375,90))
        self.compStats4 = wx.StaticText(self, -1, label="", size=(375,90))
        self.compStats5 = wx.StaticText(self, -1, label="", size=(375,90))


    def doLayout(self):
      boxSizer = wx.BoxSizer(orient=wx.HORIZONTAL)
      elemlist = [(self.timeLabel, dict()),
            (self.compLabel0, dict()),
            (self.compStats0, dict()),
            (self.compLabel1, dict()),
            (self.compStats1, dict()),
            (self.compLabel2, dict()),
            (self.compStats2, dict()),
            (self.compLabel3, dict()),
            (self.compStats3, dict()),
            (self.compLabel4, dict()),
            (self.compStats4, dict()),
            (self.compLabel5, dict()),
            (self.compStats5, dict())]
      cols = 1
      rows = int(np.ceil(len(elemlist)/float(cols)))
      gridSizer = wx.FlexGridSizer(rows=rows, cols=cols, vgap=5, hgap=5)
      for control, options in elemlist:
          gridSizer.Add(control, **options)
      for control, options in \
              [(gridSizer, dict(border=5, flag=wx.ALL))]:
          boxSizer.Add(control, **options)
      self.SetSizerAndFit(boxSizer)

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
            minText = '   Min : {} at {}\n'.format(mini[0],
                    num2date(mini[1]))
        except:
            minText = '   Unable to calculate minimum.\n'
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
            maxText = '   Max : {} at {}\n'.format(maxi[0],
                    num2date(maxi[1]))
        except:
            maxText = '   Unable to calculate maximum.\n'
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
            line = '   Mean : {}\n'.format(mean[0])
            line += u'   \u03C3 : {0:.2f}\n'.format(mean[1])
            meanText = line
        except:
            meanText = '   Unable to calculate mean.\n'
        return meanText

    def getStatistics(self, key, teststream):
        """
        DESCRIPTION:
            Function to format statistics.
        PARAMETERS:
            keys            (list) list of key(s)
            teststream      (DataStream object) stream containing data
        RETURNS:
            A string displaying statistics of a time series
        """
        minText = self.getMin(key, teststream)
        maxText = self.getMax(key, teststream)
        meanText = self.getMean(key, teststream)
        varText = self.getVariance(key, teststream)
        statisticText = minText + maxText + meanText + \
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
            varText = u'   \u03C3\u00B2 : {0:.2f}'.format(var)
        except:
            varText = '   Unable to calculate variance.'
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

        titleFont = wx.Font(12, wx.DEFAULT, wx.NORMAL, wx.BOLD)
        for idx in range(6):
            exec('self.compLabel'+str(idx)+'.SetLabel(" ")')
            exec('self.compStats'+str(idx)+'.SetLabel(" ")')
        self.timeLabel.SetFont(titleFont)
        try:
            testarray = stream._select_timerange(starttime=xlimits[0],
                    endtime=xlimits[1])
            teststream = DataStream([LineStruct()], stream, testarray)
            t_limits = teststream._find_t_limits()
            trange = '{} to {}\n'.format(t_limits[0],
                    t_limits[1])
            self.timeLabel.SetLabel(trange)
            for idx, key in enumerate(keys):
                stats = self.getStatistics(key, teststream)
                title = key.upper() + ' Component: '
                exec('self.compLabel'+str(idx)+'.SetLabel(title)')
                exec('self.compLabel'+str(idx)+'.SetFont(titleFont)')
                exec('self.compStats'+str(idx)+'.SetLabel(stats)')
        except:
            message = 'No statistics for this time range.'
            self.timeLabel.SetLabel(message)
