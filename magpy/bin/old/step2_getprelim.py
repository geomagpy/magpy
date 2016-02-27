#!/usr/bin/env python
"""
MagPy - Example Applications: Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 22.05.2012)
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from magpy.stream import *
from magpy.absolutes import *
from magpy.transfer import *

# ----------------------------------------
# ---- Daily analysis  ----
# ----------------------------------------
finaldate = datetime.utcnow()
finaldate = datetime(2010,11,6)
endtime = datetime.strptime('2010-10-27T23:59:59',"%Y-%m-%dT%H:%M:%S")
while endtime < finaldate:
    #Some definitions
    #endtime=datetime.strptime('2012-8-29T15:30:00',"%Y-%m-%dT%H:%M:%S") # datetime.replace by utcnow()
    #endtime = datetime.utcnow()
    starttime = datetime.strptime(datetime.strftime(endtime,"%Y-%m-%d"),"%Y-%m-%d")
    day = datetime.strftime(endtime,"%Y-%m-%d") # used for  graph description
    send_notification_to = ['roman.leonhardt@zamg.ac.at']
    dropmsgflag = False
    msg = ''

    print "Current day: %s" % day
    print starttime, endtime

    # ###################################
    #  Start with primary instruments
    # ###################################

    abspath = "/home/leon/Dropbox/Daten/Magnetism"
    basepath = "/home/data/WIK"
    basepath = "/home/leon/Dropbox/Daten/Magnetism"
    variopath = os.path.join(basepath,'DIDD-WIK','data','*')
    scalarpath = os.path.join(basepath,'DIDD-WIK','data','*')

    try:
        # 1.) Read Variometer RAW data
        stDIDD = read(path_or_url=variopath,starttime=starttime,endtime=endtime)
        
        # 2.) Eventually perfom some automatic filtering and or merging
        stDIDD = stDIDD.remove_flagged()

        # 3.) Add header information
        headers = stDIDD.header
        headers['Instrument'] = 'DIDD'
        headers['InstrumentSerialNum'] = 'not known'
        headers['InstrumentOrientation'] = 'xyz'
        headers['Azimuth'] = '0 deg'
        headers['Tilt'] = '0 deg'
        headers['InstrumentPeer'] = 'Shaft'
        headers['InstrumentDataLogger'] = 'Magrec1.0'
        headers['ProvidedComp'] = 'xyzf'
        headers['ProvidedInterval'] = 'min'
        headers['ProvidedType'] = 'variation'
        headers['DigitalSamplingInterval'] = '8 sec'
        headers['DigitalFilter'] = 'Gauss 45sec'
        headers['Latitude (WGS84)'] = '48.265'
        headers['Longitude (WGS84)'] = '16.318'
        headers['Elevation (NN)'] = '400 m'
        headers['IAGAcode'] = 'WIK'
        headers['Station'] = 'Cobenzl'
        headers['Institution'] = 'Zentralanstalt fuer Meteorologie und Geodynamik'
        headers['WebInfo'] = 'http://www.wiki.at'
        #headers['T-Instrument'] = 'External-USB'
        #headers['T-InstrumentSerialNum'] = str(Tserialnr)
        stDIDD.header = headers

        # 4.) Between 0:00 and 0:30 write last day
        windowstart = stDIDD._testtime(starttime)
        if windowstart < endtime < windowstart+timedelta(minutes=30):
            print "writing data"
            stDIDDout = read(path_or_url=variopath,starttime=starttime-timedelta(days=1),endtime=starttime)
            stDIDDout.write(os.path.join(basepath,'DIDD-WIK','data'),filenamebegins='DIDD_',format_type='PYCDF')

        # 5.) Perfom filtering (min data is used for plots)
        # Not necessary for DIDD

        # 6.) Do Baseline correction
        # ---- special case 2009 ---
        # baseline interrupted in March due to water income
        # DIDD System newly set up
        # ---- special case 2009 ---
        if endtime.year == 2009 and endtime.month <= 2:
            absDIDD = read(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_didd.txt'),starttime='2009-01-01', endtime='2009-02-28')
            # no rotation necessary for DIDD
            stDIDD = stDIDD.baseline(absDIDD,fitfunc='poly',fitdegree=1,plotbaseline=True,plotfilename='test.png')
            deltaF = np.median(absDIDD.trim(absDIDD._testtime(endtime)-timedelta(days=365),endtime)._get_column('df'))
        elif endtime.year == 2009 and endtime.month > 2:
            absDIDD = read(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_didd.txt'),starttime='2009-03-01', endtime='2009-12-31')
            # no rotation necessary for DIDD
            stDIDD = stDIDD.baseline(absDIDD,fitfunc='poly',fitdegree=4,plotbaseline=True,plotfilename='test.png')
            deltaF = np.median(absDIDD.trim(absDIDD._testtime(endtime)-timedelta(days=365),endtime)._get_column('df'))
        else:
            absDIDD = read(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_didd.txt'))
            # no rotation necessary for DIDD
            stDIDD = stDIDD.baseline(absDIDD,knotstep=0.05,plotbaseline=True,plotfilename='test.png')
            absDIDD = read(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_didd.txt'))
            absDIDDlst = absDIDD.trim(absDIDD._testtime(endtime)-timedelta(days=365),endtime)
            print len(absDIDDlst)
            deltaF = np.median(absDIDD.trim(absDIDD._testtime(endtime)-timedelta(days=365),endtime)._get_column('df'))
        print "Delta F to main pear (last 365 days): %f" % deltaF
        print "Delta F average 2009, 2010, 2011: 4.386, 4.149, 3.609"

        # 6b.) If currentday == 1.1.newyear then save the baseline files to the definite directory 
        if endtime.day == 31 and endtime.month == 12:
            basefigname = 'baseline_didd_%i.png' % (endtime.year)
            print "Renaming the baseline figure: %s" % (basefigname)
            os.rename('test.png',basefigname)

        # 7.) Merge with Scalar-Data and Calculate dF (respect pear differences)
        if endtime.year == 2009:
            deltaF = 4.386
        #stDIDD = stDIDD.delta_f(offset=deltaF) # use last years peardiff except for 2009 (use the mean here)
        stDIDD = stDIDD.delta_f()

        # 7a.) Apply offset to stream
        #stDIDD = stDIDD.offset({'f': deltaF})

        # 7b.) Save preliminary data # is used for storm detection
        headers['ProvidedType'] = 'preliminary'
        stDIDD.write(os.path.join(basepath,'DIDD-WIK','preliminary'),filenamebegins='DIDD_p_',format_type='PYCDF')

        # 8.) Plot HDZF 
        stDIDD = stDIDD._convertstream('xyz2hdz')
        stDIDD.plot(['x','y','z','f'],plottitle="Magnetogram : %s" % day, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','didd_%s.png' % day))

        # 9.) status plot showing T1, dF, derivative (as well as a spectrogram - only for space weather plots)
        stDIDD = stDIDD.aic_calc('x',timerange=timedelta(hours=1))
        stDIDD = stDIDD.differentiate(keys=['var2'],put2keys=['var3'])
        stDIDD.plot(['df','var3'],padding=2,plottitle="Statusplot for DIDD : %s" % starttime, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','didd_status_%s.png' % day))

        # 10.) append message if data stream to old
        numlasttimeofstream = stDIDD[-1].time
        if date2num(endtime)-0.1 > numlasttimeofstream:
            nodatatime = (date2num(endtime)-numlasttimeofstream)/24/60
            # Use new mail function to send log and plot
            msg += 'DIDD data stream dried out since at least %i minutes.\nCheck your server and data communication\n\n' % int(nodatatime)
            dropmsgflag = True
    except:
        msg += 'Problems with DIDD treatment\nPlease check\n\n'
        dropmsgflag = True

    # 11.) transfer plots to server
    #ftpdatatransfer(localfile='didd_%s.png' % day,ftppath='/stories/currentdata/wik',myproxy='94.136.40.103',port='21',login='data@conrad-observatory.at',passwd='data2COBS',logfile='transfer.log',cleanup=True)


    # ###################################
    #  Start with secondary instruments
    # ###################################


    # Repeat for secondary instruments
    variopath = os.path.join(basepath,'LEMI-WIK','data','*')
    scalarpath = os.path.join(basepath,'DIDD-WIK','data','*')

    try:
        # 1.) Read Variometer RAW data
        stLEMI = read(path_or_url=variopath,starttime=starttime,endtime=endtime)

        # 2.) Eventually perfom some automatic filtering and or merging
        stLEMI = stLEMI.remove_flagged()

        # 3.) Add header information
        headers = stLEMI.header
        headers['Instrument'] = 'Lemi025'
        headers['InstrumentSerialNum'] = 'not known'
        headers['InstrumentOrientation'] = 'hdz'
        headers['Azimuth'] = '3.3 deg'
        headers['Tilt'] = '0 deg'
        headers['InstrumentPeer'] = 'Basement-East'
        headers['InstrumentDataLogger'] = 'Lemi025'
        headers['ProvidedComp'] = 'xyzf'
        headers['ProvidedInterval'] = 'min'
        headers['ProvidedType'] = 'variation'
        headers['DigitalSamplingInterval'] = '0.00625 sec'
        headers['DigitalFilter'] = 'Gauss 45sec'
        headers['Latitude (WGS84)'] = '48.265'
        headers['Longitude (WGS84)'] = '16.318'
        headers['Elevation (NN)'] = '400 m'
        headers['IAGAcode'] = 'WIK'
        headers['Station'] = 'Cobenzl'
        headers['Institution'] = 'Zentralanstalt fuer Meteorologie und Geodynamik'
        headers['WebInfo'] = 'http://www.wiki.at'
        #headers['T-Instrument'] = 'External-USB'
        #headers['T-InstrumentSerialNum'] = str(Tserialnr)
        stLEMI.header = headers

        # 4.) Save all that to the working directory
        if windowstart < endtime < windowstart+timedelta(minutes=30):
            stLEMIout = read(path_or_url=variopath,starttime=starttime-timedelta(days=1),endtime=starttime)
            stLEMIout.write(os.path.join(basepath,'LEMI-WIK','data'),filenamebegins='LEMI_',format_type='PYCDF')

        # 5.) Perfom filtering (min data is used for plots)
        # Not necessary for LEMI

        # 6.) Do Baseline correction
        # Regard for changing baseline in 2012 
        absLEMI = read(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_lemi.txt'))
        stLEMI = stLEMI.rotation(alpha=3.3,beta=0.0)
        stLEMI = stLEMI.baseline(absLEMI,knotstep=0.05,plotbaseline=True,plotfilename='test.png')
        absLEMI = read(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_lemi.txt'))
        deltaF = np.median(absLEMI.trim(absLEMI._testtime(endtime)-timedelta(days=365),endtime)._get_column('df'))
        print "Delta F to main pear (last 365 days): %f" % deltaF


        # 6b.) If currentday == 1.1.newyear then save the baseline files to the definite directory 
        if endtime.day == 31 and endtime.month == 12:
            basefigname = 'baseline_lemi_%i.png' % (endtime.year)
            print "Renaming the baseline figure: %s" % (basefigname)
            os.rename('test.png',basefigname)

        # 7.) Merge with Scalar-Data and Calculate dF (respect pear differences)
        ssc = read(path_or_url=scalarpath,starttime=starttime,endtime=endtime)
        ssc = ssc.remove_outlier()
        ssc = ssc.remove_flagged()
        ssc = ssc.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
        stLEMI = mergeStreams(stLEMI,ssc,keys=['f'])
        #stLEMI = stLEMI.delta_f(offset=deltaF)
        stLEMI = stLEMI.delta_f()

        # 7b.) Save preliminary data # is used for storm detection
        headers['ProvidedType'] = 'preliminary'
        stLEMI.write(os.path.join(basepath,'LEMI-WIK','preliminary'),filenamebegins='LEMI_p_',format_type='PYCDF')
        
        # 8.) Plot HDZF 
        stLEMI = stLEMI._convertstream('xyz2hdz')
        stLEMI.plot(['x','y','z','f'],plottitle="Magnetogram : %s" % day, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','lemi_%s.png' % day))

        # 9.) status plot showing T1, dF, derivative (as well as a spectrogram - only for space weather plots)
        stLEMI = stLEMI.aic_calc('x',timerange=timedelta(hours=1))
        stLEMI = stLEMI.differentiate(keys=['var2'],put2keys=['var3'])
        stLEMI.plot(['df','var3'],padding=2,plottitle="Statusplot for LEMI : %s" % day, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','lemi_status_%s.png' % day))

        stdiffs = subtractStreams(stLEMI,stDIDD,keys=['x','y','z'])# add difference between lemi and DIDD
        stdiffs.plot(['x','y','z'],plottitle="Differences Lemi/DIDD : %s" % day, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','lemi_didd_diff_%s.png' % day))
       
        # 10.) append message if data stream to old
        numlasttimeofstream = stLEMI[-1].time
        if date2num(endtime)-0.1 > numlasttimeofstream:
            nodatatime = (date2num(endtime)-numlasttimeofstream)/24/60
            # Use new mail function to send log and plot
            msg += 'LEMI data stream dried out since at least %i minutes.\nCheck your server and data communication\n\n' % int(nodatatime)
            dropmsgflag = True
    except:
        msg += 'Problems with LEMI treatment\nPlease check\n\n'
        dropmsgflag = True

    endtime = endtime+timedelta(days=1)

print msg
#if dropmsgflag:
#    send_mail('roman_leonhardt@web.de', send_notification_to, text=msg, files=['magpy.log','AutoAnalysisDIDD.png','AutoAnalysisLemi.png','AutoAnalysisDifferences.png'], smtpserver='smtp.web.de',user="roman_leonhardt",pwd="2kippen")

