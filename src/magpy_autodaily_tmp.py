#!/usr/bin/env python
"""
MagPy - Example Applications: Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 22.05.2012)
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *
from core.magpy_absolutes import *
from core.magpy_transfer import *

# ----------------------------------------
# ---- Daily analysis  ----
# ----------------------------------------
finaldate = datetime.utcnow()
finaldate = datetime(2012,2,15)
endtime = datetime.strptime('2012-1-26T23:59:59',"%Y-%m-%dT%H:%M:%S")
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
    #basepath = "/home/leon/Dropbox/Daten/Magnetism"
    variopath = os.path.join(basepath,'DIDD-WIK','data','*')
    scalarpath = os.path.join(basepath,'DIDD-WIK','data','*')

    try:
        # 1.) Read Variometer RAW data
        stDIDD = pmRead(path_or_url=variopath,starttime=starttime,endtime=endtime)
        
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
            stDIDDout = pmRead(path_or_url=variopath,starttime=starttime-timedelta(days=1),endtime=starttime)
            stDIDDout.pmwrite(os.path.join(basepath,'DIDD-WIK','data'),filenamebegins='DIDD_',format_type='PYCDF')

        # 5.) Perfom filtering (min data is used for plots)
        # Not necessary for DIDD

        # 6.) Do Baseline correction
        # ---- special case 2009 ---
        # baseline interrupted in March due to water income
        # DIDD System newly set up
        # ---- special case 2009 ---
        if endtime.year == 2009 and endtime.month <= 2:
            absDIDD = pmRead(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_didd.txt'),starttime='2009-01-01', endtime='2009-02-28')
            # no rotation necessary for DIDD
            stDIDD = stDIDD.baseline(absDIDD,fitfunc='poly',fitdegree=1,plotbaseline=True,plotfilename='test.png')
            deltaF = np.median(absDIDD.trim(absDIDD._testtime(endtime)-timedelta(days=365),endtime)._get_column('df'))
        elif endtime.year == 2009 and endtime.month > 2:
            absDIDD = pmRead(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_didd.txt'),starttime='2009-03-01', endtime='2009-12-31')
            # no rotation necessary for DIDD
            stDIDD = stDIDD.baseline(absDIDD,fitfunc='poly',fitdegree=4,plotbaseline=True,plotfilename='test.png')
            deltaF = np.median(absDIDD.trim(absDIDD._testtime(endtime)-timedelta(days=365),endtime)._get_column('df'))
        else:
            absDIDD = pmRead(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_didd.txt'))
            # no rotation necessary for DIDD
            stDIDD = stDIDD.baseline(absDIDD,knotstep=0.05,plotbaseline=True,plotfilename='test.png')
            deltaF = np.median(absDIDD.trim(absDIDD._testtime(endtime)-timedelta(days=365),endtime)._get_column('df'))
        print "Delta F to main pear (last 100 days): %f" % deltaF
        print "Delta F average 2009, 2010, 2011: 4.386, 4.149, 3.609"

        # 7.) Merge with Scalar-Data and Calculate dF (respect pear differences)
        if endtime.year == 2009:
            deltaF = 4.386
        #stDIDD = stDIDD.delta_f(offset=deltaF) # use last years peardiff except for 2009 (use the mean here)
        stDIDD = stDIDD.delta_f()

        # 7a.) Apply offset to stream
        #stDIDD = stDIDD.offset({'f': deltaF})

        # 7b.) Save preliminary data # is used for storm detection
        headers['ProvidedType'] = 'preliminary'
        stDIDD.pmwrite(os.path.join(basepath,'DIDD-WIK','preliminary'),filenamebegins='DIDD_p_',format_type='PYCDF')

        # 8.) Plot HDZF 
        stDIDD = stDIDD._convertstream('xyz2hdz')
        stDIDD.pmplot(['x','y','z','f'],plottitle="Magnetogram : %s" % day, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','didd_%s.png' % day))

        # 9.) status plot showing T1, dF, derivative (as well as a spectrogram - only for space weather plots)
        stDIDD = stDIDD.aic_calc('x',timerange=timedelta(hours=1))
        stDIDD = stDIDD.differentiate(keys=['var2'],put2keys=['var3'])
        stDIDD.pmplot(['df','var3'],padding=2,plottitle="Statusplot for DIDD : %s" % starttime, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','didd_status_%s.png' % day))

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
        stLEMI = pmRead(path_or_url=variopath,starttime=starttime,endtime=endtime)

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
            stLEMIout = pmRead(path_or_url=variopath,starttime=starttime-timedelta(days=1),endtime=starttime)
            stLEMIout.pmwrite(os.path.join(basepath,'LEMI-WIK','data'),filenamebegins='LEMI_',format_type='PYCDF')

        # 5.) Perfom filtering (min data is used for plots)
        # Not necessary for LEMI

        # 6.) Do Baseline correction
        absLEMI = pmRead(path_or_url=os.path.join(abspath,'ABSOLUTE-RAW','data','absolutes_lemi.txt'))
        stLEMI = stLEMI.rotation(alpha=3.3,beta=0.0)
        stLEMI = stLEMI.baseline(absLEMI,knotstep=0.05,plotbaseline=True,plotfilename='test.png')
        deltaF = np.median(absLEMI.trim(absLEMI._testtime(endtime)-timedelta(days=100),endtime)._get_column('df'))
        print "Delta F to main pear (last 100 days): %f" % deltaF

        # 7.) Merge with Scalar-Data and Calculate dF (respect pear differences)
        ssc = pmRead(path_or_url=scalarpath,starttime=starttime,endtime=endtime)
        ssc = ssc.routlier()
        ssc = ssc.remove_flagged()
        ssc = ssc.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
        stLEMI = mergeStreams(stLEMI,ssc,keys=['f'])
        #stLEMI = stLEMI.delta_f(offset=deltaF)
        stLEMI = stLEMI.delta_f()

        # 7b.) Save preliminary data # is used for storm detection
        headers['ProvidedType'] = 'preliminary'
        stLEMI.pmwrite(os.path.join(basepath,'LEMI-WIK','preliminary'),filenamebegins='LEMI_p_',format_type='PYCDF')
        
        # 8.) Plot HDZF 
        stLEMI = stLEMI._convertstream('xyz2hdz')
        stLEMI.pmplot(['x','y','z','f'],plottitle="Magnetogram : %s" % day, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','lemi_%s.png' % day))

        # 9.) status plot showing T1, dF, derivative (as well as a spectrogram - only for space weather plots)
        stLEMI = stLEMI.aic_calc('x',timerange=timedelta(hours=1))
        stLEMI = stLEMI.differentiate(keys=['var2'],put2keys=['var3'])
        stLEMI.pmplot(['df','var3'],padding=2,plottitle="Statusplot for LEMI : %s" % day, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','lemi_status_%s.png' % day))

        stdiffs = subtractStreams(stLEMI,stDIDD,keys=['x','y','z'])# add difference between lemi and DIDD
        stdiffs.pmplot(['x','y','z'],plottitle="Differences Lemi/DIDD : %s" % day, confinex=True, fullday=True, outfile=os.path.join(basepath,'WIK-Diagrams','lemi_didd_diff_%s.png' % day))
       
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

    # 11.) transfer plots to server
    #ftpdatatransfer(localfile='lemi_%s.png' % day,ftppath='/stories/currentdata/wik',myproxy='94.136.40.103',port='21',login='data@conrad-observatory.at',passwd='data2COBS',logfile='transfer.log',cleanup=True)


    # ###################################
    #  Start with third instruments
    # ###################################

    try:
        # Finallay for third instrument
        year = datetime.strftime(endtime,"%Y")
        month = datetime.strftime(endtime,"%m")
        if len(month) < 2:
            month = str(0)+month
        scalarpath = os.path.join(basepath,'PMAG-WIK',year,month,'*')

        #
        # 1.) Read Variometer RAW data
        stPMAG = pmRead(path_or_url=scalarpath,starttime=starttime,endtime=endtime)

        # 2.) Eventually perfom some automatic filtering and or merging
        stPMAG = stPMAG.routlier()

        # 3.) Add header information
        headers = stPMAG.header
        headers['Instrument'] = 'ELSEC820'
        headers['InstrumentSerialNum'] = 'not known'
        headers['InstrumentOrientation'] = 'None'
        headers['Azimuth'] = ''
        headers['Tilt'] = ''
        headers['InstrumentPeer'] = 'F pillar'
        headers['InstrumentDataLogger'] = 'ELSEC and FieldPoint'
        headers['ProvidedComp'] = 'f'
        headers['ProvidedInterval'] = '10 sec'
        headers['ProvidedType'] = 'intensity'
        headers['DigitalSamplingInterval'] = '10 sec'
        headers['DigitalFilter'] = 'None'
        headers['Latitude (WGS84)'] = '48.265'
        headers['Longitude (WGS84)'] = '16.318'
        headers['Elevation (NN)'] = '400 m'
        headers['IAGAcode'] = 'WIK'
        headers['Station'] = 'Cobenzl'
        headers['Institution'] = 'Zentralanstalt fuer Meteorologie und Geodynamik'
        headers['WebInfo'] = 'http://www.wiki.at'
        headers['TemperatureSensors'] = ''
        stPMAG.header = headers

        # 4.) Between 0:00 and 0:30 write last day
        if windowstart < endtime < windowstart+timedelta(minutes=30):
            stPMAGout = pmRead(path_or_url=variopath,starttime=starttime-timedelta(days=1),endtime=starttime)
            stPMAGout = stPMAGout.routlier()
            stPMAGout = stPMAGout.remove_flagged()
            stPMAGout = stPMAGout.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
            stPMAGout.pmwrite(os.path.join(basepath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')

        # 5.) Perfom filtering (min data is used for plots)
        stPMAG = stPMAG.remove_flagged()
        stPMAG = stPMAG.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
        stPMAG.pmplot(['f'],plottitle="Magnetogram : %s" % day, confinex=True, outfile=os.path.join(basepath,'WIK-Diagrams','pmag_%s.png' % day))

        # 6.) Plot F and dF'S
        stDIFF = subtractStreams(stPMAG,stDIDD,keys=['f'])
        stPMAG.pmplot(['f'],plottitle="Magnetogram : %s" % day, confinex=True, outfile=os.path.join(basepath,'WIK-Diagrams','pmag_status_%s.png' % day))

        # 7.) append message if data stream to old
        numlasttimeofstream = stPMAG[-1].time
        if date2num(endtime)-0.1 > numlasttimeofstream:
            nodatatime = (date2num(endtime)-numlasttimeofstream)/24/60
            # Use new mail function to send log and plot
            msg += 'PMAG data stream dried out since at least %i minutes.\nCheck your server and data communication\n\n' % int(nodatatime)
            dropmsgflag = True
    except:
        msg += 'Problems with PMAG treatment\nPlease check\n\n'
        dropmsgflag = True

    # 8.) transfer plots to server
    #ftpdatatransfer(localfile='pmag_%s.png' % day,ftppath='/stories/currentdata/wik',myproxy='94.136.40.103',port='21',login='data@conrad-observatory.at',passwd='data2COBS',logfile='transfer.log',cleanup=True)
    endtime = endtime+timedelta(days=1)

print msg
#if dropmsgflag:
#    send_mail('roman_leonhardt@web.de', send_notification_to, text=msg, files=['magpy.log','AutoAnalysisDIDD.png','AutoAnalysisLemi.png','AutoAnalysisDifferences.png'], smtpserver='smtp.web.de',user="roman_leonhardt",pwd="2kippen")

