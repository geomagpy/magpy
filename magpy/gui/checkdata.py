

def saveReport(label, report):
    if label == 'Save':
        savepath = ''
        saveFileDialog = wx.FileDialog(self, "Save As", "", "",
                                       "Report (*.txt)|*.txt",
                                       wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        if saveFileDialog.ShowModal() == wx.ID_OK:
            savepath = saveFileDialog.GetPath()
        saveFileDialog.Destroy()
        if not savepath == '':
            if sys.version_info >= (3 ,0 ,0):
                with open(savepath, "w", newline='') as myfile:
                    myfile.write(report)
            else:
                with open(savepath, "wb") as myfile:
                    myfile.write(report)
            return True
            # self.Close(True)
        return False
    else:
        return False

def createReport(reportmsg, warningmsg, errormsg):
    """
     Definition:
          create report for dialog
    """
    warninghead = "\n++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
    warninghead += "     Warnings: You need to solve these issues\n"
    warninghead += "++++++++++++++++++++++++++++++++++++++++++++++++++++\n"

    errorhead = "\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
    errorhead += "     Critical errors: please check\n"
    errorhead += "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"

    if not warningmsg == '':
        warnrep = warninghead + warningmsg
    else:
        warnrep = " - no warnings\n"
    if not errormsg == '':
        errorrep = errorhead + errormsg
    else:
        errorrep = " - no errors\n"
    report = reportms g +'\n ' +warnre p +'\n ' +errorrep
    return report


def readSecData(secondpath ,seconddata ,rmonth ,year ,dataid=None ,debug=False):
    """
    Definition:
          reading one second data from ImagCDF or IAGA02 formats (or MagPy)
    """
    starttim e =str(year ) +'- ' +str(rmonth).zfill(2 ) +'-01'
    endmonth = rmont h +1
    endyear = year
    success = 1
    if endmonth == 13:
        endyear = yea r +1
        endmonth = 1
    endtim e =str(endyear ) +'- ' +str(endmonth).zfill(2 ) +'-01'
    if seconddata == 'cdf':
        cdfname = '* ' +str(year ) +str(rmonth).zfill(2 ) +'_000000_PT1S_4.cdf'
        loadpath = os.path.join(secondpath ,cdfname)
        try:
            secdata = read(loadpath ,debug=debug)
        except:
            secdata = DataStream()
            success = 6
        if not secdata.length()[0] > 0:
            print ("did not find an appropriate monthly cdf file")
            cdfname = '*.cdf'
            cdfnamelist = ['*_000000_PT1S_4.cdf' ,'*.cdf']
            success = 1
            for cdfname in cdfnamelist:
                loadpath = os.path.join(secondpath ,cdfname)
                success += 1
                # File name issue !!
                try:
                    secdata = read(loadpath ,debug=debug, starttime=starttime, endtime=endtime)
                    success = success
                except:
                    secdata = DataStream()
                    success = 6
                if secdata.length()[0] > 0:
                    break

    elif seconddata == 'pycdf':
        if dataid:
            cdfname = datai d +'_vario_sec_ ' +str(year ) +str(rmonth).zfill(2 ) +'.cdf'
            loadpath = os.path.join(secondpath ,cdfname)
            secdata = read(loadpath ,debug=debug)
        else:
            print ("please provide a sensorid")
    elif seconddata == 'iaga':
        loadpath = os.path.join(secondpath ,'*.sec')
        try:
            secdata = read(loadpath ,starttime=starttime ,endtime=endtime ,debug=debug)
        except:
            secdata = DataStream()
            success = 6

    return secdata, success

def readMinData(checkchoice ,minutedata ,minutepath ,month ,rmonth ,year=1900 ,resolution=None ,debug=False):
    """
    Definition:
          reading one minute data from IAF formats (and others)
    """
    starttim e =str(year ) +'- ' +str(rmonth).zfill(2 ) +'-01'
    endmonth = rmont h +1
    endyear = year
    success = 1
    if endmonth == 13:
        endyear = yea r +1
        endmonth = 1
    endtim e =str(endyear ) +'- ' +str(endmonth).zfill(2 ) +'-01'

    if minutedata == 'iaf':
        if checkchoice == 'quick':
            iafpath1 = minutepath.replace('*.' ,'* ' +month.lower( ) +'.')
            iafpath2 = minutepath.replace('*.' ,'* ' +month.upper( ) +'.')
            if len(glob.glob(iafpath1)) > 0:
                path = iafpath1
            else:
                path = iafpath2
        else:
            path = minutepath
        try:
            if resolution:
                mindata = read(path, resolution=resolution, debug=debug)
            else:
                mindata = read(path, debug=debug)
        except:
            mindata = DataStream()
            success = 6

    return mindata, success


def checkmodule1(config={}, results={}):
    if success == 1:
        succlst[0] = 1
        reportmsg += "#######################################\n"
        reportmsg += "Step 1:\n"
        reportmsg += "#######################################\n"
        reportmsg += "Def.: checking directories and for presence of files\n\n"
        # check whether paths are existing and appropriate data is contained - if failing set success to 6
        # Provide a report dialog with summaries of each test and a continue button
        # You have selected the following options:

        # if checkchoice == 'quick':
        #    print("Randomly selecting month for full data validity checks: Using {}").format(datetime(1900, rmonth, 1).strftime('%B'))
        # Step 1: # test directory structure, and presence of files
        if not minutepath == '' and os.path.isdir(minutepath):
            reportmsg += "Step 1: minute data\n"
            reportmsg += "-----------------------\n"
            reportmsg += "Step 1: Using selected IAF path: {}\n".format(minutepath)
            # import glob
            iafcnt = len(glob.glob(os.path.join(minutepath, "*.BIN")))
            if iafcnt > 0 and iafpath == '':
                iafpath = os.path.join(minutepath, "*.BIN")
            else:
                # windows is not case-sensitive ....
                iafcnt += len(glob.glob(os.path.join(minutepath, "*.bin")))
                if iafcnt > 0 and iafpath == '':
                    iafpath = os.path.join(minutepath, "*.bin")
                else:
                    iafcnt += len(glob.glob(os.path.join(minutepath, "*.zip")))
                    if iafcnt > 0 and iafpath == '':
                        iafpath = os.path.join(minutepath, "*.zip")
            if not iafcnt == 12:
                succlst[0] = 6
                errormsg += "Step 1: !!! IAF error: didn't find 12 monthly files\n"
            else:
                reportmsg += "Step 1: +++ Correct amount of binary files\n"
            blvcnt = len(glob.glob(os.path.join(minutepath, "*.blv")))
            if blvcnt > 0 and blvpath == '':
                blvpath = os.path.join(minutepath, "*.blv")
            else:
                blvcnt += len(glob.glob(os.path.join(minutepath, "*.BLV")))
                if blvcnt > 0 and blvpath == '':
                    blvpath = os.path.join(minutepath, "*.BLV")
            readmecnt = len(glob.glob(os.path.join(minutepath, "README*")))
            readmecnt += len(glob.glob(os.path.join(minutepath, "readme*")))
            readmecnt += len(glob.glob(os.path.join(minutepath, "Readme*")))
            dkacnt = len(glob.glob(os.path.join(minutepath, "*.dka")))
            if dkacnt > 0 and dkapath == '':
                dkapath = os.path.join(minutepath, "*.dka")
            else:
                dkacnt += len(glob.glob(os.path.join(minutepath, "*.DKA")))
                if dkacnt > 0 and dkapath == '':
                    dkapath = os.path.join(minutepath, "*.DKA")
            yearmeancnt = len(glob.glob(os.path.join(minutepath, "YEARMEAN*")))
            if yearmeancnt > 0 and yearmeanpath == '':
                yearmeanpath = os.path.join(minutepath, "YEARMEAN*")
            else:
                yearmeancnt += len(glob.glob(os.path.join(minutepath, "yearmean*")))
                if yearmeancnt > 0 and yearmeanpath == '':
                    yearmeanpath = os.path.join(minutepath, "yearmean*")
            pngcnt = len(glob.glob(os.path.join(minutepath, "*.png")))
            if pngcnt < 1:
                pngcnt += len(glob.glob(os.path.join(minutepath, "*.PNG")))
            if not blvcnt == 1:
                warningmsg += "Step 1: (warning)  No BLV data present\n"
                blvdata = False
                succlst[0] = 5
            else:
                blvdata = True
            if not dkacnt == 1:
                reportmsg += "Step 1: No DKA data present (file is not obligatory, will be extracted from IAF)\n"
                dkadata = False
                # succlst[0] = 5   # File is not obligatory - no change to succlst
            else:
                dkadata = True
            if not yearmeancnt >= 1:
                warningmsg += "Step 1: (warning)  No YEARMEAN data present\n"
                yearmeandata = False
                succlst[0] = 5
            else:
                yearmeandata = True
                try:
                    yldat = read(yearmeanpath)
                    year = num2date(yldat.ndarray[0][-1]).year
                except:
                    pass
            if not readmecnt >= 1:
                warningmsg += "Step 1: (warning)  No README present\n"
                readmedata = False
                succlst[0] = 5
            else:
                readmedata = True
            if not pngcnt == 1:
                reportmsg += "Step 1: No PNG data present (file is not obligatory)\n"
                pngdata = False
                # succlst[0] = 3   # File is not obligatory - no change to succlst
            else:
                pngdata = True
            if blvdata and dkadata and readmedata and yearmeandata and pngdata:
                reportmsg += "Step 1: +++ Auxiliary files are present\n"
        else:
            if not minutepath == '':
                errormsg = "Step 1: Can not access directory {}\n".format(minutepath)
                succlst[0] = 6

        reportmsg += "\nStep 1: one second data\n"
        reportmsg += "-----------------------\n"
        if secondpath == '':
            reportmsg += "Step 1: Skipping one second data checks. No path selected.\n"
        elif os.path.isdir(secondpath):
            reportmsg += "Step 1: Using selected path for one second data: {}\n".format(secondpath)
            # import glob
            cdfcnt = len(glob.glob(os.path.join(secondpath, "*.cdf")))
            cdfcnt += len(glob.glob(os.path.join(secondpath, "*.CDF")))
            iagacnt = len(glob.glob(os.path.join(secondpath, "*.sec")))
            iagacnt += len(glob.glob(os.path.join(secondpath, "*.SEC")))
            if cdfcnt >= 12:
                seconddata = 'cdf'
            elif iagacnt >= 365:
                seconddata = 'iaga'
            if not seconddata in ['cdf', 'iaga']:
                warningmsg += "Step 1: !!! one second error: didn't find either ImagCDF (*.cdf) or IAGA02 (*.sec) files\n"
                if succlst[0] < 4:
                    succlst[0] = 4
            else:
                reportmsg += "Step 1: +++ Found appropriate amount of one second data files\n"
                # print ("Here", minutepath, succlst)
                if minutepath == '':
                    onlysec = True
                    # print ("Checkparameter")
                    checkparameter['step3'] = False
                    checkparameter['step5'] = False
                    checkparameter['step7'] = False
                if succlst[0] < 1:
                    succlst[0] = 1
        else:
            warningmsg += "Step 1: Could not access provided directory of one second data {}\n".format(secondpath)

        if succlst[0] <= 2:
            if onlysec:
                reportmsg += "\n----> Step 1: successfully passed - found only secondary data however\n"
            else:
                reportmsg += "\n----> Step 1: successfully passed\n"
        elif succlst[0] <= 4:
            reportmsg += "\n----> Step 1: passed with some issues to be reviewed\n"
    self.changeStatusbar("Step 1: directories and files ... Done")

    report = createReport(reportmsg, warningmsg, errormsg)
    opendlg = True
    if opendlg:
        dlg = CheckDataReportDialog(None, title='Data check step 1 - report', report=report, rating=succlst[0],
                                    step=list(map(str, succlst)), laststep=laststep)
        dlg.ShowModal()
        if dlg.moveon:
            saveReport(dlg.contlabel, dlg.report)
        else:
            dlg.Destroy()
            # locale.setlocale(locale.LC_TIME, old_loc)
            self.changeStatusbar("Ready")
            return

def checkmodule2(config, results:)

    if succlst[0] < 6:
        if checkparameter['step2']:

            logger = setup_logger(__name__)
            logger.info("-------------------------------")
            logger.info("Starting Step 2 of Data checker")
            logger.info("-------------------------------")

            succlst[1] = 1
            reportmsg += "\n"
            reportmsg += "#######################################\n"
            reportmsg += "Step 2:\n"
            reportmsg += "#######################################\n"
            reportmsg += "Def.: checking readability of main data files and header information\n\n"
            reportmsg += "Step 2:  IAF data:\n"
            reportmsg += "-----------------------\n"
            self.changeStatusbar("Step 2: Reading minute data ... ")
            mindata, fail = readMinData(checkchoice, 'iaf', iafpath, month, rmonth)

            # print(log_stream.getvalue())

            logger = setup_logger(__name__)

            if fail == 6 and not onlysec:
                errormsg += "Step 2: Reading of IAF data failed - check file format\n"
                succlst[1] = 6
            self.changeStatusbar("Step 2: Reading minute data ... Done")

            if mindata.length()[0] > 1:
                reportmsg += "Step 2:  +++ Minute data readable - checked example for {}\n".format(month)
            else:
                if not onlysec:
                    print(" !!! Could not read data - checked example for {}".format(month))
                    errormsg += 'Step 2: Reading of IAF data failed - no data extracted for {} - check file format\n'.format(
                        month)
                    succlst[1] = 6

            reportmsg += "\nStep 2:  IAF data  - checking meta information:\n"
            reportmsg += "-----------------------\n"
            headfailure = False
            # for head in IMAGCDFMETA: # IMAGMETAMETA
            for head in IAFBINMETA:  # IMAGMETAMETA ##### need to select only meta information expected in iaf file, not the information needed to create all IAF output files
                value = mindata.header.get(head, '')
                if value == '' and not onlysec:
                    warningmsg += "Step 2: (warning) !!! IAF: no meta information for {}\n".format(head)
                    headfailure = True
            if not onlysec:
                if not headfailure:
                    reportmsg += "Step 2: +++ IAF: each required header is present. IMPORTANT: did not check content !!\n"
                else:
                    reportmsg += "Step 2: !!! IAF: got meta information warnings\n"
                    succlst[1] = 5

            obscode = mindata.header.get('StationID')
            # get year
            try:
                year = num2date(mindata.ndarray[0][-1]).year
            except:
                frame = wx.Frame(None, -1, 'win.py')
                frame.SetDimensions(0, 0, 200, 50)
                dlg = wx.TextEntryDialog(frame, 'Enter the YEAR you are checking', 'Entry')
                dlg.SetValue("2021")
                if dlg.ShowModal() == wx.ID_OK:
                    yearstr = dlg.GetValue()
                dlg.Destroy()
                try:
                    year = int(yearstr)
                except:
                    year = 1777

            if not seconddata == 'None':
                reportmsg += "\nStep 2:  Secondary data:\n"
                reportmsg += "-----------------------\n"
                proDlg = WaitDialog(None, "Processing...",
                                    "Checking file structure of one second data.\nPlease wait....")
                self.changeStatusbar(
                    "Step 2: Reading one second data ({})- please be patient ... this may need a few minutes".format(
                        seconddata))
                secdata, fail = readSecData(secondpath, seconddata, rmonth, year)
                proDlg.Destroy()
                if fail == 6:
                    errormsg += "Step 2: Reading of one second data failed - check file format and/or file name convention and/or dates\n"
                    succlst[1] = 5
                elif fail == 3:
                    errormsg += "Step 2: Reading of one second data - file names do not follow the (monthly) ImagCDF convention\n"
                    succlst[1] = 3
                self.changeStatusbar("Step 2: Reading one second data ... Done ")
                if secdata.length()[0] > 1:
                    reportmsg += "Step 2: +++ Second data readable - checked example for {}\n".format(month)
                else:
                    print(" !!! Could not read data - checked example for {}".format(month))
                    warningmsg += 'Step 2: !!! Reading of one second data failed - no data extracted for {} - check file format\n'.format(
                        month)
                    warningmsg += 'Step 2: Skipping analysis of one second data\n'.format(month)
                    seconddata = 'None'

            if not seconddata == 'None':
                headfailure = False
                reportmsg += "\nStep 2:  Secondary data  - checking meta information:\n"
                reportmsg += "-----------------------\n"
                sr = secdata.samplingrate()
                reportmsg += "Step 2: data period corresponds to {} second(s)\n".format(sr)
                reportmsg += "Step 2: SamplingFilter: {} \n".format(secdata.header.get('DataSamplingFilter', ''))
                if seconddata == 'cdf':
                    META = IMAGCDFMETA
                elif seconddata == 'iaga':
                    META = IAGAMETA
                noncriticalheadlist = ['DataReferences']
                for head in META:
                    value = secdata.header.get(head, '')
                    if head in noncriticalheadlist and value == '':
                        warningmsg += "Step 2: (info) Second data: no meta information for {} (not critical)\n".format(
                            head.replace('Data', ''))
                    elif value == '':
                        warningmsg += "Step 2: (warning) !!! Second data: no meta information for {}\n".format(head)
                        headfailure = True
                if not headfailure:
                    reportmsg += "Step 2: +++ Second data: each required header is present. IMPORTANT: did not check content !!\n"
                else:
                    reportmsg += "Step 2: !!! Second data: meta information warnings found\n"
                    succlst[1] = 4

            if succlst[1] <= 2:
                reportmsg += "\n----> Step 2: successfully passed\n"
            elif succlst[1] <= 4:
                reportmsg += "\n----> Step 2: passed with some issues to be reviewed\n"
            self.changeStatusbar("Step 2: readability check ... Done")

            logger.info("-------------------------------")
            logger.info("Step 2 of Data checker finished")
            logger.info("-------------------------------")

            report = createReport(reportmsg, warningmsg, errormsg)
            opendlg = True
            if opendlg:
                dlg = CheckDataReportDialog(None, title='Data check step 2 - report', report=report,
                                            rating=max(list(map(int, succlst))), step=list(map(str, succlst)),
                                            laststep=laststep)
                dlg.ShowModal()
                if dlg.moveon:
                    saveReport(dlg.contlabel, dlg.report)
                else:
                    dlg.Destroy()
                    # locale.setlocale(locale.LC_TIME, old_loc)
                    self.changeStatusbar("Ready")
                    return

def checkmodule3(config, results):
    if checkparameter['step3']:
        succlst[2] = 1
        reportmsg += "\n"
        reportmsg += "#######################################\n"
        reportmsg += "Step 3:\n"
        reportmsg += "#######################################\n"
        reportmsg += "Def.: checking data content and consistency\n\n"
        reportmsg += "Step 3: consistency of minute data:\n"
        reportmsg += "-----------------------\n"
        self.changeStatusbar("Step 3: Checking data consistency ... ")
        if checkchoice == 'quick':
            starttime = str(year) + '-' + str(rmonth).zfill(2) + '-01'
            endtime = str(year) + '-' + str(rmonth + 1).zfill(2) + '-01'
            days = int(
                date2num(datetime.strptime(endtime, '%Y-%m-%d')) - date2num(datetime.strptime(starttime, '%Y-%m-%d')))
            # print ("Lenght", mindata.length()[0])
            if not mindata.length()[0] > 0:
                mindata, fail = readMinData(checkchoice, 'iaf', iafpath, month, rmonth)
        else:
            starttime = str(year) + '-01-01'
            endtime = str(year + 1) + '-01-01'
            days = int(date2num(datetime.strptime(str(year + 1) + '-01-01', '%Y-%m-%d')) - date2num(
                datetime.strptime(str(year) + '-01-01', '%Y-%m-%d')))
            mindata, fail = readMinData(checkchoice, 'iaf', iafpath, month, rmonth)

        if not days * 24 * 60 - mindata.length()[0] == 0:
            reportmsg += "Step 3: Checking coverage failed. Expected data points ({}) differs from observerd ({})\n".format(
                days * 24 * 60, mindata.length()[0])
            succlst[2] = 5
        else:
            reportmsg += "Step 3: Checking coverage ... OK\n"

        ## Check f values ( f or df, sampling frequency, if f check delta F, get standard dev)
        if mindata.length()[0] > 0:
            streamid = self._initial_read(mindata)
            if streamid:  # will create a new input into datadict
                self._initial_plot(streamid)
            # if self.InitialRead(mindata):
            #    self.OnInitialPlot(self.plotstream)
            """
            #Test for vector completeness is not working yet
            colx = mindata.ndarray[1]
            coly = mindata.ndarray[2]
            colz = mindata.ndarray[3]
            colxt = [[mindata.ndarray[0][idx],el] for idx,el in enumerate(colx) if not np.isnan(el)]
            colyt = [[mindata.ndarray[0][idx],el] for idx,el in enumerate(coly) if not np.isnan(el)]
            colzt = [[mindata.ndarray[0][idx],el] for idx,el in enumerate(colz) if not np.isnan(el)]
            t1 = np.transpose(colxt)[0]
            t2 = np.transpose(colyt)[0]
            t3 = np.transpose(colzt)[0]
            s = set(t1)
            res = []
            res.extend([x for x in t3 if x not in s])
            s = set(t3)
            res.extend([x for x in t1 if x not in s])
            s = set(t2)
            res.extend([x for x in t1 if x not in s])
            s = set(t1)
            res.extend([x for x in t2 if x not in s])
            s = set(t2)
            res.extend([x for x in t3 if x not in s])
            s = set(t3)
            res.extend([x for x in t2 if x not in s])
            print (len(res))
            res = list(set(res))
            print (len(res))
            if not len(res) > 0:
                reportmsg += "Step 3: Found consistent vector components\n"
            else:
                reportmsg += "Step 3: different amount of valid values in vector components: X: {}, Y: {}, Z: {}\n".format(len(t1),len(t2),len(t3))
                for el in res:
                    warningmsg += "Step 3: found incomplete vector at time step {}\n".format(num2date(el).replace(tzinfo=None))
                succlst[2] = 3
            """

            # create a backup of minutedata
            mindataback = mindata.copy()
            reportmsg += "\nStep 3: Analyzing F, dF \n"
            reportmsg += "-----------------------\n"

            # Get f data for f criteria check:
            fcol = mindata._get_column('f')
            dfcol = mindata._get_column('df')
            if len(fcol) == 0 and len(dfcol) == 0:
                reportmsg += "Step 3: minute data: failed to find scalar (F, dF) data ... Failed\n"
                succlst[2] = 2
            else:
                if len(fcol) > 0:
                    scal = 'f'
                elif len(dfcol) > 0:
                    scal = 'df'
                ftest = mindata.copy()
                ftest = ftest._drop_nans(scal)
                fsamprate = ftest.samplingrate()
                reportmsg += "Step 3: minute data with {} - sampling period: {} sec ... OK\n".format(scal, fsamprate)
                if scal == 'f':
                    ftest = ftest.delta_f()
                fmean, fstd = ftest.mean('df', std=True)
                reportmsg += "Step 3: found an average delta F of {:.3f} +/- {:.3f}\n".format(fmean, fstd)
                if fmean >= 1.0:
                    reportmsg += "Step 3: large deviation of mean delta F\n"
                    warningmsg += "Step 3: large deviation of mean delta F - might be related to adopted baseline calcuation \n"
                    succlst[2] = 3
                if fstd >= 3.0:
                    reportmsg += "Step 3: large scatter about mean\n"
                    warningmsg += "Step 3: dF shows a realtively large scatter - please check\n"
                    succlst[2] = 4
                if fmean < 0.001 and fstd < 0.001:
                    reportmsg += "Step 3: F seems not to be measured independently \n"
                    warningmsg += "Step 3: F seems not to be measured independently\n"
                    succlst[2] = 3

        reportmsg += "\nStep 3: Checking hourly and daily mean data \n"
        reportmsg += "-----------------------\n"
        hourprob = False
        try:
            iafhour, fail = readMinData(checkchoice, 'iaf', iafpath, month, rmonth, resolution='hour')
            # iafhour = read(iafpath,resolution='hour')
        except:
            errormsg += "Step 3: Could not extract hourly data from IAF file\n"
            hourprob = True
            succlst[2] = 6

        if not iafhour.length()[0] > 0:
            errormsg += "Step 3: Did not find hourly data in IAF file\n"
            hourprob = True
            succlst[2] = 6

        if not days * 24 - iafhour.length()[0] == 0:
            errormsg += "Step 3: Did not find expected amount () of hourly data. Found {}.\n".format(days * 24,
                                                                                                     iafhour.length()[
                                                                                                         0])
            hourprob = True
            succlst[2] = 5

        if not hourprob:
            reportmsg += "Step 3: Extracting hourly data ... OK\n"
        else:
            reportmsg += "Step 3: Extracting hourly data ... Failed\n"

        if not hourprob:
            try:
                minfiltdata = mindata.filter(filter_width=timedelta(minutes=60), resampleoffset=timedelta(minutes=30),
                                             filter_type='flat', missingdata='iaga')
            except:
                errormsg += "Step 3: Filtering hourly data failed.\n"

            incon = False
            faileddiff = False
            try:
                diff = subtractStreams(iafhour, minfiltdata)
                # print ("Here", DataStream().KEYLIST.index('df'), iafhour.ndarray[DataStream().KEYLIST.index('df')], minfiltdata.ndarray[DataStream().KEYLIST.index('df')])
                if not diff.length()[0] > 0:
                    diff = subtractStreams(iafhour, minfiltdata, keys=['x', 'y', 'z'])
                    warningmsg += "Step 3: Could not get F/G differences between hourly data and the IAF minute data filtered to hourly means. Please check data file whether hourly means are complete.\n"
                    succlst[2] = 3

                if not diff.length()[0] > 0:
                    errormsg += "Step 3: Could not calculate difference between hourly mean values and filtered minute data.\n"
                    faileddiff = True
                incon = False
                for col in ['x', 'y', 'z']:
                    if not diff.amplitude(col) < 0.2:
                        warningmsg += "Step 3: found differences between expected hourly mean values and filtered minute data for component {}\n".format(
                            col)
                        incon = True
                inconcount = 0
                if incon:
                    for idx, ts in enumerate(iafhour.ndarray[0]):
                        add = ''
                        for col in ['x', 'y', 'z']:
                            colnum = DataStream().KEYLIST.index(col)
                            if not diff.ndarray[colnum][idx] < 0.2:
                                add += ' -- component {}: expected = {:.1f}; observed = {}'.format(col,
                                                                                                   minfiltdata.ndarray[
                                                                                                       colnum][idx],
                                                                                                   iafhour.ndarray[
                                                                                                       colnum][idx])
                        if not add == '':
                            inconcount += 1
                            warningmsg += 'Step 3: inconsistence at {} {}\n'.format(num2date(ts).replace(tzinfo=None),
                                                                                    add)
            except:
                errormsg += "Step 3: Failed to obtain difference between hourly data and filtered minute data.\n"
                faileddiff = True

            if not faileddiff:
                if not incon:
                    reportmsg += "Step 3:  +++ IAF: hourly data is consistent\n"
                else:
                    reportmsg += "Step 3:  !!! IAF: found inconsistencies in hourly data at {} time steps\n".format(
                        inconcount)
                    succlst[2] = 5
            else:
                reportmsg += "Step 3:  !!! IAF: failed to obtain differences\n"
                succlst[2] = 5

        dayprob = False
        try:
            iafday, fail = readMinData(checkchoice, 'iaf', iafpath, month, rmonth, resolution='day')
            # minute, fail = readMinData(checkchoice,'iaf',iafpath,month,rmonth)
            # iafhour = read(iafpath,resolution='hour')
        except:
            errormsg += "Step 3: Could not extract daily means from IAF file\n"
            dayprob = True
            succlst[2] = 6

        if not iafday.length()[0] > 0:
            errormsg += "Step 3: Did not find daily means in IAF file\n"
            dayprob = True
            succlst[2] = 6

        if not dayprob:
            reportmsg += "Step 3: Extracting daily means ... OK\n"
        else:
            reportmsg += "Step 3: Extracting daily means ... Failed\n"

        if not dayprob:
            testmindata = mindataback.copy()
            testday = testmindata.dailymeans(['x', 'y', 'z'], offset=0.0)
            ddiff = subtractStreams(iafday, testday)
            try:
                if not ddiff.length()[0] > 0:
                    errormsg += "Step 3: Could not calculate difference between daily means and average minute data.\n"
                incon = False
                for col in ['x', 'y', 'z']:
                    if not ddiff.amplitude(col) < 1.0:
                        # print ("Hello", col, ddiff.amplitude(col))
                        warningmsg += "Step 3: found differences between expected daily means and filtered minute data for component {}\n".format(
                            col)
                        incon = True

                inconcount = 0
                if incon:
                    for idx, ts in enumerate(ddiff.ndarray[0]):
                        add = ''
                        for col in ['x', 'y', 'z']:
                            colnum = DataStream().KEYLIST.index(col)
                            if not np.abs(ddiff.ndarray[colnum][idx]) < 1.0:
                                add += ' -- component {}: expected = {:.1f}; observed = {}'.format(col, testday.ndarray[
                                    colnum][idx], iafday.ndarray[colnum][idx])
                        if not add == '':
                            inconcount += 1
                            warningmsg += 'Step 3: inconsistence at {} {}\n'.format(num2date(ts).replace(tzinfo=None),
                                                                                    add)
            except:
                errormsg += "Step 3: Failed to obtain difference between daily means and filtered minute data.\n"

            if not incon:
                reportmsg += "Step 3:  +++ IAF: daily means are consistent\n"
            else:
                reportmsg += "Step 3:  !!! IAF: found inconsistencies in daily means at {} time steps\n".format(
                    inconcount)
                succlst[2] = 5

        if succlst[2] <= 2:
            reportmsg += "\n----> Step 3: successfully passed\n"
        elif succlst[2] <= 4:
            reportmsg += "\n----> Step 3: passed with some issues to be reviewed\n"
        self.changeStatusbar("Step 3: Checking data consistency ... Done")

        report = createReport(reportmsg, warningmsg, errormsg)
        opendlg = True
        if opendlg:
            dlg = CheckDataReportDialog(None, title='Data check step 3 - report', report=report,
                                        rating=max(list(map(int, succlst))), step=list(map(str, succlst)),
                                        laststep=laststep)
            dlg.ShowModal()
            if dlg.moveon:
                saveReport(dlg.contlabel, dlg.report)
            else:
                dlg.Destroy()
                # locale.setlocale(locale.LC_TIME, old_loc)
                self.changeStatusbar("Ready")
                return

def checkmodule4(config, results):
    if checkparameter['step4']:
        succlst[3] = 1
        reportmsg += "\n"
        reportmsg += "#######################################\n"
        reportmsg += "Step 4:\n"
        reportmsg += "#######################################\n"
        reportmsg += "Def.: checking one second data content and consistency\n\n"
        if seconddata == 'None':
            reportmsg += "Step 4: No second data available - continue with step 5\n"
        else:
            proDlg = WaitDialog(None, "Processing...",
                                "Checking consistency of one second data.\nPlease wait ... might need a while")

            self.changeStatusbar("Step 4: Checking one second data consistency (internally and with primary data) ")
            # message box - Continuing with step 4 - consistency of one second data with IAF

            if checkchoice == 'quick':
                # use already existing data
                monthlist = [rmonth]
            else:
                # read data for each month
                monthlist = range(1, 13)
                if secdata.length()[0] > 0:
                    oldsecdata = secdata.copy()
            for checkmonth in monthlist:
                print("Loading month {}".format(checkmonth))
                if (checkmonth == rmonth) and secdata.length()[0] > 0:
                    self.changeStatusbar(
                        "Step 4: Taking one second data (loaded already for data checks) for month {} ... be patient".format(
                            checkmonth))
                    secdata = oldsecdata.copy()
                else:
                    self.changeStatusbar(
                        "Step 4: Reading one second data for month {} ... be patient".format(checkmonth))
                    secdata, fail = readSecData(secondpath, seconddata, checkmonth, year, debug=False)
                    # readnew = True
                # print (secdata._find_t_limits())

                if not onlysec:
                    if (checkmonth == rmonth) and mindataback.length()[0] > 0:
                        mindata = mindataback.copy()
                    else:
                        mindata, fail = readMinData(checkchoice, 'iaf', iafpath, month, checkmonth)
                else:
                    mindata = DataStream()

                reportmsg += "\nStep 4: Checking secondary data for month {} \n".format(checkmonth)
                reportmsg += "-----------------------\n"
                # reportmsg += "+++++++++++++++++++++++\n"

                sr = secdata.samplingrate()
                if sr == 1:
                    reportmsg += "Step 4: Checking coverage - found vectorial 1 second data ... OK\n"
                else:
                    reportmsg += "Step 4: Checking coverage - found vectorial data every {} second(s)\n".format(sr)

                # reportmsg += "Step 4: Analyzing F, dF \n"
                # reportmsg += "-----------------------\n"
                # Get f data for f criteria check:
                fcol = secdata._get_column('f')
                dfcol = secdata._get_column('df')
                if len(fcol) == 0 and len(dfcol) == 0:
                    reportmsg += "Step 4: !!! One second data (month {}): failed to find scalar (F, dF) data ... Failed\n".format(
                        checkmonth)
                    succlst[3] = 2
                else:
                    scal = ''
                    if len(fcol) > 0:
                        scal = 'f'
                    elif len(dfcol) > 0:
                        scal = 'df'
                    ftest = secdata.copy()
                    ftest = ftest._drop_nans(scal)
                    fsamprate = ftest.samplingrate()
                    reportmsg += "Step 4: +++ found {} in one second data - sampling period: {} sec ... OK\n".format(
                        scal, fsamprate)
                    if scal == 'f':
                        ftest = ftest.delta_f()
                    fmean, fstd = ftest.mean('df', std=True)
                    reportmsg += "Step 4: +++ found an average delta F of {:.3f} +/- {:.3f}\n".format(fmean, fstd)
                    if fmean >= 1.0:
                        reportmsg += "Step 4: !!! large deviation of mean delta F - check baseline corr\n"
                        warningmsg += "Step 4: (month {}) large deviation of mean delta F - check baseline corr\n".format(
                            checkmonth)
                        succlst[3] = 3
                    if fstd >= 3.0:
                        reportmsg += "Step 4: !!! large scatter about mean\n"
                        warningmsg += "Step 4: (month {}) dF/G shows large scatter about mean\n".format(checkmonth)
                        succlst[3] = 4
                    if fmean < 0.001 and fstd < 0.001:
                        reportmsg += "Step 4: !!! F seems not to be measured independently \n"
                        warningmsg += "Step 4: (month {}) F seems not to be measured independently\n".format(checkmonth)
                        succlst[2] = 3

                # reportmsg += "\nStep 4: Filtering second data and comparing it to minute IAF data \n"
                # reportmsg += "-----------------------\n"

                self.changeStatusbar("Step 4: Filtering second data ...")
                highresfilt = secdata.filter(missingdata='iaga')

                """
                #Test for vector completeness is not working yet
                colx = highresfilt._get_column('x')
                coly = highresfilt._get_column('y')
                colz = highresfilt._get_column('z')
                colx = [el for el in colx if not np.isnan(el)]
                coly = [el for el in coly if not np.isnan(el)]
                colz = [el for el in colz if not np.isnan(el)]
                if len(colx) == len(coly) and len(coly) == len(colz):
                    reportmsg += "Step 4: +++ Found consistent vector components\n"
                else:
                    warningmsg += "Step 4: different amount of valid values in vector components: X: {}, Y: {}, Z: {}\n".format(len(colx),len(coly),len(colz))
                    succlst[3] = 3
                if not minutepath == '':
                    mcolx = mindata._get_column('x')
                    mcoly = mindata._get_column('y')
                    mcolz = mindata._get_column('z')
                    mcolx = [el for el in mcolx if not np.isnan(el)]
                    mcoly = [el for el in mcoly if not np.isnan(el)]
                    mcolz = [el for el in mcolz if not np.isnan(el)]
                    print (checkmonth, len(colx),len(coly),len(colz),len(mcolx),len(mcoly),len(mcolz))
                """
                self.changeStatusbar("Step 4: Filtering second data ... Done")

                # if not scal == '':
                #    diff = subtractStreams(highresfilt,mindata,keys=['x','y','z',scal])
                # else:
                #    diff = subtractStreams(highresfilt,mindata,keys=['x','y','z'])
                if not onlysec:
                    diff = subtractStreams(highresfilt, mindata, keys=['x', 'y', 'z'])
                    diff = diff.trim(starttime=diff.ndarray[0][0] + 0.00069)
                    print("  -> diff length: {}".format(diff.length()[0]))

                    incon = False
                    if not diff.amplitude('x') < 0.3:
                        warningmsg += "Step 4: (month {}) !!! IAF versus filtered second data: maximum differences in x/h component ({}) significantly exceed numerical uncertainty\n".format(
                            checkmonth, diff.amplitude('x'))
                        incon = True
                    elif not diff.amplitude('x') < 0.12:
                        reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in x/h component ({}) slightly exceed numerical uncertainty\n".format(
                            diff.amplitude('x'))
                    if not diff.amplitude('y') < 0.3:
                        warningmsg += "Step 4: (month {}) !!! IAF versus filtered second data: maximum differences in y/d component ({}) significantly  exceed numerical uncertainty\n".format(
                            checkmonth, diff.amplitude('y'))
                        incon = True
                    elif not diff.amplitude('y') < 0.12:
                        reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in y/d component ({}) slightly exceed numerical uncertainty\n".format(
                            diff.amplitude('y'))
                    if not diff.amplitude('z') < 0.3:
                        warningmsg += "Step 4: (month {}) !!! IAF versus filtered second data: maximum differences in z component ({}) significantly  exceed numerical uncertainty\n".format(
                            checkmonth, diff.amplitude('z'))
                        incon = True
                    elif not diff.amplitude('z') < 0.12:
                        reportmsg += "Step 4: (info) IAF/Filtered(Sec): maximum differences in z component ({}) slightly exceed numerical uncertainty\n".format(
                            diff.amplitude('z'))
                    if len(diff._get_column(scal)) > 0:
                        if not diff.amplitude(
                                scal) < 0.30:  ## uncertainty is larger because of df conversion (2 times rounding error)
                            warningmsg += "Step 4: (month {}) IAF versus filtered second data: maximum differences in f component ({}) exceed numerical uncertainty -- PLEASE NOTE: COBS standard procedure is to use mean F for minute and single best F for second\n".format(
                                checkmonth, diff.amplitude('f'))
                            try:
                                ttf, ttstd = diff.mean('f', std=True)
                                warningmsg += "Step 4: (month {}) !!! IAF versus filtered second data: mean f = {} +/- {}\n".format(
                                    checkmonth, ttf, ttstd)
                                incon = True
                            except:
                                pass
                    if not incon:
                        reportmsg += "Step 4: +++ IAF/Filtered(Sec): IAF data and filtered second data is consistent\n"
                    else:
                        reportmsg += "Step 4: !!! IAF versus filtered second data: found inconsistencies. Eventually the one minute data record has been cleaned but not the second data set? \n"
                        succlst[3] = 4
                        streamid = self._initial_read(diff)
                        if streamid:  # will create a new input into datadict
                            self._initial_plot(streamid)
                        # if self.InitialRead(diff):
                        #    self.OnInitialPlot(self.plotstream)

            proDlg.Destroy()

        if succlst[3] <= 2:
            reportmsg += "\n----> Step 4: successfully passed\n"
        elif succlst[3] <= 4:
            reportmsg += "\n----> Step 4: passed with some issues to be reviewed\n"
        self.changeStatusbar("Step 4: Checking secondary data ... Done")

        report = createReport(reportmsg, warningmsg, errormsg)
        opendlg = True
        if opendlg:
            dlg = CheckDataReportDialog(None, title='Data check step 4 - report', report=report,
                                        rating=max(list(map(int, succlst))), step=list(map(str, succlst)),
                                        laststep=laststep)
            dlg.ShowModal()
            if dlg.moveon:
                saveReport(dlg.contlabel, dlg.report)
            else:
                dlg.Destroy()
                # locale.setlocale(locale.LC_TIME, old_loc)
                self.changeStatusbar("Ready")
                return


def checkmodule5(config, result):
    if checkparameter['step5']:
        succlst[4] = 1
        reportmsg += "\n"
        reportmsg += "#######################################\n"
        reportmsg += "Step 5:\n"
        reportmsg += "#######################################\n"
        reportmsg += "Def.: baseline variation and data quality\n\n"

        if not blvpath == '':
            reportmsg += "Step 5: baseline data \n"
            reportmsg += "-----------------------\n"
            blvdata = read(blvpath)
            streamid = self._initial_read(blvdata)
            if streamid:  # will create a new input into datadict
                self._initial_plot(streamid)
            # if self.InitialRead(blvdata):
            #    #self.ActivateControls(self.plotstream)
            #    self.OnInitialPlot(self.plotstream)
            self.changeStatusbar("Step 5: checking basline ...")
            """
            self.plotstream = blvdata
            self.ActivateControls(self.plotstream)
            # set padding and plotsymbols to points
            self.OnPlot(self.plotstream,self.shownkeylist)
            """

            headx = blvdata.header.get("col-dx", "")
            heady = blvdata.header.get("col-dy", "")
            headz = blvdata.header.get("col-dz", "")
            unitx = blvdata.header.get("unit-col-dx", "")
            unity = blvdata.header.get("unit-col-dy", "")
            unitz = blvdata.header.get("unit-col-dz", "")
            # get average sampling rate
            means = blvdata.dailymeans(keys=['dx', 'dy', 'dz'])
            srmeans = means.get_sampling_period()
            reportmsg += "Step 5: basevalues measured on average every {:.1f} days \n".format(srmeans)
            # Average and maximum standard deviation
            means = means._drop_nans('dx')
            # print ("means", means.mean('dx',percentage=1), means.amplitude('dx'))
            reportmsg += "Step 5: average deviation of repeated measurements is: {:.2f}{} for {}, {:.4f}{} for {} and {:.2f}{} for {}\n".format(
                means.mean('dx', percentage=1), unitx, headx, means.mean('dy', percentage=1), unity, heady,
                means.mean('dz', percentage=1), unitz, headz)
            if means.mean('dx', percentage=1) > 0.5 or means.mean('dz', percentage=1) > 0.5:
                reportmsg += "Step 5: consistent variations between repeated measurements are present\n"
                succlst[4] = 2
            if means.mean('dx', percentage=1) > 3.0 or means.mean('dz', percentage=1) > 3.0:
                reportmsg += "Step 5: !!! found relatively large variations for repeated measurements\n"
                warningmsg += "Step 5: check basevalues\n"
                succlst[4] = 4

            # analyse residuum between baseline and basevalues
            func = blvdata.header.get('DataFunction', [])
            residual = blvdata.func2stream(func, mode='sub', keys=['dx', 'dy', 'dz'])
            # print ("Here", residual.ndarray)
            resDIx, resDIstdx = residual.mean('dx', std=True, percentage=10)
            resDIy, resDIstdy = residual.mean('dy', std=True, percentage=10)
            resDIz, resDIstdz = residual.mean('dz', std=True, percentage=10)
            reportmsg += "Step 5: average residual between baseline and basevalues is: {}={:.3f}{}, {}={:.5f}{}, {}={:.3f}{} \n".format(
                headx, resDIx, unitx, heady, resDIy, unity, headz, resDIz, unitz)
            if resDIx > 0.1 or resDIz > 0.1:
                reportmsg += "Step 5: -> found minor deviations between baseline and basevalues\n"
                succlst[4] = 2
            if resDIx > 0.5 or resDIz > 0.5:
                reportmsg += "Step 5: !!! found relatively large deviations between baseline and basevalues\n"
                warningmsg += "Step 5: check deviations between baseline and basevalues\n"
                succlst[4] = 3

            # overall baseline variation
            # get maximum and minimum of the function for x and z
            amplitude = blvdata.func2stream(func, mode='values', keys=['dx', 'dy', 'dz'])
            # amplitude = amplitude._convertstream('hdz2xyz') ## TODO this is not correct !!!
            ampx = amplitude.amplitude('dx')
            ampy = amplitude.amplitude('dy')
            ampz = amplitude.amplitude('dz')
            maxamp = np.max([ampx, ampy, ampz])  # TODO declination!!!
            reportmsg += "Step 5: maximum amplitude of baseline is {:.1f}{} \n".format(maxamp, unitx)
            amplitude = amplitude._copy_column('dx', 'x')
            amplitude = amplitude._copy_column('dy', 'y')
            amplitude = amplitude._copy_column('dz', 'z')
            # PLEASE note: amplitude test is currently, effectively only testing x and z component
            #              this is still useful as maximum amplitudes are expected in these components
            if maxamp > 5:
                reportmsg += "Step 5: !!! amplitude of adopted baseline exceeds INTERMAGNET threshold of 5 nT\n"
                warningmsg += "Step 5: adopted baseline shows relatively high variations - could be related to baseline jumps - please review data\n"
                succlst[4] = 3

            # check baseline complexity


        else:
            reportmsg += "Step 5: could not open baseline data \n"
            warningmsg += "Step 5: failed to open baseline data\n"
            succlst[4] = 6

        if succlst[4] <= 2:
            reportmsg += "\n----> Step 5: successfully passed\n"
        elif succlst[4] <= 4:
            reportmsg += "\n----> Step 5: passed with some issues to be reviewed\n"
        self.changeStatusbar("Step 5: measured and adopted basevalues ... Done")

        report = createReport(reportmsg, warningmsg, errormsg)
        opendlg = True
        if opendlg:
            dlg = CheckDataReportDialog(None, title='Data check step 5 - report', report=report, rating=succlst[4],
                                        step=list(map(str, succlst)), laststep=laststep)
            dlg.ShowModal()
            if dlg.moveon:
                saveReport(dlg.contlabel, dlg.report)
            else:
                dlg.Destroy()
                # locale.setlocale(locale.LC_TIME, old_loc)
                self.changeStatusbar("Ready")
                return

    if checkparameter['step6']:
        succlst[5] = 1
        reportmsg += "\n"
        reportmsg += "#######################################\n"
        reportmsg += "Step 6:\n"
        reportmsg += "#######################################\n"
        reportmsg += "Def.: yearly means, consistency of meta information in all files\n\n"

        # internally check yearmean  (Not yet)
        # check whether all data files contain the same means

        def diffs(success, hmean1, zmean1, hmean2, zmean2, source1='blv', source2='iaf', threshold=0.9):
            repmsg = ''
            warnmsg = ''
            if not np.isnan(hmean1) and not np.isnan(hmean2):
                diffh = np.abs(hmean1 - hmean2)
                diffz = np.abs(zmean1 - zmean2)
                if diffh < threshold and diffz < threshold:
                    repmsg += "Step 6: yearly means between {} and {} files are consistent within an threshold of {} nT\n".format(
                        source1, source2, threshold)
                else:
                    repmsg += "Step 6: yearly means differ between {} and {} files. {}: H={:.2f}nT,Z={:.2f}nT; {}: H={:.2f}nT, Z={:.2f}nT \n".format(
                        source1, source2, source1, hmean1, zmean1, source2, hmean2, zmean2)
                    success = 5
                    if source1 == 'yearmean':
                        repmsg += "    ->   difference might be related to data jumps within the Yearmean file, which are considered when reading this file\n"
                        success = 4
                    warnmsg += "Step 6: yearly means differ between {} and {} files\n".format(source1, source2)
            else:
                repmsg += "Step 6: did not compare yearly means of {} and {} data \n".format(source1, source2)
            return repmsg, warnmsg, success

        reportmsg += "Step 6: yearly means \n"
        reportmsg += "-----------------------\n"
        # blv yearly means
        blvhmean = np.nan
        blvfmean = np.nan
        if not blvpath == '':
            try:
                le = blvdata.length()[0]
            except:
                blvdata = read(blvpath)
            blvhmean = blvdata.header.get('DataScaleX')
            blvfmean = blvdata.header.get('DataScaleZ')
        else:
            reportmsg += "Step 6: blv file not available \n"
            succlst[5] = 6

        # iaf yearly means
        minhmean = np.nan
        minfmean = np.nan
        try:
            if mindataback.header.get('DataComponents', '').startswith('hdz'):
                mindataback = mindataback._convertstream('hdz2xyz')
            if checkchoice == 'full' and mindataback.length()[0] > 0:
                minxmean = mindataback.mean('x', percentage=50)
                minymean = mindataback.mean('y', percentage=50)
                minzmean = mindataback.mean('z', percentage=50)
                minhmean = np.sqrt(minxmean * minxmean + minymean * minymean)
                minfmean = np.sqrt(minxmean * minxmean + minymean * minymean + minzmean * minzmean)

            rep, warn, succlst[5] = diffs(succlst[5], blvhmean, blvfmean, minhmean, minfmean)
            reportmsg += rep
            warningmsg += warn
        except:
            reportmsg += "Step 6: could not determine yearly means from IAF - data not availbale \n"

        # yearmean yearly means
        yearmeanh = np.nan
        yearmeanf = np.nan
        if not yearmeanpath == '':
            yearmeandata = read(yearmeanpath, debug=True)
            if not yearmeandata.length()[0] > 0:
                warningmsg += "Step 6: !!! Could not read yearmean data. Please check manually!\n"
                reportmsg += "Step 6: !!! Could not read yearmean data. Please check manually!\n"
                succlst[5] = 4
            else:
                yearmeanx = yearmeandata.ndarray[1][-1]
                yearmeany = yearmeandata.ndarray[2][-1]
                yearmeanz = yearmeandata.ndarray[3][-1]
                yearmeanh = np.sqrt(yearmeanx * yearmeanx + yearmeany * yearmeany)
                yearmeanf = np.sqrt(yearmeanx * yearmeanx + yearmeany * yearmeany + yearmeanz * yearmeanz)
                # extract data for year
                rep, warn, succlst[5] = diffs(succlst[5], yearmeanh, yearmeanf, minhmean, minfmean, source1='yearmean',
                                              source2='iaf', threshold=0.6)
                reportmsg += rep
                warningmsg += warn
                rep, warn, succlst[5] = diffs(succlst[5], yearmeanh, yearmeanf, blvhmean, blvfmean, source1='yearmean',
                                              source2='blv', threshold=1.0)
                reportmsg += rep
                warningmsg += warn
                reportmsg += "Step 6: yearlmean.imo contains data from {} until {} \n".format(
                    num2date(yearmeandata.ndarray[0][0]).year, num2date(yearmeandata.ndarray[0][-1]).year)
        else:
            reportmsg += "Step 6: yearlmean not available \n"
            succlst[5] = 6

        if not seconddata == 'None':
            primeheader = secdata.header
        elif mindata:
            primeheader = mindata.header
        else:
            primeheader = {}

        reportmsg += "\nStep 6: meta information \n"
        reportmsg += "-----------------------\n"
        excludelist = ['DataFormat', 'SensorID', 'DataComponents', 'DataSamplingRate', 'DataPublicationDate', 'col-f',
                       'col-x', 'col-y', 'col-z', 'col-df', 'unit-col-f', 'unit-col-x', 'unit-col-y', 'unit-col-z',
                       'unit-col-df', 'DataSamplingFilter']
        floatlist = {'DataElevation': 0, 'DataAcquisitionLongitude': 2, 'DataAcquisitionLatitude': 2}
        secmsg = ''
        minmsg = ''
        yearmsg = ''
        if not primeheader == {}:
            if not seconddata == 'None' and mindata and yearmeandata:
                for key in secdata.header:
                    refvalue = str(secdata.header.get(key))
                    compvalue1 = str(mindata.header.get(key, ''))
                    compvalue2 = str(yearmeandata.header.get(key, ''))
                    if not key.startswith('col') and not key.startswith('unit'):
                        keyname = key.replace('Data', '').replace('station', '')
                        secmsg += "Step 6: (Secondary (ImagCDF/IAGA) meta) {}: {}\n".format(keyname, refvalue)
                    if key in floatlist:
                        refvalue = np.round(float(refvalue), floatlist.get(key))
                        if not compvalue1 == '':
                            compvalue1 = np.round(float(compvalue1), floatlist.get(key))
                        if not compvalue2 == '':
                            compvalue2 = np.round(float(compvalue2), floatlist.get(key))
                    if not compvalue1 == '' and not key in excludelist:
                        if not refvalue == compvalue1:
                            OK = False
                            try:
                                if refvalue.lower()[:3] == compvalue1.lower()[:3]:  # check capitals
                                    OK = True
                            except:
                                pass
                            if not OK:
                                reportmsg += "Step 6: !!! Found differences for {} between {} and {}: {} unequal {}\n".format(
                                    key, 'sec', 'min', refvalue, compvalue1)
                                succlst[5] = 3
                    if not compvalue2 == '' and not key in excludelist:
                        if not refvalue == compvalue2:
                            reportmsg += "Step 6: !!! Found differences for {} between {} and {}: {} unequal {}\n".format(
                                key, 'sec', 'year', refvalue, compvalue2)
                            succlst[5] = 3
            if mindata and yearmeandata:
                for key in mindata.header:
                    refvalue = mindata.header.get(key)
                    compvalue1 = yearmeandata.header.get(key, '')
                    if not key.startswith('col') and not key.startswith('unit'):
                        keyname = key.replace('Data', '').replace('station', '')
                        minmsg += "Step 6: (Primary (IAF) meta) {}: {}\n".format(keyname, refvalue)
                        if not compvalue1 == '':
                            yearmsg += "Step 6: (yearmean meta) {}: {}\n".format(keyname, compvalue1)
                    if key in floatlist:
                        refvalue = np.round(float(refvalue), floatlist.get(key))
                        if not compvalue1 == '':
                            compvalue1 = np.round(float(compvalue1), floatlist.get(key))
                    if not compvalue1 == '' and not key in excludelist:
                        if not refvalue == compvalue1:
                            reportmsg += "Step 6: !!! Found differences for {} between {} and {}: {} unequal {}\n".format(
                                key, 'min', 'year', refvalue, compvalue1)
                            succlst[5] = 3

            metamsg = secmsg + minmsg + yearmsg
            reportmsg += metamsg

        if succlst[5] <= 2:
            reportmsg += "\n----> Step 6: successfully passed\n"
        elif succlst[5] <= 4:
            reportmsg += "\n----> Step 6: passed with some issues to be reviewed\n"
        self.changeStatusbar("Step 6: yearly means and meta information ... Done")

        report = createReport(reportmsg, warningmsg, errormsg)
        opendlg = True
        if opendlg:
            dlg = CheckDataReportDialog(None, title='Data check step 6 - report', report=report, rating=succlst[5],
                                        step=list(map(str, succlst)), laststep=laststep)
            dlg.ShowModal()
            if dlg.moveon:
                saveReport(dlg.contlabel, dlg.report)
            else:
                dlg.Destroy()
                # locale.setlocale(locale.LC_TIME, old_loc)
                self.changeStatusbar("Ready")
                return

    if checkparameter['step7']:
        succlst[6] = 1
        reportmsg += "\n"
        reportmsg += "#######################################\n"
        reportmsg += "Step 7:\n"
        reportmsg += "#######################################\n"
        reportmsg += "Def.: checking K values\n\n"

        posk = DataStream().KEYLIST.index('var1')
        # compare content of dka and iaf
        if dkadata:
            dkadata = read(dkapath, debug=True)
            if not dkadata.length()[0] > 0:
                warningmsg += 'Step 7: Could not read provided dka file !!!\n'
            else:
                if dkadata.amplitude('var1') > 9:
                    warningmsg += 'Step 7: k values in DKA file exceed k level 9 !!!\n'
                    succlst[6] = 4
        try:
            iafk, fail = readMinData(checkchoice, 'iaf', iafpath, month, rmonth, resolution='k', debug=False)
            if iafk.amplitude('var1') > 9:
                warningmsg += 'Step 7: k values in IAF exceed 9 !!!\n'
                succlst[6] = 5
            streamid = self._initial_read(iafk)
            if streamid:  # will create a new input into datadict
                self._initial_plot(streamid)
            # if self.InitialRead(iafk):
            #    self.OnInitialPlot(self.plotstream)
        except:
            warningmsg += "Step 7: Could not extract k values from IAF file\n"
            dayprob = True
            succlst[6] = 5

        if dkadata and succlst[6] < 5:
            kdiffs = subtractStreams(iafk, dkadata)
            if kdiffs.length()[0] > 0:
                posk = DataStream().KEYLIST.index('var1')
                for el in kdiffs.ndarray[posk]:
                    if el >= 0.1:
                        warningmsg += 'Step 7: difference between k in IAF and DKA files at {}: IAF: {}, DKA: {}\n'.format(
                            num2date(kdiffs.ndarray[0][idx]).replace(tzinfo=None), iafk.ndarray[posk][idx],
                            dkadata.ndarray[posk][idx])
                        succlst[6] = 3
            else:
                warningmsg += 'Step 7: (optional) k value check with DKA not performed.\n'
                succlst[6] = 2
        else:
            # warningmsg += 'Step 7: k value check not performed\n'
            # succlst[6] = 2
            pass

        if succlst[6] <= 2:
            reportmsg += "Step 7: k values ... OK\n"
            reportmsg += "\n----> Step 7: successfully passed\n"
        elif succlst[6] <= 4:
            reportmsg += "\n----> Step 7: passed with some issues to be reviewed\n"
        self.changeStatusbar("Step 7: k values OK ... Done")

        report = createReport(reportmsg, warningmsg, errormsg)
        opendlg = True
        if opendlg:
            dlg = CheckDataReportDialog(None, title='Data check step 6 - report', report=report, rating=succlst[6],
                                        step=list(map(str, succlst)), laststep=laststep)
            dlg.ShowModal()
            if dlg.moveon:
                saveReport(dlg.contlabel, dlg.report)
            else:
                dlg.Destroy()
                # locale.setlocale(locale.LC_TIME, old_loc)
                self.changeStatusbar("Ready")
                return

    # locale.setlocale(locale.LC_TIME, old_loc)


self.changeStatusbar("Check data finished - Ready")

