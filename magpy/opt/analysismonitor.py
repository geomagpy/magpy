#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

"""
Tested and running in python 2.7, 3.4
logfiles and hidden pickle file need to be recreated when updating from 2.7 to 3.x
as pickle format has changed

Creating a monitoring dictionary and connecting it to a nagios log file indicator
This app used within the ANALYSIS folder and its logging routines

Concept:

1.) Each dictionary input consists of a key and a value list containing "istwert, comparison, sollwert"
e.g.

To initialize the analysis monitor use:
        mydict = analysismonitor(logfile='/home/leon/CronScripts/MagPyAnalysis/AnalysisMonitor/analog.txt')
        mydict['create_magplot'] = ['success','=','success']
        mydict['upload_magvariation'] = ['success','=','success']
        mydict['threshold_meanfGSMPS3'] = [30000,'>',20000]
        mydict['las_db_input_GSM90'] = [datetime.utcnow(),'>',datetime.utcnow()+timedelta(hours=1)]
        mydict.save('/home/leon/CronScripts/MagPyAnalysis/AnalysisMonitor/test.pkl')

        # KEY Convention
        group_subgroup_(variable)_name/dataid

        plot_
        plot_create_magplot 			[succes,=,succes]
        plot_submit_gravstatus 			[succes,=,succes]

        data_
        data_threshold_f_GSM90_14245_0001: 	[number,>,number]
        data_validity_x_BLV...: 		[valid,=,valid]
        data_presence_filename: 		[success,=,sucess]

        db_
        db_actuality_time_GSM90_14245_0001:	[date,>,date]

        upload_
        upload_gin_wicvariation 		[succes,=,succes]
        upload_zamg_wicvariation 		[succes,=,succes]
        upload_zamg_radon 			[succes,=,succes]
        upload_homepage_wicplot 		[succes,=,succes]
        upload_local_filename 			[date,=,date]  (e.g. test uploads of GWR and BGSInduction)


2.) dictionary is stored as a pickle object

3.) Every ANALYSIS job can access the pickle object, check its contents and eventually update the dictionaries "istwert"
    -> provide method "update(key, istwert, sollwert=None)"
       - updates the "istwert" of key
       - if key not existing it will be created (requires "sollwert")
       - if dictionary not existing it will be created and stored in ANALYSIS/Logs
    -> provide method "check(new dict input(s), path to logfile)"
       if istwert is matching sollwert then (if not yet existing) a line is added to a logfile
          containing "key [sollwert,comp]: OK"
          if a failed input is existing for this key, this line will be deleted
       if istwert not matching sollwert then a line is added to a logfile 
          containing "key [sollwert,comp]: Failed [istwert]"
       issues: what happens if more then one line is changed... nagios log file checker only reports last
          solution: add line with "modified N inputs: key1, key2, ..."
          this line is removed whenever modifications are observed
    -> provide method "check_monitor()"

4.) The check method is further creating a log file  

5.) Nagios is accessing the log file and creates notifications whenever changes occur 


## IMPLEMENTING THE CODE In ANALYSIS:
1. add class analysis to magpy.opt

2. from magpy.opt.analysismonitor import Analysismonitor

2. ADD to ANALYSIS - Jobs:
   from magpy.opt.analysismonitor import Analysismonitor
   mydict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor.log')
   mydict = mydict.load('/home/cobs/ANALYSIS/Logs/AnalysisMonitor.pkl')
   mydict.check({'data_threshold_f_GSMPS3_11224_0001_0001': [mean_f,'>',20000]})
   mydict.check({'upload_zamg_radondata': [status,'=','success']})

5. Activate Nagios Log File checker on /home/cobs/ANALYSIS/Logs/AnalysisMonitor.log

"""


import io, pickle, os, sys
from datetime import datetime, timedelta
import operator
from matplotlib.dates import date2num

class Analysismonitor(dict):
    def __init__(self, logfile=None, dictfile=None):
        homedir = os.path.expanduser('~')
        if not logfile:
            logfile = os.path.join(homedir,'MagpyAnalysisMonitor.log')
        if not dictfile:
            dictfile = os.path.join(os.path.split(logfile)[0],".magpyanalysismonitor.pkl")
        self.logfile = logfile
        self.dictfile = dictfile
        if os.path.isfile(self.dictfile):
            self.load()
        else:
            self['data_threshold_f_Example'] = [30000,'>',20000]
            self.save()
            self.load()

    def load(self, path=None):
        if not path:
            path = self.dictfile
        try:
            fi = io.open(path,'rb') # does not work in Py3  - requires an update of the saved pickle file
            self = pickle.load(fi)
        except:
            print ("analysismonitor: if you switch from python2 to python3 an update of the saved pickle file is necessary")
            sys.exit()
        fi.close()
        return self

    def save(self, path=None):
        if not path:
            path = self.dictfile
        fi = io.open(path,'wb')
        pickle.dump(self, fi)
        fi.close()

    def _readlog(self, path):
        if os.path.isfile(path):
            with io.open(path,'rt', newline='', encoding='utf-8') as fi:
                loglist = [line.strip() for line in fi]
                #fi.readline()
            fi.close()
            return loglist
        else:
            print ("Log file not yet existing - returning empty check list")
            return []

    def _writelog(self, loglist, path, timerange=10):
        print("Saving new logfile to {}".format(path))
        #try:
        # Last line summary
        changelist = [[line.split(' : ')[0],line.split(' : ')[-2]] for line in loglist if len(line) > 0 and not line.startswith('KEY') and not line.startswith('SUMMARY')]
        testtime = datetime.utcnow()-timedelta(minutes=timerange)
        N = [el[0] for el in changelist if datetime.strptime(el[1],"%Y-%m-%dT%H:%M:%S")>testtime]
        lastline = "SUMMARY: {} key(s) changed its/their status since {}:  {}".format(len(N),datetime.strftime(testtime,"%Y-%m-%d %H:%M:%S"),N)
        #print ("LOGLIST looks like:", loglist)
        loglist = [line for line in loglist if not line.startswith('SUMMARY')]
        loglist.append(lastline)
        #print ("LOGLIST looks like:", loglist)
        f = io.open(path,'wt', newline='', encoding='utf-8')
        for log in loglist:
            f.write(log+'\r\n')
        f.close()
        print ("... success")
        return True
        #except:
        #    print ("... failure")
        #    return False

    def _compare(self, valuelist):
        """
        DESCRIPTION:
          Internal compare method
        RETURNS:
          string:   success, failure, invalid
                    - success: comparison worked
                    - failure: comparison shows differences
                    - invalid: comparison is invalid because of e.g. numbers compared to strings etc 
        """
        if not len(valuelist) > 2:
            return 'invalid'

        actual = valuelist[0]
        if sys.version_info >= (3,0,0):
            comp = valuelist[1]
        else:
            comp = valuelist[1].decode('ascii')  # will not work in python3 as str cannot be decoded
        reference = valuelist[2]
        #print (actual, comp, reference)

        if not comp in ['=','<','>','<=','>=','!=']:
            print ("compare symbol not recognized: needs to be one of '=','<','>','<=','>=','!='")
            return 'invalid'

        ops = {'==' : operator.eq,
               '=' : operator.eq,
               '!=' : operator.ne,
               '<=' : operator.le,
               '>=' : operator.ge,
               '>'  : operator.gt,
               '<'  : operator.lt}

        def _check(act,com,ref):
            if ops[com](act, ref):
                return 'success'
            else:
                return 'failure'

        # check reference value:
        try:
            date = reference.date
            datefound = True
        except:
            datefound = False

        if not datefound and self._is_number(reference):
            if not self._is_number(actual): 
                print ("compare symbol not recognized: needs to be one of '=','<','>','<=','>=','!='")
                return 'invalid'
            else:
                status = _check(actual, comp, reference)
                #print ("Number", status)
                return status
        elif datefound:
            status = _check(date2num(actual), comp, date2num(reference))
            #print ("Datetime", status)
            return status
        else:
            # Assuming string
            status = _check(actual, comp, reference)
            #print ("String", status)
            return status

    def _is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def update(self, key, actual, reference=None, comp=None):
        if key in self:
            # key exists:
            # updating elem[0] of valuelist
            ls = self[key]
            ls[0] = actual
            self[key] = ls
        else:
            #print ("Key not yet existing - creating it")
            if not reference:
                print ("updating a new key requires a reference value") 
            if not comp:
                print ("updating a new key requires a comparison") 
            self[key] = [actual,str(comp),reference]
        return self

    def check(self, mydict, logpath=None, dictpath=None):
        """
        DESCRIPTION
            The check method is used to compare current values of the "analysis dictionary"
            with the contents of a "log file". The log file is updated if differences are observed.
            The log file can be easily checked by a nagios routine.
            The check method is called by a scheduled process (e.g. cron)
        PARAMETER
            mydict
        OUTPUT:
            created log file lines like
            key : checkstring : first date of current condition : status
        APPLICATION:
            # Initialize
            mydict = analysismonitor(logfile='/home/leon/CronScripts/MagPyAnalysis/AnalysisMonitor/analog.txt')
            # Load existing inputs
            mydict = mydict.load('/home/leon/CronScripts/MagPyAnalysis/AnalysisMonitor/test.pkl')
            # full check (eventually creates new input)
            mydict.check({'create_magplot':['success','=','success']})
            # full check (check existing inputs)
            mydict.check({'create_magplot':['failure']})
 
        """
        # read logfile and check whether logic is contained
        writenew = False
        cdate = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%S")
        if not logpath:
            logpath = self.logfile
        if not dictpath:
            dictpath = self.dictfile

        #print ("Path", logpath)
        loglist = self._readlog(logpath)
        #print ("Found", loglist)

        keylist = [line.split(' : ')[0] for line in loglist if len(line) > 0]
        statuslist = [line.split(' : ')[-1].strip() for line in loglist if len(line) > 0]
        #print (keylist)

        for key in mydict:
            if not key in keylist:
                print ("Key {} not yet existing - trying to create new input".format(key))
                vallist = mydict[key]
                if len(vallist) < 2:
                    #print (self[key])
                    try:
                        vallist = [vallist[0],self[key][1],self[key][2]]
                    except:
                        print ("Key information incomplete - please add or check")
                        print ("using .check({key:[actual,comp,reference]}")
                        writenew = False
                        break
                status = self._compare(vallist)
                teststring = "{} {} {}".format(vallist[0],vallist[1],vallist[2])
                newline = "{} : {} : {} : {}".format(key, teststring, cdate, status)
                #print ("Line to add", newline)
                loglist.append(newline)
                self.update(key, vallist[0], reference=vallist[2], comp=vallist[1])
                writenew = True
            else:
                print ("Key {} already contained in loglist - checking status".format(key))
                ind = keylist.index(key)
                prevstatus = statuslist[ind]
                vallist = mydict[key]
                if len(vallist) < 2:
                    vallist = [vallist[0],self[key][1],self[key][2]]
                status = self._compare(vallist)
                #print (prevstatus, status)
                if not status == prevstatus:
                    print ("-> Updating status")
                    # remove existing input
                    del loglist[ind]
                    #print ("Status")
                    teststring = "{} {} {}".format(vallist[0],vallist[1],vallist[2])
                    newline = "{} : {} : {} : {}".format(key, teststring, cdate, status)
                    #print ("Line to add", newline)
                    loglist.append(newline)
                    self.update(key, vallist[0])
                    writenew = True
                else:
                    print ("-> Status remaining unchanged")
                    writenew = False

            if writenew:
                print ("Updating dictionary")            
                self.save(dictpath)
                headline = ["KEY       :    CONDITION  :  TIME (condition change)  :  STATUS"]
                if not loglist[0].startswith('KEY'):
                    headline.extend(loglist)
                    loglist = headline
                #print ("Saving", loglist)
                self._writelog(loglist,logpath)

# Test section
test = False
if test:
    individual = False
    if individual:
        mydict = analysismonitor(logfile='/home/leon/CronScripts/MagPyAnalysis/AnalysisMonitor/analog.txt')

        # Saving
        mydict['plot_create_magplot'] = ['success','=','success']
        mydict['upload_zamg_wicvariation'] = ['success','=','success']
        mydict['data_threshold_f_GSMPS3_11224_0001_0001'] = [30000,'>',20000]
        mydict['db_actuality_GSM90_14245_0001_0001'] = [datetime.utcnow(),'>',datetime.utcnow()+timedelta(hours=1)]
        mydict.save()

        # Loading
        mydict = mydict.load()
        print ("Load", mydict)

        # Check
        # read existing logfile
        # if content changed then update logfile (nagios compatible)
        # save new logfile
        mydict.check({'plot_create_magplot':['success','=','success']})
        mydict.check({'data_threshold_f_GSMPS3_11224_0001_0001': [21000,'>',20000]})
        mydict.check({'db_actuality_GSM90_14245_0001_0001': [datetime.utcnow(),'>',datetime.utcnow()+timedelta(hours=1)]})

    combined = True
    if combined:
        mydict = analysismonitor(logfile='/home/leon/CronScripts/MagPyAnalysis/AnalysisMonitor/analog.txt')
        mydict = mydict.load()
        #mydict.check({'plot_create_magplot':['success','=','success']})
        mydict.check({'plot_create_magplot':['success']})
        mydict.check({'data_threshold_f_GSMPS3_11224_0001_0001': [19000]})
        #mydict.check({'data_threshold_f_GSMPS3_11224_0001_0001': [21000,'>',20000]})
        mydict.check({'plot_create_gravplot':['success','=','success']})
        mydict.check({'db_actuality_GSM90_14245_0001_0001': [datetime.utcnow(),'>',datetime.utcnow()-timedelta(hours=1)]})
        #print (mydict)

