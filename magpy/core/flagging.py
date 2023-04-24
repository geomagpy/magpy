# -*- coding: utf-8 -*-

"""
Testing flagging:
import flagging as fl
anotherflaglist = fl.load("/Users/leon/Cloud/Software/MagPyAnalysis/FlagSignatures/Flagging/flags_Back-LEMI036_1_0002_0002_quietD_2021_average_emd.json_217.json",debug=True)
flaglist = fl.load("/Users/leon/Cloud/Software/MagPyAnalysis/FlagSignatures/Training/original_flags_2021def.json",debug=True)
fl1 = flaglist.copy()
fl1.stats()
fl2 = fl1.modify(mode='delete', parameter='flagnumber', value=1)
fl2.stats()
fl1.stats()
fl3 = fl1.convert2version()
fl4 = fl1.join(anotherflaglist)
fl6 = flaglist.add("LEMI036_1_0002", "x", 3, "Test", "2022-08-15T23:30:30")
fl7 = fl1.union(remove=True, flagids=[1], debug = False)
fl7.save("/Users/leon/Cloud/Software/MagPyAnalysis/FlagSignatures/Training/mynewflags.json")
fl8 = fl1.rename_nearby({"comment":"lightning"},{"comment":"threshold"})
fl9 = fl8.drop({"comment":"no data"})
fl10 = fl9.set_labelkey()
fl10.save("/Users/leon/Cloud/Software/MagPyAnalysis/FlagSignatures/Training/mynewflags2.json")

"""
from magpy.stream import * # os, num2date
import copy as cp

class flags(object):
    """
    DESCRIPTION:
        A list object with flagging information
        The list should contain information according to FLAGKEYS
        Flag ID's are listed version specific in FLAGID dictionary.

        Some methods support a options dictionary

    APPLICTAION:
        Initiate flags with >>> flag = flags().


    EXAMPLES:


    METHODS OVERVIEW:
    ----------------------------

    - flag.union(self, options):
    - flag.clean(options):
    - flag.add(...):
    - flag.extract(self, searchdict):
    - flag.drop(self, searchdict):
    - flag.join(self, newflag(s)):
    - flag.save(self, path):
    ? flag.sort(self,by):
    - flag.rename_nearby(self, searchdict, replacedict):
    - flag.modify(...):             modes delete, select, replace
    - flag.stats(...):              provides stats on a flaglist
    - flag.copy():                  creates a deep copy of the flags object
    - flag.set_labelvalue():        replace label IDs with names
    - flag.set_labelkey():          replace label names with IDs
    - flag.l2d():                   creates flagdict from flaglist
    - flag.d2l():                   creates flaglist from flagdict
    - flag.convert2version(...):    transforms to a specific version
    ? flag.convert2magpy(...):      transforms to a specific magpyversion

    - load(self, options):  # load function supports import of old versions

    """

    def __init__(self, flaglist=None, flagdict=None):
        """
        Description
            currently supports two flagging containers (list and dict)
            at the moment a list container related to the original MagPy
            version is set as default
            The default structure should be changed to dictionary
        """
        if flagdict==None:
            flagdict={}
        self.flagdict = flagdict
        if flaglist == None:
            flaglist = []
        self.flaglist = flaglist
        if flaglist and not flagdict:
            #print ("No flagdict - constructing it")
            self.flagdict = self.l2d(flaglist)
            # reconstruct list in case flagversion is not originally in it (if i.e. a list is used to init a flags class)  
            self.flaglist = self.d2l(self.flagdict)
        if flagdict and not flaglist:
            #print ("No flaglist - constructing it")
            self.flaglist = self.d2l(flagdict)


        self.FLAGKEYS = ['starttime',         # datetime
                    'endtime',           # datetime
                    'components',        # list like [1,2,3] ref. to columns, or ['x','y'] ref. to keys
                    'id',                # integer number
                    'comment',           # text without special characters (utf-8)
                    'sensorid',          # text without special characters (utf-8)
                    'modificationtime',  # datetime
                    'flagversion']       # string like 1.0

        self.FLAGID = {'0.4' : { 0: 'normal data',
                            1: 'automatically flagged for removal',
                            2: 'observer decision: keep data',
                            3: 'observer decision: remove data',
                            4: 'special flag: define in comment'
                          },
                  '1.0' : { 0: 'normal data',
                            1: 'automatically flagged to keep',
                            2: 'automatically flagged for removal',
                            3: 'observer decision: keep data',
                            4: 'observer decision: remove data',
                            5: 'special flag: define in comment'
                          }
                  }

        self.FLAGLABEL = {  '000':['','normal','ok'],       # datetime
                            '001':['lightning','thunderstorm'],           # datetime
                            '012':['pc2'],                  # list like [1,2,3] ref. to columns, or ['x','y'] ref. to keys
                            '013':['pc3'],                  # integer number
                            '014':['pc4'],                  # text without special characters (utf-8)
                            '015':['pc5'],                  # text without special characters (utf-8)
                            '016':['pi2'],                  # datetime
                            '020':['ssc','storm onset'],
                            '030':['earthquake','shaking ground'],
                            '050':['moving object','vehicle','car','bike'],
                            '051':['static object','parking','offset'],
                            '052':['train'],
                            '055':['train'],
                            '090':['spike'],
                          }

    def __str__(self):
        return str(self.flaglist)

    def __repr__(self):
        return str(self.flaglist)

    def __getitem__(self, var):
        return self.flaglist.__getitem__(var)

    #def __setitem__(self, var, value):
    #    self.flaglist.__getitem__(var) = value

    def __len__(self):
        return len(self.flaglist)

    # ------------------------------------------------------------------------
    # Converion methods between list and dictionary
    # ------------------------------------------------------------------------
    def l2d(self,flaglist=None,flagversion='0.4'):
        """
        DESCRIPTION
             converts a flagging list to a flagging dictionary
        """
        if not flaglist:
            mylist = self.flaglist
        else:
            mylist = cp.deepcopy(flaglist)
        # Convert mylist to a dictionary
        mydic = {}
        # if flagversion is contained in list, then extract and remove it
        # if flagversion is NOT contained then either provide it or use default
        if not len(mylist) > 0:
            return {}
        if len(mylist[0]) > 7:
            flagversion = mylist[0][7]
            mylist = [elem[:7] for elem in mylist]
        # get a list of unique sensorid
        sid = [elem[5] for elem in mylist]
        sid = list(set(sid))
        for s in sid:
            slist = [elem[0:5]+elem[6:7] for elem in mylist if elem[5] == s]
            mydic[s] = slist
        mydic["flagversion"] = flagversion
        return mydic

    def d2l(self,flagdict=None):
        """
        DESCRIPTION
             converts a flagging dictionary to a flagging list
        """
        mylist = []
        if not flagdict:
            mydict = self.flagdict
        else:
            mydict = cp.deepcopy(flagdict)
        if not mydict:
            return []
        fv = mydict.get('flagversion','0.4')
        #print ("Flagversion:", fv)
        for s in mydict:
            if not s in ["flagversion"]:
                ml = mydict[s]
                if isinstance(ml,list):
                    do = [el.insert(5,s) for el in ml]
                    do = [el.insert(7,fv) for el in ml]
                    mylist.extend(mydict[s])
        return mylist

    def convert2version(self,version="1.0"):
        """
        DESCRIPTION
             converts a flagging dictionary towards a specific FlagID version
        """
        def change_flagid(oldversion, oldid, newversion):
            newid = 0
            olddic = self.FLAGID.get(oldversion)
            newdic = self.FLAGID.get(newversion)
            olddesc = olddic.get(oldid)
            for el in newdic:
                if newdic.get(el) == olddesc:
                    newid = el
            # IF NOT EXISTING USE NORMAL DATA
            return newid

        myflag = self.copy()
        mylist = []
        mydict = myflag.flagdict
        fv = mydict.get('flagversion','0.4')
        if version == fv:
            return self
        for s in mydict:
            ml = mydict[s]
            print (s,ml)
            if isinstance(ml,list):
                for el in ml:
                    el[3] = change_flagid(fv, el[3], version)
                mydict[s] = ml
        mydict['flagversion'] = version
        return flags(flaglist=None, flagdict=mydict)


    def convert2magpy(self,magpversion="1.0.0"):
        pass

    # ------------------------------------------------------------------------
    # Flag methods in alphabetical order
    # ------------------------------------------------------------------------

    def copy(self):
        """
        DESCRIPTION
            copy data into a new flaglist
        """
        flaglist = []
        if not type(self.flaglist) == list:
            print ("Did not find flaglist - returning original object")
            return self
        flagdict = cp.deepcopy(self.flagdict)
        flaglist = cp.deepcopy(self.flaglist)
        return flags(flaglist,flagdict)


    def trim(self, starttime=None,endtime=None,debug=False):
        """
        DESCRIPTION
            trim flaglist
        PARAMETER
            starttime
            endtime
        """
        fl = self.copy()
        flaglist = fl.flaglist

        if debug:
            print (" trimming flaglist. original lenght: {}".format(len(flaglist)))
        if len(flaglist) > 0:
            if starttime:
                starttime = testtime(starttime)
                flaglist = [elem for elem in flaglist if elem[1] > starttime]
            if endtime:
                endtime = testtime(endtime)
                flaglist = [elem for elem in flaglist if elem[0] < endtime]
        if debug:
            print (" -> new lenght: {}".format(len(flaglist)))
        return flags(flaglist)

    def join(self, newflaglst,debug=False):
        """
        DESCRIPTION
            add data into a flaglist
            please note: join is destructive. The original flaglist will be extended
        """
        flaglist = self.flaglist

        # Get dimensions of flags
        if type(newflaglst) == list:
            newflags = newflaglst
        elif type(newflaglst.flaglist) == list:
            newflags = newflaglst.flaglist
        else:
            print ("Could not interprete provided flags")
            return self

        if not type(newflags[0]) == list:
            # Single line
            newflags = [newflags]

        # check flagging versions:
        try:
            fl1version = flaglist[0][-1]
        except:
            print ("Flaglist seems to be empty - returning newflaglist")
            return flags(newflags)
        fl2version = newflags[0][-1]
        if debug:
            print ("Length flaglist 1", len(flaglist))
            print ("Length flaglist 2", len(newflags))
        if not fl1version == fl2version:
            print ("Flagging versions do not match: {} vs {} - aborting".format(fl1version,fl2version))
            return self
        count=0
        for flagline in newflags:
            if len(flagline) == 8 and flagline[7] in self.FLAGID:
                count+=1
                flaglist.append(flagline)
        if debug:
            print ("added {} flags".format(count))
        return flags(flaglist)


    def extract(self, searchdict, combine='and'):
        """
        DESCRIPTION:
            extract data from flaglist
            Difference to modify with option select:
            modify select only get data with exact match

        EXAMPLE:
            newflaglist = flaglist.get({'comment':'lightning'})
        """
        flagdict = cp.deepcopy(self.flagdict)
        newdict = {}
        sensorlist = []
        def get_selection(sensorlist,searchdict):
            newlist = []
            comments = searchdict.get("comment",[])
            if not isinstance(comments,list):
                comments = [comments]
            flagids = searchdict.get("flagid",[])
            if not isinstance(flagids,list):
                flagids = [flagids]
            begin = searchdict.get("begin")
            end = searchdict.get("end")
            components = searchdict.get("components",[])
            #print (comments,flagids,components)
            if not isinstance(components,list):
                components = [components]
            for el in sensorlist:
                ToAppend=[False,False,False]
                if components:
                    if el[2] in components:
                        ToAppend[0]=True
                else:
                    ToAppend[0]=True
                if flagids:
                    if el[3] in flagids:
                        ToAppend[1]=True
                else:
                    ToAppend[1]=True
                if comments:
                    hitlist = []
                    for comment in comments:
                        if el[4].find(comment)>-1:
                            hitlist.append(True)
                        else:
                            hitlist.append(False)
                        if combine in ["and","&","AND"]:
                            tester = all(hitlist)
                        else:
                            tester = any(hitlist)
                        ToAppend[2]=tester
                else:
                    ToAppend[2]=True
                #print (ToAppend)
                tester = all(ToAppend)
                #print (tester)
                if tester:
                    newlist.append(el)

            return newlist

        if "sensorid" in searchdict:
            sensorlist = flagdict.get(searchdict.get("sensorid"))
        if sensorlist:
            selectedflags = get_selection(sensorlist,searchdict)
            sensorid = searchdict.get("sensorid")
            newdict[sensorid] = selectedflags
        else:
            selectedflags=[]
            for sensorid in flagdict:
                sensorlist = flagdict.get(sensorid)
                if isinstance(sensorlist,list):
                    selflags = get_selection(sensorlist,searchdict)
                    if not selectedflags and selflags:
                        selectedflags = selflags
                    else:
                        selectedflags.join(selflags)
                newdict[sensorid] = selectedflags
        #newflag.flaglist = newflag.d2l(flagdict)
        newdict["flagversion"] = flagdict.get("flagversion")

        return flags([],newdict)

    def add(self, sensorid, keys, flagnumber, comment, startdate, enddate=None,debug=False):
        """
        DEFINITION:
            Add a specific input to a flaglist
            Flaglist elements look like
            [st,et,key,flagnumber,comment,sensorid,now]

        APPLICATION:
            newflaglist = flaglist.add("LEMI036_1_0002", "x", 3, "Test", "2022-08-15T23:30:30")
            newflaglist = flaglist.add("LEMI036_1_0002", ["y","z"], 3, "Test2", "2022-08-15T23:30:30",, "2022-08-15T23:45:00")
            newflaglist = flaglist.add("LEMI025_22_0003", "all", 3, "Test3", "2022-08-15T23:30:30",, "2022-08-15T23:45:00")
        """
        flaglist = cp.deepcopy(self.flaglist)
        # convert start and end to correct format
        st = testtime(startdate)
        if enddate:
            et = testtime(enddate)
        else:
            et = st
        now = datetime.utcnow()
        if not keys:
            return self
        elif keys in ['all','All','ALL']:
            keys = KEYLIST
        elif not isinstance(keys,list):
            keys = [keys]
        for key in keys:
            flagelem = [st,et,key,flagnumber,comment,sensorid,now]
            exists = [elem for elem in flaglist if elem[:6] == flagelem[:5]]
            if len(exists) == 0:
                flaglist.append(flagelem)
            else:
                print ("flags.add: Flag already existing - skipping")
        return flags(flaglist)


    def stats(self,intensive=False, output='stdout'):
        """
        DESCRIPTION:
            Provides some information on flag statistics
        PARAMETER:
            flaglist   (list) flaglist to be investigated
        APPLICTAION:
            flaglist = db2flaglist(db,'all')
            flaglist.stats()
        """
        flaglist = self.flaglist
        try:
            flagversion = flaglist[0][7]
        except:
            flagversion = "0.4"
        amountlist = []
        outputt = '##########################################\n'
        outputt += '           Flaglist statistics            \n'
        outputt += '##########################################\n'
        outputt += 'Flagging version: {}\n'.format(flagversion)
        outputt += '\n'
        outputt += 'A) Total contents: {}\n'.format(len(flaglist))
        outputt += '\n'
        outputt += 'B) Content for each ID:\n'
        #print (flaglist[0], len(flaglist[0]))
        if len(flaglist[0]) > 6:
            ids = [el[5] for el in flaglist]
            uniquenames = list(set(ids))
        for name in uniquenames:
            amount = len([el[0] for el in flaglist if el[5] == name])
            amountlist.append([name,amount])
            if intensive:
                flagli = [el for el in flaglist if el[5] == name]
                index = [el[3] for el in flagli]
                uniqueindicies = list(set(index))
                reasons = [el[4] for el in flagli]
                uniquereasons = list(set(reasons))
                intensiveinfo = []
                for reason in uniquereasons:
                    num = len([el for el in flagli if reason == el[4]])
                    intensiveinfo.append([reason,num])
                intensiveinfo = sorted(intensiveinfo,key=lambda x: x[1])
                intensiveinfo = ["{} : {}\n".format(e[0],e[1]) for e in intensiveinfo]
                amountlist[-1].append(intensiveinfo)
        amountlist = sorted(amountlist,key=lambda x: x[1])
        for el in amountlist:
            outputt += "Dataset: {} \t Amount: {}\n".format(el[0],el[1])
            if intensive:
                for ele in el[2]:
                    outputt += "   {}".format(ele)
        if output=='stdout':
            print (outputt)
        else:
            return outputt

    def modify(self, mode='select', parameter='key', value=None, newvalue=None, starttime=None, endtime=None,debug=False):
        """
        DEFINITION:
            Select/Replace/Delete information in flaglist
            parameters are key, flagnumber, comment, startdate, enddate=None
            mode delete: if only starttime and endtime are provided then all data inbetween is removed,
                         if parameter and value are provided this data is removed, eventuall
                         only between start and endtime
        APPLICTAION

        """
        flaglist = self.flaglist
        num = 0
        # convert start and end to correct format
        if parameter == 'key':
            num = 2
        elif parameter == 'flagnumber':
            num = 3
        elif parameter == 'comment':
            num = 4
        elif parameter == 'sensorid':
            num = 5
        if debug:
            print ("flags.modify -> running with mode {}".format(mode))

        if mode in ['select','replace'] or (mode=='delete' and value):
            if starttime:
                starttime = testtime(starttime)
                flaglist = [elem for elem in flaglist if elem[1] > starttime]
            if endtime:
                endtime = testtime(endtime)
                flaglist = [elem for elem in flaglist if elem[0] < endtime]
        elif mode == 'delete' and not value:
            print ("Only deleting")
            flaglist1, flaglist2 = [],[]
            if starttime:
                starttime = testtime(starttime)
                flaglist1 = [elem for elem in flaglist if elem[1] < starttime]
            if endtime:
                endtime = testtime(endtime)
                flaglist2 = [elem for elem in flaglist if elem[0] > endtime]
            flaglist1.extend(flaglist2)
            flaglist = flaglist1

        if mode == 'select':
            if num>0 and value:
                if num == 4:
                    flaglist = [elem for elem in flaglist if elem[num].find(value) > 0]
                elif num == 3:
                    flaglist = [elem for elem in flaglist if elem[num] == int(value)]
                else:
                    flaglist = [elem for elem in flaglist if elem[num] == value]
        elif mode == 'replace':
            if num>0 and value:
                for idx, elem in enumerate(flaglist):
                    if num == 4:
                        if elem[num].find(value) >= 0:
                            flaglist[idx][num] = newvalue
                    elif num == 3:
                        if elem[num] == int(value):
                            flaglist[idx][num] = int(newvalue)
                    else:
                        if elem[num] == value:
                            flaglist[idx][num] = newvalue
        elif mode == 'delete':
            if num>0 and value:
                if num == 4:
                    flaglist = [elem for elem in flaglist if elem[num].find(value) < 0]
                elif num == 3:
                    flaglist = [elem for elem in flaglist if not elem[num] == int(value)]
                else:
                    flaglist = [elem for elem in flaglist if not elem[num] == value]

        return flags(flaglist)

    def clean(self,progress=False,debug=False):
        """
        DESCRIPTION:
            identify and remove duplicates from flaglist, only the latest inputs are used
            start, endtime and key are used to identfy duplicates
        PARAMETER:
            flaglist   (list) flaglist to be investigated
        APPLICTAION:
            stream = DataStream()
            flaglist = db2flaglist(db,'all')
            flaglistwithoutduplicates = stream.flaglistclean(flaglist)
        """
        flaglist = self.flaglist
        # first step - remove all duplicates
        testflaglist = ['____'.join([str(date2num(elem[0])),str(date2num(elem[1])),str(elem[2]),str(elem[3]),str(elem[4]),str(elem[5]),str(date2num(elem[6]))]) for elem in flaglist]
        uniques,indi = np.unique(testflaglist,return_index=True)
        flaglist = [flaglist[idx] for idx in indi]

        # second step - remove all inputs without components
        flaglist = [elem for elem in flaglist if not elem[2] == '']

        ## Cleanup flaglist -- remove all inputs with duplicate start and endtime
        ## (use only last input)
        indicies = []
        for ti, line in enumerate(flaglist):
            if progress and ti/1000. == np.round(ti/1000.):
                print ("flags.clean -> current state: {} percent".format(ti/len(flaglist)*100))
            if len(line) > 5:
                inds = [ind for ind,elem in enumerate(flaglist) if elem[0] == line[0] and elem[1] == line[1] and elem[2] == line[2] and elem[5] == line[5]]
            else:
                inds = [ind for ind,elem in enumerate(flaglist) if elem[0] == line[0] and elem[1] == line[1] and elem[2] == line[2]]
            if len(inds) > 1:
                # get inputs dates for all duplicates and select the latest
                dates = [[flaglist[dupind][-1], dupind] for dupind in inds]
                indicies.append(sorted(dates)[-1][1])
            else:
                index = inds[-1]
                indicies.append(index)

        uniqueidx = (list(set(indicies)))
        if debug:
             print ("flags.clean: found {} unique inputs".format(len(uniqueidx)))
        uniqueidx.sort()
        flaglist = [flaglist[idx] for idx in uniqueidx]

        return flags(flaglist)

    def rename_nearby(self,searchdict,renamedict,timerange=timedelta(hours=1), debug=False):
        """
        DESCRIPTION:
            Get flags according to a searchcriteria dictionary
            Then find flags in the vicintiy with another "rename" searchcriteria
            Those flags are then renamed using comments and flagids of searchcrit
        PARAMETER:
            searchdict   (dict)  :  searchcriteria
            renamedict   (dict)  :
        RETURNS:
            flaglist
        EXAMPLES:

        """
        #find {"comment":[],"flagid":3} and replace all {comment,flagid} within timerange with searchdict
        #1. get flags with searchdict
        newflag = flags()
        flaglist = self.flaglist
        reference = self.extract(searchdict)
        #print (reference)
        #print ("----------------------------------------------")
        if debug:
            print (" Got reference data:", len(reference))
        if len(reference) > 0:
            for el in reference:
                #take a time window around the reference element and get all rename elements
                frame = self.trim(starttime=el[0]-timerange,endtime=el[1]+timerange, debug=debug)
                frame = frame.extract(renamedict)
                #print (frame)
                #print ("----------------------------------------------")
                if debug:
                    print ("Extracting a data frame with {} inputs at {}".format(len(frame),el[0]))
                for ele in frame:
                    if "comment" in searchdict:
                        comcol = self.FLAGKEYS.index('comment')
                        ele[comcol] = searchdict.get("comment")
                    if "id" in searchdict:
                        idcol = self.FLAGKEYS.index('id')
                        ele[idcol] = searchdict.get("id")
                    modcol = self.FLAGKEYS.index('modificationtime')
                    ele[modcol] = datetime.utcnow()
                #print (frame)
                if len(frame) > 0:
                    newflag = newflag.join(frame)
                #print (newflag)
        #now join with original record and union to replace old flags
        newflag = self.join(newflag)
        newflag = newflag.union()
        if debug:
            print ("Amount of flags after renaming:", len(newflag))
        return newflag

    def set_labelvalue(self):
        flaglist = self.flaglist
        newflag=[]
        for el in flaglist:
            comcol = self.FLAGKEYS.index('comment')
            cval = el[comcol]
            for key in self.FLAGLABEL:
                cont = self.FLAGLABEL.get(key)
                if cval.find(key) > -1:
                    el[comcol] = cont[0]
            newflag.append(el)
        return flags(newflag)

    def set_labelkey(self):
        flaglist = self.flaglist
        newflag=[]
        for el in flaglist:
            comcol = self.FLAGKEYS.index('comment')
            cval = el[comcol]
            for key in self.FLAGLABEL:
                cont = self.FLAGLABEL.get(key)
                for element in cont:
                    if cval.find(element) > -1:
                        el[comcol] = key
            newflag.append(el)
        return flags(newflag)

    def drop(self,searchdict, debug=True):
        newflag = []
        flaglist = self.flaglist
        reference = self.extract(searchdict)
        for el in flaglist:
            if not el in reference:
                newflag.append(el)
        return flags(newflag)


    def union(self, sr=1, overlap=True, singular=False, remove=False, critamount=20, flagids=None, debug=False):
        """
        DESCRIPTION:
            Method to inspect a flaglist and check for consecutive elements
        PARAMETER:
            sr           (float) :  [sec] Sampling rate of underlying flagged data sequence
            critamount   (int)   :  Amount of maximum allowed consecutive (to be used when removing consecutive data)
            remove       (BOOL)  :  True will replace consecutive data with a new flag, False will remove consecutive data from flaglist
            overlap      (BOOL)  :  if True than overlapping flags will also be combined, comments from last modification will be used
            singular     (BOOL)  :  if True than only single time stamp flags will be investigated (should be spikes)
        OUTPUT:
            flaglist
        EXAMPLES:

        """

        flaglist = cp.deepcopy(self.flaglist)
        if flagids:
            if isinstance(flagids, list):
                uniqueids = flagids
            elif isinstance(flagids, int):
                uniqueids = [flagids]
            else:
                uniqueids = [0,1,2,3,4,5]
        else:
            uniqueids = [0,1,2,3,4,5]

        if not len(flaglist) > 0:
            return flaglist

        # Ideally flaglist is a list of dictionaries:
        # each dictionary consists of starttime, endtime, components, flagid, comment, sensorid, modificationdate
        flagdict = [{"starttime" : el[0], "endtime" : el[1], "components" : el[2].split(','), "flagid" : el[3], "comment" : el[4], "sensorid" : el[5], "modificationdate" : el[6]} for el in flaglist]

        ## Firstly extract all flagging IDs from flaglst
        if len(flaglist[0]) > 6:
            ids = [el[5] for el in flaglist]
            uniquenames = list(set(ids))
        else:
            print ("Found an old flaglist type - aborting")
            return self

        newflaglist = []
        for name in uniquenames:
            if debug:
                print (" Dealing with {}".format(name))
            cflaglist = [el for el in flaglist if el[5] == name]
            # if singular, extract flags with identical start and endtime
            if singular:
                nonsingularflaglist = [el for el in flaglist if el[0] != el[1]]
                testlist = [el for el in flaglist if el[0] == el[1]]
                newflaglist.extend(nonsingularflaglist)
            else:
                testlist = cflaglist

            uniquecomponents = list(set([el[2] for el in testlist]))
            if debug:
                print (" - found flags for components", uniquecomponents)

            for comp in uniquecomponents:
                complist = [el for el in testlist if comp == el[2] and el[3] in uniqueids]
                if debug:
                    print ("  - Inputs for component {} with flagIDs {}: {}".format(comp,uniqueids,len(complist)))
                idxtmp = 0
                testcnt = 0
                while idxtmp < len(complist):
                    complist = complist[idxtmp:]
                    extendedcomplist = []
                    for idx,line in enumerate(complist):
                        tdiff = (line[1]-line[0]).total_seconds()
                        if tdiff > sr-(0.05*sr):
                            # add steps
                            firstt = line[0]
                            lastt = line[1]
                            steps = int(np.ceil(tdiff/float(sr)))
                            for step in np.arange(steps):
                                val0 = firstt+timedelta(seconds=int(step)*sr)
                                extendedcomplist.append([val0,val0,line[2],line[3],line[4],line[5],line[6]])
                            extendedcomplist.append([lastt,lastt,line[2],line[3],line[4],line[5],line[6]])
                        else:
                            extendedcomplist.append(line[:7])
                        if len(extendedcomplist) > 500000:
                            idxtmp = idx+1
                            break
                        idxtmp = idx+1
                    if debug:
                        print ("    -> Individual time stamps: {}".format(len(extendedcomplist)))
                    if overlap:
                        if debug:
                            print ("    -> removing overlaps")
                        # Now sort the extendedlist according to modification date
                        extendedcomplist.sort(key=lambda x: x[-1], reverse=True)
                        # Now remove all overlapping data
                        seen = set()
                        new1list = []
                        for item in extendedcomplist:
                            ti = np.round((item[0]-datetime(1900, 1, 1)).total_seconds(),0)
                            # use a second resolution for identifying identical inputs
                            if ti not in seen:
                                new1list.append(item)
                                seen.add(ti)
                        extendedcomplist = new1list
                        if debug:
                            print (" - After overlap removal - time stamps: {}".format(len(extendedcomplist)))

                    # now combine all subsequent time steps below sr to single inputs again
                    extendedcomplist.sort(key=lambda x: x[0])
                    # Important to consider FlagID in the following, otherwise ok (0,2) flags are joined with remove (1,3) flags
                    new2list = []
                    startt = None
                    endt = None
                    tmem = None
                    for idx,line in enumerate(extendedcomplist):
                        idnum0 = line[3]
                        if idx < len(extendedcomplist)-1:
                            t0 = line[0]
                            t1 = extendedcomplist[idx+1][0]
                            idnum1 = extendedcomplist[idx+1][3]
                            tdiff = (t1-t0).total_seconds()
                            if tdiff <= sr and idnum0 == idnum1:
                                if not tmem:
                                    tmem = t0
                                endt = None
                            else:
                                startt = t0
                                if tmem:
                                    startt = tmem
                                endt = t0
                        else:
                            t0 = line[0]
                            startt = t0
                            if tmem:
                                startt = tmem
                            endt = t0                            
                        if startt and endt:
                            # add new line
                            if not remove:
                                new2list.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                                newflaglist.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                            else:
                                if idnum0 == 1 and (endt-startt).total_seconds()/float(sr) >= critamount:
                                    # do not add subsequent automatic flags 
                                    pass
                                else:
                                    new2list.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                                    newflaglist.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                            tmem = None
                    if debug:
                        print ("    -> After recombination: {}".format(len(new2list)))

        return flags(newflaglist)

    def save(self, path=None, destination="file",overwrite=False):
        """
        DEFINITION:
            Save list e.g. flaglist to file (using json or pickle) or db.

        PARAMETERS:
        Variables:
            - path:  (str) Path to data files in form:

        RETURNS:
            - True if succesful otherwise False
        EXAMPLE:
            >>> flaglist.save('/my/path/myfile.pkl')

        """
        print("Saving flaglist ...")
        flag = self.copy()
        if not self.flaglist:
            print("error 1")
            return False
        if not path:
            path = 'myfile.json'
        if not overwrite:
            existflag = load(path)
            if existflag.flaglist:
                existflag.join(flag)
                flags = existflag
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        if path.endswith('.json'):
            print(" -- using json format ")
            try:
                import json
                def dateconv(d):
                    # Converter to serialize datetime objects in json
                    if isinstance(d,datetime):
                        return d.__str__()
                with open(path,'w',encoding='utf-8') as file:
                    file.write(unicode(json.dumps(self.flagdict,ensure_ascii=False,indent=4,default=dateconv)))
                print("saveflags: list saved to a json file: {}".format(path))
                return True
            except:
                return False
        else:
            print(" -- using pickle")
            try:
                # TODO: check whether package is already loaded
                from pickle import dump
                dump(self.flaglist,open(path,'wb'))
                print("saveflags: list saved to {}".format(path))
                return True
            except:
                return False

def testtime(time):
    """
    Check the date/time input and returns a datetime object if valid:

    ! Use UTC times !

    - accepted are the following inputs:
    1) absolute time: as provided by date2num
    2) strings: 2011-11-22 or 2011-11-22T11:11:00
    3) datetime objects by datetime.datetime e.g. (datetime(2011,11,22,11,11,00)

    """

    if isinstance(time, float) or isinstance(time, int):
        try:
            timeobj = num2date(time).replace(tzinfo=None)
        except:
            raise TypeError
    elif isinstance(time, str): # test for str only in Python 3 should be basestring for 2.x
        try:
            timeobj = datetime.strptime(time,"%Y-%m-%d")
        except:
            try:
                timeobj = datetime.strptime(time,"%Y-%m-%dT%H:%M:%S")
            except:
                try:
                    timeobj = datetime.strptime(time,"%Y-%m-%d %H:%M:%S.%f")
                except:
                    try:
                        timeobj = datetime.strptime(time,"%Y-%m-%dT%H:%M:%S.%f")
                    except:
                        try:
                            timeobj = datetime.strptime(time,"%Y-%m-%d %H:%M:%S")
                        except:
                            try:
                                # Not happy with that but necessary to deal
                                # with old 1000000 micro second bug
                                timearray = time.split('.')
                                if timearray[1] == '1000000':
                                    timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")+timedelta(seconds=1)
                                else:
                                    # This would be wrong but leads always to a TypeError
                                    timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")
                            except:
                                try:
                                    timeobj = num2date(float(time)).replace(tzinfo=None)
                                except:
                                    raise TypeError
    elif not isinstance(time, datetime):
        raise TypeError
    else:
        timeobj = time

    return timeobj


def load(path, sensorid=None, begin=None, end=None, source='file', format='', debug=False):
    """
    DEFINITION:
        Load list e.g. flaglist from file using pickle.

    PARAMETERS:
    Variables:
        - path:  (str) Path to data files in form:
        - begin: (datetime)
        - end:   (datetime)
        - source:(string)   can be file/webservice, db (if db, path needs to contain the credential shortcut)

    RETURNS:
        - flags obsject (e.g. flaglist)

    TODO:
        - Pickle import

    EXAMPLE:
        >>> import magpy.core.flagging as flags
        >>> flaglist = flags.load('/my/path/myfile.pkl')

    """
    flaglist = flags()
    if not path:
        return flaglist

    if source == "db":
        print ("Source from DATABASE - to be done")
    else:
        if "://" in path:
            print ("Source from WEBPATH (only json supported) - to be done")
        else:
            if not os.path.isfile(path):
                if debug:
                    print (" -> Could not find a file at the given path {}".format(path))
                return flags([])
            if format=="":
                if debug:
                    print (" -> format NOT manually provided")
                if path.endswith(".pkl"):
                    if debug:
                        print (" -> extension points towards a pickle file")
                    format="pkl"
                elif path.endswith(".json"):
                    if debug:
                        print (" -> extension points towards a json file")
                    format="json"
                else:
                    if debug:
                        print (" -> format unclear, asuming a json file")
                    format="json"
            else:
                if debug:
                    print (" -> format {} provided".format(format))

            if format == 'json':
                mylist = _readJson(path,sensorid=sensorid,begin=begin,end=end,debug=debug)
            elif format == 'pkl':
                mylist = _readPickle(path,sensorid=sensorid,begin=begin,end=end,debug=debug)
            return flags(mylist)

def _dateparser(dct):
    # Convert dates in dictionary to datetime objects
    for (key,value) in dct.items():
        for i,line in enumerate(value):
            for j,elem in enumerate(line):
                if str(elem).count('-') + str(elem).count(':') == 4:
                    try:
                        try:
                            value[i][j] = datetime.strptime(elem,"%Y-%m-%d %H:%M:%S.%f")
                        except:
                            value[i][j] = datetime.strptime(elem,"%Y-%m-%d %H:%M:%S")
                    except:
                        pass
        dct[key] = value
    return dct

def _readJson(path, sensorid=None, begin=None, end=None, debug=False):
    import json
    if debug:
        print ("Reading a json style flaglist...")
    with open(path,'r') as file:
        mydic = json.load(file,object_hook=_dateparser)
    # test for flagversion flagnumber
    fv = mydic.get('flagversion','0.4')
    if sensorid:
        mylist = mydic.get(sensorid,'')
        do = [el.insert(5,sensorid) for el in mylist]
        do = [el.insert(7,fv) for el in mylist]
    else:
        mylist = []
        for s in mydic:
            ml = mydic[s]
            if isinstance(ml,list):
                do = [el.insert(5,s) for el in ml]
                do = [el.insert(7,fv) for el in ml]
                mylist.extend(mydic[s])
    if begin:
        mylist = [el for el in mylist if el[1] > begin]
    if end:
        mylist = [el for el in mylist if el[0] < end]
    #check if components is a list
    if debug:
        print (" -> Json style loaded. Imported {} flags".format(len(mylist)))
    return mylist

def _readPickle(path,sensorid=None, begin=None, end=None,debug=False):
    from pickle import load as pklload
    mylist = pklload(open(path,"rb"))
    if debug:
        print (len(mylist[0]))
        print("load: list {a} successfully loaded, found {b} inputs".format(a=path,b=len(mylist)))
    if sensorid:
        print(" - extracting data for sensor {}".format(sensorid))
        mylist = [el for el in mylist if el[5] == sensorid]
    if begin:
        mylist = [el for el in mylist if el[1] > begin]
    if end:
        mylist = [el for el in mylist if el[0] < end]
    return mylist
