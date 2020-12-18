# -*- coding: utf-8 -*-

from magpy.stream import *
#from .stream import *

class MagPyFlag(object):
    """
    DESCRIPTION:
        A list object with flagging information
        The list should contain information according to FLAGKEYS
        Flag ID's are listed version specific in FLAGID dictionary.

        Some methods support a options dictionary

    APPLICTAION:
        Initiate flags with >>> flag = MagPyFlag().


    EXAMPLES:


    METHODS OVERVIEW:
    ----------------------------

    - flag.delete(self, searchdict, combine):  # e.g. flag.delete({'sensorid':'xxx','flagid':1 },combine='and')
    - flag.extend(self, otherflaglist):
    - flag.union(self, options):
    - flag.clean(self, options):
    - flag.get(self, searchdict):
    - flag.put(self, newflag):
    - flag.save(self, path):
    - flag.sort(self):
    - flag.modify(self):
    - flag.stats(self):

    - load(self, options):  # load function supports import of old versions

    """

    def __init__(self, flaglist=None):
        if flaglist is None:
            flaglist = []
        self.flaglist = flaglist

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


    # ------------------------------------------------------------------------
    # Flag methods in alphabetical order
    # ------------------------------------------------------------------------

    def copy(self):
        """
        DESCRIPTION
            copy data into a new flaglist
        """
        flaglist = MagPyFlag()

        if not type(self) == list:
            return self

        for el in self:
            flaglist.append(el)
        return flaglist


    def put(self, flags):
        """
        DESCRIPTION
            add data into a flaglist
        """
        flaglist = self.copy()

        # Get dimensions of flags
        if not type(flags) == list:
            return self

        if not type(flags[0]) == list:
            # Single line
            flagadd = [flags]

        for flagline in flagadd:
            if len(flagline) == 8 and flagline[7] in FLAGID:
                flaglist.append(flagline)

        return flaglist


    def get(self, searchdict, combine='and'):
        """
        DESCRIPTION:
            extract data from flaglist

        EXAMPLE:
            newflaglist = flaglist.get({'comment':'lightning'})
        """

        extractedflaglist = MagPyFlags()

        for idx,searchcrit in enumerate(searchdict):
            if combine == 'and' and idx >= 1:
                flaglist = extractedflaglist
            elif combine == 'and' and idx == 0:
                flaglist = self.copy()
            else: # or
                flaglist = self.copy()
            print (searchcrit, searchdict[searchcrit])
            pos = self.FLAGKEYS.index('comment')
            fl = [el for el in flaglist if searchdict[searchcrit] in el[pos]]
            extractedflaglist.put(fl)

        return extractedflaglist


def consecutive_check(flaglist, sr=1, overlap=True, singular=False, remove=False, critamount=20, flagids=None, debug=False):
    """
    DESCRIPTION:
        Method to inspect a flaglist and check for consecutive elements
    PARAMETER:
        sr           (float) :  [sec] Sampling rate of underlying flagged data sequence
        critamount   (int)   :  Amount of maximum allowed consecutive (to be used when removing consecutive data)
        result       (BOOL)  :  True will replace consecutive data with a new flag, False will remove consecutive data from flaglist
        overlap      (BOOL)  :  if True than overlapping flags will also be combined, comments from last modification will be used
        singular     (BOOL)  :  if True than only single time stamp flags will be investigated (should be spikes)
    INPUT:
        flaglist with line like
        [datetime.datetime(2016, 4, 13, 16, 54, 40, 32004), datetime.datetime(2016, 4, 13, 16, 54, 40, 32004), 't2', 3,
         'spike and woodwork', 'LEMI036_1_0002', datetime.datetime(2016, 4, 28, 15, 25, 41, 894402)]
    OUTPUT:
        flaglist

    """
    if flagids:
        if isinstance(flagids, list):
            uniqueids = flagids
        elif isinstance(flagids, int):
            uniqueids = [flagids]
        else:
            uniqueids = [0,1,2,3,4]
    else:
        uniqueids = [0,1,2,3,4]

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
        return flaglist

    newflaglist = []
    for name in uniquenames:
        cflaglist = [el for el in flaglist if el[5] == name]
        # if singular, extract flags with identical start and endtime
        if singular:
            nonsingularflaglist = [el for el in flaglist if el[0] != el[1]]
            testlist = [el for el in flaglist if el[0] == el[1]]
            newflaglist.extend(nonsingularflaglist)
        else:
            testlist = cflaglist

        #if debug:
        #    print (name, len(testlist))
        # extract possible components
        #uniquecomponents = list(set([item for sublist in [el[2].split(',') for el in testlist] for item in sublist]))
        # better use componentgroups
        uniquecomponents = list(set([el[2] for el in testlist]))
        if debug:
            print (" - Components", uniquecomponents)

        for unid in uniqueids:
            idlist = [el for el in testlist if el[3] == unid]
            for comp in uniquecomponents:
                complist = [el for el in idlist if comp == el[2]]
                if debug:
                    print ("  - Inputs for component {} with flagID {}: {}".format(comp,unid,len(complist)))
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
                            extendedcomplist.append(line)
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
                        #print (extendedcomplist)
                        # Now remove all overlapping data
                        seen = set()
                        new1list = []
                        for item in extendedcomplist:
                            ti = item[0]
                            if item[0] not in seen:
                                new1list.append(item)
                                seen.add(ti)
                        extendedcomplist = new1list
                        if debug:
                            print ("    -> After overlap removal - time stamps: {}".format(len(extendedcomplist)))
                    # now combine all subsequent time steps below sr to single inputs again
                    extendedcomplist.sort(key=lambda x: x[0])
                    new2list = []
                    startt = None
                    endt = None
                    tmem = None
                    for idx,line in enumerate(extendedcomplist):
                        if idx < len(extendedcomplist)-1:
                            t0 = line[0]
                            t1 = extendedcomplist[idx+1][0]
                            tdiff = (t1-t0).total_seconds()
                            if tdiff <= sr+(0.05*sr):
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
                                if unid == 1 and (endt-startt).total_seconds()/float(sr) >= critamount:
                                    # do not add subsequent automatic flags
                                    pass
                                else:
                                    new2list.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                                    newflaglist.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                            tmem = None
                    if debug:
                        print ("    -> After recombination: {}".format(len(new2list)))
            #print (unid, len(newflaglist))

    return newflaglist



def load(self, path, sensorid=None, begin=None, end=None, format='json'):
    """
    DEFINITION:
        Load list e.g. flaglist from file using pickle.

    PARAMETERS:
    Variables:
        - path:  (str) Path to data files in form:
        - begin: (datetime)
        - end:   (datetime)

    RETURNS:
        - MagPyFlag obsject (e.g. flaglist)

    EXAMPLE:
        >>> import magpy.flags as flags
        >>> flaglist = flags.load('/my/path/myfile.pkl')
        
    """
    flaglist = MagPyFlag()
    if not path:
        return flaglist
    if not os.path.isfile(path):
        return flaglist
    if not format in ['json','pkl']:
        return flaglist

    if format == 'json':
            import json
            print ("Reading a json style flaglist...")
            def dateparser(dct):
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

            if os.path.isfile(path):
                with open(path,'r') as file:
                    mydic = json.load(file,object_hook=dateparser)
                if sensorid:
                    mylist = mydic.get(sensorid,'')
                    do = [el.insert(5,sensorid) for el in mylist]
                else:
                    mylist = []
                    for s in mydic:
                        ml = mydic[s]
                        do = [el.insert(5,s) for el in ml]
                        mylist.extend(mydic[s])
                if begin:
                    mylist = [el for el in mylist if el[1] > begin]
                if end:
                    mylist = [el for el in mylist if el[0] < end]
                
                return MagPyFlag(mylist)
            else:
                print ("Flagfile not yet existing ...")
                return []

    elif format == 'pkl':
        try:
            from pickle import load as pklload
            mylist = pklload(open(path,"rb"))
            print("loadflags: list {a} successfully loaded, found {b} inputs".format(a=path,b=len(mylist)))
            if sensorid:
                print(" - extracting data for sensor {}".format(sensorid))
                mylist = [el for el in mylist if el[5] == sensorid]
                if begin:
                    mylist = [el for el in mylist if el[1] > begin]
                if end:
                    mylist = [el for el in mylist if el[0] < end]
                #print(" -> remaining flags: {b}".format(b=len(mylist)))
            return MagPyFlag(mylist)
        except:
            return []


