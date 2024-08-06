# -*- coding: utf-8 -*-

import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2

from magpy.stream import DataStream, basestring, magpyversion, unicode
from magpy.core.methods import *
from collections import Counter
import copy as cp
import hashlib
import json


class flags(object):
    """
    DESCRIPTION:
        Everything regarding flagging, marking and annotating data in MagPy 2.0 and future versions.
        The main object of a flagging class is a dictionary and not a list as used in MagPy 1.x and 0.x.
        The flagging class contains a number of functions and methods to create, modify and analyse data
        flags. An overview about all supported methods is listed below.
        Each flag is mainly characterized by a SensorID, the sensor which was used to determine the flag,
        a time range and components at which the flag was identified. Type and label are also assigned.
        If a flag should be applied to other sensors, you should use the group item, which contains a list
        of sensors or general groups. Groups is a list of dictionaries

    VERSIONS (MagPy):
        since 0.4 : traditional flagging version as supported by MagPy 0.x and 1.x
        since 1.1.5 : extended flagging version with dict support (has not been used in any productive environment)
        since 2.0 : flagging dictionary including flagging labels and optional fields

    STRUCTURE
        A flagging object consists of a dictionary with unique identifiers as keys and a dictionary as value:
        flags = {'flagid1' : flagvalue1, 'flagid2' : flagvalue2, ...}

            flagid     (int) # unique integer identifier created from values (sensorid, starttime,enddtime,components,flagtype,labelid, modificatontime)

            flagvalue = {
                    'sensorid',          (string) # SensorId of Sensor on which flagging was conducted
                    'starttime',         (datetime) # first flagged data point
                    'endtime',           (datetime) # last flagged data point
                    'components',        (list) # list like [1,2,3] ref. to columns, or ['x','y'] ref. to keys for the sensorid
                    'flagtype',          (int) # 0 (just a comment), 1 (remove for definitiv - auto), 2 (keep for definitive - auto), 3 (remove for definitiv - human), 4 (keep for definitive - human),
                    'labelid',           (string) # string with number i.e. '001', see list below with prefilled options
                    'label',             (string) # name associated with labelid i.e. lightning
                    'comment',           (string) # text without special characters (utf-8)
                    'group',             (list) # define flaggroups-list i.e. [{'magnetism':['x','y','z','f']}, ['meteorology','gravity'] , ['SensorID_1','SensorID_1']
                    'probabilities',     (list) # measure of probabilities - list
                    'stationid',         (string) # stationid of flag
                    'validity',          (char) # character code: d (delete in cleanup), h (invalid/hide), default None
                    'operator',          (string) # text with name/shortcut of flagging person i.e. RL
                    'color',             (string) # None or string with color code to override flagid, will override automatic choice by flagtype
                    'modificationtime',  (datetime) # datetime of last edition
                    'flagversion'}       (string) # string like 2.0

    APPLICTAION:
        Initiate flags with >>> flag = flags().

    EXAMPLES:
        # Load old flags and create new inputs by extracting labels and operators from comments
        # Load a flag list and display contents for a specific sensor without data streams
        # ...


    METHODS OVERVIEW:
    ----------------------------

    class  |  method  |  since version  |  until version  |  runtime test  |  result verification  |  manual  |  *tested by
---------  |  ------  |  -------------  |  -------------  |  ------------  |  --------------  |  ------  |  ----------
**core.flagging** |   |                 |                 |                |                  |          |
flags  |  _list       |  2.0.0          |                 |  yes           |  yes             |         |
flags  |  _set_label_from_comment |  2.0.0 |              |                |                  |      | flagging.load
flags  |  add         |  2.0.0          |                 |  yes           |  yes             |      |
flags  |  copy        |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  create_patch |  2.0.0         |                 |                |                  |    |
flags  |  diff        |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  drop        |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  fprint      |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  join        |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  replace     |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  rename_nearby |  2.0.0        |                 |  yes           |  yes             |    |
flags  |  save        |  2.0.0          |                 |  yes           |                  |    |
flags  |  select      |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  stats       |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  trim        |  2.0.0          |                 |  yes           |  yes             |    |
flags  |  union       |  2.0.0          |                 |  yes           |  yes             |    |
       |  _dateparser |  2.0.0          |                 |                |                  |    | flagging.load
       |  _readJson   |  2.0.0          |                 |                |                  |    | flagging.load
       |  _readPickle |  2.0.0          |                 |                |                  |    | flagging.load
       |  load        |  2.0.0          |                 |                |                  |    |


class  |  method  |  variables  |  description
-----  |  ------  |  ---------  |  -----------
**core.flagging** |   |         |
flags  |  _list       |           | convert flagging dictionary to a list
flags  | _set_label_from_comment | dictionary | interprete comment as label when importing old data
flags  |  add         |  ...      | insert, replace, modify, update flag as defined by its parameters
flags  |  copy        |           | creates a deep copy of the flags object
flags  |  create_patch |          | create a patch dictionary for plotting from flags
flags  |  diff        |  flagobject | differences between two flag objects
flags  |  drop        |  parameter, values | drop selected flags from object
flags  |  fprint      |  sensorid | create a formated output
flags  |  join        |  flagobject | combine two flagging objects
flags  |  replace     |  parameter, value, newvalue | replace selected contents with new values
flags  |  rename_nearby |  parameter, values, timerange | replace contents of nearby flags with reference
flags  |  save        |  path     | save flagging dictionary
flags  |  select      |  parameter, values | select specifc data
flags  |  stats       |  intensive | provides stats on a flaglist
flags  |  trim        |  starttime, endtime | trim the time range of flagging information
flags  |  union        | level, samplingrate, typeforce | combine overlapping time ranges
       |  _dateparser |           | load support - convert strings to datetimes
       |  _readJson   |           | load support - interprete json
       |  _readPickle |           | load support - interprete pickle
       |  load        |  path     | load function needs to support import of old versions


    deprecated in 2.0.0
    - flag.clean(options):             # not necessary any more as id can not be duplicated
    - flag.extract(self, searchdict):  # replaced by select
    - flag.modify(...):                # replaced by replace
    - flag.set_labelvalue():           # replaced by _set_label_from_comment
    - flag.set_labelkey():             # replaced by _set_label_from_comment

    """

    def __init__(self, flagdict=None):
        """
        Description
            currently supports two flagging containers (list and dict)
            at the moment a list container related to the original MagPy
            version is set as default
            The default structure should be changed to dictionary
        """
        if flagdict == None:
            flagdict = {}
        self.flagdict = flagdict

        self.FLAGKEYS = [
            'flagid',  # integer - unique number
            'sensorid',  # SensorId of Sensor on which flagging was conducted
            'starttime',  # datetime
            'endtime',  # datetime
            'components',  # list like [1,2,3] ref. to columns, or ['x','y'] ref. to keys for the sensorid
            'flagtype',
            # integer 0 (just a comment), 1 (remove for definitiv - auto), 2 (keep for definitive - auto), 3 (remove for definitiv - human), 4 (keep for definitive - human),
            'labelid',  # string with number i.e. '001'
            'label',  # name asociated with labelid i.e. lightning
            'comment',  # text without special characters (utf-8)
            'group',
            # define flaggroups-list i.e. ['magnetism'], ['meteorology','gravity'] , ['SensorID_1','SensorID_1']
            'probabilities',  # measure of probabilities - list
            'stationid',  # stationid of flag
            'validity',  # character code: d (delete in cleanup), h (invalid/hide), default None
            'operator',  # text with name/shortcut of flagging person i.e. RL
            'color',  # None or string with color code to override flagid, will override automatic choice by flagtype
            'modificationtime',  # datetime
            'flagversion']  # string like 2.0

        self.FLAGTYPE = {'0.4': {0: 'normal data',
                                 1: 'automatically flagged for removal',
                                 2: 'observer decision: keep data',
                                 3: 'observer decision: remove data',
                                 4: 'special flag: define in comment'
                                 },
                         '1.0': {0: 'normal data',
                                 1: 'automatically flagged to keep',
                                 2: 'automatically flagged for removal',
                                 3: 'observer decision: keep data',
                                 4: 'observer decision: remove data',
                                 5: 'special flag: define in comment'
                                 },
                         '2.0': {0: 'normal data',  # the higher the number the more important
                                 1: 'automatically flagged for removal',
                                 2: 'automatically flagged to keep',
                                 3: 'observer decision: remove data',
                                 4: 'observer decision: keep data'
                                 }
                         }

        # Flaglabes of 2.0 - extendable
        # and associated flagids
        # number ranges:
        # 000     : (0) basic zero-flag (annotation without group)
        # 001-009 : (1,3) HF disturbances of natural or artificial source
        # 010-019 : (2,4) periodic/quasi-periodic natural disturbances with sub-minute to sub-six-hourly periods
        # 020-029 : (2,4) non-periodic natural disturbances with very infrequent nature and sub-weekly time ranges
        # 030-039 : () natural disturbances with physically different effects (i.e. earthquake affecting suspended systems)
        # 040-049 : (2,4) long period natural signals
        # 050-059 : (1,3) sub-minute to sub-hourly anthropogenic disturbances
        # 080-089 : (0) data treatment notations
        # 090-    : (0,1,2,3,4) yet to be classified
        # Flagids of the labels
        self.FLAGLABEL = {'000': 'normal',
                          '001': 'lightning strike',
                          '002': 'spike',
                          '012': 'pulsation pc 2',
                          '013': 'pulsation pc 3',
                          '014': 'pulsation pc 4',
                          '015': 'pulsation pc 5',
                          '016': 'pulsation pi 2',
                          '020': 'ssc geomagnetic storm',
                          '021': 'geomagnetic storm',
                          '022': 'crochete',
                          '030': 'earthquake',
                          '050': 'vehicle passing above',
                          '051': 'nearby moving disturbing source',
                          '052': 'nearby static disturbing source',
                          '053': 'train',
                          '090': 'unknown disturbance'
                          }

    def __str__(self):
        return str(self.flagdict)

    def __repr__(self):
        return str(self.flagdict)

    # def __getitem__(self, var):
    #    return self.flaglist.__getitem__(var)

    # def __setitem__(self, var, value):
    #    self.flaglist.__getitem__(var) = value

    def __len__(self):
        return len(self.flagdict)

    def _list(self, parameter=None):
        """
        DESCRIPTION
            create a list from flagdict consting of desired parameters and main key
        """
        if not parameter:
            return []
        if not isinstance(parameter, (list, tuple)):
            return []
        thelist = []
        for el in self.flagdict:
            cont = []
            cont.append(el)
            for p in parameter:
                if p in self.FLAGKEYS:
                    cont.append(self.flagdict[el].get(p))
            thelist.append(cont)
        return thelist

    def _set_label_from_comment(self, dictionary, parameter='comment'):
        """
        DESCRIPTION
           helps to ill labels and labelids from comments
        PARAMETER
           dictionary like self.flagdict
        APPLICATION
           to be used by import routine
        """
        # select comment which contains any thing from FLAGLABEL
        cdictionary = cp.deepcopy(dictionary)
        res = {}
        ncont = {}
        for id in self.FLAGLABEL:
            label = self.FLAGLABEL.get(id)
            labels = label.split()
            for d in cdictionary:
                econt = cdictionary[d]
                ncont = {}
                for value in labels:
                    if econt.get(parameter).find(value) > -1:
                        econt['labelid'] = id
                        econt['label'] = label
                ncont = econt
            if ncont:
                res[id] = ncont
        return res

        # ------------------------------------------------------------------------

    # Flag methods in alphabetical order
    # ------------------------------------------------------------------------

    def add(self, sensorid=None, starttime=None, endtime=None, components=None, flagtype=0, labelid='000', label='',
            comment='', groups=None, probabilities=None, stationid='', validity='', operator='', color='',
            flagversion='2.0', debug=False):
        """
        DESCRIPTION
            Create a flagging dictionary input oot of given information
            Each flag will be defined by a unqiue flagID which is constructed
            from sensorid,starttime,endtime,",".join(components),flagtype,labelid
            if you want to add a flag with identical information use overwrite?
        """
        # check validity of information
        if not sensorid or not starttime or not endtime or not components:
            print("create_flag: essential information is missing -aborting")
            return {}
        if not isinstance(sensorid, basestring):
            print("create_flag: sensorid need to be a string -. aborting")
            return {}
        starttime = testtime(starttime)
        if not isinstance(starttime, datetime):
            print("create_flag: starttime could not be interpreted as datetime  - aborting")
            return {}
        endtime = testtime(endtime)
        if not isinstance(endtime, datetime):
            print("create_flag: endtime could not be interpreted as datetime  - aborting")
            return {}
        if not isinstance(components, (list, tuple)):
            print("create_flag: components need to be a list  - aborting")
            return {}
        if comment:
            comment = str(comment)
        if groups:
            if not isinstance(group, (list, tuple)):
                groups = None
        if probabilities:
            if not isinstance(probabilities, (list, tuple)):
                probabilities = None
        if labelid and labelid in self.FLAGLABEL:
            label = self.FLAGLABEL.get(labelid)
        modificationtime = datetime.utcnow()

        if not isinstance(self.flagdict, dict):
            return ("Provide a flagging dictionary")

        # create a unique flagid
        idgenerator = "{}{}{}{}{}{}".format(sensorid, starttime, endtime, ",".join(components), flagtype, labelid,
                                            modificationtime)
        if debug:
            print("Creating ID out of ", idgenerator)
        m = hashlib.md5()
        m.update(idgenerator.encode())
        idstring = str(int(m.hexdigest(), 16))[0:12]
        if debug:
            print("ID look like ", idstring)

        # check if id is already exitsing
        flagd = self.flagdict.get(idstring, {})
        if flagd:
            print("input {} already existing - replacing data".format(idstring))
        flagid = idstring
        flagd['sensorid'] = sensorid
        flagd['starttime'] = starttime
        flagd['endtime'] = endtime
        flagd['components'] = components
        flagd['flagtype'] = flagtype
        flagd['labelid'] = labelid
        flagd['label'] = label
        flagd['comment'] = comment
        flagd['groups'] = groups
        flagd['probabilities'] = probabilities
        flagd['stationid'] = stationid
        flagd['validity'] = validity
        flagd['operator'] = operator
        flagd['color'] = color
        flagd['modificationtime'] = modificationtime
        flagd['flagversion'] = flagversion
        self.flagdict[flagid] = flagd

        return self

    def copy(self):
        """
        DESCRIPTION
            copy data into a new flaglist
        RETURNS
            a exact and independent copy of the input stream
        """
        if not isinstance(self.flagdict, dict):
            return self
        flagdict = cp.deepcopy(self)
        return flagdict

    def create_patch(self):
        """
        DESCRIPTION:
            construct a simple patch dictionary for plotting from any given and preselected flaglist
        APPLICTAION:
        RETURNS:
            a patch dictionary for plotting
            of the following structure
            {flagid : {"start":"","end":"","flagtype":"","color":"","labelid":"","label":""}, nextflagid : {}}
        """
        patchdict = {}
        flagdict = self.flagdict
        for d in flagdict:
            cont = {}
            cont['start'] = flagdict[d].get('starttime', None)
            cont['end'] = flagdict[d].get('endtime')
            cont['flagtype'] = flagdict[d].get('flagtype')
            cont['color'] = flagdict[d].get('color')
            cont['labelid'] = flagdict[d].get('labelid')
            cont['label'] = flagdict[d].get('label')
            if cont['start']:
                patchdict[d] = cont
        return patchdict

    def diff(self, compare):
        """
        DESCRIPTION
            compares two dictionaries and returns only the differences as flag object
        APPLICATION
             different_flags = fl.diff(myotherflags)
        """
        value = {k: compare.flagdict[k] for k in set(compare.flagdict) - set(self.flagdict)}
        return flags(value)

    def drop(self, parameter='sensorid', values=None):
        """
        DESCRIPTION:
            drop specific flags from a flagdictionary.
            For dropping specific time ranges use flagdict.trim
            For the given values a "find" command is used. Thus also parts can be searched for
            i.e. comment="incredible lightning strike" will be found by parameter value "lightning"

            Filter options are:
                    'sensorid',          # SensorId of Sensor on which flagging was conducted
                    'components',        # list like [1,2,3] ref. to columns, or ['x','y'] ref. to keys for the sensorid
                    'flagtype',          # integer 0 (just a comment), 1 (remove for definitiv - auto), 2 (keep for definitive - auto), 3 (remove for definitiv - human), 4 (keep for definitive - human),
                    'labelid',           # string with number i.e. '001'
                    'label',             # name asociated with labelid i.e. lightning
                    'comment',           # text without special characters (utf-8)
                    'group',             # define flaggroups-list i.e. ['magnetism'], ['meteorology','gravity'] , ['SensorID_1','SensorID_1']
                    'probabilities',     # measure of probabilities - list
                    'stationid',         # stationid of flag
                    'validity',          # character code: d (delete in cleanup), h (invalid/hide), default None
                    'operator',          # text with name/shortcut of flagging person i.e. RL
            Please note that every search parameter is provided as list
        APPLICTAION:
            flagswithoutcars = flags.drop('labelid',['050'])

        RETURNS:
            a filtered flag dictionary
        """
        if not values:
            return self
        if not isinstance(values, (list, tuple)):
            values = [values]
        para = self.select(parameter=parameter, values=values)
        diff = para.diff(self)
        return diff

    def fprint(self, sensorid, type='date', debug=True):
        """
        DESCRIPTION
           create a formated output of flags
        """
        nfl = self.select(parameter='sensorid', values=[sensorid])
        startdaylist = []
        for el in nfl.flagdict:
            ld = nfl.flagdict.get(el)
            startday = ld.get('starttime').date()
            endday = ld.get('endtime').date()
            if type == 'date':
                if startday == endday:
                    if not startday in startdaylist:
                        startdaylist.append(startday)
                        print(
                            "{} :  {}-{};  {}  (flag: {}, components: {})".format(startday, ld.get('starttime').time(),
                                                                                  ld.get('endtime').time(),
                                                                                  ld.get('label'),
                                                                                  ld.get('flagtype'),
                                                                                  ",".join(ld.get('components'))))
                    else:
                        print("           :  {}-{};  {}  (flag: {}, components: {})".format(ld.get('starttime').time(),
                                                                                            ld.get('endtime').time(),
                                                                                            ld.get('label'),
                                                                                            ld.get('flagtype'),
                                                                                            ",".join(
                                                                                                ld.get('components'))))
                else:
                    print("{} - {};  {}  (flag: {}, components: {})".format(ld.get('starttime'), ld.get('endtime'),
                                                                            ld.get('label'), ld.get('flagtype'),
                                                                            ",".join(ld.get('components'))))
            elif type == 'onlydate':
                if not startday in startdaylist:
                    startdaylist.append(startday)
                    print("{}".format(startday))
            else:
                print(ld)

    def join(self, flagobject, debug=False):
        """
        DESCRIPTION
            add data into a flaglist
            please note: join is non-destructive.
        """
        fl = self.flagdict
        fo = flagobject.flagdict
        new = {**fl, **fo}
        return flags(new)

    def rename_nearby(self, parameter='labelid', values=None, searchcomment='', timerange=timedelta(hours=1),
                      debug=False):
        """
        DESCRIPTION:
            Get flags according to a select request. Then find all flags in the vincinity and replace contents
            according with data of the selected 'reference' if flagtype - groups match (i.e. 1 and 3) and are smaller
            i.e 1 in if reference type is 3.
            The main reason for this method is replacing automatic outlier detection signals during thunderstorms
            by "lightning" flags. The observer marks a single flag within the thunderstorm and then runs rename_nearby
            to replace automatically assigned "outliers" with the "lightning" information data.
            New Operator will be set to "rename_nearby"
        PARAMETER:
            parameter   (dict)  :  searchcriteria
            values      (dict)  :
            timerange
        RETURNS:
            flagobject
        EXAMPLES:
            results = fl.rename_nearby(parameter='labelid', values=['001'], searchcomment='autodetect')
        """
        if not values:
            return self
        if not isinstance(values, (list, tuple)):
            values = [values]
        newflag = self.copy()
        # select reference data
        reference = newflag.select(parameter=parameter, values=values)

        if debug:
            print(" Got reference data:", len(reference))
        if len(reference) > 0:
            for d in reference.flagdict:
                # take a time window around the reference element and get all rename elements
                st = reference.flagdict.get(d).get('starttime')
                et = reference.flagdict.get(d).get('endtime')
                reftype = reference.flagdict.get(d).get('flagtype')
                reflabel = reference.flagdict.get(d).get('label')
                reflabelid = reference.flagdict.get(d).get('labelid')
                refcomment = reference.flagdict.get(d).get('comment')
                refgroups = reference.flagdict.get(d).get('groups')
                searchtype = reftype
                if (reftype - 2) > 0:
                    # if 3 select 1 (not-used for definitive), if 4 select 2
                    searchtype = reftype - 2
                frame = newflag.trim(starttime=st - timerange, endtime=et + timerange, debug=debug)
                # select only flagtyps x-2 if x-2 > 0 (only automatic flags with the same flagtype group
                # optional select only if searchcomment is found in comment
                selframe = frame.select(parameter='flagtype', values=[searchtype])
                if searchcomment:
                    selframe = selframe.select(parameter='comment', values=[searchcomment])
                if debug:
                    print("Selected {} inputs for renaming close to reference at {}".format(len(selframe), st))
                if selframe:
                    for sel in selframe.flagdict:
                        d = selframe.flagdict[sel]
                        d['label'] = reflabel
                        d['labelid'] = reflabelid
                        d['comment'] = refcomment
                        d['groups'] = refgroups
                        d['operator'] = "rename_nearby"

        if debug:
            print("Amount of flags after renaming:", len(newflag))
        return newflag

    def replace(self, parameter='sensorid', value=None, newvalue=None):
        """
        DESCRIPTION:
            modify specific flags from a flagdictionary.
            For dropping specific time ranges use flagdict.trim
            For the given values a "find" command is used. Thus also parts can be searched for
            i.e. comment="incredible lightning strike" will be found by parameter value "lightning"

            Filter options are:
                    'sensorid',          # SensorId of Sensor on which flagging was conducted
                    'components',        # list like [1,2,3] ref. to columns, or ['x','y'] ref. to keys for the sensorid
                    'flagtype',          # integer 0 (just a comment), 1 (remove for definitiv - auto), 2 (keep for definitive - auto), 3 (remove for definitiv - human), 4 (keep for definitive - human),
                    'labelid',           # string with number i.e. '001'
                    'label',             # name asociated with labelid i.e. lightning
                    'comment',           # text without special characters (utf-8)
                    'groups',             # define flaggroups-list i.e. ['magnetism'], ['meteorology','gravity'] , ['SensorID_1','SensorID_1']
                    'probabilities',     # measure of probabilities - list
                    'stationid',         # stationid of flag
                    'validity',          # character code: d (delete in cleanup), h (invalid/hide), default None
                    'operator',          # text with name/shortcut of flagging person i.e. RL
            Please note that every search parameter is provided as list
        APPLICTAION:
            flagsmodified = fl.replace('comment','lightning','hell of a lightining strike')
            flagsmodified = fl.replace('groups',None,['magnetism'])
            flagsmodified = fl.replace('stationid','','WIC')


        RETURNS:
            a modified flag dictionary
        """
        fl = self.copy()
        if not newvalue:
            return self
        res = {}
        for id in fl.flagdict:
            econt = fl.flagdict[id]
            ncont = {}
            if isinstance(econt.get(parameter), basestring):
                if econt.get(parameter).find(value) > -1:
                    econt[parameter] = newvalue
            elif econt.get(parameter) == value:
                econt[parameter] = newvalue
            ncont = econt
            if ncont:
                res[id] = ncont
        return flags(res)

    def save(self, path=None, destination="file", overwrite=False, debug=False):
        """
        DEFINITION:
            Save list e.g. flaglist to file (using json or pickle) or db.

        PARAMETERS:
        Variables:
            - path:  (str) Path to data files in form:

        RETURNS:
            - True if succesful otherwise False
        EXAMPLE:
            fl.save('/my/path/myfile.json')

        """
        if debug:
            print("Saving flaglist ...")
        if not self.flagdict:
            print("error 1")
            return False
        if not path:
            path = 'myfile.json'
        flagdict = self.flagdict
        if not overwrite:
            existflag = load(path)
            if existflag.flagdict:
                fl = existflag.join(self)
                flagdict = fl.flagdict
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        if path.endswith('.pkl'):
            if debug:
                print(" -- using pickle")
            try:
                # TODO: check whether package is already loaded
                from pickle import dump
                dump(flagdict, open(path, 'wb'))
                if debug:
                    print("save: saved to {}".format(path))
                return True
            except:
                return False
        else:
            if debug:
                print(" -- using json format ")
            try:
                def dateconv(d):
                    # Converter to serialize datetime objects in json
                    if isinstance(d, datetime):
                        return d.__str__()

                with open(path, 'w', encoding='utf-8') as file:
                    file.write(unicode(json.dumps(flagdict, ensure_ascii=False, indent=4, default=dateconv)))
                if debug:
                    print("save: saved flagging data to a json file: {}".format(path))
                return True
            except:
                return False

    def select(self, parameter='sensorid', values=None):
        """
        DESCRIPTION:
            select specific flags from a flagdictionary.
            For selecting specific time ranges use flagdict.trim
            For the given values a "find" command is used. Thus also parts can be searched for
            i.e. comment="incredible lightning strike" will be found by parameter value "lightning"

            Filter options are:
                    'flagid',            # FlagID of flag
                    'sensorid',          # SensorId of Sensor on which flagging was conducted
                    'components',        # list like [1,2,3] ref. to columns, or ['x','y'] ref. to keys for the sensorid
                    'flagtype',          # integer 0 (just a comment), 1 (remove for definitiv - auto), 2 (keep for definitive - auto), 3 (remove for definitiv - human), 4 (keep for definitive - human),
                    'labelid',           # string with number i.e. '001'
                    'label',             # name asociated with labelid i.e. lightning
                    'comment',           # text without special characters (utf-8)
                    'groups',             # define flaggroups-list i.e. ['magnetism'], ['meteorology','gravity'] , ['SensorID_1','SensorID_1']
                    'probabilities',     # measure of probabilities - list
                    'stationid',         # stationid of flag
                    'validity',          # character code: d (delete in cleanup), h (invalid/hide), default None
                    'operator',          # text with name/shortcut of flagging person i.e. RL
            Please note that every search parameter is provided as list
        APPLICTAION:
            selected = flagdict.select('labelid',['050','052'])

        RETURNS:
            a filtered flag dictionary
        """
        if not values:
            return self
        if not isinstance(values, (list, tuple)):
            values = [values]
        res = {}
        for id in self.flagdict:
            if parameter == 'flagid' and id in values:
                res[id] = self.flagdict[id]
            else:
                econt = self.flagdict[id]
                ncont = {}
                if values and isinstance(values, (list, tuple)):
                    if isinstance(econt.get(parameter), basestring):
                        for val in values:
                            if econt.get(parameter).find(val) > -1:
                                ncont = econt
                    elif econt.get(parameter) in values:
                        ncont = econt
                if ncont:
                    res[id] = ncont
        return flags(res)

    def stats(self, intensive=False, output='stdout'):
        """
        DESCRIPTION:
            Provides some information on flags and their statistics
        PARAMETER:
            flags   (object) flagdict to be investigated
        APPLICTAION:
            fl = db2flaglist(db,'all')
            fl.stats()
        """

        flaglist = np.asarray(self._list(
            ['starttime', 'endtime', 'flagtype', 'labelid', 'sensorid', 'modificationtime', 'flagversion', 'stationid',
             'groups', 'operator'])).T
        verl = Counter(flaglist[7])
        vers = verl.keys()
        outputt = '##########################################\n'
        outputt += '           Flaglist statistics            \n'
        outputt += '##########################################\n'
        for v in vers:
            outputt += ('Flagging version: {}, Total:  {}\n'.format(v, verl[v]))
        outputt += '\n Total contents: {}\n'.format(len(self.flagdict))
        outputt += '-------------------------------------------\n'
        sensl = Counter(flaglist[5])
        sens = sensl.keys()
        for s in sens:
            outputt += (' SensorID: {}, Total:  {}\n'.format(s, sensl[s]))
            if intensive:
                # bylabel
                # get indices of all data belonging to this sensorid/group
                inds = np.where(flaglist[5] == s)
                sellabid = flaglist[4][inds]
                labl = Counter(sellabid)
                labs = labl.keys()
                for l in labs:
                    outputt += ('     LabelID: {} - {}, Total:  {}\n'.format(l, self.FLAGLABEL.get(l), labl[l]))
        if output == 'stdout':
            print(outputt)
        else:
            return outputt

    def trim(self, starttime=None, endtime=None, debug=False):
        """
        DESCRIPTION
            trim flag dictionary by given starttime and endtime.
            Return data which begins within the given time range datastart <= starttime
            or ends within the timerange dataend <= endtime.
        PARAMETER
            starttime : interpretable as datetime (datetime, string, etc)
            endtime   : interpretable as datetime
        APPLICATION
            test = newfl.trim(starttime='2022-11-22T20:53:12.654362',endtime='2022-11-22T20:59:12.654362')
        """
        # fl = self.copy()
        fl = self.flagdict
        st = testtime(starttime)
        et = testtime(endtime)
        if debug:
            print(" trimming flaglist. original lenght: {}".format(len(fl)))

        # extract starttime and endtimes from flagdict
        array = np.asarray(self._list(['starttime', 'endtime'])).T
        # get all indices where starttime <= lstarttime < endtime - > range begins within timerange
        startinds = np.where(np.logical_and(array[1] >= st, array[1] < et))
        # get all indices where starttime < lendtime <= lendtime  -> range end within timerange
        endinds = np.where(np.logical_and(array[2] > st, array[2] <= et))
        # combine indices lists
        res = list(set(startinds[0]) | set(endinds[0]))
        # get IDs for valid indices
        validids = array[0][res]
        nd = {key: fl.get(key) for key in fl if key in validids}
        if debug:
            print(" -> new lenght: {}".format(len(nd)))
        return flags(nd)

    def union(self, samplingrate=0, level=0, typeforce=True, debug=False):
        """
        DESCRIPTION:
            Method to inspect a flaglist and check for consecutive/overlapping time ranges with identical or similar contents.
            Three levels of union are available:
            0 : combine consecutive and overlapping time ranges with identical sensor, flagtype, label, components
            1 : combine consecutive and overlapping time ranges with identical sensor, flagtype, label even if
                components differ (extend component list)
            2 : combine consecutive and overlapping time ranges with identical sensor, flagtype, even if label and
                components differ (extend component list)
            3 : combine consecutive and overlapping time ranges with identical sensor, even if flagtype, label and
                components differ (extend component list)
            When combining data and eventually replacing other information then contents from higher flagtype and later
            modification time are used (in this order).

        PARAMETER:
            samplingrate (float) :  [sec] Sampling rate of underlying flagged data sequence
                                    if no sampling rate is provided then consecutivity can not be determined
                                    and only overlapping time ranges are combined
            level        (int)   :  if True than overlapping flags will also be combined, comments from last
                                    modification will be used
            typeforce    (BOOL)  :  if False than flagtype 1 and 3, as well 2 and 4 will be rated as equivalent for
                                    combination (not for assigning the combined contents)
        RETURNS:
            flagobject

        EXAMPLES:
            combined = fl.union(samplingrate=1,level=0)
        """

        fl = self.copy()
        if not len(fl) > 0:
            return fl
        newflags = flags()
        ids_to_remember = []
        idlist = []

        # Ideally flaglist is a list of dictionaries:
        # each dictionary consists of starttime, endtime, components, flagid, comment, sensorid, modificationdate
        # flagdict = [{"starttime" : el[0], "endtime" : el[1], "components" : el[2].split(','), "flagid" : el[3], "comment" : el[4], "sensorid" : el[5], "modificationdate" : el[6]} for el in flaglist]
        flaglist = np.asarray(fl._list(['starttime', 'endtime', 'flagtype', 'labelid', 'sensorid'])).T

        ## Firstly extract all unique SensorIDs from flaglst
        sensl = Counter(flaglist[5])
        sens = sensl.keys()
        uniquenames = list(sens)
        if debug:
            print("Sensorlist:", uniquenames)

        for name in uniquenames:
            if debug:
                print(" Dealing with {}".format(name))
            # get all flags with this name
            flsens = fl.select(parameter='sensorid', values=[name])
            # now create subdirectories depending on level
            subflaglist = []
            # get unique flagids, components and labelids for specific sensorlist
            sensflaglist = np.asarray(flsens._list(
                ['starttime', 'endtime', 'flagtype', 'labelid', 'sensorid', 'components', 'modificationtime',
                 'flagversion', 'stationid', 'groups', 'operator']), dtype=object).T
            sensflaglist[6] = [",".join(el) for el in sensflaglist[6]]
            compsl = Counter(sensflaglist[6])
            comps = list(compsl.keys())
            labsl = Counter(sensflaglist[4])
            labs = list(labsl.keys())
            typsl = Counter(sensflaglist[3])
            typs = list(typsl.keys())
            # create lists of subgroups depending on levels, these subgroups will then be combined
            # level separation has been tested
            if level < 4:
                # flsens is used for combination
                idlist = [sensflaglist[0]]
                if debug:
                    print("Level 3: ", idlist)
            if level < 3:
                idlist = []
                typs = [[el] for el in typs]
                if not typeforce:
                    # typs should then look like [[0],[1,3], [2,4]]
                    typs = [[0], [1, 3], [2, 4]]
                for typ in typs:
                    subidlist = [id for i, id in enumerate(sensflaglist[0]) if sensflaglist[3][i] in typ]
                    idlist.append(subidlist)
                if debug:
                    print("Level 2: ", idlist)
            if level < 2:
                newidlist = []
                for lab in labs:
                    for uids in idlist:
                        labidlst = []
                        for uid in uids:
                            val = flsens.flagdict.get(uid)
                            if val.get('labelid') == lab:
                                labidlst.append(uid)
                        if len(labidlst) > 0:
                            newidlist.append(labidlst)
                idlist = newidlist
                if debug:
                    print("Level 1: ", idlist)
            if level < 1:
                newidlist = []
                for comp in comps:
                    # print (comp, comp.split(','))
                    for uids in idlist:
                        compidlst = []
                        for uid in uids:
                            val = flsens.flagdict.get(uid)
                            if val.get('components') == comp.split(','):
                                compidlst.append(uid)
                        if len(compidlst) > 0:
                            newidlist.append(compidlst)
                idlist = newidlist
                if debug:
                    print("Level 0: ", idlist)
            for subids in idlist:  # speed this up
                for flagid in subids:
                    # get start and endtime for all ids and select ids with overlapping/consecutive time ranges
                    # remember combined ids to remove them at the end
                    val = flsens.flagdict.get(flagid)
                    mod = val.get('modificationtime')
                    typ = val.get('flagtype')
                    comps = val.get('components')
                    st = val.get('starttime') - timedelta(seconds=samplingrate)
                    et = val.get('endtime') + timedelta(seconds=samplingrate)
                    for id2 in subids:
                        if not flagid == id2 and not id2 in ids_to_remember:
                            val2 = flsens.flagdict.get(id2)
                            mod2 = val2.get('modificationtime')
                            typ2 = val2.get('flagtype')
                            comps2 = val2.get('components')
                            st2 = val2.get('starttime')
                            et2 = val2.get('endtime')
                            if st <= st2 <= et or st <= et2 <= et:
                                # overlap found
                                ids_to_remember.append(flagid)
                                ids_to_remember.append(id2)
                                if debug:
                                    print("Found overlapp for {} and {}".format(flagid, id2))
                                newst = np.min([val.get('starttime'), st2])
                                newet = np.max([val.get('endtime'), et2])
                                newcomps = list(set(comps + comps2))
                                primval = val
                                # select the dictionary with higest flagtype (observers decision, or latest modificationtime)
                                if typ2 > typ:
                                    primval = val2
                                if mod2 > mod:
                                    primval = val2
                                newflags.add(sensorid=primval.get('sensorid'),
                                             starttime=newst,
                                             endtime=newet,
                                             components=newcomps,
                                             flagtype=primval.get('flagtype'),
                                             labelid=primval.get('labelid'),
                                             label=primval.get('label'),
                                             comment=primval.get('comment'),
                                             groups=primval.get('groups'),
                                             probabilities=primval.get('probabilities'),
                                             stationid=primval.get('stationid'),
                                             validity=primval.get('vailidity'),
                                             operator=primval.get('operator'),
                                             color=primval.get('color'),
                                             flagversion=primval.get('flagversion'))

        # join new flags with fl
        fl = fl.join(newflags)
        # remove all ids which have been combined
        fl = fl.drop(parameter='flagid', values=ids_to_remember)

        if debug:
            print("    -> Originally {} flags were provided".format(len(self)))
            print("       of which {} were combined into {} new flags".format(len(ids_to_remember), len(newflags)))
            print("       using level {}, samplingrate {} and typeforcing {}.".format(level, samplingrate, typeforce))
            print("       New flaglist now contains {} flags.".format(len(fl)))

        return fl


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
        - Pickle import and all old data formats!!!

    EXAMPLE:
        import magpy.core.flagging as flags
        flaglist = flags.load('/my/path/myfile.pkl')

    TODO:
    When loading old data, then flagids need to be constrcuted and flags shoudl be added using the add method with
    last modifications dates at last. This makes sure that for identical flags the last modified ones are used.

    """
    fl = flags()
    myd = {}
    if not path:
        return fl

    if source == "db":
        print("Source from DATABASE - to be done")
    else:
        if "://" in path:
            print("Source from WEBPATH (only json supported) - to be done")
        else:
            if not os.path.isfile(path):
                if debug:
                    print(" -> Could not find a file at the given path {}".format(path))
                return flags([])
            if format == "":
                if debug:
                    print(" -> format NOT manually provided")
                if path.endswith(".pkl"):
                    if debug:
                        print(" -> extension points towards a pickle file")
                    format = "pkl"
                elif path.endswith(".json"):
                    if debug:
                        print(" -> extension points towards a json file")
                    format = "json"
                else:
                    if debug:
                        print(" -> format unclear, asuming a json file")
                    format = "json"
            else:
                if debug:
                    print(" -> format {} provided".format(format))

            if format == 'json':
                myd = _readJson(path, debug=debug)
                fl = flags(myd)
            elif format == 'pkl':
                myd = _readPickle(path, sensorid=sensorid, begin=begin, end=end, debug=debug)
                fl = flags(myd)
            if begin or end:
                fl = fl.trim(starttime=begin, endtime=end)
            if sensorid:
                fl = fl.select(parameter='sensorid', values=[sensorid])
            return flags(myd)


def _dateparser(dct):
    # Convert dates in dictionary to datetime objects
    for (key, value) in dct.items():
        if str(value).count('-') + str(value).count(':') == 4:
            try:
                try:
                    value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                except:
                    value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except:
                pass
        dct[key] = value
    return dct


def _readJson(path, debug=False):
    if debug:
        print("Reading a json style flaglist...")
    with open(path, 'r') as file:
        mydic = json.load(file, object_hook=_dateparser)
    return mydic


def _readPickle(path, debug=False):
    from pickle import load as pklload
    myd = pklload(open(path, "rb"))
    if debug:
        print("load: list {a} successfully loaded, found {b} inputs".format(a=path, b=len(myd)))
    return myd


if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: Flagging PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY.CORE FLAGGING PACKAGE.")
    print("All main methods will be tested. This may take a while.")
    print("If errors are encountered they will be listed at the end.")
    print("Otherwise True will be returned")
    print("----------------------------------------------------------")
    print()

import subprocess
# #######################################################
#                     Runtime testing
# #######################################################

fl = flags()
newfl = flags()
fo = flags()
nextfl = flags()
flagsmodified=flags()
ok = True
errors = {}
successes = {}
if ok:
    testrun = './testflagfile.json' # define a test file later on
    t_start_test = datetime.utcnow()
    while True:
        try:
            ts = datetime.utcnow()
            fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
            fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
            te = datetime.utcnow()
            successes['add'] = ("Version: {}: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['add'] = str(excep)
            print(datetime.utcnow(), "--- ERROR adding new data - will affect all other tests.")
        try:
            ts = datetime.utcnow()
            reslist = fl._list(['starttime','endtime'])
            te = datetime.utcnow()
            successes['_list'] = ("Version: {}, _list: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['_list'] = str(excep)
            print(datetime.utcnow(), "--- ERROR extracting _list.")
        try:
            ts = datetime.utcnow()
            newfl = fl.copy()
            te = datetime.utcnow()
            successes['copy'] = ("Version: {}, copy: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['copy'] = str(excep)
            print(datetime.utcnow(), "--- ERROR copy stream.")
        try:
            ts = datetime.utcnow()
            trimfl = newfl.trim(starttime='2022-11-22T19:57:12.654362',endtime='2022-11-22T22:59:12.654362')
            te = datetime.utcnow()
            successes['trim'] = ("Version: {}, trim: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['trim'] = str(excep)
            print(datetime.utcnow(), "--- ERROR trim flag dictionary.")
        try:
            ts = datetime.utcnow()
            nextfl = newfl.add(sensorid="GSM90_Y1112_0001",starttime="2022-11-22T10:56:12.654362",endtime="2022-11-22T10:59:12.654362",components=['f'],labelid='050',debug=False)
            obt = nextfl.select('labelid',['050'])
            te = datetime.utcnow()
            successes['select'] = ("Version: {}, select: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['select'] = str(excep)
            print(datetime.utcnow(), "--- ERROR select flags.")
        try:
            ts = datetime.utcnow()
            fo = flags()
            fo = fo.add(sensorid="GSM90_Y1112_0001",starttime="2022-11-22T10:56:12.654362",endtime="2022-11-22T10:59:12.654362",components=['f'],labelid='050',debug=False)
            combfl = newfl.join(fo)
            te = datetime.utcnow()
            successes['join'] = ("Version: {}, join: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['join'] = str(excep)
            print(datetime.utcnow(), "--- ERROR join flags.")
        try:
            ts = datetime.utcnow()
            fo = flags()
            fo = fo.add(sensorid="GSM90_Y1112_0001",starttime="2022-11-22T10:56:12.654362",endtime="2022-11-22T10:59:12.654362",components=['f'],labelid='050',debug=False)
            fo.stats()
            te = datetime.utcnow()
            successes['stats'] = ("Version: {}, stats: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['stats'] = str(excep)
            print(datetime.utcnow(), "--- ERROR stats.")
        try:
            ts = datetime.utcnow()
            diff = fo.diff(nextfl)
            te = datetime.utcnow()
            successes['diff'] = ("Version: {}, diff: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['diff'] = str(excep)
            print(datetime.utcnow(), "--- ERROR differences of flags.")
        try:
            ts = datetime.utcnow()
            clean = fo.drop(parameter='sensorid', values=['GSM90_Y1112_0001'])
            te = datetime.utcnow()
            successes['drop'] = ("Version: {}, drop: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['drop'] = str(excep)
            print(datetime.utcnow(), "--- ERROR in drop flags.")
        try:
            ts = datetime.utcnow()
            flagsmodified = nextfl.replace('comment','lightning','hell of a lightining strike')
            te = datetime.utcnow()
            successes['replace'] = ("Version: {}, replace: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['replace'] = str(excep)
            print(datetime.utcnow(), "--- ERROR in replace flags.")
        try:
            ts = datetime.utcnow()
            flagsmodified.fprint('GSM90_Y1112_0001')
            te = datetime.utcnow()
            successes['fprint'] = ("Version: {}, fprint: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['fprint'] = str(excep)
            print(datetime.utcnow(), "--- ERROR in fprint flags.")
        try:
            ts = datetime.utcnow()
            combfl = nextfl.union(level=0)
            te = datetime.utcnow()
            successes['union'] = ("Version: {}, union: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['union'] = str(excep)
            print(datetime.utcnow(), "--- ERROR in union flags.")
        try:
            ts = datetime.utcnow()
            combfl = nextfl.rename_nearby(parameter='labelid', values=['001'])
            te = datetime.utcnow()
            successes['ename_nearby'] = ("Version: {}, ename_nearby: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['ename_nearby'] = str(excep)
            print(datetime.utcnow(), "--- ERROR in ename_nearby flags.")
        try:
            ts = datetime.utcnow()
            nextfl.save(path=testrun)
            te = datetime.utcnow()
            successes['save'] = ("Version: {}, save: {}".format(magpyversion,(te-ts).total_seconds()))
        except Exception as excep:
            errors['save'] = str(excep)
            print(datetime.utcnow(), "--- ERROR saving flags.")

        # If end of routine is reached... break.
        break

    t_end_test = datetime.utcnow()
    time_taken = t_end_test - t_start_test
    print(datetime.utcnow(), "- Flagging runtime testing completed in {} s. Results below.".format(time_taken.total_seconds()))

    print()
    print("----------------------------------------------------------")
    del_test_files = 'rm {}*'.format(testrun)
    subprocess.call(del_test_files,shell=True)
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(" {}".format(errors.keys()))
        print()
        for item in errors:
                print(item + " error string:")
                print("    " + errors.get(item))
    print()
    print("Good-bye!")
    print("----------------------------------------------------------")
