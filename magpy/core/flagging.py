# -*- coding: utf-8 -*-

import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2

from magpy.stream import DataStream, basestring, magpyversion, unicode
from magpy.core.methods import *
#from datetime import datetime, timedelta, timezone
from magpy.core.flagbrain import *
from collections import Counter
import copy as cp
import hashlib
import json


class Flags(object):
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
                    'groups',            (dict) # define flaggroups-list i.e. [{'magnetism':['x','y','z','f']}, ['meteorology','gravity'] , ['SensorID_1','SensorID_1']
                    'probabilities',     (list) # measure of probabilities - list
                    'stationid',         (string) # stationid of flag
                    'validity',          (char) # character code: d (delete in cleanup), h (invalid/hide), default None
                    'operator',          (string) # text with name/shortcut of flagging person i.e. RL
                    'color',             (string) # None or string with color code to override flagid, will override automatic choice by flagtype
                    'modificationtime',  (datetime) # datetime of last edition
                    'flagversion'}       (string) # string like 2.0

    APPLICTAION:
        Initiate flags with >>> flag = Flags().

    EXAMPLES:
        # Load old flags and create new inputs by extracting labels and operators from comments
        # Load a flag list and display contents for a specific sensor without data streams
        # ...


    METHODS OVERVIEW:
    ----------------------------

|  class            |  method  |  since version  |  until version  |  runtime test  |  result verification  |  manual  |  *tested by |
|-------------------|  ------  |  -------------  |  -------------  |  ------------  |  --------------  |  ------  |  ---------- |
| **core.flagging** |   |                 |                 |                |                  |          | |
| flags             |  _check_version |  2.0.0       |                 |                |                  |         | flagging.load |
| flags             |  _match_groups |  2.0.0        |                 |  yes           |  yes*         |    | apply_flags, create_patch |
| flags             |  _list       |  2.0.0          |                 |  yes           |  yes             |         | |
| flags             |  _set_label_from_comment |  2.0.0 |              |                |                  |         | flagging.load |
| flags             |  _readJson_string |  2.0.0     |                 |                |                  |         | imagcdf read |
| flags             |  _writeson_string |  2.0.0     |                 |                |                  |         | imagcdf write |
| flags             |  add         |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  apply_flags |  2.0.0          |                 |  yes           |                  |  6.1    | |
| flags             |  copy        |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  create_patch |  2.0.0         |                 |                |  app**           |  6.1    | |
| flags             |  diff        |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  drop        |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  fprint      |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  join        |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  replace     |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  rename_nearby |  2.0.0        |                 |  yes           |  yes             |  6.1    | |
| flags             |  save        |  2.0.0          |                 |  yes           |                  |  6.1    | |
| flags             |  select      |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  stats       |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  timerange   |  2.0.0          |                 |  yes           |                  |  6.1    | |
| flags             |  trim        |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
| flags             |  union       |  2.0.0          |                 |  yes           |  yes             |  6.1    | |
|       | _dateparser       |  2.0.0          |                 |                |                  |         | flagging.load |
|       | _readJson         |  2.0.0          |                 |                |                  |         | flagging.load |
|       | _readPickle       |  2.0.0          |                 |                |                  |         | flagging.load |
|       | load              |  2.0.0          |                 |                |  app**           |  6.6    | |
|       | convert_to_flags  |  2.0.0          |                 |                |  app**           |  6.5    | |
|       | extract_flags     |  2.0.0          |                 |  yes           |  app**           |         | lib magpycdf |
|       | flag_outlier      |  2.0.0          |                 |  yes           |  app**           |  6.2    | |
|       | flag_range        |  2.0.0          |                 |  yes           |  app**           |  6.3    | |
|       | flag_binary       |  2.0.0          |                 |  yes           |  app**           |  6.4    | |
|       | flag_ultra        |  2.0.0          |                 |  no            |  no              |  6.7    | |


verification test marked by app** are done within the manual and example data sets

class  |  method  |  variables  |  description
-----  |  ------  |  ---------  |  -----------
**core.flagging** |   |         |
flags  |  _list       |           | convert flagging dictionary to a list
flags  | _set_label_from_comment | dictionary | interpret comment as label when importing old data
flags  |  add         |  ...      | insert, replace, modify, update flag as defined by its parameters
flags  |  apply_flags |  datastream, mode | apply flag to a datastream
flags  |  copy        |           | creates a deep copy of the flags object
flags  |  create_patch |          | create a patch dictionary for plotting from flags
flags  |  diff        |  flagobject | differences between two flag objects
flags  |  drop        |  parameter, values | drop selected flags from object
flags  |  fprint      |  sensorid | create a formated output
flags  |  join        |  flagobject | combine two flagging objects
flags  |  replace     |  parameter, value, newvalue | replace selected contents with new values
flags  |  rename_nearby |  parameter, values, timerange | replace contents of nearby flags with reference
flags  |  save        |  path     | save flagging dictionary
flags  |  select      |  parameter, values | select specific data
flags  |  stats       |  intensive | provides stats on a flaglist
flags  |  timerange   |            | get min and maxtime of flagging data
flags  |  trim        |  starttime, endtime | trim the time range of flagging information
flags  |  union        | level, samplingrate, typeforce | combine overlapping time ranges
       |  _dateparser |           | load support - convert strings to datetimes
       |  _readJson   |           | load support - interpret json
       |  _readPickle |           | load support - interpret pickle
       |  load        |  path     | load function needs to support import of old versions
       |  convert_to_flags |  data, ...   | convert contents of a data set to a flagging structure
       |  flag_outlier |  data, keys, threshold, timerange  | flag outliers based on IQR analysis
       |  flag_range  |  data, keys, above, below, timerange  | flag ranges based on thresholds
       |  flag_binary |  data, keys, keystoflag,              | flag switches from 0-1 and 1-0
       |  flag_ultra  |  data, keys, parameter | experimental flagging label assignment


    deprecated in 2.0.0
    - flag.clean(options):             # not necessary any more as id can not be duplicated
    - flag.extract(self, searchdict):  # replaced by select
    - flag.modify(...):                # replaced by replace
    - flag.set_labelvalue():           # replaced by _set_label_from_comment
    - flag.set_labelkey():             # replaced by _set_label_from_comment

    """

    def __init__(self, flagdict=None, flaglabel={}):
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
            'groups',
            # define flaggroups-dictionary i.e. {'magnetism':['X','y','z']}, ['meteorology':['X','y','z'],'gravity':['X','y','z']] , ['SensorID_1':['X','y','z'],'SensorID_1']
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
        # 070-079 : (0) switches and state information
        # 080-089 : (0) data treatment notations
        # 090-    : (0,1,2,3,4) yet to be classified
        # Flagids of the labels
        if not flaglabel:
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
                          '050': 'vehicle passing',
                          '051': 'nearby moving disturbing source',
                          '052': 'nearby static disturbing source',
                          '053': 'train',
                          '060': 'data gap',
                          '061': 'temperature effect electronics',
                          '062': 'temperature effect sensor',
                          '070': 'switch',
                          '090': 'unknown disturbance',
                          '099': 'unlabeled signature'
                          }
        else:
            self.FLAGLABEL = flaglabel

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

    def _match_groups(self, header, flag_sensor, flag_keys=None, flag_groups=None):
        """
        DESCRIPTION
            check if sensor, keys and group of the flagging object match header information of the current data stream
        VARIABLES
            header        (dict) the header of the current data stream
            flag_sensor   (string) sensorid of current flagdict
            flag_keys     (list) components of current flagdict
            flag_groups   (dict) groups of current flagdict
        RETURNS
            bool, keystoflag
        """
        sensorid = header.get('SensorID')
        sensorgroup = header.get('SensorGroup')
        if not flag_groups:
            if sensorid == flag_sensor:
                return True, flag_keys
            else:
                return False, []
        gkeys = list(flag_groups.keys())
        if sensorid == flag_sensor:
            return True, flag_keys
        elif sensorid in gkeys:
            flkeys = flag_groups.get(sensorid)
            return True, flkeys
        elif sensorgroup in gkeys:
            flkeys = flag_groups.get(sensorgroup)
            return True, flkeys
        return False, []



    def _check_version(self, commentconversion='cobs', debug=False):
        """
        DESCRIPTION
            check the current flagging version and transform into a flagdict structure as used
            by the current flagging package
        DEVELOPMENTS
            The method might be transformed into a library based reading system as used for file
            imports. As it is unlikely that many different formats for flagging information are
            developed in the near future this is postponed so far
        APPLICATION
            used by magpy.core.flagging load
        SUPPORTS
            MagPy1.1.x flagging structures (version 0.4) (version 1.0 has never been used in any productive environment
                {'WIC_1_0001': [['2018-08-02 14:51:33.999992', '2018-08-02 14:51:33.999992', 'x', 3, 'lightning RL', '2023-02-02 10:22:28.888995'], ['2018-08-02 14:51:33.999992', '2018-08-02 14:51:33.999992', 'y', 3, 'lightning RL', '2023-02-02 10:22:28.888995']]}
        """

        newfl = Flags()
        fd = self.flagdict
        converted = False
        version = '2.0'
        # identify data source
        for key in fd:
            value = fd[key]
            if isinstance(value, dict):
                version = '2.0'
                converted = False
            elif isinstance(value, (list, tuple)):
                version = '1.0'
                converted = True
                labelid = '000'
                operator = 'unknown'
                # check if data is contained
                if len(value) > 0:
                    newlist = []
                    for line in value:
                        # extract all line with identical information except components
                        newlist.append(line[:2] + line[3:])
                    # print (newlist)
                    newlist = [i for n, i in enumerate(newlist) if i not in newlist[:n]]
                    for line in newlist:
                        # now add a component list to each new line and construct a version 2.0 dict
                        comps = []
                        for oline in value:
                            if oline[:2] + oline[3:] == line:
                                comps.append(oline[2])
                        ft = line[2]
                        if ft == 2:
                            ft = 4
                        # round endtime to the next second
                        st = testtime(line[0])
                        et = round_second(testtime(line[1]))
                        if not st <= et:
                            st = round_second(testtime(line[0]))
                            et = testtime(line[1])
                        if commentconversion == 'cobs':
                            labelid, operator = newfl._import_conradosb(line[3])
                        newfl.add(sensorid=key, starttime=st, endtime=et,
                                  components=comps, flagtype=ft, labelid=labelid,
                                  comment=line[3], modificationtime=line[4],
                                  operator=operator,
                                  flagversion='2.0')

        if debug:
            print (" loaded flagging data of version {}".format(version))
        if converted:
            return newfl
        else:
            # if no conversion is necessary
            newfl = self.copy()
            return newfl


    def _get_cobs_groups(self, sensorid, comment):
        """
        DESCRIPTION
            Specific method of the Conrad Observatory to obtain some groups from old input
        """
        groups = {}
        if comment.lower().find('ssc') >= 0 or comment.lower().find('pulsation') >= 0:
            groups['magnetism'] = ['x','y','z','f']
        if comment.lower().find('earthquake') >= 0:
            if not groups.get('magnetism'):
                groups['magnetism'] = ['x','y','z']
        if sensorid.startswith('BLV'):
            groups['absolutes'] = ['x', 'y', 'z', 'dx', 'dy', 'dz','df']
        return groups


    def _import_conradosb(self, comment):
        """
        DESCRIPTION
            Specific method of the Conrad Observatory to convert old flags into the new format
            and evetually assign labels and operator. Will not affect imports of other observatories.
            This method was incorporated for simplicity of converting my data. Can be remove in a later
            version but will not affect any other usage.
        """
        labelid = '090'
        operator = 'unkown'
        not_consider_list = ['nearby', 'passing', 'normal']
        # drop numbers from string
        testcomment = re.sub(r'[0-9]', '', comment)
        for lab in self.FLAGLABEL:
            lid = lab
            lct = self.FLAGLABEL.get(lid)
            lcts = lct.split()
            lcts = [el for el in lcts if el not in not_consider_list]
            for el in lcts:
                if el in testcomment.lower():
                    labelid = lid
                    break
            if 'BL' in testcomment:
                operator = 'Barbara Leichter'
            if 'RL' in testcomment:
                operator = 'Roman Leonhardt'
        return labelid, operator

    def _list(self, parameter=None):
        """
        DESCRIPTION
            create a list from flagdict consisting of desired parameters and main key
        APPLICATION:
            flaglist = fl._list(parameter=['starttime','endtime','components','flagtype','comment','sensorid',
                                           'modificationtime'])
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

    def _set_label_from_comment(self, parameter='comment'):
        """
        DESCRIPTION
           helps to fill labels and labelids from comments
        PARAMETER
           dictionary like self.flagdict
        APPLICATION
           to be used by import routine
        """
        # select comment which contains any thing from FLAGLABEL
        flaggingdict = self.flagdict
        newflaggingdict = cp.deepcopy(flaggingdict)
        for fid in newflaggingdict:
            fcont = newflaggingdict[fid]
            result = {}
            for lid in self.FLAGLABEL:
                label = self.FLAGLABEL.get(lid)
                newlabels = label.split()
                oldcontents = fcont.get(parameter).split()
                amount_of_similars = len([w for w in oldcontents if w in newlabels])
                result[lid] = amount_of_similars
            newid = max(result, key=result.get)
            if not result.get(newid) > 0:
                newid = "099"
            fcont["labelid"] = newid
            fcont['label'] = self.FLAGLABEL.get(newid)
        self.flagdict = newflaggingdict
        return self


    def _readJson_string(self, flagstring, debug=False):
        if debug:
            print("Reading a json style string...")
        self.flagdict = json.loads(flagstring, object_hook=_dateparser)
        return self

    def _writeJson_string(self, debug=False):
        if debug:
            print("Writing a json style string...")

        def dateconv(d):
            # Converter to serialize datetime objects in json
            if isinstance(d, datetime):
                return d.__str__()

        flagstring = json.dumps(self.flagdict, ensure_ascii=False, default=dateconv)
        return flagstring


            # ------------------------------------------------------------------------

    # Flag methods in alphabetical order
    # ------------------------------------------------------------------------

    def add(self, sensorid=None, starttime=None, endtime=None, components=None, flagtype=0, labelid='000', label='',
            comment='', groups=None, probabilities=None, stationid='', validity='', operator='', color='',
            modificationtime=None, flagversion='2.0', minimumtimediff=0.1, debug=False):
        """
        DESCRIPTION
            Create a flagging dictionary input out of given information
            Each flag will be defined by a unqiue flagID which is constructed
            from sensorid,starttime,endtime,",".join(components),flagtype,labelid
            if you want to add a flag with identical information use overwrite?
        PARAMETER:
            minimumtimediff: if starttime and endtime are identical add/subtract this timediff in seconds
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
        if starttime == endtime:
            starttime = starttime-timedelta(seconds=minimumtimediff)
            endtime = endtime+timedelta(seconds=minimumtimediff)
        if not isinstance(components, (list, tuple)):
            print("create_flag: components need to be a list  - aborting")
            return {}
        if comment:
            comment = str(comment)
        if groups:
            if not isinstance(groups, dict):
                groups = {}
        if not isinstance(probabilities, (list, tuple)):
            probabilities = None
        if labelid and labelid in self.FLAGLABEL:
            label = self.FLAGLABEL.get(labelid)
        if not modificationtime:
            modificationtime = datetime.now(timezone.utc).replace(tzinfo=None)

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
        if flagd and debug:
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


    def apply_flags(self, data, flagtype=None, mode='drop', addlabel=False, debug=False):
        """
        DESCRIPTION
            drop flagged sequences from the data stream. You selected which
            flagtypes should be dropped and assign labels to the data stream's
            flagging columns.
        VARIABLES
            datastream   (DataStream)
            flagtypes    (list)   a list containing flagtypes - default is [1,3] - not to be used for
                                  definitive data
            mode         (string) 'drop' (default) - replaces data with np.nan for flags corresponding to flagtype
                                                     (MagPy1.x stream.remove_flagged())
                                  'insert' will keep data and just add labels to the stream (addlabel will be set True)
                                                     (MagPy1.x stream.flag(), flag_stream())
            addlabel     (bool)   add labels directly to the datastream (as done in MagPy 1.x)
        EXAMPLES
            1) old remove_flagged
            ndata = flags.apply_flags(data, mode='drop',addlabel=True)
            2) old flag/flag_stream
            ndata = flags.apply_flags(data, mode='insert')
        """
        ndata = data.copy()
        if not flagtype:
            flagtype = [1,3]
        if not isinstance(flagtype, (list, tuple)):
            flagtype = [flagtype]
        st, et = ndata._find_t_limits()
        tcol = ndata.ndarray[0]
        commentcol = ndata.KEYLIST.index('comment')
        flagcol = ndata.KEYLIST.index('flag')
        if mode == 'insert':
            addlabel = True
        if addlabel:
            if not len(data.ndarray[commentcol]) > 0:
                ndata.ndarray[commentcol] = np.asarray([''] * len(tcol))
            if not len(data.ndarray[flagcol]) > 0:
                ndata.ndarray[flagcol] = np.asarray(['0000000000000000-'] * len(tcol))

        fl = self.trim(starttime=st, endtime=et)
        flagdict = fl.flagdict
        for d in flagdict:
            flagcont = flagdict[d]
            # test, if sensorid is fitting or sensorid/group is part of groups
            valid, comps = self._match_groups(data.header, flagcont.get('sensorid'), flag_keys=flagcont.get('components'), flag_groups=flagcont.get('groups'))
            # test validity parameter for d or h
            if flagcont.get('validity') in ['d','h']:
                valid = False
            if debug:
                print (valid, comps, flagcont.get('groups'))
            if valid:
                stfind = ndata.findtime(flagcont.get('starttime'))
                etfind = ndata.findtime(flagcont.get('endtime'))+1
                # etfind need to be etfind +1 as this will replace input at stfind until etfind
                #comps = flagcont.get('components')
                if addlabel:
                    ndata.ndarray[commentcol][stfind:etfind] = "{} - {}".format(flagcont.get('labelid'),
                                                                                flagcont.get('label'))
                    codes = ['0' if not key in comps else str(flagcont.get('flagtype')) for key in data.FLAGKEYLIST]
                    codes.append('-')
                    flagcode = "".join(codes)
                    ndata.ndarray[flagcol][stfind:etfind] = flagcode
                if mode == 'drop' and flagcont.get('flagtype') in flagtype:
                    for key in comps:
                        ki = ndata.KEYLIST.index(key)
                        ndata.ndarray[ki] = np.asarray(ndata.ndarray[ki], dtype=float)
                        if len(data.ndarray[ki]) >= etfind:
                            ndata.ndarray[ki][stfind:etfind] = np.nan
        return ndata


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

    def create_patch(self, data=None):
        """
        DESCRIPTION:
            construct a simple patch dictionary for plotting from any given and preselected flaglist
        VARIABLES:
            data    (DataStream) : the data stream for which the patches are created
                                   if not provided then patches are created by ignoring
                                   sensorids
        APPLICATION:
        RETURNS:
            a patch dictionary for plotting
            of the following structure
            {flagid : {"start":"","end":"","flagtype":"","color":"","labelid":"","label":""}, nextflagid : {}}
        """
        patchdict = {}
        flagdict = self.flagdict

        def _get_color_from_flagtype(flagtype):
            color = 'grey'
            if flagtype==0:
                color = 'grey'
            elif flagtype==1:
                color = 'coral'
            elif flagtype == 2:
                color = 'lime'
            elif flagtype == 3:
                color = 'r'
            elif flagtype == 4:
                color = 'g'
            return color

        for d in flagdict:
            cont = {}
            valid = True
            comps = flagdict[d].get('components',DataStream().KEYLIST)
            if data:
                valid, comps = self._match_groups(data.header, flagdict[d].get('sensorid'), flag_keys=comps, flag_groups=flagdict[d].get('groups'))
            if flagdict[d].get('validity') in ['d','h']:
                valid = False
            if valid:
                cont['components'] = comps
                cont['start'] = flagdict[d].get('starttime', None)
                cont['end'] = flagdict[d].get('endtime')
                cont['flagtype'] = flagdict[d].get('flagtype')
                color = flagdict[d].get('color')
                if not color:
                    color = _get_color_from_flagtype(cont.get('flagtype'))
                cont['color'] = color
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
        return Flags(value)

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
                    'groups',             # define flaggroups-dict i.e. {'magnetism':['x']}
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
        return Flags(new)

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
                    'groups',            # define flaggroups-dict
                    'probabilities',     # measure of probabilities - list
                    'stationid',         # stationid of flag
                    'validity',          # character code: d (delete in cleanup), h (invalid/hide), default None
                    'operator',          # text with name/shortcut of flagging person i.e. RL
            Please note that every search parameter is provided as list
        APPLICTAION:
            flagsmodified = fl.replace('comment','lightning','hell of a lightining strike')
            flagsmodified = fl.replace('groups',None,{'magnetism':['#'x']})
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
        return Flags(res)

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

    def select(self, parameter='sensorid', values=None, identical=False, debug=False):
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
                    'flagtype',          # integer 0 (just a comment), 1 (remove for definitive - auto), 2 (keep for definitive - auto), 3 (remove for definitiv - human), 4 (keep for definitive - human),
                    'labelid',           # string with number i.e. '001'
                    'label',             # name associated with labelid i.e. lightning
                    'comment',           # text without special characters (utf-8)
                    'groups',             # define flaggroups-dict
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
                if debug:
                    print ("d", econt)
                if values and isinstance(values, (list, tuple)):
                    if isinstance(econt.get(parameter), basestring):
                        if debug:
                            print (econt.get(parameter))
                        for val in values:
                            if identical:
                                if econt.get(parameter) == val:
                                     ncont = econt
                            elif econt.get(parameter).find(val) > -1:
                                ncont = econt
                    elif econt.get(parameter) in values:
                        ncont = econt
                if ncont:
                    res[id] = ncont
        return Flags(res)


    def stats(self, level=0, intensive=False, output='stdout'):
        """
        DESCRIPTION:
            Provides some information on flags and their statistics
        PARAMETER:
            flags   (object) flagdict to be investigated
        APPLICTAION:
            fl = db2flaglist(db,'all')
            fl.stats()
        """
        outputt = ''
        if level:
            intensive=True
        flaglist = np.asarray(self._list(
            ['starttime', 'endtime', 'flagtype', 'labelid', 'sensorid', 'modificationtime', 'flagversion', 'stationid',
             'groups', 'operator'])).T
        if len(flaglist) > 0:
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


    def timerange(self):
        """
        DESCRIPTION
            get the time range covered by a flag object
        """
        array = np.asarray(self._list(['starttime', 'endtime'])).T
        mintime = np.min(array[1])
        maxtime = np.max(array[2])
        return mintime, maxtime


    def trim(self, starttime="1769-09-14", endtime=datetime.now(timezone.utc).replace(tzinfo=None), debug=False):
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
        return Flags(nd)

    def union(self, samplingrate=0, level=0, typeforce=True, debug=False):
        """
        DESCRIPTION:
            Method to inspect a flagging object and check for consecutive/overlapping time ranges with identical or
            similar contents. This method is currently pretty slow if lots of similar data is checked. I.e.
            200000 similar data points need approximately 3h to be analysed. As such large amounts are unlikely I
            did not spend much time in more effectivity. Typically less the 1000 points are investigated in fractions
            of a second.
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
        newflags = Flags()
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
                    print("Level 3: ", len(idlist))
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
                    print("Level 2: ", len(idlist))
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
                    print("Level 1: ", len(idlist))
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
                    print("Level 0: ", len(idlist))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            div = 1000
            for subids in idlist:  # speed this up
                if debug:
                    print ("Amount of subids", len(subids))
                for ik, flagid in enumerate(subids):
                    # get start and endtime for all ids and select ids with overlapping/consecutive time ranges
                    # remember combined ids to remove them at the end
                    if debug:
                        if ik/div > 1:
                            div = div+1000
                            te = datetime.now(timezone.utc).replace(tzinfo=None)
                            totsec = (te-ts).total_seconds()
                            print (" finished checking of {} IDs in {} seconds - finishing will need {} sec".format(ik, totsec, totsec*((len(subids)-div)/div)))
                    val = flsens.flagdict.get(flagid)
                    st = val.get('starttime') - timedelta(seconds=samplingrate)
                    et = val.get('endtime') + timedelta(seconds=samplingrate)
                    for ij, id2 in enumerate(subids):
                        if not flagid == id2 and not id2 in ids_to_remember:
                            val2 = flsens.flagdict.get(id2)
                            st2 = val2.get('starttime')
                            et2 = val2.get('endtime')
                            if st <= st2 <= et or st <= et2 <= et:
                                # overlap found
                                mod = val.get('modificationtime')
                                typ = val.get('flagtype')
                                comps = val.get('components')
                                mod2 = val2.get('modificationtime')
                                typ2 = val2.get('flagtype')
                                comps2 = val2.get('components')
                                ids_to_remember.append(flagid)
                                ids_to_remember.append(id2)
                                if debug:
                                    print("Found overlap for {} and {}".format(flagid, id2))
                                newst = np.min([val.get('starttime'), st2])
                                newet = np.max([val.get('endtime'), et2])
                                newcomps = list(set(comps + comps2))
                                primval = val
                                # select the dictionary with highest flagtype (observers decision, or latest modificationtime)
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
        - Pickle import !!!

    EXAMPLE:
        import magpy.core import flagging
        fl = flagging.load('/my/path/myfile.json')
    """
    fl = Flags()
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
                return fl
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
                # myd_analyse and create flagdict
                fl = Flags(myd)
            elif format == 'pkl':
                myd = _readPickle(path, sensorid=sensorid, begin=begin, end=end, debug=debug)
                fl = Flags(myd)
            fl = fl._check_version(debug=debug)
            if begin or end:
                fl = fl.trim(starttime=begin, endtime=end)
            if sensorid:
                fl = fl.select(parameter='sensorid', values=[sensorid])
            return fl


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


def convert_to_flags(data, flagtype=2, labelid='030', sensorid=None, keystoflag=None, commentkeys=None, groups=None):
    """
    DESCRIPTION:
        Constructs a flag object dependent on the content of stream
    PARAMETER:
        flagtype    (int)    integer number between 0 and 4, default is 2
        labelid     (string) default is 030 for earthquakes
        commentkeys (list)   list of keys from which comment is constructed i.e. ['f','str3'] with f=3.52 and str3='wow'
                             will be transformed to comment='3.52 wow'
                             ['magnitude','f','earthquake'] -> 'magnitude 3.52 earthquake'
                             sensorid    (string) will override sensorid of data
        groups      (dict)   define other sensors to which this flags apply

    APPLICATION
        fl = convert_to_flags(data, comment='f,str3',sensorid=nstream.header["SensorID"], userange=False, keystoflag="x")
    """
    fl = Flags()
    ### identify any given gaps and flag time ranges regarding gaps
    if not isinstance(commentkeys, (list,tuple)):
        commentkeys = ['Flagged']
    if not keystoflag:
        keystoflag = [el for el in commentkeys if el in data.KEYLIST]
        if not len(keystoflag) > 0:
            # Flag all
            keystoflag = data.FLAGKEYLIST
    if not sensorid:
        sensorid = data.header.get('SensorID')
    stationid = data.header.get('StationID','')
    if not groups:
        groups = {}

    tcol = data.ndarray[0]
    for ind in range(0,len(data)):
        part = []
        for comm in commentkeys:
            if comm in data.KEYLIST:
                j = data.KEYLIST.index(comm)
                part.append(str(data.ndarray[j][ind]))
            else:
                part.append(str(comm))
        descr = " ".join(part)
        fl.add(sensorid=sensorid, starttime=tcol[ind], endtime=tcol[ind],
               components=keystoflag, flagtype=flagtype, labelid=labelid,
               comment=descr,
               groups=groups, stationid=stationid, operator='MagPy',
               flagversion='2.0')

    return fl


def extract_flags(data, debug=False):
    """
    DEFINITION:
        Extracts flags associated with the provided DataStream object. This method is solely used for importing old
        PYCDF archive structures containing flag and comment columns.
    PARAMETERS:
        datastream (DataStream())
    Variables:
        None
    RETURNS:
        - limited flagging object     (Flags) a flag object containing [st,et,key,flagnumber,commentarray[idx],sensorid,now]

    EXAMPLE:
        fl = flagging.extract_flags(stream)
    """
    from itertools import groupby
    from operator import itemgetter

    fl = flagging.Flags()
    sensorid = data.header.get('SensorID','')
    #now = datetime.now(timezone.utc).replace(tzinfo=None)

    flpos = data.KEYLIST.index('flag')
    compos = data.KEYLIST.index('comment')
    flags = data.ndarray[flpos]
    comments = data.ndarray[compos]
    if not len(flags) > 0 or not len(comments) > 0:
        return fl

    uniqueflags = data.union(flags)
    uniquecomments = data.union(comments)

    # 1. Extract relevant keys from uniqueflags
    if debug:
        print ("extract_flags: Unique Flags -", uniqueflags)
        print ("extract_flags: Unique Comments -", uniquecomments)
    keylist = []
    for elem in uniqueflags:
        if not elem in ['','-','0000000000000000-']:
            #print (elem)
            for idx,el in enumerate(elem):
                if not el == '-' and el in ['0','1','2','3','4','5','6']:
                    keylist.append(data.NUMKEYLIST[idx-1])
    # 2. Cycle through keys and extract comments
    if not len(keylist) > 0:
        return fl

    keylist = data.union(np.asarray(keylist))

    for key in keylist:
        indexflag = data.KEYLIST.index(key)
        for comment in uniquecomments:
            flagindicies = []
            for idx, elem in enumerate(comments):
                if not elem == '' and elem == comment:
                    #print ("ELEM", elem)
                    flagindicies.append(idx)
            # 2. get consecutive groups
            for k, g in groupby(enumerate(flagindicies), lambda ix: ix[0] - ix[1]):
                try:
                    consecutives = list(map(itemgetter(1), g))
                    st = data.ndarray[0][consecutives[0]]
                    et = data.ndarray[0][consecutives[-1]]
                    # in PYSTR flags with only '-' input are found - ignore those
                    if not flags[consecutives[0]] in ['','-']:
                        flagnumber = flags[consecutives[0]][indexflag]
                        labelid, operator = fl._import_conradosb(comment)
                        if not flagnumber in ['-',None]:
                            fl = fl.add(sensorid=sensorid, starttime=st,
                                    endtime=et, components=[key],
                                    flagtype=int(flagnumber), labelid = labelid, operator = operator,
                                    comment=comment)
                except:
                    print ("extract_flags: error when extracting flags from datastream (flags, comments)")

    return fl

def flag_outlier(data, keys=None, threshold=1.5, timerange=None, markall=False, labelid='002', groups=None, datawindow=None, debug=False):
    """
    DEFINITION:
        Flags outliers in data, using inner quartiles ranges
        Coding : 0 take, 1 auto remove, 2 auto take, 3 force remove, 4 force take

    PARAMETERS:
    Variables:
        - keys:         	(list) List of keys to evaluate. Default = all numerical
        - threshold:   		(float) Determines threshold for outliers.
                        	1.5 = standard
                        	5 = weak condition, keeps storm onsets in (default)
                        	4 = a useful comprimise to be used in automatic analysis.
        - timerange:    	(timedelta Object) Time range. Default = samlingrate(sec)*600
        - stdout:        	prints removed values to stdout
        - returnflaglist	(bool) if True, a flaglist is returned instead of stream
        - markall       	(bool) default is False. If True, all components (provided keys)
                                 are flagged even if outlier is only detected in one. Useful for
                                 vectorial data
    RETURNS:
        - flagobject:       (Flagging Object) Stream with flagged data.

    EXAMPLE:
        fl = flag_outlier(data, keys=['x','y','z'], threshold=2)

    APPLICATION:
    """
    fl = Flags()
    sr = data.samplingrate()
    if not timerange:
        window = 600.
    else:
        window = timerange / sr
    if datawindow:
        print ("overriding timerange/sampling rate related window size and using {} succesive data points as window length".format(datawindow))
        window = datawindow
    if debug:
        print ("samplingrate", sr)
        print ("window", window)
    if not window > 29:
        print(" timerange to small for a proper outlier detection")
        return fl
    else:
        window = int(window)
    if not keys:
        keys = data._get_key_headers(numerical=True)
    if not threshold:
        threshold = 5.0
    if not groups:
        groups = {}

    cdate = datetime.now(timezone.utc).replace(tzinfo=None).replace(tzinfo=None)
    sensorid = data.header.get('SensorID', '')
    stationid = data.header.get('StationID', '')
    flaglist = []

    if not len(data) > 0:
        return fl

    # get a poslist of all keys - used for markall
    flagposls = [data.FLAGKEYLIST.index(key) for key in keys]
    tcol = data._get_column('time')
    # Start here with for key in keys:
    for key in keys:
        flagpos = data.FLAGKEYLIST.index(key)
        fkey = [key]
        if markall:
            fkey = keys
        col = data._get_column(key)
        if not len(col) > 0:
            print("Flag_outlier: No data for key {} - skipping".format(key))
            break

        endchunk = len(col)
        if window > int(endchunk/2.):
            window = int(endchunk/2.)
            print(window)
        chunks = get_chunks(endchunk, wl=window)
        if debug:
            print (chunks)
        for chunk in chunks:
            selcol = col[chunk].astype(float)
            seltcol = tcol[chunk]
            # determine IQR
            Q1 = np.nanquantile(selcol, 0.25)
            Q3 = np.nanquantile(selcol, 0.75)
            IQR = Q3 - Q1
            md = np.nanmedian(selcol)
            whisker = threshold * IQR
            flaginds = np.where(np.logical_or(selcol < (md - whisker), selcol > (md + whisker)))
            if debug:
                print(len(selcol), md, IQR, whisker, flaginds)
            flaginds = flaginds[0]
            if len(flaginds) > 0:
                grouped_inds = group_indices(flaginds)
                # exclude a quarter double-window at the beginning and at the end, double window as chunck is using double windows
                lowwin = 2 * window / 4
                highwin = 6 * window / 4
                flag_inds = [[flaginds[el[0]], flaginds[el[1]]] for el in grouped_inds if
                             flaginds[el[0]] > lowwin and flaginds[el[1]] < highwin]
                for flagtimes in flag_inds:
                    fl.add(sensorid=sensorid, starttime=seltcol[flagtimes[0]], endtime=seltcol[flagtimes[0]+1],
                           components=fkey, flagtype=1, labelid=labelid,
                           comment='automatically marked by flag_outlier with threshold {}, timerange {}, markall {}'.format(
                               threshold, timerange, markall), groups=groups, stationid='stationid', operator='MagPy',
                           flagversion='2.0')
    fl = fl.union()

    return fl

def flag_range(data, keys=None, above=0, below=0, starttime=None, endtime=None, flagtype=1, labelid='002',
                   keystoflag=None, text=None, groups=None, operator='MagPy'):
    """
    DEFINITION:
        Flags data within time range or data exceeding a certain threshold
        Coding : 0 take, 1 remove, 2 force take, 3 force remove

    PARAMETERS:
    Variables:
        - None.
    Kwargs:
        - keys:         (list) List of keys to check for criteria. Default = all numerical
                            please note: for using above and below criteria only one element
                            is accepted (e.g. ['x']
        - text          (string) comment
        - flagtype      (int) Flagtype (0,1,2,3,4)
        - groups        (dict) flagging groups
        - keystoflag:   (list) List of keys to flag. Default = same as keys
        - below:        (float) flag data of key below this numerical value.
        - above:        (float) flag data of key exceeding this numerical value.
        - starttime:    (datetime Object)
        - endtime:      (datetime Object)
    RETURNS:
        - flaglist:     (list) flagging information - use stream.flag(flaglist) to add to stream

    EXAMPLE:
        fllist = flag_range(data, keys=['var1'], above=80, keystoflag=['x'])

    APPLICATION:
    """

    fl = Flags()
    sensorid = data.header.get('SensorID')
    stationid = data.header.get('StationID')
    if not groups:
        groups = {}
    flaglist = []
    if not keys:
        keys = data._get_key_headers(numerical=True)
    if not keystoflag:
        keystoflag = keys
    if not len(data.ndarray[0]) > 0:
        print("flag_range: No data available - aborting")
        return flaglist
    if not len(keys) == 1:
        if above or below:
            print("flag_range: for using thresholds above and below only a single key has to be provided")
            print("  -- ignoring given above and below values")
            below = False
            above = False
    # test validity of starttime and endtime
    trimmedstream = data.copy()
    if starttime or endtime:
        trimmedstream = data.trim(starttime=starttime, endtime=endtime)
    if not len(trimmedstream) > 0:
        print ("flag_range: Flag times outside of data range")
        return fl
    tcol = trimmedstream.ndarray[0]
    if not above and not below:
        # return flags for all data in trimmed stream
        if not text:
            comment = 'marked by flag_range'
        else:
            comment = text
        fkeys = []
        for elem in keystoflag:
            fkeys.append(elem)
        fl.add(sensorid=sensorid, starttime=tcol[0], endtime=tcol[-1],
               components=fkeys, flagtype=flagtype, labelid=labelid,
               comment=comment,
               groups=groups, stationid=stationid, operator=operator,
               flagversion='2.0')
        return fl

    if above and below:
        ind = data.KEYLIST.index(keys[0])
        trueindices = (trimmedstream.ndarray[ind] > above) & (trimmedstream.ndarray[ind] < below)
        d = np.diff(trueindices)
        idx, = d.nonzero()
        idx += 1
        if not text:
            text = 'outside of range {} to {}'.format(below, above)
        if trueindices[0]:
            # If the start of condition is True prepend a 0
            idx = np.r_[0, idx]
        if trueindices[-1]:
            # If the end of condition is True, append the length of the array
            idx = np.r_[idx, trimmedstream.ndarray[ind].size]  # Edit
        # Reshape the result into two columns
        idx.shape = (-1, 2)
        for start, stop in idx:
            stop = stop - 1
            fkeys = []
            for elem in keystoflag:
                fkeys.append(elem)
            fl.add(sensorid=sensorid, starttime=tcol[start], endtime=tcol[stop],
                   components=fkeys, flagtype=flagtype, labelid=labelid,
                   comment=text,
                   groups=groups, stationid=stationid, operator=operator,
                   flagversion='2.0')
    elif above:
        ind = data.KEYLIST.index(keys[0])
        trueindices = trimmedstream.ndarray[ind] > above
        d = np.diff(trueindices)
        idx, = d.nonzero()
        idx += 1
        if not text:
            text = 'exceeding {}'.format(above)
        if trueindices[0]:
            # If the start of condition is True prepend a 0
            idx = np.r_[0, idx]
        if trueindices[-1]:
            # If the end of condition is True, append the length of the array
            idx = np.r_[idx, trimmedstream.ndarray[ind].size]  # Edit
        # Reshape the result into two columns
        idx.shape = (-1, 2)
        for start, stop in idx:
            stop = stop - 1
            fkeys = []
            for elem in keystoflag:
                fkeys.append(elem)
            fl.add(sensorid=sensorid, starttime=tcol[start], endtime=tcol[stop],
                   components=fkeys, flagtype=flagtype, labelid=labelid,
                   comment=text,
                   groups=groups, stationid=stationid, operator=operator,
                   flagversion='2.0')
    elif below:
        ind = data.KEYLIST.index(keys[0])
        truefalse = trimmedstream.ndarray[ind] < below
        d = np.diff(truefalse)
        idx, = d.nonzero()
        idx += 1
        if not text:
            text = 'below {}'.format(below)
        if truefalse[0]:
            # If the start of condition is True prepend a 0
            idx = np.r_[0, idx]
        if truefalse[-1]:
            # If the end of condition is True, append the length of the array
            idx = np.r_[idx, trimmedstream.ndarray[ind].size]  # Edit
        # Reshape the result into two columns
        idx.shape = (-1, 2)
        for start, stop in idx:
            stop = stop - 1
            fkeys = []
            for elem in keystoflag:
                fkeys.append(elem)
            fl.add(sensorid=sensorid, starttime=tcol[start], endtime=tcol[stop],
                   components=fkeys, flagtype=flagtype, labelid=labelid,
                   comment=text,
                   groups=groups, stationid=stationid, operator=operator,
                   flagversion='2.0')
    return fl

def flag_binary(data, key, flagtype=0, labelid='070', keystoflag=None, sensorid=None, text=None, markallon=False, markalloff=False,
                    groups=None):
    """
    DEFINITION:
        Function to detect changes between 0 and 1 and create a flaglist for zero or one states.
        Please note, the last state will not be flagged if markallon/off is selectefd, simply
        because this state is obviously not over yet
    PARAMETERS:
        key:           (key) key to investigate
        flagtype:      (int) integer between 0 and 4, default is 0
        labelid:       (string) default is 070 for some switch
        keystoflag:	   (list) list of keys to be flagged
        sensorid:	   (string) sensorid for flaglist, default is sensorid of self
        text:          (string) text to be added to comments/stdout,
                                will be extended by on/off
        markallon:     (BOOL) add comment to all ons
        markalloff:    (BOOL) add comment to all offs
        groups:        (dict) flagging group
    RETURNS:
        - flag object

    EXAMPLE:
        flags = bindetector(data, 'z', flagtype=0, ['x'], sensorid='SensorID',
                            text='Maintanence switch for rain bucket',
                            markallon=True, groups=['RCST7':['f'], 'meteosgo':['x']])
    """
    fl = Flags()
    if not key:
        print("bindetector: define key wih binary data")
        return data
    if not keystoflag:
        keystoflag = [key]
    if not sensorid:
        sensorid = data.header.get('SensorID')
    if not groups:
        groups = {}
    stationid = data.header.get('StationID')

    if not len(data.ndarray[0]) > 0:
        print("bindetector: No ndarray data found - aborting")
        return data

    tcol = data.ndarray[0]
    ind = data.KEYLIST.index(key)
    startstate = data.ndarray[ind][0]
    flaglist = []
    switchindices, = np.nonzero(np.diff(data.ndarray[ind], prepend=startstate))

    prevelem = 0
    for elem in switchindices:
        csprev = 'off'
        csnew = 'on'
        if startstate:
            csprev = 'on'
            csnew = 'off'
        if not text:
            descr = 'switching {} from {} to {}'.format(data.header.get('col-{}'.format(key)), csprev, csnew)
        else:
            descr = '{} from {} to {}'.format(text, csprev, csnew)
        # create a label for switches
        fl.add(sensorid=sensorid, starttime=tcol[elem - 1], endtime=tcol[elem],
               components=keystoflag, flagtype=flagtype, labelid=labelid,
               comment=descr,
               groups=groups, stationid=stationid, operator='MagPy',
               flagversion='2.0')
        if markallon and startstate:
            if not text:
                descr = 'switch {} is on'.format(data.header.get('col-{}'.format(key)))
            else:
                descr = '{} is on'.format(text)
            fl.add(sensorid=sensorid, starttime=tcol[prevelem], endtime=tcol[elem - 1],
                   components=keystoflag, flagtype=flagtype, labelid=labelid,
                   comment=descr,
                   groups=groups, stationid=stationid, operator='MagPy',
                   flagversion='2.0')
        if markalloff and not startstate:
            if not text:
                descr = 'switch {} is off'.format(data.header.get('col-{}'.format(key)))
            else:
                descr = '{} is off'.format(text)
            fl.add(sensorid=sensorid, starttime=tcol[prevelem], endtime=tcol[elem - 1],
                   components=keystoflag, flagtype=flagtype, labelid=labelid,
                   comment=descr,
                   groups=groups, stationid=stationid, operator='MagPy',
                   flagversion='2.0')
        startstate = np.abs(startstate - 1)
        prevelem = elem

    return fl


def flag_ultra(data, keys=None, factordict=None, mode='magnetism', groups=None):
    """
    DEFINITION:
        Flags data using IQR analysis in decomposed signals. An empirical probability method filled
        with widely arbitray (August 2024) probability distributions is used to assign preliminary
        labelid's. Please note, this method has been developed to assist (and better understand)
        the creation of training data sets for the AI flagging bot. Probabilities and thresholds
        are optimzed for very specific signatures strongly dependend on instruments and local
        disturbances of the Conrad Observatory. For other locations/instruments the parameters
        of this method need to be carefully adapted.
        This method is very experimental and might fail for numerous different reasons.

    PARAMETERS:
    Variables:
        - keys:         (list) List of keys to check for criteria. Default = all numerical
                            please note: for using above and below criteria only one element
                            is accepted (e.g. ['x']
        - factordict    (dict) a decompositon/frequency dependend threshold dictionary for
                            identification of disturbed sequences
        - mode          (string) mode is used to select among stored probability label assignments
        - groups        (dict) flags will be assigned to these groups
    RETURNS:
        - flag object:  flagging information - use stream.flag(flaglist) to add to stream

    EXAMPLE:
        fllist = flag_ultra(data, keys=['x','y','z'])

    APPLICATION:
    """

    fl = flagging.Flags()
    sensorid = data.header.get('SensorID')
    stationid = data.header.get('StationID')
    starttime, endtime = data._find_t_limits()  # obtain start and endtime
    sample_rate = data.samplingrate()  # obtain sampling rate
    if not keys:
        keys = data._get_key_headers(numerical=True)
    if not factordict:
        factordict = {}
        for key in keys:
            factordict[key] = {0: 14, 1: 12, 2: 10, 3: 8, 4: 6, 5: 5}
    if not groups:
        groups = {mode: ['x','y','z','f']}
    elif not mode in groups:
        groups[mode] =  ['x','y','z','f']

    analysisdict = create_feature_dictionary(data, factor=factordict, config={})
    imfflagdict = create_basic_flagdict(analysisdict, components=keys, sensorid=sensorid, mode='magnetism')
    nimfflagdict = combine_flagid_ranges(imfflagdict, components=keys, debug=False)
    timfflagdict = combine_frequency_ranges(nimfflagdict, components=keys, debug=False)
    fl = convert_imfflagdict_to_flaglist(sensorid, timfflagdict, starttime=starttime, sample_rate=sample_rate,
                                         stationid=stationid, groups=groups)
    return fl


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

    def create_teststream(startdate=datetime(2022, 11, 21), coverage=86400):
        # Create a random data signal with some nan values in x and z
        c = 1000  # 1000 nan values are filled at random places
        l = coverage
        array = [[] for el in DataStream().KEYLIST]
        import scipy
        win = scipy.signal.windows.hann(60)
        a = np.random.uniform(20950, 21000, size=int((l + 2880) / 2))
        b = np.random.uniform(20950, 21050, size=int((l + 2880) / 2))
        x = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        array[1] = np.asarray(x[1440:-1440])
        a = np.random.uniform(1950, 2000, size=int((l + 2880) / 2))
        b = np.random.uniform(1900, 2050, size=int((l + 2880) / 2))
        y = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
        array[2] = np.asarray(y[1440:-1440])
        a = np.random.uniform(44300, 44400, size=(l + 2880))
        z = scipy.signal.convolve(a, win, mode='same') / sum(win)
        array[3] = np.asarray(z[1440:-1440])
        array[4] = np.asarray(np.sqrt((x * x) + (y * y) + (z * z))[1440:-1440])
        var1 = [0] * l
        var1[43200:50400] = [1] * 7200
        varind = DataStream().KEYLIST.index('var1')
        array[varind] = np.asarray(var1)
        array[0] = np.asarray([startdate + timedelta(seconds=i) for i in range(0, l)])
        teststream = DataStream(header={'SensorID': 'Test_0001_0001'}, ndarray=np.asarray(array, dtype=object))
        teststream.header['col-x'] = 'X'
        teststream.header['col-y'] = 'Y'
        teststream.header['col-z'] = 'Z'
        teststream.header['col-f'] = 'F'
        teststream.header['unit-col-x'] = 'nT'
        teststream.header['unit-col-y'] = 'nT'
        teststream.header['unit-col-z'] = 'nT'
        teststream.header['unit-col-f'] = 'nT'
        teststream.header['col-var1'] = 'Switch'
        return teststream

    teststream = create_teststream(startdate=datetime(2022, 11, 22))

    fl = Flags()
    newfl = Flags()
    fo = Flags()
    nextfl = Flags()
    flagsmodified=Flags()
    ok = True
    errors = {}
    successes = {}
    if ok:
        testrun = './testflagfile.json' # define a test file later on
        t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)
        while True:
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],debug=False)
                fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['add'] = ("Version: {}: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['add'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR adding new data - will affect all other tests.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                reslist = fl._list(['starttime','endtime'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_list'] = ("Version: {}, _list: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['_list'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR extracting _list.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                newfl = fl.copy()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['copy'] = ("Version: {}, copy: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['copy'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR copy stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                trimfl = newfl.trim(starttime='2022-11-22T19:57:12.654362',endtime='2022-11-22T22:59:12.654362')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['trim'] = ("Version: {}, trim: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['trim'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR trim flag dictionary.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                nextfl = newfl.add(sensorid="GSM90_Y1112_0001",starttime="2022-11-22T10:56:12.654362",endtime="2022-11-22T10:59:12.654362",components=['f'],labelid='050',debug=False)
                obt = nextfl.select('labelid',['050'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['select'] = ("Version: {}, select: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['select'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR select flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                fo = Flags()
                fo = fo.add(sensorid="GSM90_Y1112_0001",starttime="2022-11-22T10:56:12.654362",endtime="2022-11-22T10:59:12.654362",components=['f'],labelid='050',debug=False)
                combfl = newfl.join(fo)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['join'] = ("Version: {}, join: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['join'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR join flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                fo = Flags()
                fo = fo.add(sensorid="GSM90_Y1112_0001",starttime="2022-11-22T10:56:12.654362",endtime="2022-11-22T10:59:12.654362",components=['f'],labelid='050',debug=False)
                fo.stats()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['stats'] = ("Version: {}, stats: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['stats'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR stats.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                diff = fo.diff(nextfl)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['diff'] = ("Version: {}, diff: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['diff'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR differences of flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                clean = fo.drop(parameter='sensorid', values=['GSM90_Y1112_0001'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['drop'] = ("Version: {}, drop: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['drop'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in drop flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                flagsmodified = nextfl.replace('comment','lightning','hell of a lightining strike')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['replace'] = ("Version: {}, replace: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['replace'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in replace flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                flagsmodified.fprint('GSM90_Y1112_0001')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['fprint'] = ("Version: {}, fprint: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['fprint'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in fprint flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                combfl = nextfl.union(level=0)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['union'] = ("Version: {}, union: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['union'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in union flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                mit,mat = nextfl.timerange()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['timerange'] = ("Version: {}, timerange: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['timerange'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in timerange flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                combfl = nextfl.rename_nearby(parameter='labelid', values=['001'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['rename_nearby'] = ("Version: {}, rename_nearby: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['rename_nearby'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in rename_nearby flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                trimstream = teststream.trim(starttime="2022-11-22", endtime="2022-11-23")
                fl = flag_range(trimstream, keys=['x'], above=20990)
                f1stream = fl.apply_flags(trimstream, mode='insert')
                f2stream = fl.apply_flags(trimstream, mode='drop')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['apply_flags'] = ("Version: {}, apply_flags: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['apply_flags'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in apply_flags flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                nextfl.save(path=testrun)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['save'] = ("Version: {}, save: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['save'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR saving flags.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                oldteststream = create_teststream(startdate=datetime(2022, 11, 22))
                oldteststream.ndarray[20] = ['0110000000000000-'] * len(oldteststream)
                oldteststream.ndarray[21] = ['Incredible awful data'] * len(oldteststream)
                fl = extract_flags(oldteststream)
                print (len(fl))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['extract_flags'] = (
                    "Version: {}, extract_flags: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['extract_flags'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with extract_flags")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                stfl1 = flag_range(teststream, keys=['x'], above=340)
                stfl2 = flag_range(teststream, keys=['x'], below=340)
                stfl3 = flag_range(teststream, keys=['x'], starttime='2022-11-22T07:00:00', endtime='2022-11-22T08:00:00',
                                keystoflag=['z'])
                stfl4 = flag_range(teststream, keys=['x'], starttime='2022-11-22T07:00:00', endtime='2022-11-22T08:00:00',
                                keystoflag=['z'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['flag_range'] = (
                    "Version: {}, flag_range: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['flag_range'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with flag_range")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                # convertstream = teststream.copy()
                stfl1 = flag_outlier(teststream)
                stfl2 = flag_outlier(teststream, keys=['x'], threshold=4, timerange=200)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['flag_outlier'] = (
                    "Version: {}, flag_outlier: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['flag_outlier'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with flag_outlier")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                bifl = flag_binary(teststream, key='var1', keystoflag=['f'], markalloff=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['flag_binary'] = (
                    "Version: {}, flag_outlier: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['flag_binary'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with flag_binary")

            # If end of routine is reached... break.
            break

        t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
        time_taken = t_end_test - t_start_test
        print(datetime.now(timezone.utc).replace(tzinfo=None), "- Flagging runtime testing completed in {} s. Results below.".format(time_taken.total_seconds()))

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
