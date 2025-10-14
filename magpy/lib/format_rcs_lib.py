
from datetime import *
import os

def compare_signal(a,b,headers):
    diff_a = {}
    diff_b = {}
    for elem in a:
        if not a[elem]==b[elem] and not elem in headers['ini_ignore']:
            diff_a[elem] = a[elem]
            diff_b[elem] = b[elem]
            #diffstr + ' | '+elem+' |old: '+a[elem]+' |new: '+b[elem]+'\t|' # before: string instead of dict
    return diff_a, diff_b


def compare_signallist(a,b,headers):
    debug = False
    difflist_a = {}
    difflist_b = {}
    if not len(a) == len(b):
        #diffstr = 'length of signallists differ: '+str(len(a))+' -> '+str(len(b)) # before: string string instead of dict
        difflist_a['length'] = len(a)
        difflist_b['length'] = len(b)
        if debug:
            print(difflist_a)
            print(difflist_b)
    if len(a) >= len(b):
        for sig in a:
            if sig in b:
                if not a[sig] == b[sig]:
                    diff_a, diff_b = compare_signal(a[sig],b[sig],headers)
                    if diff_a or diff_b:
                        difflist_a[sig] = diff_a
                        difflist_b[sig] = diff_b
                        if debug:
                            print('+++++++++++++++++++++++++++')
                            print('Signal a: '+sig)
                            print(diff_a)
                            print('Signal b: '+sig)
                            print(diff_b)
                        pass
    else:
        for sig in b:
            if sig in a:
                if not a[sig] == b[sig]:
                    diff_a, diff_b = compare_signal(a[sig],b[sig],headers)
                    if diff_a or diff_b:
                        difflist_a[sig] = diff_a
                        difflist_b[sig] = diff_b
                        if debug:
                            print('+++++++++++++++++++++++++++')
                            print('Signal a: '+sig)
                            print(diff_a)
                            print('Signal b: '+sig)
                            print(diff_b)
                        pass
    return difflist_a, difflist_b


def get_max_range(time,configlist,headers):
    debug = False
    fr, un, s = getini(time,configlist,headers)
    valid_from = fr
    difflist_a = {}
    # difflist_b can't be empty if difflist_a isn't
    while not difflist_a:
        fr_before = fr
        fr, un, s_older = getini(fr - timedelta(seconds=1),configlist,headers)
        if fr == fr_before:
            # this means that getini didn't find an older valid file
            # continue searching younger files
            break
        if 'error' in s_older:
            if s_older['error'] == 'no config file found - time too old?':
                s = s_older
                break
        if debug:
            print('back - trying '+str(fr)+' - '+str(un))
            #print(s_older)
        difflist_a, difflist_b = compare_signallist(s_older, s, headers)
        if not difflist_a and not 'error' in s_older:
            # if no difference try older configlist, skip erroneous file
            valid_from = fr
            s = s_older
    # "goto" given ini file "in the middle"
    fr, un, s = getini(time,configlist,headers)
    valid_until = un
    difflist_a = {}
    while not difflist_a:
        fr, un, s_younger = getini(un,configlist,headers)
        if debug:
            print('forward - trying '+str(fr)+' - '+str(un))
        difflist_a, difflist_b = compare_signallist(s, s_younger, headers)
        if not difflist_a and not 'error' in s_younger:
            # if no difference try younger configlist, skip erroneous file
            valid_until = un
            s = s_younger
        if datetime.utcnow() - valid_until < timedelta(seconds=50):
            break

    fr, un, s = getini(valid_from,configlist,headers)
    return valid_from, valid_until, s



def getini(time,configlist,headers):
    '''
    DESCRIPTION:
      find config file of time range
      returns valid time range, index of configlist (filename)
      example file:
        S1-config_2025_07_16_151302.INI
    APPLICATION:
      valid_until, index_configlist = getini(time,configlist,headers)
    '''
    debug = False
    FP = headers['FP']
    configlist.sort()
    idx = len(configlist)
    oldest_idx_remembered = None
    last_idx = None
    valid_until = datetime.utcnow()
    while idx > 0:
        idx = idx - 1
        try:
            initime = datetime.strptime(configlist[idx],FP+'-config_%Y_%m_%d_%H%M%S.INI')
        except:
            if debug:
                print('warning: got no time stamp from filename '+configlist[idx])
            continue
        if initime < datetime(2010,6,1):
            # filenames 1904 are not valid
            if not oldest_idx_remembered:
                # save oldest reasonable ini file
                oldest_idx_remembered = idx + 1
            continue
        if time < initime:
            # ini file too young, searching for older one
            valid_until = initime
            last_idx = idx
            continue
        else:
            signaldict = readIniFile(configlist[idx],headers)
            if not 'error' in signaldict:
                valid_from = initime
                return valid_from, valid_until, signaldict
            else:
                if debug:
                    print('error in '+configlist[idx])
                    #print(signaldict)
                    print('searching for older one')
    if not oldest_idx_remembered:
        # oldest_idx_remembered works for only for files with 1904...
        oldest_idx_remembered = last_idx
    signaldict = {}
    #signaldict['error'] = 'no config file found - time too old? - try '+configlist[oldest_idx_remembered]
    # "no config file found" means that the given time is too old,
    # so the first valid file should be returned
    valid = False
    idx = oldest_idx_remembered
    while not valid:
        if idx == len(configlist):
            print('get_ini: searched in all config files, no match.')
        signaldict = readIniFile(configlist[idx],headers)
        if 'error' in signaldict:
            idx = idx + 1
        else:
            valid = True
    return valid_until, valid_until, signaldict


def readIniFile(config_filename,headers):
    '''
    DESCRIPTION:
      get signal description from RCS ini file
      if ini file has errors, there will be 'error' in the dict 
    APPLICATION:
      signaldict = readIniFile(configlist,idx)

    [op]
    user=Monitor
    [general]
    Client_Name=zagtfps1
    Client_IP=138.22.188.71
    Server_IP=138.22.188.209
    [frontpanel]
    1="Spannungsüberwachung 1-11"
    2=reserve
    3=reserve
    4=reserve
    5=reserve
    6=reserve
    7=reserve
    8=Status
    [states]
    1="ZAGTFPS1     M1      I,cFP-AI-110    CH00    1P1     13,8V 1: GPS/LWL Konv: GPS8, GPS9       OK      Out Of Range, S AV266H40        mA      y=6000x+0       AI"

    '''
    inidir = headers['inidir']
    if 'debug' in headers:
        debug = headers['debug']
    else:
        debug = False
    getAllSignals = headers.get('getAllSignals')
    signaldict = {}
    ini = open(os.path.join(inidir,config_filename),'r',encoding='windows-1252')
    status = 'start'
    fileOK = True
    cnt = 0
    errorstr = ''
    FP = headers['FP']
    signals = headers['signals']
    for line in ini.readlines():
        if line.strip().startswith('#'):
            continue
        if status == 'start':
            if line.strip().startswith('Client_Name='):
                # TODO not yet used
                fpname = line.split('=zagtfp')[1]
                status = '[states]'
        elif status == '[states]':
            if line.strip() == '[states]':
                status = 'read'
        elif status == 'read':
            sig = {}
            if line.strip() == '':
                continue
            cnt = cnt + 1
            if not getAllSignals:
                # don't skip any signals
                if not cnt in signals:
                    # signal not in signals list, skip line
                    continue
            # reserve is used to ignore non given longnames, nokmessages and messagetypes
            reserve = 'reserve' in line or 'RESERVE' in line or 'Reserve' in line
            sig['nr'] = line.strip().split('=')[0]
            if not sig['nr'].isdigit():
                errorstr = 'Signal '+str(cnt)+': no number: '+sig['nr']
                sig['error'] = errorstr
                fileOK = False
                break
            elif not int(sig['nr']) == cnt:
                errorstr = 'Signal '+str(cnt)+': wrong number: '+sig['nr']
                sig['error'] = errorstr
                fileOK = False
                break
            l = line.strip().split('\t')
            if not len(l) == 12:
                errorstr = 'Signal '+str(cnt)+': wrong number of columns: '+str(len(l))+'/12'
                sig['error'] = errorstr
                fileOK = False
                break
            l1 = l[0].split('ZAGTFP')
            if not len(l1) == 2:
                errorstr = 'Signal '+str(cnt)+': cannot get FP name: '+l[0]
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['FP'] = l[0].split('ZAGTFP')[1]
            if l[1] == '':
                errorstr = 'Signal '+str(cnt)+': cannot get module'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['module'] = l[1]
            if l[2] == '':
                errorstr = 'Signal '+str(cnt)+': cannot get modulename'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['modulename'] = l[2]
            if l[3] == '':
                errorstr = 'Signal '+str(cnt)+': cannot get channelname'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['channelname'] = l[3]
            if l[4] == '':
                errorstr = 'Signal '+str(cnt)+': cannot get shortname'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['shortname'] = l[4]
            if l[5] == '' and not reserve:
                errorstr = 'Signal '+str(cnt)+': cannot get longname'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['longname'] = l[5]
            if l[6] == '':
                errorstr = 'Signal '+str(cnt)+': cannot get OKname'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['okmessage'] = l[6]  # message when in default state
            l1 = l[7].split(', ')
            if l1[0] == '':
                errorstr = 'Signal '+str(cnt)+': cannot get NOKname'
                sig['error'] = errorstr
                fileOK = False
                break
            sig['nokmessage'] = l[7].split(', ')[0] # message when in abnormal state
            if len(l1) == 2:
                if not l1[1] == '' or reserve:
                    sig['messagetype'] = l[7].split(', ')[1] # .. kind of message: 'B':operation 'S':error 'A':alarm
                else:
                    errorstr = 'Signal '+str(cnt)+': cannot get messagetype'
                    sig['error'] = errorstr
                    fileOK = False
                    break
            elif reserve:
                sig['messagetype'] = ''
            else:
                errorstr = 'Signal '+str(cnt)+': cannot get messagetype'
                sig['error'] = errorstr
                fileOK = False
                break
            if l[8] == '':
                errorstr = 'Signal '+str(cnt)+': cannot get defaultrange'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['defaultrange'] = l[8]
            # unit not mandatory, can be ''
            sig['unit'] = l[9]
            if l[10] == '':
                errorstr = 'Signal '+str(cnt)+': cannot get equation'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['equation'] = l[10]
            if len(l[11]) < 2:
                errorstr = 'Signal '+str(cnt)+': cannot get binary/float nor direction'
                sig['error'] = errorstr
                fileOK = False
                break
            if not l[11][0] in ['D','A']:
                errorstr = 'Signal '+str(cnt)+': cannot get binary/float'
                sig['error'] = errorstr
                fileOK = False
                break
            sig['bin_float'] = l[11][0] # 'D'..binary 'A'..float number
            if not l[11][1] in ['I','O']:
                errorstr = 'Signal '+str(cnt)+': cannot get direction'
                sig['error'] = errorstr
                fileOK = False
                break
            else:
                sig['direction'] = l[11][1] # 'I'..input signal 'O'..output signal
            # TODO weg, war ein Versuch vor getAllSignals
            #if fileOK and int(sig['nr']) == len(s)+1:
            # only if all signals are chosen
            if fileOK:
                if not sig['nr'] in signaldict:
                    signaldict[sig['nr']] = sig
                else:
                    errorstr = 'Signal '+sig['nr']+'at least twice in signallist'
                    signaldict[sig['nr']]['error'] = errorstr
                    fileOK = False
                    break
    ini.close()
    if cnt == 0:
        errorstr = 'no signals got from file'
        signaldict['error'] = errorstr
        fileOK = False
    if fileOK and signals:
        for s in signals:
            if not str(s) in signaldict:
                if debug:
                    print(str(s)+' not in '+config_filename)
                errorstr = config_filename+': not all signals got from file'
                signaldict['error'] = errorstr
                fileOK = False

    if fileOK:
        return signaldict
    else:
        if not status == 'read':
            errorstr = config_filename+': no signals found'
            signaldict['error'] = errorstr
            return signaldict
        if 'error' in sig:
            errorstr = config_filename+': '+sig['error']
        else:
            errorstr = config_filename+': errors in some signal descriptions'
        signaldict['error'] = errorstr
        return signaldict
