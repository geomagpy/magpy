# -*- coding: utf-8 -*

"""
Code for transformation of datatime.datetime objects in isodate tuple format into float values
with 0.0 : year=0, month=1, day=1, hour=0, minute=0, second=0 called 'zerotime' values
leap seconds, days and years are taken into account
"""
from os import getppid#, getpid
import sys
from multiprocessing import Pool, freeze_support#, Queue#, Array#, Process, Lock
#from multiprocessing import Process, Manager, freeze_support#, Array, Queue, Lock
#from concurrent.futures import ProcessPoolExecutor as PPE
#from concurrent.futures import ThreadPoolExecutor as TPE
#from concurrent.futures import Executor as PE
#from concurrent.futures import as_completed
#import loky as lk
#from loky import get_reusable_executor
from psutil import cpu_count
from psutil import Process as Prcs
from numpy import array, shape, max, floor, array_split, sort, ndarray
from itertools import chain
import datetime
#from matplotlib.dates import num2date, date2num
import gc # for garbage collection of unused variables
#lk.backend(method='spawn') # loky backend forking settings - may not work
gc.enable()
#def worker((isodatetuple)): # for Pool.apply:async

debug = False



def worker(L, isodatetuple):#, lock): # for Processes and Manager().list()
    #lock.acquire()
    #pid = (Prcs(getpid())).pid
    #print( '\t... with pid: {}'.format( pid))
#def worker(q, l, isodatetuple): # for Process and acquire
#def NUM2DATE20TIME(isodatestring):
###############################
# CONVERTS A num2date OUTPUT IN ISOFORMAT TO A zerotime VALUE STARTING FROM YEAR 0, DAY 0, HOUR 0, MINUTE 0 AND SECOND 0
###############################
    #year = int(isodatestring[0:4])
    ###########
    # ONLY FOR Process
    #isodatetuple = isodatetuple.get()
    #q =Queue()
    #l.acquire() # for Process
    ###########
    #print( 'before conversion isodatetuple is:\n\t{}'.format( isodatetuple))
    #iterableList = num2date( isodatetuple)
    iterableList = isodatetuple
    #iterableList = datetime.datetime.combine( datetime.datetime.date( temp), datetime.datetime.time( temp))
    #print( 'iterableList is:\n\t{}'.format( iterableList))
    #print( 'shape of iterableList is:\n\t{}'.format( shape( iterableList)))
    #print( 'after conversion iterableList is:\n\t{}'.format( iterableList))
    #isodatetuple = list( isodatetuple)
    #zerotime = []
    for t in iterableList:
        #print ('isodatetuple is: {}'.format( t))
        year = t.year
        #mon = int(isodatestring[5:7])
        mon = t.month
        #day = int(isodatestring[8:10])
        day = t.day
        #hh = int(isodatestring[11:13])
        hh = t.hour
        #mm = int(isodatestring[14:16])
        mm = t.minute
        #ss = float(isodatestring[17:25])
        ss = t.second + t.microsecond/1000000.0
        #hhshift = int(isodatestring[27:29])
        #mmshift = int(isodatestring[30:32])
        # COUNTING THE DAYS SINCE BEGINNING OF YEAR
        #print( 'year {}, month {}, day {}, hh {}, mm {}, ss {}'.format( year, mon, day, hh, mm, ss))
        pastdays = 0.0
        for n in range(1,mon+1):
        #    if mod(n,2) != 0 and n <= 7:
            if n%2 != 0 and n <= 7:
                multi = 31.0
            elif n%2 == 0 and n >= 8:
                multi = 31.0
            elif n%2 == 0 and n <= 7 and n != 2:
              multi = 30.0
            elif n%2 != 0 and n >= 8:
              multi = 30.0
            elif n == 2 and year%4 == 0 and year%100 != 0 and year%400 == 0:
              multi = 29.0
            elif n == 2 and year%4 != 0:
              multi = 28.0
            pastdays = pastdays + multi
        pastdays = pastdays - multi
        #      printf("pastdays : ##%i##\n",pastdays);
        #sumwinshift = float(hhshift) + float(mmshift)/60.0 # SHIFT FOR SUMMER-WINTERTIME-DEVIATIONS !!! TO BE IMPROVED !!!
        sumwinshift = 0.0 # SHIFT FOR SUMMER-WINTERTIME-DEVIATIONS !!! TO BE IMPROVED !!!
        #
        timeof = (float((float(year)*float(365.2425)))+float(pastdays)+float(day))*float(86400.0)+float(hh-sumwinshift)*float(3600.0) + float(mm)*float(60.0) + float(ss) # + float(mus)/float(1000000);
        #      zerotime = float(substr(isodatestring,length(isodatestring)-17,18));
        L.append( float(timeof))
    #zerotime = array( L) # in combination with Pool
    #q.put( array( zerotime)) # for Process
    #l.release()
    #gc.collect() # garbage collection of unused variables
    #print( 'zerotime is: {}'.format( zerotime))
    #L.append( zerotime)
    #print( 'L is: {}'.format( L))
    """
    localvars = list( locals())
    for f in localvars:
        #if( f != 'zerotime'):
        if( f != 'L'):
            #print( 'variable to delete is: {}'.format( f))
            del f
    """
    #lock.release()
    return# zerotime UEBERGABE DER WERTE NUR ÜBER DIE SHARED MEMORY LIST L


def poolworker((isodatetuple)): # for Pool.apply_async
#def worker(L, isodatetuple):#, lock): # for Processes and Manager().list()
    #lock.acquire()
    #pid = (Prcs(getpid())).pid
    #print( '\t... with pid: {}'.format( pid))
#def worker(q, l, isodatetuple): # for Process and acquire
#def NUM2DATE20TIME(isodatestring):
###############################
# CONVERTS A num2date OUTPUT IN ISOFORMAT TO A zerotime VALUE STARTING FROM YEAR 0, DAY 0, HOUR 0, MINUTE 0 AND SECOND 0
###############################
    #year = int(isodatestring[0:4])
    ###########
    # ONLY FOR Process
    #isodatetuple = isodatetuple.get()
    #q =Queue()
    #l.acquire() # for Process
    ###########
    #print( 'before conversion isodatetuple is:\n\t{}'.format( isodatetuple))
    #iterableList = num2date( isodatetuple)
    iterableList = isodatetuple
    #iterableList = datetime.datetime.combine( datetime.datetime.date( temp), datetime.datetime.time( temp))
    #print( 'iterableList is:\n\t{}'.format( iterableList))
    #print( 'shape of iterableList is:\n\t{}'.format( shape( iterableList)))
    #print( 'after conversion iterableList is:\n\t{}'.format( iterableList))
    #print( 'hour of iterableList is:\n\t{}'.format( iterableList[0].hour))
    #isodatetuple = list( isodatetuple)
    zerotime = []
    for t in iterableList:
        #print ('isodatetuple is: {}'.format( t))
        year = t.year
        #mon = int(isodatestring[5:7])
        mon = t.month
        #day = int(isodatestring[8:10])
        day = t.day
        #hh = int(isodatestring[11:13])
        hh = t.hour
        #mm = int(isodatestring[14:16])
        mm = t.minute
        #ss = float(isodatestring[17:25])
        ss = t.second + t.microsecond/1000000.0
        #hhshift = int(isodatestring[27:29])
        #mmshift = int(isodatestring[30:32])
        # COUNTING THE DAYS SINCE BEGINNING OF YEAR
        #print( 'year {}, month {}, day {}, hh {}, mm {}, ss {}'.format( year, mon, day, hh, mm, ss))
        pastdays = 0.0
        for n in range(1,mon+1):
        #    if mod(n,2) != 0 and n <= 7:
            if n%2 != 0 and n <= 7:
                multi = 31.0
            elif n%2 == 0 and n >= 8:
                multi = 31.0
            elif n%2 == 0 and n <= 7 and n != 2:
              multi = 30.0
            elif n%2 != 0 and n >= 8:
              multi = 30.0
            elif n == 2 and year%4 == 0 and year%100 != 0 and year%400 == 0:
              multi = 29.0
            elif n == 2 and year%4 != 0:
              multi = 28.0
            pastdays = pastdays + multi
        pastdays = pastdays - multi
        #      printf("pastdays : ##%i##\n",pastdays);
        #sumwinshift = float(hhshift) + float(mmshift)/60.0 # SHIFT FOR SUMMER-WINTERTIME-DEVIATIONS !!! TO BE IMPROVED !!!
        sumwinshift = 0.0 # SHIFT FOR SUMMER-WINTERTIME-DEVIATIONS !!! TO BE IMPROVED !!!
        #
        timeof = (float((float(year)*float(365.2425)))+float(pastdays)+float(day))*float(86400.0)+float(hh-sumwinshift)*float(3600.0) + float(mm)*float(60.0) + float(ss) # + float(mus)/float(1000000);
        #      zerotime = float(substr(isodatestring,length(isodatestring)-17,18));
        zerotime.append( float(timeof))
    zerotime = array( zerotime) # in combination with Pool
    #print( 'zerotime in subprocess is: {}'.format( zerotime))
    #q.put( array( zerotime)) # for Process
    #l.release()
    #gc.collect() # garbage collection of unused variables
    #print( 'zerotime is: {}'.format( zerotime))
    #L.append( zerotime)
    #print( 'L is: {}'.format( L))
    """
    localvars = list( locals())
    for f in localvars:
        #if( f != 'zerotime'):
        if( f != 'L'):
            #print( 'variable to delete is: {}'.format( f))
            del f
    """
    #lock.release()
    return zerotime




def NUM2DATE20TIME(isodatetuple):
    if( debug):
        print( 'NUM2DATE20TIME started...')
    #pid = os.getpid()
    #print( 'starting parent-process with pid: {}'.format( pid))
#def NUM2DATE20TIME(isodatestring):
###############################
# CONVERTS A num2date OUTPUT IN ISOFORMAT TO A zerotime VALUE STARTING FROM YEAR 0, DAY 0, HOUR 0, MINUTE 0 AND SECOND 0
###############################
    #year = int(isodatestring[0:4])
    #print( '__name_ is: {}'.format( __name__))
    #print( 'isodatetuple is ndarray:\n\t{}'.format( isinstance( isodatetuple, ndarray)))
    if __name__ == "num2date20time":
        freeze_support()
        #gc.enable()
        dump = []
        zerotime =[]
        #isodatetuple = array( isodatetuple)
        #print 'type of isodatetuple is: ', type( isodatetuple)
        #print 'type of isodatetuple is datetime.datetime: ', isinstance( isodatetuple, datetime.datetime)
        #print ('isodatetuple is: ', isodatetuple)
        
        #if( isinstance( isodatetuple, datetime.datetime)):
        #    isodatetuple = date2num( isodatetuple)
        #if( isinstance( isodatetuple, datetime.datetime)):
        if( debug):
            print( 'isodatetuple is of type:\n\t{}'.format( type( isodatetuple)))
        #print( 'isodatetuple is:\n\t{}'.format( isodatetuple))
        #print( 'shape of isodatetuple is:\n\t{}'.format( shape( isodatetuple)))
        #print( 'type datetime.datetime type is: {}'.format( datetime.datetime))
        if( isinstance( isodatetuple, list)):
            #print 'NO'
            dump.append( isodatetuple)
            dump = array( isodatetuple) # so that data is always an array
            #print( 'shape of array like dump is:{}'.format( shape( dump)))
        elif( isinstance( isodatetuple, datetime.datetime)):
            #print 'YES'
            dump.append( isodatetuple)
            dump = array( dump)
            #print( 'shape of float like dump is:{}'.format( shape( dump)))
            #dump.append( isodatetuple)
            #print 'type of dump is: ', type( dump)
            #print( 'dump is:{}'.format( dump))
            #print( 'shape of dump is:{}'.format( shape( dump)))
        elif( isinstance( isodatetuple, ndarray)):
            if( debug):
                print( 'recognized isodatetuple as ndarray')
            dump = isodatetuple
        else:
            print( 'wrong input format! use datetime.datetime objects or numpy.ndarray of posix timestamps')
            sys.exit()
        
        
        agents = cpu_count(logical = False) - 1
        taskspercpu = agents * cpu_count(logical = True)
        dumpdim = shape( dump)
        if( dumpdim == () ):
            dumpdim = (1,)
        #data = []
        if( max( dumpdim) > 1) :
            if( debug):
                print( 'MULTIPLE VALUES...')
            incind = int( floor( float( max( shape( dump)))/ float( agents * taskspercpu)))
            if( debug):
                print( 'Splitting array into\n{} long arrays\nmultiprocessing...'.format( incind))
            """
            for i in range( 0, incind, incind): #max( shape( dump)) - incind + 1
                data.append( dump[0][i:i + incind:])
                #print( 'HERE shape of data[-1] is: {}'.format( shape( data[-1])))
            if( i + incind < max( shape( dump))):
                data.append( dump[0][i + incind::])
            print( '{} physicsal cpu cores will be used...'.format( agents))
            #print( 'HERE shape of data is: {}'.format( shape( data)))
            print( 'HERE len of data is: {}'.format( len( data)))
            """
            #print( '{} physicsal cpu cores will be used...'.format( agents))
            data = array_split( dump, agents * taskspercpu)
            #print( 'HERE shape of data is: {}'.format( shape( data)))
            #sys.exit()
        else:
            if( debug):
                print( 'ONLY SINGLE VALUE...')
            agents = 1 # - 1
            data = dump
            #print( '{} physicsal cpu cores will be used...'.format( agents))
            #print( 'HERE shape of data is: {}'.format( shape( data)))
        #agents = cpu_count() - 1
        
        
        TruePool = True
        if( not TruePool): # STARTING MANAGED PROCESS ALGORYTHM
            
            
            #print( 'HERE shape of dump is: {}'.format( shape( dump)))
            pid = (Prcs(getppid())).pid
            if( debug):
                print( '{} physicsal cpu cores with {} tasks per cpu will be used...'.format( agents, taskspercpu))
                print( 'Parent process with pid {} starting child-processes...'.format( pid))
            #with PPE( max_workers = agents) as p:
            
            with Manager() as manager:
                if( debug):
                    print( 'Starting Manager Processes...')
                #p = Pool( processes = agents, maxtasksperchild = agents*10)
                #p = Pool( agents, maxtasksperchild = agents + 1) # WORKING GOOD WITH os.fork() problem when used on big data
                """ 
                maxtasksperchild: from python multiprocessing docs: 
                New in version 2.7: maxtasksperchild is the number of tasks a worker process
                can complete before it will exit and be replaced with a fresh worker process,
                to enable unused resources to be freed. The default maxtasksperchild is None,
                which means worker processes will live as long as the pool.
                
                Note Worker processes within a Pool typically live for the complete duration 
                of the Pool’s work queue. A frequent pattern found in other systems (such as 
                Apache, mod_wsgi, etc) to free resources held by workers is to allow a worker
                within a pool to complete only a set amount of work before being exiting, 
                being cleaned up and a new process spawned to replace the old one. The 
                maxtasksperchild argument to the Pool exposes this ability to the end user. 
                """
                #q = Queue() # with Process
                #lock = Lock() # with Process
                #q.put( dump)
                #print( 'q is: {}'.format( q.get()))
                
                L = manager.list()
                #IL.append( dump)
                processes = []
                if( len( dump) > 1):
                    #print( 'max of dumpdim is: {}'.format( max( dumpdim)))
                    #print( 'shape of dump is:\n\t{}'.format( shape( data)))
                    #g = Array( 'd', dump[0])
                    #print( 'shape of shared memory array is:\n\t{}'.format( shape( g)))
                    #lock = Lock()
                    for d in data:
                        IL = manager.list( array( d))
                        #g = Array( 'd', d)
                        #print( 'shape of part of data is: {}'.format( shape( d)))
                        p = Process( target = worker, args = ( L, IL ))#, lock))
                        p.start()
                        if( debug):
                            print( '\t... with pid: {}'.format( p.pid))
                        processes.append( p)
                    for p in processes:
                        #p.close()
                        #q.get()
                        p.join()
                    #zerotime = L
                else:
                    #print( 'max of dumpdim is: {}'.format( max( dumpdim)))
                    #d = dump
                    #lock = Lock()
                    IL = manager.list( data)
                    #print( 'd single is: {}'.format( d))
                    p = Process( target = worker, args = ( L, IL ))#, lock ))
                    p.start()
                    if( debug):
                        print( '\t... with pid: {}'.format( p.pid))
                    processes.append( p)
                    #p.close()
                    p.join()
                    if( debug):
                        print( '\t\t {}...finished:'.format( p.pid))
                gc.collect()
                #print( 'L is:\n\t{}'.format( L))
                #zerotime = array( list( chain( *L)))
                zerotime = array( L)
                #p.close()
        elif( TruePool): # STARTING POOL ALGORYTHM
            if( debug):
                print( 'Starting Pool Processes...')
            #q = Queue()
            #q.put( dump)
            #print( 'data for input is: {}'.format( data))+
            try:
                p = Pool( processes = agents, maxtasksperchild = taskspercpu)
                multiprocesses = []
                temp = []
                if( max( dumpdim) > 1):
                    if( debug):
                        print( '...with {} cpu and {} taskspercpu, applied on {} lists'.format( agents, taskspercpu, min( shape( data))))
                    for d in data:
                        #print( 'd in data is: {}'.format( d))
                        multiprocesses.append( p.apply_async( poolworker, (d,)))
                    for res in multiprocesses:
                        res.wait()
                        temp.append( res.get(timeout = 2))
                    temp = array( list( chain(*temp)))
                    #print( 'temp after multiprocessing is: {}'.format( temp))
                    #print( 'shape of temp after multiprocessing is: {}'.format( shape( temp)))
                else:
                    if( debug):
                        print( '...with {} cpu and {} taskspercpu, applied on {} element'.format( agents, taskspercpu, min( dumpdim)))
                    #print( 'data is: {}'.format( data))
                    multiprocesses.append( p.apply_async( poolworker, (data,)))
                    for res in multiprocesses:
                        res.wait()
                        temp.append( res.get(timeout = 2))
                    temp = list( chain(*temp))
                zerotime = array( sort( temp))
                p.close()
                p.join()
            except Exception as exc:
                print( 'Pool Error with:\n\t{}'.format( exc))
            #p.join()
            #print( 'Pool zerotime is: \n\t{}'.format( zerotime))
            #zerotime = p.map( worker, (dump,))[0] # WORKING GOOD with multiprocessing pool except for big data amounts. for example above 1 week
        if( debug):
            print( 'shape of zerotime is: {}'.format( shape( zerotime)))
        if( max( shape( zerotime)) == 1):
            zerotime = float(zerotime[0])
            """
            year = isodatetuple.year
            #mon = int(isodatestring[5:7])
            mon = isodatetuple.month
            #day = int(isodatestring[8:10])
            day = isodatetuple.day
            #hh = int(isodatestring[11:13])
            hh = isodatetuple.hour
            #mm = int(isodatestring[14:16])
            mm = isodatetuple.minute
            #ss = float(isodatestring[17:25])
            ss = isodatetuple.second + isodatetuple.microsecond/1000000.0
            #hhshift = int(isodatestring[27:29])
            #mmshift = int(isodatestring[30:32])
            OUNTING THE DAYS SINCE BEGINNING OF YEAR
            pastdays = 0.0
            for n in range(1,mon+1):
            if mod(n,2) != 0 and n <= 7:
                if n%2 != 0 and n <= 7:
                    multi = 31.0
                elif n%2 == 0 and n >= 8:
                    multi = 31.0
            elif n%2 == 0 and n <= 7 and n != 2:
                multi = 30.0
            elif n%2 != 0 and n >= 8:
                multi = 30.0
            elif n == 2 and year%4 == 0 and year%100 != 0 and year%400 == 0:
                multi = 29.0
            elif n == 2 and year%4 != 0:
                multi = 28.0
            pastdays = pastdays + multi
        pastdays = pastdays - multi
        #      printf("pastdays : ##%i##\n",pastdays);
        #sumwinshift = float(hhshift) + float(mmshift)/60.0 # SHIFT FOR SUMMER-WINTERTIME-DEVIATIONS !!! TO BE IMPROVED !!!
        sumwinshift = 0.0 # SHIFT FOR SUMMER-WINTERTIME-DEVIATIONS !!! TO BE IMPROVED !!!
        #
        timeof = (float((float(year)*float(365.2425)))+float(pastdays)+float(day))*float(86400.0)+float(hh-sumwinshift)*float(3600.0) + float(mm)*float(60.0) + float(ss) # + float(mus)/float(1000000);
        #      zerotime = float(substr(isodatestring,length(isodatestring)-17,18));
        zerotime = float(timeof)
        """
        #print( 'zerotime is: {}'.format( zerotime))
        #gc.collect()
        localvars = list( locals())
        for f in localvars:
            if( f != 'zerotime'):
                #print( 'variable to delete is: {}'.format( f))
                del f
        gc.collect()
        del gc.garbage[:]
    return array( zerotime)



"""
OLD WORKING

def NUM2DATE20TIME(isodatetuple):
#def NUM2DATE20TIME(isodatestring):
###############################
# CONVERTS A num2date OUTPUT IN ISOFORMAT TO A zerotime VALUE STARTING FROM YEAR 0, DAY 0, HOUR 0, MINUTE 0 AND SECOND 0
###############################
  #year = int(isodatestring[0:4])
  year = isodatetuple.year
  #mon = int(isodatestring[5:7])
  mon = isodatetuple.month
  #day = int(isodatestring[8:10])
  day = isodatetuple.day
  #hh = int(isodatestring[11:13])
  hh = isodatetuple.hour
  #mm = int(isodatestring[14:16])
  mm = isodatetuple.minute
  #ss = float(isodatestring[17:25])
  ss = isodatetuple.second + isodatetuple.microsecond/1000000.0
  #hhshift = int(isodatestring[27:29])
  #mmshift = int(isodatestring[30:32])
	# COUNTING THE DAYS SINCE BEGINNING OF YEAR
  pastdays = 0.0
  for n in range(1,mon+1):
#    if mod(n,2) != 0 and n <= 7:
    if n%2 != 0 and n <= 7:
      multi = 31.0
    elif n%2 == 0 and n >= 8:
      multi = 31.0
    elif n%2 == 0 and n <= 7 and n != 2:
      multi = 30.0
    elif n%2 != 0 and n >= 8:
      multi = 30.0
    elif n == 2 and year%4 == 0 and year%100 != 0 and year%400 == 0:
      multi = 29.0
    elif n == 2 and year%4 != 0:
      multi = 28.0
    pastdays = pastdays + multi
  pastdays = pastdays - multi
	#      printf("pastdays : ##%i##\n",pastdays);
  #sumwinshift = float(hhshift) + float(mmshift)/60.0 # SHIFT FOR SUMMER-WINTERTIME-DEVIATIONS !!! TO BE IMPROVED !!!
  sumwinshift = 0.0 # SHIFT FOR SUMMER-WINTERTIME-DEVIATIONS !!! TO BE IMPROVED !!!
	#
  timeof = (float((float(year)*float(365.2425)))+float(pastdays)+float(day))*float(86400.0)+float(hh-sumwinshift)*float(3600.0) + float(mm)*float(60.0) + float(ss) # + float(mus)/float(1000000);
	#      zerotime = float(substr(isodatestring,length(isodatestring)-17,18));
  zerotime = float(timeof)
  return zerotime
"""