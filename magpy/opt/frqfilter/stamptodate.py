from multiprocessing import Pool
from psutil import cpu_count
from numpy import array, int32, floor, mod, shape, vstack, sort, any, int64, min, max
from itertools import chain
from datetime import datetime
try:
    from datetime import timezone
    utc = timezone.utc
except ImportError:
    #Hi there python2 user
    class UTC(tzinfo):
        def utcoffset(self, dt):
            return timedelta(0)
        def tzname(self, dt):
            return "UTC"
        def dst(self, dt):
            return timedelta(0)
    utc = UTC()
#import time
agents = cpu_count(logical = False) - 1
debug = False
if( debug):
    import sys


def NoMinFloor( x):
    remain = float( x) - float( floor( x))
    return remain


#def worker((zerotime)): # python2.7
def worker(zerotime): # python3
    ############
    # year ... integer 4 digits
    # mon  ... integer 2 digits
    # dd   ... integer 2 digits
    # hh   ... integer 2 digits
    # mm   ... integer 2 digits
    # ss   ... float float
    ############
    year =[]
    mon = []
    dd = []
    hh = []
    mm = []
    ss = []
    out = []
    array31 = array( [ 1, 3, 5, 7, 8, 10, 12])
    array30 = array( [ 4, 6, 9, 11])
    dayseconds = float( 86400.0)
    #print( zerotime)
    
    
    #####
    # preliminary year determination
    #####
    
    l = 1
    cz = int64( 0)
    
    minz = min( zerotime)
    maxz = max( zerotime)
    
    z = minz#, maxz]:
    if( debug):
        print( 'cz', cz)
        print( 'z', z)
        print( 'float( z) - dayseconds', float( z) - dayseconds)
        print( 'minz', minz)
        print( 'maxz', maxz)
    
    if( float( z) >= dayseconds):
        if( debug):
            print( 'float( z) >= dayseconds', float( z) >= dayseconds)
        while( ( float( cz) <= float( z) - dayseconds)):
            #print( 'float( cz) <= float( z)', float( cz) <= float( z), 'float( cz), float( z)', float( cz),  float( z))
            bakcz = cz
            if( ( ( mod( l, 4) == 0) or ( mod( l, 400) == 0)) and ( mod( l, 100) != 0)):
                cz = cz + int64( 366* 86400)
            else:
                cz = cz + int64( 365* 86400)
            #print( 'cz', cz, 'l', l)
            l += 1
    else:
        if( debug):
            print( 'float( z) >= dayseconds', float( z) >= dayseconds)
        while( ( float( cz) <= dayseconds)):
            #print( 'float( cz) <= dayseconds', float( cz) <= dayseconds, 'float( cz), dayseconds', float( cz),  dayseconds)
            bakcz = cz
            if( ( ( mod( l, 4) == 0) or ( mod( l, 400) == 0)) and ( mod( l, 100) != 0)):
                cz = cz + int64( 366* 86400)
            else:
                cz = cz + int64( 365* 86400)
            #print( 'cz', cz, 'l', l)
            l += 1
    cz = float( bakcz)
    #print( 'float( cz) <= float( z)', float( cz) <= float( z), 'float( cz), float( z)', float( cz),  float( z))
    l -= 1
    realbakl = l
    realbakcz = cz
    
    
    if( debug):
        print( 'STAMPTODATE: minyear {}, minzerotime {}'.format( realbakl, realbakcz))
    #sys.exit()
    #####
    # main date determination
    #####
    for k, z in enumerate( zerotime):
        #l = 1
        #cz = int64( 0)
        l = realbakl
        cz = realbakcz
        if( float( z) >= dayseconds):
            if( debug):
                print( 'float( z) >= dayseconds', float( z) >= dayseconds)
            while( float( cz) <= float( z) - dayseconds):
                #print( 'float( cz) <= float( z)', float( cz) <= float( z), 'float( cz), float( z)', float( cz),  float( z))
                bakcz = cz
                if( ( ( mod( l, 4) == 0) or ( mod( l, 400) == 0)) and ( mod( l, 100) != 0)):
                    cz = cz + int64( 366* 86400)
                else:
                    cz = cz + int64( 365* 86400)
                #print( 'cz', cz, 'l', l)
                l += 1
        else:
            if( debug):
                print( 'float( z) >= dayseconds', float( z) >= dayseconds)
            while( ( float( cz) <= dayseconds)):
                #print( 'float( cz) <= dayseconds', float( cz) <= dayseconds, 'float( cz), dayseconds', float( cz),  dayseconds)
                bakcz = cz
                if( ( ( mod( l, 4) == 0) or ( mod( l, 400) == 0)) and ( mod( l, 100) != 0)):
                    cz = cz + int64( 366* 86400)
                else:
                    cz = cz + int64( 365* 86400)
                #print( 'cz', cz, 'l', l)
                l += 1
        cz = float( bakcz)
        #print( 'float( cz) <= float( z)', float( cz) <= float( z), 'float( cz), float( z)', float( cz),  float( z))
        l -= 1
        #print( 'z', z, 'cz', cz, 'l', l)
        #sys.exit()
        #year.append( int32( floor( z/ float( 86400)/ float( 365.2425))))
        if( l < 1970):
            l = 1970
        year.append( l)
        #print( 'year[-1], l', year[-1], l)
        #sys.exit()
        #fpastdays = z/ float( 86400) - float( year[-1])* float( 365.2425)
        #fpastdays = ( z - float( year[-1])* float( 365.2425)* 86400.0)/ float( 86400)
        fpastdays = ( z - cz)#/ float( 86400)
        #print( 'fpastdays', fpastdays, 'year', year[-1])
        #sys.exit()
        #print 'fpastdays = ',fpastdays
        pastdays = int32( floor( fpastdays/ dayseconds))
        bakpastdays = pastdays
        #bakfpastdays = fpastdays
        n = 1
        #print( 'year[-1] is {}'.format( year[-1]))
        #print( 'mod( year[-1], 4) is {}'.format( mod( year[-1], 4)))
        #sys.exit()
        #print( ' pastdays, n, year', pastdays, n, year[-1])
        #checkvar = False
        multi = 0.0
        if( debug):
            print( '\n\nSTAMPTODATE: pastdays', pastdays, 'fpastdays', fpastdays, 'year[-1]', year[-1])
            #sys.exit()
        #if( pastdays <= 1):
        #    sys.exit()
        #if( year[-1] > 2020):
        #    sys.exit()
        leapcond = ( ( mod( year[-1], 4) == 0) or ( mod( year[-1], 400) == 0)) and ( mod( year[-1], 100) != 0)
        #removalpart = 0.0
        """
        m = n
        if( any( m == array31)):
            chkmulti = 31.0
        elif( any( n == array30)):
            chkmulti = 30.0
        elif( m == 2):
            if( leapcond):
                chkmulti = 29.0
                #print( 'STAMPTODATE: leap year')
            else:
                chkmulti = 28.0
        else:
            if( debug):
                print( 'STAMPTODATE: month not correct...stopping')
                sys.exit()
        """
        while( (pastdays >= 1) & (n <= 12)):
            if( any( n == array31)):
                multi = 31.0
            elif( any( n == array30)):
                multi = 30.0
            elif( n == 2):
                if( leapcond):
                    multi = 29.0
                    #print( 'STAMPTODATE: leap year')
                else:
                    multi = 28.0
            else:
                if( debug):
                    print( 'STAMPTODATE: month not correct...stopping')
                    sys.exit()
            #if( n == 12):
            #    print( 'Should only happen in AUGUST, OKTOBER, DECEMBER...')
            #    sys.exit()
            pastdays = pastdays - int32( multi)
            #removalpart = removalpart + multi
            n += 1
            #if( debug):
            #    print( 'STAMPTODATE: remaining days {} month {} multi is {}'.format( pastdays, n, multi))
        
        
        #if( bakpastdays >= 1):
        #if( pastdays < 1):
        pastdays = pastdays + int32( multi)
        #if( n > 12):
        n -= 1
        if( n == 0): # month zero is not possible
            n = n + 1
        if( debug):
            print( 'STAMPTODATE: remaining days {} month {} multi is {}'.format( pastdays, n, multi))
        #removalpart = removalpart - multi
        #print 'pastdays', pastdays
        #if( pastdays >= 1):
        #print( 'pastdays', pastdays, 'year[-1]', year[-1])
        #print( 'remaining days {} month {} multi is {}'.format( pastdays, n, multi))
        
        #if( ( multi == 0.0) | ( year[-1] > 2020)):
        #    sys.exit()
        mon.append( int32( n))
        #if( pastdays <= 0):
        #    dd.append( int32( floor( pastdays)))
        #else:
        #    dd.append( int32( floor( pastdays + 1)))
        #dd.append( int32( floor( pastdays + 1)))
        daytoAdd = int32( floor( pastdays))
        if( daytoAdd == 0):
            daytoAdd = daytoAdd + 1
        dd.append( daytoAdd)
        if( debug):
            print( 'STAMPTODATE: remaining days {} month {} multi is {}'.format( pastdays, dd[-1], multi))
        #if( k > 10):
        #    sys.exit()
        #dd.append( int32( floor( bakpastdays - removalpart)))
        #print 'dd is:', dd[-1]
        #hh.append( int32( floor( 24.0*( fpastdays - float( dd[-1])))))
        #print( 'fpastdays', fpastdays, 'float( pastdays)* dayseconds', float( pastdays)* dayseconds)
        temp = float( 24.0)* ( NoMinFloor( ( ( fpastdays - float( pastdays)* dayseconds)/ dayseconds)))
        #print( 'temp hh', temp)
        hh.append( int32( floor( temp)))#float( dd[-1])))))
        #print 'hh is:', hh[-1]
        #print 'temp is:', temp
        #mm.append( int32( floor( 1440.0*( pastdays - float( dd[-1])) - 60.0* ( float( hh[-1])))))
        temp = float( 60.0)* NoMinFloor(temp)
        #print( 'temp mm', temp)
        mm.append( int32( floor( temp)))
        #print 'mm is:', mm[-1]
        #print 'temp is:', temp
        #ss.append( float( 86400.0* ( pastdays - float( dd[-1])) - 3600.0* float( hh[-1])- 60.0* float( mm[-1])))
        temp = float( 60.0)* NoMinFloor(temp)
        #print( 'temp ss', temp)
        ss.append( float( temp))
        if( debug):
            print( 'STAMPTODATE: remaining days {} year {} month {} day {} hh {} mm {} ss {} multi {}'.format( pastdays, year[-1], n, dd[-1], hh[-1], mm[-1], ss[-1], multi))
            #if( k > 20948):
            #if( hh[-1] > 0 and mm[-1] > 56):
            #sys.exit()
        #print 'ss is:', ss[-1]
        #temp = vstack((array(year), array( mon), array( dd), array( hh), array( mm), array( ss)))
        #temp = list( chain(year + mon + dd + hh + mm + ss))
        ####################
        # WORKING
        ####################
        """
        out.append( year[-1])# = list( chain(year + mon + dd + hh + mm + ss))
        out.append( mon[-1])
        out.append( dd[-1])
        out.append( hh[-1])
        out.append( mm[-1])
        out.append( ss[-1])
        """
        
        #out.append( tuple( (year[-1],  mon[-1], dd[-1], hh[-1], mm[-1], ss[-1])))# = list( chain(year + mon + dd + hh + mm + ss))
        #myyear = year[-1]
        #mymonth = mon[-1]
        #myday = dd[-1]
        #myhour = hh[-1]
        #myminute = mm[-1]
        #mysecond = int32( ss[-1])
        #mymicrosecond = int32( 10.0**6.0* ( NoMinFloor( ss[-1])))
        if( debug):
            #print( 'float( ss[-1])', float( ss[-1]))
            #print( 'len( ss)', len( ss))
            #print( 'float( ss[-2])', float( ss[-2]))
            if( len( ss) > 1):
                print( 'fpastdays {}, bakfpastdays, {}, dt {} seconds'.format( fpastdays, bakfpastdays, ( fpastdays - bakfpastdays)))
                print( 'z {}, {}'.format( z, bakz))
                print( 'myyear {}, {}'.format( year[-1], myyear))
                print( 'mymonth {}, {}'.format( mon[-1], mymonth))
                print( 'myday {}, {}'.format( dd[-1], myday))
                print( 'myhour {}, {}'.format( hh[-1], myhour))
                print( 'myminute {}, {}'.format( mm[-1], myminute))
                print( 'mysecond {}, {}'.format( int32( ss[-1]), int32( mysecond)))
                print( 'mymicrosecond {}, {}'.format( int32( 10.0**6.0* ( NoMinFloor( ss[-1]))), int32( mymicrosecond)))
                print( 'k', k)
                #if( abs( float( fpastdays) - float( bakfpastdays)) > 1.0):
                #if( k > 20948):
                #if( year[-1] > 2020):
                #    print( 'Something went wrong...stopping!')
                #sys.exit()
        bakfpastdays = fpastdays
        bakz = z
        myyear = year[-1]
        mymonth = mon[-1]
        myday = dd[-1]
        myhour = hh[-1]
        myminute = mm[-1]
        mysecond = int32( ss[-1])
        mymicrosecond = int32( ( 10.0**6.0)* ( NoMinFloor( ss[-1])))
            #sys.exit()
            #if( mymonth == 0):
            #    print( 'mymonth is still zero ... debug better! STOPPING')
            #    sys.exit()
        
        out.append( datetime( year = myyear, month = mymonth, day = myday, hour = myhour, minute = myminute, second = mysecond, microsecond = mymicrosecond, tzinfo = utc))
        #print temp
    return out#array( temp).reshape( -1, len(zerotime))






def STAMPTODATE(zerotime):
    """
    # year ... integer 4 digits
    # mon  ... integer 2 digits
    # dd   ... integer 2 digits
    # hh   ... integer 2 digits
    # mm   ... integer 2 digits
    # ss   ... float float
    """
    dump = []
    if( isinstance( zerotime, float)):
        dump.append( zerotime)
    else:
        #print 'YES'
        dump = sort( array( zerotime))
    #print 'dump is: ',dump
    if( debug):
        print( 'np.shape( dump)', shape( dump))
        #sys.exit()
    if( not debug):
        print( 'STAMPTODATE: starting ...')
        p = Pool(agents)
        results = p.map(worker, (dump,))
        results = results[0]
        p.close()
        p.join()
        print( 'STAMPTODATE: ...done')
    else:
        print( 'STAMPTODATE: starting worker process in debugging mode')
        results = worker( dump)
        print( 'STAMPTODATE: starting worker process in debugging mode ...finished')
        sys.exit()
    if( debug):
        print( 'results', results)
        for k, date in enumerate( sort( results)):
            print( k, date)
            #if( date.year > 2020):
            #    print( 'np.shape( results)', shape( results))
            #    sys.exit()
        print( 'np.shape( results)', shape( results))
        
    mindate = results[0]
    maxdate = results[-1]
    if( mindate > maxdate):
        for date in results:
            print( date)
        print( 'mindate {} maxdate {}'.format( mindate, maxdate))
        print( 'mindate later than maxdate...stopping!')
        results = []
        #sys.exit()
    return results