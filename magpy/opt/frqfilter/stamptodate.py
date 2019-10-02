from multiprocessing import Pool
from psutil import cpu_count
from numpy import array, int32, floor, mod, shape, vstack
from itertools import chain
from datetime import datetime
agents = cpu_count(logical = False) - 1



def NoMinFloor( x):
    remain = float( x) - float( floor( x))
    return remain


def worker((zerotime)):
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
    for z in zerotime:
        year.append( int32( floor( z/ float( 86400)/ float( 365.2425))))
        fpastdays = z/ float( 86400) - float( year[-1])* float( 365.2425)
        #print 'fpastdays = ',fpastdays
        pastdays = int32 ( fpastdays)
        n = 1
        while pastdays >= 0:
            if mod( n, 2) != 0 and n <= 7:
                multi = 31.0
            elif mod( n, 2) == 0 and n >= 8:
                multi = 31.0
            elif mod( n, 2) == 0 and n <= 7 and n != 2:
                multi = 30.0
            elif mod( n, 2) != 0 and n >= 8:
                multi = 30.0
            elif n == 2 and mod( year[-1], 4) == 0 and mod( year[-1], 100) != 0 and mod( year[-1], 400) == 0:
                multi = 29.0
            elif n == 2 and mod( year[-1], 4) != 0:
                multi = 28.0
            pastdays = pastdays - multi
            n += 1
        pastdays = pastdays + multi
        #print 'pastdays', pastdays
        n -= 1
        mon.append( int32( n))
        dd.append( int32( floor( pastdays)))
        #print 'dd is:', dd[-1]
        #hh.append( int32( floor( 24.0*( fpastdays - float( dd[-1])))))
        temp = 24.0*( NoMinFloor( fpastdays))
        hh.append( int32( floor( temp)))#float( dd[-1])))))
        #print 'hh is:', hh[-1]
        #print 'temp is:', temp
        #mm.append( int32( floor( 1440.0*( pastdays - float( dd[-1])) - 60.0* ( float( hh[-1])))))
        temp = 60.0* NoMinFloor(temp)
        mm.append( int32( floor( temp)))
        #print 'mm is:', mm[-1]
        #print 'temp is:', temp
        #ss.append( float( 86400.0* ( pastdays - float( dd[-1])) - 3600.0* float( hh[-1])- 60.0* float( mm[-1])))
        temp = 60.0* NoMinFloor(temp)
        ss.append( float( ( temp)))
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
        out.append( datetime( year = year[-1], month = mon[-1], day = dd[-1], hour = hh[-1], minute = mm[-1], second = int32( ss[-1]), microsecond = int32( 10.0**6.0* ( NoMinFloor( ss[-1])))))
        #print temp
    return out#array( temp).reshape( -1, len(zerotime))






def STAMPTODATE(zerotime):
############
# year ... integer 4 digits
# mon  ... integer 2 digits
# dd   ... integer 2 digits
# hh   ... integer 2 digits
# mm   ... integer 2 digits
# ss   ... float float
############
    dump = []
    if( isinstance( zerotime, float)):
        dump.append( zerotime)
    else:
        #print 'YES'
        dump = array( zerotime)
    #print 'dump is: ',dump
    p = Pool(agents)
    results = p.map(worker, (dump,))
    results = results[0]
    p.close()
    p.join()
    """
    if( max( shape( results[0])) == 1):
        results[0] = int32(results[0])
        results[1] = int32(results[1])
        results[2] = int32(results[2])
        results[3] = int32(results[3])
        results[4] = int32(results[4])
        results[5] = float(results[5])
    """
    #year = int32(floor(zerotime/float(86400)/float(365.2425)))
    #pastdays = int32(zerotime/float(86400)-float(year)*float(365.2425))
    #n = 1
    #while pastdays >= 0:
    #    if mod(n,2) != 0 and n <= 7:
    #        multi = 31.0
    #    elif mod(n,2) == 0 and n >= 8:
    #        multi = 31.0
    #    elif mod(n,2) == 0 and n <= 7 and n != 2:
    #        multi = 30.0
    #    elif mod(n,2) != 0 and n >= 8:
    #        multi = 30.0
    #    elif n == 2 and mod(year,4) == 0 and mod(year,100) != 0 and mod(year,400) == 0:
    #        multi = 29.0
    #    elif n == 2 and mod(year,4) != 0:
    #        multi = 28.0
    #    pastdays = pastdays - multi
    #    n += 1
    #pastdays = pastdays + multi
    #n -= 1
    #mon = int32(n)
    #dd = int32(floor(pastdays))
    #hh = int32(floor(24.0*(pastdays-float(dd))))
    #mm = int32(floor(1440*(pastdays-float(dd))-60.0*(float(hh))))
    #ss = float(86400.0*(pastdays-float(dd))-3600.0*float(hh)-60.0*float(mm))
    #print shape(results)
    #print 'results is: {}'.format( results)
    #n = 0
    #year =[]
    #mon = []
    #dd = []
    #hh = []
    #mm = []
    #ss = []
    """
    outvec = []
    if( max( shape( results)) > 6):
        print 'results is longer than 6'
        while (n < max( shape( results)) - 5):
            print 'results[n]', results[n]
            year.append( results[n])
            print 'results[n+1]', results[n+1]
            mon.append( results[n+1])
            print 'results[n+2]', results[n+2]
            dd.append( results[n+2])
            print 'results[n+3]', results[n+3]
            hh.append( results[n+3])
            print 'results[n+4]', results[n+4]
            mm.append( results[n+4])
            print 'results[n+5]', results[n+5]
            ss.append( results[n+5])
            #print 'results[n+6]', results[n+6]
            n = n + 6
    else:
        year.append( results[n])
        mon.append( results[n+1])
        dd.append( results[n+2])
        hh.append( results[n+3])
        mm.append( results[n+4])
        ss.append( results[n+5])
    #print year, mon, dd, hh, mm, ss
    for r in results:
        outvec.append(tuple( ))
    #return year, mon, dd, hh, mm, ss
    """
    return results

"""
OLD WORKING VERSION

def STAMPTODATE(zerotime):
############
# year ... integer 4 digits
# mon  ... integer 2 digits
# dd   ... integer 2 digits
# hh   ... integer 2 digits
# mm   ... integer 2 digits
# ss   ... float float
############
	year = int32(floor(zerotime/float(86400)/float(365.2425)))
	pastdays = int32(zerotime/float(86400)-float(year)*float(365.2425))
	n = 1
	while pastdays >= 0:
		if mod(n,2) != 0 and n <= 7:
		       	multi = 31.0
		elif mod(n,2) == 0 and n >= 8:
		      	multi = 31.0
		elif mod(n,2) == 0 and n <= 7 and n != 2:
	     	 	multi = 30.0
		elif mod(n,2) != 0 and n >= 8:
		   	multi = 30.0
		elif n == 2 and mod(year,4) == 0 and mod(year,100) != 0 and mod(year,400) == 0:
		      	multi = 29.0
		elif n == 2 and mod(year,4) != 0:
		      	multi = 28.0
	    	pastdays = pastdays - multi
	    	n += 1
	pastdays = pastdays + multi
	n -= 1
	mon = int32(n)
	dd = int32(floor(pastdays))
	hh = int32(floor(24.0*(pastdays-float(dd))))
	mm = int32(floor(1440*(pastdays-float(dd))-60.0*(float(hh))))
	ss = float(86400.0*(pastdays-float(dd))-3600.0*float(hh)-60.0*float(mm))
	return year,mon,dd,hh,mm,ss
"""