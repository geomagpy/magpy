#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 12:11:37 2019

@author: niko
"""

import os as os
import sys
from mr import MR
from frqfilter import FrqFilter
import datetime as datet
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors
from fnmatch import fnmatch

colors = ['red', 'green', 'blue', 'cyan', 'magenta', 'black', 'olive', 'orange', 'purple']
setalpha = 0.4



# VarioRead
###################
#starttime = datet.datetime( year = 2018, month = 12, day = 16)
#endtime = starttime + datet.timedelta( minutes = 30)

# GradRead
###################
#starttime = datet.datetime( year = 2019, month = 6, day = 7)
#endtime = starttime + datet.timedelta( minutes = 30)

# MiniSeed CONNECTS
###################
#starttime = datet.datetime( year = 2016, month = 7, day = 13, hour = 9, minute = 4, second = 00)
#endtime = starttime + datet.timedelta( seconds = 60)

# DATABASE CONNECTS
###################
endtime = datet.datetime.utcnow()
starttime = endtime - datet.timedelta( hours = 10)



sensortype = 'dBGrad'
#pathstring = '/home/niko/Dokumente/daten/zamg/vergleichsmessung_hohe_warte_edl_picolog_silber_Kupfer_elektroden/EDL'
pathstring = '/home/niko/Software/sources/pythonsources/sources'
#DataCont = MR(starttime = starttime, endtime = endtime, sensortype = sensortype, path = os.path.abspath( pathstring), channels = ['pri0', 'pri1'], SrcStr = str( 'e604616'), TrF = True, TrFVal = 0.00000000001) # EDL READ
#DataCont = MR(starttime = starttime, endtime = endtime, sensortype = sensortype, path = os.path.abspath( pathstring), channels = 'bin') # VarioRead
DataCont = MR(starttime = starttime, endtime = endtime, sensortype = sensortype, hostip = '138.22.188.195', usr = 'cobs', pw = '8ung2rad', db = 'cobsdb')
#files = DataCont.__LookForFilesMatching__()

#for k, el in enumerate( sorted( files)):
#    print( 'file#{}\tis:\t{}'.format( k, el))
zerotime, data, sensPos, mydate = DataCont.GetData()
#print( 'mydate is: {}'.format( mydate))
#print( 'shape of mydate is: {}'.format( np.shape( mydate)))
#sys.exit()
myaxis = 1
mindim, maxdim = np.nanmin( np.shape( data)), np.nanmax( np.shape( data))
mylvl = 5.0*np.nanstd( np.diff( np.diff( data, axis = myaxis), axis = myaxis), axis = myaxis)
print( 'mylvl is: {}'.format( mylvl))
print( 'shape of mylvl is: {}'.format( np.shape( mylvl)))
#sys.exit()
nmylvl = np.zeros( np.shape( mylvl), dtype = float)
for k, el in enumerate( mylvl):
    if( np.isnan( el) or np.isinf( el)):
        nmylvl[k] = np.nanmean( mylvl)
    else:
        nmylvl[k] = mylvl[k]
mylvl = nmylvl
del nmylvl

print( '\nGot data:')
print( '\ndata array dimension:{}'.format( np.shape( data)))
print( '\ndata mindim:{}\tdata maxdim:{}'.format( mindim, maxdim))
if( sensortype == 'dBVario' or sensortype == 'VarioRead'):
    filterind = [0,1,2]
    sensPos = sensPos[filterind,:]
    sensPos[0,0] = 'X'
    sensPos[1,0] = 'Y'
    sensPos[2,0] = 'Z'
    for k, el in enumerate( sensPos[3::,0]):
        sensPos[k + 3,0] = str('var{}'.format( k + 1))
elif( sensortype == 'dBGrad' or sensortype == 'GradRead'):
    filterind = [0,1,2,6,7,8,12,13,14]
    filterind = np.array( filterind + np.ones( np.shape( filterind))*3, dtype=int)
else:
    filterind = np.arange(0, mindim)
data = data[filterind,:]
print( '\nGot data after index selection:')
print( '\ndata array dimension:{}'.format( np.shape( data)))
print( '\ndata mindim:{}\tdata maxdim:{}'.format( mindim, maxdim))
ndata = []
nsensPos = []
for el, sp in zip( data, sensPos):
    if( not np.nanstd( el) == 0.0):
        ndata.append( el)
        nsensPos.append( sp)
data = np.array( ndata)
sensPos = np.array( nsensPos)
del ndata, nsensPos

mindim, maxdim = np.nanmin( np.shape( data)), np.nanmax( np.shape( data))
labels = sensPos[:,0]

mylvl = mylvl[:len( labels)]
print( '\nGot data after clearing of zero columns:')
print( '\ndata array dimension:{}'.format( np.shape( data)))
print( '\ndata mindim:{}\tdata maxdim:{}'.format( mindim, maxdim))
print( '\ndata contains nan values:{}'.format( np.any( np.isnan( data))))
#sys.stdin.readline()




dt = (zerotime[-1] - zerotime[0])/ float( len( zerotime)  - 1.0)

if( mindim > np.nanmax( np.shape( colors))):
    colors = list( dict(mcolors.BASE_COLORS, **mcolors.XKCD_COLORS))
    gdclrs = []
    for el in colors:
        if( not fnmatch( el, '*white*') and not fnmatch( el, '*yellow*')):
            gdclrs.append( el)
    colors = gdclrs[0:mindim:]
    colors = colors[::-1]

if( False):
    
    """
    for k, el in enumerate( zerotime):
        print( 'zerotime#{}\tis:\t{}'.format( k, el))
    
    for k, el in enumerate( data):
        print( 'data#{}\tis:\t{}'.format( k, el))
    
    for k, el in enumerate( sensPos):
        print( 'sensPos#{}\tis:\t{}'.format( k, el))
    """
    print( 'shape of colors is: {}'.format( np.shape( colors)))
    
    for k, (d, c) in enumerate( zip( data, colors)):
        #plt.plot( zerotime, d - np.nanmean( d), color = c, alpha = setalpha, label = str( k))
        plt.plot( mydate, d - np.nanmean( d), color = c, alpha = setalpha, label = labels[k])
    plt.legend( loc = 'lower left')
    plt.show()

else:
    
    
    data = ( data.T  - np.nanmean( data, axis = myaxis)).T # cause shape in axis direction 0 has to be the longer for FrqFilter to work properly
    FrqCont = FrqFilter( data = data, axis = myaxis, dt = dt, PeriodAxis = False, aug = True, despike = True, rplval = 'linear', despikeperc = 0.05)
    #faxis, fdata, myfilter, filtdata = FrqCont.filter()
    
    faxis, fdata = FrqCont.fdataMag()
    #faxis, fdata = FrqCont.fdataAng()
    #faxis, fdata = FrqCont.fdata()
    posind = FrqCont.posind
    #faxis, fdata = FrqCont.fdata()
    NoiseLvl = []
    print( 'shape of data:\t{}\nshape of fdata:\t{}'.format( np.shape( data), np.shape( fdata)))
    #if( myaxis == 1):
    #    fdata = fdata.T
    for el in fdata:
        try:
            NoiseLvl.append( FrqCont.GetNoiseLevel(data = el[posind], perc = 0.05))
        except:
            NoiseLvl.append( el[-1])
    if( True):
        for k, el in enumerate( NoiseLvl):
            print( 'NoiseLvl[{}]\t=\t{}'.format( k, el))
        #sys.exit()
    for k, (d, c) in enumerate( zip( fdata, colors)):
        #if( k >= 3 and k < 6):
        if( True):
            plt.loglog( faxis[posind], np.abs( d)[posind], color = c, alpha = setalpha, label = labels[k])
            plt.loglog( faxis[posind], np.ones( ( np.max( np.shape( d[posind])), ))*NoiseLvl[k], color = c, alpha = setalpha, label = str('Noise {}'.format( labels[k])))
        else:
            plt.semilogx( faxis[posind], d[posind], color = c, alpha = setalpha, label = labels[k])
            plt.semilogx( faxis[posind], np.ones( ( np.max( np.shape( d[posind])), ))*NoiseLvl[k], color = c, alpha = setalpha, label = str('Noise {}'.format( labels[k])))
    plt.legend( loc = 'lower left')
    plt.grid(True)
    plt.show()
    
if( True):
    
    #data = data.T  - np.nanmean( data, axis = myaxis) # cause shape in axis direction 0 has to be the longer for FrqFilter to work properly
    FrqCont = FrqFilter( data = data, axis = myaxis, dt = dt, rplval = 'linear', lvl = mylvl, despike = True, despikeperc = 0.11)
    despikedData, chgind = FrqCont.DeSpike()
    
    
    for k, (d, od, c, cind) in enumerate( zip( despikedData, data, colors, chgind)):
        #if( k >= 3 and k < 6):
        ind = np.array( cind, dtype = int)
        if( False):
            plt.plot( mydate, d - np.nanmean( d), color = c, alpha = setalpha, label = labels[k])
            #plt.plot( mydate, od - np.nanmean( od), color = c, alpha = np.sqrt( setalpha), label = labels[k])
        else:
            dummy = []
            for n, el in enumerate( mydate):
                if( n in ind):
                    dummy.append(el)
            plt.plot( mydate, d - np.nanmean( d), color = c, alpha = setalpha, label = labels[k])
            plt.plot( dummy, ( od - np.nanmean( od))[ind], linewidth = 0.0, marker = '*',  color = c, alpha = np.sqrt( setalpha), label = labels[k])
    plt.legend( loc = 'lower left')
    plt.grid(True)
    plt.show()
