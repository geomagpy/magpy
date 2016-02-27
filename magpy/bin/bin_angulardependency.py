#!/usr/bin/env python
"""
MagPy - autodif analysis

uses continuous autodif measurements to calculate the optimal angular
orientation of the used variometer

makes use of the fact that all measurements should vary 
randomly and do not reflect any daily pattern

TODO:
- create plots of good and bad angular compensation - add the mean and standard dev lines
- use the h component !!
Check different days and component (and azimuth influence)
date		az		x	y	z
22.10.2013 	267.4242	3.62	2.51	-- (does not work)
23.10.2013 	267.4242	4.26	1.4	-- (does not work)
24.10.2013	267.4242	3.68	2.36	--
25.10.2013	267.4242	4.16	2.37	--
"""


import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

from magpy.stream import *   
from magpy.absolutes import *
from magpy.transfer import *
from magpy.database import *

# Generell paths and addresses
fgepath = os.path.join('/home','leon','Dropbox','Daten','Magnetism','FGE-WIC','IAGA')
pospath = '/home/leon/Dropbox/Daten/Magnetism/autodif'
lemipath = '/home/leon/Dropbox/Daten/Magnetism/autodif'


# Define Date and boundary conditions:
# select vario below
startdate = "2013-10-25"
varioorientation_alpha = 3.35
varioorientation_beta = 0.0
deltaF = 0.0
absinst = 'autodif' # can be autodif or manual 
#absinst = 'manual' # can be autodif or manual 
#comps = ['x','z']
#comps = ['x','y','z']
comps = ['z']
#comps = ['x']

alpha_range = [val-10 for val in range(21)]
steps = 3
days = 1
dates,xall,yall,zall = [],[],[],[]
for dayit in range(days):
    x,y,z,h = [],[],[],[]
    a = []
    # derived parameter
    date = datetime.strftime(datetime.strptime(startdate,"%Y-%m-%d")+timedelta(days=dayit),"%Y-%m-%d")
    nolinedate = datetime.strftime(datetime.strptime(date,"%Y-%m-%d"),"%Y%m%d")
    dayofyear = datetime.strftime(datetime.strptime(date,"%Y-%m-%d"),"%j")
    print "Analyzing date ", date
    print "-------------------------------------------"

    #variopath = fgepath
    variopath = lemipath
    scalarpath = fgepath
    resultstream = DataStream()
    if variopath == fgepath:
        variopath = os.path.join(variopath, 'gdas-aw_' + date + '.txt')
    elif variopath == lemipath:
        variopath = os.path.join(variopath, nolinedate + 'v.sec')
    if scalarpath == fgepath:
        scalarpath = os.path.join(scalarpath, 'gdas-aw_' + date + '.txt')
    elif scalarpath == pospath:
        scalarpath = os.path.join(scalarpath, date[:4] + '.' + dayofyear)
    if absinst == 'autodif':
        abspath = '/home/leon/Dropbox/Daten/Magnetism/autodif/'+nolinedate+'.txt'
        azimuth = 267.4242 # A16 to refelctor
    else:
        abspath = '/home/leon/Dropbox/Daten/Magnetism/DI-WIC/raw/' + date + '_07-17-00_A2_WIC.txt'
        #abspath = '/home/leon/Dropbox/Daten/Magnetism/DI-WIC/raw/' + date + '*'
        azimuth = 180.108 # A2 to main
    scalarstr = read(scalarpath)
    scfunc = scalarstr.interpol(['f'])
    for comp in comps:
        for j in range(steps):
            for i in alpha_range:
                count = 0
                absst = absRead(path_or_url=abspath,azimuth=azimuth,output='DIListStruct')
                resultstream = DataStream()
                if j == 0:
                    angle = i
                elif j == 1:
                    angle = newcenter + float(i)/10
                elif j == 2:
                    angle = newcenter + float(i)/100
                print "Analyzing alpha = ", angle
                print "------------------------------"
                variostr = read(variopath)
                if comp == 'x' or comp == 'y':
                    variostr =variostr.rotation(alpha=angle, beta=varioorientation_beta)
                if comp == 'z':
                    variostr =variostr.rotation(alpha=varioorientation_alpha, beta=angle)
                vafunc = variostr.interpol(['x','y','z'])
                for elem in absst:
                    count = count+1
                    stream = elem.getAbsDIStruct()
                    stream = stream._insert_function_values(vafunc)
                    stream = stream._insert_function_values(scfunc,funckeys=['f'],offset=deltaF)
                    result = stream.calcabsolutes(usestep=2,annualmeans=[20000,1200,43000],printresults=False)
                    resultstream.add(result)
                meanbasex = resultstream.mean('dx',meanfunction='median')
                meanbasey = resultstream.mean('dy',meanfunction='median')
                meanbasez = resultstream.mean('dz',meanfunction='median')
                xysum = np.sqrt(meanbasex*meanbasex + meanbasey*meanbasey)
                print "Median and Standard deviation: ", meanbasex, np.std([elem.dx for elem in resultstream])
                print "Median and Standard deviation: ", meanbasey, np.std([elem.dy for elem in resultstream])
                print "Median and Standard deviation: ", meanbasez, np.std([elem.dz for elem in resultstream])
                print "Median and Standard horizontal: ", np.sqrt(meanbasex*meanbasex + meanbasey*meanbasey), np.sqrt(meanbasex/xysum*np.std([elem.dx for elem in resultstream])+meanbasey/xysum*np.std([elem.dy for elem in resultstream]))
                a.append(angle)
                x.append(np.std([elem.dx for elem in resultstream]))
                y.append(np.std([elem.dy for elem in resultstream]))
                # use the correct h error (sqrt(x2+y2)dx  = sqrt((1/(2 * sqrt(x2+y2)) * 2x ))2 + (1/(2 * sqrt(x2+y2)) * 2y)2 )
                h.append(np.sqrt(meanbasex/xysum*np.std([elem.dx for elem in resultstream])+meanbasey/xysum*np.std([elem.dy for elem in resultstream])))
                z.append(np.std([elem.dz for elem in resultstream]))
                #if angle == -10 or angle == -8 or angle == 0: 
                resultstream.plot(['dx','dy','dz'], plottitle='Variometer rotated by %s degree' % str(angle),confinex=True, noshow=True)
            if comp == 'x' or comp == 'y':
                newcenter = a[h.index(min(h))]
            else:
                exec('newcenter = a['+comp+'.index(min('+comp+'))]')

        print "Result (best fitting angle): ", newcenter
        exec(comp+'all.append(newcenter)')
        #resultstream.plot(['x','y','z'], noshow=True)
        if comp == 'x' or comp == 'y':
            plt.plot(a,h,'o')
        else:
            plt.plot(a,eval(comp),'o')
        a = []
        plt.show()
    dates.append(date)

print dates, xall, yall

#plt.plot(a,h,'o')

#plt.plot_date(dates,xall,'o')
plt.show()
