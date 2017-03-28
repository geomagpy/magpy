# MagPy
MagPy (or GeomagPy) is a Python package for analysing and displaying geomagnetic data.

Version Info: (please note: this package is still in development state with frequent modifcations)
    please check the release notes

MagPy (GeomagPy) provides tools for geomagnetic analysis with special focus on typical data processing in observatories. MagPy provides methods for format conversion, plotting routines and mathematical procedures with special geomagnetic analysis routines like basevalue and baseline calculation, database features and routines. Among the supported data formats are: ImagCDF, IAGA-02, WDC, IMF, IAF, BLV, and many more.
Full installation further provides a graphical user interface - xmagpy.   

Typical usage often looks like this:

    #!/usr/bin/env python
    
    from magpy.stream import read
    import magpy.mpplot as mp
    stream = read(path_or_url='filename')
    mp.plot(stream,['x'])

Below you will find a quick guide for the MagPy package. The quickest approach can be accomplished when skipping everything except the tutorials. 

=======
INSTALL
=======


Windows installation - WinPython Package
========================================

1. install NASA CDF support
        - enabling CDF support for formats like ImagCDF: 
             go to http://cdf.gsfc.nasa.gov/
             get and install a recent version of CDF e.g. cdf36_2_1-setup-32.exe
2. install MagPy
        - install MagPy based on a WinPython package): 
             go to http://www.conrad-observatory.at/
             download and execute magpy-install.exe
3. postinstallation information
        - after installation:
             check your programs overview for MagPy:
             below MagPy you will find three submenus
                 * python -> open a python shell ready for MagPy
                 * xmagpy -> open the graphical user interface of MagPy
                 * update -> check for and install MagPy updates

IMPORTANT: NASA CDF and SpacePy only support 32 bit 


Linux/MacOs installations - Anaconda
====================================

1. install anaconda for your operating system
        - https://docs.continuum.io/anaconda/install
          (currently tested on anaconda with python2.7)
2. install NASA CDF support
        - http://cdf.gsfc.nasa.gov/
          dowload and install the latest cdf version for your operating system
          (installation instructions are provided on the webpage give above)          
3. install MagPy and SpacePy (required for CDF support)
        - open the anaconda prompt or change to the anaconda2/bin directory (if not set as default)
        - run './pip install spacepy' 
                 known issues: installation of spacepy eventually requires a fortran compiler
        - run './pip install geomagpy'
               possible issues: MySQL-python problem -> install libmysqlclient-dev on linux
                   (e.g. debian/ubuntu: sudo apt-get install libmysqlclient-dev)
4. postinstallation information
        - please note that anaconda provides a full python environment 
          with many packages not used by MagPy 
        - for a "slim" installation follow the "from scratch" instructions below (for experienced users)
        - for upgrades: run './pip install geomagpy --upgrade'
        Installation provides both shell based magpy and the graphical user interface xmagpy 
                 * type "python" -> opens a python shell ready for MagPy
                 * type "xmagpy" in a shell -> open the graphical user interface of MagPy
                   adding a shortcut for xmagpy:
                       coming soon    

MacOs installations - MacPorts
==============================

coming soon


Platform independent installations - Docker
===========================================

1. Install Docker (toolbox) for your operating system
        - https://docs.docker.com/engine/installation/
2. Get the MagPy Image
        - open a docker shell
        >>> docker pull geomagpy/magpy:latest
        >>> docker run -d --name magpy -p 8000:8000 geomagpy/magpy:latest
3. Open a browser
        - open address http://localhost:8000 (or http://"IP of your VM":8000)
        - run python shell (not conda) 
        - in python shell
        >>> %matplotlib inline
        >>> from magpy.stream import read
        >>> ...


Install from source - Experts only
==================================

Requirements:
  * Python 2.7,3.x (xmagpy will only work with python 2.7)
Recommended: 
  * Python packages:
    * NasaCDF
    * SpacePy ()
    * pexpect (for SSH support)
  * Other useful Software:
    * MySQL (database features)
    * NetCDF4 (support is currently in preparation)
    * Webserver (e.g. Apache2, PHP)

Linux
-----

1. Get python packages and other extensions (for other distros than debian/ubuntu install similar packages):
        sudo apt-get install python-numpy python-scipy python-matplotlib python-nose python-wxgtk2.8 python-wxtools python-dev build-essential python-networkx python-h5py python-f2py gfortran ncurses-dev libhdf5-serial-dev hdf5-tools libnetcdf-dev python-netcdf python-serial python-twisted owfs python-ow python-setuptools git-core mysql-server python-mysqldb libmysqlclient-dev
        sudo pip install ffnet
        sudo pip install pexpect

2. Get CDF and Omni database support:
    a) CDF (Nasa): http://cdf.gsfc.nasa.gov/html/sw_and_docs.html (tested with 3.6.1.0, please check validity of belows make command for any future versions)

        tar -zxvf cdf36_1-dist-all.tar.gz
        cd cdf36*
        make OS=linux ENV=gnu CURSES=yes FORTRAN=no UCOPTIONS=-O2 SHARED=yes all
        sudo make INSTALLDIR=/usr/local/cdf install

    b) SpacePy (Los Alamos): https://sourceforge.net/projects/spacepy/files/spacepy/ (tested with 0.1.6)

        sudo pip install spacepy

3. Install MagPy

    a) Using pip

        sudo pip install GeomagPy
          * specific version:
        sudo pip install GeomagPy==v0.3.9

    b) Using github (latest development versions)

        git clone git://github.com/GeomagPy/MagPy.git
        cd MagPy*
        sudo python setup.py install


Windows
-------
Tested on XP, Win7, Win10
1. Get a current version of Python(x,y) and install it
   - optionally select packages ffnet and netcdf during install - for cdf support
3. Download nasaCDF packages and install (see links above)
4. get python-spacepy package
5. download and unpack GeomagPy-x.x.x.tar.gz
6. open a command window
7. go to the unpacked directory e.g. cd c:\user\Downloads\GeomagPy\
8. execute "setup.py install"




======================
A Quick guide to MagPy
======================

written by R. Leonhardt, R. Bailey (June 2014)

a Getting started
=================

Start python.... 
then import the basic read method form the stream object

    from magpy.stream import read

You should get an output like:

    MagPy version x.x.xxx
    Loaded Matplotlib - Version [1, 1, 1]
    Loading Numpy and SciPy...
    Loading SpacePy package cdf support ...
    trying CDF lib in /usr/local/cdf
    SpacePy: Space Science Tools for Python
    SpacePy is released under license.
    See __licence__ for details, __citation__ for citation information,
    and help() for HTML help.
    ... success
    Loading python's SQL support
    ... success


b Reading and writing data
==========================

MagPy supports the following data formats and thus conversions between them:
WDC: 	World Data Centre format
JSON: 	JavaScript Object Notation
IMF: 	Intermagnet Format
IAF: 	Intermagnet Archive Format
NEIC: 	WGET data from USGS - NEIC
IAGA: 	IAGA 2002 text format
IMAGCDF: 	Intermagnet CDF Format
GFZKP: 	GeoForschungsZentrum KP-Index format
GSM19/GSM90: 	Output formats from GSM magnetometers
POS1: 	POS-1 binary output
BLV: 	Baseline format Intermagnet
IYFV: 	Yearly mean format Intermagnet
and many others...  To get a full list: from magpy.stream import *; print (PYMAG_SUPPORTED_FORMATS))

You will find files `example1.cdf` and `example2.min` in an example directory provided together with mapy. The `cdf` file is stored along with meta information in the NASA's common data format (cdf). Reading this file requires a working installation of Spacepy cdf and a `'success'` information when Loading SpacePy as shown in (a).

If you do not have any geomagnetic data file you can access example data by using the following commands:

        from pkg_resources import resource_filename
        example1 = resource_filename('magpy', 'examples/example1.cdf')
        data = read(example1)


1. Reading:
-----------


        data = read('example.min') 

or

        data = read('/path/to/file/example.min') 

or

        data = read('c:\\path\\to\\file\\example.min')

Pathnames are related to your operating system. In the following we will assume a Linux system. 
Any file is uploaded to the memory and each data column (or header information) is assigned to an internal variable (key). To get a quick overview about the assigned keys you can use the following method:

        print data._get_key_headers() 

After loading some data file we would like to save it as IAGA02 and IMAGCDF output

2. Writing:
-----------

Creating an IAGA-02 format:

        data.write('/path/to/diretory/',format_type='IAGA')

Creating a INTERMAGNET CDF (ImagCDF) format:

        data.write('/path/to/diretory/',format_type='IMAGCDF')

By default, daily files are created and the date is added to the filename inbetween the optional parameters `filenamebegins` and `filenameends`. If `filenameends` is missing, `.txt` is used as default.

3. Other possibilities to read files:
-------------------------------------

All local files ending with .min within a directory:

        data = read('/path/to/file/*.min')

Getting magnetic data directly from the WDC:

        data = read('ftp://thewellknownaddress/single_year/2011/fur2011.wdc')

Getting kp data from the GFZ Potsdam:

        data = read('http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab')

Getting ACE data from NASA:

        data = read('http://')

(please note: data access and usage is subjected to terms and policy of the indvidual data provider. Please make sure to read them before accessing any of these products.)

No format specifications are required for reading. If MagPy can handle the format, it will be automatically recognized.

Getting data of a specific time window:
Local files:

        data = read('/path/to/files/*.min',starttime="2014-01-01", endtime="2014-05-01")

Remote files:

        data = read('ftp://address/fur2013.wdc',starttime="2013-01-01", endtime="2013-02-01")


4. Selecting timerange:
-------------------------------------

You can trim the data stream anytime later to a specified time interval by applying the trim method:

        data = data.trim(starttime="2013-01-01", endtime="2013-02-01")


5. Tutorial
-----------

For the ongoing quick example please use the following steps. This will create daily IAGA02 files within the directory. Please make sure that the directory is empty before writing data to it.

1. Load example data
------------------------

        # Skip the following two lines, if you have your own data 
        from pkg_resources import resource_filename
        example1 = resource_filename('magpy', 'examples/example1.cdf')

        data = read(example1)

2. Store it locally in your favorite directory
------------------------

        data.write('/tmp/',filenamebegins='MyExample_', format_type='IAGA')

Please note that storing data in a different formt might require additional meta information. Checkout section (i) on how to deal with these aspects.


c Getting help on options and usage
===================================

1. Pythons help function
------------------------

Information on individual methods and their options can be obtained as follows:

For basic functions:

        help(read)

For specific methods related to e.g. a stream object "data":

        help(data.fit)

(this reqires the existance of a "data" object, which is obtained e.g. by data = read(...) or data = DataStream() )


2. Tutorial
-----------

        help(data.fit)


d Plotting 
==========

You will find some example plots at the [Conrad Observatory](http://www.conrad-observatory.at).

1. Quick (and not dirty)
------------------------

        import magpy.mpplot as mp
        mp.plot(data)

2. Some options
---------------

Select specific keys:

        mp.plot(data,variables=['x','y','z'])

3. Multiple streams
-------------------

Provide  a list of stream and an array of keys:

        mp.plotStreams([data1,data2],[['x','y','z'],['f']])

4. Tutorial
-----------

Read a second stream

        otherdata = read(WDC)

Plot xyz data from both streams

        mp.plotStreams([data,otherdata]) 


e Flagging data 
===========================

The flagging procedure allows the observer to mark specific data (like spikes, storm onsets, pulsations, disturbances, lightning strikes, etc). Each flag is asociated with a comment and a type number. Flagtype number ranges between 0 and 4. 
        0:  normal data with comment (e.g. Hello World)
        1:  automatic process added mark (e.g. spike)
        2:  observer marked data as valid geomagnetic signature (e.g. storm onset, pulsation)
            - such data cannot be marked invalid by automatic procedures
        3:  observer marked data as invalid (e.g. lightning, magnetic disturbance)
        4:  merging mark (e.g. data inserted from another source/instrument as defined in the comment)   
Flags can be stored along with the data set (requires CDF output) or separatly in a binary archive. These flags can then be applied anytime to the raw data again, acertaining perfect reproducability.


1. Mark spikes
------------

Getting a spiked record:

        datawithspikes = read(example1)

Mark all spikes using defaults options

        flaggeddata = datawithspikes.flag_outlier(timerange=timedelta(minutes=1),threshold=3)

Show flagged data data

        mp.plot(flaggeddata,['f'],annotate=True)


2. Flag range
----------------------------------

Flag a certain time range

        flaglist = flaggeddata.flag_range(keys=['f'], starttime='2012-08-02T04:33:40', endtime='2012-08-02T04:44:10', flagnum=3, text="iron metal near sensor")

Apply flags to data

        flaggeddata = flaggeddata.flag(flaglist)

Show flagged data data

        mp.plot(flaggeddata,['f'],annotate=True)


3. Save flagged data
-----------

        flaggeddata.write('/tmp/',filenamebegins='MyFlaggedExample_', format_type='PYCDF')

    Check it:
        newdata = read("/tmp/MyFlaggedExample_*")
        mp.plot(newdata,annotate=True, plottitle='Reloaded flagged CDF data')


4. Save flags separately
-----------

        fullflaglist = flaggeddata.extractflags()
        saveflags(fullflaglist,"/tmp/MyFlagList.pkl"))

    Check it:
        data = read(example1)
        flaglist = loadflags("/tmp/MyFlagList.pkl")
        data = data.flag(flaglist)
        mp.plot(data,annotate=True, plottitle='Raw data with flags from file')

5. Drop flagged data
-----------

For some further analyses it is necessary to drop data marked invalid. By default the following method removes all data marked with flagtype numbers 1 and 3.

        cleandata = flaggeddata.remove_flagged()
        mp.plot(cleandata, ['f'], plottitle='Flagged data dropped')


f Basic methods 
===========================

1. Filtering
------------

Filtering uses by default IAGA/INTERMAGNET recommended settings. Ckeck help(data.filter) for options and possible definitions of filter types and pass bands.

Get sampling rate before filtering in seconds: 

        print ("Sampling rate before [sec]:", cleandata.samplingrate())

Filter the data set with default parameters (automatically chooses the correct settings depending on provided sanmpling rate):

        filtereddata = cleandata.filter()

Get sampling rate and filter data after filtering (please note that all filterinformation is added to the data's meta information dictionary (data.header): 

        print ("Sampling rate after [sec]:", filtereddata.samplingrate())
        print ("Filter and pass band:", filtereddata.header.get('DataSamplingFilter',''))

2. Coordinate transform
------------

Assuming vector data in columns x,y,z you can freely convert between xyz, hdz, idf:

        cleandata = cleandata.xyz2hdz()

3. Calculate delta F
------------

If the data file contains x,y,z (hdz, idf) data and an independently measured f value you can calculate delta F:

        cleandata = cleandata.delta_f()
        mp.plot(cleandata,plottitle='Data with delta F')

4. Calculate Means
------------

Mean values for certain data columns can be obtained using the mean method. Missing data is considered using the percentage option (default 95). If more data is missing as denoted by this value, then no mean is calulated (result NaN).

        print (cleandata.mean('df', percentage=80))

5. Applying offsets
------------

Constant offsets can be added to individual columns using the offset method.

        offsetdata = cleandata.offset({'time':timedelta(seconds=0.19),'f':1.24})

6. Scaling data
------------

Individual columns can also be mulitplied by provided values.

        multdata = cleandata.multiply({'x':1.1})

7. Fit functions
------------

MagPy offers the possibility to fit data using either polynomial functions or cubic splines (default). 

        func = cleandata.fit(keys=['x','y','z'],knotstep=0.1)
        mp.plot(cleandata,variables=['x','y','z'],function=func)

8. Derivatives
------------

Derivaties, which are useful to identify outliers and sharp changes, are calculated as follows:

        diffdata = cleandata.differentiate(keys=['x','y','z'],put2keys = ['dx','dy','dz'])
        mp.plot(diffdata,variables=['dx','dy','dz'])


9. All methods at a glance
------------------------------
        
A summary of all supported methods is provided in section x. 



g Geomagnetic analysis
======================

1. Determination of K indicies
------------------------------

MagPy supports the FMI method for determination of K indicies. Please read the MagPy publication for details on this method and its application. A month of one minute data is provided in example2, which corresponds to an INTERMAGNET IAF archive file. Reading such a file will load one minute data by default. Accessing hourly data and other information is described below.

        data2 = read(example2)
        kvals = data2.k_fmi()

Detemination of K values will nees a while as the filtering window is dyanmically adjusted within this method. In order to plot original data (H component) and K values together we now use the multiple stream plotting method plotStreams. Here you need to provide at least a list of streams and an array containing variables for each stream. The additional options determine the look (limits, bar chart, symbols). 

        mp.plotStreams([data2,kvals],[['x'],['var1']],specialdict = [{},{'var1':[0,9]}],symbollist=['-','z'],bartrange=0.06)


2. Geomagnetic storm detection
------------------------------

Geomagnetic storm detection is supported by MagPy using two procedures based on wavelets and the Akaike-information criterion as outlined in detail by Bailey and Leonhardt (2016). 

3. Validity check of data
------------------------------

A common application of such software can be a general validity check of geomagnetic data too be submitted to IAGA, WDC, or INTERMAGNET.



h Multiple streams 
==================

1. Merging streams
----------------------------

Merging data comprises combinations of two stream into one new stream. This includes adding a new column from another stream, filling gaps with data from another stream or replacing data from one column with data from another stream. The following example scetches the typical usage:

        print ("     Used columns in data2:", data2._get_key_headers())
        newstream = mergeStreams(data2,kvals,keys=['var1'])
        print ("     Columns now:", data2._get_key_headers())

If column "var1" s not existing in data2, then this column is added. If column var1 would exist, then missing data would be inserted from stream kvals. In order to replace any existing data use option "mode='replace'".

2. Differences
----------------------------

Sometimes it is necessary to examine differences between two data streams e.g. differences between the F values of two instruments running in parallel at the observatory. For this analyses teh method "subtractStreams" is provided.

        diff = subtractStreams(data1,data2,keys=['f'])


i The art of meta information
=============================

Each data set is accompanied by a dictionary containing meta information for this data. This dictionary is completely dynamic and can be filled freely. Yet there are a number of predefined fields, which should help the user to provide essential meta information as requested by IAGA, INTERMAGNET
and other data providers. All provided meta information is saved only to MagPy own archive format's 'PYCDF' and 'PYSTR'. All other export formats save only specific information as required the projected format.

The current content of this dictionary can be accessed by:

        print (data.header)

Information is added to the dictionary by:

        data.header['SensorName'] = 'FGE'

Information is obtained from the dictionary by:

        print (data.header.get('SensorName'))


1. Conversions to ImagCDF - adding meta
--------------------------------

Example: IAF to ImagCDF

        data2.write('/tmp',format_type='IMAGCDF')



Similar informations are obtained for all other conversions:

        data2.write('/tmp',format_type='IAGA')
        data2.write('/tmp',format_type='IMF')
        data2.write('/tmp',format_type='IAF')
        data2.write('/tmp',format_type='WDC')


2. Providing location data 
--------------------------------


3. Special meta information fields 
----------------------------------

Basevalue, baseline parameters, sensor information and serial numbers 





j Data transfer 
=====================

MagPy contains a number of methods to simplify data transfer for observatory applications. Beside you can always use the basic python functionality. Using the implemented methods requires:

        import magpy.transfer as mt

1. Ftp upload
-------------

2. Secure communication
-----------------------

3. Upload data to GIN
-----------------------


k DI measurements, basevalues and baselines 
===========================================

These procedures require an additional object

    from magpy.absolutes import *

1. Data structure of DI measurements
------------------------------------

2. Reading DI data
------------------


    absresult = absoluteAnalysis('/path/to/DI/','path/to/vario/','path/to/scalar/', alpha=alpha, deltaF=2.1)

What happens here? 

Firstly, for each DI data set in path/to/DI the appropriate vario and scalar sequences are loaded.

The resulting `absresult` is a stream object containing the DI analysis, the collimation angles, scale factors and the base values for the selected variometer, beside some additional meta information provided in the data input form.

3. Baselines
------------

4. Tutorial
-----------

Prerequisites:
1. Use the Conrad Obs Input sheet on the webpage to input your data (contact R.Leonhardt for help/access on that)
2. Make sure that you have variometer data available e.g. `/MyData/Vario/myvariodataset.min`
3. Make sure that you have scalar data available e.g. `/MyData/Scalar/myscalardataset.min`
4. Create an additional directory `/MyData/DI-Analysis/`
5. Create an additional directory `/MyData/DI-Archive/`

Getting started (we need the following packages):

    from magpy.stream import *
    from magpy.absolutes import *
    from magpy.transfer import *

# Get DI data from the Cobs-Server
# ################################

    ftptransfer()

# Analyzing the DI measurement
# ################################

    diresult = analyzeAbsolute()

# Save the result
# ################################

    diresult.write()

l Database support
==================

These procedures require an additional object

    from magpy.database import *

1. Setting up a MagPy database (using MySQL)
--------------------------------------------

2. Adding data to the database
------------------------------


    db = db.open()
    writeDB(db,data)

3. Reading data
------------------


    data = readDB(datainfo)

4. Meta data
--------------------

5. Tutorial
-----------


m Acquisition support
=====================

n Graphical user interface
==========================


x List of all MagPy methods
===========================

