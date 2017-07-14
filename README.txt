===========
MagPy
===========

Version Info: (please note: this package is still in development state with frequent modifcations)

Please check Release version info on
https://github.com/geomagpy/magpy/releases
for details on changes

MagPy (GeomagPy) provides tools for geomagnetic analysis with special
focus on typical data processing in observatories. MagPy provides
methods for format conversion, plotting routines and mathematical
procedures with special geomagnetic analysis routines like basevalue and
baseline calculation, database features and routines. Among the
supported data formats are: ImagCDF, IAGA-02, WDC, IMF, IAF, BLV, and
many more. Full installation further provides a graphical user interface
- xmagpy.


Typical usage often looks like this:

::

        #!/usr/bin/env python

        from magpy.stream import read
        import magpy.mpplot as mp
        stream = read(path_or_url='filename')
        mp.plot(stream,['x'])

Below you will find a quick guide for the MagPy package. The quickest
approach can be accomplished when skipping everything except the
tutorials.

1. INSTALL
----------

1.1 Windows installation - WinPython Package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1.1.1 install NASA `CDF <https://cdf.gsfc.nasa.gov/>`__ support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  enabling CDF support for formats like ImagCDF: go to
   http://cdf.gsfc.nasa.gov/
-  get and install a recent version of CDF e.g. cdf36\_2\_1-setup-32.exe

1.1.2 install MagPy for Windows
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  get the MagPy Windows installer here:
   http://www.conrad-observatory.at
-  download and execute magpy-0.x.x.exe

1.1.3 postinstallation information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  check your programs overview for MagPy: here you will find three
   sub-menus

   ::

       * python -> open a python shell ready for MagPy
       * xmagpy -> open the graphical user interface of MagPy

IMPORTANT: NASA CDF and SpacePy only support 32 bit

1.2 Linux/MacOs installations - Anaconda
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1.2.1 install `Anaconda <https://www.continuum.io/downloads>`__ for your operating system
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  https://docs.continuum.io/anaconda/install (currently tested on
   anaconda with python2.7)

1.2.2 install NASA CDF support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  http://cdf.gsfc.nasa.gov/ dowload and install the latest cdf version
   for your operating system (installation instructions are provided on
   this webpage)

1.2.3 install MagPy and SpacePy (required for CDF support)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  open the anaconda prompt or change to the anaconda2/bin directory (if
   not set as default)
-  run './pip install spacepy' known issues: installation of spacepy
   eventually requires a fortran compiler
-  run './pip install geomagpy' possible issues: MySQL-python problem ->
   install libmysqlclient-dev on linux (e.g. debian/ubuntu: sudo apt-get
   install libmysqlclient-dev)

1.2.4 postinstallation information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  please note that anaconda provides a full python environment with
   many packages not used by MagPy
-  for a "slim" installation follow the "from scratch" instructions
   below (for experienced users)
-  for upgrades: run './pip install geomagpy --upgrade' Installation
   provides both shell based magpy and the graphical user interface
   xmagpy

   ::

       * type "python" -> opens a python shell ready for MagPy
       * type "xmagpy" in a shell -> open the graphical user interface of MagPy

-  adding a shortcut for xmagpy: coming soon

1.3 MacOs installations - MacPorts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1.3.1 install `MacPorts <https://www.macports.org/>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1.3.2 coming soon
^^^^^^^^^^^^^^^^^

1.4 Platform independent installations - Docker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1.4.1 Install `Docker <https://www.docker.com/>`__ (toolbox) for your operating system
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

     - https://docs.docker.com/engine/installation/

1.4.2 Get the MagPy Image
^^^^^^^^^^^^^^^^^^^^^^^^^

::

     - open a docker shell

            >>> docker pull geomagpy/magpy:latest
            >>> docker run -d --name magpy -p 8000:8000 geomagpy/magpy:latest

1.4.3 Open a browser
^^^^^^^^^^^^^^^^^^^^

::

     - open address http://localhost:8000 (or http://"IP of your VM":8000)
     - NEW: first time access might require a token or passwd

            >>> docker logs magpy

          will show the token 
     - run python shell (not conda) 
     - in python shell

            >>> %matplotlib inline
            >>> from magpy.stream import read
            >>> ...

1.5 Install from source - Experts only
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Requirements: - Python 2.7,3.x (xmagpy will only work with python 2.7)

Recommended: - Python packages: \* NasaCDF \* SpacePy () \* pexpect (for
SSH support)

-  Other useful Software:

   -  MySQL (database features)
   -  NetCDF4 (support is currently in preparation)
   -  Webserver (e.g. Apache2, PHP)

1.5.1 Linux
^^^^^^^^^^^

A) Get python packages and other extensions (for other distros than
   debian/ubuntu install similar packages): sudo apt-get install
   python-numpy python-scipy python-matplotlib python-nose
   python-wxgtk2.8 python-wxtools python-dev build-essential
   python-networkx python-h5py python-f2py gfortran ncurses-dev
   libhdf5-serial-dev hdf5-tools libnetcdf-dev python-netcdf
   python-serial python-twisted owfs python-ow python-setuptools
   git-core mysql-server python-mysqldb libmysqlclient-dev sudo pip
   install ffnet sudo pip install pexpect sudo pip install pyproj

B) Get CDF and Omni database support:

   a) CDF (Nasa): http://cdf.gsfc.nasa.gov/html/sw\_and\_docs.html
      (tested with 3.6.1.0, please check validity of belows make command
      for any future versions)

      tar -zxvf cdf36\_1-dist-all.tar.gz cd cdf36\* make OS=linux
      ENV=gnu CURSES=yes FORTRAN=no UCOPTIONS=-O2 SHARED=yes all sudo
      make INSTALLDIR=/usr/local/cdf install

   b) SpacePy (Los Alamos):
      https://sourceforge.net/projects/spacepy/files/spacepy/ (tested
      with 0.1.6)

      sudo pip install spacepy

C) Install MagPy

   a) Using pip

      sudo pip install GeomagPy

      -  specific version: sudo pip install GeomagPy==v0.3.9

   b) Using github (latest development versions)

      git clone git://github.com/GeomagPy/MagPy.git cd MagPy\* sudo
      python setup.py install

1.5.2 Windows
^^^^^^^^^^^^^

| Tested on XP, Win7, Win10 a) Get a current version of Python(x,y) and
  install it optionally select packages ffnet and netcdf during install
  - for cdf support b) Download nasaCDF packages and install (see links
  above) c) get python-spacepy package d) download and unpack
  GeomagPy-x.x.x.tar.gz e) open a command window f) go to the unpacked
  directory e.g. cd
  c::raw-latex:`\user`:raw-latex:`\Downloads`:raw-latex:`\GeomagPy`
| g) execute "setup.py install"

2. A Quick guide to MagPy
-------------------------

written by R. Leonhardt, R. Bailey (April 2017)

2.1 Getting started
~~~~~~~~~~~~~~~~~~~

Start python.... then import the basic read method form the stream
object

::

    from magpy.stream import read

You should get an output like:

::

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

2.2 Reading and writing data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MagPy supports the following data formats and thus conversions between
them: WDC: World Data Centre format JSON: JavaScript Object Notation
IMF: Intermagnet Format IAF: Intermagnet Archive Format NEIC: WGET data
from USGS - NEIC IAGA: IAGA 2002 text format IMAGCDF: Intermagnet CDF
Format GFZKP: GeoForschungsZentrum KP-Index format GSM19/GSM90: Output
formats from GSM magnetometers POS1: POS-1 binary output BLV: Baseline
format Intermagnet IYFV: Yearly mean format Intermagnet and many
others... To get a full list:

::

        from magpy.stream import *
        print (PYMAG_SUPPORTED_FORMATS)

You will find several example files provided together with mapy. The
``cdf`` file is stored along with meta information in the NASA's common
data format (cdf). Reading this file requires a working installation of
Spacepy cdf and a ``'success'`` information when Loading SpacePy as
shown in (a).

If you do not have any geomagnetic data file you can access example data
by using the following commands:

::

        data = read(example1)

2.2.1 Reading:
^^^^^^^^^^^^^^

::

        data = read(r'myfile.min') 

or

::

        data = read(r'/path/to/file/myfile.min') 

or

::

        data = read(r'c:\path\to\file\myfile.min')

Pathnames are related to your operating system. In the following we will
assume a Linux system. Any file is uploaded to the memory and each data
column (or header information) is assigned to an internal variable
(key). To get a quick overview about the assigned keys you can use the
following method:

::

        print data._get_key_headers() 

After loading some data file we would like to save it as IAGA02 and
IMAGCDF output

2.2.2 Writing:
^^^^^^^^^^^^^^

Creating an IAGA-02 format:

::

        data.write(r'/path/to/diretory/',format_type='IAGA')

Creating a `INTERMAGNET <http://www.intermagnet.org>`__ CDF (ImagCDF)
format:

::

        data.write(r'/path/to/diretory/',format_type='IMAGCDF')

By default, daily files are created and the date is added to the
filename inbetween the optional parameters ``filenamebegins`` and
``filenameends``. If ``filenameends`` is missing, ``.txt`` is used as
default.

2.2.3 Other possibilities to read files:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All local files ending with .min within a directory:

::

        data = read(r'/path/to/file/*.min')

Getting magnetic data directly from the WDC:

::

        data = read(r'ftp://thewellknownaddress/single_year/2011/fur2011.wdc')

Getting kp data from the GFZ Potsdam:

::

        data = read(r'http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab')

(please note: data access and usage is subjected to terms and policy of
the indvidual data provider. Please make sure to read them before
accessing any of these products.)

No format specifications are required for reading. If MagPy can handle
the format, it will be automatically recognized.

Getting data of a specific time window: Local files:

::

        data = read(r'/path/to/files/*.min',starttime="2014-01-01", endtime="2014-05-01")

Remote files:

::

        data = read(r'ftp://address/fur2013.wdc',starttime="2013-01-01", endtime="2013-02-01")

INTERMAGNET Webservice (starting soon):

::

        data = read('http://www.intermagnet.org/test/ws/?id=WIC')

2.2.4 Selecting timerange
^^^^^^^^^^^^^^^^^^^^^^^^^

You can trim the data stream anytime later to a specified time interval
by applying the trim method:

::

        data = data.trim(starttime="2013-01-01", endtime="2013-02-01")

2.2.5 Tutorial
^^^^^^^^^^^^^^

For the ongoing quick example please use the following steps. This will
create daily IAGA02 files within the directory. Please make sure that
the directory is empty before writing data to it.

A) Load example data

Along with magpy, we provide several example data sets: example1:
`INTERMAGNET <http://www.intermagnet.org>`__ CDF (ImagCDF) file with 1
second data example2: `INTERMAGNET <http://www.intermagnet.org>`__
Archive format (IAF) file with 1 min, 1 hour, K and mean data example3:
MagPy readable DI data file with data from 1 single DI measurement
example4: MagPy Basevalue file (PYSTR) with analysis results of several
DI data

::

        # Replace example1 with a full path, if you have your own data 
        data = read(example1)

B) Store it locally in your favorite directory

   ::

       data.write('/tmp/',filenamebegins='MyExample_', format_type='IAGA')

Please note that storing data in a different formt might require
additional meta information. Checkout section (i) on how to deal with
these aspects.

2.3 Getting help on options and usage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

2.3.1 Pythons help function
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Information on individual methods and their options can be obtained as
follows:

For basic functions:

::

        help(read)

For specific methods related to e.g. a stream object "data":

::

        help(data.fit)

(this reqires the existance of a "data" object, which is obtained e.g.
by data = read(...) or data = DataStream() )

2.3.2 Tutorial
^^^^^^^^^^^^^^

::

        help(data.fit)

2.4 Plotting
~~~~~~~~~~~~

You will find some example plots at the `Conrad
Observatory <http://www.conrad-observatory.at>`__.

2.4.1 Quick (and not dirty)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

        from magpy import mpplot as mp
        mp.plot(data)

2.4.2 Some options
^^^^^^^^^^^^^^^^^^

Select specific keys:

::

        mp.plot(data,variables=['x','y','z'])

2.4.3 Multiple streams
^^^^^^^^^^^^^^^^^^^^^^

Provide a list of stream and an array of keys:

::

        mp.plotStreams([data1,data2],[['x','y','z'],['f']])

2.4.4 Tutorial
^^^^^^^^^^^^^^

Read a second stream

::

        otherdata = read(WDC)

Plot xyz data from both streams

::

        mp.plotStreams([data,otherdata]) 

2.5 Flagging data
~~~~~~~~~~~~~~~~~

| The flagging procedure allows the observer to mark specific data (like
  spikes, storm onsets, pulsations, disturbances, lightning strikes,
  etc). Each flag is asociated with a comment and a type number.
  Flagtype number ranges between 0 and 4. 0: normal data with comment
  (e.g. Hello World) 1: automatic process added mark (e.g. spike) 2:
  observer marked data as valid geomagnetic signature (e.g. storm onset,
  pulsation) - such data cannot be marked invalid by automatic
  procedures 3: observer marked data as invalid (e.g. lightning,
  magnetic disturbance) 4: merging mark (e.g. data inserted from another
  source/instrument as defined in the comment)
| Flags can be stored along with the data set (requires CDF output) or
  separatly in a binary archive. These flags can then be applied anytime
  to the raw data again, acertaining perfect reproducability.

2.5.1 Mark spikes
^^^^^^^^^^^^^^^^^

Getting a spiked record:

::

        datawithspikes = read(example1)

Mark all spikes using defaults options

::

        flaggeddata = datawithspikes.flag_outlier(timerange=timedelta(minutes=1),threshold=3)

Show flagged data data

::

        mp.plot(flaggeddata,['f'],annotate=True)

2.5.2 Flag range
^^^^^^^^^^^^^^^^

Flag a certain time range

::

        flaglist = flaggeddata.flag_range(keys=['f'], starttime='2012-08-02T04:33:40', endtime='2012-08-02T04:44:10', flagnum=3, text="iron metal near sensor")

Apply flags to data

::

        flaggeddata = flaggeddata.flag(flaglist)

Show flagged data data

::

        mp.plot(flaggeddata,['f'],annotate=True)

2.5.3 Save flagged data
^^^^^^^^^^^^^^^^^^^^^^^

::

        flaggeddata.write('/tmp/',filenamebegins='MyFlaggedExample_', format_type='PYCDF')

    Check it:
        newdata = read("/tmp/MyFlaggedExample_*")
        mp.plot(newdata,annotate=True, plottitle='Reloaded flagged CDF data')

2.5.4 Save flags separately
^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

        fullflaglist = flaggeddata.extractflags()
        saveflags(fullflaglist,"/tmp/MyFlagList.pkl"))

    Check it:
        data = read(example1)
        flaglist = loadflags("/tmp/MyFlagList.pkl")
        data = data.flag(flaglist)
        mp.plot(data,annotate=True, plottitle='Raw data with flags from file')

2.5.5 Drop flagged data
^^^^^^^^^^^^^^^^^^^^^^^

For some further analyses it is necessary to drop data marked invalid.
By default the following method removes all data marked with flagtype
numbers 1 and 3.

::

        cleandata = flaggeddata.remove_flagged()
        mp.plot(cleandata, ['f'], plottitle='Flagged data dropped')

2.6 Basic methods
~~~~~~~~~~~~~~~~~

2.6.1 Filtering
^^^^^^^^^^^^^^^

Filtering uses by default
`IAGA <http://www.iaga-aiga.org/>`__/`INTERMAGNET <http://www.intermagnet.org>`__
recommended settings. Ckeck help(data.filter) for options and possible
definitions of filter types and pass bands.

Get sampling rate before filtering in seconds:

::

        print ("Sampling rate before [sec]:", cleandata.samplingrate())

Filter the data set with default parameters (automatically chooses the
correct settings depending on provided sanmpling rate):

::

        filtereddata = cleandata.filter()

Get sampling rate and filter data after filtering (please note that all
filterinformation is added to the data's meta information dictionary
(data.header):

::

        print ("Sampling rate after [sec]:", filtereddata.samplingrate())
        print ("Filter and pass band:", filtereddata.header.get('DataSamplingFilter',''))

2.6.2 Coordinate transform
^^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming vector data in columns x,y,z you can freely convert between
xyz, hdz, idf:

::

        cleandata = cleandata.xyz2hdz()

2.6.3 Calculate delta F
^^^^^^^^^^^^^^^^^^^^^^^

If the data file contains x,y,z (hdz, idf) data and an independently
measured f value you can calculate delta F:

::

        cleandata = cleandata.delta_f()
        mp.plot(cleandata,plottitle='Data with delta F')

2.6.4 Calculate Means
^^^^^^^^^^^^^^^^^^^^^

Mean values for certain data columns can be obtained using the mean
method. Missing data is considered using the percentage option (default
95). If more data is missing as denoted by this value, then no mean is
calulated (result NaN).

::

        print (cleandata.mean('df', percentage=80))

2.6.5 Applying offsets
^^^^^^^^^^^^^^^^^^^^^^

Constant offsets can be added to individual columns using the offset
method.

::

        offsetdata = cleandata.offset({'time':timedelta(seconds=0.19),'f':1.24})

2.6.6 Scaling data
^^^^^^^^^^^^^^^^^^

Individual columns can also be mulitplied by provided values.

::

        multdata = cleandata.multiply({'x':1.1})

2.6.7 Fit functions
^^^^^^^^^^^^^^^^^^^

MagPy offers the possibility to fit data using either polynomial
functions or cubic splines (default).

::

        func = cleandata.fit(keys=['x','y','z'],knotstep=0.1)
        mp.plot(cleandata,variables=['x','y','z'],function=func)

2.6.8 Derivatives
^^^^^^^^^^^^^^^^^

Derivaties, which are useful to identify outliers and sharp changes, are
calculated as follows:

::

        diffdata = cleandata.differentiate(keys=['x','y','z'],put2keys = ['dx','dy','dz'])
        mp.plot(diffdata,variables=['dx','dy','dz'])

2.6.9 All methods at a glance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A summary of all supported methods is provided in section x.

2.7 Geomagnetic analysis
~~~~~~~~~~~~~~~~~~~~~~~~

2.7.1 Determination of K indicies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

MagPy supports the FMI method for determination of K indicies. Please
read the MagPy publication for details on this method and its
application. A month of one minute data is provided in example2, which
corresponds to an `INTERMAGNET <http://www.intermagnet.org>`__ IAF
archive file. Reading such a file will load one minute data by default.
Accessing hourly data and other information is described below.

::

        data2 = read(example2)
        kvals = data2.k_fmi()

Detemination of K values will nees a while as the filtering window is
dyanmically adjusted within this method. In order to plot original data
(H component) and K values together we now use the multiple stream
plotting method plotStreams. Here you need to provide at least a list of
streams and an array containing variables for each stream. The
additional options determine the look (limits, bar chart, symbols).

::

        mp.plotStreams([data2,kvals],[['x'],['var1']],specialdict = [{},{'var1':[0,9]}],symbollist=['-','z'],bartrange=0.06)

2.7.2 Geomagnetic storm detection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Geomagnetic storm detection is supported by MagPy using two procedures
based on wavelets and the Akaike-information criterion as outlined in
detail by Bailey and Leonhardt (2016).

2.7.3 Sq analysis
^^^^^^^^^^^^^^^^^

Methods are currently in preparation.

2.7.4 Validity check of data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A common application of such software can be a general validity check of
geomagnetic data too be submitted to
`IAGA <http://www.iaga-aiga.org/>`__, WDC, or
`INTERMAGNET <http://www.intermagnet.org>`__. Please note: this is
currently under development and will be extended in the near future. A
'one-click' test method will be included into xmagpy, checking:

A) Validity of data formats (e.g.):

   ::

       data = read(myiaffile.bin, debug=True) 

B) Completness of meta information

C) Conformity of applied techniques with respective rules

D) Internal consistency of data

E) Optional: Regional consistency

2.7.5 Spectral Analysis and Noise
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For analysis of spectral data, magpy provides two basic plotting
methods. plotPS will caluclate and display a powerspectrum of the
selected component. plotSpectrogram will show a spectrogram of the
timeseries. As usual, there are many options on windows and processing
parameters which can be accessed by the help method.

::

        data = read(example1)
        mp.plotPS(data,key='f')
        help(mp.plotSpectrogram)
        mp.plotSpectrogram(data,['f'])

2.8 Multiple streams
~~~~~~~~~~~~~~~~~~~~

2.8.1 Merging streams
^^^^^^^^^^^^^^^^^^^^^

Merging data comprises combinations of two stream into one new stream.
This includes adding a new column from another stream, filling gaps with
data from another stream or replacing data from one column with data
from another stream. The following example scetches the typical usage:

::

        print ("     Used columns in data2:", data2._get_key_headers())
        newstream = mergeStreams(data2,kvals,keys=['var1'])
        print ("     Columns now:", data2._get_key_headers())

If column "var1" s not existing in data2, then this column is added. If
column var1 would exist, then missing data would be inserted from stream
kvals. In order to replace any existing data use option
"mode='replace'".

2.8.2 Differences
^^^^^^^^^^^^^^^^^

Sometimes it is necessary to examine differences between two data
streams e.g. differences between the F values of two instruments running
in parallel at the observatory. For this analyses teh method
"subtractStreams" is provided.

::

        diff = subtractStreams(data1,data2,keys=['f'])

2.9 The art of meta information
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each data set is accompanied by a dictionary containing meta information
for this data. This dictionary is completely dynamic and can be filled
freely. Yet there are a number of predefined fields, which should help
the user to provide essential meta information as requested by
`IAGA <http://www.iaga-aiga.org/>`__,
`INTERMAGNET <http://www.intermagnet.org>`__ and other data providers.
All provided meta information is saved only to MagPy own archive
format's 'PYCDF' and 'PYSTR'. All other export formats save only
specific information as required the projected format.

The current content of this dictionary can be accessed by:

::

        data = read(example1)
        print (data.header)

Information is added/changed by:

::

        data.header['SensorName'] = 'FGE'

Individual information is obtained from the dictionary by:

::

        print (data.header.get('SensorName'))

If you want to have a more readable list of the header information do:

::

        for key in data.header:
            print ("Key: {} \t Content: {}".format(key,data.header.get(key)))

2.9.1 Conversions to ImagCDF - adding meta
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you convert data from `IAGA <http://www.iaga-aiga.org/>`__ or IAF
formats to the new `INTERMAGNET <http://www.intermagnet.org>`__ CDF
format, you usually need to add additional meta information which is
required for the new data formats. MagPy assists you here, firstly by
extracting and correctly adding already existing meta information
towrads newly defined fields and secondly by informing you which
information needs to be added for producing correct output formats.

Example: IAGA02 to ImagCDF

::

        mydata = read(some IAGA-02 file)
        mydata.write('/tmp',format_type='IMAGCDF')

The console output of the write command (see below) will tell you which
information needs to be added (and how) in order to obtain correct
ImagCDF files. Please note, MagPy will store the data in any case and
will be able to read it again even if information is missing. Before
submitting to a GIN, you need to make sure that the appropriate
information is contained. Attributes that relate to publication of the
data are not checked so far, and might be included into .

::

        >>>Writing IMAGCDF Format /tmp/wic_20150828_0000_PT1M_4.cdf
        >>>writeIMAGCDF: StandardLevel not defined - please specify by yourdata.header['DataStandardLevel'] = ['None','Partial','Full']
        >>>writeIMAGCDF: Found F column
        >>>writeIMAGCDF: given components are XYZF. Checking F column...
        >>>writeIMAGCDF: analyzed F column - values are apparently independend from vector components - using column name 'S'

Now add the missing information. Selecting 'Partial' will require
additional information. You will get a 'reminder' if you forget this.
Please check IMAGCDF instructions for codes.:

::

        mydata.header['DataStandardLevel'] = 'Partial'
        mydata.header['DataPartialStandDesc'] = 'IMOS-01,IMOS-02,IMOS-03,IMOS-04,IMOS-05,IMOS-06,IMOS-11,IMOS-12,IMOS-13,IMOS-14,IMOS-15,IMOS-21,IMOS-22,IMOS-31,IMOS-41'

Similar informations are obtained for other conversions like:

::

        mydata.write('/tmp',format_type='IAGA')
        mydata.write('/tmp',format_type='IMF')
        mydata.write('/tmp',format_type='IAF',coverage='month')
        mydata.write('/tmp',format_type='WDC')

2.9.2 Providing location data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Providing location data usually requires information on the reference
system (ellepsoid,...). By default MagPy assums that these values are
provided in WGS84/WGS84 reference system. In order to facilitate most
easy referencing and conversions, MagPy supports
`epsg <https://www.epsg-registry.org/>`__ codes for coordinates. If you
provide the geodetic references as follows, and provided that the
`proj4 <https://github.com/OSGeo/proj.4>`__ python package is available
then MagPy will automatically convert location data to the requested
output format (currently WGS84).

::

        mydata.header['DataAcquisitionLongitude'] = -34949.9
        mydata.header['DataAcquisitionLatitude'] = 310087.0
        mydata.header['DataLocationReference'] = 'GK M34, EPSG: 31253'

        >>>...
        >>>writeIMAGCDF: converting coordinates to epsg 4326
        >>>...

2.9.3 Special meta information fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The meta information fields can hold much more information as requested
by most output formats. This includes basevalue, baseline parameters,
flagging details, detailed sensor information, serial numbers and many
more. MagPy makes use of these possibilities. In order to save these
information along with your data set you can use MagPy internal
archiving format (PYCDF) of which any of the above mentioned
outputformats can be obtained. You can even reconstruct a full data base
(see section l). Any upcoming meta information or output request can be
easily added/modified without disrupting already existing data sets, and
the possibilities to read/analyse old data. This data format is also
based on Nasa CDF. Ascii outputs are also supported by MagPy of which
the PYSTR format also contains all meta information and PYASCII is least
space consuming. Please consider that such ascii format require a lot of
memory especially for one second and higher resolution data.

::

        mydata.write('/tmp',format_type='PYCDF',coverage='year')

2.10 Data transfer
~~~~~~~~~~~~~~~~~~

MagPy contains a number of methods to simplify data transfer for
observatory applications. Beside you can always use the basic python
functionality. Using the implemented methods requires:

::

        from magpy import transfer as mt

2.10.1 Downloads
^^^^^^^^^^^^^^^^

Just use the read method as outlined in section a. No additional imports
are required.

2.10.2 Ftp upload
^^^^^^^^^^^^^^^^^

The upload methods using ftp, scp and gin support logging. If the data
file failed to upload correctly, the path is added to a log file and,
when called again, upload is retried. This option is useful for remote
locations with unstable network connections.

::

        mt.ftpdatatransfer(localfile='/path/to/data.cdf',ftppath='/remote/directory/',myproxy='ftpaddress or address of proxy',port=21,login='user',passwd='passwd',logfile='/path/mylog.log')

2.10.3 Secure communication
^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

        mt.scptransfer('user@address:/remote/directory/','/path/to/data.cdf',passwd,timeout=60)

2.10.4 Upload data to GIN
^^^^^^^^^^^^^^^^^^^^^^^^^

::

        mt.ginupload('/path/to/data.cdf', ginuser, ginpasswd, ginaddress, faillog=True, stdout=True)

2.10.5 Avoiding real-text passwords in scripts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to avoid using real-text password in scripts, MagPy comes along
with a simple encryption routine.

::

        from magpy.opt import cred as mpcred

Adding encrypted passwd information for data transfer to a maschine
called 'MyRemoteFTP' with an IP of 192.168.0.99:

::

        mpcred.cc('transfer', 'MyRemoteFTP', user='user', passwd='secure', address='192.168.0.99', port=21)

Extracting passwd information within your data transfer scripts:

::

        password=mpcred.lc('MyRemoteFTP','passwd')

2.11 DI measurements, basevalues and baselines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These procedures require an additional object

::

        from magpy import absolutes as di

2.11.1 Data structure of DI measurements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Please check example3 which is an example DI file. You can create these
DI files by using the input sheet from xmagpy or the online input sheet
provided by the Conrad Observatory. If you want to use this service,
please contact the Observatory staff. Also supported are di-files from
AUTODIF.

2.11.2 Reading DI data
^^^^^^^^^^^^^^^^^^^^^^

Reading and analyzing DI data requires valid DI file(s). For correct
analysis, variometer data and scalar informations needs to be provided
as well. Checkout help(di.absoluteAnalysis) for all options. The
analytical procedures are outlined in detail in the MagPy article
(citation). A typical analysis looks like:

::

        diresult = di.absoluteAnalysis('/path/to/DI/','path/to/vario/','path/to/scalar/')

Path to DI can either point to a single file, a directory or even use
wildcards to select data fro a specific observatory/pillar. Using the
examples provided along with MagPy the analysis line looks like

::

        diresult = di.absoluteAnalysis(example3,example2,example2)

Calling this method will provide an output the terminal as follows and a
stream object 'diresult' which can be used further.

::

        >>>...
        >>>Analyzing manual measurement from 2015-03-25
        >>>Vector at: 2015-03-25 08:18:00+00:00
        >>>Declination: 3:53:46, Inclination: 64:17:17, H: 21027.2, Z: 43667.9, F: 48466.7
        >>>Collimation and Offset:
        >>>Declination:    S0: -3.081, delta H: -6.492, epsilon Z: -61.730
        >>>Inclination:    S0: -1.531, epsilon Z: -60.307
        >>>Scalevalue: 1.009 deg/unit
        >>>Fext with delta F of 0.0 nT
        >>>Delta D: 0.0, delta I: 0.0

Fext indicates that F values have been used from a separate file and not
provided along with DI data. Delta values for F, D, and I have not been
provided either. ``diresult`` is a stream object containing average D, I
and F values, the collimation angles, scale factors and the base values
for the selected variometer, beside some additional meta information
provided in the data input form.

2.11.3 Reading BLV files
^^^^^^^^^^^^^^^^^^^^^^^^

Basevalues:

::

        blvdata = read('/path/myfile.blv')
        mp.plot(blvdata, symbollist=['o','o','o'])

Adopted baseline:

::

        bldata = read('/path/myfile.blv',mode='adopted')
        mp.plot(bldata)

2.11.4 Basevalues and baselines
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Basevalues as obtained in (2.11.2) or (2.11.3) are stored in a normal
data stream object and therefore all methods outlined before can be
applied to this data. The ``diresult`` object contains D, I, and F
values for each measurement in columns x,y,z. Basevalues for H, D and Z
related to the selected variometer are stored in columns dx,dy,dz. In
``example4`` you will find some more di analysis results. To plot these
basevalues we can use the following plot command, where we specify the
columns, filled circles as plotsymbols and also define a minimum spread
of each y-axis of +/- 5 nT for H and Z, +/- 0.05 deg for D.

::

        basevalues = read(example4)
        mp.plot(basevalues, variables=['dx','dy','dz'], symbollist=['o','o','o'], padding=[5,0.05,5])

Fitting a baseline can be easily accomplished with the fit method.
Firstly we test a linear fit to the data, by fitting a polynom with
degree 1.

::

        func = basevalues.fit(['dx','dy','dz'],fitfunc='poly', fitdegree=1)
        mp.plot(basevalues, variables=['dx','dy','dz'], symbollist=['o','o','o'], padding=[5,0.05,5], function=func)

The we fit a spline function using 3 knowsteps over the timerange (the
knotstep option always relatively refers to the given timerange 1).

::

        func = basevalues.fit(['dx','dy','dz'],fitfunc='spline', knotstep=0.33)
        mp.plot(basevalues, variables=['dx','dy','dz'], symbollist=['o','o','o'], padding=[5,0.05,5], function=func)

Hint: a good estimate on the necessary fit complexity can be obtained by
looking at delta F values. If delta F is rather constant, then also the
baseline should not be complex either.

2.11.5 Applying baselines
^^^^^^^^^^^^^^^^^^^^^^^^^

The baseline method provides a number of options to assist the observer
in determining baseline corrections and realted issues. The basic
building block of the baseline method is the fit function as discussed
above. Lets first load vectorial geomagnetic raw data for which
basevalues are contained in above example:

::

        rawdata = read(example5)

Now we can apply the basevalue information and the spline function as
tested above:

::

        func = rawdata.baseline(basevalues, extradays=0, fitfunc='spline', knotstep=0.33,startabs='2015-09-01',endabs='2016-01-22')

The ``baseline`` method will determine and return a fit function between
the two given timeranges, based on the provided basevalue data
``blvdata``. The option extradays allows for adding days before and
after start/endtime for which the baselinefunction will be extrapolated.
This option is useful for providing quasidefinitive data. When applying
this method, a number of new meta infomartion attributes will be added,
containing basevalues and all functional parameter to describe the
baseline. Thus, stream object still contains uncorrected raw data, but
all baseline correction information is now contained within its meta
data. To apply baseline correction you can issue the ``bc`` method.

::

        corrdata = rawdata.bc()

If baseline jumps/breaks are necessary, you call the baseline function
for each independend segment and then join the corrected streams:

::

        stream_a = read(mydata,starttime='2016-01-01',endtime='2016-02-01')
        func = stream_a.baseline(blvdata, extradays=0, fitfunc='spline', knotstep=0.3,startabs='2016-01-01',endabs='2016-02-01')
        corr = stream_a.bc()

        stream_b = read(mydata,starttime='2016-02-01',endtime='2016-03-01')
        func_b = stream.baseline(blvdata, extradays=0, fitfunc='poly', degree=1,startabs='2016-02-01',endabs='2016-03-01')
        corr_b = stream_b.bc()

        func.extend(func_b)
        corr.extend(corr_b)

        mp.plot(basevalues, variables=['dx','dy','dz'], symbollist=['o','o','o'], padding=[5,0.05,5], function=func)

The combined baseline can be plotted accordingly. Extend the function
parameters with each additional segment.

2.11.6 Saving basevalue and baseline information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

        diresult.write('/my/path',coverage='all',format_type='BLV',diff=meanstream,year='2016')

will create a BLV file. Important is the ``meanstream`` data stream
which is containing daily averages of delta F values between variometer
and F measurement and the baseline adoption data within the meta
information. You can, however, provide all this information manually as
well. A typical way to obtain such a ``meanstream`` is scetched below:

::

        finaldata = mergeStreams(vectordata_corr, scalardata, ['f'])
        finaldata = finaldata.delta_f()
        meanstream = finaldata.filter(filter_width=timedelta(days=1),filter_type='flat',resampleoffset=timedelta(hours=12), resample_period=43200, missingdata='mean')

2.12 Database support
~~~~~~~~~~~~~~~~~~~~~

MagPy supports data base access and many methods for optimizing data
treatment in connection with data bases. Among many other benefits,
using a database simplifies many typical procedures related to meta
information. Currently MagPy supports `MySQL <https://www.mysql.com/>`__
databases. To use these features you need to install MySQL on your
system. In following we provide a brief outline on how to set up and use
this optional addition. Please note that a proper usage of the database
requires sensor specific information. Unlike the often used way in
geomagnetism to combine data from different sensors into one file
structure, such data needs to remain separate for database usage and is
only combined when producing
`IAGA <http://www.iaga-aiga.org/>`__/`INTERMAGNET <http://www.intermagnet.org>`__
outputs. Furthermore, unique sensor information is requires like its
type and serial number.

::

        import magpy import database as mdb

2.12.1 Setting up a MagPy database (using MySQL)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Open mysql (e.g. linux: mysql -u root -p mysql) and create a new
database. Replace ``#DB-NAME`` with your database name (e.g. MyDB).
After creation you also need to grant priviledges to this database to a
user of your choice. Please refer to official MySQL documentations for
details and further commands.

::

         mysql> CREATE DATABASE #DB-NAME; 
         mysql> GRANT ALL PRIVILEGES ON #DB-NAME.* TO '#USERNAME'@'%' IDENTIFIED BY '#PASSWORD';

2.12.2 Intializing a MagPy database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

        db = mdb.mysql.connect(host="localhost",user="#USERNAME",passwd="#PASSWORD",db="#DB-NAME")
        mdb.dbinit(db)

2.12.3 Adding data to the database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

        iagacode = 'WIC'
        data = read(example1)
        gsm = data.selectkeys(['f'])
        fge = data.selectkeys(['x','y','z'])
        gsm.header['SensorID'] = 'GSM90_12345_0002'
        gsm.header['StationID'] = iagacode
        fge.header['SensorID'] = 'FGE_22222_0001'
        fge.header['StationID'] = iagacode
        mdb.writeDB(db,gsm)
        mdb.writeDB(db,fge)

All available meta information will be added automatically to the
related database tables. The SensorID scheme consists of three parts,
instrument (GSM90), serial number (12345), and a revision number (0002)
which might change in dependency of maintanance/calibration etc. As you
see in the example above we separete data from different instruments,
which we recommend particularly for high resolution data, as frequency
and noise characteristics of sensor types will differ.

2.12.4 Reading data
^^^^^^^^^^^^^^^^^^^

::

        data = mdb.readDB(db,'GSM90_12345_0002') 

Options e.g. starttime='' and endtime='' are similar as for normal
``read``.

2.12.5 Meta data
^^^^^^^^^^^^^^^^

An often used application of database cnnectivity will be to apply meta
information stored in the database to data files before submission. The
following command wills demostrate how to extract all missing meta
information from the database for the selected sensor and add it to the
header dictionary of the data object.

::

        rawdata = read('/path/to/rawdata.bin')
        rawdata.header = mdb.dbfields2dict(db,'FGE_22222_0001')
        rawdata.write(..., format_type='IMAGCDF')

2.13 Monitoring scheduled scripts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Automated analysis can e easily accomplished ba added a series of MagPy
commands into a script. A typical script could be:

::

        # read some data and get means
        data = read(example1)
        mean_f = data.mean('f')

        # import monitor method
        from magpy.opt import Analysismonitor
        analysisdict = Analysismonitor(logfile='/var/log/anamon.log')
        analysisdict = analysisdict.load()
        # check some arbitray threshold
        analysisdict.check({'data_threshold_f_GSM90': [mean_f,'>',20000]})

If given criteria are not valid, then the logfile is changed
accordingly. This method can assist you particularly in for checking
data actuality, data contents, data validity, upload success, etc. In
combination with an independend monitoring tool like
`Nagios <https://www.nagios.org/>`__ you can easily create mail/sms
notfications of such changes, in addition to monitoring processes, live
times, disks etc. `MARCOS <https://github.com/geomagpy/MARCOS>`__ comes
along with some instructions on how to use Nagios/MagPy for data
acquisition monitoring.

2.14 Acquisition support
~~~~~~~~~~~~~~~~~~~~~~~~

MagPy contains a couple of packages which could be used for data
acquisition, collection and organization. These methods are basically
used by two applications `MARTAS <https://github.com/geomagpy/MARTAS>`__
and `MARCOS <https://github.com/geomagpy/MARCOS>`__. MARTAS (Magpy
Automated Realtime Acquisition System) supports communication with many
common instruments (e.g. GSM, LEMI, POS1, FGE, and many non-magnetic
instruments) and transfers serial port signals to
`WAMP <http://wamp-proto.org/>`__ (Web Application Messaging Protocol)
which allows for real-time data access using e.g. WebSocket
communication through the internet. MARCOS (Magpy's Automated Realtime
Collection and Organistaion System) can access such realtime streams and
also data from many other sources and supports the Obsever by storing,
analyzing, archiving data, as well as monitoring all processes. Details
on these two applications can be found elsewhere.

2.15 Graphical user interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many of the above mentioned methods are also available within the
graphical user interface of MagPy. To use this check the installation
instructions for your operating system. You will find Video Tutorials
online (too be added) describing its usage for specific analyses.

2.16 Current developments
~~~~~~~~~~~~~~~~~~~~~~~~~

2.16.1 Exchange data objects with `ObsPy <https://github.com/obspy/obspy>`__
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

MagPy supports the exchange of data with ObsPy, the seismological
toolbox. Data objects of both python packages are very similar. Note:
ObsPy assumes regular spaced time intervals. Please be careful if this
is not the case with your data. The example below shows a simple import
routine, on how to read a seed file and plot a spectrogram (which you
can identically obtain from ObsPy as well). Conversions to MagPy allow
for vectorial analyses, and geomagnetic applications. Conversions to
ObsPy are useful for effective high frequency analysis, requiring evenly
spaced time intervals, and for exporting to seismological data formats.

::

        from obspy import read as obsread
        seeddata = obsread('/path/to/seedfile')
        magpydata = obspy2magpy(seeddata,keydict={'ObsPyColName': 'x'})
        mp.plotSpectrogram(magpydata,['x'])

2.16.2 Flagging in ImagCDF
^^^^^^^^^^^^^^^^^^^^^^^^^^

::

        datawithspikes = read(example1)
        flaggeddata = datawithspikes.flag_outlier(keys=['f'],timerange=timedelta(minutes=1),threshold=3)
        mp.plot(flaggeddata,['f'],annotate=True)
        flaggeddata.write(tmpdir,format_type='IMAGCDF',addflags=True)

The ``addflags`` option denotes that flagging information will be added
to the ImagCDF format. Please note that this is still under development
and thus content and format specifications may change. So please use it
only for test purposes and not for archiving. To read flagged ImagCDF
data just use the normal read command, and activate annotation for
plotting.

::

        new = read('/tmp/cnb_20120802_000000_PT1S_1.cdf')
        mp.plot(new,['f'],annotate=True)

2.17 List of all MagPy methods
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Please use the help method (section 2.3) for descriptions and return
values.

+--------+---------+------------+
| group  | method  | parameter  |
+========+=========+============+
| -      | **findp | name, path |
|        | ath**   |            |
+--------+---------+------------+
| -      | \*\*\_p | method     |
|        | ickle\_ |            |
|        | method\ |            |
|        | *\*     |            |
+--------+---------+------------+
| -      | \*\*\_u | func\_name |
|        | npickle | ,          |
|        | \_metho | obj, cls   |
|        | d\*\*   |            |
+--------+---------+------------+
| stream | ****ini | self,      |
|        | t****   | container= |
|        |         | None,      |
|        |         | header={}, |
|        |         | ndarray=No |
|        |         | ne         |
+--------+---------+------------+
| stream | **ext** | self,      |
|        |         | columnstru |
|        |         | cture      |
+--------+---------+------------+
| stream | **add** | self,      |
|        |         | datlst     |
+--------+---------+------------+
| stream | **lengt | self       |
|        | h**     |            |
+--------+---------+------------+
| stream | **repla | self,      |
|        | ce**    | datlst     |
+--------+---------+------------+
| stream | **copy* | self       |
|        | *       |            |
+--------+---------+------------+
| stream | ****str | self       |
|        | ****    |            |
+--------+---------+------------+
| stream | ****rep | self       |
|        | r****   |            |
+--------+---------+------------+
| stream | ****get | self,      |
|        | item*** | index      |
|        | *       |            |
+--------+---------+------------+
| stream | ****len | self       |
|        | ****    |            |
+--------+---------+------------+
| stream | **clear | self       |
|        | \_heade |            |
|        | r**     |            |
+--------+---------+------------+
| stream | **exten | self,datls |
|        | d**     | t,header,n |
|        |         | darray     |
+--------+---------+------------+
| stream | **union | self,colum |
|        | **      | n          |
+--------+---------+------------+
| stream | **remov | self       |
|        | eduplic |            |
|        | ates**  |            |
+--------+---------+------------+
| stream | **start | self,      |
|        | **      | dateformt= |
|        |         | None       |
+--------+---------+------------+
| stream | **end** | self,      |
|        |         | dateformt= |
|        |         | None       |
+--------+---------+------------+
| stream | **findt | self,time, |
|        | ime**   | \*\*kwargs |
+--------+---------+------------+
| stream | \*\*\_f | self       |
|        | ind\_t\ |            |
|        | _limits |            |
|        | \*\*    |            |
+--------+---------+------------+
| stream | \*\*\_p | self       |
|        | rint\_k |            |
|        | ey\_hea |            |
|        | ders\*\ |            |
|        | *       |            |
+--------+---------+------------+
| stream | \*\*\_g | self,\*\*k |
|        | et\_key | wargs      |
|        | \_heade |            |
|        | rs\*\*  |            |
+--------+---------+------------+
| stream | \*\*\_g | self       |
|        | et\_key |            |
|        | \_names |            |
|        | \*\*    |            |
+--------+---------+------------+
| stream | **drope | self       |
|        | mpty**  |            |
+--------+---------+------------+
| stream | **fille | self,      |
|        | mpty**  | ndarray,   |
|        |         | keylist    |
+--------+---------+------------+
| stream | **sorti | self       |
|        | ng**    |            |
+--------+---------+------------+
| stream | \*\*\_g | self, key, |
|        | et\_lin | value      |
|        | e\*\*   |            |
+--------+---------+------------+
| stream | \*\*\_t | self, keys |
|        | ake\_co |            |
|        | lumns\* |            |
|        | \*      |            |
+--------+---------+------------+
| stream | \*\*\_r | self, key, |
|        | emove\_ | value      |
|        | lines\* |            |
|        | \*      |            |
+--------+---------+------------+
| stream | \*\*\_g | self, key  |
|        | et\_col |            |
|        | umn\*\* |            |
+--------+---------+------------+
| stream | \*\*\_p | self,      |
|        | ut\_col | column,    |
|        | umn\*\* | key,       |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | \*\*\_m | self, key, |
|        | ove\_co | put2key    |
|        | lumn\*\ |            |
|        | *       |            |
+--------+---------+------------+
| stream | \*\*\_d | self,key   |
|        | rop\_co |            |
|        | lumn\*\ |            |
|        | *       |            |
+--------+---------+------------+
| stream | \*\*\_c | self, key  |
|        | lear\_c |            |
|        | olumn\* |            |
|        | \*      |            |
+--------+---------+------------+
| stream | \*\*\_r | self,      |
|        | educe\_ | pointlimit |
|        | stream\ | =100000    |
|        | *\*     |            |
+--------+---------+------------+
| stream | \*\*\_r | self       |
|        | emove\_ |            |
|        | nancolu |            |
|        | mns\*\* |            |
+--------+---------+------------+
| stream | \*\*\_a | self,      |
|        | ic\*\*  | signal, k, |
|        |         | debugmode= |
|        |         | None       |
+--------+---------+------------+
| stream | **harmf | self,nt,   |
|        | it**    | val,       |
|        |         | fitdegree  |
+--------+---------+------------+
| stream | \*\*\_g | self, key, |
|        | et\_max | returntime |
|        | \*\*    | =False     |
+--------+---------+------------+
| stream | \*\*\_g | self, key, |
|        | et\_min | returntime |
|        | \*\*    | =False     |
+--------+---------+------------+
| stream | **ampli | self,key   |
|        | tude**  |            |
+--------+---------+------------+
| stream | \*\*\_g | self, t,   |
|        | f\*\*   | tau        |
+--------+---------+------------+
| stream | \*\*\_h | self, p, x |
|        | f\*\*   |            |
+--------+---------+------------+
| stream | \*\*\_r | self,      |
|        | esidual | func, y    |
|        | \_func\ |            |
|        | *\*     |            |
+--------+---------+------------+
| stream | \*\*\_t | self,      |
|        | au\*\*  | period,    |
|        |         | fac=0.8325 |
|        |         | 5461       |
+--------+---------+------------+
| stream | \*\*\_c | self,      |
|        | onverts | coordinate |
|        | tream\* | ,          |
|        | \*      | \*\*kwargs |
+--------+---------+------------+
| stream | \*\*\_d | self,index |
|        | elete\* |            |
|        | \*      |            |
+--------+---------+------------+
| stream | \*\*\_a | self,strea |
|        | ppend\* | m          |
|        | \*      |            |
+--------+---------+------------+
| stream | \*\*\_d | self,      |
|        | et\_tra | period     |
|        | nge\*\* |            |
+--------+---------+------------+
| stream | \*\*\_i | self, s    |
|        | s\_numb |            |
|        | er\*\*  |            |
+--------+---------+------------+
| stream | \*\*\_n | self,      |
|        | ormaliz | column     |
|        | e\*\*   |            |
+--------+---------+------------+
| stream | \*\*\_t | self, time |
|        | esttime |            |
|        | \*\*    |            |
+--------+---------+------------+
| stream | \*\*\_d | self, key  |
|        | rop\_na |            |
|        | ns\*\*  |            |
+--------+---------+------------+
| stream | \*\*\_s | self, keys |
|        | elect\_ |            |
|        | keys\*\ |            |
|        | *       |            |
+--------+---------+------------+
| stream | \*\*\_s | self,      |
|        | elect\_ | starttime= |
|        | timeran | None,      |
|        | ge\*\*  | endtime=No |
|        |         | ne,        |
|        |         | maxidx=-1  |
+--------+---------+------------+
| stream | **aic\_ | self, key, |
|        | calc**  | \*\*kwargs |
+--------+---------+------------+
| stream | **basel | self,      |
|        | ine**   | absoluteda |
|        |         | ta,        |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **strea | self,      |
|        | m2dict* | keys=['dx' |
|        | *       | ,'dy','dz' |
|        |         | ],         |
|        |         | dictkey='D |
|        |         | ataBaseVal |
|        |         | ues'       |
+--------+---------+------------+
| stream | **dict2 | self,dictk |
|        | stream* | ey='DataBa |
|        | *       | seValues'  |
+--------+---------+------------+
| stream | **basel | self,      |
|        | ineAdva | absdata,   |
|        | nced**  | baselist,  |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **bc**  | self,      |
|        |         | function=N |
|        |         | one,       |
|        |         | ctype=None |
|        |         | ,          |
|        |         | alpha=0.0, |
|        |         | level='pre |
|        |         | liminary'  |
+--------+---------+------------+
| stream | **binde | self,key,f |
|        | tector* | lagnum=1,k |
|        | *       | eystoflag= |
|        |         | ['x'],sens |
|        |         | orid=None, |
|        |         | text=None, |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **calc\ | self,      |
|        | _f**    | \*\*kwargs |
+--------+---------+------------+
| stream | **daily | self,      |
|        | means** | keys=['x', |
|        |         | 'y','z','f |
|        |         | '],        |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **date\ | self,      |
|        | _offset | offset     |
|        | **      |            |
+--------+---------+------------+
| stream | **delta | self,      |
|        | \_f**   | \*\*kwargs |
+--------+---------+------------+
| stream | **f\_fr | self,      |
|        | om\_df* | \*\*kwargs |
|        | *       |            |
+--------+---------+------------+
| stream | **diffe | self,      |
|        | rentiat | \*\*kwargs |
|        | e**     |            |
+--------+---------+------------+
| stream | **DWT\_ | self,key=' |
|        | calc**  | x',wavelet |
|        |         | ='db4',lev |
|        |         | el=3,plot= |
|        |         | False,outf |
|        |         | ile=None,  |
+--------+---------+------------+
| stream | **event | self, key, |
|        | logger* | values,    |
|        | *       | compare=No |
|        |         | ne,        |
|        |         | stringvalu |
|        |         | es=None,   |
|        |         | addcomment |
|        |         | =None,     |
|        |         | debugmode= |
|        |         | None       |
+--------+---------+------------+
| stream | **extra | self, key, |
|        | ct**    | value,     |
|        |         | compare=No |
|        |         | ne,        |
|        |         | debugmode= |
|        |         | None       |
+--------+---------+------------+
| stream | **extra | self,      |
|        | ct2**   | keys,      |
|        |         | get='>',   |
|        |         | func=None, |
|        |         | debugmode= |
|        |         | None       |
+--------+---------+------------+
| stream | **extra | self,      |
|        | polate* | start, end |
|        | *       |            |
+--------+---------+------------+
| stream | **filte | self,\*\*k |
|        | r**     | wargs      |
+--------+---------+------------+
| stream | **fit** | self,      |
|        |         | keys,      |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **extra | self       |
|        | ctflags |            |
|        | **      |            |
+--------+---------+------------+
| stream | **flagf | self,index |
|        | ast**   | array,flag |
|        |         | ,          |
|        |         | comment,ke |
|        |         | ys=None    |
+--------+---------+------------+
| stream | **flag\ | self,      |
|        | _range* | \*\*kwargs |
|        | *       |            |
+--------+---------+------------+
| stream | **flag\ | self,      |
|        | _outlie | \*\*kwargs |
|        | r**     |            |
+--------+---------+------------+
| stream | **flag* | self,      |
|        | *       | flaglist,  |
|        |         | removedupl |
|        |         | icates=Fal |
|        |         | se,        |
|        |         | debug=Fals |
|        |         | e          |
+--------+---------+------------+
| stream | **flagl | self,flagl |
|        | iststat | ist        |
|        | s**     |            |
+--------+---------+------------+
| stream | **flagl | self,flagl |
|        | istclea | ist        |
|        | n**     |            |
+--------+---------+------------+
| stream | **strea | self,      |
|        | m2flagl | userange=T |
|        | ist**   | rue,       |
|        |         | flagnumber |
|        |         | =None,     |
|        |         | keystoflag |
|        |         | =None,     |
|        |         | sensorid=N |
|        |         | one,       |
|        |         | comment=No |
|        |         | ne         |
+--------+---------+------------+
| stream | **flagl | self,      |
|        | istmod* | mode='sele |
|        | *       | ct',       |
|        |         | flaglist=[ |
|        |         | ],         |
|        |         | parameter= |
|        |         | 'key',     |
|        |         | value=None |
|        |         | ,          |
|        |         | newvalue=N |
|        |         | one        |
+--------+---------+------------+
| stream | **flagl | self,      |
|        | istadd* | flaglist,  |
|        | *       | sensorid,  |
|        |         | keys,      |
|        |         | flagnumber |
|        |         | ,          |
|        |         | comment,   |
|        |         | startdate, |
|        |         | enddate=No |
|        |         | ne         |
+--------+---------+------------+
| stream | **flag\ | self, key, |
|        | _stream | flag,      |
|        | **      | comment,   |
|        |         | startdate, |
|        |         | enddate=No |
|        |         | ne,        |
|        |         | samplingra |
|        |         | te=0.,     |
|        |         | debug=Fals |
|        |         | e          |
+--------+---------+------------+
| stream | **simpl | self,basev |
|        | ebaseva | alue,\*\*k |
|        | lue2str | wargs      |
|        | eam**   |            |
+--------+---------+------------+
| stream | **func2 | self,funct |
|        | stream* | ion,\*\*kw |
|        | *       | args       |
+--------+---------+------------+
| stream | **func\ | self,funct |
|        | _add**  | ion,\*\*kw |
|        |         | args       |
+--------+---------+------------+
| stream | **func\ | self,funct |
|        | _subtra | ion,\*\*kw |
|        | ct**    | args       |
+--------+---------+------------+
| stream | **get\_ | self,      |
|        | gaps**  | \*\*kwargs |
+--------+---------+------------+
| stream | **get\_ | self,      |
|        | rotatio | xcompensat |
|        | nangle* | ion=0,keys |
|        | *       | =['x','y', |
|        |         | 'z'],\*\*k |
|        |         | wargs      |
+--------+---------+------------+
| stream | **get\_ | self       |
|        | samplin |            |
|        | g\_peri |            |
|        | od**    |            |
+--------+---------+------------+
| stream | **sampl | self,      |
|        | ingrate | \*\*kwargs |
|        | **      |            |
+--------+---------+------------+
| stream | **integ | self,      |
|        | rate**  | \*\*kwargs |
+--------+---------+------------+
| stream | **inter | self,      |
|        | pol**   | keys,      |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **k\_ex | self,      |
|        | tend**  | \*\*kwargs |
+--------+---------+------------+
| stream | **k\_fm | self,      |
|        | i**     | \*\*kwargs |
+--------+---------+------------+
| stream | **lines | self       |
|        | truct2n |            |
|        | darray* |            |
|        | *       |            |
+--------+---------+------------+
| stream | **mean* | self, key, |
|        | *       | \*\*kwargs |
+--------+---------+------------+
| stream | **missi | self,v,win |
|        | ngvalue | dow\_len,t |
|        | **      | hreshold=0 |
|        |         | .9,fill='m |
|        |         | ean'       |
+--------+---------+------------+
| stream | **MODWT | self,key=' |
|        | \_calc* | x',wavelet |
|        | *       | ='haar',le |
|        |         | vel=1,plot |
|        |         | =False,out |
|        |         | file=None  |
+--------+---------+------------+
| stream | **multi | self,      |
|        | ply**   | factors,   |
|        |         | square=Fal |
|        |         | se         |
+--------+---------+------------+
| stream | **offse | self,      |
|        | t**     | offsets,   |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **plot* | self,      |
|        | *       | keys=None, |
|        |         | debugmode= |
|        |         | None,      |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **power | self, key, |
|        | spectru | debugmode= |
|        | m**     | None,      |
|        |         | outfile=No |
|        |         | ne,        |
|        |         | fmt=None,  |
|        |         | axes=None, |
|        |         | title=None |
|        |         | ,\*\*kwarg |
|        |         | s          |
+--------+---------+------------+
| stream | **rando | self,perce |
|        | mdrop** | ntage=None |
|        |         | ,fixed\_in |
|        |         | dicies=Non |
|        |         | e          |
+--------+---------+------------+
| stream | **remov | self,      |
|        | e**     | starttime= |
|        |         | None,      |
|        |         | endtime=No |
|        |         | ne         |
+--------+---------+------------+
| stream | **remov | self,      |
|        | e\_flag | \*\*kwargs |
|        | ged**   |            |
+--------+---------+------------+
| stream | **remov | self,      |
|        | e\_outl | \*\*kwargs |
|        | ier**   |            |
+--------+---------+------------+
| stream | **resam | self,      |
|        | ple**   | keys,      |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **rotat | self,\*\*k |
|        | ion**   | wargs      |
+--------+---------+------------+
| stream | **scale | self,      |
|        | \_corre | keys,      |
|        | ction** | scales,    |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **selec | self,      |
|        | tkeys** | keys,      |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **smoot | self,      |
|        | h**     | keys=None, |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **spect | self,      |
|        | rogram* | keys,      |
|        | *       | per\_lap=0 |
|        |         | .9,        |
|        |         | wlen=None, |
|        |         | log=False, |
+--------+---------+------------+
| stream | **stead | self, key, |
|        | yrise** | timewindow |
|        |         | ,          |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **stere | self,      |
|        | oplot** | \*\*kwargs |
+--------+---------+------------+
| stream | **trim* | self,      |
|        | *       | starttime= |
|        |         | None,      |
|        |         | endtime=No |
|        |         | ne,        |
|        |         | newway=Fal |
|        |         | se         |
+--------+---------+------------+
| stream | **vario | self,      |
|        | meterco | variopath, |
|        | rrectio | thedate,   |
|        | n**     | \*\*kwargs |
+--------+---------+------------+
| stream | \*\*\_w | self,      |
|        | rite\_f | format\_ty |
|        | ormat\* | pe,        |
|        | \*      | filenamebe |
|        |         | gins,      |
|        |         | filenameen |
|        |         | ds,        |
|        |         | coverage,  |
|        |         | dateformat |
|        |         | ,year      |
+--------+---------+------------+
| stream | **write | self,      |
|        | **      | filepath,  |
|        |         | compressio |
|        |         | n=5,       |
|        |         | \*\*kwargs |
+--------+---------+------------+
| stream | **idf2x | self,\*\*k |
|        | yz**    | wargs      |
+--------+---------+------------+
| stream | **xyz2i | self,\*\*k |
|        | df**    | wargs      |
+--------+---------+------------+
| stream | **xyz2h | self,\*\*k |
|        | dz**    | wargs      |
+--------+---------+------------+
| stream | **hdz2x | self,\*\*k |
|        | yz**    | wargs      |
+--------+---------+------------+
| -      | **coord | u,v,w,kind |
|        | inatetr |            |
|        | ansform |            |
|        | **      |            |
+--------+---------+------------+
| -      | **isNum | s          |
|        | ber**   |            |
+--------+---------+------------+
| -      | **find\ | array,valu |
|        | _neares | e          |
|        | t**     |            |
+--------+---------+------------+
| -      | **ceil\ | dt,seconds |
|        | _dt**   |            |
+--------+---------+------------+
| -      | **read* | path\_or\_ |
|        | *       | url=None,  |
|        |         | dataformat |
|        |         | =None,     |
|        |         | headonly=F |
|        |         | alse,      |
|        |         | \*\*kwargs |
+--------+---------+------------+
| -      | \*\*\_r | filename,  |
|        | ead\*\* | dataformat |
|        |         | =None,     |
|        |         | headonly=F |
|        |         | alse,      |
|        |         | \*\*kwargs |
+--------+---------+------------+
| -      | **savef | mylist=Non |
|        | lags**  | e,path=Non |
|        |         | e          |
+--------+---------+------------+
| -      | **loadf | path=None, |
|        | lags**  | sensorid=N |
|        |         | one,begin= |
|        |         | None,      |
|        |         | end=None   |
+--------+---------+------------+
| -      | **joinS | stream\_a, |
|        | treams* | stream\_b, |
|        | *       | \*\*kwargs |
+--------+---------+------------+
| -      | **appen | streamlist |
|        | dStream |            |
|        | s**     |            |
+--------+---------+------------+
| -      | **merge | stream\_a, |
|        | Streams | stream\_b, |
|        | **      | \*\*kwargs |
+--------+---------+------------+
| -      | **dms2d | dms        |
|        | **      |            |
+--------+---------+------------+
| -      | **find\ | stream1,   |
|        | _offset | stream2,   |
|        | **      | guess\_low |
|        |         | =-60.,     |
|        |         | guess\_hig |
|        |         | h=60.      |
+--------+---------+------------+
| -      | **diffS | stream\_a, |
|        | treams* | stream\_b, |
|        | *       | \*\*kwargs |
+--------+---------+------------+
| -      | **subtr | stream\_a, |
|        | actStre | stream\_b, |
|        | ams**   | \*\*kwargs |
+--------+---------+------------+
| -      | **stack | streamlist |
|        | Streams | ,          |
|        | **      | \*\*kwargs |
+--------+---------+------------+
| -      | **compa | stream\_a, |
|        | reStrea | stream\_b  |
|        | ms**    |            |
+--------+---------+------------+
| -      | **array | listofarra |
|        | 2stream | ys,        |
|        | **      | keystring, |
|        |         | starttime= |
|        |         | None,sr=No |
|        |         | ne         |
+--------+---------+------------+
| -      | **obspy | opstream,  |
|        | 2magpy* | keydict={} |
|        | *       |            |
+--------+---------+------------+
| -      | **extra | datestring |
|        | ctDateF |            |
|        | romStri |            |
|        | ng**    |            |
+--------+---------+------------+
| -      | **testT | time       |
|        | imeStri |            |
|        | ng**    |            |
+--------+---------+------------+
| -      | **denor | column,    |
|        | malize* | startvalue |
|        | *       | ,          |
|        |         | endvalue   |
+--------+---------+------------+
| -      | **find\ | array,     |
|        | _neares | value      |
|        | t**     |            |
+--------+---------+------------+
| -      | **maskN | column     |
|        | AN**    |            |
+--------+---------+------------+
| -      | **nan\_ | y          |
|        | helper* |            |
|        | *       |            |
+--------+---------+------------+
| -      | **neare | x          |
|        | stPow2* |            |
|        | *       |            |
+--------+---------+------------+
| -      | **test\ | time       |
|        | _time** |            |
+--------+---------+------------+
| -      | **conve | lon,lat,pr |
|        | rtGeoCo | o1,pro2    |
|        | ordinat |            |
|        | e**     |            |
+--------+---------+------------+
| mpplot | **plote | stream     |
|        | asy**   |            |
+--------+---------+------------+
| mpplot | **plot\ | stream,var |
|        | _new**  | iables=[], |
|        |         | specialdic |
|        |         | t={},error |
|        |         | bars=False |
|        |         | ,padding=0 |
|        |         | ,noshow=Fa |
|        |         | lse        |
+--------+---------+------------+
| mpplot | **plot* | stream,var |
|        | *       | iables=[], |
|        |         | specialdic |
|        |         | t={},error |
|        |         | bars=False |
|        |         | ,padding=0 |
|        |         | ,noshow=Fa |
|        |         | lse        |
+--------+---------+------------+
| mpplot | **plotS | streamlist |
|        | treams* | ,variables |
|        | *       | ,padding=N |
|        |         | one,specia |
|        |         | ldict={},e |
|        |         | rrorbars=N |
|        |         | one        |
+--------+---------+------------+
| mpplot | **toggl | event      |
|        | e\_sele |            |
|        | ctor**  |            |
+--------+---------+------------+
| mpplot | **addFl | data,      |
|        | ag**    | flagger,   |
|        |         | indeciesto |
|        |         | beflagged, |
|        |         | variables  |
+--------+---------+------------+
| mpplot | **plotF | data,varia |
|        | lag**   | bles=None, |
|        |         | figure=Fal |
|        |         | se         |
+--------+---------+------------+
| mpplot | **plotE | stream,key |
|        | MD**    | ,verbose=F |
|        |         | alse,plott |
|        |         | itle=None  |
+--------+---------+------------+
| mpplot | **plotN | streamlist |
|        | ormStre | ,          |
|        | ams**   | key,       |
|        |         | normalize= |
|        |         | True,      |
|        |         | normalizet |
|        |         | =False     |
+--------+---------+------------+
| mpplot | **plotP | stream,key |
|        | S**     | ,debugmode |
|        |         | =False,out |
|        |         | file=None, |
|        |         | noshow=Fal |
|        |         | se         |
+--------+---------+------------+
| mpplot | **plotS | mag\_strea |
|        | atMag** | m,sat\_str |
|        |         | eam,keys,o |
|        |         | utfile=Non |
|        |         | e,plottype |
|        |         | ='disconti |
|        |         | nuous'     |
+--------+---------+------------+
| mpplot | **plotS | stream,    |
|        | pectrog | keys,      |
|        | ram**   | NFFT=1024, |
|        |         | detrend=ml |
|        |         | ab.detrend |
|        |         | \_none     |
+--------+---------+------------+
| mpplot | **magpy | x,         |
|        | Specgra | NFFT=256,  |
|        | m**     | Fs=2,      |
|        |         | Fc=0,      |
|        |         | detrend=ml |
|        |         | ab.detrend |
|        |         | \_none     |
+--------+---------+------------+
| mpplot | **plotS | stream,foc |
|        | tereopl | us='all',c |
|        | ot**    | olorlist   |
|        |         | =          |
|        |         | ['b','r',' |
|        |         | g','c','m' |
|        |         | ,'y','k']  |
+--------+---------+------------+
| mpplot | \*\*\_p | data,saved |
|        | lot\*\* | pi=80,grid |
|        |         | =True,grid |
|        |         | color=grid |
|        |         | color,nosh |
|        |         | ow=False   |
+--------+---------+------------+
| mpplot | \*\*\_c | ax, tmax,  |
|        | onfinex | tmin,      |
|        | \*\*    | timeunit   |
+--------+---------+------------+
| mpplot | \*\*\_e | stream,    |
|        | xtract\ | key        |
|        | _data\_ |            |
|        | for\_PS |            |
|        | D\*\*   |            |
+--------+---------+------------+
| databa | **dbget | db,pierid, |
| se     | Pier**  | rp, value, |
|        |         | maxdate=No |
|        |         | ne,        |
|        |         | l=False,   |
|        |         | dic='Delta |
|        |         | Dictionary |
|        |         | '          |
+--------+---------+------------+
| databa | **dbget | db,        |
| se     | lines** | tablename, |
|        |         | lines      |
+--------+---------+------------+
| databa | **dbupd | db,tablena |
| se     | ate**   | me,        |
|        |         | keys,      |
|        |         | values,    |
|        |         | condition= |
|        |         | None       |
+--------+---------+------------+
| databa | **dbget | db,tablena |
| se     | float** | me,sensori |
|        |         | d,columnid |
|        |         | ,revision= |
|        |         | None       |
+--------+---------+------------+
| databa | **dbget | db,tablena |
| se     | string* | me,sensori |
|        | *       | d,columnid |
|        |         | ,revision= |
|        |         | None       |
+--------+---------+------------+
| databa | **dbupl | db,        |
| se     | oad**   | path,stati |
|        |         | onid,\*\*k |
|        |         | wargs      |
+--------+---------+------------+
| databa | **dbini | db         |
| se     | t**     |            |
+--------+---------+------------+
| databa | **dbdel | db,datainf |
| se     | ete**   | oid,\*\*kw |
|        |         | args       |
+--------+---------+------------+
| databa | **dbdic | db,header\ |
| se     | t2field | _dict,\*\* |
|        | s**     | kwargs     |
+--------+---------+------------+
| databa | **dbfie | db,datainf |
| se     | lds2dic | oid        |
|        | t**     |            |
+--------+---------+------------+
| databa | **dbalt | db         |
| se     | er**    |            |
+--------+---------+------------+
| databa | **dbsel | db,        |
| se     | ect**   | element,   |
|        |         | table,     |
|        |         | condition= |
|        |         | None,      |
|        |         | expert=Non |
|        |         | e,         |
|        |         | debug=Fals |
|        |         | e          |
+--------+---------+------------+
| databa | **dbcoo | db, pier,  |
| se     | rdinate | epsgcode=' |
|        | s**     | epsg:4326' |
+--------+---------+------------+
| databa | **dbsen | db,sensori |
| se     | sorinfo | d,sensorke |
|        | **      | ydict=None |
|        |         | ,sensorrev |
|        |         | ision      |
|        |         | = '0001'   |
+--------+---------+------------+
| databa | **dbdat | db,sensori |
| se     | ainfo** | d,datakeyd |
|        |         | ict=None,t |
|        |         | ablenum=No |
|        |         | ne,default |
|        |         | station='W |
|        |         | IC',update |
|        |         | db=True    |
+--------+---------+------------+
| databa | **write | db,        |
| se     | DB**    | datastream |
|        |         | ,          |
|        |         | tablename= |
|        |         | None,      |
|        |         | StationID= |
|        |         | None,      |
|        |         | mode='repl |
|        |         | ace',      |
|        |         | revision=N |
|        |         | one,       |
|        |         | debug=Fals |
|        |         | e,         |
|        |         | \*\*kwargs |
+--------+---------+------------+
| databa | **dbset | db,        |
| se     | Timesin | tablename, |
|        | DataInf | colstr,uni |
|        | o**     | tstr       |
+--------+---------+------------+
| databa | **dbupd | db,        |
| se     | ateData | tablename, |
|        | Info**  | header     |
+--------+---------+------------+
| databa | **strea | db,        |
| se     | m2db**  | datastream |
|        |         | ,          |
|        |         | noheader=N |
|        |         | one,       |
|        |         | mode=None, |
|        |         | tablename= |
|        |         | None,      |
|        |         | \*\*kwargs |
+--------+---------+------------+
| databa | **readD | db, table, |
| se     | B**     | starttime= |
|        |         | None,      |
|        |         | endtime=No |
|        |         | ne,        |
|        |         | sql=None   |
+--------+---------+------------+
| databa | **db2st | db,        |
| se     | ream**  | sensorid=N |
|        |         | one,       |
|        |         | begin=None |
|        |         | ,          |
|        |         | end=None,  |
|        |         | tableext=N |
|        |         | one,       |
|        |         | sql=None   |
+--------+---------+------------+
| databa | **dilin | db,        |
| se     | e2db**  | dilinestru |
|        |         | ct,        |
|        |         | mode=None, |
|        |         | \*\*kwargs |
+--------+---------+------------+
| databa | **db2di | db,\*\*kwa |
| se     | line**  | rgs        |
+--------+---------+------------+
| databa | **apply | db, stream |
| se     | Deltas* |            |
|        | *       |            |
+--------+---------+------------+
| databa | **getBa | db,sensori |
| se     | seline* | d,         |
|        | *       | date=None  |
+--------+---------+------------+
| databa | **flagl | db,flaglis |
| se     | ist2db* | t,mode=Non |
|        | *       | e,sensorid |
|        |         | =None,modi |
|        |         | ficationda |
|        |         | te=None    |
+--------+---------+------------+
| databa | **db2fl | db,sensori |
| se     | aglist* | d,         |
|        | *       | begin=None |
|        |         | ,          |
|        |         | end=None,  |
|        |         | comment=No |
|        |         | ne,        |
|        |         | flagnumber |
|        |         | =-1,       |
|        |         | key=None,  |
|        |         | removedupl |
|        |         | icates=Fal |
|        |         | se         |
+--------+---------+------------+
| databa | **strin | string     |
| se     | g2dict* |            |
|        | *       |            |
+--------+---------+------------+
| tranfe | \*\*\_c | logfile    |
| r      | hecklog |            |
|        | file\*\ |            |
|        | *       |            |
+--------+---------+------------+
| tranfe | **ftpda | \*\*kwargs |
| r      | tatrans |            |
|        | fer**   |            |
+--------+---------+------------+
| tranfe | \*\*\_m | myproxy,   |
| r      | issingv | port,      |
|        | als\*\* | login,     |
|        |         | passwd,    |
|        |         | logfile    |
+--------+---------+------------+
| tranfe | **scptr | src,dest,p |
| r      | ansfer* | asswd,\*\* |
|        | *       | kwargs     |
+--------+---------+------------+
| tranfe | **ssh\_ | remotepath |
| r      | remotef | ,          |
|        | ilelist | filepat,   |
|        | **      | user,      |
|        |         | host,      |
|        |         | passwd     |
+--------+---------+------------+
| tranfe | **ginup | filename=N |
| r      | load**  | one,       |
|        |         | user=None, |
|        |         | password=N |
|        |         | one,       |
|        |         | url=None,\ |
|        |         | *\*kwargs  |
+--------+---------+------------+
| tranfe | **ftpdi | \*\*kwargs |
| r      | rlist** |            |
+--------+---------+------------+
| tranfe | **ftpre | \*\*kwargs |
| r      | move**  |            |
+--------+---------+------------+
| tranfe | **ftpge | ftpaddress |
| r      | t**     | ,ftpname,f |
|        |         | tppasswd,r |
|        |         | emotepath, |
|        |         | localpath, |
|        |         | identifier |
|        |         | ,port=None |
|        |         | ,\*\*kwarg |
|        |         | s          |
+--------+---------+------------+

