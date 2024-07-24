# MagPy
**MagPy (or GeomagPy) is a Python package for analysing and displaying geomagnetic data.**

Version Info: (please note: this package is still in a development state with frequent modifcations) please check the release notes.

MagPy provides tools for geomagnetic data analysis with special focus on typical data processing routines in observatories. MagPy provides methods for data format conversion, plotting and mathematical procedures with specifically geomagnetic analysis routines such as basevalue and baseline calculation and database handling. Among the supported data formats are *ImagCDF, IAGA-02, WDC, IMF, IAF, BLV*, and many more.
Full installation also provides a graphical user interface, *xmagpy*. You will find a complete manual for *xmagpy* in the docs.

Typical usage of the basic MagPy package for reading and visualising data looks like this:

        #!/usr/bin/env python

        from magpy.stream import read
        import magpy.mpplot as mp
        stream = read('filename_or_url')
        mp.plot(stream)

Below you will find a quick guide to usage of the basic MagPy package. For instructions on *xmagpy* please refer to the document "[An introduction to XMagPy]" in the docs. You can also subscribe to our information channel at [Telegram] for further information on updates and current issues.


### Contents

1. Installation and requirements
2. Quick guide with often used commands
3. Reading and writing, data typs
4. Figures
5. Timeseries methods
6. Annotating data and flagging
7. DI-flux measurements, basevalues and baselines
8. Geomagnetic activity analysis
9. Database support
10. Additional tools and applications

## 1. INSTALLATION AND REQUIREMENTS

In the following you will find a quick summary on how to install MagPy2.x on various different operating systems.
If you are looking for instructions on how to install the graphical version "XMagPy" versions 2.x please consult
the appropriate manual. Some general instructions for XMagPy are listed in section 1.x.

We highly recommend installing MagPy in appropriate python environments (Python>=3.7). Within
the following instructions we will show examples specifically for anaconda/miniconda
python environments.

### 1.1 Prerequisites

MagPy requires Python3.7 or newer. MagPy makes use of a number of packages of which the following are 
essential for its basic functionality: 
numpy
scipy
matplotlib

Optional but recommended python packages are:
cdflib : support of ImagCDF, the INTERMAGNET one-second format, and internal MagPy CDF archives)
jupyter-notebook : coding
pandas : timeseries manipulation (flagging and activity analysis)
pymysql : mysql/mariaDB data base support
paho-mqtt : realtime data access of MARTAS
pysubpub :  realtime data access of MARTAS
emd : empirical mode decomposition for Sq (solar quiet) analysis and flagging
sklearn : AI flagging support and geomagnetic activity forcasts


### 1.2 Linux installation (Ubuntu,Debian like systems)

#### 1.2.1 Recommended: Using environments
In the following we assume a basic knowledge of linux systems and installations. 
You need a working version of anaconda or miniconda.

  - we recommend [Miniconda] or [Anaconda]
  - see e.g. https://docs.continuum.io/anaconda/install for more details
  - before continuing, test whether python is working. Open a terminal and run python

Now open a terminal and create a python environment with packages for magpy which supports jupyter-notebook and includes essential packages:

        (base)$ conda create -n jnmagpy scipy numpy matplotlib notebook

Switch into this environment:

        (base)$ conda activate jnmagpy

Install some basic packages packages for full MagPy support:

        (jnmagpy)$ conda install pymysql

and finally MagPy:

        (jnmagpy)$ pip install geomagpy

Now you can run python in the terminal and import the magpy package:

        (jnmagpy)$: python
        >>> from magpy.stream import *
        >>> data = read("path_to_supported_data")

To upgrade to the most recent version (replace x.x.x with the current version number):

        $ pip install geomagpy==x.x.x

Running magpy within jupyter-notebook. First switch to a path were you want to store your notebooks:

        (jnmagpy)$: cd ~/MyNotenooks
 
and then start jupyter-notebook

        (jnmagpy)$: juypter-notebook

This will open a browser window. Please follow the jn instruction here

To use MagPy import and select an appropriate backend:

        > from magpy.stream import *
        > matplotlib.use("tkagg")
        > %matplotlib inline

#### 1.2.2 Not-Recommended: Using system python

Make sure to install most required packages using *apt install* before installing geomagpy

        $ sudo apt install python3-scipy python3-numpy python3-matplotlib python3-pip

Optional but recommended packages are 

        $ sudo apt install python3-pymysql python3-pandas python3-wxgtk4.0 libproj-dev proj-data proj-bin

Now install geomagpy and its remaining dependencies

        $ sudo pip3 install geomagpy

### 1.3 MacOs installation

Please also use a python environment for MacOs usage

  - we recommend [Miniconda] or [Anaconda]
  - see e.g. https://docs.continuum.io/anaconda/install for more details
  - before continuiung, test whether python is working. Open a terminal and run python

Follow the instructions of 1.2.


### 1.4 Windows installation

#### 1.4.1 Install MagPy for Windows

  - get the [MagPy Windows installer] here (under Downloads):
        https://cobs.geosphere.at
  - download and execute magpy-x.x.x.exe
  - all required packages are included in the installer

#### 1.4.2 Post-installation information

  - MagPy will have a sub-folder in the Start menu. Here you will find three items:

        * command -> opens a DOS shell within the Python environment e.g. for updates
        * python  -> opens a python shell ready for MagPy
        * xmagpy  -> opens the MagPy graphical user interface

#### 1.4.3 Update an existing MagPy installation on Windows

  - right-click on subfolder "command" in the start menu
  - select "run as administrator"
  - issue the following command "pip install -U geomagpy"
    (you can also specify the version e.g. pip install geomagpy==0.x.x)


### 1.5 Installing XMagPy on Linux/MacOs

XMagPy is making use of wxpython. Basically we are mainly testing the linux version 
of this application and try to keep up with support for other operating systems. We highly recommend
to follow the instructions below.

#### 1.5.1 XMagPy prerequisites on Linux

WX support requires the following package.

        $ sudo apt-get install python3-wxgtk4.0

In order to get a suitable python environment for wxpython you then need to create a new one based on wxpython

        (base)$ conda create -n wxmagpy wxpython

Then install the essential packages:

        (base)$ conda activate wxmagpy

        (wxmagpy)$ conda install numpy scipy matplotlib

Continue with the optional packages as shown in 1.2
Finally you can run the graphical user interface as follows:

        (wxmagpy)$ xmagpy

#### 1.5.2 Creating a desktop link for Linux

#### 1.5.3 XMagPy on MacOs

Follow the instructions of 1.5.1.

You can now run XMagPy from the terminal by using the following command

        $ xmagpyw

#### 1.5.4 Creating a desktop link for MacOs

To execute a python program within a specific environment it is recommended to create a small startupscript i.e. named xmagpy:

        #!/bin/bash
        eval "$(conda shell.bash hook)"
        conda activate magpy
        xmagpyw

Make it executable e.g. by chmod 755 xmagpy.
Open Finder and search for your script "xmagpy". Copy it to the desktop. To change the icon, click on the xmagpy link, open information and replace the image on the upper left with e.g. magpy128.jpg (also to be found using finder).


### 1.6 Platform independent container - Docker

#### 1.6.1 Install [Docker] (toolbox) on your operating system
     - https://docs.docker.com/engine/installation/

#### 1.6.2 Get the MagPy Image
     - open a docker shell

            >>> docker pull geomagpy/magpy:latest
            >>> docker run -d --name magpy -p 8000:8000 geomagpy/magpy:latest

#### 1.6.3 Open a browser
     - open address http://localhost:8000 (or http://"IP of your VM":8000)
     - NEW: first time access might require a token or passwd

            >>> docker logs magpy

          will show the token
     - run python shell (not conda)
     - in python shell

            >>> %matplotlib inline
            >>> from magpy.stream import read
            >>> import magpy.mpplot as mp
            >>> data = read(example1)
            >>> mp.plot(data)


## 2. A quick guide to MagPy

written by R. Leonhardt, R. Bailey (April 2017)

MagPy's functionality can be accessed basically in three different ways:
    1) Directly import and use the magpy package into a python environment
    2) Run the graphical user interface xmagpy (xmagpyw for Mac)
    3) Use predefined applications "Scripts"

The following section will primarily deal with way 1.
For 2 - xmagpy - we refer to the specfic xmagpy manual can be found in the download section of 
the [Conrad Observatory Webpage](https://cobs.geosphere.at).
Predefined applications/scripts are summarized in the appendix.

### 2.1 Getting started with the python package

Start python. Import all stream methods and classes using:

    from magpy.stream import *

Please note that this import will shadow any already existing `read` method.

## To be moved elsewhere


### 2.3 Getting help on options and usage

#### 2.3.1 Python's help function

Information on individual methods and options can be obtained as follows:

For basic functions:

        help(read)

For specific methods related to e.g. a stream object "data":

        help(data.fit)

Note that this requires the existence of a "data" object, which is obtained e.g. by data = read(...). The help text can also be shown by directly calling the *DataStream* object method using:

        help(DataStream().fit)



#### 2.3.2 MagPy's logging system

MagPy automatically logs many function options and runtime information, which can be useful for debugging purposes. This log is saved by default in the temporary file directory of your operating system, e.g. for Linux this would be `/tmp/magpy.log`. The log is formatted as follows with the date, module and function in use and the message leve (INFO/WARNING/ERROR):

        2017-04-22 09:50:11,308 INFO - magpy.stream - Initiating MagPy...

Messages on the WARNING and ERROR level will automatically be printed to shell. Messages for more detailed debugging are written at the DEBUG level and will not be printed to the log unless an additional handler for printing DEBUG is added.

Custom loggers can be defined by creating a logger object after importing MagPy and adding handlers (with formatting):

        from magpy.stream import *
        import logging

        logger = logging.getLogger()
        hdlr = logging.FileHandler('testlog.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)

The logger can also be configured to print to shell (stdout, without formatting):

        import sys
        logger = logging.getLogger()
        stdoutlog = logging.StreamHandler(sys.stdout)
        logger.addHandler(stdoutlog)




## 3. Reading and writing data

MagPy supports many different data formats and thus conversions between them. For geomagnetic purposes
the most important are listed below.

   - WDC: 	World Data Centre format
   - JSON: 	JavaScript Object Notation
   - IMF: 	Intermagnet Format
   - IAF: 	Intermagnet Archive Format
   - IAGA: 	IAGA 2002 text format
   - IMAGCDF: 	Intermagnet CDF Format
   - GFZKP: 	GeoForschungsZentrum KP-Index format
   - GSM19/GSM90: 	Output formats from GSM magnetometers
   - POS1: 	POS-1 binary output
   - LEMI: 	LEMIXXX binary output
   - BLV: 	Baseline format Intermagnet
   - IYFV: 	Yearly mean format Intermagnet
   - DKA: 	K Value file

... and many others. To get a full list, use:

        from magpy.stream import *
        print(PYMAG_SUPPORTED_FORMATS)

You will find several example files provided with MagPy. The `cdf` file is stored along with meta information in NASA's common data format (cdf). Reading this file requires a working installation of Spacepy cdf.

If you do not have any geomagnetic data file you can access example data by using the following command (after `import *`):

        data = read(example1)

The data from `example1` has been read into a MagPy *DataStream* (or *stream*) object. Most data processing routines in MagPy are applied to data streams.

Several example data sets are provided within the MagPy package:

   - `example1`: [IAGA] ZIP (IAGA2002, zip compressed) file with 1 second HEZ data
   - `example2`: [MagPy] Archive (CDF) file with 1 sec F data
   - `example3`: [MagPy] Basevalue (TXT) ascii file with DI and baseline data
   - `example4`: [INTERMAGNET] ImagCDF (CDF) file with one week of 1 second data
   - `example5`: [MagPy] Archive (CDF) raw data file with xyz and supporting data
   - `example6a`: [MagPy] DI (txt) raw data file with DI measurement
   - `example6b`: [MagPy] like 6a to be used with example4

   - `flagging_example`: [MagPy] FlagDictionary (JSON) flagging info to be used with example1
   - `recipe1_flags`: [MagPy] FlagDictionary (JSON) to be used with cookbook recipe 1

### 3.1 Reading

For a file in the same directory:

        data = read(r'myfile.min')

... or for specific paths in Linux:

        data = read(r'/path/to/file/myfile.min')

... or for specific paths in Windows:

        data = read(r'c:\path\to\file\myfile.min')

Pathnames are related to your operating system. Hereinafter we will assume a Linux system in this guide. Files that are read in are uploaded to the memory and each data column (or piece of header information) is assigned to an internal variable (key). To get a quick overview of the assigned keys in any given stream (`data`) you can use the following method:

        print(data._get_key_headers() )

### 3.2 Other possibilities for reading files

To read all local files ending with .min within a directory (creates a single stream of all data):

        data = read(r'/path/to/file/*.min')

Getting magnetic data directly from an online source such as the WDC:

        data = read(r'ftp://thewellknownaddress/single_year/2011/fur2011.wdc')

Getting *kp* data from the GFZ Potsdam:

        data = read(r'http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab')

(Please note: data access and usage is subjected to the terms and conditions of the individual data provider. Please make sure to read them before accessing any of these products.)

No format specifications are required for reading. If MagPy can handle the format, it will be automatically recognized.

Getting data for a specific time window for local files:

        data = read(r'/path/to/files/*.min',starttime="2014-01-01", endtime="2014-05-01")

... and remote files:

        data = read(r'ftp://address/fur2013.wdc',starttime="2013-01-01", endtime="2013-02-01")

Reading data from the INTERMAGNET Webservice:

        data = read('https://imag-data-staging.bgs.ac.uk/GIN_V1/GINServices?request=GetData&observatoryIagaCode=WIC&dataStartDate=2021-03-10T00:00:00Z&dataEndDate=2021-03-11T23:59:59Z&Format=iaga2002&elements=&publicationState=adj-or-rep&samplesPerDay=minute')


### 3.3 Writing

After loading data from a file, we can save the data in the standard IAGA02 and IMAGCDF formats with the following commands.

To create an IAGA-02 format file, use:

        data.write(r'/path/to/diretory/',format_type='IAGA')

To create an [INTERMAGNET] CDF (ImagCDF) file:

        data.write(r'/path/to/diretory/',format_type='IMAGCDF')

The filename will be created automatically according to the defined format. By default, daily files are created and the date is added to the filename in-between the optional parameters `filenamebegins` and `filenameends`. If `filenameends` is missing, `.txt` is used as default.

To get an overview about possible write options use:

        help(DataStream().write)


### 3.4 Format-specific options

Some file formats contain multiple data sources and when writing certain archive formats, additional information will bve save in separate files. Below you will find descriptions for such format-specific pecularities.

#### IAF format

The IAF (INTERMAGNET archive format) contains 1-minute data along with filtered 1-hour data and daily averages. Typically components X,Y,Z and delta F (G) values are provided. Beside the geomagnetic components, the K indicies (3 hour resolution) are also contained within  the IAF structure.
When reading IAF data, by default only the 1-minute data is loaded. If you want to access other resolutions data or K values you can use the following "resolution" options (hour, day, k) while reading (please note: XMagPy only allows for reading minute data):

        data = read('/path/to/IAF/*.bin', resolution='hour')

When writing IAF data, you need to provide 1-minute geomagnetic data covering at least one month. Hourly means, daily averages and, if not provided, k values are automatically determined using IAGA/IM recommendations and saved within the IAF structure. You can however provide k values also using an independent data stream with such data:

        data.write('/path/to/export/IAF/', kvals=k_datastream)

Additionally a README.IMO file will be created and filled with existing meta information. If at least on year of 1-minute data is written, then also a DKA file will be created containing K values separatly. Please checkout INTERMAGNET format specifications for further details on DKA, README and IAF formats.   

#### IMF format

The IMF (INTERMAGNET format) is a seldom used ascii data file for one minute data products. The IMF format can be created from basically and data set in 1-minute resolution. Individual files cover one day. The data header of the IMF file contains an abbrevation of the geomagnetic information node GIN which by default is set to EDI (for Edinbourgh). To change that use the "gin" option.

        data.write('/path/to/export/IMF/', gin="GOL")

#### IMAGCDF format

The IMAGCDF format can contain several data sets from different instruments represented by different time columns. Typical examples are scalar data with lower sampling resolution as vector data and/or temperature data in lower resolution.
MagPy's IMAGCDF library will read all those data sets and, by default, will only use the most detailed time column which typically is GeomagneticVectorTimes. Low resolution data will refer to this new time column and "missing values" will be represented as NaN.
The select options allows you to specifically load lower resolution data like scalar or temperature readings.  

        data = read('/path/to/IMAGCDF/*.cdf', select='scalar')

When writing IMAGCDF files MagPy is using np.nan as fill value for missing data. You can change that by providing a different fill value using the option fillvalue:

        data.write('/path/to/export/IMAGCDF/', fillvalue=99999.0)

MagPy is generally exporting IMAGCDF version 1.2 data files. Additionally, MagPy is also supports flagging information to be added into the IMAGCDF structure (IMAGCDF version 1.3, work in progress):

        data.write('/path/to/export/IMAGCDF/', addflags=True)

Hint for XMagPy: When reading a IMAGCDF file with mutiple data contents of varying sampling rates the plots of the lower resolution data are apparently empty. Got to "Plot Options" on the Data panel and use "plottype" -> "continuous" to display graphs of low resolution data sets.  

### 3.5 Selecting timerange

The stream can be trimmed to a specific time interval after reading by applying the trim method, e.g. for a specific month:

        data = data.trim(starttime="2013-01-01", endtime="2013-02-01")



### 3.6 An overview of all format libraries

 library            | formats                      | since version | read/write | internal tests | requirements
 -----------------  |------------------------------|---------------|------------|----------------| ------------
 format_abs_magpy.py |                              |               |            |                |
 format_acecdf.py    |                              | 1.x           |            |                |
 format_autodif.py   |                              |               |            |                |
 format_autodif_fread.py |                              |               |            |                |
 format_basiccsv.py  | CSV                          | 2.0.0         | rw         | yes            |  csv 
 format_bdv.py       |                              | 1.x           |            |                |
 format_covjson.py   |                              |               |            |                |
 format_cr800.py     |                              | 1.x           |            |                |
 format_didd.py      |                              |               |            |                |
 format_dtu.py       |                              | 1.x           |            |                |
 format_gdas.py      |                              | 1.x           |            |                |
 format_gfz.py       |                              |               |            |                |
 format_gfztmp.py    |                              | 1.x           |            |                |
 format_gsm19.py     |                              |               |            |                |
 format_hapijson.py  |                              |               |            |                |
 format_iaga02.py    | IAGA                         | 2.0.0         | rw         | yes            | pyproj
 format_imagcdf.py   | IMAGCDF                      | 2.0.0         | rw         | yes            | pyproj, cdflib
 format_imf.py       | IAF, IMF, DKA, *BLV*, *IYFV* | 2.0.0         | rw         | yes            | pyproj
 format_iono.py      |                              |               |            |                |
 format_json.py      |                              |               |            |                |
 format_latex.py     |                              |               |            |                |
 format_lemi.py      |                              |               |            |                |
 format_magpy.py     | PYASCII, PYSTR, PYBIN        | 2.0.0         | rw,rw,r    | yes            | csv
 format_magpycdf.py  | PYCDF                        | 2.0.0         | rw         | yes            | cdflib
 format_nc.py        |                              | 1.x           | r          | defunc         | netcdf
 format_neic.py      | NEIC                         | 2.0.0         | r          | untested       |
 format_noaa.py      | NOAAACE                      | 2.0.0         | r          | no             |
 format_pha.py       |                              |               |            |                |
 format_pos1.py      |                              |               |            |                |
 format_predstorm.py |                              |               |            |                |
 format_qspin.py     |                              | 1.x           |            |                |
 format_rcs.py       |                              |               | r          | defunc         |
 format_sfs.py       |                              |               |            |                |
 format_simpletable.py |                              | 1.x           |            |                |
 format_tsf.py       |                              | 2.0.0         | r          | untested       |
 format_wdc.py       |                              |               | rw         |                |
 format_wic.py       |                              |               |            |                |
 format_wik.py       |                              | 1.x           |            |                |
 magpy_absformats.py |                              |               |            |                |
 magpy_formats.py    |                              |               |            |                |



## 4. Figures

You will find some example plots at the [Conrad Observatory](http://www.conrad-observatory.at).

### 4.1 A quick timersies plot

        import magpy.mpplot as mp
        mp.plot(data)

## 4.2 Some options

Select specific keys to plot:

        mp.plot(data,variables=['x','y','z'])

Defining a plot title and specific colors:

        mp.plot(data,variables=['x','y'],plottitle="Test plot",
                colorlist=['g', 'c'])

Reefining the y-axis range for the y colum between 0 and automatic maximum value (see `help(mp.plot)` for list and all options):

        mp.plot(data,variables=['x','y'],plottitle="Test plot",
                colorlist=['g', 'c'], specialdict = {'y':[0,]})

## 4.3 Data from multiple streams

Various datasets from multiple data streams will be plotted above one another. Provide a list of streams and an array of keys:

        mp.plotStreams([data1,data2],[['x','y','z'],['f']])

Please note that the gui is also using the plotstreams method and all options have to be provided as list.


## 5. Timeseries methods

### 5.1 Filtering

MagPy's `filter` uses the settings recommended by [IAGA]/[INTERMAGNET]. Ckeck `help(data.filter)` for further options and definitions of filter types and pass bands.

First, get the sampling rate before filtering in seconds:

        print("Sampling rate before [sec]:", cleandata.samplingrate())

Filter the data set with default parameters (`filter` automatically chooses the correct settings depending on the provided sanmpling rate):

        filtereddata = cleandata.filter()

Get sampling rate and filtered data after filtering (please note that all filter information is added to the data's meta information dictionary (data.header):

        print("Sampling rate after [sec]:", filtereddata.samplingrate())
        print("Filter and pass band:", filtereddata.header.get('DataSamplingFilter',''))

### 5.2 Coordinate transformation

Assuming vector data in columns [x,y,z] you can freely convert between xyz, hdz, and idf coordinates:

        cleandata = cleandata.xyz2hdz()

### 5.3 Calculate delta F

If the data file contains xyz (hdz, idf) data and an independently measured f value, you can calculate delta F between the two instruments using the following:

        cleandata = cleandata.delta_f()
        mp.plot(cleandata,plottitle='delta F')

### 5.4 Calculate Means

Mean values for certain data columns can be obtained using the `mean` method. The mean will only be calculated for data with the percentage of valid data (in contrast to missing data) points not falling below the value given by the percentage option (default 95). If too much data is missing, then no mean is calulated and the function returns NaN.

        print(cleandata.mean('df', percentage=80))

The median can be calculated by defining the `meanfunction` option:

        print(cleandata.mean('df', meanfunction='median'))

### 5.5 Applying offsets

Constant offsets can be added to individual columns using the `offset` method with a dictionary defining the MagPy stream column keys and the offset to be applied (datetime.timedelta object for time column, float for all others):

        offsetdata = cleandata.offset({'time':timedelta(seconds=0.19),'f':1.24})

### 5.6 Scaling data

Individual columns can also be multiplied by values provided in a dictionary:

        multdata = cleandata.multiply({'x':-1})

### 5.7 Fit functions

MagPy offers the possibility to fit functions to data using either polynomial functions or cubic splines (default):

        func = cleandata.fit(keys=['x','y','z'],knotstep=0.1)
        mp.plot(cleandata,variables=['x','y','z'],function=func)

### 5.8 Derivatives

Time derivatives, which are useful to identify outliers and sharp changes, are calculated as follows:

        diffdata = cleandata.differentiate(keys=['x','y','z'],put2keys = ['dx','dy','dz'])
        mp.plot(diffdata,variables=['dx','dy','dz'])


### 5.9 Merging streams

Merging data comprises combining two streams into one new stream. This includes adding a new column from another stream, filling gaps with data from another stream or replacing data from one column with data from another stream. The following example sketches the typical usage:

        print("Data columns in data2:", data2._get_key_headers())
        newstream = mergeStreams(data2,kvals,keys=['var1'])
        print("Data columns after merging:", data2._get_key_headers())
        mp.plot(newstream, ['x','y','z','var1'],symbollist=['-','-','-','z'])

If column `var1` does not existing in data2 (as above), then this column is added. If column `var1` had already existed, then missing data would be inserted from stream `kvals`. In order to replace any existing data, use option `mode='replace'`.

### 5.10 Differences between streams

Sometimes it is necessary to examine the differences between two data streams e.g. differences between the F values of two instruments running in parallel at an observatory. The method `subtractStreams` is provided for this analysis:

        diff = subtractStreams(data1,data2,keys=['f'])


### 5.11 All methods at a glance

For a summary of all supported methods, see the section **List of all MagPy methods** below.

## 6. Annotating data and flagging

Data flagging is handled by an additional package.

The flagging procedure allows the observer to mark specific data points or ranges. Falgs are useful for labelling data spikes, storm onsets, pulsations, disturbances, lightning strikes, etc. Each flag is asociated with a comment and a type number. The flagtype number ranges between 0 and 4:

  - 0:  normal data with comment (e.g. "Hello World")
  - 1:  data marked by automated analysis (e.g. spike)
  - 2:  data marked by observer as valid geomagnetic signature (e.g. storm onset, pulsation). Such data cannot be marked invalid by automated procedures
  - 3:  data marked by observer as invalid (e.g. lightning, magnetic disturbance)
  - 4:  merged data (e.g. data inserted from another source/instrument as defined in the comment)   

Flags can be stored along with the data set (requires CDF format output) or separately in a binary archive. These flags can then be applied to the raw data again, ascertaining perfect reproducibility.


### 6.1 Mark data spikes

Load a data record with data spikes:

        datawithspikes = read(example1)

Mark all spikes using the automated function `flag_outlier` with default options:

        flaggeddata = datawithspikes.flag_outlier(timerange=timedelta(minutes=1),threshold=3)

Show flagged data in a plot:

        mp.plot(flaggeddata,['f'],annotate=True)


### 6.2 Flag time range

Flag a certain time range:

        flaglist = flaggeddata.flag_range(keys=['f'], starttime='2012-08-02T04:33:40',
                                          endtime='2012-08-02T04:44:10',
                                          flagnum=3, text="iron metal near sensor")

Apply these flags to the data:

        flaggeddata = flaggeddata.flag(flaglist)

Show flagged data in a plot:

        mp.plot(flaggeddata,['f'],annotate=True)


### 6.3 Save flagged data

To save the data together with the list of flags to a CDF file:

        flaggeddata.write('/tmp/',filenamebegins='MyFlaggedExample_', format_type='PYCDF')

To check for correct save procedure, read and plot the new file:

        newdata = read("/tmp/MyFlaggedExample_*")
        mp.plot(newdata,annotate=True, plottitle='Reloaded flagged CDF data')


### 6.4 Save flags separately

To save the list of flags seperately from the data in a pickled binary file:

        fullflaglist = flaggeddata.extractflags()
        saveflags(fullflaglist,"/tmp/MyFlagList.pkl"))

These flags can be loaded in and then reapplied to the data set:

        data = read(example1)
        flaglist = loadflags("/tmp/MyFlagList.pkl")
        data = data.flag(flaglist)
        mp.plot(data,annotate=True, plottitle='Raw data with flags from file')

### 6.5 Drop flagged data

For some analyses it is necessary to use "clean" data, which can be produced by dropping data flagged as invalid (e.g. spikes). By default, the following method removes all data marked with flagtype numbers 1 and 3.

        cleandata = flaggeddata.remove_flagged()
        mp.plot(cleandata, ['f'], plottitle='Flagged data dropped')


## 7. DI-flux measurements, basevalues and baselines

The first sections will give you a quick overview about the application of methods related to DI-Flux analysis, determination und usage of baseline values (basevalues), and adopted baselines. The theoretical background and details on these application are found in section 2.11.7. These procedures require an additional import:

        from magpy import absolutes as di

### 7.1 Data structure of DI measurements

Please check `example3`, which is an example DI file. You can create these DI files by using the input sheet from xmagpy or the online input sheet provided by the Conrad Observatory. If you want to use this service, please contact the Observatory staff. Also supported are DI-files from the AUTODIF.

### 7.2 Reading DI data

Reading and analyzing DI data requires valid DI file(s). For correct analysis, variometer data and scalar field information needs to be provided as well. Checkout `help(di.absoluteAnalysis)` for all options. The analytical procedures are outlined in detail in section 2.11.7. A typical analysis looks like:

        diresult = di.absoluteAnalysis('/path/to/DI/','path/to/vario/','path/to/scalar/')

Path to DI can either point to a single file, a directory or even use wildcards to select data from a specific observatory/pillar. Using the examples provided along with MagPy, an analysis can be performed as follows. Firstly we copy the files to a temporary folder and we need to rename the basevalue file. Date and time need to be part of the filename. For the following commands to work you need to be within the examples directory.

        $ mkdir /tmp/DI
        $ cp example6a.txt /tmp/DI/2018-08-29_07-16-00_A2_WIC.txt
        $ cp example5.sec /tmp/DI/

The we start python and import necessary packages

        >>>from magpy import absolutes as di
        >>>import magpy.mpplot as mp
        >>>from magpy.stream import read

Finally we issue the analysis command.

        >>>diresult = di.absoluteAnalysis('/tmp/DI/2018-08-29_07-16-00_A2_WIC.txt','/tmp/DI/*.sec','/tmp/DI/*.sec')


Calling this method will provide terminal output as follows and a stream object `diresult` which can be used for further analyses.

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

Fext indicates that F values have been used from a separate file and not provided along with DI data. Delta values for F, D, and I have not been provided either. `diresult` is a stream object containing average D, I and F values, the collimation angles, scale factors and the base values for the selected variometer, beside some additional meta information provided in the data input form.


### 7.3 Reading BLV files

Basevalues:

        blvdata = read('/path/myfile.blv')
        mp.plot(blvdata, symbollist=['o','o','o'])

Adopted baseline:

        bldata = read('/path/myfile.blv',mode='adopted')
        mp.plot(bldata)

### 7.4 Basevalues and baselines

Basevalues as obtained in (2.11.2) or (2.11.3) are stored in a normal data stream object, therefore all analysis methods outlined above can be applied to this data. The `diresult` object contains D, I, and F values for each measurement in columns x,y,z. Basevalues for H, D and Z related to the selected variometer are stored in columns dx,dy,dz. In `example4`, you will find some more DI analysis results. To plot these basevalues we can use the following plot command, where we specify the columns, filled circles as plotsymbols and also define a minimum spread of each y-axis of +/- 5 nT for H and Z, +/- 0.05 deg for D.

        basevalues = read(example3)
        mp.plot(basevalues, variables=['dx','dy','dz'], symbollist=['o','o','o'], padding=[5,0.05,5])

Fitting a baseline can be easily accomplished with the `fit` method. First we test a linear fit to the data by fitting a polynomial function with degree 1.

        func = basevalues.fit(['dx','dy','dz'],fitfunc='poly', fitdegree=1)
        mp.plot(basevalues, variables=['dx','dy','dz'], symbollist=['o','o','o'], padding=[5,0.05,5], function=func)

We then fit a spline function using 3 knotsteps over the timerange (the knotstep option is always related to the given timerange).

        func = basevalues.fit(['dx','dy','dz'],fitfunc='spline', knotstep=0.33)
        mp.plot(basevalues, variables=['dx','dy','dz'], symbollist=['o','o','o'], padding=[5,0.05,5], function=func)

Hint: a good estimate on the necessary fit complexity can be obtained by looking at delta F values. If delta F is mostly constant, then the baseline should also not be very complex.


### 7.5 Applying baselines


The baseline method provides a number of options to assist the observer in determining baseline corrections and realted issues. The basic building block of the baseline method is the fit function as discussed above. Lets first load raw vectorial geomagnetic data, the absevalues of which are contained in above example:

        rawdata = read(example5)

Now we can apply the basevalue information and the spline function as tested above:

        func = rawdata.baseline(basevalues, extradays=0, fitfunc='spline',
                                knotstep=0.33,startabs='2015-09-01',endabs='2016-01-22')

The `baseline` method will determine and return a fit function between the two given timeranges based on the provided basevalue data `blvdata`. The option `extradays` allows for adding days before and after start/endtime for which the baseline function will be extrapolated. This option is useful for providing quasi-definitive data. When applying this method, a number of new meta-information attributes will be added, containing basevalues and all functional parameters to describe the baseline. Thus, the stream object still contains uncorrected raw data, but all baseline correction information is now contained within its meta data. To apply baseline correction you can use the `bc` method:

        corrdata = rawdata.bc()


Pease note that MagPy by defaults expects basevalues for HDZ (see example3.txt). When applying these basevalues the D-base value is automatically converted to nT and applied to your variation data. Alternatively you can also use MaPy basevalue files with XYZ basevalues. In order to apply such data correctly, the column names need to contain the correct names, i.e. X-base, Y-base, Z-base instead of H-base, D-base and Z-base (as in example3.txt).


If baseline jumps/breaks are necessary due to missing data, you can call the baseline function for each independent segment and combine the resulting baseline functions to  a list:

        stream = read(mydata,starttime='2016-01-01',endtime='2016-03-01')
        basevalues = read(mybasevalues)
        adoptedbasefunc = []
        adoptedbasefunc.append(stream.baseline(basevalues, extradays=0, fitfunc='poly', fitdegree=1,startabs='2016-01-01',endabs='2016-02-01')
        adoptedbasefunc.append(stream.baseline(basevalues, extradays=0, fitfunc='spline', knotstep=0.33,startabs='2016-01-02',endabs='2016-01-03')

        corr = stream.bc()

The combined baseline can be plotted accordingly. Extend the function parameters with each additional segment.

        mp.plot(basevalues, variables=['dx','dy','dz'], symbollist=['o','o','o'], padding=[5,0.05,5], function=adoptedbasefunc)

Adding a baseline for scalar data, which is determined from the delta F values provided within the basevalue data stream:

        scalarbasefunc = []
        scalarbasefunc.append(basevalues.baseline(basevalues, keys=['df'], extradays=0, fitfunc='poly', fitdegree=1,startabs='2016-01-01',endabs='2016-03-01'))
        plotfunc = adoptedbasefunc
        plotfunc.extend(scalarbasefunc)
        mp.plot(basevalues, variables=['dx','dy','dz','df'], symbollist=['o','o','o','o'], padding=[5,0.05,5,5], function=plotfunc)

Getting dailymeans and correction for scalar baseline can be acomplished by:

        meanstream = stream.dailymeans()
        meanstream = meanstream.func2stream(scalarbasefunc,mode='sub',keys=['f'],fkeys=['df'])
        meanstream = meanstream.delta_f()

Please note that here the function originally determined from the deltaF (df) values of the basevalue data needs to be applied to the F column (f) from the data stream. Before saving we will also extract the baseline parameters from the meta information, which is automatically generated by the `baseline` method.

        absinfo = stream.header.get('DataAbsInfo','')
        fabsinfo = basevalues.header.get('DataAbsInfo','')


### 7.6 Saving basevalue and baseline information

The following will create a BLV file:

        basevalues.write('/my/path', coverage='all', format_type='BLV', diff=meanstream, year='2016', absinfo=absinfo, deltaF=fabsinfo)

Information on the adopted baselines will be extracted from option `absinfo`. If several functions are provided, baseline jumps will be automatically inserted into the BLV data file. The output of adopted scalar baselines is configured by option `deltaF`. If a number is provided, this value is assumed to represent the adopted scalar baseline. If either 'mean' or 'median' are given (e.g. `deltaF='mean'`), then the mean/median value of all delta F values in the `basevalues` stream is used, requiring that such data is contained. Providing functional parameters as stored in a `DataAbsInfo` meta information field, as shown above, will calculate and use the scalar baseline function. The `meanstream` stream contains daily averages of delta F values between variometer and F measurements and the baseline adoption data in the meta-information. You can, however, provide all this information manually as well. The typical way to obtain such a `meanstream` is sketched above.

 ### 7.7 Details on DI-flux analysis and calculation of basevalues

Basevalues, often also referred to as **(component) baseline values**, are commonly obtained from DI-flux measurements, which are analyzed in combination with an independent fluxgate variometer. 
Dependent on the DI-flux measurement technique, the variometer orientation and the source of also required scalar data varying analysis procedures have been suggested. In the following we outline the analysis technique of MagPy specifically related to different orientations and measurement techniques.
The following terms are used throughout the methodological description and MagPy's interfaces. Fluxgate variometers are most commonly oriented either along a magnetic coordinate system, hereinafter denoted as **HEZ** (sometimes HDZ), or a geographic coordinate system **XYZ**. 
Within the  magnetic coordinate system, the orthogonal fluxgate triple of variometers is oriented in a way, that the north component points towards magnetic north (H component), the east component E towards magnetic east and vertical points down. For geographic orientation Z is identically pointing down, X towards geographic north and Y towards geographic east. For other orientations please refer to the [IM technical manual](https://intermagnet.github.io/docs/Technical-Manual/technical_manual.pdf).

#### 7.7.1 Theory of DI-analysis and basevalue calculation

For describing the mathematical methodology we apply a similar notation as used within the [IM technical manual](https://intermagnet.github.io/docs/Technical-Manual/technical_manual.pdf). Lets start with the following setup. The variometer used for evaluating the DI-flux measurement is oriented along a magnetic coordinate system (Figure XX). The actually measured components of the variometer are denoted N, E and V (North, East Vertical close to magnetic coordinate system). Each component consists of the following elements: 

$$N = N_{base} + N_{bias} + N_{var}$$

where $N_{var}$ is the measured variation, $N_{bias}$ contains the fluxgates bias field, and $N_{base}$ the components basevalue.
Some instruments measure the quasi-absolute field variation, which would correspond to 

$$N_{v} = N_{bias} + N_{var}$$

and thus the basevalues $N_{base}$ are typically small. This approach, making use of constant bias fields as provided within the LEMI025 binary data output is used for example at the Conrad Observatory. Another commonly used analysis approach combines bias fields and actual baseline values to  

$$N_{b} = N_{bias} + N_{base}$$

wherefore the hereby used $N_{b}$ are large in comparison to the measured variations $N_{var}$. All components are dependent on time. Bias field and basevalues, however, can be assumed to stay constant throughout the DI-flux measurement. Therefore, both approaches outlined above are equally effective. Hereinafter, we always assume variation measurements close to the total field value and for all field measurements within one DI-flux analysis we can describe north and vertical components as follows:

$$N(t_i) = N_{base} + N_{v}(t_i)$$

$$V(t_i) = V_{base} + V_{v}(t_i)$$
 
For the east component in an HEZ oriented instrument bias fields are usually set to zero. Thus $E$ simplifies to $E = E_{base} + E_{var}$. If the instrument is properly aligned along magnetic coordinates is simplifies further to 

$$E(t_i) = E_{var}(t_i)$$

as $E_{base}$ gets negligible (?? is that true??). The correct geomagnetic field components H, D and Z at time t for a HEZ oriented variometer can thus be calculated using the following formula (see also [IM technical manual](https://intermagnet.github.io/docs/Technical-Manual/technical_manual.pdf)):

$$H(t) =  \sqrt{(N_{base} + N_{v}(t))^2 + E_{var}(t)^2}$$

$$D(t) =  D_{base} + arctan(\frac{E_{var}(t)}{N_{base} + N_{v}(t)}$$

$$Z(t) =  V_{base} + V_{v}(t)$$

In turn, basevalues can be determined from the DI-Flux measurement as follows:

$$N_{base} =  \sqrt{(H(t_i))^2 – E_{var}(t_i)2} - N_{v}(t_i)$$

$$D_{base} =  D(t_i) - arctan(\frac{E_{var}(t_i)}{N_{base} + N_{v}(t_i)}$$

$$V_{base} =  Z(t_i) – V_{v}(t_i)$$

where $H(t_i)$, $D(t_i)$ and $Z(t_i)$ are determined from the DI-Flux measurement providing declination $D(t_i)$ and inclination $I(t_i)$, in combination with an absolute scalar value obtained either on the same pier prior or after the DI-Flux measurement $(F(t_j))$, or from continuous measurements on a different pier.  As variometer measurements and eventually scalar data are obtained on different piers, pier differences also need to be considered. Such pier differences are denoted by $\delta D_v$, $\delta I_v$ and $\delta F_s$.
 
The measurement procedure of the DI-flux technique requires magnetic east-west orientation of the optical axis of the theodolite. This is achieved by turning the theodolite so that the fluxgate reading shows zero (zero field method). Alternatively, small residual readings of the mounted fluxgate probe $(E_{res})$ can be considered (residual method). 

#### 7.7.2 Iterative application in MagPy

MagPy’s DI-flux analysis scheme for HEZ oriented variometers follows almost exactly the DTU scheme (citation , Juergen), using an iterative application. Basically, the analysis makes use of two main blocks. The first block (method *calcdec*) analyses the horizontal DI flux measurements, the second block (*calcinc*) analyses the inclination related steps of the DI-flux technique. 
The first block determines declination $D(t)$ and $D_{base}$ by considering optional measurements of residuals and pier differences:

$$D_{base} =  D(t_i) - arctan(\frac{E_{var}(t_i)}{N_{base} + N_{v}(t_i)} + arcsin(\frac{E_{res}(t_i)}{sqrt{(N_{base} + N_{v}(t_i))^2 + E_{var}(t_i)2}} + \delta D_v$$

If residuals are zero, the residual term will also be zero and the resulting base values analysis is identical to a zero field technique. Initially, $N_{base}$ is unknown. Therefore, $N_{base}$ will either be set to zero or optionally provided annual mean values will be used as a starting criteria. It should be said that the choice is not really important as the iterative technique will provide suitable estimates already during the next call. A valid input for $H(t)$ is also required to correctly determine collimation data of the horizontal plane.
The second block will determine inclination $I(t)$ as well as $H(t) = F(t) cos(I(t))$ and $Z(t) = F(t) sin(I(t))$. It will further determine $H_{base}$ and $Z_{base}$. Of significant importance hereby is a valid evaluation of F for each DI-Flux measurement. 

$$F(t_i) = F_m + (N_v(t_i) – N_m) cos(I) + (V_v(t_i)-V_m) sin(I) + (E_v(t_i)^2-E_m^2) / (2 F_m)$$

where $F_m$ is the mean F value during certain time window, and $N_m$, $V_m$, $E_m$ are means of the variation measurement in the same time window. Thus $F(t_i)$ will contain variation corrected F values for each cycle of the DI-flux measurement.
Based on these F values the angular correction related to residuals can be determined

$$I_{res}(t_i) =  arcsin(\frac{E_{res}(t_i)}{F(t_i)}$$

and finally, considering any provided $\delta I$ the DI-flux inclination value.
H(t) and Z(t) are calculated using the resulting inclination by

$$H(t) = F(t) cos(I)$$

$$Z(t) = F(t) sin(I)$$

and basevalues are finally obtained using formulas given above.
As both evaluation blocks contain initially unkown parameters, which are however determined by the complementary block, the whole procedure is iteratively conducted until resulting parameters do not change any more in floating point resolution. Firstly, calcdec is conducted and afterwards calcinc. Then the results for $H$ and $H_{basis}$ are feed into calcdec when starting the next cycle. Usually not more than two cycles are necessary for obtaining final DI-flux parameters. Provision off starting parameters (i.e. annual means) is possible, but not necessary. By default, MagPy is running three analysis cycles.

#### 7.7.3 Scalar data source

Scalar data is essential for correctly determining basevalues. The user has basically three options to provide such data. Firstly, a scalar estimate can be taken from provided annual means (use option annualmeans=[21300,1700,44000] in method **absoluteAnalysis** (2.11.2), annual means have to be provided in XYZ, in nT). A correct determination of basevalues is not possible this way but at least a rough estimate can be obtained. If only such scalar source is provided then the F-description column in the resulting basevalue time series (diresults, see 2.11.2) will contain the input **Fannual**. 
If F data is continuously available on a different pier, you should feed that time series into the **absoluteAnalysis** call (or use the add scalar source option in XMagPy). Every MagPy supported data format or source can be used for this purpose. Such independent/external F data, denoted $F_{ext}$, requires however the knowledge of pier differences between the DI-flux pier and the scalar data (F) pier. If $F_{ext}$ is your only data source you need to provide pier differences $\delta F_s$ to **absoluteAnalysis** in nT using option deltaF. In XMagPy you have to open „Analysis Parameters“ on the DI panel and set „dideltaF“.  The F-description column in the resulting basevalue time series (diresults, see 2.11.2) will contain the input **Fext**. The provided $\delta F_s$ value will be included into **diresults**, both within the deltaF column and added to the description string **Fext**. 
If F data is measured at the same pier as used for the DI-flux measurement, usually either directly before or after the DI-flux series, this data should be added into the DI absolute file structure (see 2.11.1).  Variation data, covering the time range of F measurements and DI-Flux measurements is required to correctly analyze the measurement. If such F data is used **diresults** will contain the input **Fabs**. 
If $F_{abs}$ and $F_{ext}$ are both available during the analysis, then MagPy will use  $F_{abs}$ (F data from the DI-flux pier) for evaluating the DI-Flux measurement. It will also determine the pier difference 

$$\delta F_s  = F_{abs} – F_{ext}(uncorr)$$.

This pier difference will be included into diresults within the delta F column. The F-description column in **diresults** will contain **Fabs**.  Any additionally, manually provided delta F value will show up in this column as well (**Fabs_12.5**). For the standard output of the DI-flux analysis any manually provided delta F will have been applied to $F_{ext}(corr)$.  

#### 7.7.4 Using a geographically oriented variometer (XZY)

The above outlined basevalue determination method is rather stable against deviations from ideal variometer orientations. Thus, you can use the very same technique also to evaluate basevalues for XYZ oriented variometers as long as your sites’ declination is small. A rough number would be that angular deviations (declination) of 3 degrees will lead to differences below 0.1 nT in basevalues. The small differences are related to the fact that strictly speaking the above technique is only valid if the variometer is oriented perfectly along the current magnetic coordinate system.
MagPy (since version 1.1.3) also allows for evaluating XYZ variometer data by obtaining basevalues also in a XYZ representation. This technique requires accurate orientation of your variation instrument in geographic coordinates. Provided such precise orientation, the basic formula for obtaining basevalues get linear and simplifies to 

$$X_{base} =  X(t_i) – X_{v}(t_i)$$

$$Y_{base} =  Y(t_i) - Y_{v}(t_i)$$

$$Z_{base} =  Z(t_i) – Z_{v}(t_i)$$

By default, MagPy will always create basevalues in HDZ components, even if xyz variation data is provided. If you want basevalues in XYZ components you need to confirm manually that the provided variation data is geographically oriented when calling **absoluteAnalysis**. Use option **variometerorientation=”XYZ”** for this purpose.  

#### 7.7.5 Using other variometer orientation

If you want to use variometer data in any other orientation then the two discussed above, it is necessary rotate your data set into one of the supported coordinate systems. Such rotations can be performed using MagPy's **rotate** method. Please note, that is then also necessary to rotate your variometers raw data using the same angular parameters prior to baseline adoption.


### 7.8 Summary - General procedure for the baselineAnalysis method: 

- Reading variometer data from defined source (DB, file, URL)
- Convert coordinate representation to nT for all axis.
- If DB only: apply DB flaglist, get and apply all DB header info, apply DB delta values (timediff, offsets)
- Rotation or compensation option selected: headers bias/compensation fields are applied
- If DB and rotation: apply alpha and beta rotations from DB meta info
- manually provided offsets and rotation are applied
- flags are removed
- interpolate variometerdata using default DataStream.interpol
- Reading scalar data from defined source
- If DB: apply flaglist, header and deltas
- apply option offsets
- remove flags and interpolate
- add interpolated values of vario and scalar into DI structure (at corresponding time steps), here manually provided delta F's are considered
- If DB and not provided manually: extract pier differences for variometer from DB (dD and dI)
- Start of iterative basevalue calculation procedure (repeated 3 times)

## 8. Geomagnetic activity

Please import activity related functionality to enable the methods shown below:

        (jnmagpy)$ from magpy.core import activity as act

### 8.1 Determination of K indices

MagPy supports the FMI method for determination of K indices. This method is derived from the 
original C code  K_index.h by Lasse Hakkinen, Finnish Meteorological Institute.  
Details on the procedure can be found here: citation
We strongly recommend to supply minute data to this routine. Lower resolution data will throw an error
message. Higher resolution data will be filtered using IAGA recommendations. The supplied data set needs to cover 
at least three subsequent days of data. The first and last day of the sequence will not be analyzed.

The datas et need to contain X,Y and Z components of which X and Y are analyzed. You can use
MagPy's timeseries methods to transform your data sets accordingly if needed. 

A month of one minute data is provided in `example2`, which corresponds to an [INTERMAGNET] IAF archive file. Reading a file in this format will load one minute data by default. Accessing hourly data and other information is described below.

        data2 = read(example2)

        kvals = act.K_fmi(data2)

The output of the K_fmi method is a DataStream object which contains timesteps and K values associated with the 'var1' key. 

For plotting we provide x and y components of magnetic data as well as the Kvalue results. The additional options determine the appearance of the plot (limits, bar chart):

        p = tsplot([data2,kvals],keys=[['x','y'],['var1']], labelx=-0.08, symbols=[["-","-"],["k"]], title="K value plot", symbolcolor=[[0.5, 0.5, 0.5]], patch=patch, showpatch=[True,False], grid=True,height=2)

`'k'` in `symbols` refers to the second subplot (K), which will then be plotted as bars rather than the standard line (`'-'`).


### 8.2 Automated geomagnetic storm detection

Geomagnetic storm detection is supported by MagPy using two procedures based on wavelets and the Akaike Information Criterion (AIC) as outlined in detail in [Bailey and Leonhardt (2016)](https://earth-planets-space.springeropen.com/articles/10.1186/s40623-016-0477-2). A basic example of usage to find an SSC using a Discrete Wavelet Transform (DWT) is shown below:

        from magpy.stream import read
        from magpy.core import activity as act
        stormdata = read("LEMI025_2015-03-17.cdf")      # 1s variometer data
        stormdata = stormdata.smooth('x', window_len=25)
        detection, ssc_dict = act.seek_storm(stormdata, method="MODWT")
        print("Possible SSCs detected:", ssc_dict)

The method `seek_storm` will return two variables: `detection` is True if any detection was made, while `ssc_list` is a flagging type dictionary containing data on each detection. Note that this method alone can return a long list of possible SSCs (most incorrectly detected), particularly during active storm times. It is most useful when additional restrictions based on satellite solar wind data apply (currently only optimised for ACE data, e.g. from the NOAA website):

        satdata_ace_1m = read('20150317_ace_swepam_1m.txt')
        satdata_ace_5m = read('20150317_ace_epam_5m.txt')
        detection, ssc_dict = act.seek_storm(stormdata,
                    satdata_1m=satdata_ace_1m, satdata_5m=satdata_ace_5m,
                    method='MODWT', returnsat=True)
        print("Possible CMEs detected:", ssc_dict.select_flags(parameter='sensorid', values=['ACE'])
        print("Possible SSCs detected:", ssc_dict.select_flags(parameter='sensorid', values=['LEMI'])


### 8.3 Sq analysis

Identifying solar quiet (Sq) variations is a challenging subject of geomagnetic data analysis. The basic objective 
is finding/identifying a variation curve of geomagnetic data which is unaffected by active solar regions. Thus such
solar quiet curve can be subtracted from actually measured data to unambiguously identify solar activity influences
on the geomagnetic field. Such solar quiet curve is often also referred to a 'baseline'. Please be careful not to 
mix it with the observatory baseline as outlined in chapter 7. In the following we will use the term "sq-variation". 
Before discussing the actual methods currently provided by MagPy it is worthwhile to look briefly at some
influences on the geomagnetic records and there effective time ranges  in order to understand the complexity of separating such sq-variation curve.

The following sources and variations should affect the baseline/variation:
- long term secular variation and jerks (earth internal field)
- solar cycle variations (sun cycle)
- yearly/seasonal variations (position of current system, relative distance, angle)
- neutral atmospheric variation - should also be seasonal) 
- 27 day solar rotation cycle (recurrence of solar activity)
- daily variation (ionospheric current systems)
- lunar cycle with 12.4 hours
- solar activity (cme - days)
- solar activity (flare,spe - less than hours)
- pulsations (typically, less then hours)
- artifical/technical disturbances (mostly less than seconds to minutes):

Our general approach relies on a frequency separation. Higher frequencies are removed and lower frequencies define the Sq variation. This is a general 
feature of many sq-variation separation techniques and also forms the basis of our approach. For frequency separation we are decomposing the original signal
into "frequency" bands using an empirical mode decomposition technique (EMD). For this purpose we are using the python package [emd](https://emd.readthedocs.io/en/stable/index.html). Geomagnetic data is non-stationary, highly dynamic, and contains
non-sinusoidal contributions. In comparison to other Fourier-transform based decomposition techniques, which basically determine a set of sinusoidal basis functions, EMD is perfectly suited for such data sets and isolates a small number of temporally adaptive basis functions and derive dynamics in frequency and amplitude directly from them.
These adaptive basis functions are called Intrinsic Mode Functions (IMF's).

In section 8.3.2 we show how our Sq technique is working in detail. If you are interested its simple, single-command application then move to the next section 8.3.1. Please note that you need to feed at least 6 days of one-minute data to this method. Ideally you would choose much longer time ranges. In the following examples we are analyzing
three months of one-minute data.

>![IMPORTANT]
> Method requires one-minute data, at least 6 days, preferably 3 months to cover a number of solar rotation cycles

#### 8.3.1 Getting an Sq variation curve

The act.sqbase command will return an estimate of the Sq variation curve. Three methods are currently supported.
baseline_type='emd' will return a purly frequency filtered curve based on emperical-mode decomposition and recombining 
low-frequency contents. baseline_type='median' will analyze the median cyclicity within a solar rotation cycle and use this
estimate for Sq determination. baseline_type='joint' will use both apporaches, 'emd' for non-stormy times and 'median' for 
storm-times. Details in 8.3.2

        from magpy.core import activity as act
        data = read("path-to-one-minute-data")
        sqcurve = act.sqbase(data, components=['x','y'], baseline_type='joint')


#### 8.3.2 Combining empirical mode decomposition, cyclicity and disturbance analysis for Sq determination

##### Empirical mode decomposition

The emd python package is used to determine IMF's from any given input signal. For the following example we are analyzing 3 months of definitive h data containing
various different disturbances from weak geomagnetic storms. Each decomposition step, "sift" is removing complexity from the original data curve. 
The original data is show in the upper diagram of Figure ![8.1.](./magpy/doc/sqbase-emd.png "Emperical mode decomposition") Altogether 16 sifts were found containing decreasing complex signal contributions. 
Summing up all these IMF curves will exactly reconstruct the original data, another important feature of EMD.  
In order to get comparable amount of sifts with similar frequency contents for different data selections you will need to supply 131500 minutes, corresponding to 3 months of geomagnetic data.
This time range is good enough to cover essential periods affecting Sq-variation evolution below seasonal effects. Additionally it is quickly applicable. If you supply less data, the maximum amounts of sifts will be lower.
Nevertheless, individual low-order sifts will contain similar frequency contributions. 

##### Frequency and amplitude characteristics

In a next step we are 
specifically interested in the frequency content of each sift. For this purpose we apply a Hilbert-Huang-transform to analyse distributions of 
instantaneous frequencies, amplitudes and phases of each sift. Results for IMF-6 are shown in Figure ![8.2.](./magpy/doc/sqbase-imf6-characteristics.png "Characteristics of IMF-6 with 3h periodicity") IMF-6 is hereby marking a period of about 3h,
a range which is often used for the general baseline approximation (i.e. for K values).Its amplitude variation indicates a few time ranges containing "disturbed" data characterized by larger amplitude. The dashed line 
is related to the upper inner-quartile limit with a standard factor of 1.5 (i.e. Q3+f*IQR).

If you are interested in determination of Sq baselines based on a frequency related filtering you can stop here already. Recombining all IMF from IMF-6 onwards will correspond to
such frequency based filtering and provide a baseline very similar/almost identical to one used for K-value extraction.

Full application of this technique in MagPy is as follows:

        from magpy.core import activity as act
        data = read("path-to-one-minute-data")
        sqcurce = act.sqbase(data, components=['x','y'], baseline_type='imf')

##### Cyclicity based Sq variation

For this approach we assume that any Sq signal is fully contained within the periodic oscillations that are present in our IMF's.
In order to analyze these oscillations we follow the approach which is described [here](https://emd.readthedocs.io/en/stable/emd_tutorials/03_cycle_ananlysis/index.html) in detail. 
For each IMF we are examining cyclicity and distinguish between good and bad cycles. A good cycle is charcterized by 

a) A strictly positively increasing phase, 
b) A phase starting within phase_step of zero i.e. the lowest value of the instantaneous phase (IP) must be less than phase_step
c) A phase ending within phase_step of 2Pi the highest value of IP must be between 2Pi and 2pi-phase_step
d) A set of 4 unique control points (ascending zero, peak, descending zero & trough)

An example for IMF-9, which contains the most prominent diurnal signal is shown in Figure ![8.3.](./magpy/doc/sqbase-imf9-cycles.png) 
Cycles not satisfying above criteria are termed "bad" cycles and are masked from the Sq approximation.

Starting with IMF-6 (period 3h) we are then determining a median of the average linear waveforms of identified "good" cycles, by 
running a gliding window of +/- 13 cycles across the investigated timeseries. In order to fill remaining gaps and smooth transitions between individual median cycles, the median cycle IMF is fitted by a cubic spline function with
knots at each data point and using zero weights for non-existing data. The 13-cycle range is related to the dominating diurnal period, for which waveforms
of -13 days + current day + 13 days = 27 days are considered. 27 days correspond to the solar rotation period, containing recurrent solar effects. 
Median IMF-10 and IMF-11 curves are calculated for 13 cycles (covering 27 days for IMF-10). 
For IMF's above 12 (period exceeding 8 days) we are using a simply linear fit of available data, as the average approximated length is significantly below the cycle frequency.

We obtain a running median waveform considering oscillation of the individual IMF's from IMF-6 onwards. Hereby we also excluded HF signal contributions by limiting to 
IMF-6 and larger. The Sq baseline will be a sum of individual median oscillations signals identified within the decomposed signal. Unlike the frequency technique above, 
this method will likely better estimate Sq variations during disturbed periods affecting hours/days. During quiet periods, however, a frequency related method
is likely superior as such methods will remove any non-solar driven multi-day variation (i.e neutral atmosphere, see day2day variability in Haberle et al).

>![IMPORTANT]
> Correct application of the cyclicity analysis requires at least 1 month of one-minute data

Full application of the median Sq technique in MagPy is as follows:

        from magpy.core import activity as act
        data = read("path-to-one-minute-data")
        sqcurce = act.sqbase(data, components=['x','y'], baseline_type='median')


##### Considering solar disturbances affecting low frequencies

The problem of purly frequency based baseline separation is, that during disturbed time of the geomagnetic field 
also longer periods of the geomagnetic field are affected with large amplitudes. A CME for example will affect the 
geomagnetic field for hours to days and thus is not adequately considered using a simple frequency based Sq determination technique.
Low-frequency "periodicities" clearly affected by disturbed time ranges will still be contained and assumed to represent Sq variations.

For a many applications we are primarily interested in detecting significant features of the geomagnetic field of natural and artificial origin. 
CME effects and an optimal description of onset, amplitude and duration certainly belong to these features. Therefore
the "solar quiet" reference baseline, containing untested features should not be biased by features which we are interested in.  

To deal with such effects two approaches are used so far, at least to our knowledge, the method of [SuperMag](), which is not easily reproducible, and the [Haberle]() method. 
Please consider the publication of Veronika Haberele as this approach, comprises the reasoning behind the technique described here, although 
application, theory and methods of MagPy are different.

In principle we are using two characteristics of IMF's in order to identify clearly disturbed time ranges, for which a standard baseline approximation as shown above is not precise.
Firstly we are examining the amplitude variation of an IMF with periods just above the lower Sq period range. Hereby we assume that larger amplitudes are 
connected to disturbances related to solar effects, but still well above the period range of eventually undetected artificial disturbing sources. This is a perfectly valid 
assumption as shown in Fig. 4 (original signal with flags of CME from the CME database).
Based on a statistical standard procedure, we assume time ranges 
(plus-minus a period range) of any IMF-6 amplitude variation data exceeding the upper limit of the inner-quartile range by IQR*1.5 as being disturbed. This approach can be applied 
to any data set independent from location.

Secondly, we analyze the cyclicity of the diurnal signal, which is obviously the most prominent period, and also might be affected by solar effects on the ionospheric current system. For this 
purpose we are analyzing the phase signal of IMF-9 as shown above. Cycles not satisfying above mentioned criteria are termed "bad" cycles and are also removed from the frequency related Sq approximation.
In combination, these two methods will lead to an identification of time ranges for which a simple frequency based
Sq determination technique does presumably not hold. A joint Sq baseline will assume that the median baseline will represent Sq variations better during such disturbed periods. 

Thus, the joint procedure will determine gaps as described above. in a next step a weighting function will be determined with linear transitions between EMD Sq and median Sq curves. 
The weighting function for the median Sq curve is shown in Figure ![8.4.](./magpy/doc/sqbase-joint.png) The weighting function of the EMD Sq baseline corresponds to the inverse. 
The window length for the gradual shift from EMD to Median curve is arbitrarily chosen to 12 hours and can be changed by options. 
All three Sq curve approximations are shown in th lower plot.


Full application of the joint Sq technique in MagPy works as follows:

        from magpy.core import activity as act
        data = read("path-to-one-minute-data")
        sqcurce = act.sqbase(data, components=['x','y'], baseline_type='joint')


## 9. Additional methods and functions

### 9.1 Testing data validity before submissions to IM and IAGA

A common and important application used in the geomagnetism community is a general validity check of geomagnetic data to be submitted to the official data repositories [IAGA], WDC, or [INTERMAGNET]. Please note: this is currently under development and will be extended in the near future. A 'one-click' test method will be included in xmagpy in the future, checking:

A) Validity of data formats, e.g.:

        data = read('myiaffile.bin', debug=True)

B) Completeness of meta-information

C) Conformity of applied techniques to respective rules

D) Internal consistency of data

E) Optional: regional consistency


#### 2.7.5 Spectral Analysis and Noise

For analysis of the spectral content of data, MagPy provides two basic plotting methods. `plotPS` will calculate and display a power spectrum of the selected component. `plotSpectrogram` will plot a spectrogram of the time series. As usual, there are many options for plot window and processing parameters that can be accessed using the help method.

        data = read(example1)
        mp.plotPS(data,key='f')
        mp.plotSpectrogram(data,['f'])




### 2.9 The art of meta-information

Each data set is accompanied by a dictionary containing meta-information for this data. This dictionary is completely dynamic and can be filled freely, but there are a number of predefined fields that help the user provide essential meta-information as requested by [IAGA], [INTERMAGNET] and other data providers. All meta information is saved only to MagPy-specific archive formats PYCDF and PYSTR. All other export formats save only specific information as required by the projected format.

The current content of this dictionary can be accessed by:

        data = read(example1)
        print(data.header)

Information is added/changed by using:

        data.header['SensorName'] = 'FGE'

Individual information is obtained from the dictionary using standard key input:

        print(data.header.get('SensorName'))

If you want to have a more readable list of the header information, do:

        for key in data.header:
            print ("Key: {} \t Content: {}".format(key,data.header.get(key)))


#### 2.9.1 Conversion to ImagCDF - Adding meta-information

To convert data from [IAGA] or IAF formats to the new [INTERMAGNET] CDF format, you will usually need to add additional meta-information required for the new format. MagPy can assist you here, firstly by extracting and correctly adding already existing meta-information into newly defined fields, and secondly by informing you of which information needs to be added for producing the correct output format.

Example of IAGA02 to ImagCDF:

        mydata = read('IAGA02-file.min')
        mydata.write('/tmp',format_type='IMAGCDF')

The console output of the write command (see below) will tell you which information needs to be added (and how) in order to obtain correct ImagCDF files. Please note, MagPy will store the data in any case and will be able to read it again even if information is missing. Before submitting to a GIN, you need to make sure that the appropriate information is contained. Attributes that relate to publication of the data will not be checked at this point, and might be included later.

        >>>Writing IMAGCDF Format /tmp/wic_20150828_0000_PT1M_4.cdf
        >>>writeIMAGCDF: StandardLevel not defined - please specify by yourdata.header['DataStandardLevel'] = ['None','Partial','Full']
        >>>writeIMAGCDF: Found F column
        >>>writeIMAGCDF: given components are XYZF. Checking F column...
        >>>writeIMAGCDF: analyzed F column - values are apparently independend from vector components - using column name 'S'

Now add the missing information. Selecting 'Partial' will require additional information. You will get a 'reminder' if you forget this. Please check IMAGCDF instructions on specific codes:

        mydata.header['DataStandardLevel'] = 'Partial'
        mydata.header['DataPartialStandDesc'] = 'IMOS-01,IMOS-02,IMOS-03,IMOS-04,IMOS-05,IMOS-06,IMOS-11,IMOS-12,IMOS-13,IMOS-14,IMOS-15,IMOS-21,IMOS-22,IMOS-31,IMOS-41'


Similar reminders to fill out complete header information will be shown for other conversions like:

        mydata.write('/tmp',format_type='IAGA')
        mydata.write('/tmp',format_type='IMF')
        mydata.write('/tmp',format_type='IAF',coverage='month')
        mydata.write('/tmp',format_type='WDC')


#### 2.9.2 Providing location data

Providing location data usually requires information on the reference system (ellipsoid,...). By default MagPy assumes that these values are provided in WGS84/WGS84 reference system. In order to facilitate most easy referencing and conversions, MagPy supports [EPSG] codes for coordinates. If you provide the geodetic references as follows, and provided that the [proj4] Python package is available, MagPy will automatically convert location data to the requested output format (currently WGS84).

        mydata.header['DataAcquisitionLongitude'] = -34949.9
        mydata.header['DataAcquisitionLatitude'] = 310087.0
        mydata.header['DataLocationReference'] = 'GK M34, EPSG: 31253'

        >>>...
        >>>writeIMAGCDF: converting coordinates to epsg 4326
        >>>...

#### 2.9.3 Special meta-information fields

The meta-information fields can hold much more information than required by most output formats. This includes basevalue and baseline parameters, flagging details, detailed sensor information, serial numbers and much more. MagPy makes use of these possibilities. In order to save this meta-information along with your data set you can use MagPy internal archiving format, `PYCDF`, which can later be converted to any of the aforementioned output formats. You can even reconstruct a full data base. Any upcoming meta-information or output request can be easily added/modified without disrupting already existing data sets and the ability to read and analyse old data. This data format is also based on Nasa CDF. ASCII outputs are also supported by MagPy, of which the `PYSTR` format also contains all meta information and `PYASCII` is the most compact. Please consider that ASCII formats require a lot of memory, especially for one second and higher resolution data.


        mydata.write('/tmp',format_type='PYCDF',coverage='year')



### 2.10 Data transfer

MagPy contains a number of methods to simplify data transfer for observatory applications. Methods within the basic Python functionality can also be very useful. Using the implemented methods requires:

        from magpy import transfer as mt

#### 2.10.1 Downloads

Use the `read` method as outlined above. No additional imports are required.

#### 2.10.2 FTP upload

Files can also be uploaded to an FTP server:

        mt.ftpdatatransfer(localfile='/path/to/data.cdf',ftppath='/remote/directory/',myproxy='ftpaddress or address of proxy',port=21,login='user',passwd='passwd',logfile='/path/mylog.log')

The upload methods using FTP, SCP and GIN support logging. If the data file failed to upload correctly, the path is added to a log file and, when called again, upload of the file is retried. This option is useful for remote locations with unstable network connections.

#### 2.10.3 Secure communication protocol (SCP)

To transfer via SCP:

        mt.scptransfer('user@address:/remote/directory/','/path/to/data.cdf',passwd,timeout=60)

#### 2.10.4 Upload data to GIN

Use the following command:

        mt.ginupload('/path/to/data.cdf', ginuser, ginpasswd, ginaddress, faillog=True, stdout=True)

#### 2.10.5 Avoiding real-text passwords in scripts

In order to avoid using real-text password in scripts, MagPy comes along with a simple encryption routine.

        from magpy.opt import cred as mpcred

Credentials will be saved to a hidden file with encrypted passwords. To add information for data transfer to a machine called 'MyRemoteFTP' with an IP of 192.168.0.99:

        mpcred.cc('transfer', 'MyRemoteFTP', user='user', passwd='secure', address='192.168.0.99', port=21)

Extracting passwd information within your data transfer scripts:

        user = mpcred.lc('MyRemoteFTP', 'user')
        password = mpcred.lc('MyRemoteFTP','passwd')


##### Citations

To be added


### 2.12 Database support

MagPy supports database access and many methods for optimizing data treatment in connection with databases. Among many other benefits, using a database simplifies many typical procedures related to meta-information. Currently, MagPy supports [MySQL] databases. To use these features, you need to have MySQL installed on your system. In the following we provide a brief outline of how to set up and use this optional addition. Please note that a proper usage of the database requires sensor-specific information. In geomagnetism, it is common to combine data from different sensors into one file structure. In this case, such data needs to remain separate for database usage and is only combined when producing [IAGA]/[INTERMAGNET] definitive data. Furthermore, unique sensor information such as type and serial number is required.

        import magpy import database as mdb


#### 2.12.1 Setting up a MagPy database (using MySQL)

Open mysql (e.g. Linux: `mysql -u root -p mysql`) and create a new database. Replace `#DB-NAME` with your database name (e.g. `MyDB`). After creation, you will need to grant priviledges to this database to a user of your choice. Please refer to official MySQL documentations for details and further commands.

         mysql> CREATE DATABASE #DB-NAME;
         mysql> GRANT ALL PRIVILEGES ON #DB-NAME.* TO '#USERNAME'@'%' IDENTIFIED BY '#PASSWORD';


#### 2.12.2 Initializing a MagPy database

Connecting to a database using MagPy is done using following command:

        db = mdb.mysql.connect(host="localhost",user="#USERNAME",passwd="#PASSWORD",db="#DB-NAME")
        mdb.dbinit(db)

#### 2.12.3 Adding data to the database

Examples of useful meta-information:

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

All available meta-information will be added automatically to the relevant database tables. The SensorID scheme consists of three parts: instrument (GSM90), serial number (12345), and a revision number (0002) which might change in dependency of maintenance, calibration, etc. As you can see in the example above, we separate data from different instruments, which we recommend particularly for high resolution data, as frequency and noise characteristics of sensor types will differ.


#### 2.12.4 Reading data

To read data from an established database:

        data = mdb.readDB(db,'GSM90_12345_0002')

Options e.g. starttime='' and endtime='' are similar as for normal `read`.

#### 2.12.5 Meta data

An often used application of database connectivity with MagPy will be to apply meta-information stored in the database to data files before submission. The following command demostrates how to extract all missing meta-information from the database for the selected sensor and add it to the header dictionary of the data object.

        rawdata = read('/path/to/rawdata.bin')
        rawdata.header = mdb.dbfields2dict(db,'FGE_22222_0001')
        rawdata.write(..., format_type='IMAGCDF')


### 2.13 Monitoring scheduled scripts

Automated analysis can e easily accomplished by adding a series of MagPy commands into a script. A typical script could be:

        # read some data and get means
        data = read(example1)
        mean_f = data.mean('f')

        # import monitor method
        from magpy.opt import Analysismonitor
        analysisdict = Analysismonitor(logfile='/var/log/anamon.log')
        analysisdict = analysisdict.load()
        # check some arbitray threshold
        analysisdict.check({'data_threshold_f_GSM90': [mean_f,'>',20000]})

If provided criteria are invalid, then the logfile is changed accordingly. This method can assist you particularly in checking data actuality, data contents, data validity, upload success, etc. In combination with an independent monitoring tool like [Nagios], you can easily create mail/SMS notfications of such changes, in addition to monitoring processes, live times, disks etc. [MARCOS] comes along with some instructions on how to use Nagios/MagPy for data acquisition monitoring.

### 2.14 Data acquisition support

MagPy contains a couple of packages which can be used for data acquisition, collection and organization. These methods are primarily contained in two applications: [MARTAS] and [MARCOS]. MARTAS (Magpy Automated Realtime Acquisition System) supports communication with many common instruments (e.g. GSM, LEMI, POS1, FGE, and many non-magnetic instruments) and transfers serial port signals to [WAMP] (Web Application Messaging Protocol), which allows for real-time data access using e.g. WebSocket communication through the internet. MARCOS (Magpy's Automated Realtime Collection and Organistaion System) can access such real-time streams and also data from many other sources and supports the observer by storing, analyzing, archiving data, as well as monitoring all processes. Details on these two applications can be found elsewhere.


### 2.15 Graphical user interface

Many of the above mentioned methods are also available within the graphical user interface of MagPy.
To use this check the installation instructions for your operating system. You will find Video Tutorials online (to be added) describing its usage for specific analyses.


### 2.16 Current developments

#### 2.16.1 Exchange data objects with [ObsPy]

MagPy supports the exchange of data with ObsPy, the seismological toolbox. Data objects of both python packages are very similar. Note: ObsPy assumes regular spaced time intervals. Please be careful if this is not the case with your data. The example below shows a simple import routine, on how to read a seed file and plot a spectrogram (which you can identically obtain from ObsPy as well). Conversions to MagPy allow for vectorial analyses, and geomagnetic applications. Conversions to ObsPy are useful for effective high frequency analysis, requiring evenly spaced time intervals, and for exporting to seismological data formats.

        from obspy import read as obsread
        seeddata = obsread('/path/to/seedfile')
        magpydata = obspy2magpy(seeddata,keydict={'ObsPyColName': 'x'})
        mp.plotSpectrogram(magpydata,['x'])

Possible issues with MagPy and ObsPy on the same machine as obspy requires specific, eventually conflicting scipy/numpy packages:
If you observe such problems, consider installing ObsPy via APT

  https://github.com/obspy/obspy/wiki/Installation-on-Linux-via-Apt-Repository

Afterwards you can install magpy as described above.
Using essential python3 packages from apt is also useful, if dependency problems are observerd:

        sudo apt install python3-scipy, python3-matplotlib, python3-numpy


#### 2.16.2 Flagging in ImagCDF

        datawithspikes = read(example1)
        flaggeddata = datawithspikes.flag_outlier(keys=['f'],timerange=timedelta(minutes=1),threshold=3)
        mp.plot(flaggeddata,['f'],annotate=True)
        flaggeddata.write(tmpdir,format_type='IMAGCDF',addflags=True)

The `addflags` option denotes that flagging information will be added to the ImagCDF format. Please note that this is still under development and thus content and format specifications may change. So please use it only for test purposes and not for archiving. To read and view flagged ImagCDF data, just use the normal read command, and activate annotation for plotting.

        new = read('/tmp/cnb_20120802_000000_PT1S_1.cdf')
        mp.plot(new,['f'],annotate=True)


## 3. Predefined scripts

MagPy comes with a steadily increasing number of applications for various purposes. These applications can be run from some command prompt and allow to simplify/automize some commonly used applications of MagPy. All applications have the same syntax, consisting of the name of application and options. The option -h is available for all applications and provides an overview about purpose and options of the application:

        $> application -h


### 3.1 Running applications in Linux/MacOs

On Linux Systems all applications are added the bin directory and can be run directly from any command interface/terminal after installation of MagPy:

        $> application -h

### 3.2 Running applications in Windows

After installing MagPy/GeomagPy on Windows, three executables are found in the MagPy program folder. For running applications you have to start the MagPy "command prompt". In this terminal you will have to go to the Scripts directory:

        .../> cd Scripts

And here you now can run the application of your choice using the python environment:

        .../Scripts>python application -h


### 3.3 Applications

The available applications are briefly intruduced in the following. Please refer to "application -h" for all available options for each application.

#### 3.3.1 mpconvert

mpconvert converts bewteen data formats based on MagPy.
Typical applications are the conversion of binary data formats
to readable ASCII data sets or the conversion.

Typical applications include

a) Convert IAGA seconds to IMAGCDF and include obligatory meta information:

        mpconvert -r "/iagaseconds/wic201701*" -f IMAGCDF -c month -w "/tmp"
                     -m "DataStandardLevel:Full,IAGACode:WIC,DataReferences:myref"

b) Convert IMAGCDF seconds to IAF minute (using IAGA/IM filtering procedures):

        mpconvert -r "/imagcdf/wic_201701_000000_PT1S_4.cdf" -f IAF -i -w "/tmp"


mpconvert -r "/srv/products/data/magnetism/definitive/wic2017/ImagCDF/wic_201708_000000_PT1S_4.cdf" -f IAF -i -w "/tmp"

#### 3.3.2 addcred

Used to store encrypted credential information for automatic data transfer. So that sensitive information has not to be written in plain text in scripts or cron jobs.


a) Add information for ftp data transfer. This information is encrypted and can be accessed by referring to the shortcut "zamg".

        addcred -t transfer -c zamg -u max -p geheim
                  -a "ftp://ftp.remote.ac.at" -l 21

## 4. List of all MagPy methods

Please use the help method (section 2.3) for descriptions and return values.

| group | method | parameter |
| ----- | ------ | --------- |
| - | **findpath** | name, path |
| - | **_pickle_method** | method |
| - | **_unpickle_method** | func_name, obj, cls |
| stream | **__init__** | self, container=None, header={},ndarray=None |
| stream | **ext** | self, columnstructure |
| stream | **add** | self, datlst |
| stream | **length** | self |
| stream | **replace** | self, datlst |
| stream | **copy** | self |
| stream | **__str__** | self |
| stream | **__repr__** | self |
| stream | **__getitem__** | self, index |
| stream | **__len__** | self |
| stream | **clear_header** | self |
| stream | **extend** | self,datlst,header,ndarray |
| stream | **union** | self,column |
| stream | **removeduplicates** | self |
| stream | **start** | self, dateformt=None |
| stream | **end** | self, dateformt=None |
| stream | **findtime** | self,time,**kwargs |
| stream | **_find_t_limits** | self |
| stream | **_print_key_headers** | self |
| stream | **_get_key_headers** | self,**kwargs |
| stream | **_get_key_names** | self |
| stream | **dropempty** | self |
| stream | **fillempty** | self, ndarray, keylist |
| stream | **sorting** | self |
| stream | **_get_line** | self, key, value |
| stream | **_take_columns** | self, keys |
| stream | **_remove_lines** | self, key, value |
| stream | **_get_column** | self, key |
| stream | **_put_column** | self, column, key, **kwargs |
| stream | **_move_column** | self, key, put2key |
| stream | **_drop_column** | self,key |
| stream | **_clear_column** | self, key |
| stream | **_reduce_stream** | self, pointlimit=100000 |
| stream | **_remove_nancolumns** | self |
| stream | **_aic** | self, signal, k, debugmode=None |
| stream | **harmfit** | self,nt, val, fitdegree |
| stream | **_get_max** | self, key, returntime=False |
| stream | **_get_min** | self, key, returntime=False |
| stream | **amplitude** | self,key |
| stream | **_gf** | self, t, tau |
| stream | **_hf** | self, p, x |
| stream | **_residual_func** | self, func, y |
| stream | **_tau** | self, period, fac=0.83255461 |
| stream | **_convertstream** | self, coordinate, **kwargs |
| stream | **_delete** | self,index |
| stream | **_append** | self,stream |
| stream | **_det_trange** | self, period |
| stream | **_is_number** | self, s |
| stream | **_normalize** | self, column |
| stream | **_testtime** | self, time |
| stream | **_drop_nans** | self, key |
| stream | **_select_keys** | self, keys |
| stream | **_select_timerange** | self, starttime=None, endtime=None, maxidx=-1 |
| stream | **aic_calc** | self, key, **kwargs |
| stream | **baseline** | self, absolutedata, **kwargs |
| stream | **stream2dict** | self, keys=['dx','dy','dz'], dictkey='DataBaseValues' |
| stream | **dict2stream** | self,dictkey='DataBaseValues' |
| stream | **baselineAdvanced** | self, absdata, baselist, **kwargs |
| stream | **bc** | self, function=None, ctype=None, alpha=0.0,level='preliminary' |
| stream | **bindetector** | self,key,flagnum=1,keystoflag=['x'],sensorid=None,text=None,**kwargs |
| stream | **calc_f** | self, **kwargs |
| stream | **dailymeans** | self, keys=['x','y','z','f'], **kwargs |
| stream | **date_offset** | self, offset |
| stream | **delta_f** | self, **kwargs |
| stream | **f_from_df** | self, **kwargs |
| stream | **differentiate** | self, **kwargs |
| stream | **DWT_calc** | self,key='x',wavelet='db4',level=3,plot=False,outfile=None,
| stream | **eventlogger** | self, key, values, compare=None, stringvalues=None, addcomment=None, debugmode=None |
| stream | **extract** | self, key, value, compare=None, debugmode=None |
| stream | **extract2** | self, keys, get='>', func=None, debugmode=None |
| stream | **extrapolate** | self, start, end |
| stream | **filter** | self,**kwargs |
| stream | **fit** | self, keys, **kwargs |
| stream | **extractflags** | self |
| stream | **flagfast** | self,indexarray,flag, comment,keys=None |
| stream | **flag_range** | self, **kwargs |
| stream | **flag_outlier** | self, **kwargs |
| stream | **flag** | self, flaglist, removeduplicates=False, debug=False |
| stream | **flagliststats** | self,flaglist |
| stream | **flaglistclean** | self,flaglist |
| stream | **stream2flaglist** | self, userange=True, flagnumber=None, keystoflag=None, sensorid=None, comment=None |
| stream | **flaglistmod** | self, mode='select', flaglist=[], parameter='key', value=None, newvalue=None |
| stream | **flaglistadd** | self, flaglist, sensorid, keys, flagnumber, comment, startdate, enddate=None |
| stream | **flag_stream** | self, key, flag, comment, startdate, enddate=None, samplingrate=0., debug=False |
| stream | **simplebasevalue2stream** | self,basevalue,**kwargs |
| stream | **func2stream** | self,function,**kwargs |
| stream | **func_add** | self,function,**kwargs |
| stream | **func_subtract** | self,function,**kwargs |
| stream | **get_gaps** | self, **kwargs |
| stream | **get_rotationangle** | self, xcompensation=0,keys=['x','y','z'],**kwargs |
| stream | **get_sampling_period** | self |
| stream | **samplingrate** | self, **kwargs |
| stream | **integrate** | self, **kwargs |
| stream | **interpol** | self, keys, **kwargs |
| stream | **k_extend** | self, **kwargs |
| stream | **k_fmi** | self, **kwargs |
| stream | **linestruct2ndarray** | self |
| stream | **mean** | self, key, **kwargs |
| stream | **missingvalue** | self,v,window_len,threshold=0.9,fill='mean' |
| stream | **MODWT_calc** | self,key='x',wavelet='haar',level=1,plot=False,outfile=None |
| stream | **multiply** | self, factors, square=False |
| stream | **offset** | self, offsets, **kwargs |
| stream | **plot** | self, keys=None, debugmode=None, **kwargs |
| stream | **powerspectrum** | self, key, debugmode=None, outfile=None, fmt=None, axes=None, title=None,**kwargs |
| stream | **randomdrop** | self,percentage=None,fixed_indicies=None |
| stream | **remove** | self, starttime=None, endtime=None |
| stream | **remove_flagged** | self, **kwargs |
| stream | **remove_outlier** | self, **kwargs |
| stream | **resample** | self, keys, **kwargs |
| stream | **rotation** | self,**kwargs |
| stream | **scale_correction** | self, keys, scales, **kwargs |
| stream | **selectkeys** | self, keys, **kwargs |
| stream | **smooth** | self, keys=None, **kwargs |
| stream | **spectrogram** | self, keys, per_lap=0.9, wlen=None, log=False,
| stream | **steadyrise** | self, key, timewindow, **kwargs |
| stream | **stereoplot** | self, **kwargs |
| stream | **trim** | self, starttime=None, endtime=None, newway=False |
| stream | **variometercorrection** | self, variopath, thedate, **kwargs |
| stream | **_write_format** | self, format_type, filenamebegins, filenameends, coverage, dateformat,year |
| stream | **write** | self, filepath, compression=5, **kwargs |
| stream | **idf2xyz** | self,**kwargs |
| stream | **xyz2idf** | self,**kwargs |
| stream | **xyz2hdz** | self,**kwargs |
| stream | **hdz2xyz** | self,**kwargs |
| - | **coordinatetransform** | u,v,w,kind |
| - | **isNumber** | s |
| - | **find_nearest** | array,value |
| - | **ceil_dt** | dt,seconds |
| - | **read** | path_or_url=None, dataformat=None, headonly=False, **kwargs |
| - | **_read** | filename, dataformat=None, headonly=False, **kwargs |
| - | **saveflags** | mylist=None,path=None |
| - | **loadflags** | path=None,sensorid=None,begin=None, end=None |
| - | **joinStreams** | stream_a,stream_b, **kwargs |
| - | **appendStreams** | streamlist |
| - | **mergeStreams** | stream_a, stream_b, **kwargs |
| - | **dms2d** | dms |
| - | **find_offset** | stream1, stream2, guess_low=-60., guess_high=60. |
| - | **diffStreams** | stream_a, stream_b, **kwargs |
| - | **subtractStreams** | stream_a, stream_b, **kwargs |
| - | **stackStreams** | streamlist, **kwargs |
| - | **compareStreams** | stream_a, stream_b |
| - | **array2stream** | listofarrays, keystring,starttime=None,sr=None |
| - | **obspy2magpy** | opstream, keydict={} |
| - | **extractDateFromString** | datestring |
| - | **testTimeString** | time |
| - | **denormalize** | column, startvalue, endvalue |
| - | **find_nearest** | array, value |
| - | **maskNAN** | column |
| - | **nan_helper** | y |
| - | **nearestPow2** | x |
| - | **test_time** | time |
| - | **convertGeoCoordinate** | lon,lat,pro1,pro2 |
| mpplot | **ploteasy** | stream |
| mpplot | **plot_new** | stream,variables=[],specialdict={},errorbars=False,padding=0,noshow=False |
| mpplot | **plot** | stream,variables=[],specialdict={},errorbars=False,padding=0,noshow=False |
| mpplot | **plotStreams** | streamlist,variables,padding=None,specialdict={},errorbars=None |
| mpplot | **toggle_selector** | event |
| mpplot | **addFlag** | data, flagger, indeciestobeflagged, variables |
| mpplot | **plotFlag** | data,variables=None,figure=False |
| mpplot | **plotEMD** | stream,key,verbose=False,plottitle=None |
| mpplot | **plotNormStreams** | streamlist, key, normalize=True, normalizet=False |
| mpplot | **plotPS** | stream,key,debugmode=False,outfile=None,noshow=False |
| mpplot | **plotSatMag** | mag_stream,sat_stream,keys,outfile=None,plottype='discontinuous' |
| mpplot | **plotSpectrogram** | stream, keys, NFFT=1024, detrend=mlab.detrend_none |
| mpplot | **magpySpecgram** | x, NFFT=256, Fs=2, Fc=0, detrend=mlab.detrend_none |
| mpplot | **plotStereoplot** | stream,focus='all',colorlist = ['b','r','g','c','m','y','k'] |
| mpplot | **_plot** | data,savedpi=80,grid=True,gridcolor=gridcolor,noshow=False |
| mpplot | **_confinex** | ax, tmax, tmin, timeunit |
| mpplot | **_extract_data_for_PSD** | stream, key |
| database | **dbgetPier** | db,pierid, rp, value, maxdate=None, l=False, dic='DeltaDictionary' |
| database | **dbgetlines** | db, tablename, lines |
| database | **dbupdate** | db,tablename, keys, values, condition=None |
| database | **dbgetfloat** | db,tablename,sensorid,columnid,revision=None |
| database | **dbgetstring** | db,tablename,sensorid,columnid,revision=None |
| database | **dbupload** | db, path,stationid,**kwargs |
| database | **dbinit** | db |
| database | **dbdelete** | db,datainfoid,**kwargs |
| database | **dbdict2fields** | db,header_dict,**kwargs |
| database | **dbfields2dict** | db,datainfoid |
| database | **dbalter** | db |
| database | **dbselect** | db, element, table, condition=None, expert=None, debug=False |
| database | **dbcoordinates** | db, pier, epsgcode='epsg:4326' |
| database | **dbsensorinfo** | db,sensorid,sensorkeydict=None,sensorrevision = '0001' |
| database | **dbdatainfo** | db,sensorid,datakeydict=None,tablenum=None,defaultstation='WIC',updatedb=True |
| database | **writeDB** | db, datastream, tablename=None, StationID=None, mode='replace', revision=None, debug=False, **kwargs |
| database | **dbsetTimesinDataInfo** | db, tablename,colstr,unitstr |
| database | **dbupdateDataInfo** | db, tablename, header |
| database | **stream2db** | db, datastream, noheader=None, mode=None, tablename=None, **kwargs |
| database | **readDB** | db, table, starttime=None, endtime=None, sql=None |
| database | **db2stream** | db, sensorid=None, begin=None, end=None, tableext=None, sql=None |
| database | **diline2db** | db, dilinestruct, mode=None, **kwargs |
| database | **db2diline** | db,**kwargs |
| database | **applyDeltas** | db, stream |
| database | **getBaseline** | db,sensorid, date=None |
| database | **flaglist2db** | db,flaglist,mode=None,sensorid=None,modificationdate=None |
| database | **db2flaglist** | db,sensorid, begin=None, end=None, comment=None, flagnumber=-1, key=None, removeduplicates=False |
| database | **string2dict** | string |
| tranfer | **_checklogfile** | logfile |
| tranfer | **ftpdatatransfer** | **kwargs |
| tranfer | **_missingvals** | myproxy, port, login, passwd, logfile |
| tranfer | **scptransfer** | src,dest,passwd,**kwargs |
| tranfer | **ssh_remotefilelist** | remotepath, filepat, user, host, passwd |
| tranfer | **ginupload** | filename=None, user=None, password=None, url=None,**kwargs |
| tranfer | **ftpdirlist** | **kwargs |
| tranfer | **ftpremove** | **kwargs |
| tranfer | **ftpget** | ftpaddress,ftpname,ftppasswd,remotepath,localpath,identifier,port=None,**kwargs |


## 5. Appendix


### 5.1 Installation instructions for Python 2.7

The current version of magpy is still supporting python 2.7, although it is highly recommended to switch to python >= 3.6. Installation on python 2.7 is more complex, as some packages for graphical user interface and CDF support not as well supported. Please note: None of the addtional steps is necessary for python 3.x.

#### 1.4.1 Pre-installation work

Get a recent version of NasaCDF for your platform, enables CDF support for formats like ImagCDF.
Package details and files can be found at http://cdf.gsfc.nasa.gov/

On Linux such installation will look like (http://cdf.gsfc.nasa.gov/html/sw_and_docs.html)

        $ tar -zxvf cdf37_0-dist-all.tar.gz
        $ cd cdf37...
        $ make OS=linux ENV=gnu CURSES=yes FORTRAN=no UCOPTIONS=-O2 SHARED=yes all
        $ sudo make INSTALLDIR=/usr/local/cdf install


Install the following additional compilers before continuing (required for spacepy):
     Linux: install gcc
     MacOs: install gcc and gfortran

Install coordinate system transformation support:

        $ sudo apt-get install libproj-dev proj-data proj-bin


#### 1.4.2 Install MagPy and dependencies

On Linux this will look like:

        $ sudo apt-get install python-matplotlib python-scipy python-h5py cython python-pip  
        $ sudo apt-get install python-wxgtk3.0 # or python-wxgtk2.8 (Debian Stretch)  
        $ sudo apt-get install python-twisted  
        $ sudo pip install ffnet
        $ sudo pip install pyproj==1.9.5
        $ sudo pip install pyserial
        $ sudo pip install service_identity
        $ sudo pip install ownet
        $ sudo pip install spacepy
        $ sudo pip install geomagpy  

On Mac and Windows you need to download a python interpreter like [Anaconda] or [WinPython] and then install similar packages, particluarly the old wxpython 3.x.

   [magpy-git]: <https://github.com/geomagpy/magpy>
   [magpy_win]: <http://www.conrad-observatory.at>
   [epsg]: <https://www.epsg-registry.org/>
   [proj4]: <https://github.com/OSGeo/proj.4>
   [MySQL]: <https://www.mysql.com/>
   [INTERMAGNET]: <http://www.intermagnet.org>
   [IAGA]: <http://www.iaga-aiga.org/>
   [WAMP]: <http://wamp-proto.org/>
   [MARTAS]: <https://github.com/geomagpy/MARTAS>
   [MARCOS]: <https://github.com/geomagpy/MARCOS>
   [MacPorts]: <https://www.macports.org/>
   [Miniconda]: <https://docs.conda.io/en/latest/miniconda.html>
   [Anaconda]: <https://www.continuum.io/downloads>
   [Docker]: <https://www.docker.com/>
   [CDF]: <https://cdf.gsfc.nasa.gov/>
   [ObsPy]: <https://github.com/obspy/obspy>
   [Nagios]: <https://www.nagios.org/>
   [Telegram]: <https://t.me/geomagpy>
   [MagPy Windows installer]: <https://cobs.zamg.ac.at/data/index.php/en/downloads/category/1-magnetism>
   [An introduction to XMagPy]: <https://github.com/geomagpy/magpy/blob/master/magpy/doc/xmagpy-manual.pdf>
