# XMagPy

Version 2.0.0

Authors: Leonhardt, R. et al

### Table of Contents
1. [Introduction](#1-introduction)
2. [Installation](#2-installation)
3. [Main window and its menu bar](#3-main-window-and-its-menu-bar)
   1. [Layout and default configuration](#31-layout-and-default-configuration)
   2. [Reading and exporting data sets](#32-reading-and-exporting-data-sets)
   3. [Database](#33-database)
   4. [DI](#34-di-)
   5. [Memory](#35-memory)
   6. [Special](#36-specials)
   7. [Options](#37-options)
   8. [Help](#38-help)
4. [Panels](#4-panels)
   1. [The data panel](#41-the-data-panel)
   2. [The flag panel](#42-the-flag-panel)
   3. [The meta panel](#43-the-meta-panel)
   4. [The analysis panel](#44-the-analysis-panel)
   5. [The DI panel](#45-the-di-panel)
   6. [The report panel](#46-the-report-panel)
   7. [The monitor panel](#47-the-monitor-panel)
5. [Application recipies for geomagnetic observatory data analysis](#5-application-recipies-for-geomagnetic-observatory-data-analysis)
6. [Additional applications](#6-additional-applications)
7. [Appendix](#7-appendix)


## 1. Introduction

The analysis of geomagnetic data is a primary responsibility of approximately two hundred
observatories around the globe. All procedures, from data quality control to final data submission,
increasingly require more sophisticated and effective tools, especially when it comes to high
resolution records. MagPy, short for Geomagnetic Python, is a platform-independent, multi-purpose
software package to assist with geomagnetic data acquisition, storage, distribution and practically
all analysis procedures used in geomagnetic observatories. It supports all common data formats
used in the geomagnetic community, which include both instrument-specific and general purpose
types such as IAGA02, WDC, IMF, IAF, and ImagCDF. Direct access to data stored in online
sources using URLs is also supported, allowing the user to obtain data directly from the dedicated
World Data Centers. Routines for common analysis procedures covering quality control, filtering,
data series merging, baseline adoption, format conversion, and data submission are provided. Data
handling functions from basic import, treatment, and export up to complex automated real-time
analysis of geomagnetic data can also be easily implemented. MagPy can communicate with a
MySQL database, which further facilitates effective archiving of data and all necessary meta
information. MagPy is an open source project hosted on GitHub
(https://github.com/geomagpy/magpy) and is available on PyPi
(https://pypi.python.org/pypi/geomagpy).

The graphical user interface XMagPy provides easy access to most routines offered by the
underlying MagPy python package, which will hereinafter be referred to as the MagPy back-end. 
Version numbers of graphical user interface XMagPy and the back-end are identical. Before starting with 
XMagPy and using its functionality it is highly recommended to read the basic MagPy manual to be found 
[here](https://github.com/geomagpy/magpy. 

In the following we will focus solely on the front-end, the graphical user interface. For better
readability we are using a number of conventions. Any reference to menu items, submenus, panels
and buttons are printed in **bold** face. Code fragments are contained in separate boxes. Example files are part 
of the MagPy package. They can also be downloaded directly from the
github directory given above in folder magpy/examples/.

## 2. Installation

Linux and MacOs: 
1. install anaconda or miniconda
2. conda create -n xmagpy wxpython
    Will create a python 3.7.16 environment with working wxpython 4.0.4. 
3. go to this environment
    conda activate xmagpy
4. install required conda packages
    conda install numpy, scipy, matplotlib, pymysql
5. install required pip packages
    pip install cdflib, pyproj, emd, pypupsub


## 3. Main window and its menu bar

### 3.1 Layout and default configuration

The graphical user interface of MagPy is based on [WXPython](https://wxpython.org/).  

Figure 3.1: The graphical user interface of XMagPy consists of 4 standard sections and 1 optional part. 

The main program window is separated in several parts as shown in Figure 3. Figure 3.1a contains the main menu for 
data access and memory/multiple data operations. Within the main menu you will find drop down lists for file operations, 
database issues, DI analysis, memory operations, some extra stuff, options and the help menu. The main menu bar 
will be discussed in this section. The panels (Figure 3.1b) provide access to individual methods for data analysis, 
interpretation and manipulation of active data sets. An individual section is devoted to each panel in chapter 4. 
Timeseries plots are displayed within the plotting area (Figure 3.1c). Options for modifying the plots appearance are
discussed in section 3.7. Status information (Figure 3.1d) and current working states are found at the bottom of the
window. An optional statistics view (Figure 3.1e) can be  activated within the Analysis Panel (see section 4.1). 

### 3.2 Reading and exporting data sets

MagPy supports a large variety of different data sources and formats, which are typically used within the geomagnetic
community. When looking at the file menu, you see direct support for the following sources: file(s), webservices,
remote data sources and database. Databases such as MySQL and MariaDB are also supported. In order to access existing
data go to menu **File** in the main menu bar.

#### 3.2.1 Loading single or multiple files

Most users will access data files on a local or connected file system. To do this use **Open File** in the **File** menu.
This will open a standard file selection menu for your operating system. Locate the file you want to open. 
No further selections are necessary. If the format type is supported, MagPy will identify it and open the data. 
Multiple selections are possible. Currently supported file formats can be found in menu **Help**, **Read Formats...**.


#### 3.2.2 Accessing Webservices

Using **Open Webservice** in the **File** menu will gives you access to machine readable webservices of institutions
and organizations. Currently (01/2024) webservices of [INTERMAGNET](https://www.intermagnet.org), the [USGS] and 
[GeoSphere Austria](https://cobs.geosphere.at) are included for 
accessing geomagnetic variation data and also a few other data sources. Please check the corresponding data licenses 
of the data suppliers before using it. **Open Webservice** will open a selection dialog as displayed in figure 3.2.1

Figure 3.2.1: Webservice selection window. The data request url will be constructed from the given parameters.  

In future versions this list can be extended to other webservices as well. 

#### 3.2.2 Accessing general URLs

It is possible to open supported web based data from basically any web address. When suing **Open  URL** you can enter 
basically every web address and access available data products directly. A number of examples for various data sources
are listed in section 3 of the main MagPy manual. Pre-installed with XMagPy are a few examples for the Kp webservice 
of the [GFZ Potsdam], data access at the [WDC] and some satellite data from [ACE]. As shown in figure 3.2.2 you can 
change/modify bookmarks and add your favorite quick access list here.   

Figure 3.2.2: Input window for remote connections. The example accesses hourly wdc data from the world data center in 
Edinburgh via FTP. 


#### 3.2.3 Database access

If a MagPy based MySQL/MariaDB database is connected you can also access and data sources directly from the database. 
Please check the main MagPy manual and section 3.3 for details on how to setup such a database. Using a data base comes
with a number of benefits regarding data organization, quick access, flagging and meta information storage. When 
clicking on **Open database** a drop down combo box will give you access to data tables contained within the database. 
When opening you will further be asked to define a time range.  

Figure 3.2.3: Database table access.

### 3.2.4 Exporting data

You can save data by using the **Export data** method of the file menu. A selection window as shown in Figure 3.3.1 
is showing up. 

Figure 3.3.1: Export data window.

Within the export data window you can select the destination path. In dependency of the selected format, file name 
eventual prerequisites are constructed. Please note, that some file formats require specific meta information. 
A number of file formats also allow a more complex storage protocol. Please use the button **Export Option** within the 
**Export Data** dialog to few these additional option which are different for the respective export formats. 

As an example we will load variation data in one
second resolution, , : Just load for example your IAGA-2002 one second data for one month 
and then export it to ImagCDF. For correct outputs you have to add ImagCDF specific meta information before exporting as shown in 4.3. As a plot will be generated when opening one month of one second data, this will need some time. If you have a large amount of data to be converted it is advisable to use the MagPy back-end‘s read/write methods or the MagPy command line application mpconvert (see appendix 8.3.  

Please note that not all export formats make sense for all data types. Here it is assumed that the user knows about 
benefits and drawbacks of specific well known geomagnetic data formats. Details on these geomagnetic data formats and
their contents as well as meta information requirements can be found here:
https://www.intermagnet.org/data-donnee/formatdata-eng.php
There are also three MagPy specific output formats which can be used for archiving and storage. PYCDF is based on
NasaCDF and can hold basically any meta information you define. It is specifically designed to save flagging
information and adopted baselines along with raw data, to allow for a single file storage (and thus reconstruction)
of all analysis steps typically used in geomagnetic data analysis. PYSTR is a comma separated ascii text file which
supports all basic meta information. PYASCII is a semi-colon separated ASCIItext file which focuses on data.  


#### 3.2.5 Quit

Does what is says... will quit XMagPy.


### 3.3 Database

For connecting a MagPy database you use **Connect MySQL DB** from menu **Database**. 
This will open a dialog window to define host, name and credentials for the MySQL/MariaDB
database. Please note that this usage requires a existing and accessible MagPy database.

If you want to create a new MagPy database you can follow the steps below to establish one. Please
note that these instructions are valid for a Linux system and might be modified for other systems.

1. Install and configure MariaDB or MySQL if not already existing (please refer to related
instructions). Then run MySQL

       $ sudo mysql -u root -p mysql

2. Create a new empty database (in this example called mydb, please change).

       mysql> CREATE database mydb;

3. Create a new user and give him permission on this database

       mysql> GRANT ALL PRIVILEGES ON mydb.* to `myuser`@`%` IDENTIFIED BY `mypasswd`;
       mysql> FLUSH PRIVILEGES;

4. Initialize this database with a MagPy supported table structure. Go to menu **Database** and run **Initialize new database**

You can now connect to this database and use it. Simply test it by storing flagging information or
meta information into the database.

### 3.4 DI 

### 3.5 Memory

The main menu item **Memory** provides gives you access to previously opened/modified data sets and allows for some 
multiple data operations. The dialog as shown in figure 3.5.1 will give you access to all data sets and working states
in the memory. They are characterized by a unique ID which is constructed from data characteristics. Shown data columns
(keys) can be modified. On the right side you will see a number of operations:

**Plot** will activate and show the selected plot. If multiple plots are selected all of them will be shown above each 
other as long as similar time ranges are available. Selecting multiple plots will disable all analysis functions.

**Plot nested** will show similar data sets (similar timerange, similar keys) within a single plot.

**Merge** will add data from a secondary source into a primary source. If some data is already existing this will remain
untouched and only gaps will be filled by the merged secondary source. Typical place holders as used in IAGA files
(99999, 88888) will be treated as non-existing data. The time series will always be limited to the time range of the
primary source. You can only merge two data sets at once.

**Subtract** will subtract identical columns from two data sets. You can only subtract two data sets at once.

**Combine** will join two time series and fill non-existing columns. If data is already existing, they remain
untouched. 

### 3.6 Specials

TODO

### 3.7 Options

The **Options** menu provides access to two submenus for basic initialization parameters and global
DI analysis parameters. Within the basic initialization parameters you can specify the default 
observatory code, default paths for loading and saving data as well as default parameters for 
fitting. The special parameters **Apply baseline** affects the way how baseline corrections are performed.
By default XMagPy will not apply conduct a baseline correction after its calculation but just store the correction
function in the data sets header. The user has to actively apply this correction as described in section 
[5.2](#5-application-recipies-for-geomagnetic-observatory-data-analysis). You can however skip this step and apply the 
baseline correction directly. XMagPy might contain some *hidden* **experimental methods** which can be activated here 
as well. Please be careful as hidden methods are under current development and might not work correctly yet. The 
significance of the scanning time for MARTAS real time data connections is described in section 4.

The DI initialization parameter option is useful, if you want to use MagPy‘s DI input sheets, as you
can change the layout here (Figure xx). The text-edit provides the order of measurements as
shown in the input sheet: MU (mire up), MD (mire down), EU (east up), WD (west up)… provide
the current order. You can modify that for example by simply changing …, EU, WU, ... to …, WU,
EU, …. Please leave the rest unchanged. If you are not using repeated measurements for each
position, disable the repeated positions check-mark. If you are not using a residual method or do
not use scale value checks then disable scale value. MagPy is supporting different parameters in dependency of the 
observatory code. If you want to add an additional observatory firstly open the **basic parameter** dialog from **Options**.
Insert a new default observatory code there and save it. Then open the **DI parameter** dialog. The new station code is 
automatically selected. Change the desired parameters and save.

### 3.8 Help

General information about XMagPy and currently supported file formats for reading and writing can be found here.

## 4. Panels

### 4.1 The data panel

An overview about the data panel is shown in figure 4.1.1. On top of the data panel you will find two quick access
buttons, **Previous** and **Next**. When opening data files with dates in the filename, assuming multiple adjacent files
in the same directory, database sources or webservice/url data with time ranges, then these buttons will be activated
and you can quickly switch to the next or previous data with a similar coverage. At the moment this quick access buttons
are available if dates are characterized by numerical values. Thus, for some data files like geomagnetic IAF data these
options are NOT available. 

The source of the opened time series and its covered time range is shown below the quick access buttons within the
**Data information** field. You can modify start and end time by changing times or dates subsequently using the trim 
button. Please note: the trim button will cut down the original data set and create an new input in the memory. 

The field **Data selection** allows you to select specific key for displaying in the diagram (button **Select keys**). 
You can also modify the data set but deleting unused keys using **Drop keys**, which will lead to an new memory input.
**Extract values** gives you the possibility to remove larger or smaller values than a given threshold. Multiple 
criteria are possible. By default such data, including time stamp, is removed. If you want to keep timestamps and use
missing data place holders (np.nan by default) use the **Get Gaps** method afterwards, which will also then lead to a
discontinuous line plot.
You might want to change the **coordinate system** between cartesian, cylindrical and spherical coordinates if vector
data is provided. Select a line or point plot. More options are available using **Plotting options** in the **Options**
menu.

Finally, you can activate the **Continuous statistics** panel below the main frame. 

### 4.2 The flag panel

The flag panel contains methods for flagging data, as well as possibilities to store and load such information. Please 
note that flags are always connected to a specific SensorID which is related to and obtained from the data source. 
Every flag is defined by a number of parameters of which the flagtype defines whether data has to be removed for 
definitive analysis or not. A label and its labelid provides information about the flag reason. Observers can comment 
the flag and actual operator who issued the flag can be given. Further details are found in the general manual.
The main flagging functions are accessible by buttons: **Flag outlier** will scan the time series for outliers (4.2.1). 
**Flag selection** will mark currently selected data enlarged by zoom. To use this method you zoom into a specific plot 
area (see 3.3) and then press the **Flag selection** button. This is the most common method for regular data flagging. 
**Flag range** allows for defining either a value or a time range to be flagged. If you just want to flag maxima or minima
within the time series, you firstly select the component(s), label and flag ID, and then use the **Flag maximum** or 
**Flag minimum button**. Flags can either be saved within a connected data base (which I would recommend) or into a 
data file. The flag file supports two formats, pickle, a binary structure, or json, an ASCII structure, of which I 
recommend the latter. **Drop flagged** will remove flagged data with a 'remove data' flagtype and replace them by NaN.
**Clear flags** deletes all current flagging information, keeping all data unchanged. 

#### 4.2.1 Flag outlier

Figure 4.2.2: Flag outlier selection menu

The Flag outlier method makes use of interquartile ranges (IQR) to identify how spread-out values are within a 
certain time window, moving along the time series. Data outside the IQR range multiplied by a certain multiplier, also
referred to as threshold value, are identified as outliers. In statistics usually a multiplier of 1.5 is assumed. I 
recommend larger values here. You can change time range and multipliers in a selection window when applying this method
(Figure 4.2.2). Thresholds of 4 and window lengths of 30 to 60 seconds are typically good to identify lightning strikes
in one-second data. Thresholds of 5 and window lengths > 300 seconds are good enough to identify spikes and keep most
natural signals in. By default a time window of 600 times the sampling rate is assumed. If marking outliers in all
components is chosen then every outlier detected  in a single component is also marked in other components as well.
This is particularly useful for vectorial data. Outlier flags are „automatic flags“ and will be assigned to the 
operator "MagPy". The flag comment will contain threshold (muliplier) and time window length.

#### 4.2.2 Flag selection

Figure 4.2.3: Flag selection menu. 

In the Flag selection menu you can define the components (keys) to which this flag has to be applied. You can further
select a general flag type, characterized by an ID. This type can be either 

ID 0, normal data: 	just add a comment to the selected data
ID 1, automatic flag - remove: 	    should only be used by automatic processes like outlier detection
ID 2, automatic flag - keep data: 	data to be kept for definitive data production 
ID 3, observers flag - remove data:	don‘t use this data for definitive data production
ID 4, observers flag - keep: 		keep it

Flagtype 3 and 4 cannot be overwritten by automatic processes 1 and 2. The provided comment is free-form text, but 
don‘t use special characters here. 


#### 4.2.3 Flag range

Figure 4.2.4: Flag range menu allows you to flag data between given boundaries either in time or amplitude.

The Flag range method allows you to select specific boundary values. Data exceeding these boundaries are flagged. 
Initially you have to choose between amplitude (value) or time range. When selecting value, you provide the
component/key and threshold values.  If a time range is to be flagged, as shown in figure 4.2.4, you will have to
provide lower and upper date and time. If you zoomed into a graph, these values are prefilled by the boundaries of the
zooming window. 
For both selections you have to provide label, operator, comments, flagtype, and components/keys to be flagged as described in section 4.2.3. 

#### 4.2.4 Flagging extrema

For flagging either maximum or minimum values in specific diagrams you firstly need to select the component(s) which 
you want to flag. Before actually pressing the flag button you also should select the flag ID which should be 
connected to your extrema flag. Default is a “removal” flag. All field found on the main panel as indicated by
“Mark extrema” in figure 4.1.1. 


#### 4.2.5 Annotation and flagging info

By default 

The flags button on the analysis panel will open a flag statistics dialog (figure 4.4.5). In case that no flags are 
currently available, the status field (figure 3.1d) will tell you and nothing else will happen.
Figure 4.4.5: The flag dialog opened in the analysis panel.
The flag dialog will provide some basic information on the flags currently stored in the sensor related flagging object.
As shown in the example (flagging_example.json applied to example1) , 252 individual flags are currently included. 
The flags have all been created by automatic outlier detection (aof = automatic outlier flag), which is also expressed 
by the yellow flag annotation color. 252 flags were created using the outlier detection parameters as given in the 
comment. Within this dialog it is also possible to modify the flagging information. Please note that this method is 
preliminary and more sophisticated flagging tools will be available in a future version of MagPy. 
Figure 4.4.5: Modifying flags will open this dialog.
At present (MagPy1.0) you can change between different modification types (select, replace, delete), apply this type to either the flags key, comment, sensorid, or flagnumber. In above example we are deleting all flags for column key f.  


### 4.3 The meta panel

The meta data panel gives you an overview about all meta information connected to the currently active data set. This
meta information is usually obtained from the data file. If a database is connected, and you have information for
DataID, SensorID and/or StationID within your database, you can extend that information with database contents
(**Get from DB**). This is useful for exporting data e.g. when converting IAGA-2002 to ImagCDF. 

You can always modify or extend meta information when clicking on the buttons for data related, sensor related or 
station related meta information. 

As an example the input sheet for station related information is shown in figure 4.3.2. Here you will see several 
field which can be filled with your information. The field names are self explaining and will be automatically 
converted to obligatory field names when exporting specific formats like ImagCDF. Numbers in parenthesis behind some
field names indicate that this information has to be provided for the corresponding output
(1 → INTERMAGNET Archive Format IAF, 2 → IAGA 2002 format, 3→ ImagCDF).

Figure 4.3.2: Input sheet for station related information. 

### 4.4 The analysis panel

The analysis panel provides access to many methods which are useful for data checking and evaluation. In the following
you will find subsections for buttons and underlying methods on the analysis panel.

**Derivative** will calculate the derivative of all data columns diagrams shown in the plot. If you want the second
derivative, just press the button again. The derivative method is helpful to identify strong gradients within the
components, which can be related to spikes, SSC-sudden storm commencements and other artificial or natural reasons.

**Rotation**: When using the rotation button a window will open asking you for rotation angles alpha, beta, gamma. The
rotation function will use an Eigen rotation to rotate the vector (x,y,z) into a new coordinate system (u,v,w). Alpha
corresponds to a rotation in the horizontal plane, beta will rotate in the vertical plane. When only using alpha, 
a „horizontal“ rotation within the x-y plane is accomplished.

The **Fit** method will open an input dialog as shown in figure 4.4.1. The dialog window will ask you to define a fit
function, its parameters as well as a time range. When choosing spline as fit function, you are asked to provide a
relative measure of knotsteps for the spline. The provided number defines the position of knotsteps, relative to a
signal length of 1. 0.33 thus means that two knots at 33% and 66% of the time series are used. Lower numbers increase
the amount of knots. When choosing polynomial fits, the polynomial order needs to be provided. The higher the
polynomial order the more complex the fit. Further fit methods are linear least-square, which is equivalent to a
polynomial fit with order 1, and mean, which will calculate an arithmetic average and use this a horizontal linear fit.
Choosing none will remove all previously performed fits. You can add multiple fits to a single timeseries and you can 
apply fits to selected keys only. We will come
back to this option when discussing adopted baselines. You can save and load fit parameters from disk. The Save button
will save all applied fit parameters to a json file. The Load method will obtain such stored data and directly apply
the fitting parameters to the data set. When replacing a given “enddate” within the json file by “now”, then
datetime.utcnow() will be used whenever loading this file. 

Figure 4.4.3: Fitting data dialog window. 

The three methods **Maxima**, **Minima** and **Mean** will open an information dialog with the requested values. 
In case of mean also the standard deviation of these values for the shown data within the plotting window. You can
change the active window using the plots’ zoom method.

**Filter** will allow you apply typically geomagnetically recommended filters and also some others to your time series.
Principally, filtering is a combination of smoothing using a certain smoothing window e.g. gaussian and a resampling
of the smoothed time series at new intervals. The default filter parameters will be given in dependency to the sampling
resolution of the time series and will correspond to IAGA/INTERMAGNET recommended filter parameters.
The filter dialog allows you to select an appropriate filter window. Detailed descriptions of all possible filter 
windows can be found elsewhere e.g. python signal analysis. You further define a window length, a sampling interval in
seconds and eventually an offset for the window center. 0.0 as shown above will center the window on the minute.
Furthermore you have to select a preferred missing data treatment. By default, the IAGA recommended method will be
used, which means that filtering will be performed if more than 90% of data is available within the filtering window.
Please note the filter weight are always determined analytically and therefore missing data is correctly accounted for
when applying the filter window. Interpolation as missing data treatment will interpolate missing data if more than
90% of data is available. If less than 90% is present than this window will not be filtered. Conservative treatment
will not perform any filtering if already one data point is missing within the filter window.  

**Offset** allows you to define a certain offset in time or component for either the whole time series or the selected
(zoomed) time window.  Figure 4.4.7 shows an example of an offset within a time series and the offset dialog to correct
this offset. 

Figure 4.4.7: a) time series with an offset of 10 nT for almost 12 minutes. When using the offset button, the dialog window b) will open. Here you can provide offset values which are then applied to the time series.

**Smooth** data sets support the same smoothing windows as the filter method. In principle smoothing does exactly the 
same as the filter method without resampling the data set a new time stamps. Please check the filter function for
supported methods.

**Resample** method can be used to subsample a time series at specific periodic intervals. If no measurements are
available at the specific time stamp, than a linear extrapolation between surrounding points is used. 

**Delta F** is available if vector components (x,y,z) and an independent f column is present within the active data
set. When clicking on delta F, the vector sum (F[Vector]) is determined and subtracted from f (F[Scalar]).
Delta F = F[Vector] - F[Scalar]

**Calculate F** will calculate the vector sum of x,y,z components and store the result within the reserved f column of
the data object. If an independent f is already existing within this column, you will be asked whether you want to
overwrite the existing information. 

**Activity** will calculate the geomagnetic activity index k from the provided time series using the [FMI] method. The
activity button is only active if geomagnetic components covering at least 3 days are available. Currently, the
k index is solely calculated with the FMI method. Depending on the provided data eventually additional steps are
automatically performed. The FMI method basically works with minute data resolution. If you have second resolution
data, the filtering process is automatically performed using IAGA recommended filter options before k calculation.

4.4.12 Baseline
The baseline method is only available if a basevalue data set has been opened previously and is still present in the memory. When now opening a variation time series, which is covered by the time range of available basevalues, the baseline button gets enabled. When pressing the baseline button you will be asked to provide fitting parameters for adopted baselines. This baseline will be calculated and a baseline correction function of the the time series will  be obtained. This function will not be applied directly but stored within the time series meta information. You can apply the adopted baseline be pressing on the now available button „Baseline Corr(ection)“ on the data panel. Exporting this data set now as PYCDF will export the adopted baseline function along with the data meta info. This way, uncorrected and corrected data is not separated any more. Extensive details and a complete walkthrough with hands-on examples for this method is provided in section 6.2.

**Power** will plot the power spectral density based on the matplotlib psd method for the selected components. You can
zoom etc. If you want to return to the original plot access the **Memory**. Parameters for the PSD plot can be changed
using plot options.

**Spectrum** is disabled by default as this method is currently under development. You can activate a preliminary 
access in the options menu. Currently you can view power and spectral densities for individual components. Yet, it is
not yet possible to access any parameters, window sizes , colors etc. This will come in a future version on MagPy.  

### 4.5 The DI panel

The DI panel provides all necessary functions and methods to calculate basevalues and DI timeseries from basic/residual 
DI fluxgate measurements.
 
The upper part of the DI panel is dedicated to the definition of data sources. Firstly you need to provide access to 
DI measurements data (DI data). Such DI data can either be located on your file system, within a MagPy database, or 
provided by a web service. Supported data formats can be found in the basic MagPy manual. When clicking on DI data, 
an input dialog will open asking you for a specific data source.

In order to regard for geomagnetic variation throughout the DI measurement and obtaining basevalues you further 
require variation data and measurements of F. Scalar F measurements can either be provided along with the DI data set, 
as usually the case if you measure F before or after the DI measurements at the same pier, or you can provide access 
to a continuously measured F using vario/scalar associated by a delta F value to consider pier differences. All MagPy 
supported data formats are also supported for this analysis.  

Before analyzing the data set, you can modify/set analysis parameters. Such parameter include pier differences like 
delta F between continuously recording instruments and the DI pier, as well as threshold parameters etc. It is 
recommended to provide such parameters along with the meta information of the DI data.  If you want to export baseline
values in geographic XYZ coordinates please insert “XYZ” into the blvoutput field within the analysis parameter dialog.
Please note: this will only work if the provided variation data is also from a XYZ oriented instruments
and available in this coordinate system. 

When finally using the **Analyze** button, which will be enabled if sufficient information is provided, a DI analysis
will be performed. The results will be written to the logging window and can be saved as ASCII txt files by using the 
**Save Log** button. The plot panel will show the time series of resulting basevalues if available. 
By default the basevalues baseH, baseD and baseZ will be shown. An empty plot will be created if basevalues could not
be calculated i.e. no vaild variometer data is available. D, I and eventually F however will be available even then und
can be selected on the **Data** panel.
Explanations for the shown results and other parameters like collimation angles, as well as further details on the 
DI-flux evaluation method used by MagPy are summarized in the general MagPy README.md on github, chapter 7.

Before starting a new analysis you might want to clear the report box by using the **Clear Log** button.

### 4.6 The report panel

The report panel will provide a summary of all analysis steps which have been performed so far. This report might be
used for better transparency and the ability reproducing analysis routines. You can save the report messages as ASCII
txt file by using the **Save log** button.

### 4.7 The monitor panel

## 5. Application recipies for geomagnetic observatory data analysis

### 5.1 Daily review of data and flagging

### 5.2 DI analysis and adopted baselines

Here you will get a step by step example about analyzing DI measurement data, obtaining basevalues, getting adopted 
baselines and applying baseline corrections to variation data in order to produce adjusted, quasi-definitive and 
definitive data sets. For this recipe we will use example3.txt, example5.sec, as well as DI measurement data in 
example6a.txt and example6b.txt.

Lets start with the input sheet. In the Input sheet dialog we click on Open DI data 
and locate/open example6a.txt. Here you will have an example of a measurement using the residual method. It is 
recommended to use serial number for unique identification of theodolite and fluxgate. Also a self-defined revision 
number, for the theodolite might be useful to verify e.g. maintenance changes for fluxgate alignment and thus the
collimation angles.

MagPy DI data files usually contain only measurements directly connected to the DI pier. Thus theodolite measurements 
are contained as well as F data prior to or after the directional measurements on the same pier. In order to analyze 
such data and to calculate absolute values of declination D and inclination I for a specific time we will need to 
consider magnetic field variations throughout the DI measurement period. For our example data we need both, a
continuously recording variometer and also scalar data, as well as the pier differences between continuously recording
instruments and the DI pier.

In order to analyze DI data files we now go to the DI panel, and firstly load DI data files into the memory. You can
get DI data from files of which MagPy Di files, AutoDIF abs data and USGS DI json structures are supported. If files
are accessed, like example61 and example6b of the MagPy package, source information will then show “file”. You will
need to enter a station code "WIC" as this code is not contained in the example files.  

Next, we will have to access variometer and scalar data for this time range. Such data is contained in
example5.sec. You can also select a database as variometer and scaler data source, if connected. Please note
that the DATAINFO table is essential for selecting such data and only variometer data containing XYZ or HEZ components
are shown. Scalar data is limited to F or S notations. Finally you can also choose a webservice as data source. Please
note that although all webservices can be selected only the USGS and Conrad webservices are currently supported
(MagPy 2.0.0).

Example5.sec contains variation data and independent F from the observatory. In order to consider the pier difference
between F and DI pier one has to provide such data by modifying the analysis parameters. 

For our analysis the delta F values have already been considered in the F record and other deltas are negligible.
We can directly use the Analyze button. This will result in a plot of D,I,F and basevalues as well as a detailed
report on the results in the Analysis Log window of the DI panel (Figure 6.2.1). 

Figure 6.2.1: After DI analysis is finished, the screen will look similar as in this figure. You can change padding
ranges in the graph using the plot options method of the Data panel. 

Finally we export baseline and analysis data. The recommended export formats are either PYCDF or PYSTR as those will
contain all analysis information and you can create i.e. INTERMAGNET IBVF files anytime later from them.

For our example we choose PYSTR as output format, modify name, coverage and path, and check the contents of the 
newly created file using a text editor of your choice. You will see that all analysis information is contained 
in this time series file. For further analysis we will now switch to example3.txt as this file contains such DI 
analysis data for one year.

When opening this data file you get figure 6.2.2. You will find many individual DI measurements for one year. All 
these measurements are performed at a single pier (“A2”) and for analysis we used the same variometer/scalar 
instruments, thus obtaining base values for these instruments. Please note that measurements from other piers are 
saved in separate files.  

Figure 6.2.2: Basevalues for 2018 for pier A2 using a Lemi036 variometer and a GP20S3 potassium scalar magnetometer
as reference. Only measurements of s specific theodolite are shown. The gaps in May and June are related to the usage
of various other DI instruments in preparation of an IAGA workshop. Checkout the Conrad Observatory Yearbook 2018 for
complete data.


Having the spot basevalues we can now continue with fitting an adopted baseline. It is obvious that a simple linear 
fit will do a good job here. Anyway, for demonstration purposes we will use two separate fits to describe the baseline.

In the fit dialog we add the first fit, a cubic spline,  as shown in figure 6.2.3a. Then we add a second fit, a linear
least-squares according to figure 6.2.3b.  

Figure 6.2.3: Fit dialog for first and second fit.
Both fits will be shown in the plot (Figure 6.2.4) and also be automatically recognized as fits to basevalue data in
MagPy’s memory. 
Figure 6.2.4: Adopted baseline.

If you are satisfied with that you can save a BLV file. Please note that the file name will automatically be set with
the correct year. Now we want to use the adopted baseline for baseline correction of the variation data. Therefore 
we load such variation data (example5.sec). After that you will find the button Baseline on the Analysis page enabled.
Time to use it.

A dialog will open as shown in figure 6.2.6. This dialog will let you choose from different baseline data sets and
their fitting options within the memory from the upper drop down box. You can also redefine an adopted baseline by
using  Change Fit. For our baseline we just use the latest (final) input in the drop down menu, telling us that this
will use a linear-least square based on example3.txt contents for our data set.

Figure 6.2.5: Baseline adoption dialog.

After using Adopt baseline an information dialog (Figure 6.2.6) will pop-up telling you that the baseline correction
has been calculated. Its functional parameters are now contained in the meta information of the variation data set.
Yet, the correction has not yet been performed. If you want you can now save your variation data set as an PYCDF
archive which will store the functional parameters and, if available, also any flagging information currently connected
with the data set. This way you can keep a single archive file which contains basically every data analysis step
between raw data and definitive products. 

Figure 6.2.6: Information pop-up that an adopted baseline has been calculated and added to the variation meta data.

In order to apply baseline correction go to 

This will apply the functional parameter and you will now have a baseline corrected record for publication. 

Special cases for XYZ basevalues (since v1.0.1): 
Please note that MagPy by defaults expects basevalues for HDZ (see example3). When applying these basevalues the
D-base value is automatically converted to nT and applied to your variation data. Alternatively you can also use MaPy
basevalue files with XYZ basevalues. In order to apply such data correctly, the column names need to contain the
correct names, i.e. X-base, Y-base, Z-base instead of H-base, D-base and Z-base (as in example3.txt). 

## 6. Additional applications

## 7. Appendix

### 7.1 Known issues and bugs

Fitting: if fits are applied to a data set and afterwards the amount of shown columns is reduced or increased using the
**SelectColumns** option on the **Data panel** then the fits are not shown any more. Only if the originally selected 
columns are selected again the fitting curves will be visible again. The reason is that plotting depends on lists of keys,
and lists for symbols, colors, padding and functions, which all depend on the key list. Possible solutions: do not use lists 
but dictionaries. Well.... in a major future version.