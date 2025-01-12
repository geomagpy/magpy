# XMagPy

Version 2.0.0

Authors: Leonhardt, R. et al

### Table of Contents
1. [Introduction](#1-introduction)
2. [Installation](#2-installation)
3. [Main window and its menu bar](#3-main-window-and-its-menu-bar)
   1. [Layout and default configuration](#31-layout-and-default-configuration)
   2. [Reading and exporting data sets](#32-reading-and-exporting-data-sets)
4. [Panels](#4-panels)
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

### 3.2 Reading and exporting data sets

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

### 3.6 Specials

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

## 5. Application recipies for geomagnetic observatory data analysis

## 6. Additional applications

## 7. Appendix