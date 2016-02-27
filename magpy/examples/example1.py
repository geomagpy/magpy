#!/usr/bin/env python
"""
MagPy - Example 1 

Reading and Plotting data
-------------------------

The following code contains three different ways to read data and 
shows some possibilities how to visualize the data

(some principle understanding of the Python programming language is advisable)

The code:

1.) Firstly the required magpy packages are imported (all examples start with this section):
 
import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')
from magpy.stream import *   
from magpy.transfer import *

2.) The read function of the magpy package is used to load data - in this case directly from the wdc:
The optional arguments "starttime" and "endtime" trim the dataset to the selected time range
starttime (and endtime) is of the from "Year-Month-Day H:M:S" or a datetime object

stream = read(path_or_url='ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc',starttime='2011-05-1',endtime='2011-07-1')

3.) The plot function of the magpy package:
Select the components which you want to plot
By default the plot fuction opens a graph (matplotlib) and stops further processing of the code until the 
graph is closed. The x axis scales are optimzed for dyanimc zooming. For saving the graph with decent, nonoverlapping x scales
adding the option confinex = True is helpful.

stream.plot(['x','y','z','f'])   or stream.plot(['x','y','z','f'], confinex=True)
 
"""

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')
from magpy.stream import *   
from magpy.transfer import *

stream = read(path_or_url='ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc',starttime='2011-05-1',endtime='2011-06-1')
stream.plot(['x','y','z','f'],confinex=True)

