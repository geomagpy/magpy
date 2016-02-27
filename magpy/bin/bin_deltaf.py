#!/usr/bin/env python
"""
Example program to analyze a single absolute file
"""

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

from magpy.stream import *

streamcs = read('/home/leon/leon/Observatory/Messdaten/Data-Magnetism/GEM_download 2013-07-25/G823A_C228_0001_2013-07-26.bin')
streamcs.plot(['f'])

gemstr = read('/home/leon/leon/Observatory/Messdaten/Data-Magnetism/GEM_download 2013-07-25/GEM_AS_A2_2013-07-*')
gemstr.plot(['f'])

# Stream 1
gemstr = read('/home/leon/leon/Observatory/Messdaten/Data-Magnetism/GEM_download 2013-07-25/GEM_AS-A6-A5_2013-07-18.txt')
gemstr = gemstr.remove_outlier()
gemstr = gemstr.remove_flagged()
gemstr = gemstr.filter(filter_type='gauss',filter_width=timedelta(minutes=1))
gemstr.plot(['f'])

# Stream 2
posstr = read('/srv/data/GS-W-30/raw/pos1_2013-07-18.bin')
posstr = posstr.remove_outlier()
posstr = posstr.remove_flagged()
posstr = posstr.filter(filter_type='gauss',filter_width=timedelta(minutes=1))
posstr.plot(['f'])

diff = subtractStreams(gemstr,posstr,keys=['f'])
diff.plot(['f','df'])

deltaF = np.median(diff._get_column('df'))
print "Delta F: ", deltaF
