#!/usr/bin/env python

import sys
from socket import gethostname

planet = gethostname()
leonplanets = ['Earth', 'uranus', 'saturn']


if planet == 'zagll1':
    magpypath = '/home/rachel/Software/MagPyDev/magpy/trunk/src/'
else:
    magpypath = '/home/leon/Software/magpy-git/'

sys.path.insert(1,magpypath)

from magpy.gui.magpy_gui import MagPyApp

app = MagPyApp(0)
app.MainLoop()
