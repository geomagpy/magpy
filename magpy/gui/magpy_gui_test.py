#!/usr/bin/env python

import os
import sys

# run the MagPy GUI from a local source rather than from the installed version
magpypath = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
sys.path.insert(1,magpypath)

from magpy.gui.magpy_gui import MagPyApp

app = MagPyApp(0)
app.MainLoop()
