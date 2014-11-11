# -*- coding: utf-8 -*-
"""
Various Format descriptions for MagPy.

:copyright:
    The MagPy Development Team
:license:
    GNU Lesser General Public License, Version 3
    (http://www.gnu.org/copyleft/lesser.html)
"""

# import order matters - NamedTemporaryFile must be one of the first!
__all__ = ['lib','opt','stream','absolutes','transfer','database','mpplot','collector'
]
from stream import *
from absolutes import *
from transfer import *
from database import *
from mpplot import *

import lib
import opt
import collector

