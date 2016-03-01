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
from magpy.stream import *
from magpy.absolutes import *
from magpy.transfer import *
from magpy.database import *
from magpy.mpplot import *

import magpy.lib
import magpy.opt
import magpy.collector

from .version import __version__
