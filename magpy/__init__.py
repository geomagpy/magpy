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
__all__ = ['lib','opt','stream','absolutes','core'
]
from magpy.stream import *
from magpy.absolutes import *
from magpy.core.methods import *

import magpy.lib
import magpy.opt
import magpy.core

from .version import __version__
