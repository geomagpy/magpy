# -*- coding: utf-8 -*-
"""
Various Format descriptions for MagPy.

:copyright:
    The MagPy Development Team
:license:
    GNU Lesser General Public License, Version 3
    (http://www.gnu.org/copyleft/lesser.html)
"""
from __future__ import absolute_import

# import order matters - NamedTemporaryFile must be one of the first!
#from magpy.stream import *
__all__ = ['streampage','dialogclasses','absolutespage','developpage']

from .streampage import *
from .dialogclasses import *
from .absolutespage import *
from .developpage import *
