import sys
from socket import gethostname

planet = gethostname()
leonplanets = ['Earth', 'uranus', 'saturn']


if planet == 'zagll1':
    magpypath = '/home/rachel/Software/MagPyDev/magpy/trunk/src/'
else:
    magpypath = '/home/leon/Software/magpy-git/'

sys.path.insert(1,magpypath)

#from magpy.stream import *
from magpy.gui.magpy_gui import *

app = wx.App(redirect=False)
frame = MainFrame(None,-1,"")
frame.Show()
app.MainLoop()
