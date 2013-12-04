import sys
from socket import gethostname

planet = gethostname()
leonplanets = ['earth', 'uranus', 'saturn']

if planet in leonplanets:
    magpypath = '/home/leon/Software/magpy/trunk/src/'
if planet == 'zagll1':
    magpypath = '/home/rachel/Software/MagPyDev/magpy/trunk/src/'

sys.path.append(magpypath)

#from stream import *
from gui.magpy_gui import *
  
app = wx.App(redirect=False)
frame = MainFrame(None,-1,"")
frame.Show()
app.MainLoop()
