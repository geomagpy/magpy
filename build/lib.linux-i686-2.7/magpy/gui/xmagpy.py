import sys
from socket import gethostname

try:
    from magpy.gui.magpy_gui import *
except:

    planet = gethostname()
    leonplanets = ['Earth', 'uranus', 'saturn']

    if planet == 'zagll1':
        magpypath = '/home/rachel/Software/MagPyDev/magpy/trunk/src/'
    elif planet in leonplanets:
        magpypath = '/home/leon/Software/magpy/trunk/src/'
    elif planet == 'Venus':
        magpypath = '/home/leon/Software/magpy/'
    else:
        magpypath = '/home/leon/Software/magpy/trunk/src/'

    sys.path.append(magpypath)
    from magpy.gui.magpy_gui import *
  
app = wx.App(redirect=False)
frame = MainFrame(None,-1,"")
frame.Show()
app.MainLoop()
