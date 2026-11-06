# MagPy

**MagPy (or GeomagPy) is a Python package for analysing and displaying geomagnetic data.**

![0.0.0](./magpy/doc/magpy2-logo.png "MagPy2 - painting from Benoit Heumez")

Version Info: (please note: this package is still in a development state with frequent modifications) please check the
release notes.

MagPy provides tools for geomagnetic data analysis with special focus on typical data processing routines in
observatories. MagPy provides methods for data format conversion, plotting and mathematical procedures with 
specifically geomagnetic analysis routines such as basevalue and baseline calculation and database handling. Among the
supported data formats are *ImagCDF, IAGA-02, WDC, IMF, IAF, BLV*, and many more.
Full installation also provides a graphical user interface, *XMagPy*. Please consult the [online manual](https://geomagpy.github.io/magpy) 
for details in installation and all methods of the python package and the graphical user interface.

Typical usage of the basic MagPy package for reading and visualising data looks like this:

        #!/usr/bin/env python

        from magpy.stream import read
        from magpy.core import plot as mp
        stream = read('filename_or_url')
        mp.tsplot(stream)

You can also subscribe to our information
channel at [Telegram] for further information on updates and current issues.

