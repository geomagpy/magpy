try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

exec(open('magpy/version.py').read())

setup(
    name='geomagpy',
    version=__version__,
    author='R. Leonhardt, R. Bailey, M. Miklavec, J. Fee, H. Schovanec',
    author_email='roman.leonhardt@zamg.ac.at',
    packages=['magpy', 'magpy.opt', 'magpy.examples', 'magpy.lib', 'magpy.acquisition', 'magpy.collector', 'magpy.gui'],
    scripts=['magpy/gui/xmagpy','magpy/gui/xmagpyw','magpy/scripts/mpconvert','magpy/scripts/addcred','magpy/scripts/mptest'],
    url='http://pypi.python.org/pypi/geomagpy/',
    license='LICENSE.txt',
    description='Geomagnetic analysis tools.',
    long_description=open('README.md').read(),
    package_data={'magpy': ['gui/*.png','gui/*.xpm','examples/*.cdf','examples/*.bin','examples/*.txt']},
    install_requires=[
        "matplotlib >= 0.9.8",
        "numpy >= 1.5.0",
        "scipy >= 0.8.6",
        "paho-mqtt >= 1.2.0",
        #"MySQL-python >= 1.2.3",
        #"pexpect >= 3.1",
    ],
)
