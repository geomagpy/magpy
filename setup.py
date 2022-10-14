try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import sys
exec(open('magpy/version.py').read())

if sys.version_info < (3,5):
        install_requires=[
            "matplotlib < 3.0.0",
            "numpy >= 1.5.0",
            "scipy <= 1.2.1",
            "paho-mqtt >= 1.2.0",
            "pymysql <= 0.9.3",
            "ffnet >= 0.8.0",
            "spacepy <= 0.1.8",
            "pexpect >= 3.1.0",
            "kiwisolver <= 1.0.1",
            ]
else:
        install_requires=[
            "matplotlib > 2.0.2",
            "numpy >= 1.5.0",
            "scipy >= 0.8.6",
            "paho-mqtt >= 1.2.0",
            "pymysql >= 0.6.0",
            "cdflib >= 0.3.0",
            "pexpect >= 3.1.0",
            "pypubsub >= 4.0.0",
        ]

setup(
    name='geomagpy',
    version=__version__,
    author='R. Leonhardt, R. Bailey, M. Miklavec, J. Fee, H. Schovanec, S. Bracke',
    author_email='roman.leonhardt@zamg.ac.at',
    packages=['magpy', 'magpy.opt', 'magpy.examples', 'magpy.lib', 'magpy.acquisition', 'magpy.collector', 'magpy.gui', 'magpy.doc', 'magpy.core'],
    scripts=['magpy/gui/xmagpy','magpy/gui/xmagpyw','magpy/scripts/mpconvert','magpy/scripts/addcred','magpy/scripts/mptest'],
    url='http://pypi.python.org/pypi/geomagpy/',
    license='LICENSE.txt',
    description='Geomagnetic analysis tools.',
    long_description=open('README.rst').read(),
    package_data={'magpy': ['gui/*.png','gui/*.xpm','gui/*.jpg','examples/*.cdf','examples/*.json','examples/*.zip','examples/*.txt','examples/*.sec','doc/*.pdf']},
    install_requires=install_requires,
)
