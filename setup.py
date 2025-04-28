try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import sys
exec(open('magpy/version.py').read())

install_requires=[
            "matplotlib >= 3.5.3",
            "numpy >= 1.21.0",
            "scipy >= 1.7.3",
            "paho-mqtt >= 1.6.0",
            "setuptools",
            "pymysql >= 1.0.2",
            "cdflib >= 1.2.3,<=1.3.3",
            "pexpect >= 4.8.0",
            "emd >= 0.7.0",
            "pypubsub >= 4.0.0",
            "PyWavelets >= 1.3.0"
          ]

setup(
    name='geomagpy',
    version=__version__,
    author='R. Leonhardt, R. Bailey, M. Miklavec, J. Fee, H. Schovanec, S. Bracke',
    author_email='roman.leonhardt@geosphere.at',
    packages=['magpy', 'magpy.opt', 'magpy.examples', 'magpy.lib', 'magpy.gui', 'magpy.doc', 'magpy.core'],
    scripts=['magpy/gui/xmagpy','magpy/gui/xmagpyw','magpy/scripts/mpconvert','magpy/scripts/addcred'],
    url='http://pypi.python.org/pypi/geomagpy/',
    license='LICENSE.txt',
    description='Geomagnetic analysis tools.',
    long_description=open('README.md', encoding="utf8").read(),
    long_description_content_type='text/markdown',
    package_data={'magpy': ['gui/*.png','gui/*.xpm','gui/*.jpg','examples/*.cdf','examples/*.json','examples/*.zip','examples/*.txt','examples/*.sec','doc/*.png','doc/*.ipynb']},
    install_requires=install_requires,
)
