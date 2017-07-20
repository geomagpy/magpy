try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

exec(open('magpy/version.py').read())

setup(
    name='GeomagPy',
    version=__version__,
    author='R. Leonhardt, R. Bailey, M. Miklavec',
    author_email='roman.leonhardt@zamg.ac.at',
    packages=['magpy', 'magpy.opt', 'magpy.examples', 'magpy.lib', 'magpy.acquisition', 'magpy.collector', 'magpy.gui'],
    scripts=['magpy/gui/xmagpy','magpy/gui/xmagpyw'],
    url='http://pypi.python.org/pypi/GeomagPy/',
    license='LICENSE.txt',
    description='Geomagnetic analysis tools.',
    long_description=open('README.rst').read(),
    package_data={'magpy': ['gui/*.png','gui/*.xpm','examples/*.cdf','examples/*.bin','examples/*.txt']},
    install_requires=[
        "matplotlib >= 2.0.2",
        "numpy >= 1.5.0",
        "scipy >= 0.8.6",
        #"MySQL-python >= 1.2.3",
        #"pexpect >= 3.1",
    ],
)
