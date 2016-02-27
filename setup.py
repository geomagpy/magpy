try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

exec(open('magpy/version.py').read())

setup(
    name='GeomagPy',
    version=__version__,
    author='R. Leonhardt, R. Bailey',
    author_email='roman.leonhardt@zamg.ac.at',
    packages=['magpy', 'magpy.opt', 'magpy.examples', 'magpy.lib', 'magpy.acquisition', 'src.collector', 'src.gui'],
    #scripts=['bin/example.py'],
    url='http://pypi.python.org/pypi/GeomagPy/',
    license='LICENSE.txt',
    description='Geomagnetic analysis tools.',
    long_description=open('README.txt').read(),
    data_files=[('magpy/gui', ['magpy/gui/magpy.png', 'magpy/gui/magpy128.xpm'])],
    install_requires=[
        "matplotlib >= 0.9.8",
        "numpy >= 1.5.0",
        "scipy >= 0.8.6",
        "MySQL-python >= 1.2.3",
        #"pexpect >= 3.1",
    ],
)
