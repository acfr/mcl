import os
from setuptools import setup


# Read long description from README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='mcl',
    version='0.1.0',
    author='Australian Centre for Field Robotics',
    author_email='a.bender@acfr.usyd.edu.au',
    url='http://its.acfr.usyd.edu.au/',
    description=('multiprocess communications library'),
    long_description=read('README'),

    install_requires=[
        'Sphinx',               # Tested with 1.3.1
        'coverage',             # Tested with 4.0.3
        'msgpack-python',       # Tested with 0.4.2
        'nose',                 # Tested with 0.4.1
        'nose-timer',           # Tested with 0.4.3
        'numpy',                # Tested with 1.9.2
    ],

    packages=['mcl'],

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: Communications',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Logging',
        'Topic :: System :: Networking',
    ],

)
