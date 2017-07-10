#!/usr/bin/env python2

'''
This Package contains a tool for extracting structured data from HTML and JSON
pages.
It reads the page into a Node based tree, from which you, on the bases of a json
data-file, can extract your data into a list of items. It can first extract a
list of keyNodes and extract for each of them the same data-list. During the
extraction several data manipulation functions are available.

Main advantages
 - It gives you a highly dependable dataset from a potentially changable source.
 - You can easily update on changes in the source without touching your code.
 - You can make the data_def available on a central location while distributing
   the aplication and so giving your users easy access to (automated) updates.
'''

from distutils.core import setup
from DataTreeGrab import version, __version__

classifiers=[
    'Operating System :: Microsoft :: Windows',
    'Operating System :: POSIX',
    'Programming Language :: Python :: 2.7',
    'Intended Audience :: Developers',
    'Intended Audience :: End Users/Desktop',
    'License :: Public Domain',
    'Topic :: Internet :: WWW/HTTP :: Indexing/Search']

if version()[6]:
    classifiers.append('Development Status :: 3 - Alpha')

elif version()[5]:
    classifiers.append('Development Status :: 4 - Beta')

else:
    classifiers.append('Development Status :: 5 - Production/Stable')

setup(
    name = version()[0],
    version = __version__,
    description = 'Node-Tree based data extraction',
    py_modules = ['DataTreeGrab', 'test_json_struct'],
    scripts=['test_data_def.py'],
    requires = ['pytz'],
    provides = ['%s (%s.%s)' % (version()[0], version()[1], version()[2])],
    long_description = __doc__,
    maintainer = 'Hika van den Hoven',
    maintainer_email = 'hikavdh at gmail dot com',
    license='GPL',
    url='https://github.com/tvgrabbers/DataTree',
    classifiers=classifiers
        )
