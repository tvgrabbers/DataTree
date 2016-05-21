#!/usr/bin/env python2

'''
This Package contains a tool for extracting structured data from HTML and JSON
pages.
It reads the page into a Node based tree, from which you, on the bases of a json
data-file, can extract your data into a list of items. It can first extract a
list of keyNodes and extract for each of them the same data-list. During the
extraction several data manipulation functions are available.'''

from distutils.core import setup
from DataTreeGrab import version

dtv = '%s.%s.%s' % (version()[1],version()[2],version()[3])
if version()[6]:
    dtv = '%s-alfa' % (dtv)

elif version()[5]:
    dtv = '%s-beta' % (dtv)

setup(
    name = version()[0],
    version = dtv,
    description = 'Node-Tree based data extraction',
    py_modules = ['DataTreeGrab'],
    requires = ['pytz'],
    provides = ['%s (%s.%s)' % (version()[0], version()[1], version()[2])],
    long_description = __doc__,
    maintainer = 'Hika van den Hoven',
    maintainer_email = 'hikavdh at gmail dot com',
    license='GPL',
    url='https://github.com/tvgrabbers/DataTree',
)
