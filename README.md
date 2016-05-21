# DataTreeGrab
[Go to the WIKI](https://github.com/tvgrabbers/DataTree/wiki)  
[Go to tvgrabnlpy](https://github.com/tvgrabbers/tvgrabnlpy)  

Spin-off python module for extracting structured data from HTML and JSON pages.  
It is at the heart of the tvgrabpyAPI and was initially named just DataTree,  
but as this name is already taken in the Python library...

It reads the page into a Node based tree, from which you, on the bases of a json  
data-file, can extract your data into a list of items. It can first extract a  
list of keyNodes and extract for each of them the same data-list. During the  
extraction several data manipulation functions are available.  

The current install will only install under Python 2. It probably will run under  
Python 3 but is not jet tested. It is still considered beta  
It will need the pytz package: https://pypi.python.org/pypi/pytz/ 

Run: `sudo ./setup.py install` to install it into your Python2 module tree

Further documentation will follow, but to give an indication, a short list of keywords.  
See also the source definition files at  
https://github.com/tvgrabbers/tvgrabnlpy/tree/tvgrabnlpy3/sources.  
Check the secundary "data" items:    
###path-dict keywords:
 * "path": "all", "root", "parent"
 * "key":<name>
 * "keys":{"<name>":{"link":1},"<name>":""} (selection on child presence)
 * "tag":"<name>"
 * "attrs":{"<name>":{"link":1},"<name>":{"not":[]},"<name>":"","<name>":null}
 * "index":{"link":1}

###selection-keywords:
 * "select": "key", "text", "tag", "index", "value"
 * "attr":"<name>"
 * "link":1		(create a link)
 * "link-index":1		(create a link)

###link examples
```
[{"key":"abstract_key", "link":1},
        "root",{"key":"library"},"all",{"key":"abstracts"},
        {"keys":{"abstract_key":{"link":1}}},
        {"key":"name","default":""}],

        [...,{ "attr":"value", "ascii-replace":["ss","s", "[-!?(), ]"], "link":1}],

        [...,{"tag":"img", "attrs":{"class": {"link":1}},"attr":"src"}],
```
###selection-format keywords:
 * "lower","upper","capitalize"
 * "ascii-replace":["ss","s", "[-!?(), ]"]
 * "lstrip", "rstrip":"')"
 * "sub":["",""]
 * "split":[["/",-1],["\\.",0,1]]
 * "multiplier", "divider":1000	(for timestamp)
 * "replace":{"tv":2, "radio":12}
 * "default":
 * "type":
   * "datetimestring","timestamp","time","timedelta","date","datestamp", "relative-weekday","string", "lower-ascii","int", "float","boolean","list",
 * "member-off"

