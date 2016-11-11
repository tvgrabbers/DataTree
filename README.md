# DataTreeGrab
[Some examples](https://github.com/tvgrabbers/DataTree/wiki/examples)  
[The latest stable release](https://github.com/tvgrabbers/DataTree/releases/latest)  
[Go to the WIKI](https://github.com/tvgrabbers/DataTree/wiki)  
[Go to tvgrabpyAPI](https://github.com/tvgrabbers/tvgrabpyAPI)  
[Go to tvgrabnlpy](https://github.com/tvgrabbers/tvgrabnlpy)  

Spin-off python module for extracting structured data from HTML and JSON pages.  
It is at the heart of the tv_grab_py_API and was initially named just DataTree,  
but as this name is already taken in the Python library...

###Requirements
 * Python 2.7.9 or higher (currently not python 3.x)
 * The [pytz module](http://pypi.python.org/pypi/pytz)

###Installation
* Especially under Windows, make sure Python 2.7.9 or higher is installed 
* Make sure the above mentioned Python 2 package is installed on your system
* Download the latest release and unpack it into a directory
* Run:
  * under Linux: `sudo ./setup.py install` from that directory
  * under Windows depending on how you installed Python:
    * `setup.py install` from that directory
    * Or: `Python setup.py install` from that directory

###Main advantages
 * It gives you a highly dependable dataset from a potentially changable source.
 * You can easily update on changes in the source without touching your code.
 * You can make the data_def available on a central location while distributing  
   the aplication and so giving your users easy access to (automated) updates.
   
###Known issues
 * Adding warning rules to DataTreeShell prior to DataTree initialization will plase the 
   general rule before those added rules

With [version 1.3.0](https://github.com/tvgrabbers/DataTree/releases/tag/stable-1.3.0) 9-11-2016
 * first not beta release
 * added functionality to show progress while running the extract_datalist function in a 
   multi-threading environment
 * added a flag to abort the extract_datalist function in a multi-threading environment

With [version 1.2.5](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.2.5-p20161015) 15-10-2016
 * added a print_datatree function to DataTreeShell
 * some cosmetic updates on the internal print functions

With [version 1.2.4](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.2.4-p20160930) 30-9-2016
 * implemented "text_replace" keyword to search and replace in the html data before importing
 * implemented "unquote_html" keyword to correct `", < and >` occurence in html text by replacing 
   them with the correct `&quot;, &lt; and &gt;`
 * made it possible to read a partial html page resulting from an "HTTP incomplete read" by 
   checking and adding on a missing `</html>` and/or `</body>`tag. If more then the tail part
   is missing it probably will later fail on your search. (Any tag with a missing clossing tag
   is assumed to be auto-closing. This will, except on the enclosing `<html>` tag, prevent HTML
   errors. However if the missing is inadvertently it can cause a change in the tree hierarchy, 
   making the search, even when the data is present, fail. So this will only work if all the 
   other higher closing tags are in the download.)

With [version 1.2.3](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.2.3-p20160925) 25-9-2016
 * implemented "url-relative-weekdays" keyword
 * some bug fixes
 * Updates on the test module

With [version 1.2.2](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.2.2-p20160831) 31-8-2016
 * Updates on the test module
 * Some code sanitation

With [version 1.2.1](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.2.1-p20160822) 22-8-2016
 * Updates on the test module

With [version 1.2.0](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.2.0-p20160820) 20-8-2016
 * Implemented a data_def test module

With [version 1.1.4](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.1.4-p20160723) 23-7-2016
 * Implemented a stripped and extended Warnings framework
 * Added optional sorting before extraction of part of a JSON tree
 * Some fixes

With [version 1.1.3](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.1.3-p20160709) 9-7-2016
 * More unified HTML and JSON parsing with added keywords "notchildkeys" and "tags",  
   renamed keyword "childkeys" and extended functionality for some of the others.  
   Also allowing to use a linked value in most cases.
 * Added selection keyword "inclusive text" for HTML to include text in sub tags like  
   "i", "b" etc.
 * Added support for a tupple with multiple dtype values in the is_data_value function.

With [version 1.1.2](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.1.2-p20160705) 5-7-2016
 * A new warnings category for invalid data imports into a tree
 * A new search keyword "notattrs"

With [version 1.1](https://github.com/tvgrabbers/DataTree/releases/tag/beta-1.1.1-p20160628) 28-6-2016
we have next to some patches added several new features:  
 * Added support for 12 hour time values
 * Added the str-list type 
 * Added a warnings framework
 * Added a DataTreeShell class with pre and post processing functionality.

It reads the page into a Node based tree, from which you, on the bases of a json  
data-file, can extract your data into a list of items. For this a special 
[Data_def language](https://github.com/tvgrabbers/DataTree/wiki/data_def_language)  
has been developed. It can first extract a list of keyNodes and extract for each  
of them the same data-list. During the extraction several data manipulation  
functions are available.  

Check [the WIKI](https://github.com/tvgrabbers/DataTree/wiki) for the syntax. 
Here a short incomplete list of possible keywords:      
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

