#!/usr/bin/env python2
# -*- coding: utf-8 -*-

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

For the newest version and documentation see:

    https://github.com/tvgrabbers/DataTree/

    LICENSE

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.'''

from __future__ import unicode_literals
import re, sys, traceback, types
import time, datetime, pytz
from threading import RLock
from Queue import Queue

try:
    from html.parser import HTMLParser, HTMLParseError
except ImportError:
    from HTMLParser import HTMLParser, HTMLParseError

try:
    from html.entities import name2codepoint
except ImportError:
    from htmlentitydefs import name2codepoint

dt_name = u'DataTreeGrab'
dt_major = 1
dt_minor = 3
dt_patch = 3
dt_patchdate = u'20170516'
dt_alfa = False
dt_beta = False
_warnings = None

__version__  = '%s.%s.%s' % (dt_major,'{:0>2}'.format(dt_minor),'{:0>2}'.format(dt_patch))
if dt_alfa:
    __version__ = '%s-alfa' % (__version__)

elif dt_beta:
    __version__ = '%s-beta' % (__version__)

def version():
    return (dt_name, dt_major, dt_minor, dt_patch, dt_patchdate, dt_beta, dt_alfa)
# end version()

def is_data_value(searchpath, searchtree, dtype = None, empty_is_false = False):
    """
    Follow searchpath through the datatree in searchtree
    and report if there exists a value of type dtype
    searchpath is a list of keys/indices
    If dtype is None check for any value
    you can also supply a tuple  to dtype
    """
    if isinstance(searchpath, (str, unicode, int)):
        searchpath = [searchpath]

    if not isinstance(searchpath, (list, tuple)):
        return False

    for d in searchpath:
        if isinstance(searchtree, dict):
            if not d in searchtree.keys():
                return False

        elif isinstance(searchtree, (list, tuple)):
            if (not isinstance(d, int) or d >= len(searchtree)):
                return False

        else:
            return False

        searchtree = searchtree[d]

    if dtype == None and not (empty_is_false and searchtree == None):
        return True

    if empty_is_false and searchtree in (None, "", {}, []):
        return False

    if isinstance(dtype, tuple):
        dtype = list(dtype)

    elif not isinstance(dtype, list):
        dtype = [dtype]

    if float in dtype and not int in dtype:
        dtype.append(int)

    if str in dtype or unicode in dtype or 'string' in dtype:
        for dt in (str, unicode, 'string'):
            while dt in dtype:
                dtype.remove(dt)
        dtype.extend([str, unicode])

    if list in dtype or tuple in dtype or 'list' in dtype:
        for dt in (list, tuple, 'list'):
            while dt in dtype:
                dtype.remove(dt)
        dtype.extend([list, tuple])

    dtype = tuple(dtype)

    return bool(isinstance(searchtree, dtype))
# end is_data_value()

def data_value(searchpath, searchtree, dtype = None, default = None):
    """
    Follow searchpath through the datatree in searchtree
    and return if it exists a value of type dtype
    searchpath is a list of keys/indices
    If dtype is None check for any value
    If it is not found return default or if dtype is set to
    a string, list or dict, an empty one
    """
    if is_data_value(searchpath, searchtree, dtype):
        if isinstance(searchpath, (str, unicode, int)):
            searchpath = [searchpath]

        for d in searchpath:
            searchtree = searchtree[d]

    else:
        searchtree = None

    if searchtree == None:
        if default != None:
            return default

        elif dtype in (str, unicode, 'string'):
            return ""

        elif dtype == dict:
            return {}

        elif dtype in (list, tuple, 'list'):
            return []

    return searchtree
# end data_value()

def extend_list(base_list, extend_list):
    if not isinstance(base_list, list):
        base_list = [base_list]

    if not isinstance(extend_list, list):
        base_list.append(extend_list)

    else:
        base_list.extend(extend_list)

    return base_list
# end extend_list()

# DataTreeShell errorcodes
dtQuiting = -1
dtDataOK = 0
dtDataInvalid = 1
dtStartNodeInvalid = 2
dtDataDefInvalid = 3
dtNoData = 7
dtFatalError = 7

dtSortFailed = 8
dtUnquoteFailed = 16
dtTextReplaceFailed = 32
dtTimeZoneFailed = 64
dtCurrentDateFailed = 128
dtErrorTexts = {
-1: 'The execution was aborted',
0: 'Data OK',
1: 'Invalid dataset!',
2: 'Invalid startnode!',
3: 'Invalid data_def',
4: 'Unknown State',
5: 'Unknown State',
6: 'Unknown State',
7: 'No Data',
8: 'Data sorting failed',
16: 'The Unquote filter failed',
32: 'The Textreplace filter failed',
64: 'Timezone initialization failed',
128: 'Setting the current date failed'
}

class dtWarning(UserWarning):
    # The root of all DataTreeGrab warnings.
    name = 'General Warning'

class dtDataWarning(dtWarning):
    name = 'Data Warning'

class dtdata_defWarning(dtWarning):
    name = 'data_def Warning'

class dtParseWarning(dtdata_defWarning):
    name = 'Parse Warning'

class dtCalcWarning(dtdata_defWarning):
    name = 'Calc Warning'

class dtUrlWarning(dtWarning):
    name = 'URL Warning'

class dtLinkWarning(dtWarning):
    name = 'Link Warning'

class _Warnings():
    def __init__(self, warnaction = None, warngoal = sys.stderr, caller_id = 0):
        self.warn_lock = RLock()
        self.onceregistry = {}
        self.filters = []
        self._ids = []
        if not caller_id in self._ids:
            self._ids.append(caller_id)
        self.warngoal = warngoal
        if warnaction == None:
            warnaction = "default"

        self.set_warnaction(warnaction, caller_id)

    def set_warnaction(self, warnaction = "default", caller_id = 0):
        with self.warn_lock:
            self.resetwarnings(caller_id)
            if not caller_id in self._ids:
                self._ids.append(caller_id)

            if not warnaction in ("error", "ignore", "always", "default", "module", "once"):
                warnaction = "default"

            self.simplefilter(warnaction, dtWarning, caller_id = caller_id)
            self.defaultaction = warnaction

    def _show_warning(self, message, category, caller_id, severity, lineno):
        with self.warn_lock:
            message = "DataTreeGrab,id:%s:%s at line:%s: %s\n" % (caller_id, category.name, lineno, message)
            try:
                if isinstance(self.warngoal, Queue):
                    self.warngoal.put((message, caller_id, severity))

                else:
                    self.warngoal.write(message)

            except IOError:
                pass # the file (probably stderr) is invalid - this warning gets lost.

    def warn(self, message, category=None, caller_id=0, severity=1, stacklevel=1):
        # 1 = serious
        # 2 = invalid data_def
        # 4 = invalid data
        with self.warn_lock:
            # Check if message is already a Warning object
            if isinstance(message, Warning):
                category = message.__class__
            # Check category argument
            if category is None:
                category = UserWarning
            assert issubclass(category, Warning)
            # Get context information
            try:
                caller = sys._getframe(stacklevel)
            except ValueError:
                globals = sys.__dict__
                lineno = 1
            else:
                globals = caller.f_globals
                lineno = caller.f_lineno
            if '__name__' in globals:
                module = globals['__name__']
            else:
                module = "<string>"
            filename = globals.get('__file__')
            if filename:
                fnl = filename.lower()
                if fnl.endswith((".pyc", ".pyo")):
                    filename = filename[:-1]
            else:
                if module == "__main__":
                    try:
                        filename = sys.argv[0]
                    except AttributeError:
                        # embedded interpreters don't have sys.argv, see bug #839151
                        filename = '__main__'
                if not filename:
                    filename = module
            registry = globals.setdefault("__warningregistry__", {})
            self.warn_explicit(message, category, filename, lineno, caller_id, severity, module, registry, globals)

    def warn_explicit(self, message, category, filename, lineno, caller_id=0, severity=1, module=None, registry=None, module_globals=None):
        with self.warn_lock:
            lineno = int(lineno)
            if module is None:
                module = filename or "<unknown>"
                if module[-3:].lower() == ".py":
                    module = module[:-3] # XXX What about leading pathname?
            if registry is None:
                registry = {}
            if isinstance(message, Warning):
                text = str(message)
                category = message.__class__
            else:
                text = message
                message = category(message)
            key = (text, category, lineno)
            # Quick test for common case
            if registry.get(key):
                return
            # Search the filters
            for item in self.filters:
                action, msg, cat, mod, ln, cid, sev = item
                if ((msg is None or msg.match(text)) and
                    issubclass(category, cat) and
                    (mod is None or mod.match(module)) and
                    (ln == 0 or lineno == ln) and
                    (cid == 0 or caller_id == cid) and
                    (sev == 0 or severity & sev)):
                    break
            else:
                action = self.defaultaction

            # Early exit actions
            if action == "ignore":
                registry[key] = 1
                return

            if action == "error":
                raise message
            # Other actions
            if action == "once":
                registry[key] = 1
                oncekey = (text, category)
                if self.onceregistry.get(oncekey):
                    return
                self.onceregistry[oncekey] = 1
            elif action == "always":
                pass
            elif action == "module":
                registry[key] = 1
                altkey = (text, category, 0)
                if registry.get(altkey):
                    return
                registry[altkey] = 1
            elif action == "default":
                registry[key] = 1
            else:
                # Unrecognized actions are errors
                raise RuntimeError(
                      "Unrecognized action (%r) in warnings.filters:\n %s" %
                      (action, item))
            # Print message and context
            self._show_warning(message, category, caller_id, severity, lineno)

    def resetwarnings(self, caller_id = 0):
        with self.warn_lock:
            if caller_id == 0:
                self.filters[:] = []

            else:
                for item in self.filters[:]:
                    if item[5] == caller_id:
                        self.filters.remove(item)

    def simplefilter(self, action, category=Warning, lineno=0, append=0, caller_id = 0, severity=0):
        with self.warn_lock:
            assert action in ("error", "ignore", "always", "default", "module",
                              "once"), "invalid action: %r" % (action,)
            assert isinstance(category, (type, types.ClassType)), \
                   "category must be a class"
            assert issubclass(category, Warning), "category must be a Warning subclass"
            assert isinstance(lineno, int) and lineno >= 0, \
                   "lineno must be an int >= 0"
            item = (action, None, category, None, lineno, caller_id, severity)
            if item in self.filters:
                self.filters.remove(item)
            if append:
                self.filters.append(item)
            else:
                self.filters.insert(0, item)

    def filterwarnings(self, action, message="", category=Warning, module="", lineno=0, append=0, caller_id = 0, severity=0):
        with self.warn_lock:
            assert action in ("error", "ignore", "always", "default", "module",
                              "once"), "invalid action: %r" % (action,)
            assert isinstance(message, basestring), "message must be a string"
            assert isinstance(category, (type, types.ClassType)), \
                   "category must be a class"
            assert issubclass(category, Warning), "category must be a Warning subclass"
            assert isinstance(module, basestring), "module must be a string"
            assert isinstance(lineno, int) and lineno >= 0, \
                   "lineno must be an int >= 0"
            item = (action, re.compile(message, re.I), category,
                    re.compile(module), lineno, caller_id, severity)
            if item in self.filters:
                self.filters.remove(item)
            if append:
                self.filters.append(item)
            else:
                self.filters.insert(0, item)

# end _Warnings()

class NULLnode():
    value = None

# end NULLnode

class DATAnode():
    """Basic DataNode functionality to be detailed in JSONnode and HTMLnode"""
    def __init__(self, dtree, parent = None):
        self.node_lock = RLock()
        with self.node_lock:
            self.children = []
            self.dtree = dtree
            self.parent = parent
            self.value = None
            self.child_index = 0
            self.level = 0
            self.links = {}
            self.links["values"] = {}
            self.links["nodes"] = {}
            self.end_links = {}
            self.end_links["values"] = {}
            self.end_links["nodes"] = {}

            self.is_root = bool(self.parent == None)
            n = self
            while not n.is_root:
                n = n.parent

            self.root = n
            if isinstance(parent, DATAnode):
                self.parent.append_child(self)
                self.level = parent.level + 1

    def append_child(self, node):
        # Used in initializing the Tree
        with self.node_lock:
            node.child_index = len(self.children)
            self.children.append(node)

    def get_children(self, path_def = None, links=None):
        """
        The basic function to walk through the Tree
        It first checks on the kind of node_def, extracting any name or link definition
        And then follows the node_def to the defined nodes
        """
        nm = None
        d_def = path_def if isinstance(path_def, list) else [path_def]
        if not isinstance(links, dict):
            links = {"values": {},"nodes": {}}

        if not is_data_value("values", links, dict):
            links["values"] = {}

        if not is_data_value("nodes", links, dict):
            links["nodes"] = {}

        def match_node(node, only_check_validity = False):
            # check through the HTML/JSON specific functions
            nfound = node.match_node(node_def = d_def[0], links=links, only_check_validity = only_check_validity)
            if not only_check_validity and nfound:
                if self.dtree.show_result:
                    self.dtree.print_text(u'    found node %s;\n%s' % (node.print_node(), node.print_node_def(d_def[0])))

            if (not only_check_validity and nfound) or (only_check_validity and nfound == None):
                # If we found a match or it is not a node definition
                # We retrieve any found links
                if len(node.links["values"]) > 0:
                    for k, v in node.links["values"].items():
                        links["values"][k] = v

                    node.links["values"] = {}

                if len(node.links["nodes"]) > 0:
                    for k, v in node.links["nodes"].items():
                        links["nodes"][k] = v

                    node.links["nodes"] = {}

            return nfound

        def found_nodes(fnodes):
            # Return the found nodes, adding if defined a name
            if fnodes == [self]:
                # This is an end node, so we store link values to use on further searches
                self.end_links["values"] = links["values"].copy()
                self.end_links["nodes"] = links["nodes"].copy()
                if self.dtree.show_result:
                    self.dtree.print_text(u'  adding node %s' % (self.print_node()))

            if nm == None:
                return fnodes

            else:
                return {nm: fnodes}

        if len(d_def) == 0 or d_def[0] == None:
            # We seem to be past the end of the path_def so we consider ourselfs found
            return found_nodes([self])

        # Check on any name statement, to use in the return value
        nm = self.find_name(d_def[0])
        # Check if the node_def contains a child definition and if not collect any link request
        if match_node(self, True) == None:
            # It's not a child definition
            if len(d_def) == 1:
                # It's the final node_def, so we consider ourselfs found
                return found_nodes([self])

            else:
                # We'll check the next node_def,
                return found_nodes(self.get_children(path_def = d_def[1:], links=links))

        # Is there a path statement
        elif is_data_value('path', d_def[0]):
            sel_val = d_def[0]['path']
            if sel_val in links["nodes"].keys() and isinstance(links["nodes"][sel_val], DATAnode):
                if match_node(links["nodes"][sel_val]):
                    return found_nodes(links["nodes"][sel_val].get_children(path_def = d_def[1:], links=links))

            elif sel_val == 'all':
                childs = []
                for item in self.children:
                    if match_node(item):
                        childs = extend_list(childs, item.get_children(path_def = d_def[1:], links=links))

                return found_nodes(childs)

            elif sel_val == 'root':
                if match_node(self.root):
                    return found_nodes(self.root.get_children(path_def = d_def[1:], links=links))

            elif sel_val == 'parent' and not self.is_root:
                if match_node(self.parent):
                    return found_nodes(self.parent.get_children(path_def = d_def[1:], links=links))

        else:
            childs = []
            # We look for matching children
            for item in self.children:
                if match_node(item):
                    # We found a matching child
                    childs = extend_list(childs, item.get_children(path_def = d_def[1:], links=links))

            return found_nodes(childs)

        return []

    def check_for_linkrequest(self, node_def):
        if is_data_value('node', node_def, int):
            self.links["nodes"][node_def['node']] = self
            if self.dtree.show_result:
                self.dtree.print_text(u'    saving link to node: %s\n      %s' % \
                    (self.print_node(), self.print_node_def(node_def)))

        if is_data_value('link', node_def, int):
            lv = self.find_value(node_def)
            self.links["values"][node_def['link']] = lv
            if self.dtree.show_result:
                if isinstance(lv, (str,unicode)):
                    self.dtree.print_text(u'    saving link to nodevalue (="%s"): %s\n      %s' % \
                        (lv, self.print_node(), self.print_node_def(node_def)))

                else:
                    self.dtree.print_text(u'    saving link to nodevalue (=%s): %s\n      %s' % \
                        (lv, self.print_node(), self.print_node_def(node_def)))

    def get_link(self, sub_def, link_values, ltype = None):
        # retrieve a stored link_value
        if not is_data_value(data_value(['link'], sub_def, int), link_values):
            self.dtree.warn('You requested a link, but link value %s is not stored!' % data_value(['link'], sub_def, int), dtParseWarning, 2)
            return None

        il = link_values[data_value(['link'], sub_def, int)]
        if ltype == 'int':
            try:
                il = int(il)

            except:
                self.dtree.warn('Invalid linkvalue "%s" requested. Should be integer.' % (il), dtParseWarning, 2)
                return None

        if ltype == 'lower':
            try:
                return unicode(il).lower()

            except:
                self.dtree.warn('Invalid linkvalue "%s" requested. Should be string.' % (il), dtParseWarning, 2)
                return None

        if ltype == 'str':
            try:
                return unicode(il)

            except:
                self.dtree.warn('Invalid linkvalue "%s" requested. Should be string.' % (il), dtParseWarning, 2)
                return None

        if isinstance(il, (int, float)):
            clist = data_value(['calc'], sub_def, list)
            if len(clist) == 2 and isinstance(clist[1], (int, float)):
                if clist[0] == 'min':
                    il -= clist[1]

                elif clist[0] == 'plus':
                    il += clist[1]

        return il

    def check_index(self, node_def, link_values):
        # Check if this node satisfies  the requested index
        if is_data_value(['index','link'], node_def, int):
            # There is an index request to an earlier linked index
            il = self.get_link(data_value(['index'], node_def, dict), link_values, 'int')
            if not isinstance(il, int):
                self.dtree.warn('You requested an index link, but the stored value is no integer!', dtParseWarning, 2)
                return None

            if is_data_value(['index','previous'], node_def):
                if self.child_index < il:
                    return True

            elif is_data_value(['index','next'], node_def):
                if self.child_index > il:
                    return True

            elif self.child_index == il:
                return True

        elif is_data_value(['index'], node_def, int):
            # There is an index request to a set value
            if self.child_index == data_value(['index'], node_def, int):
                return True

        else:
            return None

        return False

    def get_value_list(self, vlist, link_values, valuetype = '', ltype = None):
        # Check a list for links and then replace it with the link_value
        if not isinstance(vlist, (list, tuple)):
            vlist = [vlist]

        rlist = []
        for index in range(len(vlist)):
            if is_data_value([index,'link'], vlist, int):
                rlist.append(self.get_link(data_value([index], vlist, dict), link_values, ltype))

            elif ltype == 'lower':
                try:
                    rlist.append(unicode(vlist[index]).lower())

                except:
                    self.dtree.warn('Invalid %s matchvalue "%s" requested. Should be string.' % (valuetype, vlist[index]), dtParseWarning, 2)

            elif ltype == 'str':
                try:
                    rlist.append(unicode(vlist[index]))

                except:
                    self.dtree.warn('Invalid %s matchvalue "%s" requested. Should be string.' % (valuetype, vlist[index]), dtParseWarning, 2)

            elif ltype == 'int':
                try:
                    rlist.append(int(vlist[index]).lower())

                except:
                    self.dtree.warn('Invalid %s matchvalue "%s" requested. Should be integer.' % (valuetype, vlist[index]), dtParseWarning, 2)

            else:
                rlist.append(vlist[index])

        return rlist

    def get_value(self, value, link_values, valuetype = '', ltype = None):
        # Check if value is a link statement and then return the link value in stead of value
        if is_data_value(['link'], value, int):
            return self.get_link(value ,link_values, ltype)

        elif ltype == 'lower':
            try:
                return unicode(value).lower()

            except:
                self.dtree.warn('Invalid %s matchvalue "%s" requested. Should be string.' % (valuetype, value), dtParseWarning, 2)

        elif ltype == 'str':
            try:
                return unicode(value)

            except:
                self.dtree.warn('Invalid %s matchvalue "%s" requested. Should be string.' % (valuetype, value), dtParseWarning, 2)

        elif ltype == 'int':
            try:
                return int(value).lower()

            except:
                self.dtree.warn('Invalid %s matchvalue "%s" requested. Should be integer.' % (valuetype, value), dtParseWarning, 2)

        else:
            return value


    def match_node(self, node_def = None, links = None, only_check_validity = False):
        # Return None if node_def is not a node definition then only collect any link request
        # Detailed in HTML/JSON class, return True on matching the node_def
        # Return False on failure to match
        self.links["values"] = {}
        self.links["nodes"] = {}
        return False

    def find_name(self, node_def):
        # Detailed in child class. Collect and return any name definition
        return None

    def find_value(self, node_def = None):
        # Detailed in child class Collect and return any value
        return self.dtree.calc_value(self.value, node_def)

    def print_node(self, print_all = False):
        # Detailed in child class
        return u'%s = %s' % (self.level, self.find_value())

    def print_node_def(self, node_def):
        spc = self.dtree.get_leveltabs(self.level,4)
        rstr = u'%snode_def: ' % spc
        for k, v in node_def.items():
            rstr = u'%s%s: %s\n%s          ' % (rstr, k, v, spc)

        return rstr.rstrip('\n').rstrip()

    def print_tree(self):
        sstr =u'%s%s' % (self.dtree.get_leveltabs(self.level,4), self.print_node(True))
        self.dtree.print_text(sstr)
        for n in self.children:
            n.print_tree()

# end DATAnode

class HTMLnode(DATAnode):
    def __init__(self, dtree, data = None, parent = None):
        self.tag = u''
        self.text = u''
        self.tail = u''
        self.attributes = {}
        self.attr_names = []
        DATAnode.__init__(self, dtree, parent)
        with self.node_lock:
            if isinstance(data, (str, unicode)):
                self.tag = data.lower().strip()

            elif isinstance(data, list):
                if len(data) > 0:
                    self.tag = data[0].lower().strip()

                if len(data) > 1 and isinstance(data[1], (list, tuple)):
                    for a in data[1]:
                        if isinstance(a[1], (str, unicode)):
                            self.attributes[a[0].lower().strip()] = a[1].strip()

                        else:
                            self.attributes[a[0].lower().strip()] = a[1]

                    if 'class' in self.attributes.keys():
                        self.attr_names.append('class')

                    if 'id' in self.attributes.keys():
                        self.attr_names.append('id')

                    for a in self.attributes.keys():
                        if a not in self.attr_names:
                            self.attr_names.append(a)

    def get_attribute(self, name):
        if name.lower() in self.attributes.keys():
            return self.attributes[name.lower()]

        return None

    def is_attribute(self, name, value = None):
        if name.lower() in self.attributes.keys():
            if value == None or value.lower() == self.attributes[name.lower()].lower():
                return True

        return False

    def get_child(self, tag = None, attributes = None):
        childs = []
        if not isinstance(attributes,list):
            attributes = []

        for c in self.children:
            if c.match_node(tag, attributes):
                childs.append(c)

        return childs

    def match_node(self, tag = None, attributes = None, node_def = None, links=None, only_check_validity = False):
        self.links["values"] = {}
        if not isinstance(links, dict):
            links = {"values": {},"nodes": {}}

        if not is_data_value("values", links, dict):
            links["values"] = {}

        if not is_data_value("nodes", links, dict):
            links["nodes"] = {}

        if node_def == None:
            # It's not a selection through a node_def
            if not isinstance(attributes,list):
                attributes = []

            if tag.lower() in (None, self.tag.lower()):
                if attributes == None:
                    return True

                if not isinstance(attributes, dict):
                    return False

                for a, v in attributes.items():
                    if not self.is_attribute(a, v):
                        return False

                return True

            else:
                return False

        elif is_data_value('tag', node_def):
            if self.get_value(node_def["tag"], links["values"], 'tag', 'lower') in (None, self.tag.lower()):
                # The tag matches
                if not self.check_index(node_def, links["values"]) in (True, None):
                    return False

            else:
                # The tag doesn't matches
                return False

        elif is_data_value('tags', node_def, list):
            if self.tag.lower() in self.get_value_list(data_value('tags', node_def, list), links["values"], 'tag', 'lower'):
                # The tag matches
                if not self.check_index(node_def, links["values"]) in (True, None):
                    return False

            else:
                # The tag doesn't matches
                return False

        elif is_data_value('index', node_def):
            if self.check_index(node_def, links["values"]) in (False, None):
                return False

        elif not is_data_value('path', node_def):
            # It's not a node definition
            if only_check_validity:
                self.check_for_linkrequest(node_def)

            return None

        for kw in ('text', 'tail'):
            if (is_data_value([kw,'link'], node_def, int) or is_data_value(kw, node_def, str)) \
              and self.get_value(node_def[kw], links["values"], kw, 'lower') != self.text.lower():
                return False

        if is_data_value('attrs', node_def, (dict, list)):
            ck = data_value('attrs', node_def)
            if not is_data_value('attrs', node_def, list):
                ck = [ck]

            for cd in ck:
                if not isinstance(cd, dict):
                    continue

                for a, v in cd.items():
                    if is_data_value('not', v, list):
                        # There is a negative attrib match requested
                        if not self.is_attribute(a):
                            # but the attribute is not there
                            continue

                        alist = self.get_value_list(data_value('not', v, list), links["values"], 'attribute', 'str')
                        if len(alist) == 0:
                            # No values to exclude
                            continue

                        elif (len(alist) == 1 and alist[0] == None) or self.attributes[a] in alist:
                            # the current value is in the list so we exclude
                            return False

                    else:
                        if not self.is_attribute(a):
                            # but the attribute is not there
                            return False

                        alist = self.get_value_list(v, links["values"], 'attribute', 'str')
                        if v == None or (len(alist) == 1 and alist[0] == None):
                            # All values are OK so continue
                            continue

                        elif len(alist) == 0 or not self.attributes[a] in alist:
                            # No values  specified or not present so exclude
                            return False

        if is_data_value('notattrs', node_def, (dict, list)):
            ck = data_value('notattrs', node_def)
            if not is_data_value('notattrs', node_def, list):
                ck = [ck]

            for cd in ck:
                if not isinstance(cd, dict):
                    continue

                for a, v in cd.items():
                    if self.is_attribute(a):
                        alist = self.get_value_list(v, links["values"], 'notattrs', 'str')
                        if v == None or (len(alist) == 1 and alist[0] == None):
                            # All values are OK so exclude
                            return False

                        elif len(alist) > 0 and self.attributes[a] in alist:
                            # The attribute is in the ban list so exclude
                            return False

        if not only_check_validity:
            self.check_for_linkrequest(node_def)

        return True

    def find_name(self, node_def):
        sv = None
        if is_data_value('name', node_def, dict):
            if is_data_value(['name','select'], node_def, str):
                if node_def[ 'name']['select'] == 'tag':
                    sv = self.tag

                elif node_def[ 'name']['select'] == 'text':
                    sv = self.text

                elif node_def[ 'name']['select'] == 'tail':
                    sv = self.tail

            elif is_data_value(['name','attr'], node_def, str):
                sv = self.get_attribute(node_def['name'][ 'attr'].lower())

        if sv != None:
            nv = self.dtree.calc_value(sv, node_def['name'])
            if self.dtree.show_result:
                if isinstance(nv, (str,unicode)):
                    self.dtree.print_text(u'  storing name = "%s" from node: %s\n      %s' % \
                        (nv, self.print_node(), self.print_node_def(node_def['name'])))

                else:
                    self.dtree.print_text(u'  storing name = %s from node: %s\n      %s' % \
                        (nv, self.print_node(), self.print_node_def(node_def['name'])))
            return nv

    def find_value(self, node_def = None):
        def add_child_text(child, depth, in_text = None, ex_text = None):
            t = u''
            if in_text != None:
                if child.tag in in_text:
                    if child.text != '':
                        t = u'%s %s' % (t, child.text)

                    if depth > 1:
                        for c in child.children:
                            t = u'%s %s' % (t, add_child_text(c, depth - 1, in_text, ex_text))

            elif ex_text != None:
                if not child.tag in ex_text:
                    if child.text != '':
                        t = u'%s %s' % (t, child.text)

                    if depth > 1:
                        for c in child.children:
                            t = u'%s %s' % (t, add_child_text(c, depth - 1, in_text, ex_text))

            else:
                if child.text != '':
                    t = u'%s %s' % (t, child.text)

                if depth > 1:
                    for c in child.children:
                        t = u'%s %s' % (t, add_child_text(c, depth - 1, in_text, ex_text))

            if child.tail != '':
                t = u'%s %s' % (t, child.tail)

            return t.strip()

        if is_data_value('value', node_def):
            sv = node_def['value']

        elif is_data_value('attr', node_def, str):
            sv = self.get_attribute(node_def[ 'attr'].lower())

        elif is_data_value('select', node_def, str):
            if node_def[ 'select'] == 'index':
                sv = self.child_index

            elif node_def[ 'select'] == 'tag':
                sv = self.tag

            elif node_def[ 'select'] == 'text':
                sv = self.text

            elif node_def[ 'select'] == 'tail':
                sv = self.tail

            elif node_def[ 'select'] == 'presence':
                return True

            elif node_def[ 'select'] == 'inclusive text':
                sv = self.text
                depth = data_value('depth', node_def, int, 1)
                in_text = None
                ex_text = None
                if is_data_value('include', node_def, list):
                    in_text = data_value('include', node_def, list)

                elif is_data_value('exclude', node_def, list):
                    ex_text = data_value('exclude', node_def, list)

                for c in self.children:
                    sv = u'%s %s' % (sv, add_child_text(c, depth, in_text, ex_text))

            else:
                sv = self.text

        else:
            sv = self.text

        return self.dtree.calc_value(sv, node_def)

    def print_node(self, print_all = False):
        attributes = u''
        spc = self.dtree.get_leveltabs(self.level,4)
        if len(self.attributes) > 0:
            for a in self.attr_names:
                v = self.attributes[a]
                if isinstance(v, (str,unicode)):
                    v = re.sub('\r','', v)
                    v = re.sub('\n', ' ', v)
                attributes = u'%s%s = "%s",\n    %s' % (attributes, a, v, spc)
            attributes = attributes[:-(len(spc)+6)]

        rstr = u'%s: %s(%s)' % (self.level, self.tag, attributes)
        rstr = u'%s\n    %sindex: %s' % (rstr, spc, self.child_index)
        if print_all:
            if self.text != '':
                rstr = u'%s\n    %stext: %s' % (rstr, spc, self.text)

            if self.tail != '':
                rstr = u'%s\n    %stail: %s' % (rstr, spc, self.tail)

        else:
            tx = self.find_value()
            if tx != "":
                rstr = u'%s\n    %s%s' % (rstr, spc, tx)

        return rstr
# end HTMLnode

class JSONnode(DATAnode):
    def __init__(self, dtree, data = None, parent = None, key = None):
        self.type = "value"
        self.key = key
        self.keys = []
        self.key_index = {}
        self.value = None
        DATAnode.__init__(self, dtree, parent)
        with self.node_lock:
            if isinstance(data, list):
                self.type = "list"
                for k in range(len(data)):
                    JSONnode(self.dtree, data[k], self, k)

            elif isinstance(data, dict):
                self.type = "dict"
                for k, item in data.items():
                    JSONnode(self.dtree, item, self, k)

            else:
                self.type = "value"
                self.value = data

    def append_child(self, node):
        with self.node_lock:
            node.child_index = len(self.children)
            self.key_index[node.key] = node.child_index
            self.children.append(node)
            self.keys.append(node.key)

    def get_child(self, key):
        if key in self.keys:
            return self.children[self.key_index[key]]

        return None

    def match_node(self, node_def = None, links = None, only_check_validity = False):
        self.links["values"] = {}
        if not isinstance(links, dict):
            links = {"values": {},"nodes": {}}

        if not is_data_value("values", links, dict):
            links["values"] = {}

        if not is_data_value("nodes", links, dict):
            links["nodes"] = {}

        if is_data_value('key', node_def):
            #~ if is_data_value(['key','link'], node_def, int):
                #~ kl = self.get_link(data_value(['key'], node_def, dict), links["values"])

            #~ else:
                #~ kl = node_def["key"]

            #~ if not self.key == kl:
            if self.get_value(node_def["key"], links["values"], 'key') != self.key:
                # The requested key doesn't matches
                return False

        elif is_data_value('keys', node_def, list):
            #~ klist = []
            #~ for index in range(len(data_value('keys', node_def, list))):
                #~ if is_data_value(['keys', index,'link'], node_def, int):
                    #~ klist.append(self.get_link(data_value(['keys', index], node_def, dict), links["values"]))

                #~ else:
                    #~ klist.append(node_def['keys', index])

            #~ if self.key in klist:
            if self.key in self.get_value_list(data_value('keys', node_def, list), links["values"], 'key'):
                # This key is in the list with requested keys
                if not self.check_index(node_def, links["values"]) in (True, None):
                    return False

            else:
                # This key isn't in the list with requested keys
                return False

        elif is_data_value('index', node_def):
            if self.check_index(node_def, links["values"]) in (False, None):
                return False

        elif not is_data_value('path', node_def):
            # It's not a node definition
            if only_check_validity:
                self.check_for_linkrequest(node_def)

            return None

        if is_data_value('childkeys', node_def, (dict, list)) or is_data_value('keys', node_def, dict):
            ck = []
            if is_data_value('childkeys', node_def, dict):
                ck = [node_def['childkeys']]

            elif is_data_value('childkeys', node_def, list):
                ck = node_def['childkeys']

            elif is_data_value('keys', node_def, dict):
                ck = [node_def['keys']]

            for cd in ck:
                if not isinstance(cd, dict):
                    continue

                for k, v in cd.items():
                    if is_data_value('not', v, list):
                        # There is a negative childkey match requested
                        if not  k in self.keys:
                            # but the childkey is not there
                            continue

                        alist = self.get_value_list(data_value('not', v, list), links["values"], 'childkeys')
                        if len(alist) == 0:
                            # No values to exclude
                            continue

                        elif (len(alist) == 1 and alist[0] == None) or self.get_child(k).value in alist:
                            # the current value is in the list so we exclude
                            return False

                    else:
                        if not  k in self.keys:
                            # but the childkey is not there
                            return False

                        alist = self.get_value_list(v, links["values"], 'childkeys')
                        if v == None or (len(alist) == 1 and alist[0] == None):
                            # All values are OK so continue
                            continue

                        elif len(alist) == 0 or not self.get_child(k).value in alist:
                            # No values  specified or not present so exclude
                            return False

        if is_data_value('notchildkeys', node_def, (dict, list)):
            ck = data_value('notchildkeys', node_def)
            if not is_data_value('notchildkeys', node_def, list):
                ck = [ck]

            for cd in ck:
                if not isinstance(cd, dict):
                    continue

                for k, v in cd.items():
                    if k in self.keys:
                        alist = self.get_value_list(v, links["values"], 'notchildkeys')
                        if v == None or (len(alist) == 1 and alist[0] == None):
                            # All values are OK so exclude
                            return False

                        elif len(alist) > 0 and self.get_child(k).value in alist:
                            # The attribute is in the ban list so exclude
                            return False

        if not only_check_validity:
            self.check_for_linkrequest(node_def)

        return True

    def find_name(self, node_def):
        sv = None
        if is_data_value('name', node_def, dict):
            if is_data_value(['name','select'], node_def, str):
                if node_def[ 'name']['select'] == 'key':
                    sv = self.key

                elif node_def[ 'name']['select'] == 'value':
                    sv = self.value

        if sv != None:
            nv = self.dtree.calc_value(sv, node_def['name'])
            if self.dtree.show_result:
                if isinstance(lv, (str,unicode)):
                    self.dtree.print_text(u'  storing name = "%s" from node: %s\n      %s' % \
                        (nv, self.print_node(), self.print_node_def(node_def['name'])))

                else:
                    self.dtree.print_text(u'  storing name = %s from node: %s\n      %s' % \
                        (nv, self.print_node(), self.print_node_def(node_def['name'])))
            return nv

    def find_value(self, node_def = None):
        if is_data_value('value', node_def):
            sv = node_def['value']

        elif is_data_value('select', node_def):
            if node_def[ 'select'] == 'index':
                sv = self.child_index

            elif node_def[ 'select'] == 'key':
                sv = self.key

            elif node_def[ 'select'] == 'value':
                sv = self.value

            elif node_def[ 'select'] == 'presence':
                return True

            else:
                sv = self.value

        else:
            sv = self.value

        return self.dtree.calc_value(sv, node_def)

    def print_node(self, print_all = False):
        value = self.find_value() if self.type == "value" else '"%s"' % self.type
        return u'%s = %s' % (self.key, value)

# end JSONnode

class DATAtree():
    def __init__(self, output = sys.stdout, warnaction = None, warngoal = sys.stderr, caller_id = 0):
        self.tree_lock = RLock()
        with self.tree_lock:
            self.print_searchtree = False
            self.show_result = False
            self.fle = output
            self.show_progress = False
            self.progress_queue = Queue()
            self.caller_id = caller_id
            self.extract_from_parent = False
            self.result = []
            self.quit = False
            self.data_def = {}
            self.month_names = []
            self.weekdays = []
            self.relative_weekdays = {}
            self.datetimestring = u"%Y-%m-%d %H:%M:%S"
            self.time_splitter = u':'
            self.time_type = [24]
            self.date_sequence = ["y","m","d"]
            self.date_splitter = u'-'
            self.utc = pytz.utc
            self.timezone = pytz.utc
            self.value_filters = {}
            self.str_list_splitter = '\|'
            if sys.modules['DataTreeGrab']._warnings == None:
                sys.modules['DataTreeGrab']._warnings = _Warnings(warnaction, warngoal, caller_id)

            elif caller_id not in sys.modules['DataTreeGrab']._warnings._ids:
                sys.modules['DataTreeGrab']._warnings.set_warnaction(warnaction, caller_id)

            elif warnaction != None:
                sys.modules['DataTreeGrab']._warnings.set_warnaction(warnaction, caller_id)

    def check_data_def(self, data_def):
        with self.tree_lock:
            self.data_def = data_def if isinstance(data_def, dict) else {}
            self.month_names = self.data_value("month-names", list)
            self.weekdays = self.data_value("weekdays", list)
            self.datetimestring = self.data_value("datetimestring", str, default = u"%Y-%m-%d %H:%M:%S")
            self.time_splitter = self.data_value("time-splitter", str, default = ':')
            self.date_sequence = self.data_value("date-sequence", list, default = ["y","m","d"])
            self.date_splitter = self.data_value("date-splitter", str, default = '-')
            if self.is_data_value('time-type', list) \
              and self.is_data_value(['time-type',0], int) \
              and self.data_value(['time-type',0], int) in (12, 24):
                self.time_type = self.data_value('time-type', list)

            self.set_timezone()
            self.value_filters = self.data_value("value-filters", dict)
            self.str_list_splitter = self.data_value("str-list-splitter", str, default = '\|')

    def set_timezone(self, timezone = None):
        with self.tree_lock:
            if isinstance(timezone, datetime.tzinfo):
                self.timezone = timezone

            else:
                if timezone == None:
                    timezone = self.data_value(["timezone"], str, default='utc')

                try:
                    oldtz = self.timezone
                    self.timezone = pytz.timezone(timezone)

                except:
                    if isinstance(oldtz, datetime.tzinfo):
                        self.warn('Invalid timezone "%s" suplied. Falling back to the old timezone "%s"' % (timezone, oldtz.tzname), dtdata_defWarning, 2)
                        self.timezone = oldtz

                    else:
                        self.warn('Invalid timezone "%s" suplied. Falling back to UTC' % (timezone), dtdata_defWarning, 2)
                        self.timezone = pytz.utc

            self.set_current_date()
            self.set_current_weekdays()

    def set_current_date(self, cdate = None):
        with self.tree_lock:
            if isinstance(cdate, datetime.datetime):
                if cdate.tzinfo == None:
                    self.current_date = self.timezone.localize(cdate).date()

                else:
                    self.current_date = self.timezone.normalize(cdate.astimezone(self.timezone)).date()

                self.current_ordinal = self.current_date.toordinal()

            elif isinstance(cdate, datetime.date):
                self.current_date = cdate
                self.current_ordinal = self.current_date.toordinal()

            elif isinstance(cdate, int):
                self.current_ordinal = cdate
                datetime.datetime.fromordinal(cdate)

            else:
                if cdate != None:
                    self.warn('Invalid or no current_date "%s" suplied. Falling back to NOW' % (cdate), dtdata_defWarning, 2)

                self.current_date = self.timezone.normalize(datetime.datetime.now(pytz.utc).astimezone(self.timezone)).date()
                self.current_ordinal = self.current_date.toordinal()

    def set_current_weekdays(self):
        with self.tree_lock:
            rw = self.data_value( "relative-weekdays", dict)
            for name, index in rw.items():
                self.relative_weekdays[name] = datetime.date.fromordinal(self.current_ordinal + index)

            current_weekday = self.timezone.normalize(datetime.datetime.now(pytz.utc).astimezone(self.timezone)).weekday()
            for index in range(len(self.weekdays)):
                name = self.weekdays[index]
                if index < current_weekday:
                    self.relative_weekdays[name] = datetime.date.fromordinal(self.current_ordinal + index + 7 - current_weekday)

                else:
                    self.relative_weekdays[name] = datetime.date.fromordinal(self.current_ordinal + index - current_weekday)

    def find_start_node(self, data_def=None):
        with self.tree_lock:
            if isinstance(data_def, dict):
                self.data_def = data_def

            if not isinstance(self.start_node, DATAnode):
                self.warn('Unable to set a start_node. Invalid dataset!', dtDataWarning, 1)
                return dtStartNodeInvalid

            if self.print_searchtree:
                self.print_text('The root Tree:')
                self.start_node.print_tree()

            init_path = self.data_value(['data',"init-path"],list)
            if self.show_result:
                self.print_text(self.root.print_node())

            links = {"values": {},"nodes": {}}
            sn = self.root.get_children(path_def = init_path, links = links)
            if sn == None or len(sn) == 0 or not isinstance(sn[0], DATAnode):
                self.warn('"init-path": %s did not result in a valid node. Falling back to the rootnode' % (init_path), dtParseWarning, 2)
                self.start_node = self.root
                return dtStartNodeInvalid

            else:
                self.start_node = sn[0]
                return dtDataOK

    def find_data_value(self, path_def, start_node = None, links = None):
        with self.tree_lock:
            if not isinstance(path_def, (list, tuple)):
                self.warn('Invalid "path_def": %s supplied to "find_data_value"' % (path_def), dtParseWarning, 1)
                return

            if len(path_def) == 0:
                path_def = [{}]

            if start_node == None or not isinstance(start_node, DATAnode):
                start_node = self.start_node

            if not isinstance(start_node, DATAnode):
                self.warn('Unable to search the tree. Invalid dataset!', dtDataWarning, 1)
                return

            links = {"values": {},"nodes": {}} if links == None else links
            nlist = start_node.get_children(path_def = path_def, links = links)
            if data_value('select', path_def[-1], str) == 'presence':
                # We return True if exactly one node is found, else False
                return bool(isinstance(nlist, DATAnode) or (isinstance(nlist, list) and len(nlist) == 1 and  isinstance(nlist[0], DATAnode)))

            if nlist in ([], None):
                # Nothing found, so give the default or None
                if data_value('type', path_def[-1]) == 'list':
                    return []

                else:
                    return data_value('default', path_def[-1])

            if is_data_value('first', path_def[-1]) and isinstance(nlist, list):
                # There is a request to only return the first
                nlist = nlist[0]

            elif is_data_value('last', path_def[-1]) and isinstance(nlist, list):
                # There is a request to only return the last
                nlist = nlist[-1]

            # We found multiple values
            if (isinstance(nlist, list) and len(nlist) > 1) or (data_value('type', path_def[-1]) == 'list'):
                vlist = []
                for node in nlist:
                    if isinstance(node, DATAnode):
                        vlist.append(node.find_value(path_def[-1]))

                    elif isinstance(node, dict):
                        # There is a named subset of the found nodes
                        for k, v in node.items():
                            slist = []
                            for item in v:
                                if isinstance(item, DATAnode):
                                    slist.append(item.find_value(path_def[-1]))

                            vlist.append({k: slist})

                return vlist

            # We found one value
            if isinstance(nlist, list):
                nlist = nlist[0]

            if not isinstance(nlist, DATAnode):
                if isinstance(path_def, list) and len(path_def)>0:
                    if data_value('type', path_def[-1]) == 'list':
                        return []

                    else:
                        return data_value('default', path_def[-1])

            else:
                return nlist.find_value(path_def[-1])

    def extract_datalist(self, data_def=None):
        with self.tree_lock:
            if isinstance(data_def, dict):
                self.data_def = data_def

            if not isinstance(self.start_node, DATAnode):
                self.warn('Unable to search the tree. Invalid dataset!', dtDataWarning, 1)
                if self.show_progress:
                    self.progress_queue.put((0, 0))

                return dtStartNodeInvalid

            if self.print_searchtree:
                self.print_text('The %s Tree:' % self.start_node.print_node())
                self.start_node.print_tree()

            self.result = []
            # Are there multiple data definitions
            def_list = []
            if self.is_data_value(['data',"iter"],list):
                for def_item in self.data_value(['data','iter'],list):
                    if not isinstance(def_item, dict):
                        continue

                    dset = {}
                    dset["key-path"] = []
                    dset[""] = []
                    if is_data_value("key-path", def_item, list):
                        dset["key-path"] = def_item["key-path"]

                    if is_data_value("values2", def_item, list):
                        dset["values"] = def_item["values2"]

                    elif is_data_value("values", def_item, list):
                        dset["values"] = def_item["values"]

                    def_list.append(dset)

            # Or just one
            elif self.is_data_value('data',dict):
                def_item = self.data_value('data',dict)
                dset = {}
                dset["key-path"] = []
                dset[""] = []
                if is_data_value("key-path", def_item, list):
                    dset["key-path"] = def_item["key-path"]

                if is_data_value("values2", def_item, list):
                    dset["values"] = def_item["values2"]

                elif is_data_value("values", def_item, list):
                    dset["values"] = def_item["values"]

                def_list.append(dset)

            else:
                self.warn('No valid "data" keyword found in the "data_def": %s' % (data_def), dtParseWarning, 1)
                if self.show_progress:
                    self.progress_queue.put((0, 0))

                return dtDataDefInvalid

            for dset in def_list:
                # Get all the key nodes
                if is_data_value(["key-path"], dset, list):
                    kp = data_value(["key-path"], dset, list)
                    if len(kp) == 0:
                        continue

                    if self.show_result:
                        self.print_text(u'parsing keypath: %s' % (kp[0]))

                    links = {"values": {},"nodes": {}}
                    self.key_list = self.start_node.get_children(path_def = kp, links = links)
                    k_cnt = len(self.key_list)
                    k_item = 0
                    if self.show_progress:
                        self.progress_queue.put((k_item, k_cnt))

                    for k in self.key_list:
                        if self.quit:
                            return dtQuiting

                        k_item += 1
                        if self.show_progress:
                            self.progress_queue.put((k_item, k_cnt))

                        if not isinstance(k, DATAnode):
                            continue

                        # And if it's a valid node, find the belonging end_links
                        # and value (the last dict in a path list contains the value definition)
                        links = k.end_links
                        tlist = [k.find_value(kp[-1])]
                        for v in data_value(["values"], dset, list):
                            if not isinstance(v, list) or len(v) == 0:
                                tlist.append(None)
                                continue

                            if is_data_value('value', v[0]):
                                tlist.append(data_value('value',v[0]))
                                continue

                            if self.show_result:
                                self.print_text(u'parsing key %s' % ( [k.find_value(kp[-1])]))

                            pv = data_value('path', v[0])
                            if pv in links["nodes"].keys() or pv in ("root", "parent"):
                                dt = []
                                if len(v[0]) > 1:
                                    dt = [{}]
                                    for n, nd in v[0].items():
                                        if n != "path":
                                            dt[0][n] = nd

                                dt.extend(v[1:])

                            else:
                                dt = v

                            if pv in links["nodes"].keys():
                                kn = links["nodes"][pv]

                            elif pv == "root":
                                kn = self.root

                            elif pv == "parent" and self.extract_from_parent and isinstance(k.parent.parent, DATAnode):
                                kn = k.parent.parent

                            elif pv == "parent" and isinstance(k.parent, DATAnode):
                                kn = k.parent

                            elif self.extract_from_parent and isinstance(k.parent, DATAnode):
                                kn = k.parent

                            else:
                                kn = k

                            dv = self.find_data_value(dt, kn, links)

                            if isinstance(dv, NULLnode):
                                break

                            tlist.append(dv)

                        else:
                            self.result.append(tlist)

            if len(self.result) == 0:
                if self.show_progress:
                    self.progress_queue.put((0, 0))

                return dtNoData

            return dtDataOK

    def calc_value(self, value, node_def = None):
        def calc_warning(text, severity=4):
            self.warn('%s calculation Error on value: "%s"\n   Using node_def: %s' % (text, value, node_def), dtCalcWarning, severity, 3)

        if isinstance(value, (str, unicode)):
            if is_data_value('lower',  node_def):
                value = unicode(value).lower().strip()

            if is_data_value('upper',  node_def):
                value = unicode(value).upper().strip()

            if is_data_value('capitalize',  node_def):
                value = unicode(value).capitalize().strip()

            if is_data_value('ascii-replace', node_def, list) and len(node_def['ascii-replace']) > 0:
                arep = node_def['ascii-replace']
                value = value.lower()
                if len(arep) > 2:
                    value = re.sub(arep[2], arep[1], value)

                value = value.encode('ascii','replace')
                value = re.sub('\?', arep[0], value)

            # Is there something to strip of
            if is_data_value('lstrip', node_def, str):
                if value.strip().lower()[:len(node_def['lstrip'])] == node_def['lstrip'].lower():
                    value = unicode(value[len(node_def['lstrip']):]).strip()

            if is_data_value('rstrip', node_def, str):
                if value.strip().lower()[-len(node_def['rstrip']):] == node_def['rstrip'].lower():
                    value = unicode(value[:-len(node_def['rstrip'])]).strip()

            # Is there something to substitute
            if is_data_value('sub', node_def, list) and len(node_def['sub']) > 1:
                for i in range(int(len(node_def['sub'])/2)):
                    value = re.sub(node_def['sub'][i*2], node_def['sub'][i*2+1], value).strip()

            # Is there a split list
            if is_data_value('split', node_def, list) and len(node_def['split']) > 0:
                if not isinstance(node_def['split'][0],list):
                    slist = [node_def['split']]

                else:
                    slist = node_def['split']

                for sdef in slist:
                    if len(sdef) < 2 or not isinstance(sdef[0],(str,unicode)):
                        calc_warning('invalid split')
                        continue

                    try:
                        fill_char = sdef[0]
                        if fill_char in ('\\s', '\\t', '\\n', '\\r', '\\f', '\\v', ' ','\\s*', '\\t*', '\\n*', '\\r*', '\\f*', '\\v*'):
                            fill_char = ' '
                            value = value.strip()

                        dat = re.split(sdef[0],value)
                        if sdef[1] == 'list-all':
                            value = dat
                        elif isinstance(sdef[1], int):
                            value = dat[sdef[1]]
                            for i in range(2, len(sdef)):
                                if isinstance(sdef[i], int) and (( 0<= sdef[i] < len(dat)) or (-len(dat) <= sdef[i] < 0)):
                                    value = value + fill_char +  dat[sdef[i]]

                    except:
                        calc_warning('split')

        if is_data_value('multiplier', node_def, int) and not data_value('type', node_def, unicode) in ('timestamp', 'datestamp'):

            try:
                value = int(value) * node_def['multiplier']

            except:
                calc_warning('multiplier')

        if is_data_value('divider', node_def, int) and node_def['divider'] != 0:
            try:
                value = int(value) // node_def['divider']

            except:
                calc_warning('divider')

        # Is there a replace dict
        if is_data_value('replace', node_def, dict):
            if value == None or not isinstance(value, (str, unicode)):
                calc_warning('replace')

            elif value.strip().lower() in node_def['replace'].keys():
                value = node_def['replace'][value.strip().lower()]

            else:
                value = None

        # is there a default
        if value == None and is_data_value('default', node_def):
            value = node_def['default']

        # Make sure a string is unicode and free of HTML entities
        if isinstance(value, (str, unicode)):
            value = re.sub('\n','', re.sub('\r','', self.un_escape(unicode(value)))).strip()

        # is there a type definition in node_def
        if is_data_value('type', node_def, unicode):
            try:
                if node_def['type'] == 'timestamp':
                    val = value
                    if is_data_value('multiplier', node_def, int):
                        val = value/node_def['multiplier']

                    value = datetime.datetime.fromtimestamp(float(val), self.utc)

                elif node_def['type'] == 'datetimestring':
                    dts = self.datetimestring
                    if is_data_value('datetimestring', node_def, str):
                        dts = data_value('datetimestring', node_def, str)

                    date = self.timezone.localize(datetime.datetime.strptime(value, dts))
                    value = self.utc.normalize(date.astimezone(self.utc))

                elif node_def['type'] == 'time':
                    try:
                        tt = self.time_type
                        if is_data_value('time-type', node_def, list) \
                          and is_data_value(['time-type',0], node_def, int) \
                          and data_value(['time-type',0], node_def, int) in (12, 24):
                            tt = data_value('time-type', node_def, list)

                        if tt[0] == 12:
                            ttam = data_value(['time-type',1], node_def, str, 'am')
                            ttpm = data_value(['time-type',2], node_def, str, 'pm')
                            if value.strip()[-len(ttpm):].lower() == ttpm.lower():
                                ttype = 'pm'
                                tvalue = value.strip()[:-len(ttpm)].strip()

                            elif value.strip()[-len(ttam):].lower() == ttam.lower():
                                ttype = 'am'
                                tvalue = value.strip()[:-len(ttam)].strip()

                            else:
                                ttype = 'am'
                                tvalue = value.strip()

                        else:
                            ttype = '24'
                            tvalue = value.strip()

                        ts = self.time_splitter
                        if is_data_value('time-splitter', node_def, str):
                            ts = data_value('time-splitter', node_def, str)

                        t = re.split(ts, tvalue)
                        hour = int(data_value(0, t, str, '00'))
                        minute = int(data_value(1, t, str, '00'))
                        second = int(data_value(2, t, str, '00'))
                        if ttype == 'pm':
                            hour += 12

                        value = datetime.time(hour, minute, second)

                    except:
                        calc_warning('time type')

                elif node_def['type'] == 'timedelta':
                    try:
                            value = datetime.timedelta(seconds = int(value))

                    except:
                        calc_warning('timedelta type')

                elif node_def['type'] == 'date':
                    try:
                        day = self.current_date.day
                        month = self.current_date.month
                        year = self.current_date.year
                        ds = self.date_splitter
                        if is_data_value('date-splitter', node_def, str):
                            ds = data_value('date-splitter', node_def, str)

                        dseq = self.date_sequence
                        if is_data_value('date-sequence', node_def, list):
                            dseq = data_value('date-sequence', node_def, list)

                        d = re.split(ds, value)
                        for index in range(len(d)):
                            if index > len(dseq)-1:
                                break

                            if not dseq[index].lower() in ('d', 'm', 'y'):
                                continue

                            try:
                                d[index] = int(d[index])

                            except ValueError:
                                if d[index].lower() in self.month_names:
                                    d[index] = self.month_names.index(d[index].lower())

                                else:
                                    calc_warning('invalid "%s" value for date type' % (dseq[index]))
                                    continue

                            if dseq[index].lower() == 'd':
                                day = d[index]

                            if dseq[index].lower() == 'm':
                                month = d[index]

                            if dseq[index].lower() == 'y':
                                year = d[index]

                        value = datetime.date(year, month, day)

                    except:
                        calc_warning('date type')

                elif node_def['type'] == 'datestamp':
                    val = value
                    if is_data_value('multiplier', node_def, int):
                        val = value/node_def['multiplier']

                    value = datetime.date.fromtimestamp(float(val))

                elif node_def['type'] == 'relative-weekday':
                    if value.strip().lower() in self.relative_weekdays.keys():
                        value = self.relative_weekdays[value.strip().lower()]

                elif node_def['type'] == 'string':
                    value = unicode(value)

                elif node_def['type'] == 'int':
                    try:
                        value = int(value)

                    except:
                        calc_warning('int type')
                        value = 0

                elif node_def['type'] == 'float':
                    try:
                        value = float(value)

                    except:
                        calc_warning('float type')
                        value = 0

                elif node_def['type'] == 'boolean':
                    if not isinstance(value, bool):
                        if isinstance(value, (int, float)):
                            value = bool(value>0)

                        elif isinstance(value, (str, unicode)):
                            value = bool(len(value) > 0 and value != '0')

                        else:
                            value = False

                elif node_def['type'] == 'lower-ascii' and isinstance(value, (str, unicode)):
                    value = value.lower()
                    value =re.sub('[ /]', '_', value)
                    value =re.sub('[!(),]', '', value)
                    value = re.sub('á','a', value)
                    value = re.sub('à','a', value)
                    value = re.sub('ä','a', value)
                    value = re.sub('â','a', value)
                    value = re.sub('ã','a', value)
                    value = re.sub('@','a', value)
                    value = re.sub('é','e', value)
                    value = re.sub('è','e', value)
                    value = re.sub('ë','e', value)
                    value = re.sub('ê','e', value)
                    value = re.sub('í','i', value)
                    value = re.sub('ì','i', value)
                    value = re.sub('ï','i', value)
                    value = re.sub('î','i', value)
                    value = re.sub('ó','o', value)
                    value = re.sub('ò','o', value)
                    value = re.sub('ö','o', value)
                    value = re.sub('ô','o', value)
                    value = re.sub('õ','o', value)
                    value = re.sub('ú','u', value)
                    value = re.sub('ù','u', value)
                    value = re.sub('ü','u', value)
                    value = re.sub('û','u', value)
                    value = re.sub('ý','y', value)
                    value = re.sub('ÿ','y', value)
                    value = value.encode('ascii','replace')

                elif node_def['type'] == 'str-list':
                    try:
                        sls = self.str_list_splitter
                        if is_data_value('str-list-splitter', node_def, str):
                            sls = data_value('str-list-splitter', node_def, str)

                        value = list(re.split(sls, value))
                        if data_value("omit-empty-list-items", node_def, bool, False):
                            while '' in value:
                                value.remove('')

                            while None in value:
                                value.remove(None)

                    except:
                        calc_warning('str-list type')

                elif node_def['type'] == 'list':
                    # this is handled in find_data_value to prefent double listing
                    pass

                elif node_def['type'] == '':
                    pass

            except:
                calc_warning('type')

        if is_data_value('member-off', node_def, unicode) and data_value('member-off', node_def, unicode) in self.value_filters.keys():
            vf = self.value_filters[data_value('member-off', node_def, unicode)]
            if not value in vf:
                value = NULLnode()

        return value

    def un_escape(self, text):
        # Removes HTML or XML character references and entities from a text string.
        # source: http://effbot.org/zone/re-sub.htm#unescape-html
        #
        # @param text The HTML (or XML) source text.
        # @return The plain text, as a Unicode string

        def fixup(m):
            text = m.group(0)
            if text[:2] == "&#":
                # character reference
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))

                    else:
                        return unichr(int(text[2:-1]))

                except ValueError:
                    pass

            else:
                # named entity
                try:
                    text = unichr(name2codepoint[text[1:-1]])

                except KeyError:
                    pass

            return text # leave as is

        if not isinstance(text,(str, unicode)):
            return text

        return unicode(re.sub("&#?\w+;", fixup, text))
    def print_text(self, text):
        if self.fle in (sys.stdout, sys.stderr):
            self.fle.write(text.encode('utf-8', 'replace'))

        else:
            self.fle.write(u'%s\n' % text)

    def get_leveltabs(self, level, spaces=3):
        stab = u''
        for i in range(spaces):
            stab += u' '

        sstr = u''
        for i in range(level):
            sstr += stab

        return sstr

    def simplefilter(self, action, category=Warning, lineno=0, append=0, severity=0):
        with self.tree_lock:
            sys.modules['DataTreeGrab']._warnings.simplefilter(action, category, lineno, append, self.caller_id, severity)

    def warn(self, message, category, severity, stacklevel = 2):
        sys.modules['DataTreeGrab']._warnings.warn(message, category, self.caller_id, severity, stacklevel)

    def is_data_value(self, searchpath, dtype = None, empty_is_false = False):
        return is_data_value(searchpath, self.data_def, dtype, empty_is_false)

    def data_value(self, searchpath, dtype = None, default = None):
        return data_value(searchpath, self.data_def, dtype, default)

# end DATAtree

class HTMLtree(HTMLParser, DATAtree):
    def __init__(self, data, autoclose_tags=[], print_tags = False, output = sys.stdout, warnaction = "default", warngoal = sys.stderr, caller_id = 0):
        HTMLParser.__init__(self)
        DATAtree.__init__(self, output, warnaction, warngoal, caller_id)
        with self.tree_lock:
            self.print_tags = print_tags
            self.autoclose_tags = autoclose_tags
            self.is_tail = False
            self.root = HTMLnode(self, 'root')
            self.current_node = self.root
            self.last_node = None
            self.text = u''
            self.open_tags = {}
            self.count_tags(data)
            # read the html page into the tree
            try:
                # Cover for incomplete reads where the essentiel body part is retrieved
                if u'<body>' in data and not u'</body>' in data:
                    data = u'%s</body>' % data

                if u'<BODY>' in data and not u'</BODY>' in data:
                    data = u'%s</BODY>' % data

                if u'<html>' in data and not u'</html>' in data:
                    data = u'%s</html>' % data

                if u'<HTML>' in data and not u'</HTML>' in data:
                    data = u'%s</HTML>' % data

                self.feed(data)
                self.reset()
                self.start_node = self.root

            except:
                self.warn('Unable to parse the HTML data. Invalid dataset!', dtDataWarning, 1)
                self.start_node = NULLnode()

    def count_tags(self, data):
        tag_list = re.compile("\<(.*?)\>", re.DOTALL)
        self.tag_count = {}
        for t in tag_list.findall(data):
            if t[0] == '\\':
                t = t[1:]

            if t[0] == '/':
                sub = 'close'
                tag = t.split (' ')[0][1:].lower()

            elif t[:3] == '!--':
                continue
                sub = 'comment'
                tag = t[3:].lower()

            elif t[0] == '?':
                continue
                sub = 'pi'
                tag = t[1:].lower()

            elif t[0] == '!':
                continue
                sub = 'html'
                tag = t[1:].lower()

            elif t[-1] == '/':
                sub = 'auto'
                tag = t.split(' ')[0].lower()

            else:
                sub = 'start'
                tag = t.split (' ')[0].lower()

            if not tag in self.tag_count.keys():
                self.tag_count[tag] ={}
                self.tag_count[tag]['close'] = 0
                self.tag_count[tag]['comment'] = 0
                self.tag_count[tag]['pi'] = 0
                self.tag_count[tag]['html'] = 0
                self.tag_count[tag]['auto'] = 0
                self.tag_count[tag]['start'] = 0

            self.tag_count[tag][sub] += 1

        for t, c in self.tag_count.items():
            if c['close'] == 0 and (c['start'] >0 or c['auto'] > 0):
                self.autoclose_tags.append(t)

            if self.print_tags:
                self.print_text(u'%5.0f %5.0f %5.0f %s' % (c['start'], c['close'], c['auto'], t))

    def handle_starttag(self, tag, attrs):
        if not tag in self.open_tags.keys():
            self.open_tags[tag] = 0

        self.open_tags[tag] += 1
        if self.print_tags:
            if len(attrs) > 0:
                self.print_text(u'%sstarting %s %s %s' % (self.get_leveltabs(self.current_node.level,2), self.current_node.level+1, tag, attrs[0]))
                for a in range(1, len(attrs)):
                    self.print_text(u'%s        %s' % (self.get_leveltabs(self.current_node.level,2), attrs[a]))

            else:
                self.print_text(u'%sstarting %s %s' % (self.get_leveltabs(self.current_node.level,2), self.current_node.level,tag))

        node = HTMLnode(self, [tag.lower(), attrs], self.current_node)
        self.add_text()
        self.current_node = node
        self.is_tail = False
        if tag.lower() in self.autoclose_tags:
            self.handle_endtag(tag)
            return False

        return True

    def handle_endtag(self, tag):
        if not tag in self.open_tags.keys() or self.open_tags[tag] == 0:
            return

        self.open_tags[tag] -= 1
        if self.current_node.tag != tag.lower():
            # To catch missing close tags
            self.handle_endtag(self.current_node.tag)

        self.add_text()
        if self.print_tags:
            if self.current_node.text.strip() != '':
                self.print_text(u'%s        %s' % (self.get_leveltabs(self.current_node.level-1,2), self.current_node.text.strip()))
            self.print_text(u'%sclosing %s %s %s' % (self.get_leveltabs(self.current_node.level-1,2), self.current_node.level,tag, self.current_node.tag))

        self.last_node = self.current_node
        self.is_tail = True
        self.current_node = self.current_node.parent
        if self.current_node.is_root:
            self.reset()

    def handle_startendtag(self, tag, attrs):
        if self.handle_starttag(tag, attrs):
            self.handle_endtag(tag)

    def handle_data(self, data):
        data = re.sub("", "...", data)
        data = re.sub("", "'", data)
        data = re.sub("", "'", data)
        data = re.sub("", "", data)
        self.text += data

    def handle_entityref(self, name):
        try:
            c = unichr(name2codepoint[name])
            self.text += c

        except:
            pass

    def handle_charref(self, name):
        if name.startswith('x'):
            c = unichr(int(name[1:], 16))

        else:
            c = unichr(int(name))

        self.text += c

    def handle_comment(self, data):
        # <!--comment-->
        pass

    def handle_decl(self, decl):
        # <!DOCTYPE html>
        pass

    def handle_pi(self, data):
        # <?proc color='red'>
        pass

    def add_text(self):
        if self.is_tail:
            self.last_node.tail += unicode(re.sub('\n','', re.sub('\r','', self.text)).strip())

        else:
            self.current_node.text += unicode(re.sub('\n','', re.sub('\r','', self.text)).strip())

        self.text = u''

    def remove_text(self):
        if self.is_tail:
            self.text += self.current_node.tail
            self.current_node.tail = u''

        else:
            self.text += self.current_node.text
            self.current_node.text = u''

# end HTMLtree

class JSONtree(DATAtree):
    def __init__(self, data, output = sys.stdout, warnaction = "default", warngoal = sys.stderr, caller_id = 0):
        DATAtree.__init__(self, output, warnaction, warngoal, caller_id)
        with self.tree_lock:
            self.extract_from_parent = True
            self.data = data
            # Read the json data into the tree
            try:
                self.root = JSONnode(self, data, key = 'ROOT')
                self.start_node = self.root

            except:
                self.warn('Unable to parse the JSON data. Invalid dataset!', dtDataWarning, 1)
                self.start_node = NULLnode()

# end JSONtree

class DataTreeShell():
    def __init__(self, data_def, data = None, warnaction = "default", warngoal = sys.stderr, caller_id = 0):
        self.tree_lock = RLock()
        with self.tree_lock:
            self.caller_id = caller_id
            self.print_tags = False
            self.print_searchtree = False
            self.show_result = False
            self.fle = sys.stdout
            if sys.modules['DataTreeGrab']._warnings == None:
                sys.modules['DataTreeGrab']._warnings = _Warnings(warnaction, warngoal, caller_id)

            else:
                sys.modules['DataTreeGrab']._warnings.set_warnaction(warnaction, caller_id)

            self.searchtree = None
            self.timezone = pytz.utc
            self.errorcode = dtDataInvalid
            self.result = []
            self.data_def = data_def if isinstance(data_def, dict) else {}
            self.init_data_def()
            if data != None:
                self.init_data(data)

    def init_data_def(self, data_def = None, init_start_node = True):
        with self.tree_lock:
            if isinstance(data_def, dict):
                self.data_def = data_def

            self.set_timezone()
            self.empty_values = self.data_value('empty-values', list, default = [None, ''])
            if isinstance(self.searchtree, DATAtree):
                self.searchtree.check_data_def(self.data_def)
                if init_start_node:
                    self.set_errorcode(self.searchtree.find_start_node(), True)

    def set_timezone(self, timezone = None):
        with self.tree_lock:
            if isinstance(timezone, datetime.tzinfo):
                self.timezone = timezone

            else:
                if timezone == None:
                    timezone = self.data_value(["timezone"], str, default='utc')

                try:
                    oldtz = self.timezone
                    self.timezone = pytz.timezone(timezone)

                except:
                    if isinstance(oldtz, datetime.tzinfo):
                        self.warn('Invalid timezone "%s" suplied. Falling back to the old timezone "%s"' % (timezone, oldtz.tzname), dtdata_defWarning, 2)
                        self.set_errorcode(dtTimeZoneFailed)
                        self.timezone = oldtz

                    else:
                        self.warn('Invalid timezone "%s" suplied. Falling back to UTC' % (timezone), dtdata_defWarning, 2)
                        self.set_errorcode(dtTimeZoneFailed)
                        self.timezone = pytz.utc

            self.set_current_date()
            if isinstance(self.searchtree, DATAtree):
                self.searchtree.set_timezone(self.timezone)

    def set_current_date(self, cdate = None):
        with self.tree_lock:
            if isinstance(cdate, datetime.datetime):
                if cdate.tzinfo == None:
                    self.current_date = self.timezone.localize(cdate).date()

                else:
                    self.current_date = self.timezone.normalize(cdate.astimezone(self.timezone)).date()

                self.current_ordinal = self.current_date.toordinal()

            elif isinstance(cdate, datetime.date):
                self.current_date = cdate
                self.current_ordinal = self.current_date.toordinal()

            elif isinstance(cdate, int):
                self.current_ordinal = cdate
                self.current_date = datetime.date.fromordinal(cdate)

            else:
                if cdate != None:
                    self.set_errorcode(dtCurrentDateFailed)
                    self.warn('Invalid or no current_date "%s" suplied. Falling back to NOW' % (cdate), dtdata_defWarning, 2)

                self.current_date = self.timezone.normalize(datetime.datetime.now(pytz.utc).astimezone(self.timezone)).date()
                self.current_ordinal = self.current_date.toordinal()

    def get_url(self, url_data = None):
        # "url", "encoding", "accept-header", "url-data", "data-format", 'default-item-count', "item-range-splitter"
        # "url-date-type" 0 = offset or formated string, 1 = timestamp, 2 = weekday
        # "url-date-format", "url-date-multiplier", "url-weekdays"
        # 'url-var', 'count', 'cnt-offset', 'offset'
        def get_url_part(u_part):
            if isinstance(u_part, (str, unicode)):
                return u_part

            # get a variable
            elif isinstance(u_part, int):
                urlid = u_part
                u_data = []

            elif isinstance(u_part, list):
                if is_data_value(0, u_part, int):
                    urlid = u_part[0]
                    u_data = u_part[1:]

                else:
                    urlid = 0
                    u_data = u_part

            else:
                return None

            return self.url_functions(urlid, u_data)

        with self.tree_lock:
            self.url_data = url_data
            if self.is_data_value(["url"], str):
                url = self.data_value(["url"], str)

            elif self.is_data_value(["url"], list):
                url = u''
                for u_part in self.data_value(["url"], list):
                    uval = get_url_part(u_part)
                    if uval == None:
                        self.warn('Invalid url_part definition: %s' % (u_part), dtUrlWarning, 1)
                        return None

                    else:
                        url += unicode(uval)

            else:
                self.warn('Missing or invalid "url" keyword.', dtUrlWarning, 1)
                return None

            encoding = self.data_value(["encoding"], str,)
            if self.is_data_value("url-header", dict):
                accept_header = {}
                for k, v in self.data_value(["url-header"], dict).items():
                    uval = get_url_part(v)
                    if uval == None:
                        self.warn('Invalid url-header definition: %s' % (v), dtUrlWarning, 1)
                        return None

                    else:
                        accept_header[k] = uval

            else:
                accept_header = self.data_value(["accept-header"], str)

            url_data = {}
            for k, v in self.data_value(["url-data"], dict).items():
                uval = get_url_part(v)
                if uval == None:
                    self.warn('Invalid url-data definition: %s' % (v), dtUrlWarning, 1)
                    return None

                else:
                    url_data[k] = uval

            is_json = bool('json' in self.data_value(["data-format"], str))
            return (url, encoding, accept_header, url_data, is_json)

    def url_functions(self, urlid, data = None):
        def url_warning(text, severity=2):
            self.warn('%s on function: "%s": %s\n   Using url_data: %s' % (text, urlid, data, self.url_data), dtUrlWarning, severity, 3)

        def get_dtstring(dtordinal):
            try:
                return datetime.date.fromordinal(dtordinal).strftime(udf)

            except:
                url_warning('Invalid "url-date-format"')

        def get_timestamp(dtordinal):
            return int(time.mktime(datetime.date.fromordinal(dtordinal).timetuple())) * udm

        def get_weekday(dtordinal):
            wd = datetime.date.fromordinal(dtordinal).weekday()
            if len(uwd) == 7:
                return unicode(uwd[wd])

            url_warning('Invalid "url-weekdays"')
            return unicode(wd)

        try:
            if urlid > 99:
                retval =  self.add_on_url_functions(urlid, data)
                if retval in (None, ''):
                    url_warning('No result on custom url function')

                return retval

            udt = self.data_value(["url-date-type"], int, default=0)
            udf = self.data_value(["url-date-format"], str, default=None)
            udm = self.data_value(["url-date-multiplier"], int, default=1)
            uwd = self.data_value(["url-weekdays"], list)
            rwd = {}
            if self.is_data_value( "url-relative-weekdays", dict):
                wd = self.data_value( "url-relative-weekdays", dict)
                for dname, dno in wd.items():
                    try:
                        rwd[int(dno)] = dname

                    except:
                        pass

            if urlid == 0:
                # Return the value of the given variable in data
                # transposing a list or dict-key list to a comma separated list
                if is_data_value(0, data, str):
                    dkey = data[0]

                else:
                    dkey = 'url-var'

                if is_data_value(dkey, self.url_data, str):
                    return self.url_data[dkey]

                elif is_data_value(dkey, self.url_data, list):
                    cc = ''
                    for c in self.url_data[dkey]:
                        cc = u'%s,%s'% (cc, c)

                    return cc[1:]

                elif is_data_value(dkey, self.url_data, dict):
                    cc = ''
                    for c in self.url_data[dkey].values():
                        cc = u'%s,%s'% (cc, c)

                    return cc[1:]

                else:
                    url_warning('No value found')

            elif urlid == 4:
                # return a range from cnt_offset * cnt +1 to cnt_offset * cnt +cnt
                cnt = data_value('count', self.url_data, int, default=self.data_value(['default-item-count'], int, default=0))
                cnt_offset = data_value('cnt-offset', self.url_data, int, default=0)
                cstep = cnt_offset * cnt
                splitter = self.data_value(["item-range-splitter"], str, default='-')
                return u'%s%s%s' % (cstep + 1, splitter, cstep  + cnt)

            elif urlid == 11:
                if is_data_value(0, data, str):
                    dkey = data[0]

                else:
                    dkey = 'offset'

                offset = data_value(dkey, self.url_data, int, default=0)
                if udt == 0:
                    if udf not in (None, ''):
                        # vrt.be, npo.nl, vpro.nl, humo.nl
                        return get_dtstring(self.current_ordinal + offset)

                    else:
                        #tvgids.nl, tvgids.tv,
                        return unicode(offset)

                elif udt == 1:
                    # primo.eu,
                    return get_timestamp(self.current_ordinal + offset)

                elif udt == 2:
                    # oorboekje.nl, nieuwsblad.be
                    if offset in rwd.keys():
                        return unicode(rwd[offset])

                    return get_weekday(self.current_ordinal + offset)

                else:
                    url_warning('Invalid "url-date-type"')

            elif urlid == 14:
                if is_data_value(0, data, str):
                    startkey = data[0]

                else:
                    startkey = 'start'

                start = data_value(startkey, self.url_data, int, default=0)
                if is_data_value(1, data, str):
                    endkey = data[1]

                else:
                    endkey = 'end'

                end = data_value(endkey, self.url_data, int, default=0)
                if udt == 0:
                    if udf not in (None, ''):
                        start = get_dtstring(self.current_ordinal + start)
                        end = get_dtstring(self.current_ordinal + end)

                    else:
                        start = unicode(start)
                        end = unicode(end)

                elif udt == 1:
                    # horizon.tv
                    start = get_timestamp(self.current_ordinal + start)
                    end = get_timestamp(self.current_ordinal + end)

                elif udt == 2:
                    if start in rwd.keys():
                        start = unicode(rwd[start])
                    else:
                        start = get_weekday(self.current_ordinal + start)

                    if end in rwd.keys():
                        end = unicode(rwd[end])
                    else:
                        end = get_weekday(self.current_ordinal + end)

                else:
                    url_warning('Invalid "url-date-type"')

                splitter = self.data_value(["date-range-splitter"], str, default='~')
                return '%s%s%s' % (start, splitter, end )

            else:
                url_warning('Unknown Url function')
                return None

        except:
            self.warn('Unknown Url Error on function: "%s": %s\n   Using url_data: %s\n%s' % \
                (urlid, data, self.url_data, traceback.print_exc()), dtUrlWarning, 1)
            return None

    def add_on_url_functions(self, urlid, data = None):
        pass

    def init_data(self, data, init_start_node = True):
        def unquote(matchobj):
            rval = matchobj.group(0)
            try:
                for mg in matchobj.groups():
                    if mg == None:
                        continue

                    tt = mg
                    for s in (('"', '&quot;'), ('<', '&lt;'), ('>', '&gt;')):
                        if s[0] in tt:
                            tt = re.sub(s[0], s[1], tt)

                    rval = re.sub(re.escape(mg), tt, rval)
                return rval

            except:
                self.set_errorcode(dtUnquoteFailed)
                return rval

        def sort_list(page, path, childkeys):
            if is_data_value(path, page, list):
                if len(childkeys) == 1:
                    data_value(path, page, list).sort(key=lambda l: (l[childkeys[0]]))

                elif len(childkeys) == 2:
                    data_value(path, page, list).sort(key=lambda l: (l[childkeys[0]], l[childkeys[1]]))

                elif len(childkeys) > 2:
                    data_value(path, page, list).sort(key=lambda l: (l[childkeys[0]], l[childkeys[1]], l[childkeys[2]]))

            else:
                self.set_errorcode(dtSortFailed)
                self.warn('Sort request {"path": %s, "childkeys": %s}" failed\n   as "path" is not present in the data or is not a list!' % \
                    (path, childkeys), dtDataWarning, 2)

        with self.tree_lock:
            dttype = None
            self.searchtree = None
            self.errorcode = dtDataInvalid
            self.result = []
            if isinstance(data, (dict, list)):
                dttype = 'json'

            elif isinstance(data, (str, unicode)) and data.strip()[0] in ("{", "["):
                try:
                    data = json.loads(data)
                    dttype = 'json'

                except:
                    self.warn('Failed to initialise the searchtree. Run with a valid dataset %s' % type(data), dtDataWarning, 1)

            elif isinstance(data, (str, unicode)) and data.strip()[0] == "<":
                dttype = 'html'
                autoclose_tags = self.data_value(["autoclose-tags"], list)
                # Cover for incomplete reads where the essentiel body part is retrieved
                for ctag in ('body', 'BODY', 'html', 'HTML', 'xml', 'XML'):
                    if u'<%s>' % ctag in data and not u'</%s>' % ctag in data:
                        data = u'%s</%s>' % (data, ctag)

                if self.data_value(["enclose-with-html-tag"], bool, default=False):
                    data = u'<html>%s</html>' % data

                for subset in self.data_value(["text_replace"], list):
                    if isinstance(subset, list) and len(subset) >= 2:
                        try:
                            data = re.sub(subset[0], subset[1], data, 0, re.DOTALL)

                        except:
                            self.set_errorcode(dtTextReplaceFailed)
                            self.warn('An error occured applying "text_replace" regex: "%s"' % subset, dtDataWarning, 2)

                for ut in self.data_value(["unquote_html"], list):
                    if isinstance(ut, (str, unicode)):
                        try:
                            data = re.sub(ut, unquote, data, 0, re.DOTALL)

                        except:
                            self.set_errorcode(dtUnquoteFailed)
                            self.warn('An error occured applying "unquote_html" regex: "%s"' % ut, dtDataWarning, 2)

                self.searchtree = HTMLtree(data, autoclose_tags, self.print_tags, self.fle, caller_id = self.caller_id, warnaction = None)

            else:
                self.warn('Failed to initialise the searchtree. Run with a valid dataset', dtDataWarning, 1)

            if dttype == 'json':
                if self.is_data_value(['data', 'sort'], list):
                    # There is a sort request
                    for sitem in self.data_value(['data', 'sort'], list):
                        try:
                            sort_list(data, data_value('path', sitem, list), data_value('childkeys', sitem, list))

                        except:
                            self.set_errorcode(dtSortFailed)
                            self.warn('Sort request "%s" failed!' % (sitem), dtDataWarning, 2)

                self.searchtree = JSONtree(data, self.fle, caller_id = self.caller_id, warnaction = None)

            if  isinstance(self.searchtree, DATAtree) and isinstance(self.searchtree.start_node, DATAnode):
                self.set_errorcode(dtDataOK, True)
                self.searchtree.show_result = self.show_result
                self.searchtree.print_searchtree = self.print_searchtree
                self.searchtree.check_data_def(self.data_def)
                if init_start_node:
                    self.set_errorcode(self.searchtree.find_start_node(), True)

            return self.check_errorcode()

    def print_datatree(self, data = None, fobj = None, from_start_node = False):
        with self.tree_lock:
            if self.searchtree == None and data == None:
                self.warn('Nothing to print!', dtDataWarning, 4)
                return

            if data!= None and self.init_data(data, from_start_node):
                # Data was supplied but gave a fatal error on initializing
                self.warn('Nothing to print!', dtDataWarning, 4)
                return

            oldfobj = self.searchtree.fle
            if fobj != None:
                self.searchtree.fle = fobj

            if from_start_node:
                self.searchtree.start_node.print_tree()

            else:
                self.searchtree.root.print_tree()

            self.searchtree.fle = oldfobj

    def extract_datalist(self, init_start_node = False):
        with self.tree_lock:
            self.result = []
            x = self.check_errorcode()
            if x:
                self.warn('The searchtree has not jet been initialized. Run .init_data() first with a valid dataset', dtDataWarning, 1)
                return x

            if init_start_node:
                self.set_errorcode(self.searchtree.find_start_node(), True)
                x = self.check_errorcode()
                if x:
                    return x

            self.set_errorcode(self.searchtree.extract_datalist(), True)
            if not self.check_errorcode():
                if self.is_data_value("values", dict) and isinstance(self.searchtree.result, list):
                    for keydata in self.searchtree.result:
                        self.result.append(self.link_values(keydata))

                else:
                    self.warn('No valid "values" keyword found or no data retrieved to process', dtDataWarning, 2)
                    self.result = self.searchtree.result

            return self.check_errorcode()

    def link_values(self, linkdata):
        """
        Following the definition in the values definition.
        Her the data-list for every keyword
        retreived with the DataTree module is validated and linked to keywords
        A dict is return
        """
        def get_variable(vdef):
            varid = data_value("varid", vdef, int)
            if not ((isinstance(linkdata, list) and (0 <= varid < len(linkdata))) \
              or (isinstance(linkdata, dict) and varid in linkdata.keys())):
                self.warn('Requested datavalue "%s" does not exist in: %s'% (varid, linkdata), dtLinkWarning, 2)
                return

            # remove any leading or trailing spaces on a string/unicode value
            value = linkdata[varid] if (not  isinstance(linkdata[varid], (unicode, str))) else unicode(linkdata[varid]).strip()

            # apply any regex, type or calc statements
            if is_data_value('regex', vdef, str):
                value = get_regex(vdef, value)

            if is_data_value('type', vdef, str):
                value = check_type(vdef, value)

            if is_data_value('calc', vdef, dict):
                value = calc_value(vdef['calc'], value)

            return value

        def process_link_function(vdef):
            funcid = data_value("funcid", vdef, int)
            default = data_value("default", vdef)
            if funcid == None:
                self.warn('Invalid linkfunction ID "%s" in: %s'% (funcid, vdef), dtLinkWarning, 1)
                return

            # Process the datavalues given for the function
            funcdata = data_value("data", vdef, list)
            data = []
            for fd in funcdata:
                if is_data_value("varid", fd, int):
                    dvar = get_variable(fd)
                    if dvar == None:
                        data.append('')

                    else:
                        data.append(dvar)

                elif is_data_value("funcid", fd, int):
                    data.append(process_link_function(fd))

                else:
                    data.append(fd)

            # And call the linkfunction
            value = self.link_functions(funcid, data, default)
            if value in self.empty_values:
                return

            # apply any regex, type or calc statements
            if is_data_value('regex', vdef, str):
                value = get_regex(vdef, value)

            if is_data_value('type', vdef, str):
                value = check_type(vdef, value)

            if is_data_value('calc', vdef, dict):
                value = calc_value(vdef['calc'], value)

            return value

        def check_length(vdef, value, name):
            max_length = data_value('max length', vdef, int, 0)
            min_length = data_value('min length', vdef, int, 0)

            if max_length > 0:
                if isinstance(value, (str, unicode, list, dict)) and len(value) > max_length:
                    self.warn('Requested datavalue "%s" is longer then %s'% (name, max_length), dtLinkWarning, 4)
                    return False

                if isinstance(value, (int, float)) and value > max_length:
                    self.warn('Requested datavalue "%s" is bigger then %s'% (name, max_length), dtLinkWarning, 4)
                    return False

            if min_length > 0:
                if isinstance(value, (str, unicode, list, dict)) and len(value) < min_length:
                    self.warn('Requested datavalue "%s" is shorter then %s'% (name, min_length), dtLinkWarning, 4)
                    return False

                if isinstance(value, (int, float)) and value < min_length:
                    self.warn('Requested datavalue "%s" is smaller then %s'% (name, min_length), dtLinkWarning, 4)
                    return False

            return True

        def get_regex(vdef, value):
            search_regex = data_value('regex', vdef, str, None)
            try:
                dd = re.search(search_regex, value, re.DOTALL)
                if dd.group(1) not in ('', None):
                    return dd.group(1)

                else:
                    self.warn('Regex "%s" in: %s returned no value on "%s"'% (search_regex, vdef, value), dtLinkWarning, 4)
                    return

            except:
                self.warn('Invalid value "%s" or invalid regex "%s" in: %s'% (value, search_regex, vdef), dtLinkWarning, 4)
                return

        def check_type(vdef, value):
            dtype = data_value('type', vdef, str)
            try:
                if dtype == 'string':
                    return unicode(value)

                elif dtype == 'lower':
                    return unicode(value).lower()

                elif dtype == 'upper':
                    return unicode(value).upper()

                elif dtype == 'capitalize':
                    return unicode(value).capitalize()

                elif dtype == 'int':
                    return int(value)

                elif dtype == 'float':
                    return float(value)

                elif dtype == 'bool':
                    return bool(value)

                else:
                    self.warn('Invalid type "%s" requested'% (dtype), dtLinkWarning, 2)
                    return value

            except:
                self.warn('Error on applying type "%s" on "%s"'% (dtype, value), dtLinkWarning, 4)
                return None

        def calc_value(vdef, value):
            if is_data_value('multiplier', vdef, float):
                try:
                    if not isinstance(value, (int, float)):
                        value = float(value)
                    value = value * vdef['multiplier']

                except:
                    self.warn('Error on applying multiplier "%s" on "%s"'% (vdef['multiplier'], value), dtLinkWarning, 4)

            if is_data_value('divider', vdef, float):
                try:
                    if not isinstance(value, (int, float)):
                        value = float(value)
                    value = value / vdef['divider']

                except:
                    self.warn('Error on applying devider "%s" on "%s"'% (vdef['devider'], value), dtLinkWarning, 4)

            return value

        values = {}
        if isinstance(linkdata, (list, dict)):
            for k, v in self.data_value(["values"], dict).items():
                # first check if it contains a varid statement
                if is_data_value("varid", v, int):
                    vv = get_variable(v)
                    if vv not in self.empty_values and check_length(v, vv, k):
                        values[k] = vv
                        continue

                # else look for a funcid statement
                elif is_data_value("funcid", v, int):
                    cval = process_link_function(v)
                    if cval not in self.empty_values and check_length(v, cval, k):
                        values[k] = cval
                        continue

                # and last for value statement
                elif is_data_value("value", v):
                    values[k] = data_value("value", v)
                    continue

                # If no resulting value was retrieved go for a default
                if is_data_value('default', v):
                    values[k] = v['default']

        else:
            self.warn('No valid data "%s" to link with' % (linkdata), dtLinkWarning, 2)

        return values

    def link_functions(self, fid, data = None, default = None):
        def link_warning(text, severity=4):
            self.warn('%s on function: "%s"\n   Using link_data: %s' % (text, fid, data), dtLinkWarning, severity, 3)

        try:
            if fid > 99:
                retval = self.add_on_link_functions(fid, data, default)
                if fid < 200 and retval in self.empty_values:
                    self.warn('No result on custom link function: "%s"\n   Using link_data: %s' % (fid, data), dtLinkWarning, 4)

                return retval

            # strip data[1] from the end of data[0] if present and make sure it's unicode
            elif fid == 0:
                if not is_data_value(0, data, str):
                    link_warning('Missing or invalid data value 0')
                    if default != None:
                        return default

                    return u''

                if is_data_value(1, data, str) and data[0].strip().lower()[-len(data[1]):] == data[1].lower():
                    return unicode(data[0][:-len(data[1])]).strip()

                else:
                    return unicode(data[0]).strip()

            # strip data[1] from the start of data[0] if present and make sure it's unicode
            elif fid == 1:
                if not is_data_value(0, data, str):
                    link_warning('Missing or invalid data value 0')
                    if default != None:
                        return default

                    return u''

                if is_data_value(1, data, str) and data[0].strip().lower()[:len(data[1])] == data[1].lower():
                    return unicode(data[0][len(data[1]):]).strip()

                else:
                    return unicode(data[0]).strip()

            # concatenate stringparts and make sure it's unicode
            elif fid == 2:
                dd = u''
                for d in data:
                    if d != None:
                        try:
                            dd += unicode(d)

                        except:
                            link_warning('Invalid data value')
                            continue

                return dd

            # get 1 or more parts of a path
            elif fid == 3:
                if is_data_value(0, data, str):
                    dd = data[0].split('/')

                    if is_data_value(1, data, int):
                        if data[1] < len(dd):
                            return dd[data[1]]

                        else:
                            link_warning('Missing or invalid data value 1')

                    elif is_data_value(1, data, list):
                        rval = u''
                        for dpart in data[1]:
                            if isinstance(dpart, int) and dpart < len(dd):
                                rval = u'%s/%s' % (rval, dpart)

                            else:
                                link_warning('Missing or invalid data value in list 1')

                        if len(rval) == 0:
                            return rval

                        else:
                            return rval[1:]

                else:
                    link_warning('Missing or invalid data value 0')

                if default == None:
                    return u''

                else:
                    return default

            # Combine a date and time value
            elif fid == 4:
                if not is_data_value(0, data, datetime.date):
                    data[0] = self.current_date

                if not(is_data_value(0, data, datetime.date) \
                  and is_data_value(1, data, datetime.time) \
                  and is_data_value(2, data, int)):
                    link_warning('Missing or invalid date and/or time values')
                    return default

                dt = self.timezone.localize(datetime.datetime.combine(data[0], data[1]))
                dt = pytz.utc.normalize(dt.astimezone(pytz.utc))
                if is_data_value(3, data, datetime.time):
                    # We check if this time is after the first and if so we assume a midnight passing
                    dc = self.timezone.localize(datetime.datetime.combine(data[0], data[1]))
                    dc = pytz.utc.normalize(dc.astimezone(pytz.utc))
                    if dc > dt:
                        data[0] += datetime.timedelta(days = 1)
                        dt = self.timezone.localize(datetime.datetime.combine(data[0], data[1]))
                        dt = pytz.utc.normalize(dt.astimezone(pytz.utc))

                return dt

            # Return True (or data[2]) if data[1] is present in data[0], else False (or data[3])
            elif fid == 5:
                if is_data_value(0, data, str) and is_data_value(1, data, str) and data[1].lower() in data[0].lower():
                    return data_value(2, data, default = True)

                return data_value(3, data, default = False)

            # Compare the values 1 and 2 returning 3 (or True) if equal, 4 (or False) if unequal and 5 (or None) if one of them is None
            elif fid == 6:
                if not is_data_value(0, data, None, True) or not is_data_value(1, data, None, True):
                    return data_value(4, data, default = None)

                elif data[0] == data[1]:
                    return data_value(2, data, default = True)

                return data_value(3, data, default = False)

            # Return a string on value True
            elif fid == 7:
                if is_data_value(0, data, bool):
                    if data[0] and is_data_value(1, data):
                        return data[1]

                    elif is_data_value(2, data):
                        return data[2]

                    else:
                        link_warning('Missing return values 1 and or 2')

                link_warning('No boolean value 0')
                return default

            # Return the longest not empty text value
            elif fid == 8:
                text = default if isinstance(default, (str, unicode)) else u''
                if len(data) == 0:
                    link_warning('Missing data values')
                    return text

                for item in range(len(data)):
                    if is_data_value(item, data, str):
                        if len(data[item]) > len(text):
                            text = unicode(data[item].strip())

                return text

            # Return the first not empty value
            elif fid == 9:
                if len(data) == 0:
                    link_warning('Missing data values')
                    return default

                for item in data:
                    if (isinstance(item, (str, unicode, list, tuple, dict)) and len(item) > 0) or \
                      (not isinstance(item, (str, unicode, list, tuple, dict)) and item != None):
                        return item

            # look for item 2 in list 0 and return the corresponding value in list1, If not found return item 3 (or None)
            elif fid == 10:
                if len(data) < 3 :
                    link_warning('Missing data values (min. 3)')
                    return default

                if not isinstance(data[0], (list,tuple)):
                    data[0] = [data[0]]

                for index in range(len(data[0])):
                    data[0][index] = data[0][index].lower().strip()

                if not isinstance(data[1], (list,tuple)):
                    data[1] = [data[1]]

                if data[2].lower().strip() in data[0]:
                    index = data[0].index(data[2].lower().strip())
                    if index < len(data[1]):
                        return data[1][index]

                if len(data) > 3 :
                    return data[3]

                link_warning('Item 2 not found in list 0 or list 1 to short')
                return default

            # look for item 1 in the keys from dict 0 and return the corresponding value
            elif fid == 11:
                if len(data) < 2:
                    link_warning('Missing data values (min. 2)')
                    return default

                if is_data_value(0, data, dict):
                    data[0] =[data[0]]

                if not is_data_value(1, data, list):
                    data[1] = [data[1]]

                if is_data_value(0, data, list):
                    for item in data[1]:
                        for sitem in data[0]:
                            if isinstance(sitem, dict):
                                if item.lower() in sitem.keys():
                                    if isinstance(sitem[item.lower()], (list, tuple)) and len(sitem[item.lower()]) == 0:
                                        continue

                                    if isinstance(sitem[item.lower()], (list, tuple)) and len(sitem[item.lower()]) == 1:
                                        return sitem[item.lower()][0]

                                    return sitem[item.lower()]

                link_warning('Item 1 not found in dict 0')
                return default

            # remove data[1] from the string in data[0] if present and make sure it's unicode
            elif fid == 12:
                if not is_data_value(0, data, str):
                    link_warning('Missing or invalid data value 0')
                    if default != None:
                        return default

                    return u''

                if is_data_value(1, data, str) and data[1] in data[0]:
                    re.sub('  ', ' ', re.sub(data[1], '', data[0]))

                else:
                    return unicode(data[0]).strip()

            else:
                link_warning('Unknown link function',2)
                return None

        except:
            self.warn('Unknown link Error on function: "%s"\n   Using link_data: %s\n%s' % (fid, data, traceback.print_exc()), dtLinkWarning, 2)
            return default

    def add_on_link_functions(self, fid, data = None, default = None):
        pass

    def set_errorcode(self, code, set_fatal = False):
        with self.tree_lock:
            fcode = self.errorcode & dtFatalError
            rcode = self.errorcode - fcode
            if set_fatal:
                fcode = (code & dtFatalError)

            rcode = rcode | code
            rcode = rcode - (rcode & dtFatalError)
            self.errorcode = rcode + fcode

    def check_errorcode(self, only_fatal = True, code = None, text_values = False):
        fcode = self.errorcode & dtFatalError
        rcode = self.errorcode - fcode
        txtreturn = []
        intreturn = 0
        if code == None:
            if only_fatal:
                intreturn = fcode
                txtreturn = [dtErrorTexts[fcode]]

            else:
                intreturn = fcode + rcode
                txtreturn = [dtErrorTexts[fcode]]
                for i in (8, 16, 32, 64, 128):
                    if i & rcode:
                        txtreturn.append(dtErrorTexts[i])

        else:
            rfcode = code & dtFatalError
            rrcode = code - rfcode
            if rfcode == dtFatalError or rfcode == fcode:
                intreturn = fcode
                txtreturn = [dtErrorTexts[fcode]]

            for i in (8, 16, 32, 64, 128):
                if (i & rrcode) and (i & rcode):
                    intreturn += i
                    txtreturn.append(dtErrorTexts[i])

        if text_values:
            return txtreturn

        if len(txtreturn) > 0:
            return intreturn

    def simplefilter(self, action, category=Warning, lineno=0, append=0, severity=0):
        with self.tree_lock:
            sys.modules['DataTreeGrab']._warnings.simplefilter(action, category, lineno, append, self.caller_id, severity)

    def warn(self, message, category, severity, stacklevel = 2):
        sys.modules['DataTreeGrab']._warnings.warn(message, category, self.caller_id, severity, stacklevel)

    def is_data_value(self, searchpath, dtype = None, empty_is_false = False):
        return is_data_value(searchpath, self.data_def, dtype, empty_is_false)

    def data_value(self, searchpath, dtype = None, default = None):
        return data_value(searchpath, self.data_def, dtype, default)
# end DataTreeShell()

if __name__ == '__main__':
    if version()[6]:
        sys.stdout.write('%s-%s.%s.%s-p%s alfa\n' % (version()[0],version()[1],version()[2],version()[3],version()[4]))

    elif version()[5]:
        sys.stdout.write('%s-%s.%s.%s-p%s beta\n' % (version()[0],version()[1],version()[2],version()[3],version()[4]))

    else:
        sys.stdout.write('%s-%s.%s.%s-p%s\n' % (version()[0],version()[1],version()[2],version()[3],version()[4]))

    sys.stdout.write('This package is intended to be used as a module!\n')
    sys.stdout.write('Run:\n')
    sys.stdout.write('>   sudo ./setup.py install\n\n')
    sys.stdout.write('%s\n' % __doc__)
