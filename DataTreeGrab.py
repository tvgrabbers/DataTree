#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
This Package contains a tool for extracting structured data from HTML and JSON
pages.
It reads the page into a Node based tree, from which you, on the bases of a json
data-file, can extract your data into a list of items. It can first extract a
list of keyNodes and extract for each of them the same data-list. During the
extraction several data manipulation functions are available.
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
import re, sys, traceback
import time, datetime, pytz
from threading import RLock
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
dt_minor = 0
dt_patch = 0
dt_patchdate = u'20160529'
dt_alfa = False
dt_beta = True

__version__  = '%s.%s.%s' % (dt_major,dt_minor,dt_patch)
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

    if empty_is_false and searchtree in (None, "", {}, []):
        return False

    if dtype == None:
        return True

    if dtype == float:
        return bool(isinstance(searchtree, (float, int)))

    if dtype in (str, unicode, 'string'):
        return bool(isinstance(searchtree, (str, unicode)))

    if dtype in (list, tuple, 'list'):
        return bool(isinstance(searchtree, (list, tuple)))

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

        elif dtype in (str, unicode):
            return ""

        elif dtype == dict:
            return {}

        elif dtype in (list, tuple):
            return []

    return searchtree
# end data_value()

class NULLnode():
    value = None

# end NULLnode

class DATAnode():
    def __init__(self, dtree, parent = None):
        self.node_lock = RLock()
        with self.node_lock:
            self.children = []
            self.dtree = dtree
            self.parent = parent
            self.value = None
            self.child_index = 0
            self.level = 0
            self.link_value = {}

            self.is_root = bool(self.parent == None)
            n = self
            while not n.is_root:
                n = n.parent

            self.root = n
            if isinstance(parent, DATAnode):
                self.parent.append_child(self)
                self.level = parent.level + 1

    def append_child(self, node):
        with self.node_lock:
            node.child_index = len(self.children)
            self.children.append(node)

    def get_children(self, path_def = None, link_values=None):
        childs = []
        if not isinstance(link_values, dict):
            link_values = {}

        d_def = path_def if isinstance(path_def, list) else [path_def]
        if len(d_def) == 0 or d_def[0] == None:
            # It's not a child definition
            if self.dtree.show_result:
                self.dtree.print_text(u'    adding node %s\n'.encode('utf-8', 'replace') % (self.print_node()))
            return [self]

        nm = self.find_name(d_def[0])
        if self.match_node(node_def = d_def[0], link_values=link_values, last_node_def = True) == None:
            # It's not a child definition
            if len(d_def) == 1:
                if self.dtree.show_result:
                    self.dtree.print_text(u'    adding node %s; %s\n'.encode('utf-8', 'replace') % (self.print_node(), d_def[0]))

                if nm == None:
                    return [self]

                else:
                    return [{nm:self}]

            else:
                if len(self.link_value) > 0:
                    for k, v in self.link_value.items():
                        link_values[k] = v

                self.link_value = {}
                childs = self.get_children(path_def = d_def[1:], link_values=link_values)
                if nm == None:
                    return childs

                else:
                    return self.tag,{nm:childs}

        elif is_data_value('path', d_def[0]):
            sel_val = d_def[0]['path']
            if sel_val == 'parent' and not self.is_root:
                if self.dtree.show_result:
                    self.dtree.print_text(u'  found node %s; %s\n'.encode('utf-8', 'replace') % (self.parent.print_node(), d_def[0]))
                self.parent.match_node(node_def = d_def[0], link_values=link_values)
                if len(self.parent.link_value) > 0:
                    for k, v in self.parent.link_value.items():
                        link_values[k] = v

                self.parent.link_value = {}
                childs = self.parent.get_children(path_def = d_def[1:], link_values=link_values)
                if nm == None:
                    return childs

                else:
                    return {nm:childs}

            elif sel_val == 'root':
                if self.dtree.show_result:
                    self.dtree.print_text(u'  found node %s; %s\n'.encode('utf-8', 'replace') % (self.root.print_node(), d_def[0]))
                self.root.match_node(node_def = d_def[0], link_values=link_values)
                if len(self.root.link_value) > 0:
                    for k, v in self.root.link_value.items():
                        link_values[k] = v

                self.root.link_value = {}
                childs = self.root.get_children(path_def = d_def[1:], link_values=link_values)
                if nm == None:
                    return childs

                else:
                    return {nm:childs}

            elif sel_val == 'all':
                for item in self.children:
                    if self.dtree.show_result:
                        self.dtree.print_text(u'  found node %s; %s\n'.encode('utf-8', 'replace') % (item.print_node(), d_def[0]))
                    item.match_node(node_def = d_def[0], link_values=link_values)
                    if len(item.link_value) > 0:
                        for k, v in item.link_value.items():
                            link_values[k] = v

                    item.link_value = {}
                    jl = item.get_children(path_def = d_def[1:], link_values=link_values)
                    if isinstance(jl, list):
                        childs.extend(jl)

                    elif jl != None:
                        childs.append(jl)

                if nm == None:
                    return childs

                else:
                    return {nm:childs}

        else:
            for item in self.children:
                # We look for matching children
                if item.match_node(node_def = d_def[0], link_values=link_values):
                    # We found a matching child
                    if self.dtree.show_result:
                        self.dtree.print_text(u'  found node %s; %s\n'.encode('utf-8', 'replace') % (item.print_node(), d_def[0]))
                    if len(item.link_value) > 0:
                        for k, v in item.link_value.items():
                            link_values[k] = v

                    item.link_value = {}
                    jl = item.get_children(path_def = d_def[1:], link_values=link_values)
                    if isinstance(jl, list):
                        childs.extend(jl)

                    elif jl != None:
                        childs.append(jl)

            if nm == None:
                return childs

            else:
                return {nm:childs}

        #~ else:
            #~ if self.dtree.show_result:
                #~ self.dtree.print_text(u'    adding node %s; %s\n'.encode('utf-8', 'replace') % (self.print_node(), d_def[0]))
            #~ return [self]

        if nm == None:
            return childs

        else:
            return {nm:childs}

    def check_for_linkrequest(self, node_def):
        if is_data_value('link', node_def, int):
            self.link_value[node_def['link']] = self.find_value(node_def)
            if self.dtree.show_result:
                self.dtree.print_text(u'    saving link to node %s: %s\n      %s\n'.encode('utf-8', 'replace') % (self.find_value(node_def), self.print_node(), node_def))

    def match_node(self, node_def = None, link_values = None, last_node_def = False):
        self.link_value = {}
        return False

    def find_name(self, node_def):
        return None

    def find_value(self, node_def = None):
        return self.dtree.calc_value(self.value, node_def)

    def print_node(self, print_all = False):
        return u'%s = %s' % (self.level, self.find_value())

    def print_tree(self):
        sstr =u'%s%s\n' % (self.dtree.get_leveltabs(self.level,4), self.print_node(True))
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
                self.tag = data.lower()

            elif isinstance(data, list):
                if len(data) > 0:
                    self.tag = data[0].lower()

                if len(data) > 1 and isinstance(data[1], (list, tuple)):
                    for a in data[1]:
                        self.attributes[a[0].lower()] = a[1]

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

    def match_node(self, tag = None, attributes = None, node_def = None, link_values=None, last_node_def = False):
        def check_index_link():
            if not data_value(['index','link'], node_def, int) in link_values.keys():
                sys.stderr.write('You requested an index link, but link value %s is not stored!\n' % data_value(['index','link'], node_def, int))
                return False

            il = link_values[data_value(['index','link'], node_def, int)]
            if not isinstance(il, int):
                sys.stderr.write('You requested an index link, but the stored value is no integer!\n')
                return False

            clist = data_value(['index','calc'], node_def, list)
            if len(clist) == 2 and isinstance(clist[1], int):
                if clist[0] == 'min':
                    il -= clist[1]

                elif clist[0] == 'plus':
                    il += clist[1]

            if is_data_value(['index','previous'], node_def):
                if self.child_index < il:
                    return True

            if is_data_value(['index','next'], node_def):
                if self.child_index > il:
                    return True

            if self.child_index == il:
                return True

            return False

        self.link_value = {}
        if not isinstance(attributes,list):
            attributes = []

        if not isinstance(link_values, dict):
            link_values ={}

        if node_def == None:
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
            if node_def['tag'].lower() in (None, self.tag.lower()):
                # The tag matches
                if is_data_value(['index','link'], node_def, int):
                    # There is an index request to an earlier linked index
                    if not check_index_link():
                        return False

                elif is_data_value(['index'], node_def, int):
                    # There is an index request to a set value
                    if self.child_index != data_value(['index'], node_def, int):
                        return False

            else:
                return False

        elif is_data_value('index', node_def):
            if is_data_value(['index','link'], node_def, int):
                # There is an index request to an earlier linked index
                if not check_index_link():
                    return False

            elif is_data_value(['index'], node_def, int):
                # There is an index request to a set value
                if self.child_index != data_value(['index'], node_def, int):
                    return False

            else:
                return False

        elif is_data_value('path', node_def):
            if not last_node_def:
                self.check_for_linkrequest(node_def)

            return False

        else:
            if last_node_def:
                self.check_for_linkrequest(node_def)

            return None

        if is_data_value('text', node_def, str):
            if node_def['text'].lower() != self.text.lower():
                return False

        if is_data_value('tail', node_def, str):
            if node_def['tail'].lower() != self.tail.lower():
                return False

        if not is_data_value('attrs', node_def, dict):
            # And there are no attrib matches requested
            if not last_node_def:
                self.check_for_linkrequest(node_def)

            return True

        for a, v in node_def['attrs'].items():
            if is_data_value('not', v, list):
                # There is a negative attrib match requested
                for val in v['not']:
                    if self.is_attribute(a) and self.attributes[a] == val:
                        return False

            elif is_data_value('link', v, int) and v["link"] in link_values.keys():
                # The requested value is in link_values
                if not self.is_attribute(a, link_values[v["link"]]):
                    return False

            elif not self.is_attribute(a, v):
                return False

        if not last_node_def:
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
            return self.dtree.calc_value(sv, node_def['name'])

    def find_value(self, node_def = None):
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

    def match_node(self, node_def = None, link_values = None, last_node_def = False):
        self.link_value = {}
        if not isinstance(link_values, dict):
            link_values ={}

        if is_data_value('key', node_def):
            if self.key == node_def["key"]:
                # The requested key matches
                if not last_node_def:
                    self.check_for_linkrequest(node_def)

                return True

            return False

        elif is_data_value('keys', node_def, list):
            if self.key in node_def['keys']:
                # This key is in the list with requested keys
                if not last_node_def:
                    self.check_for_linkrequest(node_def)

                return True

            return False

        elif is_data_value('keys', node_def, dict):
            # Does it contain the requested key/value pairs
            for item, v in node_def["keys"].items():
                if not item in self.keys:
                    return False

                val = v
                if is_data_value('link', v, int) and v["link"] in link_values.keys():
                    # The requested value is in link_values
                    val = link_values[v["link"]]

                if self.get_child(item).value != val:
                    return False

            if not last_node_def:
                self.check_for_linkrequest(node_def)

            return True

        elif is_data_value(['index','link'], node_def, int):
            # There is an index request to an earlier linked index
            if not data_value(['index','link'], node_def, int) in link_values.keys():
                sys.stderr.write('You requested an index link, but link value %s is not stored!\n' % data_value(['index','link'], node_def, int))
                return False

            il = link_values[data_value(['index','link'], node_def, int)]
            if not isinstance(il, int):
                sys.stderr.write('You requested an index link, but the stored value is no integer!\n')
                return False

            clist = data_value(['index','calc'], node_def, list)
            if len(clist) == 2 and isinstance(clist[1], int):
                if clist[0] == 'min':
                    il -= clist[1]

                elif clist[0] == 'plus':
                    il += clist[1]

            if is_data_value(['index','previous'], node_def) and self.child_index < il:
                return True

            if is_data_value(['index','next'], node_def) and self.child_index > il:
                return True

            if self.child_index == il:
                return True

            else:
                return False

        elif is_data_value(['index'], node_def, int):
            # There is an index request to a set value
            if self.child_index == data_value(['index'], node_def, int):
                if not last_node_def:
                    self.check_for_linkrequest(node_def)

                return True

            else:
                return False

        elif is_data_value('path', node_def):
            if not last_node_def:
                self.check_for_linkrequest(node_def)

            return False

        else:
            if last_node_def:
                self.check_for_linkrequest(node_def)

            return None

    def find_name(self, node_def):
        sv = None
        if is_data_value('name', node_def, dict):
            if is_data_value(['name','select'], node_def, str):
                if node_def[ 'name']['select'] == 'key':
                    sv = self.key

                elif node_def[ 'name']['select'] == 'value':
                    sv = self.value

        if sv != None:
            return self.dtree.calc_value(sv, node_def[ 'name'])

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
    def __init__(self, output = sys.stdout):
        self.tree_lock = RLock()
        with self.tree_lock:
            self.print_searchtree = False
            self.show_result = False
            self.fle = output
            self.extract_from_parent = False
            self.result = []
            self.data_def = {}
            self.month_names = []
            self.weekdays = []
            self.relative_weekdays = {}
            self.datetimestring = u"%Y-%m-%d %H:%M:%S"
            self.time_splitter = u':'
            self.date_sequence = ["y","m","d"]
            self.date_splitter = u'-'
            self.utc = pytz.utc
            self.timezone = pytz.utc
            self.value_filters = {}

    def check_data_def(self, data_def):
        with self.tree_lock:
            self.data_def = data_def if isinstance(data_def, dict) else {}
            self.month_names = self.data_value("month-names", list)
            self.weekdays = self.data_value("weekdays", list)
            self.datetimestring = self.data_value("datetimestring", str, default = u"%Y-%m-%d %H:%M:%S")
            self.time_splitter = self.data_value("time-splitter", str, default = ':')
            self.date_sequence = self.data_value("date-sequence", list, default = ["y","m","d"])
            self.date_splitter = self.data_value("date-splitter", str, default = '-')
            self.timezone = pytz.timezone(self.data_value('timezone', str, default = 'utc'))
            self.value_filters = self.data_value("value-filters", dict)
            self.current_date = self.timezone.normalize(datetime.datetime.now(pytz.utc).astimezone(self.timezone)).toordinal()
            rw = self.data_value( "relative-weekdays", dict)
            for name, index in rw.items():
                self.relative_weekdays[name] = datetime.date.fromordinal(self.current_date + index)

            current_weekday = datetime.date.fromordinal(self.current_date).weekday()
            for index in range(len(self.weekdays)):
                name = self.weekdays[index]
                if index < current_weekday:
                    self.relative_weekdays[name] = datetime.date.fromordinal(self.current_date + index + 7 - current_weekday)

                else:
                    self.relative_weekdays[name] = datetime.date.fromordinal(self.current_date + index - current_weekday)

    def find_start_node(self, data_def=None):
        with self.tree_lock:
            if isinstance(data_def, dict):
                self.data_def = data_def

            if self.print_searchtree:
                self.print_text('The root Tree:\n')
                self.start_node.print_tree()

            init_path = self.data_value(['data',"init-path"],list)
            if self.show_result:
                self.print_text(self.root.print_node())

            sn = self.root.get_children(path_def = init_path)
            self.start_node = self.root if (sn == None or len(sn) == 0) else sn[0]

    def find_data_value(self, path_def, start_node = None, link_values = None):
        with self.tree_lock:
            if not isinstance(path_def, (list, tuple)) or len(path_def) == 0:
                return

            if start_node == None or not isinstance(start_node, DATAnode):
                start_node = self.start_node

            nlist = start_node.get_children(path_def = path_def, link_values = link_values)
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

            if self.print_searchtree:
                self.print_text('The %s Tree:\n' % self.start_node.print_node())
                self.start_node.print_tree()

            self.result = []
            # Are there multiple data definitions
            if self.is_data_value(['data',"iter"],list):
                def_list = self.data_value(['data','iter'],list)

            # Or just one
            elif self.is_data_value('data',dict):
                def_list = [self.data_value('data',dict)]

            else:
                return

            for dset in def_list:
                # Get all the key nodes
                if is_data_value(["key-path"], dset, list):
                    kp = data_value(["key-path"], dset, list)
                    if len(kp) == 0:
                        continue

                    if self.show_result:
                        self.fle.write('parsing keypath %s\n'.encode('utf-8') % (kp[0]))

                    self.key_list = self.start_node.get_children(path_def = kp)
                    for k in self.key_list:
                        if not isinstance(k, DATAnode):
                            continue

                        # And if it's a valid node, find the belonging values (the last dict in a path list contains the value definition)
                        tlist = [k.find_value(kp[-1])]
                        link_values = {}
                        if is_data_value('link', kp[-1], int):
                            link_values = {kp[-1]["link"]: k.find_value(kp[-1])}

                        for v in data_value(["values"], dset, list):
                            if not isinstance(v, list) or len(v) == 0:
                                tlist.append(None)
                                continue

                            if is_data_value('value', v[0]):
                                tlist.append(data_value('value',v[0]))
                                continue

                            if self.show_result:
                                self.fle.write('parsing key %s %s\n'.encode('utf-8') % ( [k.find_value(kp[-1])], v[-1]))

                            if self.extract_from_parent and isinstance(k.parent, DATAnode):
                                dv = self.find_data_value(v, k.parent, link_values)

                            else:
                                dv = self.find_data_value(v, k, link_values)

                            if isinstance(dv, NULLnode):
                                break

                            tlist.append(dv)

                        else:
                            self.result.append(tlist)

    def calc_value(self, value, node_def = None):
        if isinstance(value, (str, unicode)):
            # Is there something to strip of
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

            #~ # Is there a search regex
            #~ if is_data_value('regex', node_def, str):
                #~ try:
                    #~ dd = re.search(node_def['regex'],  value, re.DOTALL)
                        #~ if dd.group(1) not in ('', None):
                            #~ value = dd.group(1)

            # Is there a split list
            if is_data_value('split', node_def, list) and len(node_def['split']) > 0:
                if not isinstance(node_def['split'][0],list):
                    slist = [node_def['split']]

                else:
                    slist = node_def['split']

                for sdef in slist:
                    if len(sdef) < 2 or not isinstance(sdef[0],(str,unicode)):
                        continue

                    try:
                        fill_char = sdef[0]
                        if fill_char in ('\\s', '\\t', '\\n', '\\r', '\\f', '\\v', ' '):
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
                        #~ traceback.print_exc()
                        pass

        if is_data_value('multiplier', node_def, int) and not data_value('type', node_def, unicode) in ('timestamp', 'datestamp'):

            try:
                value = int(value) * node_def['multiplier']

            except:
                #~ traceback.print_exc()
                pass

        if is_data_value('divider', node_def, int) and node_def['divider'] != 0:
            try:
                value = int(value) // node_def['divider']

            except:
                #~ traceback.print_exc()
                pass

        # Is there a replace dict
        if is_data_value('replace', node_def, dict):
            if value == None or not isinstance(value, (str, unicode)):
                pass

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
                        ts = self.time_splitter
                        if is_data_value('time-splitter', node_def, str):
                            ts = data_value('time-splitter', node_def, str)

                        t = re.split(ts, value)
                        if len(t) == 2:
                            value = datetime.time(int(t[0]), int(t[1]))

                        elif len(t) > 2:
                            value = datetime.time(int(t[0]), int(t[1]), int(t[2][:2]))

                    except:
                        #~ traceback.print_exc()
                        pass

                elif node_def['type'] == 'timedelta':
                    try:
                            value = datetime.timedelta(seconds = int(value))

                    except:
                        #~ traceback.print_exc()
                        pass

                elif node_def['type'] == 'date':
                    try:
                        current_date = self.timezone.normalize(datetime.datetime.now(pytz.utc).astimezone(self.timezone))
                        day = current_date.day
                        month = current_date.month
                        year = current_date.year
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

                            try:
                                d[index] = int(d[index])

                            except ValueError:
                                if d[index].lower() in self.month_names:
                                    d[index] = self.month_names.index(d[index].lower())

                                else:
                                    continue

                            if dseq[index].lower() == 'd':
                                day = d[index]

                            if dseq[index].lower() == 'm':
                                month = d[index]

                            if dseq[index].lower() == 'y':
                                year = d[index]

                        value = datetime.date(year, month, day)

                    except:
                        #~ traceback.print_exc()
                        pass


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
                        value = 0

                elif node_def['type'] == 'float':
                    try:
                        value = float(value)

                    except:
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

                elif node_def['type'] == '':
                    pass

            except:
                #~ traceback.print_exc()
                pass

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
        self.fle.write(text.encode('utf-8', 'replace'))

    def get_leveltabs(self, level, spaces=3):
        stab = u''
        for i in range(spaces):
            stab += u' '

        sstr = u''
        for i in range(level):
            sstr += stab

        return sstr

    def is_data_value(self, searchpath, dtype = None, searchtree = None):
        if searchtree == None:
            searchtree = self.data_def

        return is_data_value(searchpath, searchtree, dtype)

    def data_value(self, searchpath, dtype = None, searchtree = None, default = None):
        if searchtree == None:
            searchtree = self.data_def

        return data_value(searchpath, searchtree, dtype, default)

# end DATAtree

class HTMLtree(HTMLParser, DATAtree):
    def __init__(self, data, autoclose_tags=[], print_tags = False, output = sys.stdout):
        HTMLParser.__init__(self)
        DATAtree.__init__(self, output)
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
            self.feed(data)
            self.reset()
            # And find the dataset into self.result
            self.start_node = self.root

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
                self.print_text(u'%5.0f %5.0f %5.0f %s\n' % (c['start'], c['close'], c['auto'], t))

    def handle_starttag(self, tag, attrs):
        if not tag in self.open_tags.keys():
            self.open_tags[tag] = 0

        self.open_tags[tag] += 1
        if self.print_tags:
            if len(attrs) > 0:
                self.print_text(u'%sstarting %s %s %s\n' % (self.get_leveltabs(self.current_node.level,2), self.current_node.level+1, tag, attrs[0]))
                for a in range(1, len(attrs)):
                    self.print_text(u'%s        %s\n' % (self.get_leveltabs(self.current_node.level,2), attrs[a]))

            else:
                self.print_text(u'%sstarting %s %s\n' % (self.get_leveltabs(self.current_node.level,2), self.current_node.level,tag))

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
            #~ self.remove_text()
            self.handle_endtag(self.current_node.tag)

        self.add_text()
        if self.print_tags:
            if self.current_node.text.strip() != '':
                self.print_text(u'%s        %s\n' % (self.get_leveltabs(self.current_node.level-1,2), self.current_node.text.strip()))
            self.print_text(u'%sclosing %s %s %s\n' % (self.get_leveltabs(self.current_node.level-1,2), self.current_node.level,tag, self.current_node.tag))

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
    def __init__(self, data, output = sys.stdout):
        DATAtree.__init__(self, output)
        with self.tree_lock:
            self.extract_from_parent = True
            self.data = data
            # Read the json data into the tree
            self.root = JSONnode(self, data, key = 'ROOT')
            self.start_node = self.root

# end JSONtree

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
