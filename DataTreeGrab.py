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
dt_minor = 4
dt_patch = 0
dt_patchdate = u'20170620'
dt_alfa = False
dt_beta = True
_warnings = None

# DataTreeShell errorcodes
dtQuiting = -1
dtDataOK = 0
dtDataDefOK = 0
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
dtInvalidValueLink = 256
dtInvalidNodeLink = 512
dtInvalidPathDef = 1024
dtInvalidLinkDef = 2048
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
    128: 'Setting the current date failed',
    256: 'A not jet stored value link was requested',
    512: 'A not jet stored node link was requested'}

# The allowances for a path_def
dtpathWithValue = 1
dtpathWithNames = 2
dtpathMulti = 4
dtpathInit = 0
dtpathKey = 5
dtpathValue = 7
# The node_def type
dtisGroup = 7
dtisNone = 0
dtemptyNodeDef = ((dtisNone, ), )
dtisNodeSel = 1
dtisNodeLink = 2
dtstoreName = 3
dtisValue = 4
dthasCalc = 8
dthasDefault = 16
dthasType = 32
dtisMemberOff = 64
dtstoreLinkValue = 128
dtstorePathValue =256
dtgetOnlyOne = 512
dtgetLast = 1024
# Node selection and the tuple position for details (node_def type 1 and 2)
dtselMain = 7
dtselNone = 0
dtselPathAll = 1
dtselPathParent = 2
dtselPathRoot = 3
dtselPathLink = 4
dtselKey = 5
dtselTag = 6
dtselKeys = 7
dtselTags = 7
dtselIndex = 8
dtselText = 16
dtselTail = 32
dtselChildKeys = 64
dtselAttrs = 64
dtselNotChildKeys = 128
dtselNotAttrs = 128
dtattr = 0
dtattrNot = 1
dtselPosMax = 7
dtselPos = {
    dtselPathAll: 2,
    dtselPathParent: 2,
    dtselPathRoot: 2,
    dtselPathLink: 2,
    dtselKey: 2,
    dtselTag: 2,
    dtselKeys: 2,
    dtselIndex: 3,
    dtselChildKeys: 4,
    dtselNotChildKeys: 5,
    dtselText: 6,
    dtselTail: 7}
# What data to extract (node_def type 5 (+64/+128)
dtgetPosMax = 6
dtgetPos = {
    dthasCalc: 2,
    dthasType: 3,
    dtisMemberOff: 4,
    dtstoreLinkValue: 5,
    dthasDefault: 6}
dtgetGroup = 15
dtgetNone = 0
dtgetIndex = 1
dtgetKey = 2
dtgetTag = 2
dtgetDefault = 3
dtgetValue = 3
dtgetText = 3
dtgetTail = 4
dtgetInclusiveText = 5
dtgetPresence = 6
dtgetLitteral = 7
dtgetAttr = 8
# Is it a value or a linkvalue and what manipulations to do to a retrieved linkvalue
dtvalValue = 0
dtvalLink = 1
dtvalLinkPlus = 2
dtvalLinkMin = 4
dtvalLinkNext = 8
dtvalLinkPrevious = 16
# What data manipulations
dtcalcNone = 0
dtcalcLettering = 1
dtcalcLower = 1
dtcalcUpper = 2
dtcalcCapitalize = 3
dtcalcAsciiReplace = 2
dtcalcLstrip = 3
dtcalcRstrip = 4
dtcalcSub = 5
dtcalcSplit = 6
dtcalcMultiply = 16
dtcalcDivide = 17
dtcalcReplace = 7
dtcalcDefault = 32
# What type to select
dttypeNone = 0
dttypeTimeStamp = 1
dttypeDateTimeString = 2
dttypeTime = 3
dttypeTimeDelta = 4
dttypeDate = 5
dttypeDateStamp = 6
dttypeRelativeWeekday = 7
dttypeString = 8
dttypeInteger = 9
dttypeFloat = 10
dttypeBoolean = 11
dttypeLowerAscii = 12
dttypeStringList = 13
dttypeList = 14
dttypeLower = 15
dttypeUpper = 16
dttypeCapitalize = 17
# About the link_defs
dtlinkNone = 0
dtlinkGroup = 3
dtlinkVarID =1
dtlinkFuncID = 2
dtlinkValue = 3
dtlinkhasDefault = 4
dtlinkhasRegex = 8
dtlinkhasType = 16
dtlinkhasCalc = 32
dtlinkhasMax = 64
dtlinkhasMin = 128
dtlinkPosMax = 7
dtlinkPos = {
    dtlinkhasDefault: 2,
    dtlinkhasRegex: 3,
    dtlinkhasType: 4,
    dtlinkhasCalc: 5,
    dtlinkhasMax: 6,
    dtlinkhasMin: 7}

__version__  = '%s.%s.%s' % (dt_major,'{:0>2}'.format(dt_minor),'{:0>2}'.format(dt_patch))
if dt_alfa:
    __version__ = '%s-alfa' % (__version__, )

elif dt_beta:
    __version__ = '%s-beta' % (__version__, )

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
            if (not isinstance(d, int) or (d >= 0 and d >= len(searchtree)) or (d < 0 and -d > len(searchtree))):
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

class dtWarning(UserWarning):
    # The root of all DataTreeGrab warnings.
    name = 'General Warning'

class dtDataWarning(dtWarning):
    name = 'Data Warning'

class dtdata_defWarning(dtWarning):
    name = 'data_def Warning'

class dtConversionWarning(dtdata_defWarning):
    name = 'Conversion Warning'

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

class DataDef_Convert():
    def __init__(self, data_def = None, warnaction = "default", warngoal = sys.stderr, caller_id = 0):
        self.tree_lock = RLock()
        with self.tree_lock:
            self.known_urlid = (0, 4, 11, 14)
            self.known_linkid = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
            self.errorcode = dtDataDefOK
            self.caller_id = caller_id
            self.cdata_def = {}
            self.ddtype = ""
            if sys.modules['DataTreeGrab']._warnings == None:
                sys.modules['DataTreeGrab']._warnings = _Warnings(warnaction, warngoal, caller_id)

            elif caller_id not in sys.modules['DataTreeGrab']._warnings._ids or warnaction != None:
                sys.modules['DataTreeGrab']._warnings.set_warnaction(warnaction, caller_id)

            if isinstance(data_def, dict):
                self.data_def = data_def
                self.convert_data_def()

            else:
                self.data_def = {}

    def convert_path_def(self, path_def, ptype = "", path_type = dtpathValue, link_list = None, init_errors = True):
        # check whether it is a link or a value
        # return a list (typeint, value/link, plus/min)
        def convert_value_link(lvalue, is_index = False):
            if is_data_value("link",lvalue, int):
                if not lvalue["link"] in self.link_list["values"]:
                    self.errorcode |= (dtInvalidValueLink + dtDataDefInvalid)
                    self.warn('LinkID: %s is not jet stored' % ( lvalue["link"], ), dtConversionWarning, 1, 3)
                    return (dtvalValue,None,0)

                val = [dtvalLink, lvalue["link"], 0]

            else:
                return (dtvalValue,lvalue,0)

            if data_value(["calc", 0],lvalue) == "plus":
                val[0] += dtvalLinkPlus
                val[2] = data_value(["calc", 1],lvalue, int, 0)

            elif data_value(["calc", 0],lvalue) == "min":
                val[0] += dtvalLinkMin
                val[2] = data_value(["calc", 1],lvalue, int, 0)

            elif is_index and is_data_value("previous",lvalue):
                val[0] += dtvalLinkPrevious

            elif is_index and is_data_value("next",lvalue):
                val[0] += dtvalLinkNext

            return tuple(val)

        # ensure it's a list and process the above
        # return a list of (value/link)
        def convert_value_list(lvalue, is_index = False):
            vlist = []
            if isinstance(lvalue, (list, tuple)):
                for lv in lvalue:
                    vlist.append(convert_value_link(lv, is_index))

            else:
                vlist.append(convert_value_link(lvalue, is_index))

            return tuple(vlist)

        # process an (attrs/childkeys) values dict
        # return a list of lists (name, typeint, (value/link))
        def convert_attr_dict(ldict):
            llist = []
            for k, v in ldict.items():
                if is_data_value(["not"], v):
                    dta = dtattrNot
                    vl = convert_value_list(v["not"])

                else:
                    vl = convert_value_list(v)
                    dta = dtattr

                llist.append((k, dta, vl))

            return tuple(llist)

        # process Data extraction/manipulation
        # return (node_type,linkid,data) or (node_type,0,data)
        # with data = (sel_type,sel_data),((calc_int,calc_data)),(type_int, type_data), memberoff
        def convert_data_extraction(node_def, node_type = dtisValue):
            nlink = 0
            if ((node_type & dtstoreLinkValue) or (node_type & dtstorePathValue)) \
                and (node_type & dtisGroup) != dtisValue:
                    node_type -= (node_type & dtisGroup)
                    node_type += dtisValue

            if (node_type & dtstoreLinkValue):
                nlink = node_def["link"]

            sel_node = [dtgetDefault, None]
            calc_list = []
            type_def = []
            memberoff = ""
            ndefault = None
            if isinstance(node_def,dict):
                if "value" in node_def.keys():
                    sel_node = [dtgetLitteral, node_def["value"]]

                elif is_data_value("attr", node_def, str) and self.ddtype in ("html", ""):
                    self.ddtype="html"
                    sel_node = [dtgetAttr, node_def["attr"].lower()]

                elif is_data_value("select", node_def, str):
                    if node_def["select"] == "index":
                        sel_node[0] = dtgetIndex

                    elif node_def["select"] == "key" and self.ddtype in ("json", ""):
                        self.ddtype="json"
                        sel_node[0] = dtgetKey

                    elif node_def["select"] == "tag" and self.ddtype in ("html", ""):
                        self.ddtype="html"
                        sel_node[0] = dtgetTag

                    elif node_def["select"] == "text" and self.ddtype in ("html", ""):
                        self.ddtype="html"
                        sel_node[0] = dtgetText

                    elif node_def["select"] == "tail" and self.ddtype in ("html", ""):
                        self.ddtype="html"
                        sel_node[0] = dtgetTail

                    elif node_def["select"] == "value" and self.ddtype in ("json", ""):
                        self.ddtype="json"
                        sel_node[0] = dtgetValue

                    elif node_def["select"] == "presence":
                        sel_node[0] = dtgetPresence

                    elif node_def["select"] == "inclusive text" and self.ddtype in ("html", ""):
                        self.ddtype="html"
                        depth = data_value("depth", node_def, int, 1)
                        if is_data_value("include", node_def, list):
                            specs = (depth, 1, node_def["include"])

                        elif is_data_value("exclude", node_def, list):
                            specs = (depth, -1, node_def["exclude"])

                        else:
                            specs = (depth, 0, [])

                        sel_node = [dtgetInclusiveText, specs]

                # Process any calc statements
                calc_type = dtcalcNone
                if "lower" in node_def.keys():
                    calc_list.append((dtcalcLettering, dtcalcLower))

                elif "upper" in node_def.keys():
                    calc_list.append((dtcalcLettering, dtcalcUpper))

                elif "capitalize" in node_def.keys():
                    calc_list.append((dtcalcLettering, dtcalcCapitalize))

                if is_data_value('ascii-replace', node_def, list) and len(node_def['ascii-replace']) > 0:
                    calc_list.append((dtcalcAsciiReplace, tuple(node_def["ascii-replace"])))

                if is_data_value('lstrip', node_def, str):
                    calc_list.append((dtcalcLstrip, node_def["lstrip"]))

                if is_data_value('rstrip', node_def, str):
                    calc_list.append((dtcalcRstrip, node_def["rstrip"]))

                if is_data_value('sub', node_def, list) and len(node_def['sub']) > 1:
                    sl = []
                    for i in range(int(len(node_def['sub'])/2)):
                        sl.append((node_def['sub'][i*2], node_def['sub'][i*2+1]))

                    if len(sl) > 0:
                        calc_list.append((dtcalcSub, tuple(sl)))

                if is_data_value('split', node_def, list) and len(node_def['split']) > 0:
                    sl = []
                    if not isinstance(node_def['split'][0],(list, tuple)):
                        slist = [node_def['split']]

                    else:
                        slist = node_def['split']

                    for s in slist:
                        if isinstance(s[0], (str,unicode)) and len(s) >1:
                            sp = [s[0]]
                            if s[1] == 'list-all':
                                sp.append(s[1])

                            else:
                                for i in range(1, len(s)):
                                    if isinstance(s[i], int):
                                        sp.append(s[i])

                            if len(sp) >1:
                                sl.append(tuple(sp))

                    if len(sl) > 0:
                        calc_list.append((dtcalcSplit, tuple(sl)))

                if is_data_value('multiplier', node_def, int) and \
                    not data_value('type', node_def, unicode) in ('timestamp', 'datestamp'):
                        calc_list.append((dtcalcMultiply, node_def["multiplier"]))

                if is_data_value('divider', node_def, int) and node_def['divider'] != 0:
                    calc_list.append((dtcalcDivide, node_def["divider"]))

                if is_data_value('replace', node_def, dict):
                    rl1 = []
                    rl2 = []
                    for k, v in node_def["replace"].items():
                        if isinstance(k, (str, unicode)):
                            rl1.append(k.lower())
                            rl2.append(v)

                    if len(rl1) > 0:
                        calc_list.append((dtcalcReplace, tuple(rl1), tuple(rl2)))

                if len(calc_list) > 0:
                    node_type += dthasCalc

                if "default" in node_def.keys():
                    node_type += dthasDefault
                    ndefault = node_def["default"]

                # Process any type statement
                if is_data_value('type', node_def, unicode):
                    if node_def['type'] == 'timestamp':
                        if is_data_value('multiplier', node_def, int) and node_def['multiplier'] != 0:
                            type_def = (dttypeTimeStamp, node_def['multiplier'])

                        else:
                            type_def = (dttypeTimeStamp, 1)

                    elif node_def['type'] == 'datetimestring':
                        type_def = (dttypeDateTimeString, data_value('datetimestring', \
                            node_def, str, self.cdata_def["datetimestring"]))

                    elif node_def['type'] == 'time':
                        tt = self.cdata_def["time-type"]
                        if is_data_value('time-type', node_def, list) \
                          and is_data_value(['time-type',0], node_def, int) \
                          and data_value(['time-type',0], node_def, int) in (12, 24):
                            tt = [data_value(['time-type', 0], node_def, list),
                                    data_value(['time-type', 1], node_def, str, 'am'),
                                    data_value(['time-type', 2], node_def, str, 'pm')]

                        type_def = (dttypeTime, tt,data_value('time-splitter', \
                            node_def, str, self.cdata_def["time-splitter"]))

                    elif node_def['type'] == 'timedelta':
                            type_def = (dttypeTimeDelta, )

                    elif node_def['type'] == 'date':
                        type_def = (dttypeDate,
                            data_value('date-sequence', node_def, list, self.cdata_def["date-sequence"]),
                            data_value('date-splitter', node_def, str, self.cdata_def["date-splitter"]))

                    elif node_def['type'] == 'datestamp':
                        if is_data_value('multiplier', node_def, int) and node_def['multiplier'] != 0:
                            type_def = (dttypeDateStamp, node_def['multiplier'])

                        else:
                            type_def = (dttypeDateStamp, 1)

                    elif node_def['type'] == 'relative-weekday':
                        type_def = (dttypeRelativeWeekday, )

                    elif node_def['type'] == 'string':
                        type_def = (dttypeString, )

                    elif node_def['type'] == 'int':
                        type_def = (dttypeInteger, )

                    elif node_def['type'] == 'float':
                        type_def = (dttypeFloat, )

                    elif node_def['type'] == 'boolean':
                        type_def = (dttypeBoolean, )

                    elif node_def['type'] == 'lower-ascii':
                        type_def = (dttypeLowerAscii, )

                    elif node_def['type'] == 'str-list':
                        type_def = (dttypeStringList,
                            data_value('str-list-splitter', node_def, str, self.cdata_def["str-list-splitter"]),
                            data_value("omit-empty-list-items", node_def, bool, False))

                    elif node_def['type'] == 'list':
                        type_def = (dttypeList, )

                    if len(type_def) > 0:
                        node_type += dthasType

                if not (path_type & dtpathMulti) or "first" in node_def.keys() or "last" in node_def.keys():
                    node_type += dtgetOnlyOne
                    if "last" in node_def.keys():
                        node_type += dtgetLast

                if is_data_value('member-off', node_def, unicode):
                    memberoff = node_def["member-off"]
                    node_type += dtisMemberOff

            return (node_type, tuple(sel_node), tuple(calc_list), tuple(type_def), memberoff, nlink, ndefault)

        with self.tree_lock:
            if init_errors:
                self.errorcode = dtDataDefOK

            self.ddtype = ptype
            self.link_list = {"values": [],"nodes": []} if link_list == None else link_list
            if not isinstance(self.link_list, dict):
                self.link_list = {"values": [],"nodes": []}

            if not is_data_value("values", self.link_list, list):
                self.link_list["values"] = []

            if not is_data_value("nodes", self.link_list, list):
                self.link_list["nodes"] = []

            if not isinstance(path_def, (list, tuple)):
                self.errorcode |= dtInvalidPathDef
                self.warn('An invalid path_def "%s" was encountered. It must be a list.' % \
                    ( path_def, ), dtConversionWarning, 1)
                return tuple()

            pd = []
            for nd in path_def:
                if isinstance(nd, dict):
                    pd.append(nd)

            dpath=[]
            for n in range(len(pd)):
                inode = pd[n]
                # Add any name definition as independent node
                if (path_type & dtpathWithNames) and "name" in inode.keys():
                    dpath.append(convert_data_extraction(inode["name"], dtstoreName))

                # Look for node selection statements
                sel_node = [dtisNodeSel, dtselNone]
                if not (path_type & dtpathMulti) or "first" in inode.keys() or "last" in inode.keys():
                    sel_node[0] += dtgetOnlyOne
                    if "last" in inode.keys():
                        sel_node[0] += dtgetLast

                for i in range(1, dtselPosMax):
                    sel_node.append(None)

                if "path" in inode.keys():
                    for ttext, dtsel in (
                            ("all", dtselPathAll),
                            ("parent", dtselPathParent),
                            ("root", dtselPathRoot)):
                        if inode["path"] == ttext:
                            sel_node[1] = dtsel
                            break

                    else:
                        if not inode["path"] in self.link_list["nodes"]:
                            self.errorcode |= (dtInvalidNodeLink + dtDataDefInvalid)
                            self.warn('NodeID: %s is not jet stored' % ( inode["path"], ), dtConversionWarning, 1)
                            continue

                        sel_node[1] = dtselPathLink
                        sel_node[dtselPos[dtselPathLink]] = inode["path"]

                else:
                    for ttext, dtsel, tree_type, singleval in (
                            ("key", dtselKey, "json", True),
                            ("tag", dtselTag, "html", True),
                            ("keys", dtselKeys, "json", False),
                            ("tags", dtselTags, "html", False)):
                        if ttext in inode.keys() and self.ddtype in (tree_type, ""):
                            self.ddtype = tree_type
                            sel_node[1] = dtsel
                            if singleval:
                                sel_node[dtselPos[dtsel]] = convert_value_link(inode[ttext])

                            else:
                                sel_node[dtselPos[dtsel]] = convert_value_list(inode[ttext])

                            break

                if sel_node[1] in (dtselNone, dtselTag, dtselTags, dtselKeys) and "index" in inode.keys():
                    sel_node[1] += dtselIndex
                    sel_node[dtselPos[dtselIndex]] = convert_value_list(inode["index"], True)

                if sel_node[1] not in (dtselNone, dtselPathParent, dtselPathRoot, dtselPathLink):
                    # Look for detail node selection statements
                    for ttext, dtsel in (
                            ("text", dtselText),
                            ("tail", dtselTail)):
                        if ttext in inode.keys() and self.ddtype in ("html", ""):
                            self.ddtype = "html"
                            sel_node[1] += dtsel
                            sel_node[dtselPos[dtsel]] = convert_value_list(inode[ttext])

                    for ttext, dtsel, tree_type in (
                            ("childkeys", dtselChildKeys, "json"),
                            ("attrs", dtselAttrs, "html"),
                            ("notchildkeys", dtselNotChildKeys, "json"),
                            ("notattrs", dtselNotAttrs, "html")):
                        if ttext in inode.keys() and self.ddtype in (tree_type, ""):
                            self.ddtype = tree_type
                            sel_node[1] += dtsel
                            if isinstance(inode[ttext], dict):
                                sel_node[dtselPos[dtsel]] = (convert_attr_dict(inode[ttext]), )

                            elif isinstance(inode[ttext], (list, tuple)):
                                dtt = []
                                for ldict in inode[ttext]:
                                    if isinstance(ldict, dict):
                                        dtt.append(convert_attr_dict(ldict))

                                sel_node[dtselPos[dtsel]] = tuple(dtt)

                if sel_node[1] > 0:
                    # Add any found node selection statements as independent node
                    while sel_node[-1] == None:
                        sel_node.pop(-1)

                    dpath.append(tuple(sel_node))

                # Add any node link statement as independent node
                if "node" in inode.keys():
                    self.link_list["nodes"].append(inode["node"])
                    dpath.append((dtisNodeLink, inode["node"]))

                # Add any link statement as independent node
                if "link" in inode.keys():
                    self.link_list["values"].append(inode["link"])
                    if (path_type & dtpathWithValue) and n ==len(pd) -1:
                        dpath.append(convert_data_extraction(inode, dtstoreLinkValue + dtstorePathValue))

                    else:
                        dpath.append(convert_data_extraction(inode, dtstoreLinkValue))

                elif (path_type & dtpathWithValue) and n ==len(pd) -1:
                    dpath.append(convert_data_extraction(inode, dtstorePathValue))

            return tuple(dpath)

    def convert_link_def(self, link_def, key, maxid, init_errors = True):
        def convert_funcid(ldict, key, maxid):
            if ldict['funcid'] < 100 and not ldict['funcid'] in self.known_linkid:
                self.warn('Requested link function ID "%s" in the "%s" link statement is unknown'% \
                    (ldict["funcid"], key), dtConversionWarning, 2, 3)

                self.errorcode |= dtInvalidLinkDef
                return (dtlinkNone, None, None)

            funcdata = []
            for fd in data_value("data", ldict, list):
                if is_data_value("varid", fd, int):
                    funcdata.append(convert_varid(fd, key, maxid))

                elif is_data_value("funcid", fd, int):
                    funcdata.append(convert_funcid(fd, key, maxid))

                else:
                    funcdata.append((dtlinkValue, fd))

            return check_extras([dtlinkFuncID, (ldict["funcid"], funcdata)], ldict, key)

        def convert_varid(ldict, key, maxid):
            if 0 <= ldict["varid"] <= maxid:
                return check_extras([dtlinkVarID, (ldict["varid"], )], ldict, key)

            elif ldict["varid"] > maxid:
                self.warn('Requested datavalue ID "%s" in the "%s" link statement\n'% (ldict["varid"], key) + \
                    ' is higher then the number of value_defs suplied', dtConversionWarning, 2, 3)

            elif ldict["varid"] <0 :
                self.warn('Requested datavalue ID "%s" in the "%s" link statement is Negative!'% \
                    (ldict["varid"], key), dtConversionWarning, 2, 3)

            self.errorcode |= dtInvalidLinkDef
            return (dtlinkNone, None, None)

        def check_extras(link_node, ldict, key):
            for i in range(1, dtlinkPosMax):
                link_node.append(None)

            if is_data_value('default', ldict):
                link_node[0] += dtlinkhasDefault
                link_node[dtlinkPos[dtlinkhasDefault]] = ldict['default']

            if is_data_value('regex', ldict, str):
                link_node[0] += dtlinkhasRegex
                link_node[dtlinkPos[dtlinkhasRegex]] = ldict['regex']

            if is_data_value('type', ldict, str):
                if ldict['type'] == "string":
                    link_node[dtlinkPos[dtlinkhasType]] = dttypeString

                elif ldict['type'] == "lower":
                    link_node[dtlinkPos[dtlinkhasType]] = dttypeLower

                elif ldict['type'] == "upper":
                    link_node[dtlinkPos[dtlinkhasType]] = dttypeUpper

                elif ldict['type'] == "capitalize":
                    link_node[dtlinkPos[dtlinkhasType]] = dttypeCapitalize

                elif ldict['type'] == "int":
                    link_node[dtlinkPos[dtlinkhasType]] = dttypeInteger

                elif ldict['type'] == "float":
                    link_node[dtlinkPos[dtlinkhasType]] = dttypeFloat

                elif ldict['type'] == "bool":
                    link_node[dtlinkPos[dtlinkhasType]] = dttypeBoolean

                else:
                    self.warn('Invalid type "%s" requested for the "%s" link statement'% \
                        (ldict['type'], key),dtConversionWarning , 2, 3)

                if link_node[dtlinkPos[dtlinkhasType]] != None:
                    link_node[0] += dtlinkhasType

            if is_data_value('calc', ldict, dict):
                calc_type = dtcalcNone
                calc_list = []
                if is_data_value(['calc', 'multiplier'], ldict, float) and ldict['calc']['multiplier'] != 0:
                    calc_list.append((dtcalcMultiply, ldict['calc']['multiplier']))

                if is_data_value(['calc', 'divider'], ldict, float) and ldict['calc']['divider'] != 0:
                    calc_list.append((dtcalcDivide, ldict['calc']['divider']))

                if len(calc_list)> 0:
                    link_node[0] += dtlinkhasCalc
                    link_node[dtlinkPos[dtlinkhasCalc]] = tuple(calc_list)

            if is_data_value('max length', ldict, int) and ldict['max length'] > 0:
                link_node[0] += dtlinkhasMax
                link_node[dtlinkPos[dtlinkhasMax]] = ldict['max length']

            if is_data_value('min length', ldict, int) and ldict['min length'] > 0:
                link_node[0] += dtlinkhasMin
                link_node[dtlinkPos[dtlinkhasMin]] = ldict['min length']

            while len(link_node) > 3 and link_node[-1] == None:
                link_node.pop(-1)

            return tuple(link_node)

        with self.tree_lock:
            if link_def == None:
                link_node = (dtlinkNone, None, None)

            elif is_data_value("varid", link_def, int):
                link_node = convert_varid(link_def, key, maxid)

            elif is_data_value("funcid", link_def, int):
                link_node = convert_funcid(link_def, key, maxid)

            elif is_data_value("value", link_def):
                link_node = (dtlinkValue, (link_def["value"], ))

            else:
                self.warn('No "varid", "funcid" or "value" keyword supplied\n' + \
                    'in the link_def for the %s keyword!'% \
                    (key, ), dtConversionWarning, 2, 2)
                self.errorcode |= dtInvalidLinkDef
                link_node = (dtlinkNone, None, None)

            return link_node

    def convert_data_def(self, data_def = None, ptype = "", include_url = True, include_links = True):
        def _get_url_part(u_part):
            urlid = None
            if isinstance(u_part, (str, unicode)):
                return u_part

            # get a variable
            elif isinstance(u_part, int):
                urlid = u_part
                u_data = tuple([])

            elif isinstance(u_part, list):
                if is_data_value(0, u_part, int):
                    urlid = u_part[0]
                    u_data = tuple(u_part[1:])

                else:
                    urlid = 0
                    u_data = tuple(u_part)

            if urlid in self.known_urlid or urlid > 99:
                return (urlid, u_data)

            else:
                return None

        with self.tree_lock:
            self.link_list = {"values": [],"nodes": []}
            if isinstance(data_def, dict):
                self.data_def = data_def

            self.ddtype = ptype
            if 'json' in self.data_value(["data-format"], str):
                self.ddtype = 'json'

            elif 'html' in self.data_value(["data-format"], str):
                self.ddtype = 'html'

            elif 'xml' in self.data_value(["data-format"], str):
                self.ddtype = 'html'

            self.cdata_def = {}
            self.cdata_def["datetimestring"] = self.data_value("datetimestring", str, default = u"%Y-%m-%d %H:%M:%S")
            self.cdata_def["date-sequence"] = self.data_value("date-sequence", list, default = ["y","m","d"])
            self.cdata_def["date-splitter"] = self.data_value("date-splitter", str, default = '-')
            self.cdata_def["month-names"] = self.data_value("month-names", list)
            self.cdata_def["weekdays"] = self.data_value("weekdays", list)
            self.cdata_def["relative-weekdays"] = self.data_value( "relative-weekdays", dict)
            self.cdata_def["time-splitter"] = self.data_value("time-splitter", str, default = ':')
            self.cdata_def["time-type"] = [24]
            if self.is_data_value('time-type', list) \
                and self.data_value(['time-type',0], int) in (12, 24):
                    self.cdata_def["time-type"] = [self.data_value(['time-type', 0], list),
                                    self.data_value(['time-type', 1], str, 'am'),
                                    self.data_value(['time-type', 2], str, 'pm')]

            self.cdata_def["timezone"] = self.data_value(["timezone"], str, default='utc')
            try:
                self.cdata_def["tz"] = pytz.timezone(self.cdata_def["timezone"])

            except:
                self.warn('Invalid timezone definition: %s' % (self.data_value(["timezone"]), ), dtConversionWarning, 2)
                self.cdata_def["timezone"] = 'utc'
                self.cdata_def["tz"] = pytz.utc

            self.cdata_def["str-list-splitter"] = self.data_value("str-list-splitter", str, default = '\|')
            self.cdata_def["value-filters"] = self.data_value("value-filters", dict)
            self.cdata_def["text_replace"] = self.data_value("text_replace", list)
            self.cdata_def["unquote_html"] = self.data_value("unquote_html", list)
            self.cdata_def["enclose-with-html-tag"] = self.data_value("enclose-with-html-tag", bool, default = False)
            self.cdata_def["autoclose-tags"] = self.data_value("autoclose-tags", list)
            if include_url:
                if self.is_data_value("url", (str, unicode)):
                    dd_url = [self.data_def["url"]]

                else:
                    dd_url = []
                    for u_part in self.data_value("url", list):
                        uval = _get_url_part(u_part)
                        if uval == None:
                            self.warn('Invalid url_part definition: %s' % (u_part, ), dtConversionWarning, 2)

                        else:
                            dd_url.append(uval)

                self.cdata_def["url"] = tuple(dd_url)
                self.cdata_def["url-type"] = self.data_value("url-type", int, 0)
                self.cdata_def["encoding"] = self.data_value("encoding", str)
                self.cdata_def["url-header"] = {}
                if self.is_data_value("url-header", dict):
                    for k, u_part in self.data_value(["url-header"], dict).items():
                        uval = _get_url_part(u_part)
                        if uval == None:
                            self.warn('Invalid url-header definition: %s' % (u_part, ), dtConversionWarning, 2)

                        else:
                            self.cdata_def["url-header"][k] = uval

                elif self.is_data_value(["accept-header"], str, True):
                    self.cdata_def["url-header"] = {"Accept": self.data_value(["accept-header"], str)}

                self.cdata_def["url-data"] = {}
                for k, u_part in self.data_value(["url-data"], dict).items():
                    uval = _get_url_part(u_part)
                    if uval == None:
                        self.warn('Invalid url-data definition: %s' % (u_part, ), dtConversionWarning, 2)

                    else:
                        self.cdata_def["url-data"][k] = uval

                self.cdata_def["data-format"] = self.data_value("data-format", str)
                self.cdata_def["default-item-count"] = self.data_value("default-item-count", int, default = 0)
                self.cdata_def["item-range-splitter"] = self.data_value("item-range-splitter", str, default = "-")
                self.cdata_def["date-range-splitter"] = self.data_value("date-range-splitter", str, default = "~")
                self.cdata_def["url-date-type"] = self.data_value("url-date-type", int, default = 0)
                self.cdata_def["url-weekdays"] = self.data_value("url-weekdays", list)
                self.cdata_def["url-relative-weekdays"] = {}
                for dname, dno in self.data_value( "url-relative-weekdays", dict).items():
                    try:
                         self.cdata_def["url-relative-weekdays"][int(dno)] = dname

                    except:
                        pass

                self.cdata_def["url-date-multiplier"] = self.data_value("url-date-multiplier", int, default = 1)
                if self.cdata_def["url-date-multiplier"] == 0:
                    self.cdata_def["url-date-multiplier"] = 1

                if self.is_data_value("url-date-format", str):
                    self.cdata_def["url-date-format"] = self.data_value("url-date-format", str)

                else:
                    self.cdata_def["url-date-format"] = None

            self.cdata_def["data"] = {}
            self.cdata_def["data"]["sort"] = []
            for s_rule in self.data_value(['data',"sort"],list):
                if not "path" in s_rule.keys() or not "childkeys" in s_rule.keys():
                    continue

                if not isinstance(s_rule["path"],list):
                    s_rule["path"] = [s_rule["path"]]

                if not isinstance(s_rule["childkeys"],list):
                    s_rule["childkeys"] = [s_rule["childkeys"]]

                self.cdata_def["data"]["sort"].append((tuple(s_rule["path"]), tuple(s_rule["childkeys"])))

            self.cdata_def["data"]["init-path"] = self.convert_path_def(self.data_value(['data',"init-path"],list),
                self.ddtype, dtpathInit, self.link_list, False)
            di = []
            if self.is_data_value(['data',"iter"],list):
                ol = self.data_def["data"]["iter"]

            else:
                ol = [self.data_value(['data'],dict)]

            slist = []
            for sl in ol:
                if is_data_value(["values2"], sl ,list):
                    slist.append({"key-path": data_value(["key-path"], sl,list), "values": data_value(["values2"], sl,list)})

                else:
                    slist.append({"key-path": data_value(["key-path"], sl,list), "values": data_value(["values"], sl,list)})

            value_count = 0
            for sel_dict in slist:
                conv_dict = {}
                conv_dict["key-path"] = self.convert_path_def(data_value("key-path", sel_dict, list), \
                        self.ddtype, dtpathKey, self.link_list, False)
                vv = []
                vlist = data_value("values", sel_dict, list)
                value_count = len(vlist) if len(vlist) > value_count else value_count
                for sel_val in vlist:
                    conv_val = self.convert_path_def(sel_val, self.ddtype, dtpathValue, self.link_list, False)
                    vv.append(conv_val)

                conv_dict["values"] = tuple(vv)
                di.append(conv_dict)

            self.cdata_def["data"]["iter"] = tuple(di)
            self.cdata_def["dttype"] = self.ddtype
            self.cdata_def["dtversion"] = self.dtversion()
            if include_links:
                self.cdata_def["empty-values"] = self.data_value('empty-values', list, default = [None, ''])
                self.cdata_def["values"] = {}
                for k, v in self.data_value("values", dict).items():
                    self.cdata_def["values"][k] = self.convert_link_def(v, k, value_count,False)

            return self.errorcode

    def dtversion(self):
        return tuple(version()[1:4])

    def simplefilter(self, action, category=Warning, lineno=0, append=0, severity=0):
        with self.tree_lock:
            sys.modules['DataTreeGrab']._warnings.simplefilter(action, category, lineno, append, self.caller_id, severity)

    def warn(self, message, category, severity, stacklevel = 2):
        sys.modules['DataTreeGrab']._warnings.warn(message, category, self.caller_id, severity, stacklevel)

    def is_data_value(self, searchpath, dtype = None, empty_is_false = False):
        return is_data_value(searchpath, self.data_def, dtype, empty_is_false)

    def data_value(self, searchpath, dtype = None, default = None):
        return data_value(searchpath, self.data_def, dtype, default)

# end DataDef_Convert

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
        d_def = path_def if isinstance(path_def, tuple) else (path_def, )
        nm = None
        childs = []
        def match_node(node, sel_node=dtselNone):
            # check through the HTML/JSON specific functions
            nfound = node.match_node(node_def = d_def[0], link_values=links["values"], sel_node=sel_node)
            if nfound  and sel_node in (dtselNone, dtselTag, dtselTags, dtselKeys) and (d_def[0][1] & dtselIndex):
                nfound = node.check_index(d_def[0][dtselPos[dtselIndex]], links["values"])

            if nfound:
                if self.dtree.show_result:
                    self.dtree.print_text(u'    found node %s;\n      %s' % \
                        (node.print_node(), node.print_node_def(d_def[0])))

            return nfound

        while True:
            if len(d_def) == 0:
                d_def = dtemptyNodeDef
                childs = [(self, None)]
                break

            ndef_type = (d_def[0][0] & dtisGroup)
            if ndef_type == dtisNodeSel:
                sel_node = (d_def[0][1] & dtselMain)
                if sel_node == dtselPathLink:
                    if is_data_value(["nodes", d_def[0][2]], links, DATAnode) and match_node(links["nodes"][d_def[0][2]], sel_node):
                        childs = links["nodes"][d_def[0][2]].get_children(path_def = d_def[1:], links=links)

                elif sel_node == dtselPathRoot:
                    if match_node(self.root, sel_node):
                        childs = self.root.get_children(path_def = d_def[1:], links=links)

                elif sel_node == dtselPathParent:
                    if match_node(self.parent, sel_node):
                        childs = self.parent.get_children(path_def = d_def[1:], links=links)

                else:
                    clist = self.children[:]
                    if (d_def[0][0] & dtgetLast):
                        clist.reverse()

                    for item in clist:
                        if match_node(item, sel_node):
                            childs = extend_list(childs, item.get_children(path_def = d_def[1:], links=links))
                            if (d_def[0][0] & dtgetOnlyOne) and len(childs) > 0:
                                break

                break

            elif ndef_type == dtisValue:
                val = self.find_value(d_def[0])
                if self.dtree.show_result:
                    if isinstance(val, (str,unicode)):
                        self.dtree.print_text(u'    found nodevalue (="%s"): %s\n      %s' % \
                            (val, self.print_node(), self.print_node_def(d_def[0])))

                    else:
                        self.dtree.print_text(u'    found nodevalue (=%s): %s\n      %s' % \
                            (val, self.print_node(), self.print_node_def(d_def[0])))

                if (d_def[0][0] & dtstoreLinkValue):
                    links["values"][d_def[0][dtgetPos[dtstoreLinkValue]]] = val

                if (d_def[0][0] & dtstorePathValue):
                    childs = [(self, val)]
                    break

                d_def = d_def[1:]

            elif ndef_type == dtisNodeLink:
                if self.dtree.show_result:
                    self.dtree.print_text(self.print_node_def(d_def[0]))

                links["nodes"][d_def[0][1]] = self
                d_def = d_def[1:]

            elif ndef_type == dtstoreName:
                nm = self.find_name(d_def[0])
                d_def = d_def[1:]

            else:
                break

        # Return the found nodes, adding if defined a name
        if data_value([0, 0], childs) == self:
            # This is an end node, so we store link values to use on further searches
            self.end_links["values"] = links["values"].copy()
            self.end_links["nodes"] = links["nodes"].copy()
            if self.dtree.show_result:
                self.dtree.print_text(u'  adding node %s' % (self.print_node(), ))

        if nm == None:
            return childs

        else:
            return [{nm: childs}]

    def check_index(self, ilist, link_values):
        for v in ilist:
            il = self.get_value(v, link_values, 'index', 'int')
            if (il[1] & dtvalLinkPrevious):
                if self.child_index < il[0]:
                    return True

            elif (il[1] & dtvalLinkNext):
                if self.child_index > il[0]:
                    return True

            elif self.child_index == il[0]:
                return True

        return False

    def get_value_list(self, vlist, link_values, valuetype = '', ltype = None):
        rlist = []
        for v in vlist:
            rlist.append(self.get_value(v, link_values, valuetype, ltype)[0])

        return rlist

    def get_value(self, sub_def, link_values, valuetype = '', ltype = None):
        def wrong_value(value, vtype):
            if sub_def[0] == dtvalValue:
                self.dtree.warn('Invalid %s %s "%s" requested. Should be %s.' % \
                    (valuetype, "matchvalue", value, vtype), dtParseWarning, 3)

            else:
                self.dtree.warn('Invalid %s %s "%s" requested. Should be %s.' % \
                    (valuetype, "linkvalue", value, vtype), dtParseWarning, 2)

            return (None, 0)

        if sub_def[0] == dtvalValue:
            value = sub_def[1]

        else:
            if not is_data_value(sub_def[1], link_values):
                self.dtree.warn('You requested a link, but link value %s is not stored!' % \
                    (sub_def[1], ), dtParseWarning, 3)
                return (None, 0)

            value = link_values[sub_def[1]]

        if value == None:
            pass

        elif ltype == 'int':
            try:
                value = int(value)

            except:
                return wrong_value(value, "integer")

        elif ltype == 'lower':
            try:
                value = unicode(value).lower()

            except:
                return wrong_value(value, "string")

        elif ltype == 'str':
            try:
                value = unicode(value)

            except:
                return wrong_value(value, "string")

        if isinstance(value, (int, float)):
            if sub_def[0] & dtvalLinkMin:
                value -= sub_def[2]

            elif sub_def[0] & dtvalLinkPlus:
                value += sub_def[2]

        return (value, sub_def[0] & (dtvalLinkNext + dtvalLinkPrevious))
        # zip(value, sub_def[0] & (dtvalLinkNext + dtvalLinkPrevious))[0]

    def match_node(self, node_def = None, link_values = None, sel_node=dtselNone):
        # Detailed in HTML/JSON class, return True on matching the node_def
        # Return False on failure to match
        return False

    def find_name(self, node_def):
        nv = self.find_value(node_def)
        if nv != None:
            if self.dtree.show_result:
                if isinstance(nv, (str,unicode)):
                    self.dtree.print_text(u'  storing name = "%s" from node: %s\n      %s' % \
                        (nv, self.print_node(), self.print_node_def(node_def)))

                else:
                    self.dtree.print_text(u'  storing name = %s from node: %s\n      %s' % \
                        (nv, self.print_node(), self.print_node_def(node_def)))
            return nv

    def find_value(self, node_def = None):
        # Detailed in child class Collect and return any value
        if node_def[0] & dtisGroup not in (dtisValue, dtstoreName):
            return None

        sv = self.find_node_value(node_def[1])
        if node_def[0] & dthasCalc:
            sv = self.dtree.calc_value(sv, node_def[dtgetPos[dthasCalc]])

        if sv == None and node_def[0] & dthasDefault:
            sv = node_def[dtgetPos[dthasDefault]]

        if node_def[0] & dthasType:
            sv = self.dtree.calc_type(sv, node_def[dtgetPos[dthasType]])

        if node_def[0] & dtisMemberOff:
            imo = node_def[dtgetPos[dtisMemberOff]]
            if not imo in self.dtree.value_filters.keys() or not sv in self.dtree.value_filters[imo]:
                sv = NULLnode()

        # Make sure a string is unicode and free of HTML entities
        if isinstance(sv, (str, unicode)):
            sv = re.sub('\n','', re.sub('\r','', self.dtree.un_escape(unicode(sv)))).strip()

        return sv

    def find_node_value(self, val_def=None):
        # Detailed in child class Collect and return any value
        return self.value

    def print_node_def(self, node_def):
        def print_val_def(val_def):
            return val_def

        def print_calc_def(calc_def):
            return calc_def

        def print_type_def(type_def):
            vtype = {
                    dttypeTimeStamp: "TimeStamp",
                    dttypeDateTimeString: "DateTimeString",
                    dttypeTime: "Time",
                    dttypeTimeDelta: "TimeDelta",
                    dttypeDate: "Date",
                    dttypeDateStamp: "DateStamp",
                    dttypeRelativeWeekday: "RelativeWeekday",
                    dttypeString: "String",
                    dttypeInteger: "Integer",
                    dttypeFloat: "Float",
                    dttypeBoolean: "Boolean",
                    dttypeLowerAscii: "LowerAscii",
                    dttypeStringList: "StringList",
                    dttypeList: "List"}
            return "%s: %s" % (vtype[type_def[0]], type_def)

        def print_node_sel_def(sel_def, spc):
            return sel_def

                #~ if sel_node == dtselPathLink:
                #~ elif sel_node == dtselPathRoot:
                #~ elif sel_node == dtselPathParent:
                #~ elif sel_node == dtselPathAll:

        nodetype = {
                dtisNodeSel: "Node",
                dtstoreName: "Name",
                dtisValue: "Value"}

        spc = self.dtree.get_leveltabs(self.level,4)
        if self.dtree.is_data_value("dtversion", tuple):
            ndef_type = (node_def[0] & dtisGroup)
            if ndef_type == dtisNodeSel:
                sel_node = (node_def[1] & dtselMain)
                rstr = u'%s%s Selection node_def' % (spc, nodetype[ndef_type])
                if node_def[0] & dtgetLast:
                    rstr = u'%s returning only the last found node :\n%s          ' % (rstr, spc)

                elif node_def[0] & dtgetOnlyOne:
                    rstr = u'%s returning only the first found node :\n%s          ' % (rstr, spc)

                else:
                    rstr = u'%s :\n%s          ' % (rstr, spc)

                rstr = u'%s%s\n%s          ' % (rstr, print_node_sel_def(node_def, spc), spc)

            elif ndef_type == dtisNodeLink:
                rstr = u'%sSaving Node (%s) as ID = %s' % (spc, self.print_node(), node_def[1])

            elif ndef_type in (dtstoreName, dtisValue):
                rstr = u'%s%s Selection node_def: %s' % (spc, nodetype[ndef_type], print_val_def(node_def[1]))
                if node_def[0] & dthasCalc:
                    rstr = u'%s\n%s      with calcfunctions: (%s)' % \
                        (rstr, spc, print_calc_def(node_def[dtgetPos[dthasCalc]]))

                if node_def[0] & dthasDefault:
                    rstr = u'%s\n%s       with a default value of: (%s)' % (rstr, spc, node_def[dtgetPos[dthasDefault]])

                if node_def[0] & dthasType:
                    rstr = u'%s\n%s      with a type definition as: %s' % \
                        (rstr, spc, print_type_def(node_def[dtgetPos[dthasType]]))

                if node_def[0] & dtisMemberOff:
                    rstr = u'%s\n%s      which must be present in the %s value_filter list' % \
                        (rstr, spc, node_def[dtgetPos[dtisMemberOff]])

                if node_def[0] & dtstoreLinkValue:
                    rstr = u'%s\n%s    storing it as value link: (%s)' % (rstr, spc, node_def[dtgetPos[dtstoreLinkValue]])

                if node_def[0] & dtstorePathValue:
                    rstr = u'%s\n%s    returning it as path_def value' % (rstr, spc)

        else:
            rstr = u'%snode_def: ' % (spc, )
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

    def match_node(self, tag = None, attributes = None, node_def = None, link_values = None, sel_node=dtselNone):
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

        if sel_node == dtselTag:
            if not self.get_value(node_def[dtselPos[dtselTag]], link_values, 'tag', 'lower')[0] in (None, self.tag.lower()):
                # The requested tag doesn't matches
                return False

        elif sel_node == dtselTags:
            if not self.tag.lower() in self.get_value_list(node_def[dtselPos[dtselTags]], link_values, 'tag', 'lower'):
                # This tag isn't in the list with requested tags
                return False

        if (node_def[1] & dtselText):
            if not self.text.lower() in self.get_value_list(node_def[dtselPos[dtselText]], link_values, 'text', 'lower'):
                # This text isn't in the list with requested values
                return False

        if (node_def[1] & dtselTail):
            if not self.tail.lower() in self.get_value_list(node_def[dtselPos[dtselTail]], link_values, 'tail', 'lower'):
                # This tailtext isn't in the list with requested values
                return False

        if (node_def[1] & dtselAttrs):
            for cd in node_def[dtselPos[dtselAttrs]]:
                # For each set
                for ck in cd:
                    # For each attribute ck[0]
                    if self.is_attribute(ck[0]):
                        # The attribute is there
                        alist = self.get_value_list(ck[2], link_values, 'attribute', 'str')
                        if ck[1] == dtattrNot and self.attributes[ck[0]] not in alist:
                            # Without a forbidden value
                            continue

                        elif ck[1] == dtattr and ((len(alist) == 1 and alist[0] == None) or self.attributes[ck[0]] in alist):
                            # With an allowed value
                            continue

                        else:
                            # Mismatch on this set
                            break

                    else:
                        # No Attribute so fail
                        break

                else:
                    # This Set Matches
                    break

            else:
                # No Matching Set found
                return False

        if (node_def[1] & dtselNotAttrs):
            for cd in node_def[dtselPos[dtselNotAttrs]]:
                # For each set
                for ck in cd:
                    # For each attribute ck[0]
                    if self.is_attribute(ck[0]):
                        # The attribute is there
                        alist = self.get_value_list(ck[2], link_values, 'notattrs', 'str')
                        if ck[1] == dtattrNot and self.attributes[ck[0]] in alist:
                            # With an allowed value
                            continue

                        elif ck[1] == dtattr and not ((len(alist) == 1 and alist[0] == None) or self.attributes[ck[0]] in alist):
                            # Without a forbidden value
                            continue

                        else:
                            # Mismatch on this set
                            break

                    else:
                        # No Attribute so OK
                        continue

                else:
                    # This Set Matches
                    break

            else:
                # No Matching Set found
                return False

        return True

    def find_node_value(self, val_def=None):
        if val_def == None:
            return self.text

        val_source = val_def[0] & dtgetGroup
        if val_source == dtgetText:
            sv = self.text

        elif val_source == dtgetAttr:
            sv = self.get_attribute(val_def[1])

        elif val_source == dtgetIndex:
            sv = self.child_index

        elif val_source == dtgetTag:
            sv = self.tag

        elif val_source == dtgetTail:
            sv = self.tail

        elif val_source == dtgetInclusiveText:
            def add_child_text(child, depth, in_ex, tag_list):
                t = u''
                if in_ex == 0 or (in_ex == 1 and child.tag in tag_list) or (in_ex == -1 and child.tag not in tag_list):
                    if child.text != '':
                        t = u'%s %s' % (t, child.text)

                    if depth > 1:
                        for c in child.children:
                            t = u'%s %s' % (t, add_child_text(c, depth - 1, in_ex, tag_list))

                if child.tail != '':
                    t = u'%s %s' % (t, child.tail)

                return t.strip()

            depth = val_def[1][0]
            in_ex = val_def[1][1]
            tag_list = val_def[1][2]
            sv = self.text
            for c in self.children:
                sv = u'%s %s' % (sv, add_child_text(c, depth, in_ex, tag_list))

        elif val_source == dtgetLitteral:
            sv = val_def[1]

        elif val_source == dtgetPresence:
            return True

        else:
            sv = self.text

        return sv

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
            tx = self.find_node_value()
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

    def match_node(self, node_def = None, link_values = None, sel_node=dtselNone):
        if sel_node == dtselKey:
            if self.get_value(node_def[dtselPos[dtselKey]], link_values, 'key')[0] != self.key:
                # The requested key doesn't matches
                return False

        elif sel_node == dtselKeys:
            if not self.key in self.get_value_list(node_def[dtselPos[dtselKeys]], link_values, 'key'):
                # This key isn't in the list with requested keys
                return False

        if (node_def[1] & dtselChildKeys):
            for cd in node_def[dtselPos[dtselChildKeys]]:
                # For each set
                for ck in cd:
                    # For each key ck[0]
                    if ck[0] in self.keys:
                        # The Key is there
                        alist = self.get_value_list(ck[2], link_values, 'childkeys')
                        if ck[1] == dtattrNot and self.get_child(ck[0]).value not in alist:
                            # Without a forbidden value
                            continue

                        elif ck[1] == dtattr and ((len(alist) == 1 and alist[0] == None) or self.get_child(ck[0]).value in alist):
                            # With an allowed value
                            continue

                        else:
                            # Mismatch on this set
                            break

                    else:
                        # No Key so fail
                        break

                else:
                    # This Set Matches
                    break

            else:
                # No Matching Set found
                return False

        if (node_def[1] & dtselNotChildKeys):
            for cd in node_def[dtselPos[dtselNotChildKeys]]:
                # For each set
                for ck in cd:
                    # For each key ck[0]
                    if ck[0] in self.keys:
                        # The Key is there
                        alist = self.get_value_list(ck[2], link_values, 'notchildkeys')
                        if ck[1] == dtattrNot and self.get_child(ck[0]).value in alist:
                            # With an allowed value
                            continue

                        elif ck[1] == dtattr and not ((len(alist) == 1 and alist[0] == None) or self.get_child(ck[0]).value in alist):
                            # Without a forbidden value
                            continue

                        else:
                            # Mismatch on this set
                            break

                    else:
                        # No Key so OK
                        continue

                else:
                    # This Set Matches
                    break

            else:
                # No Matching Set found
                return False

        return True

    def find_node_value(self, val_def=None):
        if val_def == None:
            return self.value

        val_source = val_def[0] & dtgetGroup
        if val_source == dtgetValue:
            sv = self.value

        elif val_source == dtgetIndex:
            sv = self.child_index

        elif val_source == dtgetKey:
            sv = self.key

        elif val_source == dtgetLitteral:
            sv = val_def[1]

        elif val_source == dtgetPresence:
            return True

        else:
            sv = self.value

        return sv

    def print_node(self, print_all = False):
        value = self.find_node_value() if self.type == "value" else '"%s"' % (self.type, )
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

            elif caller_id not in sys.modules['DataTreeGrab']._warnings._ids or warnaction != None:
                sys.modules['DataTreeGrab']._warnings.set_warnaction(warnaction, caller_id)

            self.ddconv = DataDef_Convert(warnaction = warnaction , warngoal = warngoal, caller_id = caller_id)

    def _get_type(self, node_def):
        if (node_def[0] & dtisGroup) == dtisValue and (node_def[0] & dthasType):
            return node_def[dtgetPos[dthasType]][0]

        else:
            return dttypeNone

    def _get_default(self, node_def):
        if (node_def[0] & dtisGroup) == dtisValue and (node_def[0] & dthasDefault):
            return node_def[dtgetPos[dthasDefault]]

        else:
            return None

    def check_data_def(self, data_def):
        with self.tree_lock:
            if is_data_value("dtversion", data_def, tuple):
                self.data_def = data_def
                if data_def["dtversion"] != self.ddconv.dtversion():
                    self.warn('Your supplied data_def was converted using version %d.%d.%d.\n' % data_def["dtversion"] + \
                        'You best reconvert it with the current version.', dtdata_defWarning, 2)

            else:
                self.ddconv.convert_data_def(data_def)
                if self.ddconv.errorcode & dtFatalError != dtDataDefOK:
                    return self.ddconv.errorcode & dtFatalError

                self.data_def = self.ddconv.cdata_def

            self.month_names = self.data_def["month-names"]
            self.weekdays = self.data_def["weekdays"]
            self.datetimestring = self.data_def["datetimestring"]
            self.time_splitter = self.data_def["time-splitter"]
            self.date_sequence = self.data_def["date-sequence"]
            self.date_splitter = self.data_def["date-splitter"]
            self.time_type = self.data_def['time-type']
            self.set_timezone(self.data_def["tz"])
            self.value_filters = self.data_def["value-filters"]
            self.str_list_splitter = self.data_def["str-list-splitter"]

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
                        self.warn('Invalid timezone "%s" suplied. Falling back to the old timezone "%s"' \
                            % (timezone, oldtz.tzname), dtdata_defWarning, 2)
                        self.timezone = oldtz

                    else:
                        self.warn('Invalid timezone "%s" suplied. Falling back to UTC' % (timezone, ), dtdata_defWarning, 2)
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
                    self.warn('Invalid or no current_date "%s" suplied. Falling back to NOW' % (cdate, ), dtdata_defWarning, 2)

                self.current_date = self.timezone.normalize(datetime.datetime.now(pytz.utc).astimezone(self.timezone)).date()
                self.current_ordinal = self.current_date.toordinal()

    def set_current_weekdays(self):
        with self.tree_lock:
            rw = self.data_def["relative-weekdays"]

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

            if not isinstance(self.root, DATAnode):
                self.warn('Unable to set a start_node. Invalid dataset!', dtDataWarning, 1)
                return dtStartNodeInvalid

            if self.print_searchtree:
                self.print_text('The root Tree:')
                self.root.print_tree()

            if self.show_result:
                self.print_text(self.root.print_node())

            links = {"values": {},"nodes": {}}
            init_path = self.data_def["data"]["init-path"]
            sn = self.root.get_children(path_def = init_path, links = links)
            if sn == None or len(sn) == 0 or not isinstance(sn[0][0], DATAnode):
                self.warn('"init-path": %s did not result in a valid node. Falling back to the rootnode' \
                    % (init_path, ), dtParseWarning, 2)
                self.start_node = self.root
                return dtStartNodeInvalid

            else:
                self.start_node = sn[0][0]
                return dtDataOK

    def find_data_value(self, path_def, start_node = None, links = None):
        with self.tree_lock:
            if isinstance(path_def, list):
                path_def = self.ddconv.convert_path_def(path_def)

            if not isinstance(path_def, tuple):
                self.warn('Invalid "path_def": %s supplied to "find_data_value"' % (path_def, ), dtParseWarning, 1)
                return

            if len(path_def) == 0:
                path_def = dtemptyNodeDef

            if start_node == None or not isinstance(start_node, DATAnode):
                start_node = self.start_node

            if not isinstance(start_node, DATAnode):
                self.warn('Unable to search the tree. Invalid dataset!', dtDataWarning, 1)
                return

            links = {"values": {},"nodes": {}} if links == None else links
            nlist = start_node.get_children(path_def = path_def, links = links)
            if (path_def[-1][0] & dtisGroup == dtisValue) and (path_def[-1][1][0] & dtgetGroup == dtgetPresence):
                # We return True if exactly one node is found, else False
                return (isinstance(nlist, list) and len(nlist) == 1)

            # Nothing found, so give the default or None
            if not isinstance(nlist, list) or nlist in ([], None):
                if self._get_type(path_def[-1]) == dttypeList:
                    return []

                else:
                    return self._get_default(path_def[-1])

            # We found multiple values
            if len(nlist) > 1 and (path_def[-1][0] & dtgetOnlyOne):
                # There is only one child allowed
                if (path_def[-1][0] & dtgetLast):
                    # There is a request to only return the last
                    nlist = nlist[-1:]

                else:
                    nlist = nlist[:1]

            if len(nlist) > 1 or self._get_type(path_def[-1]) == dttypeList:
                vlist = []
                for node in nlist:
                    if isinstance(node, tuple) and len(node) == 2:
                        vlist.append(node[1])

                    elif isinstance(node, dict):
                        # There is a named subset of the found nodes
                        for k, v in node.items():
                            slist = []
                            for item in v:
                                if isinstance(item, tuple) and len(item) == 2:
                                    slist.append(item[1])

                            vlist.append({k: slist})

                return vlist

            # We found one value
            if isinstance(nlist[0], tuple) and len(nlist[0]) == 2:
                return nlist[0][1]

            elif self._get_type(path_def[-1]) == dttypeList:
                return []

            else:
                return self._get_default(path_def[-1])

    def extract_datalist(self, data_def=None):
        with self.tree_lock:
            if isinstance(data_def, dict):
                x = self.check_data_def(data_def)
                if x > 0:
                    return x

            if not isinstance(self.start_node, DATAnode):
                self.warn('Unable to search the tree. Invalid dataset!', dtDataWarning, 1)
                if self.show_progress:
                    self.progress_queue.put((0, 0))

                return dtStartNodeInvalid

            if self.print_searchtree:
                self.print_text('The %s Tree:' % (self.start_node.print_node(), ))
                self.start_node.print_tree()

            self.result = []
            def_list = []
            for dset in self.data_def['data']['iter']:
                if len(dset["key-path"]) == 0:
                    continue

                if self.show_result:
                    self.print_text(u'parsing keypath: %s' % (dset["key-path"][0], ))

                links = {"values": {},"nodes": {}}
                self.key_list = self.start_node.get_children(path_def = dset["key-path"], links = links)
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

                    if not (isinstance(k, tuple) and len(k) == 2):
                        continue

                    # And if it's a valid node, find the belonging end_links
                    # and value (the last dict in a path list contains the value definition)
                    links = k[0].end_links
                    tlist = [k[1]]
                    if self.show_result:
                        self.print_text(u'parsing key %s' % (tlist, ))

                    for v in dset["values"][:]:
                        if not isinstance(v, tuple) or len(v) == 0:
                            tlist.append(None)
                            continue

                        if self.extract_from_parent and isinstance(k[0].parent, DATAnode):
                            dv = self.find_data_value(v, k[0].parent, links)

                        else:
                            dv = self.find_data_value(v, k[0], links)

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

    def calc_value(self, value, calc_def):
        def calc_warning(text, severity=4):
            self.warn('%s calculation Error on value: "%s"\n   Using node_def: %s' % \
                (text, value, calc_def), dtCalcWarning, severity, 3)

        if not isinstance(calc_def, tuple):
            return value

        for cd in calc_def:
            try:
                if isinstance(value, (str, unicode)):
                    if cd[0] == dtcalcLettering:
                        if cd[1] == dtcalcLower:
                            value = unicode(value).lower().strip()

                        elif cd[1] == dtcalcUpper:
                            value = unicode(value).upper().strip()

                        elif cd[1] == dtcalcCapitalize:
                            value = unicode(value).capitalize().strip()

                    elif cd[0] == dtcalcAsciiReplace:
                        value = value.lower()
                        if len(cd[1]) > 2:
                            value = re.sub(cd[1][2], cd[1][1], value)

                        value = value.encode('ascii','replace')
                        value = re.sub('\?', cd[1][0], value)

                    elif cd[0] == dtcalcLstrip:
                        if value.strip().lower()[:len(cd[1])] == cd[1].lower():
                            value = unicode(value[len(cd[1]):]).strip()

                    elif cd[0] == dtcalcRstrip:
                        if value.strip().lower()[-len(cd[1]):] == cd[1].lower():
                            value = unicode(value[:-len(cd[1])]).strip()

                    elif cd[0] == dtcalcSub:
                        for sset in cd[1]:
                            value = re.sub(sset[0], sset[1], value).strip()

                    elif cd[0] == dtcalcSplit:
                        for sset in cd[1]:
                            try:
                                fill_char = sset[0]
                                if fill_char in ('\\s', '\\t', '\\n', '\\r', '\\f', '\\v', ' ','\\s*', '\\t*', '\\n*', '\\r*', '\\f*', '\\v*'):
                                    fill_char = ' '
                                    value = value.strip()

                                dat = re.split(sset[0],value)
                                if sset[1] == 'list-all':
                                    value = dat

                                else:
                                    value = dat[sset[1]]
                                    for i in range(2, len(sset)):
                                        if ( 0<= sset[i] < len(dat)) or (-len(dat) <= sset[i] < 0):
                                            value = value + fill_char +  dat[sset[i]]

                            except:
                                calc_warning('split')

                elif isinstance(value, (int, float)):
                    if cd[0] == dtcalcMultiply:
                        value = int(value) * cd[1]

                    elif cd[0] == dtcalcDivide:
                        value = int(value) // cd[1]

                if cd[0] == dtcalcReplace:
                    if isinstance(value, (str, unicode)):
                        v = value.strip().lower()

                    else:
                        v = value

                    if v in cd[1]:
                        value = cd[2][cd[1].index(v)]

                    else:
                        value = None

            except:
                #~ traceback.print_exc()
                calc_warning('unknown')

        return value

    def calc_type(self, value, type_def):
        def calc_warning(text, severity=4):
            self.warn('%s typesetting Error on value: "%s"\n   Using node_def: %s' % \
                (text, value, type_def), dtCalcWarning, severity, 3)

        try:
            if type_def[0] == dttypeTimeStamp:
                value = int(value)
                value = datetime.datetime.fromtimestamp(float(value/type_def[1]), self.utc)

            elif type_def[0] == dttypeDateTimeString:
                date = self.timezone.localize(datetime.datetime.strptime(value, type_def[1]))
                value = self.utc.normalize(date.astimezone(self.utc))

            elif type_def[0] == dttypeTime:
                try:
                    if type_def[1][0] == 12:
                        if value.strip()[-len(type_def[1][2]):].lower() == type_def[1][2].lower():
                            ttype = 'pm'
                            tvalue = value.strip()[:-len(type_def[1][2])].strip()

                        elif value.strip()[-len(type_def[1][1]):].lower() == type_def[1][1].lower():
                            ttype = 'am'
                            tvalue = value.strip()[:-len(type_def[1][1])].strip()

                        else:
                            ttype = 'am'
                            tvalue = value.strip()

                    else:
                        ttype = '24'
                        tvalue = value.strip()

                    t = re.split(type_def[2], tvalue)
                    hour = int(data_value(0, t, str, '00'))
                    minute = int(data_value(1, t, str, '00'))
                    second = int(data_value(2, t, str, '00'))
                    if ttype == 'pm':
                        hour += 12

                    value = datetime.time(hour, minute, second)

                except:
                    calc_warning('time type')

            elif type_def[0] == dttypeTimeDelta:
                try:
                    value = datetime.timedelta(seconds = int(value))

                except:
                    calc_warning('timedelta type')

            elif type_def[0] == dttypeDate:
                try:
                    day = self.current_date.day
                    month = self.current_date.month
                    year = self.current_date.year
                    d = re.split(type_def[2], value)
                    for index in range(len(d)):
                        if index > len(type_def[1])-1:
                            break

                        if type_def[1][index].lower() == 'd':
                            day = int(d[index])

                        elif type_def[1][index].lower() == 'm':
                            try:
                                month = int(d[index])

                            except ValueError:
                                if d[index].lower() in self.month_names:
                                    month = self.month_names.index(d[index].lower())

                                else:
                                    calc_warning('invalid "%s" value for date type' % (type_def[1][index], ))
                                    continue

                        elif type_def[1][index].lower() == 'y':
                            year = int(d[index])

                    value = datetime.date(year, month, day)

                except:
                    calc_warning('date type')

            elif type_def[0] == dttypeDateStamp:
                value = int(value)
                value = datetime.date.fromtimestamp(float(value/type_def[1]))

            elif type_def[0] == dttypeRelativeWeekday:
                if value.strip().lower() in self.relative_weekdays.keys():
                    value = self.relative_weekdays[value.strip().lower()]

            elif type_def[0] == dttypeString:
                value = unicode(value)

            elif type_def[0] == dttypeInteger:
                try:
                    value = int(value)

                except:
                    calc_warning('int type')
                    value = 0

            elif type_def[0] == dttypeFloat:
                try:
                    value = float(value)

                except:
                    calc_warning('float type')
                    value = 0

            elif type_def[0] == dttypeBoolean:
                if not isinstance(value, bool):
                    if isinstance(value, (int, float)):
                        value = bool(value>0)

                    elif isinstance(value, (str, unicode)):
                        value = bool(len(value) > 0 and value != '0')

                    else:
                        value = False

            elif type_def[0] == dttypeLowerAscii and isinstance(value, (str, unicode)):
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

            elif type_def[0] == dttypeStringList:
                try:
                    value = list(re.split(type_def[1], value))
                    if type_def[2]:
                        while '' in value:
                            value.remove('')

                        while None in value:
                            value.remove(None)

                except:
                    calc_warning('str-list type')

            elif type_def[0] == dttypeList:
                pass

        except:
            #~ traceback.print_exc()
            calc_warning('unknown')

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
            self.fle.write(u'%s\n' % (text, ))

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
                for ctag in ('body', 'BODY', 'html', 'HTML', 'xml', 'XML'):
                    if u'<%s>' % (ctag, ) in data and not u'</%s>' % (ctag, ) in data:
                        data = u'%s</%s>' % (data, ctag)

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

            if ',' in tag or '"' in tag or "'" in tag:
                continue

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
            if c['close'] == 0 and (c['start'] >0 or c['auto'] > 0) and not t in self.autoclose_tags:
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
            self.ddconv = DataDef_Convert(warnaction = warnaction , warngoal = warngoal, caller_id = caller_id)
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
            self.init_data_def(data_def)
            if data != None:
                self.init_data(data)

    def init_data_def(self, data_def = None, init_start_node = True):
        with self.tree_lock:
            if is_data_value("dtversion", data_def, tuple):
                self.data_def = data_def
                if data_def["dtversion"] != self.ddconv.dtversion():
                    self.warn('Your supplied data_def was converted using version %d.%d.%d.\n' % data_def["dtversion"] + \
                        'You best reconvert it with the current version.', dtdata_defWarning, 2)

            else:
                self.set_errorcode(self.ddconv.convert_data_def(data_def))
                if self.ddconv.errorcode & dtFatalError != dtDataDefOK:
                    return self.check_errorcode()

                self.data_def = self.ddconv.cdata_def

            self.set_timezone(self.data_def["tz"])
            self.empty_values = self.data_def['empty-values']
            if isinstance(self.searchtree, DATAtree):
                self.searchtree.check_data_def(self.data_def)
                if init_start_node:
                    self.set_errorcode(self.searchtree.find_start_node(), True)

            return self.check_errorcode()

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
                        self.warn('Invalid timezone "%s" suplied. Falling back to the old timezone "%s"' % \
                            (timezone, oldtz.tzname), dtdata_defWarning, 2)
                        self.set_errorcode(dtTimeZoneFailed)
                        self.timezone = oldtz

                    else:
                        self.warn('Invalid timezone "%s" suplied. Falling back to UTC' % (timezone, ), dtdata_defWarning, 2)
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
                    self.warn('Invalid or no current_date "%s" suplied. Falling back to NOW' % \
                        (cdate, ), dtdata_defWarning, 2)

                self.current_date = self.timezone.normalize(datetime.datetime.now(pytz.utc).astimezone(self.timezone)).date()
                self.current_ordinal = self.current_date.toordinal()

    def get_url(self, url_data = None, only_acceptstring = True):
        # "url", "encoding", "accept-header", "url-data", "data-format", 'default-item-count', "item-range-splitter"
        # "url-date-type" 0 = offset or formated string, 1 = timestamp, 2 = weekday
        # "url-date-format", "url-date-multiplier", "url-weekdays"
        # 'url-var', 'count', 'cnt-offset', 'offset'
        def get_url_part(u_part):
            if isinstance(u_part, (str, unicode)):
                return u_part

            # get a variable
            return self.url_functions(u_part[0], u_part[1])

        with self.tree_lock:
            self.url_data = url_data
            url = u''
            for u_part in self.data_def["url"]:
                uval = get_url_part(u_part)
                if uval == None:
                    self.warn('Invalid url_part definition: %s' % (u_part, ), dtUrlWarning, 1)
                    return None

                else:
                    url += unicode(uval)

            encoding = self.data_def["encoding"]
            if only_acceptstring:
                if "Accept" in self.data_def["url-header"].keys():
                    accept_header = self.data_def["url-header"]["Accept"]

                else:
                    accept_header = None

            else:
                accept_header = {}
                for k, u_part in self.data_def["url-header"].items():
                    uval = get_url_part(u_part)
                    if uval == None:
                        self.warn('Invalid url-header definition: %s' % (u_part, ), dtUrlWarning, 1)
                        return None

                    else:
                        accept_header[k] = uval

            url_data = {}
            for k, u_part in self.data_def["url-data"].items():
                uval = get_url_part(u_part)
                if uval == None:
                    self.warn('Invalid url-data definition: %s' % (u_part, ), dtUrlWarning, 1)
                    return None

                else:
                    url_data[k] = uval

            is_json = bool(self.data_def["dttype"] == 'json')

            return (url, encoding, accept_header, url_data, is_json)

    def url_functions(self, urlid, data = None):
        def url_warning(text, severity=2):
            self.warn('%s on function: "%s": %s\n   Using url_data: %s' % \
                (text, urlid, data, self.url_data), dtUrlWarning, severity, 3)

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

            udt = self.data_def["url-date-type"]
            udf = self.data_def["url-date-format"]
            udm = self.data_def["url-date-multiplier"]
            uwd = self.data_def["url-weekdays"]
            rwd = self.data_def["url-relative-weekdays"]
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
                cnt = data_value('count', self.url_data, int, default=self.data_def['default-item-count'])
                cnt_offset = data_value('cnt-offset', self.url_data, int, default=0)
                cstep = cnt_offset * cnt
                splitter = self.data_def["item-range-splitter"]
                return u'%s%s%s' % (cstep + 1, splitter, cstep  + cnt)

            elif urlid == 11:
                if is_data_value(0, data, str):
                    dkey = data[0]

                else:
                    dkey = 'offset'

                offset = data_value(dkey, self.url_data, int, default=0)
                if udt == 0:
                    if udf not in (None, ''):
                        return get_dtstring(self.current_ordinal + offset)

                    else:
                        return unicode(offset)

                elif udt == 1:
                    return get_timestamp(self.current_ordinal + offset)

                elif udt == 2:
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

                splitter = self.data_def["date-range-splitter"]
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
                self.warn('Sort request {"path": %s, "childkeys": %s}" failed\n' % (path, childkeys) + \
                    '   as "path" is not present in the data or is not a list!', dtDataWarning, 2)

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
                    self.warn('Failed to initialise the searchtree. Run with a valid dataset %s' \
                        % (type(data), ), dtDataWarning, 1)

            elif isinstance(data, (str, unicode)) and data.strip()[0] == "<":
                dttype = 'html'
                autoclose_tags = self.data_def["autoclose-tags"]
                # Cover for incomplete reads where the essentiel body part is retrieved
                for ctag in ('body', 'BODY', 'html', 'HTML', 'xml', 'XML'):
                    if u'<%s>' % (ctag, ) in data and not u'</%s>' % (ctag, ) in data:
                        data = u'%s</%s>' % (data, ctag)

                if self.data_def["enclose-with-html-tag"]:
                    data = u'<html>%s</html>' % (data, )

                for subset in self.data_def["text_replace"]:
                    if isinstance(subset, list) and len(subset) >= 2:
                        try:
                            data = re.sub(subset[0], subset[1], data, 0, re.DOTALL)

                        except:
                            self.set_errorcode(dtTextReplaceFailed)
                            self.warn('An error occured applying "text_replace" regex: "%s"' % (subset, ), dtDataWarning, 2)

                for ut in self.data_def["unquote_html"]:
                    if isinstance(ut, (str, unicode)):
                        try:
                            data = re.sub(ut, unquote, data, 0, re.DOTALL)

                        except:
                            self.set_errorcode(dtUnquoteFailed)
                            self.warn('An error occured applying "unquote_html" regex: "%s"' % (ut, ), dtDataWarning, 2)

                self.searchtree = HTMLtree(data, autoclose_tags, self.print_tags, self.fle, caller_id = self.caller_id, warnaction = None)

            else:
                self.warn('Failed to initialise the searchtree. Run with a valid dataset', dtDataWarning, 1)

            if dttype == 'json':
                for sitem in self.data_def['data']['sort']:
                    try:
                        sort_list(data, list(sitem[0]), list(sitem[1]))

                    except:
                        self.set_errorcode(dtSortFailed)
                        self.warn('Sort request "%s" failed!' % (sitem, ), dtDataWarning, 2)

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
                self.warn('The searchtree has not jet been initialized.\n' + \
                    'Run .init_data() first with a valid dataset', dtDataWarning, 1)
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
        def get_variable(vdef, key):
            varid = vdef[1][0]
            if  varid >= len(linkdata):
                self.warn('Requested datavalue "%s" does not exist in: %s'% (varid, linkdata), dtLinkWarning, 2)
                return

            # remove any leading or trailing spaces on a string/unicode value
            value = linkdata[varid] if (not  isinstance(linkdata[varid], (unicode, str))) else unicode(linkdata[varid]).strip()
            return process_extras(value, vdef, key)

        def process_link_function(vdef, key):
            funcid = vdef[1][0]
            default = vdef[dtlinkPos[dtlinkhasDefault]]
            # Process the datavalues given for the function
            data = []
            for fd in vdef[1][1]:
                lact = fd[0] & dtlinkGroup
                if lact == dtlinkVarID:
                    data.append(get_variable(fd, key))

                elif lact == dtlinkFuncID:
                    data.append(process_link_function(fd, key))

                elif lact == dtlinkValue:
                    data.append(fd[1])

            # And call the linkfunction
            value = self.link_functions(funcid, data, default)
            if value in self.empty_values:
                return None

            return process_extras(value, vdef, key)

        def process_extras(value, vdef, key):
            if not value in self.empty_values:
                if vdef[0] & dtlinkhasRegex and isinstance(value, (str, unicode)):
                    search_regex = vdef[dtlinkPos[dtlinkhasRegex]]
                    try:
                        dd = re.search(search_regex, value, re.DOTALL)
                        if dd.group(1) not in ('', None):
                            value = dd.group(1)

                        else:
                            self.warn('Regex "%s" in: %s returned no value on "%s"'% \
                                (search_regex, vdef, value), dtLinkWarning, 4)
                            value = None

                    except:
                        self.warn('Invalid value "%s" or invalid regex "%s" in: %s'% \
                            (value, search_regex, vdef), dtLinkWarning, 4)
                        value = None

                if vdef[0] & dtlinkhasType:
                    dtype = vdef[dtlinkPos[dtlinkhasType]]
                    try:
                        if dtype == dttypeString:
                            value = unicode(value)

                        elif dtype == dttypeLower:
                            value = unicode(value).lower()

                        elif dtype == dttypeUpper:
                            value = unicode(value).upper()

                        elif dtype == dttypeCapitalize:
                            value = unicode(value).capitalize()

                        elif dtype == dttypeInteger:
                            value = int(value)

                        elif dtype == dttypeFloat:
                            value = float(value)

                        elif dtype == dttypeBoolean:
                            value = bool(value)

                    except:
                        vtype = {
                                dttypeString: "String",
                                dttypeLower: "LowerCase",
                                dttypeUpper: "UpperCase",
                                dttypeCapitalize: "Capitalize",
                                dttypeInteger: "Integer",
                                dttypeFloat: "Float",
                                dttypeBoolean: "Boolean"}
                        self.warn('Error on applying type "%s" on "%s"'% (vtype[dtype], value), dtLinkWarning, 4)
                        value = None

                if vdef[0] & dtlinkhasCalc:
                    for cv in vdef[dtlinkPos[dtlinkhasCalc]]:
                        if cv[0] == dtcalcMultiply:
                            try:
                                if not isinstance(value, (int, float)):
                                    value = float(value)
                                value = value * cv[1]

                            except:
                                self.warn('Error on applying multiplier "%s" on "%s"'% \
                                    (cv[1], value), dtLinkWarning, 4)

                        if cv[0] == dtcalcDivide:
                            try:
                                if not isinstance(value, (int, float)):
                                    value = float(value)
                                value = value / cv[1]

                            except:
                                self.warn('Error on applying divider "%s" on "%s"'% \
                                    (cv[1], value), dtLinkWarning, 4)

                if vdef[0] & dtlinkhasMax:
                    if isinstance(value, (str, unicode, list, dict)) and len(value) > vdef[dtlinkPos[dtlinkhasMax]]:
                        self.warn('Requested datavalue "%s" is longer then %s'% \
                            (key, vdef[dtlinkPos[dtlinkhasMax]]), dtLinkWarning, 4)
                        value = None

                    if isinstance(value, (int, float)) and value > vdef[dtlinkPos[dtlinkhasMax]]:
                        self.warn('Requested datavalue "%s" is bigger then %s'% \
                            (key, vdef[dtlinkPos[dtlinkhasMax]]), dtLinkWarning, 4)
                        value = None

                if vdef[0] & dtlinkhasMin:
                    if isinstance(value, (str, unicode, list, dict)) and len(value) < vdef[dtlinkPos[dtlinkhasMin]]:
                        self.warn('Requested datavalue "%s" is shorter then %s'% \
                            (key, vdef[dtlinkPos[dtlinkhasMin]]), dtLinkWarning, 4)
                        value = None

                    if isinstance(value, (int, float)) and value < vdef[dtlinkPos[dtlinkhasMin]]:
                        self.warn('Requested datavalue "%s" is smaller then %s'% \
                            (key, vdef[dtlinkPos[dtlinkhasMin]]), dtLinkWarning, 4)
                        value = None

            if value in self.empty_values:
                return vdef[dtlinkPos[dtlinkhasDefault]]

            return value

        values = {}
        if isinstance(linkdata, list):
            for k, v in self.data_def["values"].items():
                lact = v[0] & dtlinkGroup
                cval = None
                if lact == dtlinkVarID:
                    cval = get_variable(v, k)

                elif lact == dtlinkFuncID:
                    cval = process_link_function(v, k)

                elif lact == dtlinkValue:
                    cval = v[1]

                if not cval in self.empty_values:
                    values[k] = cval

                elif not v[dtlinkPos[dtlinkhasDefault]] in self.empty_values:
                    values[k] = v[dtlinkPos[dtlinkhasDefault]]
        else:
            self.warn('No valid data "%s" to link with' % (linkdata, ), dtLinkWarning, 2)

        return values

    def link_functions(self, fid, data = None, default = None):
        def link_warning(text, severity=4):
            self.warn('%s on function: "%s"\n   Using link_data: %s' % (text, fid, data), dtLinkWarning, severity, 3)

        try:
            if fid > 99:
                retval = self.add_on_link_functions(fid, data, default)
                if is_data_value("fid", retval, int) and (0 <= retval["fid"] < 13):
                    # it is redirected to a base function
                    fid = retval["fid"]

                else:
                    if fid < 200 and retval in self.empty_values:
                        self.warn('No result on custom link function: "%s"\n' % (fid, ) + \
                            '   Using link_data: %s' % (data, ), dtLinkWarning, 4)

                    return retval

            # strip data[1] from the end of data[0] if present and make sure it's unicode
            if fid == 0:
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

                    elif not data[0] and is_data_value(2, data):
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
                    if isinstance(data[0][index], (str, unicode)):
                        data[0][index] = data[0][index].lower().strip()

                if not isinstance(data[1], (list,tuple)):
                    data[1] = [data[1]]

                sd = data[2].lower().strip() if isinstance(data[2], (str, unicode)) else data[2]

                if sd in data[0]:
                    index = data[0].index(sd)
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
                        if isinstance(item, (str,unicode)):
                            item = item.lower()

                        for sitem in data[0]:
                            if isinstance(sitem, dict):
                                if item in sitem.keys():
                                    if isinstance(sitem[item], (list, tuple)) and len(sitem[item]) == 0:
                                        continue

                                    if isinstance(sitem[item], (list, tuple)) and len(sitem[item]) == 1:
                                        return sitem[item][0]

                                    return sitem[item]

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
            self.warn('Unknown link Error on function: "%s"\n' % (fid, ) + \
                    '   Using link_data: %s\n%s' % (data, traceback.print_exc()), dtLinkWarning, 2)
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
