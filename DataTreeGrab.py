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
import re, sys, traceback, types, pickle
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
dt_patchdate = u'20170710'
dt_alfa = False
dt_beta = False
_warnings = None

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
        for dtp in (str, unicode, 'string'):
            while dtp in dtype:
                dtype.remove(dtp)
        dtype.extend([str, unicode])

    if list in dtype or tuple in dtype or 'list' in dtype:
        for dtp in (list, tuple, 'list'):
            while dtp in dtype:
                dtype.remove(dtp)
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

class dtErrorConstants():
    # DataTreeShell errorcodes
    dtQuiting = -1
    dtDataOK = 0
    dtDataDefOK = 0
    dtURLerror = 1
    dtTimeoutError = 2
    dtHTTPerror = 3
    dtJSONerror = 4
    dtEmpty = 5
    dtIncompleteRead = 6
    dtStartNodeInvalid = 7
    dtDataDefInvalid = 8
    dtDataInvalid = 10
    dtNoData = 14
    dtUnknownError = 15
    dtFatalError = 15

    dtSortFailed = 16
    dtUnquoteFailed = 32
    dtTextReplaceFailed = 64
    dtTimeZoneFailed = 128
    dtCurrentDateFailed = 256
    dtInvalidValueLink = 512
    dtInvalidNodeLink = 1024
    dtInvalidPathDef = 2048
    dtInvalidLinkDef = 4096
    dtErrorTexts = {
        dtQuiting: 'The execution was aborted',
        dtDataOK: 'Data OK',
        dtURLerror: 'There was an error in the URL',
        dtTimeoutError: 'Fetching the page took to long',
        dtHTTPerror: 'A HTTP error occured',
        dtJSONerror: 'A JSON error occured',
        dtEmpty: 'Empty Page',
        dtIncompleteRead: 'Incomplete Read',
        dtStartNodeInvalid: 'Invalid startnode!',
        dtDataDefInvalid: 'Invalid data_def',
        dtDataInvalid: 'Invalid dataset!',
        9: 'Unused Error 9',
        dtNoData: 'No Data',
        11: 'User Error 11',
        12: 'User Error 12',
        13: 'User Error 13',
        dtUnknownError: 'An unknown error occured',
        dtSortFailed: 'Data sorting failed',
        dtUnquoteFailed: 'The Unquote filter failed',
        dtTextReplaceFailed: 'The Textreplace filter failed',
        dtTimeZoneFailed: 'Timezone initialization failed',
        dtCurrentDateFailed: 'Setting the current date failed',
        dtInvalidValueLink: 'A not jet stored value link was requested',
        dtInvalidNodeLink: 'A not jet stored node link was requested',
        dtInvalidPathDef: 'Errors in a Path_def were encountered',
        dtInvalidLinkDef: 'Errors in a Link_def were encountered'}

    def errortext(self, ecode):
        if ecode in self.dtErrorTexts.keys():
            return self.dtErrorTexts[ecode]

        return 'Unknown Error'

# end dtErrorConstants()
dte = dtErrorConstants()

class DataTreeConstants():
    # The allowances for a path_def
    pathWithValue = 1
    pathWithNames = 2
    pathMulti = 4
    pathInit = 0
    pathKey = 5
    pathValue = 7
    # The node_def type
    isGroup = 7
    isNone = 0
    emptyNodeDef = ((isNone, ), )
    isNodeSel = 1
    isNodeLink = 2
    storeName = 3
    isValue = 4
    hasCalc = 8
    hasDefault = 16
    hasType = 32
    isMemberOff = 64
    storeLinkValue = 128
    storePathValue =256
    getOnlyOne = 512
    getLast = 1024
    node_name = {
            isNone: "Empty node_def",
            isNodeSel: "Node Selection node_def",
            isNodeLink: "Node Storing node_def",
            storeName: "Name Selection node_def",
            isValue: "Value Selection node_def"}

    # Node selection and the tuple position for details (node_def type 1 and 2)
    selMain = 7
    selNone = 0
    selPathAll = 1
    selPathParent = 2
    selPathRoot = 3
    selPathLink = 4
    selKey = 5
    selTag = 6
    selKeys = 7
    selTags = 7
    selIndex = 8
    selText = 16
    selTail = 32
    selChildKeys = 64
    selAttrs = 64
    selNotChildKeys = 128
    selNotAttrs = 128
    attr = 0
    attrNot = 1
    # What data to extract
    getGroup = 15
    getNone = 0
    getIndex = 1
    getKey = 2
    getTag = 2
    getDefault = 3
    getValue = 3
    getText = 3
    getTail = 4
    getInclusiveText = 5
    getPresence = 6
    getLitteral = 7
    getAttr = 8
    # Is it a value or a linkvalue and what manipulations to do to a retrieved linkvalue
    valValue = 0
    valLink = 1
    valLinkPlus = 2
    valLinkMin = 4
    valLinkNext = 8
    valLinkPrevious = 16
    # What data manipulations
    calcNone = 0
    calcLettering = 1
    calcLower = 1
    calcUpper = 2
    calcCapitalize = 3
    calcAsciiReplace = 2
    calcLstrip = 3
    calcRstrip = 4
    calcSub = 5
    calcSplit = 6
    calcMultiply = 16
    calcDivide = 17
    calcReplace = 7
    calcDefault = 32
    calc_name = {
            calcNone: "No calculation",
            calcLettering: "CaseSetting",
            calcAsciiReplace: "AsciiReplace",
            calcLstrip: "Left Striping",
            calcRstrip: "Right Striping",
            calcSub: "Substituting",
            calcSplit: "Splitting",
            calcMultiply: "Multipling with",
            calcDivide: "Dividing by",
            calcReplace: "Replacing",
            calcDefault: "Default"}
    case_name = {
            calcLower: "Lower Case",
            calcUpper: "Upper Case",
            calcCapitalize: "Capitalised"}
    # What type to select
    typeNone = 0
    typeTimeStamp = 1
    typeDateTimeString = 2
    typeTime = 3
    typeTimeDelta = 4
    typeDate = 5
    typeDateStamp = 6
    typeRelativeWeekday = 7
    typeString = 8
    typeInteger = 9
    typeFloat = 10
    typeBoolean = 11
    typeLowerAscii = 12
    typeStringList = 13
    typeList = 14
    typeLower = 15
    typeUpper = 16
    typeCapitalize = 17
    type_name = {
            typeNone: "No Type",
            typeTimeStamp: "TimeStamp",
            typeDateTimeString: "DateTimeString",
            typeTime: "Time",
            typeTimeDelta: "TimeDelta",
            typeDate: "Date",
            typeDateStamp: "DateStamp",
            typeRelativeWeekday: "RelativeWeekday",
            typeString: "String",
            typeInteger: "Integer",
            typeFloat: "Float",
            typeBoolean: "Boolean",
            typeLowerAscii: "LowerAscii",
            typeStringList: "StringList",
            typeList: "List",
            typeLower: "Lower",
            typeUpper: "Upper",
            typeCapitalize: "Capitalize"}
    # About the link_defs
    linkNone = 0
    linkGroup = 3
    linkVarID =1
    linkFuncID = 2
    linkValue = 3
    linkhasDefault = 4
    linkhasRegex = 8
    linkhasType = 16
    linkhasCalc = 32
    linkhasMax = 64
    linkhasMin = 128
    selPosMax = 7
    selPos = {
        selPathAll: 2,
        selPathParent: 2,
        selPathRoot: 2,
        selPathLink: 2,
        selKey: 2,
        selTag: 2,
        selKeys: 2,
        selIndex: 3,
        selChildKeys: 4,
        selNotChildKeys: 5,
        selText: 6,
        selTail: 7}
    getPosMax = 6
    getPos = {
        hasCalc: 2,
        hasType: 3,
        isMemberOff: 4,
        storeLinkValue: 5,
        hasDefault: 6}
    linkPosMax = 7
    linkPos = {
        linkhasDefault: 2,
        linkhasRegex: 3,
        linkhasType: 4,
        linkhasCalc: 5,
        linkhasMax: 6,
        linkhasMin: 7}

    def const_text(self, ttype, tvalue):
        if ttype == 'node_name' and tvalue in self.node_name.keys():
            return self.node_name[tvalue]

        elif ttype == 'type_name' and tvalue in self.type_name.keys():
            return self.type_name[tvalue]

        elif ttype == 'calc_name' and tvalue in self.calc_name.keys():
            return self.calc_name[tvalue]

        elif ttype == 'case_name' and tvalue in self.case_name.keys():
            return self.case_name[tvalue]

        return ''
# end DataTreeConstants()

class DataDef_Convert():
    def __init__(self, data_def = None, warnaction = "default", warngoal = sys.stderr, caller_id = 0):
        self.tree_lock = RLock()
        with self.tree_lock:
            self.dtc = DataTreeConstants()
            self.known_urlid = (0, 4, 11, 14)
            self.known_linkid = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
            self.errorcode = dte.dtDataDefOK
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

    def convert_path_def(self, path_def, ptype = "", path_type = None, link_list = None, init_errors = True):
        # check whether it is a link or a value
        # return a list (typeint, value/link, plus/min)
        def convert_value_link(lvalue, is_index = False):
            if is_data_value("link",lvalue, int):
                if not lvalue["link"] in self.link_list["values"]:
                    self.errorcode |= (dte.dtInvalidValueLink + dte.dtDataDefInvalid)
                    self.warn('LinkID: %s is not jet stored' % ( lvalue["link"], ), dtConversionWarning, 1, 3)
                    return (self.dtc.valValue,None,0)

                val = [self.dtc.valLink, lvalue["link"], 0]

            else:
                return (self.dtc.valValue,lvalue,0)

            if data_value(["calc", 0],lvalue) == "plus":
                val[0] += self.dtc.valLinkPlus
                val[2] = data_value(["calc", 1],lvalue, int, 0)

            elif data_value(["calc", 0],lvalue) == "min":
                val[0] += self.dtc.valLinkMin
                val[2] = data_value(["calc", 1],lvalue, int, 0)

            elif is_index and is_data_value("previous",lvalue):
                val[0] += self.dtc.valLinkPrevious

            elif is_index and is_data_value("next",lvalue):
                val[0] += self.dtc.valLinkNext

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
                    dta = self.dtc.attrNot
                    vl = convert_value_list(v["not"])

                else:
                    vl = convert_value_list(v)
                    dta = self.dtc.attr

                llist.append((k, dta, vl))

            return tuple(llist)

        # process Data extraction/manipulation
        # return (node_type,linkid,data) or (node_type,0,data)
        # with data = (sel_type,sel_data),((calc_int,calc_data)),(type_int, type_data), memberoff
        def convert_data_extraction(node_def, node_type = self.dtc.isValue):
            nlink = 0
            if ((node_type & self.dtc.storeLinkValue) or (node_type & self.dtc.storePathValue)) \
                and (node_type & self.dtc.isGroup) != self.dtc.isValue:
                    node_type -= (node_type & self.dtc.isGroup)
                    node_type += self.dtc.isValue

            if (node_type & self.dtc.storeLinkValue):
                nlink = node_def["link"]

            sel_node = [self.dtc.getDefault, None]
            calc_list = []
            type_def = []
            memberoff = ""
            ndefault = None
            if isinstance(node_def,dict):
                if "value" in node_def.keys():
                    sel_node = [self.dtc.getLitteral, node_def["value"]]

                elif is_data_value("attr", node_def, str) and self.ddtype in ("html", ""):
                    self.ddtype="html"
                    #~ sel_node = [self.dtc.getAttr, convert_value_link(node_def["attr"])]
                    sel_node = [self.dtc.getAttr, node_def["attr"].lower()]

                elif is_data_value("select", node_def, str):
                    if node_def["select"] == "index":
                        sel_node[0] = self.dtc.getIndex

                    elif node_def["select"] == "key" and self.ddtype in ("json", ""):
                        self.ddtype="json"
                        sel_node[0] = self.dtc.getKey

                    elif node_def["select"] == "tag" and self.ddtype in ("html", ""):
                        self.ddtype="html"
                        sel_node[0] = self.dtc.getTag

                    elif node_def["select"] == "text" and self.ddtype in ("html", ""):
                        self.ddtype="html"
                        sel_node[0] = self.dtc.getText

                    elif node_def["select"] == "tail" and self.ddtype in ("html", ""):
                        self.ddtype="html"
                        sel_node[0] = self.dtc.getTail

                    elif node_def["select"] == "value" and self.ddtype in ("json", ""):
                        self.ddtype="json"
                        sel_node[0] = self.dtc.getValue

                    elif node_def["select"] == "presence":
                        sel_node[0] = self.dtc.getPresence

                    elif node_def["select"] == "inclusive text" and self.ddtype in ("html", ""):
                        self.ddtype="html"
                        depth = data_value("depth", node_def, int, 1)
                        if is_data_value("include", node_def, list):
                            specs = (depth, 1, node_def["include"])

                        elif is_data_value("exclude", node_def, list):
                            specs = (depth, -1, node_def["exclude"])

                        else:
                            specs = (depth, 0, [])

                        sel_node = [self.dtc.getInclusiveText, specs]

                # Process any calc statements
                calc_type = self.dtc.calcNone
                if "lower" in node_def.keys():
                    calc_list.append((self.dtc.calcLettering, self.dtc.calcLower))

                elif "upper" in node_def.keys():
                    calc_list.append((self.dtc.calcLettering, self.dtc.calcUpper))

                elif "capitalize" in node_def.keys():
                    calc_list.append((self.dtc.calcLettering, self.dtc.calcCapitalize))

                if is_data_value('ascii-replace', node_def, list) and len(node_def['ascii-replace']) > 0:
                    calc_list.append((self.dtc.calcAsciiReplace, tuple(node_def["ascii-replace"])))

                if is_data_value('lstrip', node_def, str):
                    calc_list.append((self.dtc.calcLstrip, node_def["lstrip"]))

                if is_data_value('rstrip', node_def, str):
                    calc_list.append((self.dtc.calcRstrip, node_def["rstrip"]))

                if is_data_value('sub', node_def, list) and len(node_def['sub']) > 1:
                    sl = []
                    for i in range(int(len(node_def['sub'])/2)):
                        sl.append((node_def['sub'][i*2], node_def['sub'][i*2+1]))

                    if len(sl) > 0:
                        calc_list.append((self.dtc.calcSub, tuple(sl)))

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
                        calc_list.append((self.dtc.calcSplit, tuple(sl)))

                if is_data_value('multiplier', node_def, int) and \
                    not data_value('type', node_def, unicode) in ('timestamp', 'datestamp'):
                        calc_list.append((self.dtc.calcMultiply, node_def["multiplier"]))

                if is_data_value('divider', node_def, int) and node_def['divider'] != 0:
                    calc_list.append((self.dtc.calcDivide, node_def["divider"]))

                if is_data_value('replace', node_def, dict):
                    rl1 = []
                    rl2 = []
                    for k, v in node_def["replace"].items():
                        if isinstance(k, (str, unicode)):
                            rl1.append(k.lower())
                            rl2.append(v)

                    if len(rl1) > 0:
                        calc_list.append((self.dtc.calcReplace, tuple(rl1), tuple(rl2)))

                if len(calc_list) > 0:
                    node_type += self.dtc.hasCalc

                if "default" in node_def.keys():
                    node_type += self.dtc.hasDefault
                    ndefault = node_def["default"]

                # Process any type statement
                if is_data_value('type', node_def, unicode):
                    if node_def['type'] == 'timestamp':
                        if is_data_value('multiplier', node_def, int) and node_def['multiplier'] != 0:
                            type_def = (self.dtc.typeTimeStamp, node_def['multiplier'])

                        else:
                            type_def = (self.dtc.typeTimeStamp, 1)

                    elif node_def['type'] == 'datetimestring':
                        type_def = (self.dtc.typeDateTimeString, data_value('datetimestring', \
                            node_def, str, self.cdata_def["datetimestring"]))

                    elif node_def['type'] == 'time':
                        tt = self.cdata_def["time-type"]
                        if is_data_value('time-type', node_def, list) \
                          and is_data_value(['time-type',0], node_def, int) \
                          and data_value(['time-type',0], node_def, int) in (12, 24):
                            tt = [data_value(['time-type', 0], node_def, list),
                                    data_value(['time-type', 1], node_def, str, 'am'),
                                    data_value(['time-type', 2], node_def, str, 'pm')]

                        type_def = (self.dtc.typeTime, tt,data_value('time-splitter', \
                            node_def, str, self.cdata_def["time-splitter"]))

                    elif node_def['type'] == 'timedelta':
                            type_def = (self.dtc.typeTimeDelta, )

                    elif node_def['type'] == 'date':
                        type_def = (self.dtc.typeDate,
                            data_value('date-sequence', node_def, list, self.cdata_def["date-sequence"]),
                            data_value('date-splitter', node_def, str, self.cdata_def["date-splitter"]))

                    elif node_def['type'] == 'datestamp':
                        if is_data_value('multiplier', node_def, int) and node_def['multiplier'] != 0:
                            type_def = (self.dtc.typeDateStamp, node_def['multiplier'])

                        else:
                            type_def = (self.dtc.typeDateStamp, 1)

                    elif node_def['type'] == 'relative-weekday':
                        type_def = (self.dtc.typeRelativeWeekday, )

                    elif node_def['type'] == 'string':
                        type_def = (self.dtc.typeString, )

                    elif node_def['type'] == 'lower':
                        type_def = (self.dtc.typeLower, )

                    elif node_def['type'] == 'upper':
                        type_def = (self.dtc.typeUpper, )

                    elif node_def['type'] == 'capitalize':
                        type_def = (self.dtc.typeCapitalize, )

                    elif node_def['type'] == 'int':
                        type_def = (self.dtc.typeInteger, )

                    elif node_def['type'] == 'float':
                        type_def = (self.dtc.typeFloat, )

                    elif node_def['type'] == 'boolean':
                        type_def = (self.dtc.typeBoolean, )

                    elif node_def['type'] == 'lower-ascii':
                        type_def = (self.dtc.typeLowerAscii, )

                    elif node_def['type'] == 'str-list':
                        type_def = (self.dtc.typeStringList,
                            data_value('str-list-splitter', node_def, str, self.cdata_def["str-list-splitter"]),
                            data_value("omit-empty-list-items", node_def, bool, False))

                    elif node_def['type'] == 'list':
                        type_def = (self.dtc.typeList, )

                    if len(type_def) > 0:
                        node_type += self.dtc.hasType

                if not (path_type & self.dtc.pathMulti) or "first" in node_def.keys() or "last" in node_def.keys():
                    node_type += self.dtc.getOnlyOne
                    if "last" in node_def.keys():
                        node_type += self.dtc.getLast

                if is_data_value('member-off', node_def, unicode):
                    memberoff = node_def["member-off"]
                    node_type += self.dtc.isMemberOff

            return (node_type, tuple(sel_node), tuple(calc_list), tuple(type_def), memberoff, nlink, ndefault)

        with self.tree_lock:
            if path_type == None:
                path_type = self.dtc.pathValue

            if init_errors:
                self.errorcode = dte.dtDataDefOK

            self.ddtype = ptype
            self.link_list = {"values": [],"nodes": []} if link_list == None else link_list
            if not isinstance(self.link_list, dict):
                self.link_list = {"values": [],"nodes": []}

            if not is_data_value("values", self.link_list, list):
                self.link_list["values"] = []

            if not is_data_value("nodes", self.link_list, list):
                self.link_list["nodes"] = []

            if not isinstance(path_def, (list, tuple)):
                self.errorcode |= dte.dtInvalidPathDef
                self.warn('An invalid path_def "%s" was encountered. It must be a list.' % \
                    ( path_def, ), dtConversionWarning, 1)
                return tuple()

            pd = []
            for nd in path_def:
                if isinstance(nd, dict):
                    pd.append(nd)

            dpath=[]
            # Parse through the node_defs
            for n in range(len(pd)):
                inode = pd[n]
                # Add any name definition as independent node
                if (path_type & self.dtc.pathWithNames) and "name" in inode.keys():
                    dpath.append(convert_data_extraction(inode["name"], self.dtc.storeName))

                # Look for node selection statements
                sel_node = [self.dtc.isNodeSel, self.dtc.selNone]
                if not (path_type & self.dtc.pathMulti) or "first" in inode.keys() or "last" in inode.keys():
                    sel_node[0] += self.dtc.getOnlyOne
                    if "last" in inode.keys():
                        sel_node[0] += self.dtc.getLast

                for i in range(1, self.dtc.selPosMax):
                    sel_node.append(None)

                # Check for a main statement
                if "path" in inode.keys():
                    for ttext, dtsel in (
                            ("all", self.dtc.selPathAll),
                            ("parent", self.dtc.selPathParent),
                            ("root", self.dtc.selPathRoot)):
                        if inode["path"] == ttext:
                            sel_node[1] = dtsel
                            break

                    else:
                        if not inode["path"] in self.link_list["nodes"]:
                            self.errorcode |= (dte.dtInvalidNodeLink + dte.dtDataDefInvalid)
                            self.warn('NodeID: %s is not jet stored' % ( inode["path"], ), dtConversionWarning, 1)
                            continue

                        sel_node[1] = self.dtc.selPathLink
                        sel_node[self.dtc.selPos[self.dtc.selPathLink]] = inode["path"]

                else:
                    for ttext, dtsel, tree_type, singleval in (
                            ("key", self.dtc.selKey, "json", True),
                            ("tag", self.dtc.selTag, "html", True),
                            ("keys", self.dtc.selKeys, "json", False),
                            ("tags", self.dtc.selTags, "html", False)):
                        if ttext in inode.keys() and self.ddtype in (tree_type, ""):
                            self.ddtype = tree_type
                            sel_node[1] = dtsel
                            if singleval:
                                sel_node[self.dtc.selPos[dtsel]] = convert_value_link(inode[ttext])

                            else:
                                sel_node[self.dtc.selPos[dtsel]] = convert_value_list(inode[ttext])

                            break

                # Check when allowed for an Index statement
                if sel_node[1] in (self.dtc.selNone, self.dtc.selTag, self.dtc.selTags, self.dtc.selKeys) and "index" in inode.keys():
                    sel_node[1] += self.dtc.selIndex
                    sel_node[self.dtc.selPos[self.dtc.selIndex]] = convert_value_list(inode["index"], True)

                # Check when allowed for secundary statements
                if sel_node[1] not in (self.dtc.selNone, self.dtc.selPathParent, self.dtc.selPathRoot, self.dtc.selPathLink):
                    for ttext, dtsel in (
                            ("text", self.dtc.selText),
                            ("tail", self.dtc.selTail)):
                        if ttext in inode.keys() and self.ddtype in ("html", ""):
                            self.ddtype = "html"
                            sel_node[1] += dtsel
                            sel_node[self.dtc.selPos[dtsel]] = convert_value_list(inode[ttext])

                    for ttext, dtsel, tree_type in (
                            ("childkeys", self.dtc.selChildKeys, "json"),
                            ("attrs", self.dtc.selAttrs, "html"),
                            ("notchildkeys", self.dtc.selNotChildKeys, "json"),
                            ("notattrs", self.dtc.selNotAttrs, "html")):
                        if ttext in inode.keys() and self.ddtype in (tree_type, ""):
                            if isinstance(inode[ttext], dict):
                                sel_node[self.dtc.selPos[dtsel]] = (convert_attr_dict(inode[ttext]), )

                            elif isinstance(inode[ttext], (list, tuple)):
                                dtt = []
                                for ldict in inode[ttext]:
                                    if isinstance(ldict, dict):
                                        dtt.append(convert_attr_dict(ldict))

                                sel_node[self.dtc.selPos[dtsel]] = tuple(dtt)

                            else:
                                self.warn('An invalid "%s" keyword was encountered. Ignoring it.' % \
                                    ( ttext, ), dtConversionWarning, 2)

                            if sel_node[self.dtc.selPos[dtsel]] != None:
                                self.ddtype = tree_type
                                sel_node[1] += dtsel

                # Add any found node selection statements as independent node
                if sel_node[1] > 0:
                    while sel_node[-1] == None:
                        sel_node.pop(-1)

                    dpath.append(tuple(sel_node))

                # Add any node link statement as independent node
                if "node" in inode.keys():
                    self.link_list["nodes"].append(inode["node"])
                    dpath.append((self.dtc.isNodeLink, inode["node"]))

                # Add any link statement as independent node
                if "link" in inode.keys():
                    self.link_list["values"].append(inode["link"])
                    if (path_type & self.dtc.pathWithValue) and n ==len(pd) -1:
                        dpath.append(convert_data_extraction(inode, self.dtc.storeLinkValue + self.dtc.storePathValue))

                    else:
                        dpath.append(convert_data_extraction(inode, self.dtc.storeLinkValue))

                elif (path_type & self.dtc.pathWithValue) and n ==len(pd) -1:
                    dpath.append(convert_data_extraction(inode, self.dtc.storePathValue))

            return tuple(dpath)

    def convert_link_def(self, link_def, key, maxid, init_errors = True):
        def convert_funcid(ldict, key, maxid):
            if ldict['funcid'] < 100 and not ldict['funcid'] in self.known_linkid:
                self.warn('Requested link function ID "%s" in the "%s" link statement is unknown'% \
                    (ldict["funcid"], key), dtConversionWarning, 2, 3)

                self.errorcode |= dte.dtInvalidLinkDef
                return (self.dtc.linkNone, None, None)

            funcdata = []
            for fd in data_value("data", ldict, list):
                if is_data_value("varid", fd, int):
                    funcdata.append(convert_varid(fd, key, maxid))

                elif is_data_value("funcid", fd, int):
                    funcdata.append(convert_funcid(fd, key, maxid))

                else:
                    funcdata.append((self.dtc.linkValue, fd))

            return check_extras([self.dtc.linkFuncID, (ldict["funcid"], funcdata)], ldict, key)

        def convert_varid(ldict, key, maxid):
            if 0 <= ldict["varid"] <= maxid:
                return check_extras([self.dtc.linkVarID, (ldict["varid"], )], ldict, key)

            elif ldict["varid"] > maxid:
                self.warn('Requested datavalue ID "%s" in the "%s" link statement\n'% (ldict["varid"], key) + \
                    ' is higher then the number of value_defs suplied', dtConversionWarning, 2, 3)

            elif ldict["varid"] <0 :
                self.warn('Requested datavalue ID "%s" in the "%s" link statement is Negative!'% \
                    (ldict["varid"], key), dtConversionWarning, 2, 3)

            self.errorcode |= dte.dtInvalidLinkDef
            return (self.dtc.linkNone, None, None)

        def check_extras(link_node, ldict, key):
            for i in range(1, self.dtc.linkPosMax):
                link_node.append(None)

            if is_data_value('default', ldict):
                link_node[0] += self.dtc.linkhasDefault
                link_node[self.dtc.linkPos[self.dtc.linkhasDefault]] = ldict['default']

            if is_data_value('regex', ldict, str):
                link_node[0] += self.dtc.linkhasRegex
                link_node[self.dtc.linkPos[self.dtc.linkhasRegex]] = ldict['regex']

            if is_data_value('type', ldict, str):
                if ldict['type'] == "string":
                    link_node[self.dtc.linkPos[self.dtc.linkhasType]] = self.dtc.typeString

                elif ldict['type'] == "lower":
                    link_node[self.dtc.linkPos[self.dtc.linkhasType]] = self.dtc.typeLower

                elif ldict['type'] == "upper":
                    link_node[self.dtc.linkPos[self.dtc.linkhasType]] = self.dtc.typeUpper

                elif ldict['type'] == "capitalize":
                    link_node[self.dtc.linkPos[self.dtc.linkhasType]] = self.dtc.typeCapitalize

                elif ldict['type'] == "int":
                    link_node[self.dtc.linkPos[self.dtc.linkhasType]] = self.dtc.typeInteger

                elif ldict['type'] == "float":
                    link_node[self.dtc.linkPos[self.dtc.linkhasType]] = self.dtc.typeFloat

                elif ldict['type'] == "bool":
                    link_node[self.dtc.linkPos[self.dtc.linkhasType]] = self.dtc.typeBoolean

                else:
                    self.warn('Invalid type "%s" requested for the "%s" link statement'% \
                        (ldict['type'], key),dtConversionWarning , 2, 3)

                if link_node[self.dtc.linkPos[self.dtc.linkhasType]] != None:
                    link_node[0] += self.dtc.linkhasType

            if is_data_value('calc', ldict, dict):
                calc_type = self.dtc.calcNone
                calc_list = []
                if is_data_value(['calc', 'multiplier'], ldict, float) and ldict['calc']['multiplier'] != 0:
                    calc_list.append((self.dtc.calcMultiply, ldict['calc']['multiplier']))

                if is_data_value(['calc', 'divider'], ldict, float) and ldict['calc']['divider'] != 0:
                    calc_list.append((self.dtc.calcDivide, ldict['calc']['divider']))

                if len(calc_list)> 0:
                    link_node[0] += self.dtc.linkhasCalc
                    link_node[self.dtc.linkPos[self.dtc.linkhasCalc]] = tuple(calc_list)

            if is_data_value('max length', ldict, int) and ldict['max length'] > 0:
                link_node[0] += self.dtc.linkhasMax
                link_node[self.dtc.linkPos[self.dtc.linkhasMax]] = ldict['max length']

            if is_data_value('min length', ldict, int) and ldict['min length'] > 0:
                link_node[0] += self.dtc.linkhasMin
                link_node[self.dtc.linkPos[self.dtc.linkhasMin]] = ldict['min length']

            while len(link_node) > 3 and link_node[-1] == None:
                link_node.pop(-1)

            return tuple(link_node)

        with self.tree_lock:
            if link_def == None:
                link_node = (self.dtc.linkNone, None, None)

            elif is_data_value("varid", link_def, int):
                link_node = convert_varid(link_def, key, maxid)

            elif is_data_value("funcid", link_def, int):
                link_node = convert_funcid(link_def, key, maxid)

            elif is_data_value("value", link_def):
                link_node = (self.dtc.linkValue, (link_def["value"], ))

            else:
                self.warn('No "varid", "funcid" or "value" keyword supplied\n' + \
                    'in the link_def for the %s keyword!'% \
                    (key, ), dtConversionWarning, 2, 2)
                self.errorcode |= dte.dtInvalidLinkDef
                link_node = (self.dtc.linkNone, None, None)

            return link_node

    def convert_data_def(self, data_def = None, ptype = "", include_url = True, include_links = True, file_name = None):
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
                self.ddtype, self.dtc.pathInit, self.link_list, False)
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
                        self.ddtype, self.dtc.pathKey, self.link_list, False)
                vv = []
                vlist = data_value("values", sel_dict, list)
                value_count = len(vlist) if len(vlist) > value_count else value_count
                for sel_val in vlist:
                    conv_val = self.convert_path_def(sel_val, self.ddtype, self.dtc.pathValue, self.link_list, False)
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

            if file_name != None:
                self.store_cdata_def(file_name)

            return self.errorcode

    def write_cdata_def(self, output = sys.stdout, data = None):
        with self.tree_lock:
            if data == None:
                data = self.cdata_def

            if output in (sys.stdout, sys.stderr):
                output.write(data.encode('utf-8', 'replace'))

            else:
                output.write(u'%s\n' % data)

    def store_cdata_def(self, filename, data = None):
        with self.tree_lock:
            if data == None:
                data = self.cdata_def

            try:
                pickle.dump(data, open(filename, 'w'), 2)

            except:
                self.warn('Failed to store the converted file as: "%s"' % ( filename, ), dtConversionWarning, 1)

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
            self.dtc = DataTreeConstants()
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
        def match_node(node, sel_node=self.dtc.selNone):
            # check through the HTML/JSON specific functions
            nfound = node.match_node(node_def = d_def[0], link_values=links["values"], sel_node=sel_node)
            if nfound  and sel_node in (self.dtc.selNone, self.dtc.selTag, self.dtc.selTags, self.dtc.selKeys) \
                and (d_def[0][1] & self.dtc.selIndex):
                nfound = node.check_index(d_def[0][self.dtc.selPos[self.dtc.selIndex]], links["values"])

            if nfound:
                if self.dtree.show_result:
                    self.dtree.print_text(u'    found node %s;\n%s' % \
                        (node.print_node(), node.print_node_def(d_def[0])))

            return nfound

        while True:
            if len(d_def) == 0:
                d_def = self.dtc.emptyNodeDef
                childs = [(self, None)]
                break

            ndef_type = (d_def[0][0] & self.dtc.isGroup)
            if ndef_type == self.dtc.isNodeSel:
                sel_node = (d_def[0][1] & self.dtc.selMain)
                if sel_node == self.dtc.selPathLink:
                    if is_data_value(["nodes", d_def[0][2]], links, DATAnode) and match_node(links["nodes"][d_def[0][2]], sel_node):
                        childs = links["nodes"][d_def[0][2]].get_children(path_def = d_def[1:], links=links)

                elif sel_node == self.dtc.selPathRoot:
                    if match_node(self.root, sel_node):
                        childs = self.root.get_children(path_def = d_def[1:], links=links)

                elif sel_node == self.dtc.selPathParent:
                    if match_node(self.parent, sel_node):
                        childs = self.parent.get_children(path_def = d_def[1:], links=links)

                else:
                    clist = self.children[:]
                    if (d_def[0][0] & self.dtc.getLast):
                        clist.reverse()

                    for item in clist:
                        if match_node(item, sel_node):
                            childs = extend_list(childs, item.get_children(path_def = d_def[1:], links=links))
                            if (d_def[0][0] & self.dtc.getOnlyOne) and len(childs) > 0:
                                break

                break

            elif ndef_type == self.dtc.isValue:
                val = self.find_value(d_def[0])
                if self.dtree.show_result:
                    if isinstance(val, (str,unicode)):
                        self.dtree.print_text(u'    found nodevalue (="%s"): %s\n%s' % \
                            (val, self.print_node(), self.print_node_def(d_def[0])))

                    else:
                        self.dtree.print_text(u'    found nodevalue (=%s): %s\n%s' % \
                            (val, self.print_node(), self.print_node_def(d_def[0])))

                if (d_def[0][0] & self.dtc.storeLinkValue):
                    links["values"][d_def[0][self.dtc.getPos[self.dtc.storeLinkValue]]] = val

                if (d_def[0][0] & self.dtc.storePathValue):
                    childs = [(self, val)]
                    break

                d_def = d_def[1:]

            elif ndef_type == self.dtc.isNodeLink:
                if self.dtree.show_result:
                    self.dtree.print_text(self.print_node_def(d_def[0]))

                links["nodes"][d_def[0][1]] = self
                d_def = d_def[1:]

            elif ndef_type == self.dtc.storeName:
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
                self.dtree.print_text(u'  adding node (= %s) %s' % (childs[0][1], self.print_node()))

        if nm == None:
            return childs

        else:
            return [{nm: childs}]

    def check_index(self, ilist, link_values):
        for v in ilist:
            il = self.get_value(v, link_values, 'index', 'int')
            if (il[1] & self.dtc.valLinkPrevious):
                if self.child_index < il[0]:
                    return True

            elif (il[1] & self.dtc.valLinkNext):
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
            if sub_def[0] == self.dtc.valValue:
                self.dtree.warn('Invalid %s %s "%s" requested. Should be %s.' % \
                    (valuetype, "matchvalue", value, vtype), dtParseWarning, 3, 3)

            else:
                self.dtree.warn('Invalid %s %s "%s" requested. Should be %s.' % \
                    (valuetype, "linkvalue", value, vtype), dtParseWarning, 2, 3)

            return (None, 0)

        if sub_def[0] == self.dtc.valValue:
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
            if sub_def[0] & self.dtc.valLinkMin:
                value -= sub_def[2]

            elif sub_def[0] & self.dtc.valLinkPlus:
                value += sub_def[2]

        return (value, sub_def[0] & (self.dtc.valLinkNext + self.dtc.valLinkPrevious))
        # zip(value, sub_def[0] & (self.dtc.valLinkNext + self.dtc.valLinkPrevious))[0]

    def match_node(self, node_def = None, link_values = None, sel_node=0):
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
        if node_def[0] & self.dtc.isGroup not in (self.dtc.isValue, self.dtc.storeName):
            return None

        sv = self.find_node_value(node_def[1])
        if node_def[0] & self.dtc.hasCalc:
            sv = self.dtree.calc_value(sv, node_def[self.dtc.getPos[self.dtc.hasCalc]])

        if sv == None and node_def[0] & self.dtc.hasDefault:
            sv = node_def[self.dtc.getPos[self.dtc.hasDefault]]

        if node_def[0] & self.dtc.hasType:
            sv = self.dtree.calc_type(sv, node_def[self.dtc.getPos[self.dtc.hasType]])

        if node_def[0] & self.dtc.isMemberOff:
            imo = node_def[self.dtc.getPos[self.dtc.isMemberOff]]
            if not imo in self.dtree.value_filters.keys() or not sv in self.dtree.value_filters[imo]:
                sv = NULLnode()

        # Make sure a string is unicode and free of HTML entities
        if isinstance(sv, (str, unicode)):
            sv = re.sub('\n','', re.sub('\r','', self.dtree.un_escape(unicode(sv)))).strip()

        return sv

    def find_node_value(self, val_def=None):
        # Detailed in child class Collect and return any value
        return self.value

    def get_leveltabs(self, spaces=4, pluslevel=0):
        return u''.ljust(spaces * (self.level + pluslevel))

    def print_sel_def(self, sel_def, spc):
        # Detailed in child class to print its specific keys in human form
        return sel_def

    def print_value_link(self, vldef, is_index = False):
        if vldef[0] == self.dtc.valValue:
            if isinstance(vldef[1], (str, unicode)):
                return u'"%s"' % vldef[1]
            return vldef[1]

        if vldef[0] & self.dtc.valLinkPlus:
            lv = 'Value-Link ID: %s + %s' % (vldef[1], vldef[2])

        elif vldef[0] & self.dtc.valLinkMin:
            lv = 'Value-Link ID: %s - %s' % (vldef[1], vldef[2])

        else:
            lv = 'Value-Link ID: %s' % (vldef[1], )

        if is_index and vldef[0] & self.dtc.valLinkNext:
            return 'higher then %s' % (lv, )

        elif is_index and vldef[0] & self.dtc.valLinkPrevious:
            return 'lower then %s' % (lv, )

        return lv

    def print_value_link_list(self, vldef, is_index = False, add_starter = False):
        if isinstance(vldef[0], int):
            return self.print_value_link(vldef, is_index)

        vlist = []
        for item in vldef:
            vlist.append(self.print_value_link(item, is_index))

        if len(vlist) == 0:
            return 'Any of the values: ()'

        elif vlist[0] == None:
            if add_starter:
                return 'with any value'

            else:
                return 'any value'

        elif len(vlist) == 1:
            if add_starter:
                return 'with a value: %s' % vlist[0]

            else:
                return vlist[0]

        else:
            if add_starter:
                rtx = 'with any of the values:('

            else:
                rtx = '('

            for v in vlist:
                rtx = '%s %s, ' % (rtx, v)

            return '%s)' % (rtx.rstrip().rstrip(','), )

    def print_val_def(self, val_def):
        # Detailed in child class to print its specific keys in human form
        return val_def

    def print_node_def(self, node_def):
        def print_calc_def(calc_def, spc):
            rv = ''
            for cd in calc_def:
                if cd[0] == self.dtc.calcLettering:
                    rv = '%s%s to %s\n%s'% \
                        (rv, self.dtc.const_text('calc_name',cd[0]), self.dtc.const_text('case_name',cd[1]), spc)

                elif cd[0] == self.dtc.calcAsciiReplace:
                    st = 'replacing non ascii characters with "%s"' % (cd[1][0], )
                    n = 2
                    while len(cd[1]) > n:
                        st += ', "%s" with "%s"' % (cd[1][2], cd[1][1])
                        n+= 2

                    rv = '%s%s: %s\n%s'% (rv, self.dtc.const_text('calc_name',cd[0]), st, spc)

                elif cd[0] in (self.dtc.calcLstrip, self.dtc.calcRstrip):
                    rv = '%s%s: %s if present\n%s'% (rv, self.dtc.const_text('calc_name',cd[0]), cd[1], spc)

                elif cd[0] == self.dtc.calcSub:
                    st = ''
                    for sset in cd[1]:
                        st += ', "%s" with "%s"' % (sset[0], sset[1])

                    rv = '%s%s: %s\n%s'% (rv, self.dtc.const_text('calc_name',cd[0]), st, spc)

                elif cd[0] == self.dtc.calcSplit:
                    for sset in cd[1]:
                        rv = '%s%s on "%s" and returning parts: %s\n%s'% \
                            (rv, self.dtc.const_text('calc_name',cd[0]), sset[0], sset[1:], spc)

                elif cd[0] in (self.dtc.calcMultiply, self.dtc.calcDivide):
                    rv = '%s%s %s\n%s'% (rv, self.dtc.const_text('calc_name',cd[0]), cd[1], spc)

                elif cd[0] == self.dtc.calcReplace:
                    rv = '%s%s any found value in %s with the corresponding value in %s\n%s'% \
                        (rv, self.dtc.const_text('calc_name',cd[0]), cd[1], cd[2], spc)

            return rv.rstrip('\n').rstrip()

        def print_type_def(type_def):
            if type_def[0] in (self.dtc.typeTimeStamp, self.dtc.typeDateStamp):
                return "%s: dividing the value first by: %s" % \
                    (self.dtc.const_text('type_name', type_def[0]), type_def[1])

            elif type_def[0] == self.dtc.typeDateTimeString:
                return "%s using %s" % (self.dtc.const_text('type_name', type_def[0]), type_def[1])

            elif type_def[0] == self.dtc.typeTime:
                return '%s using %s hour clock and splitting on "%s"' % \
                    (self.dtc.const_text('type_name', type_def[0]), type_def[1], type_def[2])

            elif type_def[0] == self.dtc.typeDate:
                return "%s using %s" % (self.dtc.const_text('type_name', type_def[0]), type_def[1])

            elif type_def[0] == self.dtc.typeStringList:
                if type_def[2]:
                    return '%s: splitting on "%s" removing any empty (strings/values' % \
                        (self.dtc.const_text('type_name', type_def[0]), type_def[1])

                return '%s: splitting on "%s"' % (self.dtc.const_text('type_name', type_def[0]), type_def[1])

            else:
                return self.dtc.const_text('type_name', type_def[0])

        def print_node_sel_def(sel_def, spc):
            sel_node = (sel_def[1] & self.dtc.selMain)
            if sel_node == self.dtc.selPathLink:
                return 'returning the earlier stored Node link: %s' % (sel_def[self.dtc.selPos[self.dtc.selPathLink]], )

            elif sel_node == self.dtc.selPathRoot:
                return 'returning the Root Node'

            elif sel_node == self.dtc.selPathParent:
                return 'returning the Parent Node'

            elif node_def[0] & self.dtc.getLast:
                return u'returning the last found Child Node%s'% \
                        (self.print_sel_def(sel_def, spc), )

            elif node_def[0] & self.dtc.getOnlyOne:
                return u'returning the first found Child Node%s'% \
                        (self.print_sel_def(sel_def, spc), )

            else:
                return u'returning all Child Nodes%s'% (self.print_sel_def(sel_def, spc), )

        spc = self.get_leveltabs(4, 1)
        spc2 = self.get_leveltabs(4, 2)
        spc3 = self.get_leveltabs(4, 3)
        if self.dtree.is_data_value("dtversion", tuple):
            ndef_type = (node_def[0] & self.dtc.isGroup)
            if ndef_type == self.dtc.isNodeSel:
                rstr = u'%s%s %s\n%s' % \
                    (spc, self.dtc.const_text('node_name',ndef_type), print_node_sel_def(node_def, spc), spc)

            elif ndef_type == self.dtc.isNodeLink:
                rstr = u'%sStoring the Node under Node-Link ID: %s' % (spc, node_def[1])

            elif ndef_type in (self.dtc.storeName, self.dtc.isValue):
                rstr = u'%s%s: returning %s' % (spc, self.dtc.const_text('node_name', ndef_type), self.print_val_def(node_def[1]))
                if node_def[0] & self.dtc.hasCalc:
                    rstr = u'%s\n%swith calcfunctions: (%s)' % \
                        (rstr, spc2, print_calc_def(node_def[self.dtc.getPos[self.dtc.hasCalc]], spc3))

                if node_def[0] & self.dtc.hasDefault:
                    rstr = u'%s\n%swith a default value of: (%s)' % \
                        (rstr, spc2, node_def[self.dtc.getPos[self.dtc.hasDefault]])

                if node_def[0] & self.dtc.hasType:
                    rstr = u'%s\n%swith a type definition as: %s' % \
                        (rstr, spc2, print_type_def(node_def[self.dtc.getPos[self.dtc.hasType]]))

                if node_def[0] & self.dtc.isMemberOff:
                    rstr = u'%s\n%swhich must be present in the %s value_filter list' % \
                        (rstr, spc2, node_def[self.dtc.getPos[self.dtc.isMemberOff]])

                if node_def[0] & self.dtc.storeLinkValue:
                    rstr = u'%s\n%sStoring it under Value-Link ID: %s' % \
                        (rstr, spc, node_def[self.dtc.getPos[self.dtc.storeLinkValue]])

                if node_def[0] & self.dtc.storePathValue:
                    rstr = u'%s\n%sReturning it as path_def value' % (rstr, spc)

        else:
            rstr = u'%snode_def: ' % (spc, )
            for k, v in node_def.items():
                rstr = u'%s%s: %s\n%s          ' % (rstr, k, v, spc)

        return rstr.rstrip('\n').rstrip()

    def print_tree(self):
        sstr =u'%s%s' % (self.get_leveltabs(), self.print_node(True))
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

    def match_node(self, tag = None, attributes = None, node_def = None, link_values = None, sel_node=0):
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

        if sel_node == self.dtc.selTag:
            if not self.get_value(node_def[self.dtc.selPos[self.dtc.selTag]], link_values, 'tag', 'lower')[0] in (None, self.tag.lower()):
                # The requested tag doesn't matches
                return False

        elif sel_node == self.dtc.selTags:
            if not self.tag.lower() in self.get_value_list(node_def[self.dtc.selPos[self.dtc.selTags]], link_values, 'tag', 'lower'):
                # This tag isn't in the list with requested tags
                return False

        if (node_def[1] & self.dtc.selText):
            if not self.text.lower() in self.get_value_list(node_def[self.dtc.selPos[self.dtc.selText]], link_values, 'text', 'lower'):
                # This text isn't in the list with requested values
                return False

        if (node_def[1] & self.dtc.selTail):
            if not self.tail.lower() in self.get_value_list(node_def[self.dtc.selPos[self.dtc.selTail]], link_values, 'tail', 'lower'):
                # This tailtext isn't in the list with requested values
                return False

        if (node_def[1] & self.dtc.selAttrs):
            for cd in node_def[self.dtc.selPos[self.dtc.selAttrs]]:
                # For each set
                for ck in cd:
                    # For each attribute ck[0]
                    if self.is_attribute(ck[0]):
                        # The attribute is there
                        alist = self.get_value_list(ck[2], link_values, 'attribute', 'str')
                        if ck[1] == self.dtc.attrNot and self.attributes[ck[0]] not in alist:
                            # Without a forbidden value
                            continue

                        elif ck[1] == self.dtc.attr and ((len(alist) == 1 and alist[0] == None) or self.attributes[ck[0]] in alist):
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

        if (node_def[1] & self.dtc.selNotAttrs):
            for cd in node_def[self.dtc.selPos[self.dtc.selNotAttrs]]:
                # For each set
                for ck in cd:
                    # For each attribute ck[0]
                    if self.is_attribute(ck[0]):
                        # The attribute is there
                        alist = self.get_value_list(ck[2], link_values, 'notattrs', 'str')
                        if ck[1] == self.dtc.attrNot and self.attributes[ck[0]] in alist:
                            # With an allowed value
                            continue

                        elif ck[1] == self.dtc.attr and not ((len(alist) == 1 and alist[0] == None) or self.attributes[ck[0]] in alist):
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

        val_source = val_def[0] & self.dtc.getGroup
        if val_source == self.dtc.getText:
            return self.text

        elif val_source == self.dtc.getAttr:
            return self.get_attribute(val_def[1])

        elif val_source == self.dtc.getIndex:
            return self.child_index

        elif val_source == self.dtc.getTag:
            return self.tag

        elif val_source == self.dtc.getTail:
            return self.tail

        elif val_source == self.dtc.getInclusiveText:
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

            return sv

        elif val_source == self.dtc.getLitteral:
            return val_def[1]

        elif val_source == self.dtc.getPresence:
            return True

        else:
            return self.text

    def print_val_def(self, val_def):
        if val_def == None:
            return "the text"

        val_source = val_def[0] & self.dtc.getGroup
        if val_source == self.dtc.getText:
            return "the text"

        elif val_source == self.dtc.getAttr:
            return 'the attribute value for "%s"' % (val_def[1], )

        elif val_source == self.dtc.getIndex:
            return "the index"

        elif val_source == self.dtc.getTag:
            return "the tag"

        elif val_source == self.dtc.getTail:
            return "the tail text"

        elif val_source == self.dtc.getInclusiveText:
            if  val_def[1][1] == -1:
                return 'the inclusive text for a depth of %s excluding the tags: %s' % \
                    (val_def[1][0], val_def[1][2])

            elif  val_def[1][1] == 1:
                return 'the inclusive text for a depth of %s only including the tags: %s' % \
                    (val_def[1][0], val_def[1][2])

            else:
                return 'the inclusive text for a depth of %s' % (val_def[1][0], )

        elif val_source == self.dtc.getLitteral:
            return 'the value: "%s"' % ( val_def[1], )

        elif val_source == self.dtc.getPresence:
            return 'True if found'

        else:
            return "the text"

    def print_sel_def(self, sel_def, spc):
        sel_node = (sel_def[1] & self.dtc.selMain)
        rstr = u''
        if sel_node == self.dtc.selTag:
            rstr = u'    a tag: %s,\n%s' % \
                (self.print_value_link(sel_def[self.dtc.selPos[self.dtc.selTag]]), spc)

        elif sel_node == self.dtc.selTags:
            rstr = u'    a tag: %s,\n%s' % \
                (self.print_value_link_list(sel_def[self.dtc.selPos[self.dtc.selTags]]), spc)

        if sel_def[1] &  self.dtc.selAttrs:
            for cd in sel_def[self.dtc.selPos[self.dtc.selAttrs]]:
                for ck in cd:
                    rstr = u'%s    an attribute: "%s",\n%s' % (rstr, ck[0], spc)
                    if ck[1] == self.dtc.attr:
                        rstr = u'%s        %s,\n%s' % \
                            (rstr, self.print_value_link_list(ck[2], add_starter = True), spc)

                    else:
                        rstr = u'%s        but not %s,\n%s' % \
                            (rstr, self.print_value_link_list(ck[2], add_starter = True), spc)

        if sel_def[1] &  self.dtc.selNotAttrs:
            for cd in sel_def[self.dtc.selPos[self.dtc.selNotAttrs]]:
                for ck in cd:
                    rstr = u'%s    not an attribute: "%s",\n%s' % (rstr, ck[0], spc)
                    if ck[1] == self.dtc.attr:
                        rstr = u'%s        %s,\n%s' % \
                            (rstr, self.print_value_link_list(ck[2], add_starter = True), spc)

                    else:
                        rstr = u'%s        unless %s,\n%s' % \
                            (rstr, self.print_value_link_list(ck[2], add_starter = True), spc)

        if sel_node  in (self.dtc.selNone, self.dtc.selTag, self.dtc.selTags) and sel_def[1] &  self.dtc.selIndex:
            rstr = u'%s    an index: %s,\n%s' % \
                (rstr, self.print_value_link_list(sel_def[self.dtc.selPos[self.dtc.selIndex]], True), spc)

        if sel_def[1] &  self.dtc.selText:
            rstr = u'%s    a text: %s,\n%s' % \
                (rstr, self.print_value_link_list(sel_def[self.dtc.selPos[self.dtc.selText]]), spc)

        if sel_def[1] &  self.dtc.selTail:
            rstr = u'%s    a tailtext: %s,\n%s' % \
                (rstr, self.print_value_link_list(sel_def[self.dtc.selPos[self.dtc.selTail]]), spc)

        if rstr == u'':
            return u'.\n%s' % (spc, )

        else:
            return u' with: \n%s%s' % (spc, rstr)

    def print_node(self, print_all = False):
        attributes = u''
        spc = self.get_leveltabs(4, 1)
        if len(self.attributes) > 0:
            for a in self.attr_names:
                v = self.attributes[a]
                if isinstance(v, (str,unicode)):
                    v = re.sub('\r','', v)
                    v = re.sub('\n', ' ', v)
                attributes = u'%s%s = "%s",\n    %s' % (attributes, a, v, spc)
            attributes = attributes[:-(len(spc)+6)]

        rstr = u'at level %s:\n%s%s(%s)' % (self.level, spc, self.tag, attributes)
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

    def match_node(self, node_def = None, link_values = None, sel_node=0):
        if sel_node == self.dtc.selKey:
            if self.get_value(node_def[self.dtc.selPos[self.dtc.selKey]], link_values, 'key')[0] != self.key:
                # The requested key doesn't matches
                return False

        elif sel_node == self.dtc.selKeys:
            if not self.key in self.get_value_list(node_def[self.dtc.selPos[self.dtc.selKeys]], link_values, 'key'):
                # This key isn't in the list with requested keys
                return False

        if (node_def[1] & self.dtc.selChildKeys):
            for cd in node_def[self.dtc.selPos[self.dtc.selChildKeys]]:
                # For each set
                for ck in cd:
                    # For each key ck[0]
                    if ck[0] in self.keys:
                        # The Key is there
                        alist = self.get_value_list(ck[2], link_values, 'childkeys')
                        if ck[1] == self.dtc.attrNot and self.get_child(ck[0]).value not in alist:
                            # Without a forbidden value
                            continue

                        elif ck[1] == self.dtc.attr and ((len(alist) == 1 and alist[0] == None) or self.get_child(ck[0]).value in alist):
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

        if (node_def[1] & self.dtc.selNotChildKeys):
            for cd in node_def[self.dtc.selPos[self.dtc.selNotChildKeys]]:
                # For each set
                for ck in cd:
                    # For each key ck[0]
                    if ck[0] in self.keys:
                        # The Key is there
                        alist = self.get_value_list(ck[2], link_values, 'notchildkeys')
                        if ck[1] == self.dtc.attrNot and self.get_child(ck[0]).value in alist:
                            # With an allowed value
                            continue

                        elif ck[1] == self.dtc.attr and not ((len(alist) == 1 and alist[0] == None) or self.get_child(ck[0]).value in alist):
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

        val_source = val_def[0] & self.dtc.getGroup
        if val_source == self.dtc.getValue:
            return self.value

        elif val_source == self.dtc.getIndex:
            return self.child_index

        elif val_source == self.dtc.getKey:
            return self.key

        elif val_source == self.dtc.getLitteral:
            return val_def[1]

        elif val_source == self.dtc.getPresence:
            return True

        else:
            return self.value

    def print_val_def(self, val_def):
        if val_def == None:
            return 'the value'

        val_source = val_def[0] & self.dtc.getGroup
        if val_source == self.dtc.getValue:
            return 'the value'

        elif val_source == self.dtc.getIndex:
            return 'the index'

        elif val_source == self.dtc.getKey:
            return 'the key'

        elif val_source == self.dtc.getLitteral:
            return 'the value: "%s"' % ( val_def[1], )

        elif val_source == self.dtc.getPresence:
            return 'True if found'

        else:
            return 'the value'

    def print_sel_def(self, sel_def, spc):
        sel_node = (sel_def[1] & self.dtc.selMain)
        rstr = u''
        if sel_node == self.dtc.selKey:
            rstr = u'    with a key: %s,\n%s' % \
                (self.print_value_link(sel_def[self.dtc.selPos[self.dtc.selKey]]), spc)

        elif sel_node == self.dtc.selKeys:
            rstr = u'    with a key: %s,\n%s' % \
                (self.print_value_link_list(sel_def[self.dtc.selPos[self.dtc.selKeys]]), spc)

        if sel_def[1] &  self.dtc.selChildKeys:
            for cd in sel_def[self.dtc.selPos[self.dtc.selChildKeys]]:
                for ck in cd:
                    if isinstance(ck[0], (str, unicode)):
                        rstr = u'%s    a childkey: "%s",\n%s' % (rstr, ck[0], spc)

                    else:
                        rstr = u'%s    a childkey: %s,\n%s' % (rstr, ck[0], spc)

                    if ck[1] == self.dtc.attr:
                        rstr = u'%s        %s,\n%s' % \
                            (rstr, self.print_value_link_list(ck[2], add_starter = True), spc)

                    else:
                        rstr = u'%s        but not %s,\n%s' % \
                            (rstr, self.print_value_link_list(ck[2], add_starter = True), spc)

        if sel_def[1] &  self.dtc.selNotChildKeys:
            for cd in sel_def[self.dtc.selPos[self.dtc.selNotChildKeys]]:
                for ck in cd:
                    if isinstance(ck[0], (str, unicode)):
                        rstr = u'%s    not a childkey: "%s",\n%s' % (rstr, ck[0], spc)

                    else:
                        rstr = u'%s    not a childkey: %s,\n%s' % (rstr, ck[0], spc)

                    if ck[1] == self.dtc.attr:
                        rstr = u'%s        %s,\n%s' % \
                            (rstr, self.print_value_link_list(ck[2], add_starter = True), spc)

                    else:
                        rstr = u'%s        unless %s,\n%s' % \
                            (rstr, self.print_value_link_list(ck[2], add_starter = True), spc)

        if sel_node  in (self.dtc.selNone, self.dtc.selKeys) and sel_def[1] &  self.dtc.selIndex:
            rstr = u'%s    with an index: %s,\n%s' % \
                (rstr, self.print_value_link_list(sel_def[self.dtc.selPos[self.dtc.selIndex]], True), spc)

        if rstr == u'':
            return u'.\n%s' % (spc, )

        else:
            return u' with: \n%s%s' % (spc, rstr)

    def print_node(self, print_all = False):
        value = self.find_node_value() if self.type == "value" else '"%s"' % (self.type, )
        return u'%s = %s' % (self.key, value)

# end JSONnode

class DATAtree():
    def __init__(self, output = sys.stdout, warnaction = None, warngoal = sys.stderr, caller_id = 0):
        self.tree_lock = RLock()
        with self.tree_lock:
            self.tree_type=''
            self.dtc = DataTreeConstants()
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
        if (node_def[0] & self.dtc.isGroup) == self.dtc.isValue and (node_def[0] & self.dtc.hasType):
            return node_def[self.dtc.getPos[self.dtc.hasType]][0]

        else:
            return self.dtc.typeNone

    def _get_default(self, node_def):
        if (node_def[0] & self.dtc.isGroup) == self.dtc.isValue and (node_def[0] & self.dtc.hasDefault):
            return node_def[self.dtc.getPos[self.dtc.hasDefault]]

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
                if self.ddconv.errorcode & dte.dtFatalError != dte.dtDataDefOK:
                    return self.ddconv.errorcode & dte.dtFatalError

                self.data_def = self.ddconv.cdata_def

            if not self.data_def["dttype"] in (self.tree_type, ''):
                self.warn('Your data_def is written for a %s tree and is not usable for %s data' \
                    % (self.data_def["dttype"], self.tree_type), dtdata_defWarning, 1)
                return dte.dtDataDefInvalid

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
                return dte.dtStartNodeInvalid

            if self.print_searchtree:
                self.print_text('The root Tree:')
                self.root.print_tree()

            if self.show_result:
                self.print_text('Parsing the init_path starting at %s' % (self.root.print_node(), ))

            links = {"values": {},"nodes": {}}
            init_path = self.data_def["data"]["init-path"]
            sn = self.root.get_children(path_def = init_path, links = links)
            if sn == None or len(sn) == 0 or not isinstance(sn[0][0], DATAnode):
                self.warn('"init-path": %s did not result in a valid node. Falling back to the rootnode' \
                    % (init_path, ), dtParseWarning, 2)
                self.start_node = self.root
                return dte.dtStartNodeInvalid

            else:
                self.start_node = sn[0][0]
                return dte.dtDataOK

    def find_data_value(self, path_def, start_node = None, links = None, searchname = ''):
        with self.tree_lock:
            if isinstance(path_def, list):
                path_def = self.ddconv.convert_path_def(path_def)

            if not isinstance(path_def, tuple):
                self.warn('Invalid "path_def": %s supplied to "find_data_value"' % (path_def, ), dtParseWarning, 1)
                return

            if len(path_def) == 0:
                path_def = self.dtc.emptyNodeDef

            if start_node == None or not isinstance(start_node, DATAnode):
                start_node = self.start_node

            if not isinstance(start_node, DATAnode):
                self.warn('Unable to search the tree. Invalid dataset!', dtDataWarning, 1)
                return

            links = {"values": {},"nodes": {}} if links == None else links
            if searchname != '' and self.show_result:
                self.print_text('Parsing %s starting at %s' % (searchname, start_node.print_node()))

            nlist = start_node.get_children(path_def = path_def, links = links)
            if (path_def[-1][0] & self.dtc.isGroup == self.dtc.isValue) and \
                (path_def[-1][1][0] & self.dtc.getGroup == self.dtc.getPresence):
                # We return True if exactly one node is found, else False
                return (isinstance(nlist, list) and len(nlist) == 1)

            # Nothing found, so give the default or None
            if not isinstance(nlist, list) or nlist in ([], None):
                if self._get_type(path_def[-1]) == self.dtc.typeList:
                    return []

                else:
                    return self._get_default(path_def[-1])

            # We found multiple values
            if len(nlist) > 1 and (path_def[-1][0] & self.dtc.getOnlyOne):
                # There is only one child allowed
                if (path_def[-1][0] & self.dtc.getLast):
                    # There is a request to only return the last
                    nlist = nlist[-1:]

                else:
                    nlist = nlist[:1]

            if len(nlist) > 1 or self._get_type(path_def[-1]) == self.dtc.typeList:
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

            elif self._get_type(path_def[-1]) == self.dtc.typeList:
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

                return dte.dtStartNodeInvalid

            if self.print_searchtree:
                self.print_text('The %s Tree:' % (self.start_node.print_node(), ))
                self.start_node.print_tree()

            self.result = []
            def_list = []
            for dset in self.data_def['data']['iter']:
                if len(dset["key-path"]) == 0:
                    continue

                if self.show_result:
                    self.print_text(u'Parsing the key_path starting at %s' % (self.start_node.print_node(), ))

                links = {"values": {},"nodes": {}}
                self.key_list = self.start_node.get_children(path_def = dset["key-path"], links = links)
                k_cnt = len(self.key_list)
                k_item = 0
                if self.show_progress:
                    self.progress_queue.put((k_item, k_cnt))

                for k in self.key_list:
                    if self.quit:
                        return dte.dtQuiting

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

                    i = 0
                    for v in dset["values"][:]:
                        i += 1
                        if self.show_result:
                            self.print_text(u'  searching for value %s' % (i, ))
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

                return dte.dtNoData

            return dte.dtDataOK

    def calc_value(self, value, calc_def):
        def calc_warning(text, severity=4):
            self.warn('%s calculation Error on value: "%s"\n   Using node_def: %s' % \
                (text, value, calc_def), dtCalcWarning, severity, 3)

        if not isinstance(calc_def, tuple):
            return value

        for cd in calc_def:
            try:
                if isinstance(value, (str, unicode)):
                    if cd[0] == self.dtc.calcLettering:
                        if cd[1] == self.dtc.calcLower:
                            value = unicode(value).lower().strip()

                        elif cd[1] == self.dtc.calcUpper:
                            value = unicode(value).upper().strip()

                        elif cd[1] == self.dtc.calcCapitalize:
                            value = unicode(value).capitalize().strip()

                    elif cd[0] == self.dtc.calcAsciiReplace:
                        value = value.lower()
                        if len(cd[1]) > 2:
                            value = re.sub(cd[1][2], cd[1][1], value)

                        value = value.encode('ascii','replace')
                        value = re.sub('\?', cd[1][0], value)

                    elif cd[0] == self.dtc.calcLstrip:
                        if value.strip().lower()[:len(cd[1])] == cd[1].lower():
                            value = unicode(value[len(cd[1]):]).strip()

                    elif cd[0] == self.dtc.calcRstrip:
                        if value.strip().lower()[-len(cd[1]):] == cd[1].lower():
                            value = unicode(value[:-len(cd[1])]).strip()

                    elif cd[0] == self.dtc.calcSub:
                        for sset in cd[1]:
                            value = re.sub(sset[0], sset[1], value).strip()

                    elif cd[0] == self.dtc.calcSplit:
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

                if cd[0] == self.dtc.calcMultiply:
                    try:
                        value = int(value) * cd[1]

                    except:
                        calc_warning('multiplier')

                elif cd[0] == self.dtc.calcDivide:
                    try:
                        value = int(value) // cd[1]

                    except:
                        calc_warning('divider')

                elif cd[0] == self.dtc.calcReplace:
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
            if type_def[0] == self.dtc.typeTimeStamp:
                value = int(value)
                value = datetime.datetime.fromtimestamp(float(value/type_def[1]), self.utc)

            elif type_def[0] == self.dtc.typeDateTimeString:
                date = self.timezone.localize(datetime.datetime.strptime(value, type_def[1]))
                value = self.utc.normalize(date.astimezone(self.utc))

            elif type_def[0] == self.dtc.typeTime:
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

            elif type_def[0] == self.dtc.typeTimeDelta:
                try:
                    value = datetime.timedelta(seconds = int(value))

                except:
                    calc_warning('timedelta type')

            elif type_def[0] == self.dtc.typeDate:
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

            elif type_def[0] == self.dtc.typeDateStamp:
                value = int(value)
                value = datetime.date.fromtimestamp(float(value/type_def[1]))

            elif type_def[0] == self.dtc.typeRelativeWeekday:
                if value.strip().lower() in self.relative_weekdays.keys():
                    value = self.relative_weekdays[value.strip().lower()]

            elif type_def[0] == self.dtc.typeString:
                value = unicode(value)

            elif type_def[0] == self.dtc.typeLower:
                value = unicode(value).lower()

            elif type_def[0] == self.dtc.typeUpper:
                value = unicode(value).upper()

            elif type_def[0] == self.dtc.typeCapitalize:
                value = unicode(value).capitalize()

            elif type_def[0] == self.dtc.typeInteger:
                try:
                    value = int(value)

                except:
                    calc_warning('int type')
                    value = 0

            elif type_def[0] == self.dtc.typeFloat:
                try:
                    value = float(value)

                except:
                    calc_warning('float type')
                    value = 0

            elif type_def[0] == self.dtc.typeBoolean:
                if not isinstance(value, bool):
                    if isinstance(value, (int, float)):
                        value = bool(value>0)

                    elif isinstance(value, (str, unicode)):
                        value = bool(len(value) > 0 and value != '0')

                    else:
                        value = False

            elif type_def[0] == self.dtc.typeLowerAscii and isinstance(value, (str, unicode)):
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

            elif type_def[0] == self.dtc.typeStringList:
                try:
                    value = list(re.split(type_def[1], value))
                    if type_def[2]:
                        while '' in value:
                            value.remove('')

                        while None in value:
                            value.remove(None)

                except:
                    calc_warning('str-list type')

            elif type_def[0] == self.dtc.typeList:
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
            self.tree_type ='html'
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
                self.print_text(u'%sstarting %s %s %s' % (self.current_node.get_leveltabs(2), self.current_node.level+1, tag, attrs[0]))
                for a in range(1, len(attrs)):
                    self.print_text(u'%s        %s' % (self.current_node.get_leveltabs(2), attrs[a]))

            else:
                self.print_text(u'%sstarting %s %s' % (self.current_node.get_leveltabs(2), self.current_node.level,tag))

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
                self.print_text(u'%s        %s' % (self.current_node.get_leveltabs(2, -1), self.current_node.text.strip()))
            self.print_text(u'%sclosing %s %s %s' % (self.current_node.get_leveltabs(2, -1), self.current_node.level,tag, self.current_node.tag))

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
            self.tree_type ='json'
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
            self.dtc = DataTreeConstants()
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
            self.errorcode = dte.dtDataInvalid
            self.result = []
            self.data_def = None
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
                if self.ddconv.errorcode & dte.dtFatalError != dte.dtDataDefOK:
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
                        self.set_errorcode(dte.dtTimeZoneFailed)
                        self.timezone = oldtz

                    else:
                        self.warn('Invalid timezone "%s" suplied. Falling back to UTC' % (timezone, ), dtdata_defWarning, 2)
                        self.set_errorcode(dte.dtTimeZoneFailed)
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
                    self.set_errorcode(dte.dtCurrentDateFailed)
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
                self.set_errorcode(dte.dtUnquoteFailed)
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
                self.set_errorcode(dte.dtSortFailed)
                self.warn('Sort request {"path": %s, "childkeys": %s}" failed\n' % (path, childkeys) + \
                    '   as "path" is not present in the data or is not a list!', dtDataWarning, 2)

        with self.tree_lock:
            dttype = None
            self.searchtree = None
            if self.data_def == None:
                self.set_errorcode(dte.dtDataDefInvalid, True)
                self.warn('Please first initialize a data_def before loading your data.', dtdata_defWarning, 1)
                return self.check_errorcode()

            self.errorcode = dte.dtDataInvalid
            self.result = []
            if isinstance(data, (dict, list)):
                dttype = 'json'

            elif isinstance(data, (str, unicode)) and data.strip()[0] in ("{", "["):
                try:
                    data = json.loads(data)
                    dttype = 'json'

                except:
                    self.set_errorcode(dte.dtJSONerror, True)
                    self.warn('Failed to initialise the searchtree. Run with a valid dataset %s' \
                        % (type(data), ), dtDataWarning, 1)
                    return self.check_errorcode()

            elif isinstance(data, (str, unicode)) and data.strip()[0] == "<":
                dttype = 'html'

            else:
                self.warn('Failed to initialise the searchtree. Run with a valid dataset', dtDataWarning, 1)
                return self.check_errorcode()

            if not self.data_def["dttype"] in (dttype, ''):
                self.set_errorcode(dte.dtDataDefInvalid, True)
                self.warn('Your data_def is written for a %s tree and is not usable for %s data' \
                    % (self.data_def["dttype"], dttype), dtdata_defWarning, 1)
                return self.check_errorcode()

            if dttype == 'html':
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
                            self.set_errorcode(dte.dtTextReplaceFailed)
                            self.warn('An error occured applying "text_replace" regex: "%s"' % (subset, ), dtDataWarning, 2)

                for ut in self.data_def["unquote_html"]:
                    if isinstance(ut, (str, unicode)):
                        try:
                            data = re.sub(ut, unquote, data, 0, re.DOTALL)

                        except:
                            self.set_errorcode(dte.dtUnquoteFailed)
                            self.warn('An error occured applying "unquote_html" regex: "%s"' % (ut, ), dtDataWarning, 2)

                self.searchtree = HTMLtree(data, autoclose_tags, self.print_tags, self.fle, caller_id = self.caller_id, warnaction = None)

            elif dttype == 'json':
                for sitem in self.data_def['data']['sort']:
                    try:
                        sort_list(data, list(sitem[0]), list(sitem[1]))

                    except:
                        self.set_errorcode(dte.dtSortFailed)
                        self.warn('Sort request "%s" failed!' % (sitem, ), dtDataWarning, 2)

                self.searchtree = JSONtree(data, self.fle, caller_id = self.caller_id, warnaction = None)

            if  isinstance(self.searchtree, DATAtree) and isinstance(self.searchtree.start_node, DATAnode):
                self.set_errorcode(dte.dtDataOK, True)
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
                self.warn('The searchtree has not (jet) been initialized.\n' + \
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
            default = vdef[self.dtc.linkPos[self.dtc.linkhasDefault]]
            # Process the datavalues given for the function
            data = []
            for fd in vdef[1][1]:
                lact = fd[0] & self.dtc.linkGroup
                if lact == self.dtc.linkVarID:
                    data.append(get_variable(fd, key))

                elif lact == self.dtc.linkFuncID:
                    data.append(process_link_function(fd, key))

                elif lact == self.dtc.linkValue:
                    data.append(fd[1])

            # And call the linkfunction
            value = self.link_functions(funcid, data, default)
            if value in self.empty_values:
                return None

            return process_extras(value, vdef, key)

        def process_extras(value, vdef, key):
            if not value in self.empty_values:
                if vdef[0] & self.dtc.linkhasRegex and isinstance(value, (str, unicode)):
                    search_regex = vdef[self.dtc.linkPos[self.dtc.linkhasRegex]]
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

                if vdef[0] & self.dtc.linkhasType:
                    dtype = vdef[self.dtc.linkPos[self.dtc.linkhasType]]
                    try:
                        if dtype == self.dtc.typeString:
                            value = unicode(value)

                        elif dtype == self.dtc.typeLower:
                            value = unicode(value).lower()

                        elif dtype == self.dtc.typeUpper:
                            value = unicode(value).upper()

                        elif dtype == self.dtc.typeCapitalize:
                            value = unicode(value).capitalize()

                        elif dtype == self.dtc.typeInteger:
                            value = int(value)

                        elif dtype == self.dtc.typeFloat:
                            value = float(value)

                        elif dtype == self.dtc.typeBoolean:
                            value = bool(value)

                    except:
                        vtype = {
                                self.dtc.typeString: "String",
                                self.dtc.typeLower: "LowerCase",
                                self.dtc.typeUpper: "UpperCase",
                                self.dtc.typeCapitalize: "Capitalize",
                                self.dtc.typeInteger: "Integer",
                                self.dtc.typeFloat: "Float",
                                self.dtc.typeBoolean: "Boolean"}
                        self.warn('Error on applying type "%s" on "%s"'% (vtype[dtype], value), dtLinkWarning, 4)
                        value = None

                if vdef[0] & self.dtc.linkhasCalc:
                    for cv in vdef[self.dtc.linkPos[self.dtc.linkhasCalc]]:
                        if cv[0] == self.dtc.calcMultiply:
                            try:
                                if not isinstance(value, (int, float)):
                                    value = float(value)
                                value = value * cv[1]

                            except:
                                self.warn('Error on applying multiplier "%s" on "%s"'% \
                                    (cv[1], value), dtLinkWarning, 4)

                        if cv[0] == self.dtc.calcDivide:
                            try:
                                if not isinstance(value, (int, float)):
                                    value = float(value)
                                value = value / cv[1]

                            except:
                                self.warn('Error on applying divider "%s" on "%s"'% \
                                    (cv[1], value), dtLinkWarning, 4)

                if vdef[0] & self.dtc.linkhasMax:
                    if isinstance(value, (str, unicode, list, dict)) and len(value) > vdef[self.dtc.linkPos[self.dtc.linkhasMax]]:
                        self.warn('Requested datavalue "%s" is longer then %s'% \
                            (key, vdef[self.dtc.linkPos[self.dtc.linkhasMax]]), dtLinkWarning, 4)
                        value = None

                    if isinstance(value, (int, float)) and value > vdef[self.dtc.linkPos[self.dtc.linkhasMax]]:
                        self.warn('Requested datavalue "%s" is bigger then %s'% \
                            (key, vdef[self.dtc.linkPos[self.dtc.linkhasMax]]), dtLinkWarning, 4)
                        value = None

                if vdef[0] & self.dtc.linkhasMin:
                    if isinstance(value, (str, unicode, list, dict)) and len(value) < vdef[self.dtc.linkPos[self.dtc.linkhasMin]]:
                        self.warn('Requested datavalue "%s" is shorter then %s'% \
                            (key, vdef[self.dtc.linkPos[self.dtc.linkhasMin]]), dtLinkWarning, 4)
                        value = None

                    if isinstance(value, (int, float)) and value < vdef[self.dtc.linkPos[self.dtc.linkhasMin]]:
                        self.warn('Requested datavalue "%s" is smaller then %s'% \
                            (key, vdef[self.dtc.linkPos[self.dtc.linkhasMin]]), dtLinkWarning, 4)
                        value = None

            if value in self.empty_values:
                return vdef[self.dtc.linkPos[self.dtc.linkhasDefault]]

            return value

        values = {}
        if isinstance(linkdata, list):
            for k, v in self.data_def["values"].items():
                lact = v[0] & self.dtc.linkGroup
                cval = None
                if lact == self.dtc.linkVarID:
                    cval = get_variable(v, k)

                elif lact == self.dtc.linkFuncID:
                    cval = process_link_function(v, k)

                elif lact == self.dtc.linkValue:
                    cval = v[1]

                if not cval in self.empty_values:
                    values[k] = cval

                elif not v[self.dtc.linkPos[self.dtc.linkhasDefault]] in self.empty_values:
                    values[k] = v[self.dtc.linkPos[self.dtc.linkhasDefault]]
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
                    if is_data_value(4, data, datetime.date):
                        data[0] = data[4]

                    else:
                        data[0] = self.current_date

                if not(is_data_value(0, data, datetime.date) \
                  and is_data_value(1, data, datetime.time) \
                  and is_data_value(2, data, int)):
                    link_warning('Missing or invalid date and/or time values')
                    return default

                dtm = self.timezone.localize(datetime.datetime.combine(data[0], data[1]))
                dtm = pytz.utc.normalize(dtm.astimezone(pytz.utc))
                if is_data_value(3, data, datetime.time):
                    # We check if this time is after the first and if so we assume a midnight passing
                    dc = self.timezone.localize(datetime.datetime.combine(data[0], data[1]))
                    dc = pytz.utc.normalize(dc.astimezone(pytz.utc))
                    if dc > dtm:
                        data[0] += datetime.timedelta(days = 1)
                        dtm = self.timezone.localize(datetime.datetime.combine(data[0], data[1]))
                        dtm = pytz.utc.normalize(dtm.astimezone(pytz.utc))

                return dtm

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
            fcode = self.errorcode & dte.dtFatalError
            rcode = self.errorcode - fcode
            if set_fatal:
                fcode = (code & dte.dtFatalError)

            rcode = rcode | code
            rcode = rcode - (rcode & dte.dtFatalError)
            self.errorcode = rcode + fcode

    def check_errorcode(self, only_fatal = True, code = None, text_values = False):
        fcode = self.errorcode & dte.dtFatalError
        rcode = self.errorcode - fcode
        txtreturn = []
        intreturn = 0
        if code == None:
            if only_fatal:
                intreturn = fcode
                txtreturn = [dte.dtErrorTexts[fcode]]

            else:
                intreturn = fcode + rcode
                txtreturn = [dte.dtErrorTexts[fcode]]
                for i in (8, 16, 32, 64, 128):
                    if i & rcode:
                        txtreturn.append(dte.dtErrorTexts[i])

        else:
            rfcode = code & dte.dtFatalError
            rrcode = code - rfcode
            if rfcode == dte.dtFatalError or rfcode == fcode:
                intreturn = fcode
                txtreturn = [dte.dtErrorTexts[fcode]]

            for i in (8, 16, 32, 64, 128):
                if (i & rrcode) and (i & rcode):
                    intreturn += i
                    txtreturn.append(dte.dtErrorTexts[i])

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
