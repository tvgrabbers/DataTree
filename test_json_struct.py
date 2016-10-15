#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''test_json_struct is part of DataTreeGrab and can be used to test data_defs created for that module'''

import json, io, sys, os, re, traceback
import pytz, datetime, requests
from threading import Thread
from copy import copy, deepcopy
from DataTreeGrab import is_data_value, data_value, version

#json_struct syntax
#
# version:
# select_struct_list: list of dicts
#       teststruct: the name of the struct
#       description: a text to use on succesful idenification containing one "%s" to fill with the name of the identified file
#       title: a regex to match the file title
#       type: the root type: dict or list
#       keyvalues: with:
#           key: a key in the root or a list of keys to follow
#           present: boolean whether it should present or absent
#           value: the required value
#           default: the default value when absent
# The rest of the root keys should be struct names
#   Every struct name should start with "struct-"
#   To be used as type or in include it must be listed in a "struct_list" in json_struct-files.json
#   to be auto recognized it must be defined in the "select_struct_list" list above
#
# struct root keywords:
#       type: list, dict or numbered dict
#       include: a struct to include. The type from that struct will be taken
#           the definitions in the struct are dominant
#           any keys not present are added from the included struct
#       lookup_lists: dict with named lookup list to be used as type, a name should start with "lst-"
#           with:
#               a list keyword holding the actual list
#           or:
#               keyword: the keyword to extract the values from
#               values: the type of values: keys, intkeys, index or values
#                   In case of intkeys and index 3 lists are created
#                   one with a prefix of "int-" containing the integer values
#                   one with a prefix of "str-" containing the string values
#                   and one without prefix containing both
#               level: the level to look, default is 1
#           and:
#               append: True or False on whether to overwrite an existing list
#           optionaly a list of dicts with the following to determin which to use
#           add append; True to include all possible selections
#           add a first dict with an empty list and append: False to exclude earlier lists
#               reference_key:
#               regex:
#               value:
#           group_by: To group on a lower keyword with a level and a type keyword.
#               At any point you can store a selection value with "sublist-key" containing
#               the name of the lookup_list. Any type validation will then use that sublist.
#       reference_values: dict with named reference values to use in a conditional set
#           with:
#               keyword: a list of keyword to follow to the value
#               type: either: integer, string or boolean
#               default: the value to use if the the value is not found
#       base-type: a name to be used in conditional statements
#       report: A named dict of texts to add in a report to describe the selection critiria on failure
#   Any value in the below types dict or if type = list a types dict
#
#   types: list of (types or type-lists) or
#       list of dicts with
#           type (implied): type or type-list
#           length: (for string, list or dict)
#           if a list (potential root for a struct):
#               If the root of a struct a types keyword is expected to contain details
#               items: positional type(list) like 'types'
#               reverse_items: reversed positional type(list) like 'types'
#               other_items: A type(list) for items not covered by either of the above
#           if a dict (potential root for a struct):
#               sublist-key: use this key for sublist selection
#               keys:  type or type-list
#               either: a named dict with lists of alternatives. The name can be used inside the report keyword.
#                   Here an extra keyword "conditional_either" is possible with an extra conditional test
#                   on whether this set will participate as possible alternative
#                   It encapsulates a normal keyword together with a conditional statement
#               required: dict of key names with a dict with a types list:
#               sugested: dict of key names with a dict with a types list:
#               optional: dict of key names with a dict with a types list:
#               conditional: dict of key names with a dict with a types list:
#                   with extra keys:
#                       base-type:
#                       presence_key: a key to be present in the dict
#                       reference_key: a key value defined in reference_values
#                       value: a list of values to compare with
#                       regex: a regex string to test against or a string to be present in the value
#                       true: status if in value list defaulting to the false status
#                       false: status if not in value list defaulting to 3
#                           1: required (test and report on absence, used in selecting an either sub-struct)
#                           2: sugested (test and report on absence)
#                           3: optional (test and report on absence)
#                           0: ignore (mark as known and do not test or report)
#                          -1: unused (mark as known, report on presence)
#                          -2: forbidden (mark as known, report on presence, used in selecting an either sub-struct)
#               allowed: "all" this stops the looking for unknown keys
#               other_keys: a (list of) dicts with a keys and types statement. Any key of that type is validated
#                   and treated as known
#               ignore_keys: list of 'unknown' keys
#                   (all keys matching regex: '--.*?--' are ignored)
#               unused_keys: list of known keys
#               forbidden_keys: list of forbidden keys (at the root of a struct or an either outside struct or
#                   either validation it is treated as equal to unused_keys
#   default: valid for the last item in a types list
#   reference-default: A default pointing to value on a lower level, stored in reference_values
#
# Valid types:
#   dict ,numbered dict ,list, integer, string, tz-string, boolean, time, date, url, none
#   any struct listed in the "struct_list" lists in json_struct-files.json
#   any lookup_list defined in the struct root, included structs or parent structs
#
# Type Errors
# 1: Wrong Type
# 2: Value not in allowed list
# 3: Wrong Length
# 4: Invalid struct type
# 8: Wrong key-Type
# 16 key value not in allowed list
#
# Report Levels (default is -1 or all):
#   1: Missing required keys
#   2: Errors on required values and on either selection
#   4: Errors on not required keys
#   8: Missing sugested keys without a default
#  16: Missing sugested keys
#  32: Missing optional keys without a default
#  64: Missing optional keys
# 128: Unused keys
# 256: Unknown keys
# 512: Report on either selection (without errors)


class test_JSON():
    def __init__(self, encoding = 'utf-8', struct_file = None, struct_path = None):
        self.encoding = encoding
        self.report_file = sys.stdout
        self.testfile = None
        self.base_type = []
        self.trep = {}
        self.errors = {}
        self.etypes = ('type_errors', 'missing_keys', 'forbidden_keys',
                            'unused_keys', 'unknown_keys', 'either_test')
        self.imp = ('other',"required", "sugested", "optional", 'conditional')
        self.key_lists = ('unused_keys', 'ignore_keys', 'forbidden_keys')
        self.select_struct_list = []
        self.struct_list = []
        self.struct_tree = {}
        self.lookup_lists = {}
        self.lookup_list_keys = {}
        self.reference_values = {}
        self.only_local_files = False
        #~ self.only_local_files = True
        self.get_struct_files(struct_file, struct_path)

    def add_extra_lookup_lists(self, struct_name):
        # Function to  add extra lookup lists
        pass

    def log(self, text):
        if not isinstance(text, list):
            text = [text]

        for t in text:
            if isinstance(t, (str,unicode)):
                if not t[-1] == '\n':
                    t+= '\n'

                sys.stderr.write(t.encode(self.encoding, 'replace'))

    def report(self, text):
        if not isinstance(text, list):
            text = [text]

        for t in text:
            if isinstance(t, (str,unicode)):
                if not t[-1] == '\n':
                    t+= '\n'

                if self.report_file == sys.stdout:
                    self.report_file.write(t.encode(self.encoding, 'replace'))

                else:
                    self.report_file.write(unicode(t))

    def init_error(self):
        self.errors = {}
        for imp in self.imp:
            self.errors[imp] = {}

    def add_error(self, err, etype, vpath=None, importance=0):
        def make_path(epath):
            if not isinstance(epath, list):
                mpath = '/'

            else:
                mpath = ''
                for p in epath:
                    mpath = '%s/%s' % (mpath, p)

            return mpath

        def append_error(limp, ltype, epath, eval, errno = None, extend = False):
            limp = self.imp[limp]
            lpath = make_path(epath)
            if not limp in self.errors.keys():
                self.errors[limp] = {}

            if not ltype in self.errors[limp].keys():
                self.errors[limp][ltype] = {}

            if errno == None:
                if not lpath in self.errors[limp][ltype].keys():
                    self.errors[limp][ltype][lpath] = []

                if extend:
                    self.errors[limp][ltype][lpath].extend(eval)

                else:
                    self.errors[limp][ltype][lpath].append(eval)

            else:
                if not lpath in self.errors[limp][ltype].keys():
                    self.errors[limp][ltype][lpath] = {}

                if ltype =='either_test':
                    self.errors[limp][ltype][lpath][errno] = eval

                else:
                    if not errno in self.errors[limp][ltype][lpath].keys():
                        self.errors[limp][ltype][lpath][errno] = []

                    if extend:
                        self.errors[limp][ltype][lpath][errno].extend(eval)

                    else:
                        self.errors[limp][ltype][lpath][errno].append(eval)

        if not (0<= importance < len(self.imp) - 1):
            importance = 0

        if not etype in self.etypes:
            return
            etype = "other"

        if etype == 'type_errors':
            if not isinstance(err, list):
                return

            for e in err:
                if not isinstance(e['error'], list):
                    e['error'] = [e['error']]
                    e['type'] = [e['type']]

                for index in range(len(e['error'])):
                    if e['error'][index] == 0:
                        continue

                    if e['error'][index] & 7:
                        ve = e['error'][index] & 7
                        if ve == 1:
                            append_error(importance, etype, e['path'],
                                {'type':e['type'][index],
                                'value': deepcopy(e['value'])}, ve)

                        if ve == 2:
                            if e['list_key'] == None:
                                append_error(importance, etype, e['path'],
                                    {'type':e['type'][index],
                                    'value': deepcopy(e['value']),
                                    'list':self.lookup_lists[e['type'][index]],
                                    'list_key': e['list_key']}, ve)

                            else:
                                append_error(importance, etype, e['path'],
                                    {'type':e['type'][index],
                                    'value': deepcopy(e['value']),
                                    'list':self.lookup_lists[e['type'][index]][e['list_key']],
                                    'list_key': e['list_key']}, ve)

                        if ve == 3:
                            append_error(importance, etype, e['path'],
                                {'length':e['length'],
                                'value': deepcopy(e['value'])}, ve)

                        if ve == 4:
                            append_error(importance, etype, e['path'],
                                {'type':e['type'][index],
                                'report': e['report'],
                                'reason': e['reason']}, ve)

                    if e['error'][index] & 8:
                        # Key type error
                        for k, v in e['keyerrs'].items():
                            if v in self.lookup_lists.keys():
                                append_error(importance, etype, e['path'],
                                    {'type':v,
                                    'value': k,
                                    'list':self.lookup_lists[v]}, 16)

                            else:
                                append_error(importance, etype, e['path'],
                                    {'type':v,
                                    'value': k}, 8)

        elif etype == 'either_test':
            if len(err['list'][0][1]) == 0 and len(err['list'][0][2]) == 0 and len(err['list'][0][3]) == 0:
                append_error(0, etype, vpath, err, err['name'])

            else:
                append_error(1, etype, vpath, err, err['name'])

        elif etype in ('missing_keys', 'unused_keys', 'unknown_keys', 'forbidden_keys'):
            if isinstance(err, list):
                append_error(importance, etype, vpath, err, None, True)

            else:
                append_error(importance, etype, vpath, err)

        elif isinstance(err, list):
            append_error(0, etype, vpath, err, None, True)

        else:
            append_error(0, etype, vpath, err)

    def get_struct_files(self, struct_file = None, struct_path = None):
        if 'HOME' in os.environ:
            self.home_dir = os.environ['HOME']
        elif 'HOMEPATH' in os.environ:
            self.home_dir = os.environ['HOMEPATH']

        if os.name == 'nt' and 'USERPROFILE' in os.environ:
            self.home_dir = os.environ['USERPROFILE']

        if struct_file == None:
            struct_file = 'json_struct-files'

        if struct_path == None:
            struct_path = u'%s/.json_struct' % self.home_dir

        if not os.path.exists(struct_path):
            os.mkdir(struct_path)

        keyfiles = self._get_json_data(struct_file, struct_path = struct_path)
        if not isinstance(keyfiles, dict):
            self.log( 'We could not acces the "%s.json" file\n' % struct_file)
            self.struct_tree = None
            return

        if is_data_value("DTG-version", keyfiles, list):
            key_version = tuple(data_value("DTG-version", keyfiles, list, [2,0,0]))
            DTG_version = version()[1:4]
            if DTG_version < key_version:
                self.log('\nThe struct-files are intended for version %d.%d.%d,\n' % key_version)
                self.log('  but you have installed %s version %d.%d.%d\n' % version()[:4])
                self.log('  The test might not be accurate! Consider upgrading.\n\n')

        for item in data_value('files', keyfiles, list):
            for k, v in item.items():
                fle = self._get_json_data(k, data_value("version", v, int), struct_path)

                if not isinstance(fle, dict):
                    continue

                self.select_struct_list.extend(data_value("select_struct_list", fle, list))
                for s in data_value("struct_list", v, list):
                    if s in fle.keys():
                        self.struct_list.append(s)
                        self.struct_tree[s] = fle[s]

    def init_struct(self,struct_name, testval):
        if not data_value([struct_name, "initialized"], self.struct_tree, bool, False):
            include_list = data_value([struct_name, "include"], self.struct_tree)
            if isinstance(include_list, (str,unicode)):
                include_list = [include_list]

            if isinstance(include_list, list):
                for include_struct in include_list:
                    if include_struct in self.struct_list:
                        self.init_struct(include_struct, testval)
                        self.struct_tree[struct_name] = self.merge_structs(struct_name, include_struct)

                if "include" in self.struct_tree[struct_name].keys():
                    del self.struct_tree[struct_name]["include"]

            if isinstance(self.struct_tree[struct_name], dict):
                self.struct_tree[struct_name]["initialized"] = True

            if data_value([struct_name, "type"], self.struct_tree, str) in self.struct_list:
                # This should be an include!
                include_struct = data_value([struct_name, "type"], self.struct_tree, str)
                self.init_struct(include_struct, testval)
                self.struct_tree[struct_name] = self.merge_structs(struct_name, include_struct)

            if is_data_value([struct_name, "type"], self.struct_tree, list):
                typelist = data_value([struct_name, "type"], self.struct_tree, list)
                for index in range(len(typelist)):
                    if typelist[index] in self.struct_list:
                        # We have to create  a sub-struct
                        include_struct = typelist[index]
                        sub_struct = '%s-%s' % (struct_name, index)
                        self.init_struct(include_struct, testval)
                        self.struct_tree[sub_struct] = self.merge_structs(struct_name, include_struct)
                        self.struct_list.append(sub_struct)
                        typelist[index] = sub_struct

    def load_lookup_lists(self, struct_name, testval):
        # Load reference lists and values from the datafile
        def extract_list(dset):
            dlist = []
            ilist = []
            slist = []
            if v == 'keys' and isinstance(dset, dict):
                dlist.extend(dset.keys())

            elif v == 'intkeys' and isinstance(dset, dict):
                for k in dset.keys():
                    try:
                        s = int(k)
                        dlist.append(k)
                        dlist.append(s)
                        slist.append(k)
                        ilist.append(s)

                    except:
                        pass

            elif v == 'values':
                if isinstance(dset, dict):
                    dlist.extend(dset.values())

                if isinstance(dset, list):
                    dlist.extend(dset[:])

            elif v == 'index' and isinstance(dset, list):
                lup = range(len(dset))
                for item in lup:
                    dlist.append(item)
                    dlist.append(unicode(item))
                    ilist.append(item)
                    slist.append(unicode(item))

            return (dlist, ilist, slist)

        def add_list(lname, data, allowed = True, key = None):
            if key == None and isinstance(self.lookup_lists[lname], dict) or \
              key != None and isinstance(self.lookup_lists[lname], list):
                return

            iname = 'int-%s' % lname
            sname = 'str-%s' % lname
            if allowed:
                if key == None:
                    self.lookup_lists[lname].extend(data[0])
                    self.lookup_lists[lname] = list(set(self.lookup_lists[lname]))
                    self.lookup_lists[iname].extend(data[1])
                    self.lookup_lists[iname] = list(set(self.lookup_lists[iname]))
                    self.lookup_lists[sname].extend(data[2])
                    self.lookup_lists[sname] = list(set(self.lookup_lists[sname]))

                else:
                    if not key in self.lookup_lists[lname].keys():
                        self.lookup_lists[lname][key] = data[0]

                    else:
                        self.lookup_lists[lname][key].extend(data[0])

                    self.lookup_lists[lname][key] = list(set(self.lookup_lists[lname][key]))

                    if not key in self.lookup_lists[iname].keys():
                        self.lookup_lists[iname][key] = data[1]

                    else:
                        self.lookup_lists[iname][key].extend(data[1])

                    self.lookup_lists[iname][key] = list(set(self.lookup_lists[iname][key]))

                    if not key in self.lookup_lists[sname].keys():
                        self.lookup_lists[sname][key] = data[2]

                    else:
                        self.lookup_lists[sname][key].extend(data[2])

                    self.lookup_lists[sname][key] = list(set(self.lookup_lists[sname][key]))

            else:
                for val in data[0]:
                    if key == None:
                        if val in self.lookup_lists[lname]:
                            self.lookup_lists[lname].remove(val)

                    else:
                        if val in self.lookup_lists[lname][key]:
                            self.lookup_lists[lname][key].remove(val)

                for val in data[1]:
                    if key == None:
                        if val in self.lookup_lists[iname]:
                            self.lookup_lists[iname].remove(val)

                    else:
                        if val in self.lookup_lists[iname][key]:
                            self.lookup_lists[iname][key].remove(val)

                for val in data[2]:
                    if key == None:
                        if val in self.lookup_lists[sname]:
                            self.lookup_lists[sname].remove(val)

                    else:
                        if val in self.lookup_lists[sname][key]:
                            self.lookup_lists[sname][key].remove(val)

        for k, ldef in data_value([struct_name, "reference_values"],self.struct_tree, dict).items():
            kw = data_value(["keyword"], ldef, list)
            if not is_data_value(["default"], ldef) and not is_data_value(kw, testval):
                continue

            vtype = data_value(["type"], ldef, str)
            default = data_value(["default"], ldef, default = None)
            if vtype == 'integer':
                self.reference_values[k] = data_value(kw, testval, int, default = default)

            elif vtype == 'string':
                self.reference_values[k] = data_value(kw, testval, (str, unicode), default = default)

            elif vtype == 'boolean':
                self.reference_values[k] = data_value(kw, testval, bool, default = default)

            elif vtype == 'list':
                self.reference_values[k] = data_value(kw, testval, list, default = default)

            else:
                self.reference_values[k] = data_value(kw, testval, default = default)

        if is_data_value([struct_name, "lookup_lists", "list_order"],self.struct_tree, list):
            lupnames = data_value([struct_name, "lookup_lists", "list_order"],self.struct_tree, list)

        else:
            lupnames = data_value([struct_name, "lookup_lists"],self.struct_tree, dict).keys()

        for lname in lupnames:
            deflist = data_value([struct_name, "lookup_lists", lname],self.struct_tree)
            if not isinstance(deflist, list):
                deflist = [deflist]

            if is_data_value([0, "groupby"], deflist, dict):
                for n in (lname, 'int-%s' % lname, 'str-%s' % lname):
                    if not data_value([0, "append"], deflist, bool, False) or \
                      not is_data_value (n, self.lookup_lists, dict):
                        self.lookup_lists[n] = {}

            else:
                for n in (lname, 'int-%s' % lname, 'str-%s' % lname):
                    if not data_value([0, "append"], deflist, bool, False) or \
                      not is_data_value (n, self.lookup_lists, list):
                        self.lookup_lists[n] = []

            for ldef in deflist:
                allowed = data_value(["allowed"], ldef, bool, True)
                if not self.test_condition(ldef):
                    continue

                if is_data_value(["list"], ldef, list):
                    add_list(lname, (data_value("list", ldef, list),
                                    data_value("int-list", ldef, list),
                                    data_value("str-list", ldef, list)), allowed)

                else:
                    v = data_value(["values"], ldef, str)
                    level = data_value(["level"], ldef, int, 1)
                    kw = data_value(["keyword"], ldef, list)
                    bset = data_value(kw, testval)
                    if level == 1:
                        add_list(lname, extract_list(bset), allowed)

                    elif level == 2:
                        if is_data_value(["groupby"], ldef, dict):
                            gblevel = data_value(["groupby","level"], ldef, int, 1)
                            gbtype = data_value(["groupby","type"], ldef, str)
                            if gblevel == 1:
                                if isinstance(bset, list):
                                    for key in range(len(bset)):
                                        if gbtype == '' or self.test_type(gbtype, key) == 0:
                                            add_list(lname, extract_list(bset[key]), allowed, key)

                                elif isinstance(bset, dict):
                                    for key in bset.keys():
                                        if gbtype == '' or self.test_type(gbtype, key) == 0:
                                            add_list(lname, extract_list(bset[key]), allowed, key)

                        elif isinstance(bset, list):
                            for dset in bset:
                                add_list(lname, extract_list(dset), allowed)

                        elif isinstance(bset, dict):
                            for dset in bset.values():
                                add_list(lname, extract_list(dset), allowed)

        self.add_extra_lookup_lists(struct_name)

    def merge_structs(self, struct_name, include_struct):
        # struct2 is given through the "include" keyword in struct1
        struct1 = self.struct_tree[struct_name]
        struct2 = self.struct_tree[include_struct]
        mstruct = {}
        def key_list(sstruct):
            klist = []
            for g in range(1, len(self.imp)):
                klist.extend(data_value([self.imp[g]], sstruct, dict).keys())

            for g in self.key_lists:
                klist.extend(data_value([g], sstruct, list))

            return klist

        def check_presence(sstruct, skey):
            for g in range(1, len(self.imp)):
                if is_data_value([self.imp[g], skey], sstruct):
                    return self.imp[g]

            for g in self.key_lists:
                if skey in data_value([g], sstruct, list):
                    return g

            return None

        def merge_lookup(list_name):
            mstruct[list_name] = copy(data_value(list_name, struct1, dict))
            if is_data_value(list_name, struct2, dict):
                for k, v in struct2[list_name].items():
                    if not k in mstruct[list_name].keys():
                        mstruct[list_name][k] = v

                    elif data_value([list_name, k, "append"], mstruct, bool, False) \
                        and is_data_value([list_name, k, "list"], mstruct, list) \
                        and is_data_value([list_name, k, "list"], struct2, list):
                            mstruct[list_name][k]["list"].extend(data_value([list_name, k, "list"], struct2, list))

        def merge_base():
            mstruct['type'] = data_value('type', struct2)
            merge_lookup("lookup_lists")
            merge_lookup("reference_values")
            if is_data_value('base-type', struct1, str):
                mstruct['base-type'] = struct1['base-type']

            elif is_data_value('base-type', struct2, str):
                mstruct['base-type'] = struct2['base-type']

            if is_data_value('conditional-type', struct1, dict):
                mstruct['conditional-type'] = struct1['conditional-type']

            elif is_data_value('conditional-type', struct2, dict):
                mstruct['conditional-type'] = struct2['conditional-type']

            mstruct['report'] = {}
            if is_data_value(['report','struct'], struct1, (str,list)):
                mstruct['report']['struct'] = data_value(['report','struct'], struct1)

            elif is_data_value(['report','struct'], struct2, (str,list)):
                mstruct['report']['struct'] = data_value(['report','struct'], struct2)

        def merge_dict():
            addedlist = []
            eitherlist = []
            def add_keys(sstruct):
                for imp in range(1, len(self.imp)):
                    g = self.imp[imp]
                    for skey in data_value(g, sstruct, dict).keys():
                        if skey in addedlist:
                            continue

                        addedlist.append(skey)
                        if not g in mstruct.keys():
                            mstruct[g] = {}

                        mstruct[g][skey] = deepcopy(sstruct[g][skey])

                for g in self.key_lists:
                    for skey in data_value(g, sstruct, list):
                        if skey in addedlist:
                            continue

                        addedlist.append(skey)
                        if not g in mstruct.keys():
                            mstruct[g] = []

                        mstruct[g].append(skey)

            # We only use an 'either' in struct2 if not present in struct1
            mstruct['either'] = {}
            if is_data_value('either', struct1, dict):
                for k, v in data_value('either', struct1, dict).items():
                    eitherlist.append(k)
                    mstruct['either'][k] = deepcopy(v)
                    if is_data_value(['report', k], struct1, (str,list)):
                        mstruct['report'][k] = struct1['report'][k]
                    elif is_data_value(['report',k], struct2, (str,list)):
                        mstruct['report'][k] = struct2['report'][k]

            if is_data_value('either', struct2, dict):
                for k, v in data_value('either', struct2, dict).items():
                    if k in eitherlist:
                        continue

                    eitherlist.append(k)
                    mstruct['either'][k] = deepcopy(v)
                    if is_data_value(['report',k], struct2, (str,list)):
                        mstruct['report'][k] = struct2['report'][k]

            add_keys(struct1)
            add_keys(struct2)
            for k in (('keys', None), ('allowed', str), ('length', int)):
                if is_data_value(k[0], struct1, k[1], True):
                    mstruct[k[0]] = deepcopy(struct1[k[0]])

                elif is_data_value(k[0], struct2, k[1], True):
                    mstruct[k[0]] = deepcopy(struct2[k[0]])

            return mstruct

        def merge_list():
            if is_data_value('types', struct1, list):
                mstruct['types'] = deepcopy(struct1['types'])

            elif is_data_value('types', struct2, list):
                mstruct['types'] = deepcopy(struct2['types'])

            return mstruct

        if isinstance(struct2, list):
            return deepcopy(struct2)

        merge_base()
        if data_value('type', struct2) in ('dict', 'numbered dict'):
            return merge_dict()

        if data_value('type', struct2) == 'list' or is_data_value('types',struct1, list):
            return merge_list()

        for k, v in struct1.items():
            if k in ('type', 'base-type', 'conditional-type', "lookup_lists", "reference_values"):
                continue

            mstruct[k] = copy(v)

        for k, v in struct2.items():
            if not k in mstruct.keys():
                mstruct[k] = copy(v)

        return mstruct

    def test_file(self, file_name, struct_name = None, report_level = -1):
        self.found_data_defs = []
        self.file_struct = None
        j_file = self._open_file(file_name, 'r')
        if j_file == None:
            self.file_struct = None
            return 1

        try:
            self.testfile = json.load(j_file)

        except(ValueError) as e:
            self.log('The file: "%s" gives the following JSON error:\n' % file_name.split('/')[-1])
            self.log( '  %s\n' % e)
            return 2

        except:
            self.log(traceback.format_exc())
            self.file_struct = None
            return 2

        if struct_name != None:
            if struct_name in self.struct_list:
                self.file_struct = struct_name
                self.log('Testing "%s" with "%s"\n' % (file_name.split('/')[-1], struct_name))

            else:
                self.log(['The file %s is a correct JSON file\n' % file_name.split('/')[-1],
                            '  but "%s" is not a recognized struct name.\n'])
                self.file_struct = None
                return 3

        for struct_def in self.select_struct_list:
            if is_data_value("title", struct_def, str) and not re.match(struct_def["title"], file_name.split('/')[-1]):
                # The name does not satisfy the regex
                continue

            if data_value("type", struct_def, str) == 'dict' and not isinstance(self.testfile, dict):
                # It should be a dict and isn't
                continue

            if data_value("type", struct_def, str) == 'list' and not isinstance(self.testfile, list):
                # It should be a list and isn't
                continue

            for kv in data_value("keyvalues", struct_def, list):
                if is_data_value("present", kv, bool):
                    if is_data_value(data_value("key", kv), self.testfile) != data_value("present", kv, bool):
                        # A key precence or absence is not satisfied
                        break

                elif is_data_value("value", kv):
                    if data_value(data_value("key", kv), self.testfile, default = data_value("default", kv)) != data_value("value", kv):
                        # A key value is not satisfied
                        break

            else:
                # We have a match
                if '%s' in data_value("description", struct_def, str):
                    self.log(data_value("description", struct_def, str) % file_name.split('/')[-1])

                else:
                    self.log(data_value("description", struct_def, str))

                struct_name = data_value("teststruct", struct_def, str)
                self.file_struct = struct_name
                self.log('Testing "%s" with "%s"\n' % (file_name.split('/')[-1], struct_name))
                break

        else:
            self.log(['The file %s is a correct JSON file\n' % file_name.split('/')[-1],
                        '  but is not recognized.\n'])
            self.file_struct = None
            return 3

        self.init_error()
        self.init_struct(struct_name, self.testfile)
        tstruct = self.struct_tree[struct_name]
        self.load_lookup_lists(struct_name, self.testfile)
        if is_data_value('base-type', tstruct, str):
            self.base_type.append(data_value('base-type', tstruct, str))

        if is_data_value('conditional-type', tstruct, dict) and isinstance(self.testfile, dict):
            if data_value(['conditional-type', 'key'], tstruct, dict) in self.testfile.keys():
                self.base_type.append(data_value(['conditional-type', 'name'], tstruct, str))

        if is_data_value('types', tstruct, list):
            self.test_typelist(data_value('types', tstruct, list), self.testfile)

        elif data_value('type', tstruct, str) in ('dict', 'numbered dict'):
            self.test_dict(tstruct, self.testfile)

        self.report_errors(report_level)

    def test_struct_type(self, struct_name, testval):
        old_references = {}
        def init_struct():
            self.init_struct(struct_name, testval)
            old_references['base-type'] = self.base_type[:]
            if is_data_value('base-type', sstruct, str):
                self.base_type.append(sstruct['base-type'])

            if is_data_value('conditional-type', sstruct, dict) and isinstance(testval, dict):
                if data_value(['conditional-type', 'key'], sstruct, str) in testval.keys():
                    self.base_type.append(data_value(['conditional-type', 'name'], sstruct, str))

            old_references['lists'] = deepcopy(self.lookup_lists)
            old_references['values'] = copy(self.reference_values)
            self.load_lookup_lists(struct_name, testval)

        def reset_struct(testlist, just_reset = False):
            self.reference_values = old_references['values']
            self.lookup_lists = old_references['lists']
            self.base_type = old_references['base-type']
            if just_reset:
                return testlist

            elif len(testlist[0][1]) > 0:
                return 'required key(s) "%s" is/are missing'% testlist[0][1]

            elif len(testlist[0][2]) > 0:
                return 'forbidden key(s) "%s" is/are present'% testlist[0][2]

            elif len(testlist[0][3]) > 0:
                return 'errors were encountered'

            else:
                return True

        sstruct = self.struct_tree[struct_name]
        init_struct()
        stype = data_value('type', sstruct)
        if self.test_type(stype, testval) > 0:
            return reset_struct([(0,[],[],{struct_name:self.trep.copy()})])

        else:
            stype = self.trep['type']

        if stype in self.struct_list:
            return reset_struct(self.test_struct_type(stype, testval), True)

        elif stype == 'list' and is_data_value(['types', 0], sstruct, dict):
            typedict = data_value(['types', 0], sstruct, dict)
            len_list =  len(testval)
            len_start = len(data_value(['items'], typedict, list))
            for index in range(len_start):
                if index >= len_list:
                    break

                if self.test_type(typedict['items'][index], testval[index]) > 0:
                    return reset_struct([(0,[],[],{struct_name:self.trep.copy()})])

            len_end = len(data_value(['reverse_items'], typedict, list))
            for index in range(len_end):
                if index >= len_list:
                    break

                if self.test_type(typedict['reverse_items'][index], testval[-index-1]) > 0:
                    return reset_struct([(0,[],[],{struct_name:self.trep.copy()})])

            if is_data_value('other_items', typedict) and len_list > len_start + len_end:
                for index in range(len_start, len_list - len_end):
                    if self.test_type(typedict['other_items'], testval[index]) > 0:
                        return reset_struct([(0,[],[],{struct_name:self.trep.copy()})])

        elif stype == 'list' and is_data_value(['types', 1], sstruct):
            for item in testval:
                if self.test_type(data_value(['types', 1], sstruct), item) > 0:
                    return reset_struct([(0,[],[],{struct_name:self.trep.copy()})])

        elif stype in ('dict', 'numbered dict'):
            for k in data_value(['required'], sstruct, dict).keys():
                if not k in testval.keys():
                    return reset_struct([(0,[k],[],{})])

            for k in data_value(['forbidden_keys'], sstruct, list):
                if k in testval.keys():
                    return reset_struct([(0,[],[k],{})])

            for k in data_value(['conditional'], sstruct, dict).keys():
                imp = self.conditional_imp(k, sstruct, testval.keys())
                if imp == 1 and not k in testval.keys():
                    return reset_struct([(0,[k],[],{})])

                if imp == -2 and k in testval.keys():
                    return reset_struct([(0,[],[k],{})])

            if is_data_value('either', sstruct, dict):
                for k, v in data_value('either', sstruct, dict).items():
                    testlist = self.test_either(v, testval)
                    if (len(testlist[0][1]) == 0 and len(testlist[0][2]) == 0 and len(testlist[0][3]) == 0):
                        continue

                    return reset_struct(testlist)

        return reset_struct(True, True)

    def test_type(self, dtypes, val):
        reason = ''
        list_key = None
        def set_error(err):
            if len(keyerrs) > 0:
                if isinstance(err, list):
                    err[-1]+=8

                else:
                    err += 8

            self.trep = {'error': err,
                            'type': dtype,
                            'list_key': list_key,
                            'value': val,
                            'length':data_value("length", dtypes, int, 0),
                            'report':data_value([dtype,"report"], self.struct_tree),
                            'reason': reason,
                            'keyerrs':keyerrs}
            return err

        keyerrs = {}
        if isinstance(dtypes, (list, str, unicode)):
            dtypes = {'type': dtypes}

        if isinstance(dtypes, dict):
            dtype = data_value("type", dtypes)

        else:
            dtype = None
            return set_error(0)

        if isinstance(dtype, list):
            if len(dtype) < 1:
                return set_error(0)

            if len(dtype) == 1:
                dtype = dtype[0]

            else:
                rvals = []
                for dt in dtype:
                    dts = dtypes.copy()
                    dts['type'] = dt
                    err = self.test_type(dts, val)
                    rvals.append(err)
                    if err == 0:
                        return 0

                return set_error(rvals)

        if dtype in ('dict', 'numbered dict'):
            if not isinstance(val, dict):
                # Wrong type
                return set_error(1)

            if dtype == 'numbered dict':
                for k in val.keys():
                    if re.match('--.*?--', k):
                        continue

                    try:
                        k = int(k)

                    except:
                        keyerrs[k] = 'numeric (integer enclosed in double quotes)'

                if len(keyerrs) > 0:
                    return set_error(1)

            ktype = None
            if is_data_value("keys", dtypes, (str, list), True):
                ktype = data_value("keys", dtypes)

            if ktype != None:
                for k in val.keys():
                    if re.match('--.*?--', k):
                        continue

                    if self.test_type(ktype, k) > 0:
                        # Wrong key-type
                        keyerrs[k] = ktype

        elif dtype == 'list':
            if not isinstance(val, list):
                # Wrong type
                return set_error(1)

        elif dtype == 'integer':
            if not isinstance(val, int):
                # Wrong type
                return set_error(1)

        elif dtype == 'float':
            if not isinstance(val, (float, int)):
                # Wrong type
                return set_error(1)

        elif dtype == 'string':
            if not isinstance(val, (str, unicode)):
                # Wrong type
                return set_error(1)

        elif dtype == 'tz-string':
            if not isinstance(val, (str, unicode)):
                # Wrong type
                return set_error(1)

            try:
                tz = pytz.timezone(val)

            except:
                # Wrong type
                return set_error(1)

        elif dtype == 'boolean':
            if not isinstance(val, bool):
                # Wrong type
                return set_error(1)

        elif dtype == 'time':
            try:
                st = val.split(':')
                st = datetime.time(int(st[0]), int(st[1]))
            except:
                # Wrong type
                return set_error(1)

        elif dtype == 'date':
            try:
                sd = datetime.datetime.strptime(val, '%Y%m%d')

            except:
                # Wrong type
                return set_error(1)

        elif dtype == 'none':
            if val != None:
                # Wrong type
                return set_error(1)

        elif dtype == 'url':
            if not isinstance(val, (str, unicode)):
                # Wrong type
                return set_error(1)

        elif is_data_value(dtype, self.lookup_lists, list):
            if not val in self.lookup_lists[dtype]:
                # Wrong type
                return set_error(2)

        elif is_data_value(dtype, self.lookup_lists, dict):
            if dtype in self.lookup_list_keys.keys():
                list_key = copy(self.lookup_list_keys[dtype])

            if not dtype in self.lookup_list_keys.keys() or \
              not is_data_value([dtype, list_key], self.lookup_lists, list) or \
              not val in self.lookup_lists[dtype][list_key]:
                # Wrong type
                return set_error(2)

        elif dtype in self.struct_list:
            r = self.test_struct_type(dtype, val)
            if r != True:
                reason = r
                return set_error(4)

        else:
            pass

        if is_data_value("length", dtypes, int):
            try:
                if len(val) != dtypes["length"]:
                    # wrong length
                    return set_error(3)

            except:
                pass

        return set_error(0)

    def test_typelist(self, typelist, testval, vpath=None):
        errlist = []
        old_lookup_list_keys = deepcopy(self.lookup_list_keys)
        if typelist == None:
            return errlist

        if not isinstance(typelist, list):
            typelist = [typelist]

        if len(typelist) == 0:
            return errlist

        if self.test_type(typelist[0], testval) > 0:
            # It gives a type error
            errrep = self.trep.copy()
            errrep['path'] = vpath
            errlist.append(errrep)

        else:
            vr = self.trep.copy()
            if vr['type'] in self.struct_list:
                # Jump to the struct
                errlist.extend(self.test_struct(vr['type'], testval, vpath))

            if vr['type'] in ('dict', 'numbered dict'):
                self.test_dict(typelist[0], testval, vpath)
                if len(typelist) > 1:
                    # We check the next in the list
                    for k, v in testval.items():
                        if re.match('--.*?--', k):
                            continue

                        spath = [] if vpath == None else copy(vpath)
                        spath.append(k)
                        if is_data_value([0, "sublist-key"], typelist, str):
                            self.lookup_list_keys[data_value([0, "sublist-key"], typelist, str)] = k

                        errlist.extend(self.test_typelist( typelist[1:], v, spath))

            elif vr['type'] == 'list':
                self.test_list(typelist[0], testval, vpath)
                if len(typelist) > 1:
                    # We check the next in the list
                    for item in range(len(testval)):
                        spath = [] if vpath == None else copy(vpath)
                        spath.append(item)
                        if is_data_value([0, "sublist-key"], typelist, str):
                            self.lookup_list_keys[data_value([0, "sublist-key"], typelist, str)] = item

                        errlist.extend(self.test_typelist( typelist[1:], testval[item], spath))

        self.lookup_list_keys = old_lookup_list_keys
        return errlist

    def test_struct(self, struct_name, testval, vpath):
        errlist = []
        old_references = {}
        def init_struct():
            self.init_struct(struct_name, testval)
            old_references['base-type'] = self.base_type[:]
            if is_data_value('base-type', struct, str):
                if struct['base-type'] == "data_def":
                    self.found_data_defs.append(struct_name[7:])
                    if not is_data_value('data-format', testval, str):
                        self.log(['\nWe cannot test the data_def at "%s" as "data-format" is not set\n' % vpath,
                            '  and we cannot determin whether it is "JSON" or "HTML"!\n'])

                        return False

                self.base_type.append(struct['base-type'])

            if is_data_value('conditional-type', struct, dict) and isinstance(testval, dict):
                if data_value(['conditional-type', 'key'], struct, str) in testval.keys():
                    if data_value(['conditional-type', 'name'], struct, str) == "data_def":
                        self.found_data_defs.append(struct_name[7:])
                        if not is_data_value('data-format', testval, str):
                            self.log(['\nWe cannot test the data_def at "%s" as "data-format" is not set\n' % vpath,
                                '  and we cannot determin whether it is "JSON" or "HTML"!\n'])

                            return False

                    self.base_type.append(data_value(['conditional-type', 'name'], struct, str))

            old_references['lists'] = deepcopy(self.lookup_lists)
            old_references['values'] = copy(self.reference_values)
            self.load_lookup_lists(struct_name, testval)
            return True

        def reset_struct():
            self.reference_values = old_references['values']
            self.lookup_lists = old_references['lists']
            self.base_type = old_references['base-type']

        struct = self.struct_tree[struct_name]
        if not init_struct():
            reset_struct()
            return errlist

        stype = data_value('type', struct)
        if isinstance(stype, list):
            if self.test_type(stype, testval) > 0:
                errrep = self.trep.copy()
                errrep['path'] = vpath
                errlist.append(errrep)
                reset_struct()
                return errlist

            else:
                stype = self.trep['type']

        if stype in self.struct_list:
            stype = self.test_struct(stype, testval, vpath)

        if is_data_value('types', struct, list):
            errlist.extend(self.test_typelist(data_value('types', struct, list), testval, vpath))

        elif stype in ('dict', 'numbered dict'):
            self.test_dict(struct, testval, vpath)

        reset_struct()
        return errlist

    def test_condition(self, sstruct, testkeys = None):
        if is_data_value(['base-type'], sstruct, str):
            if not sstruct['base-type'] in self.base_type:
                return False

        if is_data_value(['base-type'], sstruct, list):
            for k in sstruct['base-type']:
                if k in self.base_type:
                    break

            else:
                return False

        if isinstance(testkeys, list):
            if is_data_value(['presence_key'], sstruct, str):
                if not sstruct['presence_key'] in testkeys:
                    return False

            if is_data_value(['presence_key'], sstruct, list):
                for k in sstruct['presence_key']:
                    if k in testkeys:
                        break

                else:
                    return False

        if is_data_value(['reference_key'], sstruct, str):
            revkey = data_value(['reference_key'], sstruct, str)
            if revkey in self.reference_values.keys():
                if is_data_value(['value'], sstruct, list):
                    if not self.reference_values[revkey] in sstruct['value']:
                        return False

                elif is_data_value(['regex'], sstruct, str):
                    if not re.search(sstruct['regex'], self.reference_values[revkey]):
                        return False

            else:
                return False

        return True

    def conditional_imp(self, dkey, sstruct, testkeys):
        impfalse = data_value(['conditional', dkey, 'false'], sstruct, int, 3)
        if self.test_condition(data_value(['conditional', dkey], sstruct, dict), testkeys):
            return data_value(['conditional', dkey, 'true'], sstruct, int, impfalse)

        return impfalse

    def test_either(self, eitherstruct, testval):
        either_test = []
        for item in range(len(eitherstruct)):
            missing = []
            wrongtype = {}
            forbidden = []
            if "conditional_either" in eitherstruct[item].keys():
                if not self.test_condition(eitherstruct[item]["conditional_either"],testval.keys()):
                    t = data_value([item, "conditional_either", 'text'], eitherstruct, str)
                    either_test.append((item, missing, forbidden, wrongtype, t))

                    continue
            # Test for required keys and their type
            for k, v in data_value([item, 'required'], eitherstruct, dict).items():
                if not k in testval.keys():
                    missing.append(k)
                    continue

                typelist = data_value(["types", 0], v)
                if self.test_type(typelist, testval[k]) > 0:
                    wrongtype[k] = self.trep.copy()

            # We test for the presence of forbidden keys
            for k in data_value([item, 'forbidden_keys'], eitherstruct, list):
                if k in testval.keys():
                    forbidden.append(k)

            for k, v in data_value([item, 'conditional'], eitherstruct, dict).items():
                imp = self.conditional_imp(k, eitherstruct[item], testval.keys())
                if imp == 1:
                    if not k in testval.keys():
                        missing.append(k)
                        continue

                    typelist = data_value(["types", 0], v)
                    if self.test_type(typelist, testval[k]) > 0:
                        wrongtype[k] = self.trep.copy()

                if imp == -2 and k in testval.keys():
                    forbidden.append(k)

            either_test.append((item, missing, forbidden, wrongtype, None))

        # We Sort to get the most likely one ( with in order the least missing keys, the least forbidden keys and the least faulty keys)
        either_test.sort(key=lambda k: (k[4], len(k[1]), len(k[2]), len(k[3])))
        return either_test

    def test_dict(self, sstruct, testval, vpath=None):
        def test_missing(dset, dkey, imp):
            if not dkey in testval.keys():
                refdev = data_value([dset, dkey, 'reference-default'], teststruct, str)
                if is_data_value([dset, dkey, 'default'], teststruct):
                    missing[imp].append((dkey, data_value([dset, dkey, 'default'], teststruct)))

                elif refdev not in ('', None) and refdev in self.reference_values.keys():
                    missing[imp].append((dkey, self.reference_values[refdev]))

                else:
                    missing[imp].append((dkey, None))

                return

            # get the type definition list for this key
            typelist = data_value([dset, dkey,"types"], teststruct, default = [])
            # set the context
            spath = [] if vpath == None else copy(vpath)
            spath.append(dkey)
            self.add_error(self.test_typelist(typelist, testval[dkey], spath), 'type_errors', spath, imp)

        old_lookup_list_keys = deepcopy(self.lookup_list_keys)
        if is_data_value('either', sstruct, dict):
            teststruct = {}
            add_keys = []
            for k, v in data_value('either', sstruct, dict).items():
                if len(v) == 0:
                    continue

                testlist = self.test_either(v, testval)
                text = data_value(['report', k], sstruct)
                self.add_error({'name': k, 'text': text, 'list': testlist}, 'either_test', vpath)

                # We use alternative testlist[0][0]
                item = testlist[0][0]
                for imp in range(1, len(self.imp)):
                    dset = self.imp[imp]
                    if is_data_value([item, dset], v, dict):
                        if not dset in teststruct.keys():
                            teststruct[dset] = {}

                        for dkey in v[item][dset].keys():
                            add_keys.append(dkey)
                            teststruct[dset][dkey] = copy(v[item][dset][dkey])

                for dset in self.key_lists:
                    if is_data_value([item, dset], v, list):
                        add_keys.extend(v[item][dset][:])
                        if not dset in teststruct.keys():
                            teststruct[dset] = v[item][dset][:]

                        else:
                            teststruct[dset].extend(v[item][dset][:])

                if 'allowed' in v[item].keys():
                    teststruct['allowed'] = copy(data_value([item, 'allowed'], v, list))

            # and add the original struct
            for imp in range(1, len(self.imp)):
                dset = self.imp[imp]
                if is_data_value([dset], sstruct, dict):
                    if not dset in teststruct.keys():
                        teststruct[dset] = {}

                    for dkey in sstruct[dset].keys():
                        if not dkey in add_keys:
                            teststruct[dset][dkey] = copy(sstruct[dset][dkey])

            for dset in self.key_lists:
                if is_data_value([dset], sstruct, list):
                    if not dset in teststruct.keys():
                        teststruct[dset] = []

                    for dkey in sstruct[dset]:
                        if not dkey in add_keys:
                            teststruct[dset].append(dkey)

            for k, v in sstruct.items():
                if k != 'either' and not k in teststruct.keys():
                    teststruct[k] = copy(v)

        else:
            teststruct = sstruct

        forbidden_list = data_value(['forbidden_keys'], teststruct, list)[:]
        unused_list = data_value(['unused_keys'], teststruct, list)[:]
        ignore_list = data_value(['ignore_keys'], teststruct, list)[:]
        known_keys = ignore_list[:]
        missing = {}
        for imp in range(1, len(self.imp) - 1):
            dset = self.imp[imp]
            missing[imp] = []
            known_keys.extend(data_value([dset], teststruct, dict).keys())
            for dkey in data_value([dset], teststruct, dict).keys():
                # test on the presence of defined keys
                test_missing(dset, dkey, imp)

        for dkey in data_value(['conditional'], teststruct, dict).keys():
            imp = self.conditional_imp(dkey, teststruct, testval.keys())
            if imp in range(1, len(self.imp) - 1):
                known_keys.append(dkey)
                # test on the presence of defined keys
                test_missing('conditional', dkey, imp)

            # ignore the key
            elif imp == 0:
                ignore_list.append(dkey)
                known_keys.append(dkey)

            # The key is unused
            elif imp == -1:
                unused_list.append(dkey)

            # The key is forbidden
            elif imp == -2:
                forbidden_list.append(dkey)

        # Check on forbidden or unused keys
        # and undefined keys unless no keys are defined or 'allowed: all' is set
        unknown = []
        unused = []
        forbidden = []
        for dkey in testval.keys():
            if not dkey in known_keys and not re.match('--.*?--', dkey):
                if dkey in forbidden_list:
                    forbidden.append(dkey)
                    continue

                if dkey in unused_list:
                    unused.append(dkey)
                    continue

                if is_data_value("other_keys", teststruct, (list, dict), True):
                    ok_list = data_value("other_keys", teststruct)
                    if not isinstance(ok_list, list):
                        ok_list = [ok_list]

                    for ok_dict in ok_list:
                        ktype = None
                        if is_data_value("keys", ok_dict, (str, list), True):
                            ktype = data_value("keys", ok_dict)

                        if ktype != None and self.test_type(ktype,dkey) == 0:
                            if is_data_value("sublist-key", ok_dict, str):
                                self.lookup_list_keys[data_value("sublist-key", ok_dict, str)] = dkey

                            typelist = data_value(["types"], ok_dict, default = [])
                            spath = [] if vpath == None else copy(vpath)
                            spath.append(dkey)
                            self.add_error(self.test_typelist(typelist, testval[dkey], spath), 'type_errors', spath)
                            break

                    else:
                        if is_data_value("keys", teststruct, (list, str), True) and \
                            self.test_type(data_value("keys", teststruct), dkey) == 0:
                                continue

                        if data_value("allowed", teststruct, str) == 'all':
                            continue

                        for dset in range(1, len(self.imp)):
                            if self.imp[dset] in teststruct:
                                unknown.append(dkey)
                                break

        # Report found errors
        for imp in range(1, len(self.imp) - 1):
            if len(missing[imp]) > 0:
                self.add_error(missing[imp], 'missing_keys', vpath, imp)

        for eset in ((unused, 'unused_keys'),(unknown, 'unknown_keys'),(forbidden, 'forbidden_keys')):
            if len(eset[0]) > 0:
                self.add_error(eset[0], eset[1],vpath)

        self.lookup_list_keys = old_lookup_list_keys

    def test_list(self, sstruct, testval, vpath=None):
        len_list =  len(testval)
        len_start = len(data_value(['items'], sstruct, list))
        len_end = len(data_value(['reverse_items'], sstruct, list))

        for index in range(len_start):
            if index < len_list:
                typelist = data_value(['items', index], sstruct)
                spath = [] if vpath == None else copy(vpath)
                spath.append(index)
                self.add_error(self.test_typelist(typelist, testval[index], spath), 'type_errors', spath,1)

        for index in range(len_end):
            if index < len_list:
                typelist = data_value(['reverse_items', index], sstruct)
                spath = [] if vpath == None else copy(vpath)
                spath.append(-index-1)
                self.add_error(self.test_typelist(typelist, testval[-index-1], spath), 'type_errors', spath,1)

        if is_data_value('other_items', sstruct) and len_list > len_start + len_end:
            typelist = data_value('other_items', sstruct)
            for index in range(len_start, len_list - len_end):
                spath = [] if vpath == None else copy(vpath)
                spath.append(index)
                self.add_error(self.test_typelist(typelist, testval[index], spath), 'type_errors', spath,1)

    def report_errors(self, report_level = -1):
        #~ print self.errors
        def type_err_string(ttype, tvals):
            if ttype ==1:
                if isinstance(tvals['value'], (dict, list)):
                    return 'Wrong value type, should be: "%s"' % tvals['type']

                elif isinstance(tvals['value'], (str, unicode)):
                    return 'Value "%s" is not of type: "%s"' % (tvals['value'],tvals['type'])

                else:
                    return 'Value %s is not of type: "%s"' % (tvals['value'],tvals['type'])

            elif ttype ==2:
                if len(tvals['list']) > 10:
                    if isinstance(tvals['value'], (str, unicode)):
                        return 'Value "%s" not in the "%s" list' % (tvals['value'],tvals['type'][4:])

                    else:
                        return 'Value %s not in the "%s" list' % (tvals['value'],tvals['type'][4:])

                else:
                    if isinstance(tvals['value'], (str, unicode)):
                        return 'Value "%s" not in "%s"' % (tvals['value'],tvals['list'])

                    else:
                        return 'Value %s not in "%s"' % (tvals['value'],tvals['list'])

            elif ttype == 3:
                return 'Value not of length: %s' % tvals['length']

            elif ttype == 4:
                rep = 'Value does not match a: "%s"' % tvals['type'][7:]
                if  is_data_value('reason', tvals, str, True):
                    rep = '%s as %s' % (rep, tvals['reason'])

                if is_data_value('report', tvals, str, True):
                    return '%s\n      %s' % (rep, tvals['report'])

                elif is_data_value('report', tvals, list, True):
                    for r in tvals['report']:
                        rep = '%s\n      %s' % (rep, r)
                    return rep

                else:
                    return rep

            if ttype == 8:
                return 'Key %s should be of type: "%s"' % (tvals['value'], tvals['type'])

            elif ttype == 16:
                return 'Key %s not in "%s"' % (tvals['value'], tvals['list'])

        def header_string(imp, etype, with_default):
            if etype in(0, 1):
                if with_default:
                    return 'The following %s, but have a default\n' % (etext[etype] % (self.imp[imp]))

                else:
                    return 'The following %s\n' % (etext[etype] % (self.imp[imp]))

            if etype in (2, 3, 4):
                return 'The following %s\n' % (etext[etype])

            if etype == 5:
                return 'From the following alternative syntaxes the first is selected%s:\n' % (etext[etype+imp])

        def format_err(imp, etype, with_default = False):
            repstring = ''
            val = data_value([self.imp[imp], self.etypes[etype]], self.errors, dict)
            if len(val) == 0:
                return

            pathlist = val.keys()
            pathlist.sort()
            for epath in pathlist:
                substring = ''
                if etype == 0:
                    for e in (1, 2, 3, 4, 8, 16):
                        if e in val[epath].keys() and len(val[epath][e]) > 0:
                            for vt in val[epath][e]:
                                substring += '    %s\n' % (type_err_string(e, vt))

                elif etype == 1:
                    val[epath].sort()
                    for key in val[epath]:
                        if key[1] == None:
                            if imp ==1 or not with_default:
                                substring += '    "%s"\n' % key[0]

                        elif isinstance(key[1], (str, unicode)):
                            if imp ==1 or with_default:
                                substring += '    "%s"  defaulting to: "%s"\n' % (key[0], key[1])

                        elif imp ==1 or with_default:
                            substring += '    "%s"  defaulting to: %s\n' % (key[0], key[1])

                elif etype in (2, 3, 4):
                    val[epath].sort()
                    for key in val[epath]:
                        substring += '    "%s"\n' % key

                elif etype == 5:
                    for k, v in val[epath].items():
                        if not isinstance(v['text'], list):
                            v['text'] = [v['text']]

                        for t in v['text']:
                            substring += '    %s\n' % t

                        substring += '    %s:\n' % k
                        for item in v['list']:
                            if item[4] != None:
                                substring += '      Option %s %s\n' % (item[0], item[4])
                                continue

                            substring += '      Option %s:\n' % (item[0])
                            if len(item[1]) == 0:
                                substring += '        With no missing keys\n'

                            else:
                                kstr = '        With the required '
                                for key in item[1]:
                                    kstr = '%s"%s", ' % (kstr, key)

                                substring += '%s key(s) missing\n' % (kstr[:-2])

                            if len(item[2]) == 0:
                                substring += '        With no forbidden keys\n'

                            else:
                                kstr = '        With the forbidden '
                                for key in item[2]:
                                    kstr = '%s"%s", ' % (kstr, key)

                                substring += '%s key(s) present\n' % (kstr[:-2])

                            if len(item[3]) == 0:
                                substring += '        With no errors\n'

                            else:
                                substring += '        With some errors\n'

                if substring != '':
                    repstring += '  at path: %s\n%s' % (epath, substring)

            if repstring != '':
                self.report(header_string(imp, etype, with_default))
                self.report(repstring)

        etext = ('errors in %s keys are encountered',
                '%s keys are not set',
                'forbidden keys are encountered. Expect unexpected results!',
                'known keys are found that will not be used',
                'keys are not recognized',
                ' without errors',
                ' with some errors')

        if len(self.errors[self.imp[1]]) == 0:
            self.log('\nNo serieus errors were found\n\n')
            retval = 0

        else:
            self.log('\nSome serious errors were encountered!\n\n')
            retval = 4

        if report_level & 1:
            format_err(imp = 1, etype = 1)

        if report_level & 2:
            for etype in (5, 0):
                format_err(imp = 1, etype = etype)

            format_err(imp = 0, etype = 2)

        if report_level & 4:
            for imp in (2, 3):
                format_err(imp = imp, etype = 0)

        if report_level & 8:
            format_err(imp = 2, etype = 1)

        if report_level & 16:
            format_err(imp = 2, etype = 1, with_default = True)

        if report_level & 32:
            format_err(imp = 3, etype = 1)

        if report_level & 64:
            format_err(imp = 3, etype = 1, with_default = True)

        if report_level & 128:
            format_err(imp = 0, etype = 3)

        if report_level & 256:
            format_err(imp = 0, etype = 4)

        if report_level & 512:
            format_err(imp = 0, etype = 5)

        return retval

    def _open_file(self, file_name, mode = 'rb', encoding = None):
        """ Open a file and return a file handler if success """
        if encoding == None:
            encoding = self.encoding

        if 'r' in mode and not (os.path.isfile(file_name) and os.access(file_name, os.R_OK)):
            self.log(u'File: "%s" not found or could not be accessed.\n' % (file_name.split('/')[-1], ))
            return None

        if ('a' in mode or 'w' in mode):
            if os.path.isfile(file_name) and not os.access(file_name, os.W_OK):
                self.log(u'File: "%s" not found or could not be accessed.\n' % (file_name.split('/')[-1], ))
                return None

        try:
            if 'b' in mode:
                file_handler =  io.open(file_name, mode = mode)
            else:
                file_handler =  io.open(file_name, mode = mode, encoding = encoding)

        except IOError as e:
            if e.errno == 2:
                self.log(u'File: "%s" not found or could not be accessed.\n' % (file_name.split('/')[-1], ))
            else:
                self.log('File: "%s": %s.\n' % (file_name.split('/')[-1], e.strerror))
            return None

        return file_handler

    def _get_json_data(self, name, version = None, struct_path = None):
        local_name = '%s.%s.json' % (name, version) if isinstance(version, int) else '%s.json' % (name)
        # Try to find the source files locally
        if isinstance(version, int) or self.only_local_files:
            # First we try to get it in the supplied location
            try:
                file_name = '%s/%s' % (struct_path, local_name)
                if os.path.isfile(file_name) and os.access(file_name, os.R_OK):
                    fle = self._open_file(file_name, 'r', 'utf-8')
                    if fle != None:
                        self.log('  Loaded "%s"\n' % local_name)
                        return json.load(fle)

            except(ValueError) as e:
                self.log( '  %s\n' % e)

            except:
                traceback.print_exc()

        # We try to download unless the only_local_files flag is set
        if not self.only_local_files:
            try:
                txtheaders = {'Keep-Alive' : '300'}
                url = 'https://raw.githubusercontent.com/tvgrabbers/sourcematching/master/json_struct'
                url = '%s/%s.json' % (url, name)
                self.log('  Downloading "%s.json"...\n' % name)
                fu = FetchURL(self, url, None, txtheaders, 'utf-8')
                fu.start()
                fu.join(11)
                page = fu.result
                if not isinstance(page, (dict, list)):
                    if isinstance(version, int):
                        return None

                else:
                    if fu.url_text != '':
                        fle = self._open_file('%s/.json_struct/%s' % (self.home_dir, local_name), 'w')
                        if fle != None:
                            fle.write(fu.url_text)
                            fle.close()

                    return page

            except:
                traceback.print_exc()
                if isinstance(version, int):
                    return None

        if version == None:
            try:
                fle = self._open_file('%s/%s' % (struct_path, local_name), 'r', 'utf-8')
                if fle != None:
                    return json.load(fle)

            except(ValueError):
                self.log( '  %s\n' % e)
                return None

            except:
                return None

class FetchURL(Thread):
    """
    A simple thread to fetch a url with a timeout
    """
    def __init__ (self, requester, url, txtdata = None, txtheaders = None, encoding = None):
        Thread.__init__(self)
        self.requester = requester
        self.url = url
        self.txtdata = txtdata
        self.txtheaders = txtheaders
        self.encoding = encoding
        self.raw = ''
        self.result = None

    def run(self):
        try:
            url_request = requests.get(self.url, headers = self.txtheaders, params = self.txtdata, timeout=5)
            self.raw = url_request.content
            if self.encoding != None:
                url_request.encoding = self.encoding

            self.url_text = url_request.text

            try:
                self.result = url_request.json()

            except(ValueError) as e:
                self.requester.log(u'Url %s does not return a valid json file\n' % (self.url, ))
                self.requester.log( '  %s\n' % e)

            except:
                self.requester.log(u'Url %s does not return a valid json file\n' % (self.url, ))
                traceback.print_exc()

        except (requests.ConnectionError) as e:
            self.requester.log(u'Cannot open url %s\n' % (self.url, ))

        except (requests.HTTPError) as e:
            self.requester.log(u'Cannot parse url %s: code=%s\n' % (self.url, e.code))

        except (requests.Timeout) as e:
            self.requester.log(u'get_page timed out on (>10 s): %s\n' % (self.url, ))

        except:
            self.requester.log(u'An unexpected error "%s:%s" has occured while fetching page: %s\n' % (sys.exc_info()[0], sys.exc_info()[1], self.url))

# end FetchURL


