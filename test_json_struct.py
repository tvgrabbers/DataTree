#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import json, io, sys, os, re, traceback
import pytz, datetime, requests
from threading import Thread
from copy import copy, deepcopy
from DataTreeGrab import is_data_value, data_value

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
#               values: the type of values: keys, intkeys or values
#               level: the level to look, default is 1
#           and:
#               append: True or False on whether to overwrite an existing list
#       reference_values: dict with named reference values to use in a conditional set
#           with:
#               keyword: a list of keyword to follow to the value
#               type: either: integer, string or boolean
#               default: the value to use if the the value is not found
#       report: text to add in a report to describe the selection critiria
#   Any value in the below types dict
#
#   types: list of (types or type-lists) or
#       list of dicts with
#           type (implied): type or type-list
#           length: (for string, list or dict)
#           if a list (potential root for a struct):
#               items: positional type(list) like 'types'
#               reverse_items: reversed positional type(list) like 'types'
#           if a dict (potential root for a struct):
#               keys:  type or type-list
#               either: list of alternatives
#               required: dict of key names with a dict with a types list:
#               sugested: dict of key names with a dict with a types list:
#               optional: dict of key names with a dict with a types list:
#               conditional: dict of key names with a dict with a types list:
#                   with extra keys:
#                       reference_key: a key value defined in reference_values
#                       value: a list of values to compare with
#                       true: status if in value list defaulting to the false status
#                       false: status if not in value list defaulting to 3
#                           1: required (test and report on absence, used in selecting an either sub-struct)
#                           2: sugested (test and report on absence)
#                           3: optional (test and report on absence)
#                           0: ignore (mark as known and do not test or report)
#                          -1: unused (mark as known, report on presence)
#                          -2: forbidden (mark as known, report on presence, used in selecting an either sub-struct)
#               allowed: "all"
#               ignore_keys: list of 'unknown' keys
#               unused_keys: list of known keys
#                   (all keys matching regex: '--.*?--' are ignored)
#               forbidden_keys: list of forbidden keys (at the root of a struct or an either)
#   default: valid for the last item in a types list
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

class test_JSON():
    def __init__(self, encoding = 'utf-8', struct_file = None, struct_path = None):
        self.encoding = encoding
        self.testfile = None
        self.trep = {}
        self.errors = {}
        self.etypes = ('type_errors', 'missing_keys',
                            'unused_keys', 'unknown_keys', 'either_test')
        self.imp = ('other',"required", "sugested", "optional", 'conditional')
        self.key_lists = ('unused_keys', 'ignore_keys', 'forbidden_keys')
        self.select_struct_list = []
        self.struct_list = []
        self.struct_tree = {}
        self.lookup_lists = {}
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

                sys.stderr.write(text.encode(self.encoding, 'replace'))

    def report(self, text):
        if not isinstance(text, list):
            text = [text]

        for t in text:
            if isinstance(t, (str,unicode)):
                if not t[-1] == '\n':
                    t+= '\n'

                sys.stdout.write(t.encode(self.encoding, 'replace'))

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

                if etype == 'either_test':
                    self.errors[limp][ltype][lpath] = eval

                elif extend:
                    self.errors[limp][ltype][lpath].extend(eval)

                else:
                    self.errors[limp][ltype][lpath].append(eval)

            else:
                if not lpath in self.errors[limp][ltype].keys():
                    self.errors[limp][ltype][lpath] = {}

                if not errno in self.errors[limp][ltype][lpath].keys():
                    self.errors[limp][ltype][lpath][errno] = []

                if extend:
                    self.errors[limp][ltype][lpath][errno].extend(eval)

                else:
                    self.errors[limp][ltype][lpath][errno].append(eval)

        if not (0<= importance < len(self.imp) - 1):
            importance = 0

        if not etype in self.etypes:
            etype = "other"

        if etype == 'type_errors':
            if not isinstance(err, list):
                return

            for e in err:
                if e['error'] == 0:
                    continue

                if e['error'] & 7:
                    ve = e['error'] & 7
                    if ve in (1, 2, 4):
                        append_error(importance, etype, e['path'], {'type':e['type'], 'value': deepcopy(e['value'])}, ve)

                    if ve == 3:
                        append_error(importance, etype, e['path'], {'length':e['length'], 'value': deepcopy(e['value'])}, ve)

                if e['error'] & 8:
                    # Key type error
                    for k, v in e['keyerrs'].items():
                        if v in self.lookup_lists.keys():
                            append_error(importance, etype, e['path'], {'type':v, 'value': k}, 16)

                        else:
                            append_error(importance, etype, e['path'], {'type':v, 'value': k}, 8)

        elif etype == 'either_test':
            if len(err['list'][0][1]) == 0 and len(err['list'][0][2]) == 0 and len(err['list'][0][3]) == 0:
                append_error(0, etype, vpath, err)

            else:
                append_error(1, etype, vpath, err)

        elif etype in ('missing_keys', 'unused_keys', 'unknown_keys'):
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
        self.load_lookup_lists(struct_name, testval)
        include_list = data_value([struct_name, "include"], self.struct_tree)
        if isinstance(include_list, (str,unicode)):
            include_list = [include_list]

        if isinstance(include_list, list):
            for include_struct in include_list:
                if include_struct in self.struct_list:
                    self.init_struct(include_struct, testval)
                    self.struct_tree[struct_name] = self.merge_structs(self.struct_tree[struct_name], self.struct_tree[include_struct])

            if "include" in self.struct_tree[struct_name].keys():
                del self.struct_tree[struct_name]["include"]

    def load_lookup_lists(self, struct_name, testval):
        # Load reference lists from the grabber_datafile
        def fill_list():
            if v == 'keys' and isinstance(dset, dict):
                lookup_lists.extend(dset.keys())

            if v == 'intkeys' and isinstance(dset, dict):
                for k in dset.keys():
                    try:
                        s = int(k)
                        lookup_lists.append(k)
                        lookup_lists.append(s)
                        int_list.append(s)

                    except:
                        pass

            if v == 'values':
                if isinstance(dset, dict):
                    lookup_lists.extend(dset.values())

                if isinstance(dset, list):
                    lookup_lists.extend(dset[:])

        for lname, ldef in data_value([struct_name, "lookup_lists"],self.struct_tree, dict).items():
            int_list = []
            if is_data_value(["list"], ldef, list):
                lookup_lists = data_value(["list"], ldef, list)

            else:
                lookup_lists = []
                v = data_value(["values"], ldef, str)
                level = data_value(["level"], ldef, int, 1)
                kw = data_value(["keyword"], ldef, list)
                if level == 1:
                    dset = data_value(kw, testval)
                    fill_list()

                elif level == 2:
                    bset = data_value(kw, testval)
                    if isinstance(bset, list):
                        for dset in bset:
                            fill_list()

                    elif isinstance(bset, dict):
                        for dset in bset.values():
                            fill_list()

            if data_value(["append"], ldef, bool, False) and is_data_value (lname, self.lookup_lists, list):
                for item in set(lookup_lists):
                    if not item in self.lookup_lists[lname]:
                        self.lookup_lists[lname].append(item)

            else:
                self.lookup_lists[lname] = list(set(lookup_lists))

            if len(int_list) > 0:
                int_name = 'int-%s' % lname
                if data_value(["append"], ldef, bool, False) and is_data_value (int_name, self.lookup_lists, list):
                    for item in set(int_list):
                        if not item in self.lookup_lists[int_name]:
                            self.lookup_lists[int_name].append(item)

                else:
                    self.lookup_lists[int_name] = list(set(int_list))

        for k, ldef in data_value([struct_name, "reference_values"],self.struct_tree, dict).items():
            kw = data_value(["keyword"], ldef, list)
            vtype = data_value(["type"], ldef, str)
            default = data_value(["default"], ldef, default = None)
            if vtype == 'integer':
                self.reference_values[k] = data_value(kw, testval, int, default = default)

            elif vtype == 'string':
                self.reference_values[k] = data_value(kw, testval, (str, unicode), default = default)

            elif vtype == 'boolean':
                self.reference_values[k] = data_value(kw, testval, bool, default = default)

            else:
                self.reference_values[k] = data_value(kw, testval, default = default)

        self.add_extra_lookup_lists(struct_name)

    def merge_structs(self, struct1, struct2):
        # struct2 is given through the "include" keyword in struct1
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

        def merge_dict(sstruct1, sstruct2, stype):
            mstruct = {}
            addedlist = []
            mstruct['type'] = stype
            if is_data_value('base-type', struct2, str):
                mstruct['base-type'] = struct2['base-type']

            elif is_data_value('base-type', struct1, str):
                mstruct['base-type'] = struct1['base-type']

            # We only use an 'either' in sstruct2 if not present in sstruct1
            if is_data_value('either', sstruct1, list):
                for item in data_value(['either'], sstruct1, list):
                    addedlist.extend(key_list(item))
                addedlist = list(set(addedlist))
                mstruct['either'] = deepcopy(sstruct1['either'])

            elif is_data_value('either', sstruct2, list):
                for item in data_value(['either'], sstruct2, list):
                    addedlist.extend(key_list(item))
                addedlist = list(set(addedlist))
                mstruct['either'] = deepcopy(sstruct2['either'])

            for imp in range(1, len(self.imp)):
                g = self.imp[imp]
                for skey in data_value(g, sstruct1, dict).keys():
                    if skey in addedlist:
                        continue

                    addedlist.append(skey)
                    if not g in mstruct.keys():
                        mstruct[g] = {}

                    mstruct[g][skey] = deepcopy(sstruct1[g][skey])

            for g in self.key_lists:
                for skey in data_value(g, sstruct1, list):
                    if skey in addedlist:
                        continue

                    addedlist.append(skey)
                    if not g in mstruct.keys():
                        mstruct[g] = []

                    mstruct[g].append(skey)

            for imp in range(1, len(self.imp)):
                g = self.imp[imp]
                for skey in data_value(g, sstruct2, dict).keys():
                    if skey in addedlist:
                        continue

                    addedlist.append(skey)
                    if not g in mstruct.keys():
                        mstruct[g] = {}

                    mstruct[g][skey] = deepcopy(sstruct2[g][skey])

            for g in self.key_lists:
                for skey in data_value(g, sstruct2, list):
                    if skey in addedlist:
                        continue

                    addedlist.append(skey)
                    if not g in mstruct.keys():
                        mstruct[g] = []

                    mstruct[g].append(skey)

            for k in (('keys', None), ('allowed', str), ('length', int)):
                if is_data_value(k[0], sstruct1, k[1], True):
                    mstruct[k[0]] = deepcopy(sstruct1[k[0]])

                elif is_data_value(k[0], sstruct2, k[1], True):
                    mstruct[k[0]] = deepcopy(sstruct2[k[0]])

            return mstruct

        def merge_list(sstruct1, sstruct2, stype):
            mstruct = {}
            mstruct['type'] = stype
            if is_data_value('base-type', struct2, str):
                mstruct['base-type'] = struct2['base-type']

            elif is_data_value('base-type', struct1, str):
                mstruct['base-type'] = struct1['base-type']

            for k in (('items', list), ('reverse_items', list), ('length', int)):
                if is_data_value(k[0], sstruct1, k[1], True):
                    mstruct[k[0]] = deepcopy(sstruct1[k[0]])

                elif is_data_value(k[0], sstruct2, k[1], True):
                    mstruct[k[0]] = deepcopy(sstruct2[k[0]])

            return mstruct

        if isinstance(struct2, list):
            return deepcopy(struct2)

        if data_value('type', struct2) in ('dict', 'numbered dict'):
            return merge_dict(struct1, struct2, data_value('type', struct2))

        if data_value('type', struct2)== 'list':
            return merge_list(struct1, struct2, data_value('type', struct2))

        return deepcopy(struct2)

    def test_file(self, file_name, struct_name = None):
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
        if data_value('type', tstruct, str) in ('dict', 'numbered dict'):
            self.test_dict(tstruct, self.testfile)

        elif data_value('type', tstruct, str) == 'list':
            self.test_list(tstruct, self.testfile)

        self.report_errors()
        return 0

    def test_struct(self, struct_name, testval):
        self.init_struct(struct_name, testval)
        sstruct = self.struct_tree[struct_name]
        if self.test_type(sstruct, testval) > 0:
            return False

        if self.trep['type'] in ('dict', 'numbered dict'):
            for k in data_value(['required'], sstruct, dict).keys():
                if not k in testval.keys():
                    return False

            for k in data_value(['forbidden_keys'], sstruct, list):
                if k in testval.keys():
                    return False

            for k in data_value(['conditional'], sstruct, dict).keys():
                revkey = data_value(['conditional', k, 'reference_key'], sstruct, str)
                revval = data_value(['conditional', k, 'value'], sstruct, list)
                impfalse = data_value(['conditional', k, 'false'], sstruct, int, 3)
                imptrue = data_value(['conditional', k, 'true'], sstruct, int, impfalse)
                if revkey in self.reference_values.keys() and self.reference_values[revkey] in revval:
                    imp = imptrue

                else:
                    imp = impfalse

                if imp == 1 and not k in testval.keys():
                    return False

                if imp == -2 and k in testval.keys():
                    return False

        #~ elif self.trep['type'] == 'list':
            #~ for index in range(len(data_value(['items'], sstruct, list))):
                #~ if index < len(testval):

        return True

    def test_type(self, dtypes, val):
        def set_error(err):
            if len(keyerrs) > 0:
                err += 8

            self.trep = {'error': err,
                            'type': dtype,
                            'value': val,
                            'length':data_value("length", dtypes, int, 0),
                            'keyerrs':keyerrs}
            return err

        if isinstance(dtypes, (list, str, unicode)):
            dtypes = {'type': dtypes}

        if isinstance(dtypes, dict):
            dtype = data_value("type", dtypes)

        else:
            return set_error(0)

        keyerrs = {}
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
            if is_data_value("keys", dtypes, str, True):
                ktype = data_value("keys", dtypes, str)

            elif is_data_value("keys", dtypes, list, True):
                ktype = data_value("keys", dtypes, list)

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

        elif dtype == 'none':
            if val != None:
                # Wrong type
                return set_error(1)

        elif dtype == 'url':
            if not isinstance(val, (str, unicode)):
                # Wrong type
                return set_error(1)

        elif dtype == 'date':
            pass

        elif dtype in self.lookup_lists.keys():
            if not val in self.lookup_lists[dtype]:
                # Wrong type
                return set_error(2)

        elif dtype in self.struct_list:
             if not self.test_struct(dtype, val):
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
                struct = self.struct_tree[vr['type']]
                stype = data_value('type', struct, str)
                if stype in ('dict', 'numbered dict'):
                    self.test_dict(struct, testval, vpath)

                elif stype == 'list':
                    self.test_list(struct, testval, vpath)

            if vr['type'] in ('dict', 'numbered dict'):
                self.test_dict(typelist[0], testval, vpath)
                if len(typelist) > 1:
                    # We check the next in the list
                    for k, v in testval.items():
                        if re.match('--.*?--', k):
                            continue

                        spath = [] if vpath == None else copy(vpath)
                        spath.append(k)
                        errlist.extend(self.test_typelist( typelist[1:], v, spath))

            elif vr['type'] == 'list':
                self.test_list(typelist[0], testval, vpath)
                if len(typelist) > 1:
                    # We check the next in the list
                    for item in range(len(testval)):
                        spath = [] if vpath == None else copy(vpath)
                        spath.append(item)
                        errlist.extend(self.test_typelist( typelist[1:], testval[item], spath))

        return errlist

    def test_dict(self, sstruct, testval, vpath=None):
        teststruct = sstruct
        if is_data_value('either', sstruct, list) and len(data_value('either', sstruct, list)) > 0:
            testlist = []
            for item in range(len(sstruct['either'])):
                missing = []
                wrongtype = {}
                forbidden = []
                # Test for required keys and their type
                for k, v in data_value(['either', item, 'required'], sstruct, dict).items():
                    if not k in testval.keys():
                        missing.append(k)
                        continue

                    typelist = data_value(["types", 0], v)
                    if self.test_type(typelist, testval[k]) > 0:
                        wrongtype[k] = self.trep.copy()

                # We test for the presence of forbidden keys
                for k in data_value(['either', item, 'forbidden_keys'], sstruct, list):
                    if k in testval.keys():
                        forbidden.append(k)

                for k, v in data_value(['either', item, 'conditional'], sstruct, dict).items():
                    revkey = data_value(['either', item, 'conditional', k, 'reference_key'], sstruct, str)
                    revval = data_value(['either', item, 'conditional', k, 'value'], sstruct, list)
                    impfalse = data_value(['either', item, 'conditional', k, 'false'], sstruct, int, 3)
                    imptrue = data_value(['either', item, 'conditional', k, 'true'], sstruct, int, impfalse)
                    if revkey in self.reference_values.keys() and self.reference_values[revkey] in revval:
                        imp = imptrue

                    else:
                        imp = impfalse

                    if imp == 1:
                        if not k in testval.keys():
                            missing.append(k)
                            continue

                        typelist = data_value(["types", 0], v)
                        if self.test_type(typelist, testval[k]) > 0:
                            wrongtype[k] = self.trep.copy()

                    if imp == -2 and k in testval.keys():
                        forbidden.append(k)

                testlist.append((item, missing, forbidden, wrongtype))

            # We Sort to get the most likely one ( with in order the least missing keys, the least forbidden keys and the least faulty keys)
            testlist.sort(key=lambda k: (len(k[1]), len(k[2]), len(k[3])))
            text = data_value('report', sstruct, str)
            self.add_error({'text': text, 'list': testlist}, 'either_test', vpath)

            # We use alternative testlist[0][0]
            item = testlist[0][0]
            teststruct = deepcopy(sstruct)
            for imp in range(1, len(self.imp)):
                dset = self.imp[imp]
                if dset in data_value(['either', item], sstruct, dict).keys():
                    if not dset in teststruct.keys():
                        teststruct[dset] = {}

                    for dkey in data_value(['either', item, dset], sstruct, dict).keys():
                        teststruct[dset][dkey] = data_value(['either', item, dset, dkey], sstruct)

            for dset in self.key_lists:
                if dset in data_value(['either', item], sstruct, dict).keys():
                    if not dset in teststruct.keys():
                        teststruct[dset] = copy(data_value(['either', item, dset], sstruct, list))

                    else:
                        teststruct[dset].extend(data_value(['either', item, dset], sstruct, list))

            if 'allowed' in data_value(['either', item], sstruct, dict).keys():
                teststruct['allowed'] = copy(data_value(['either', item, 'allowed'], sstruct, list))

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
                if not dkey in testval.keys():
                    missing[imp].append(dkey)
                    continue

                # get the type definition list for this key
                typelist = data_value([dset, dkey,"types"], teststruct, default = [])
                spath = [] if vpath == None else copy(vpath)
                spath.append(dkey)
                self.add_error(self.test_typelist(typelist, testval[dkey], spath), 'type_errors', spath, imp)

        for dkey in data_value(['conditional'], teststruct, dict).keys():
            revkey = data_value(['conditional', dkey, 'reference_key'], teststruct, str)
            revval = data_value(['conditional', dkey, 'value'], teststruct, list)
            impfalse = data_value(['conditional', dkey, 'false'], teststruct, int, 3)
            imptrue = data_value(['conditional', dkey, 'true'], teststruct, int, impfalse)
            if revkey in self.reference_values.keys() and self.reference_values[revkey] in revval:
                imp = imptrue

            else:
                imp = impfalse

            if imp in range(1, len(self.imp) - 1):
                known_keys.append(dkey)
                dset = self.imp[imp]
                # test on the presence of defined keys
                if not dkey in testval.keys():
                    if not imp in missing.keys():
                        missing[imp] = []

                    missing[imp].append(dkey)
                    continue

                # get the type definition list for this key
                typelist = data_value([dset, dkey,"types"], teststruct, default = [])
                # set the context
                spath = [] if vpath == None else copy(vpath)
                spath.append(dkey)
                self.add_error(self.test_typelist(typelist, testval[dkey], spath), 'type_errors', spath, imp)

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
        for dkey in testval.keys():
            if not dkey in known_keys and not re.match('--.*?--', dkey):
                if dkey in forbidden_list or dkey in unused_list:
                        unused.append(dkey)

                elif data_value("allowed", teststruct, str) != 'all':
                    for dset in range(1, len(self.imp)):
                        if self.imp[dset] in teststruct:
                            unknown.append(dkey)
                            break

        # Report found errors
        for imp in range(1, len(self.imp) - 1):
            if len(missing[imp]) > 0:
                self.add_error(missing[imp], 'missing_keys', vpath, imp)

        for eset in ((unused, 'unused_keys'),(unknown, 'unknown_keys')):
            if len(eset[0]) > 0:
                self.add_error(eset[0], eset[1],vpath)

    def test_list(self, sstruct, testval, vpath=None):
        for index in range(len(data_value(['items'], sstruct, list))):
            if index < len(testval):
                typelist = data_value(['items', index], sstruct)
                spath = [] if vpath == None else copy(vpath)
                spath.append(index)
                self.add_error(self.test_typelist(typelist, testval[index], spath), 'type_errors', spath,1)

        for index in range(len(data_value(['reverse_items'], sstruct, list))):
            if index < len(testval):
                typelist = data_value(['reverse_items', index], sstruct)
                spath = [] if vpath == None else copy(vpath)
                spath.append(-index-1)
                self.add_error(self.test_typelist(typelist, testval[-index-1], spath), 'type_errors', spath,1)

    def report_errors(self):
        def type_err_string(ttype, tvals):
            if ttype ==1:
                return 'Wrong value type, should be: "%s"' % tvals['type']

            elif ttype ==2:
                return 'Value not in "%s"' % self.lookup_lists[tvals['type']]

            elif ttype == 3:
                return 'Value not of length: %s' % tvals['length']

            elif ttype == 4:
                return 'Value does not match a: "%s"' % tvals['type'][7:]

            if ttype == 8:
                return 'Key %s should be of type: "%s"' % (tvals['value'], tvals['type'])

            elif ttype == 16:
                return 'Key %s not in "%s"' % (tvals['value'], self.lookup_lists[tvals['type']])

        def header_string(imp):
            if etype in(0, 1):
                return 'The following %s\n' % (etext[etype] % (self.imp[imp]))

            if etype in (2, 3):
                return 'The following %s\n' % (etext[etype])

            if etype == 4:
                return 'From the following "either" selections the first is selected%s:\n' % (etext[etype+imp])

        def format_err(val, imp):
            if len(val) == 0:
                return

            self.report(header_string(imp))
            klist = val.keys()
            klist.sort()
            for k in klist:
                self.report('  at path: %s\n' % k)
                if etype in (1, 2, 3):
                    val[k].sort()
                    for key in val[k]:
                        self.report('    %s\n' % key)

                if etype == 0:
                    for e in (1, 2, 3, 4, 8, 16):
                        if e in val[k].keys() and len(val[k][e]) > 0:
                            for vt in val[k][e]:
                                self.report('    %s\n' % (type_err_string(e, vt)))

                if etype == 4:
                    self.report('    %s\n' % val[k]['text'])
                    for item in val[k]['list']:
                        self.report('    Option %s:\n' % (item[0]))
                        if len(item[1]) == 0:
                            self.report('      With no missing keys\n')

                        else:
                            kstr = '      With the required '
                            for k in item[1]:
                                kstr = '%s"%s", ' % (kstr, k)

                            self.report('%s key(s) missing ' % (kstr[:-2]))

                        if len(item[2]) == 0:
                            self.report('      With no forbidden keys\n')

                        else:
                            kstr = '      With the forbidden '
                            for k in item[2]:
                                kstr = '%s"%s", ' % (kstr, k)

                            self.report('%s key(s) present ' % (kstr[:-2]))

                        if len(item[3]) == 0:
                            self.report('      With no errors\n')

                        else:
                            self.report('      With some errors\n')

        etext = ('errors in %s keys are encountered',
                '%s keys are not set',
                'known keys are found that will not be used',
                'keys are not recognized',
                '',
                ', that give errors')

        if len(self.errors[self.imp[1]]) == 0:
            self.log('And no serieus errors were found!\n\n')

        else:
            self.log('And some serious errors were encountered:\n\n')

        for etype in (1, 4, 0):
            format_err(data_value([self.imp[1], self.etypes[etype]], self.errors, dict), 1)

        for etype in (0, 1):
            for imp in (2, 3):
                format_err(data_value([self.imp[imp], self.etypes[etype]], self.errors, dict), imp)

        for etype in (2, 3, 4):
            format_err(data_value([self.imp[0], self.etypes[etype]], self.errors, dict), 0)

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

            except(ValueError):
                self.log( '  %s\n' % e)

            except:
                traceback.print_exc()

        # We try to download unless the only_local_files flag is set
        if not self.only_local_files:
            try:
                txtheaders = {'Keep-Alive' : '300'}
                url = 'https://raw.githubusercontent.com/tvgrabbers/sourcematching/master/json_struct'
                url = '%s/%s.json' % (url, name)
                self.log('  trying to download: "%s.json"\n' % name)
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

            except(ValueError):
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


