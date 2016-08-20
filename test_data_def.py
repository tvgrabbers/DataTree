#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import sys
import test_json_struct

testjson = test_json_struct.test_JSON()
# This value holds the struct files and is None on any load errors
if testjson.struct_tree in (None, []):
    sys.exit(3)

cmd = sys.argv
if len(cmd) < 2:
    testjson.log('Please give the name of the json file to test.\n')
    sys.exit(1)

sys.exit(testjson.test_file(cmd[1]))
