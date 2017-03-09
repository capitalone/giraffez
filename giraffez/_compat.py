# -*- coding: utf-8 -*-
#
# Copyright 2016 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3
identity = lambda x: x
default_encoding = 'UTF-8'

if PY3:
    # reduce no longer a built-in
    from functools import reduce
    import io as StringIO
    import queue
    import decimal

    basestring = (str, bytes)
    xrange = range
    iterbytes = identity

    def ensure_bytes(s):
        if type(s) == str:
            return bytes(s, default_encoding)
        elif type(s) == float:
            return ensure_bytes(str(s))
        elif type(s) == int:
            return ensure_bytes(str(s))
        else:
            return bytes(s)

    def ensure_str(s):
        if type(s) == bytes:
            return s.decode(default_encoding)
        elif type(s) == str:
            return s
        else:
            return s

    def escape_string(s):
        if isinstance(s, str):
            s = s.encode('unicode_escape').decode(default_encoding)
        elif isinstance(s, bytes):
            s = s.encode('unicode_escape')
        return s

    def unescape_string(s):
        if isinstance(s, str):
            s = s.encode(default_encoding).decode('unicode_escape')
        elif isinstance(s, bytes):
            s = s.decode('unicode_escape')
        return s
else:
    try:
        import cdecimal as decimal
    except ImportError:
        import decimal
    try:
        import cStringIO as StringIO
    except ImportError:
        import StringIO
    import Queue as queue

    # Bytes object in Py3 does not require a call to ord to return a
    # numeric type 
    iterbytes = lambda x: (ord(c) for c in x)

    input = raw_input

    ensure_bytes = lambda s: bytes(s)
    ensure_str = identity

    def escape_string(s):
        if isinstance(s, basestring):
            s = s.encode('string_escape')
        return s

    def unescape_string(s):
        if isinstance(s, basestring):
            s = s.decode('string_escape')
        return s
