# -*- coding: utf-8 -*-
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

    basestring = (str, bytes)
    xrange = range
    iterbytes = identity
    filter_list = lambda a, b: list(filter(a, b))

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

    def unescape_string(s):
        if isinstance(s, str):
            s = s.encode(default_encoding).decode('unicode_escape')
        elif isinstance(s, bytes):
            s = s.decode('unicode_escape')
        return s
else:
    try:
        import cStringIO as StringIO
    except ImportError:
        import StringIO
    import Queue as queue

    # Bytes object in Py3 does not require a call to ord to return a
    # numeric type 
    iterbytes = lambda x: (ord(c) for c in x)
    filter_list = filter

    input = raw_input

    ensure_bytes = lambda s: bytes(s)
    ensure_str = identity

    def unescape_string(s):
        if isinstance(s, basestring):
            s = s.decode('string_escape')
        return s
