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

import textwrap

from .constants import *
from .errors import *

from .logging import log

from ._compat import *


def escape_quotes(s):
    return s.replace("'", "''")

def format_indent(body, indent=" ", initial=""):
    wrapper = textwrap.TextWrapper(initial_indent=initial,
        subsequent_indent=indent)
    return wrapper.fill(body)

def format_table(lines, header=True):
    widths = [0] * len(lines[0])
    for line in lines:
        widths = [max(a, len(str(b))) for a, b in zip(widths, line)]
    output = []
    for i, line in enumerate(lines):
        output.append(" | ".join([str(field).ljust(w) for w, field in zip(widths, line)]))
        if i == 0 and header:
            output.append("-"*len(output[0]))
    return "\n".join(output)

def safe_name(s):
    return s.replace(" ", "_").lower()

def quote_string(s, quote_character="'"):
    return "{0}{1}{0}".format(quote_character, s)

def replace_cr(s):
    if not isinstance(s, str):
        return s
    return s.strip().replace('\r', '\n')

def truncate(s, n=7, c=70, placeholder="..."):
    if not isinstance(s, basestring):
        raise GiraffeError("Cannot truncate non-string value")
    lines = s.split('\n')
    line_count = len(lines)
    char_count = len(s)
    if char_count < (n * c) and line_count < n:
        return s
    if char_count > (n * c):
        s = s[0:n*c]
        lines = s.split('\n')
    lines = "\n".join(lines[0:n])
    return "{} {}".format(lines, placeholder)
