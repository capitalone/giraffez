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

import re


__all__ = ['parse_statement', 'prepare_statement', 'remove_curly_quotes']


def parse_statement(s):
    NORMAL = 1
    COMMENT = 2
    QUOTE = 3
    BLOCK_COMMENT = 4

    output = []
    statement = ""
    mode = NORMAL

    for i, c in enumerate(s):
        if mode == COMMENT:
            if c == "\n":
                mode = NORMAL
                statement += " "
        elif mode == BLOCK_COMMENT:
            if c == "/" and i > 0 and s[i-1] == "*":
                mode = NORMAL
        elif mode == QUOTE:
            if c == "'":
                mode = NORMAL
            statement += c
        elif c == "'":
            mode = QUOTE
            statement += c
        elif c == ";":
            output.append(statement.strip())
            statement = ""
        elif c == "-" and i + 1 < len(s) and s[i+1] == "-":
            mode = COMMENT
        elif c == "/" and i + 1 < len(s) and s[i+1] == "*":
            mode = BLOCK_COMMENT
        elif c == "\n" or c == "\r":
            statement += " "
        else:
            statement += c.lower()
    
    statement = statement.strip()
    if statement:
        output.append(statement)
        
    return output

def prepare_statement(statement):
    statement = remove_curly_quotes(statement)
    statements = parse_statement(statement)
    return ";".join(statements)

def remove_curly_quotes(statement):
    return re.sub('(’|‘|“|”)', "'", statement)
