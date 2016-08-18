# -*- coding: utf-8 -*-

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
