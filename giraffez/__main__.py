#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from giraffez.core import MainCommand


def main():
    try:
        MainCommand().run()
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == '__main__':
    main()
