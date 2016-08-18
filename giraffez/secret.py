# -*- coding: utf-8 -*-

from .config import *

from ._compat import *


__all__ = ['Secret']


class Secret(object):
    def __init__(self, conf=None, mode="r", key_file=None):
        self.config = Config(conf, mode, key_file)

    def get(self, key):
        if not key.startswith("secure.") and not key.startswith("connections."):
            key = "secure.{0}".format(key)
        value = self.config.get_value(key)
        if not isinstance(value, basestring):
            value = None
        return value

    def set(self, key, value):
        if not key.startswith("secure."):
            key = "secure.{0}".format(key)
        self.config.set_value(key, value)
        self.config.write()

    def __call__(self, key):
        keys = [x.strip() for x in key.split(",")]
        if len(keys) == 1:
            return self.get(keys[0])
        return [self.get(k) for k in keys]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        pass
