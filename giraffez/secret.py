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
