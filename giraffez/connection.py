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

import collections

from .constants import *
from .errors import *

from ._teradata import InvalidCredentialsError
from .config import Config
from .logging import log
from .utils import show_warning, suppress_context


__all__ = ['Connection', 'Context']


class Context(object):
    __instance__ = None

    def __new__(cls, *args, **kwargs):
        allow_without_context = kwargs.get('allow_without_context')
        if allow_without_context or getattr(Context, '_testing', False):
            if allow_without_context:
                kwargs.pop('allow_without_context')
            return cls.__instance__(*args, **kwargs)
        return super(Context, cls).__new__(cls)

    def __init__(self, *args, **kwargs):
        if self.__instance__ is None:
            raise GiraffeError("Context must specify an instance")
        self._args = args
        self._kwargs = kwargs

    def __getattr__(self, value):
        raise GiraffeError("""{} is being used outside of a with context, as of
giraffez version 2.0.6 this behavior is officially deprecated.  Please see
http://capitalone.io/giraffez/FAQ.html#with-context for more information.""".format(self.__instance__.__name__))

    def __enter__(self):
        self.__instance_object = self.__instance__(*self._args, **self._kwargs)
        return self.__instance_object.__enter__()

    def __exit__(self, exc_type, exc, exc_tb):
        self.__instance_object.__exit__(exc_type, exc, exc_tb)


class Connection(object):
    def __init__(self, host=None, username=None, password=None, log_level=INFO, config=None,
            key_file=None, dsn=None, protect=False, silent=False):
        #: Log level initially set to SILENCE to ensure that using the
        #: Python API does not produce log output unless expressly set
        log.level = log_level

        self.config = config
        self.key_file = key_file
        self.dsn = dsn
        self.protect = protect
        self.silent = silent

        #: Stores options for log output
        self.options = ConnectionOptions()

        if host is None or username is None or password is None:
            if host or username or password:
                show_warning(("You must set all arguments to overide default (host, username, "
                    " password)"), UserWarning)
            with Config(config, key_file=key_file) as c:
                conn = c.get_connection(dsn)
            self.dsn = conn.get("name")
            host = conn.get("host", None)
            if host is None:
                raise suppress_context(ConfigurationError("Connection '{}' missing host value".format(self.dsn)))
            username = conn.get("username")
            if username is None:
                raise suppress_context(ConfigurationError("Connection '{}' missing username value".format(self.dsn)))
            password = conn.get("password")
            if password is None:
                raise suppress_context(ConfigurationError("Connection '{}' missing password value".format(self.dsn)))
        try:
            if not self.silent:
                log.info("Connection", "Connecting to data source '{}' ...".format(self.dsn))
            self._connect(host, username, password)
            if not self.silent:
                log.info("Connection", "Connection to '{}' established successfully.".format(self.dsn))
        except InvalidCredentialsError as error:
            if self.protect:
                Config.lock_connection(self.config, self.dsn, self.key_file)
            raise error

    def _close(self, exc=None):
        pass

    def _connect(self, host, username, password):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        if not self.silent:
            log.info("Connection", "Closing Teradata connection ...")
        self._close(exc)
        if not self.silent:
            log.info("Connection", "Connection to '{}' closed.".format(self.dsn))


class ConnectionOptions(object):
    def __init__(self):
        self._options = collections.OrderedDict()

    @property
    def width(self):
        if not self._options.keys():
            return 0
        return len(max(self._options, key=lambda x: len(x)))

    def clear(self):
        self._options = {}

    def set(self, name, value, priority=1):
        self._options[name] = (priority, value)

    def unset(self, name, value):
        return self._options.pop(name, None)

    def __call__(self, name, value, priority=1):
        return self.set(name, value, priority)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        option = '  {}{} => "{}"'
        r = []
        for k, (priority, v) in sorted(self._options.items(), key=lambda x: str(x[1])):
            offset_width = self.width - len(k)
            value = str(v).replace('\n', '\n    ')
            r.append(option.format(k, " "*offset_width, value))
        return "\n".join(r)
