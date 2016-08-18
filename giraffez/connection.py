# -*- coding: utf-8 -*-

GIRAFFE_NOT_FOUND = False
try:
    from . import _encoder
except ImportError:
    GIRAFFE_NOT_FOUND = True

import collections

from .config import *
from .constants import *
from .errors import *
from .logging import *
from .utils import *


__all__ = ['Connection']


class Connection(object):
    def __init__(self, host=None, username=None, password=None, log_level=INFO, config=None,
            key_file=None, dsn=None, protect=False, mload_session=False):
        if GIRAFFE_NOT_FOUND:
            raise GiraffeNotFound("giraffez module was not compiled with package")
        #: Log level initially set to SILENCE to ensure that using the
        #: Python API does not produce log output unless expressly set
        log.level = log_level

        self.config = config
        self.dsn = dsn
        self.protect = protect

        self.host = host

        #: Stores options for log output
        self.options = ConnectionOptions()

        #: Attribute identifies mload sessions to suppress duplicate log
        #: output since mload requires two Connection objects
        self.mload_session = mload_session

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
            if not self.mload_session:
                log.info("Connection", "Connecting to data source '{}' ...".format(self.dsn))
            self._connect(host, username, password)
            if not self.mload_session:
                log.info("Connection", "Connection to '{}' established successfully.".format(self.dsn))
        except InvalidCredentialsError as error:
            if self.protect:
                Config.lock_connection(self.config, self.dsn)
            raise error

    def _connect(self, host, username, password):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        if not self.mload_session:
            log.info("Connection", "Closing Teradata connection ...")
        self.close()
        if not self.mload_session:
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
