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

import os
import sys
import base64

import yaml

from .errors import *

from .encrypt import Crypto
from .io import file_exists, file_permissions, home_file
from .logging import log, colors

from ._compat import *


__all__ = ['default_config', 'Config', 'message_write_default']


message_write_default = """Successfully created file '{0}'
Set the username and password using the config module:

    giraffez config --set connections.db1.host <host>
    giraffez config --set connections.db1.username <username>
    giraffez config --set connections.db1.password <password>"""

default_config = {
    "connections": {
        "default": "db1",
        "db1": {
            "host": None,
            "username": None,
            "password": None
        }
    },
    "colorful": True
}

def _get_config_file(conf=None):
    if conf is None:
        conf = home_file(".girafferc")
    else:
        conf = os.path.abspath(conf)
    if not file_exists(conf):
        raise ConfigNotFound(("Config file '{}' does not exist and credentials were not "
            "otherwise specified.").format(conf))
    return conf

def _get_key_file(key_file=None):
    if key_file is None:
        key_file = home_file(".giraffepg")
    else:
        key_file = os.path.abspath(key_file)
    if not file_exists(key_file):
        raise KeyNotFound("Key file is required to use giraffez config.")
    return key_file

def _check_file_permissions(path, perms):
    if sys.platform == 'win32':
        return True
    if file_permissions(path) != perms:
        perms = oct(perms).replace('o', '')
        command = colors.fail("chmod {} {}".format(perms, path))
        raise ConfigurationError(("File '{}' not set with secure file-level "
            "permissions. Try running chmod to correct this issue:\n\t{}").format(
            path, command))
    return True

def _read_config_dict(d, fn, path=[], level=0):
    keywords = ["pass", "password", "apikey", "key"]
    level += 1
    for k, v in d.items():
        path.append(k)
        if isinstance(v, dict):
            _read_config_dict(v, fn, path, level)
        else:
            if k in keywords:
                if v and v.startswith("ENCRYPTED:"):
                    v = v.rsplit(":", 1)[1]
                    d[k] = str(fn(v))
            if path:
                if path[0] == "secure" and v.startswith("ENCRYPTED:"):
                    v = v.rsplit(":", 1)[1]
                    d[k] = str(fn(v))
        path.remove(k)


class Config(object):
    """
    An object for reading, writing, and encrypting giraffez configuration
    files (YAML files). Uses an encryption key (required) to encrypt and
    decrypt values keyed as 'password', 'pass', 'apikey', or 'key', as
    well as any values nested under the key 'secure' (e.g. setting
    'secure.teradata.user' will encrypt the value.)

    Implements the :code:`__enter__` and :code:`__exit__` magic methods
    so that it can be used by Python's :code:`with` context-handler:

    .. code-block:: python

       with Config() as conf:
           conn = conf.get_connection() # get default connection
       username = conn.get('username')
       password = conn.get('password')

    :param str conf: A path to the configuration file to open.
        Defaults to ~/.girafferc
    :param str mode: Defaults to 'r' for read-only. If not changed,
        configuration will not be writeable while open.
    :param str key_file: A path to the key file to open.
        Defaults to ~/.giraffepg

    :raises `giraffez.errors.KeyNotFound`: if default key_file is not
        found and a valid one is not provided.
    """

    def __init__(self, conf=None, mode="r", key_file=None):
        if conf is None:
            conf = home_file(".girafferc")
        self._config_file = _get_config_file(conf)
        if key_file is None:
            self._key_file = home_file(".giraffepg")
        self._key_file = _get_key_file(key_file)
        self.mode = mode
        _check_file_permissions(self._config_file, 0o600)
        _check_file_permissions(self._key_file, 0o400)
        self.settings = self.read()
        self.cipher = Crypto.from_key_file(self._key_file)

    @property
    def connections(self):
        """
        Return a :code:`dict` of connections from the configuration settings.

        :raises `giraffez.errors.ConfigurationError`: if connections are not present
        """
        if "connections" not in self.settings:
            raise ConfigurationError("Could not retrieve connections from config file '{}'.".format(self._config_file))
        return self.settings.get("connections")

    def decrypt(self, value, path=[]):
        if isinstance(value, dict):
            _read_config_dict(value, self.cipher.decrypt, path)
        elif isinstance(value, basestring):
            if value.startswith("ENCRYPTED:"):
                value = value.rsplit(":", 1)[1]
                value = self.cipher.decrypt(value)
        return value

    def encrypt(self, value, path):
        if path:
            key = path[-1].lower()
            if key in ["pass", "password", "apikey", "key"] or path[0] in ["secure"]:
                if not isinstance(value, basestring):
                    raise ConfigurationError("Cannot encrypt a value of type '{}'.".format(type(value)))
                value = "ENCRYPTED:{}".format(ensure_str(self.cipher.encrypt(value)))
        return value

    def get_connection(self, dsn=None):
        """
        Retrieve a connection by the given :code:`dsn`, or the default connection.

        :param str dsn: The name of the connection to retrieve. Defaults to :code:`None`,
            which retrieves the default connection.
        :return: A dict of connection settings
        :raises `giraffez.errors.ConfigurationError`: if :code:`dsn` does not exist and a
        :raises `giraffez.errors.ConnectionLock`: if the corresponding connection is currently locked
            default is not set.
        """
        if dsn is None:
            dsn = self.connections.get("default", None)
            if dsn is None:
                raise ConfigurationError("No default DSN set")
        connection = self.connections.get(dsn, None)
        if connection is None:
            raise ConfigurationError("DSN '{}' does not exist".format(dsn))
        connection = self.decrypt(connection.copy())
        if connection.get('lock', 0) > 1:
            raise ConnectionLock(dsn)
        connection['name'] = dsn
        return connection

    def get_value(self, key, default={}, nested=True, decrypt=True):
        """
        Retrieve a value from the configuration based on its key. The key
        may be nested.

        :param str key: A path to the value, with nested levels joined by '.'
        :param default: Value to return if the key does not exist (defaults to :code:`dict()`)
        :param bool decrypt: If :code:`True`, decrypt an encrypted value before returning
            (if encrypted).  Defaults to :code:`True`.
        """
        key = key.lstrip()
        if key.endswith("."):
            key = key[:-1]
        if nested:
            path = key.split(".")
            curr = self.settings
            for p in path[:-1]:
                curr = curr.get(p, {})
            try:
                value = curr[path[-1]]
            except KeyError:
                return default
            value = self.decrypt(value, path)
            return value
        else:
            return self.settings.get(key, default)

    def list_value(self, decrypt=False):
        """
        Return the contents of the configuration as a :code:`dict`. Depending on
        the structure of the YAML settings, the return value may contain nested
        :code:`dict` objects.

        :param bool decrypt: If :code:`True`, decrypt the contents before returning.
        :return: (potentially) nested :code:`dict` of keys and values, as parsed from
            the configuration file YAML contents.
        """
        if decrypt:
            settings = self.decrypt(self.settings)
        else:
            settings = self.settings
        return yaml.dump(settings, default_flow_style=False)

    @classmethod
    def lock_connection(cls, conf, dsn, key=None):
        """
        A class method to lock a connection (given by :code:`dsn`) in the specified
        configuration file. Automatically opens the file and writes to it before
        closing.

        :param str conf: The configuration file to modify
        :param str dsn: The name of the connection to lock
        :raises `giraffez.errors.ConfigurationError`: if the connection does not exist
        """
        with Config(conf, "w", key) as c:
            connection = c.get_connection(dsn)
            if not connection:
                raise ConfigurationError("Unable to lock connection")
            if dsn is None:
                dsn = c.settings["connections"]["default"]
            value = "connections.{}.lock".format(dsn)
            lock = c.get_value("connections.{}.lock".format(dsn), default=0)
            if lock >= 2:
                raise ConnectionLock(dsn)
            lock += 1
            c.set_value("connections.{}.lock".format(dsn), lock)
            c.write()

    def read(self):
        with open(self._config_file, "rb") as f:
            return yaml.load(f)

    def reload(self):
        """
        Re-open and read settings from file.
        """
        self.settings = self.read()

    def set_value(self, key, value):
        """
        Set a value within the configuration based on its key. The key
        may be nested, any nested levels that do not exist prior to the final
        segment of the key path will be created.
        *Note*: In order to write changes to the file, ensure that
        :meth:`~giraffez.config.Config.write` is called prior to exit.

        :param str key: A path to the value destination, with nested levels joined by '.'
        :param value: Value to set at the given key, can be any value that is
            YAML serializeable.
        """
        if key.endswith("."):
            key = key[:-1]
        path = key.split(".")
        curr = self.settings
        for p in path[:-1]:
            if p not in curr:
                curr[p] = {}
            curr = curr[p]
        if not isinstance(curr, dict):
            raise ConfigurationError("Cannot set nested key '{}' in configuration value '{}' (destination is not a dictionary).".format(path[-1], key))
        value = self.encrypt(value, path)
        if value in {'true', 'True'}:
            value = True
        if value in {'false', 'False'}:
            value = False
        curr[path[-1]] = value

    @classmethod
    def unlock_connection(cls, conf, dsn, key=None):
        """
        A class method to unlock a connection (given by :code:`dsn`) in the specified
        configuration file. Automatically opens the file and writes to it before
        closing.

        :param str conf: The configuration file to modify
        :param str dsn: The name of the connection to unlock
        :raises `giraffez.errors.ConfigurationError`: if the connection does not exist
        """
        with Config(conf, "w", key) as c:
            connection = c.connections.get(dsn, None)
            if not connection:
                raise ConfigurationError("Unable to unlock connection")
            if dsn is None:
                dsn = c.settings["connections"]["default"]
            if connection.get("lock", None) is None:
                raise GiraffeError("Connection '{}' is not locked.".format(dsn))
            c.unset_value("connections.{}.lock".format(dsn))
            c.write()

    def unset_value(self, key):
        """
        Remove a value at the given key -- and any nested values --
        from the configuration.
        *Note*: In order to write changes to the file, ensure that
        :meth:`~giraffez.config.Config.write` is called prior to exit.

        :param str key: A path to the value destination, with nested levels joined by '.'
        :raises `giraffez.errors.ConfigurationError`: if the key specifies an invalid path, or does not exist
        """
        if key.endswith("."):
            key = key[:-1]
        path = key.split(".")
        curr = self.settings
        for p in path[:-1]:
            if p not in curr:
                raise ConfigurationError("Cannot unset '{}', nested key '{}' does not exist.".format(key, p))
            curr = curr[p]
        if not isinstance(curr, dict):
            raise ConfigurationError("Cannot unset nested key '{}' in configuration value '{}'.".format(path[-1], key))
        if path[-1] not in curr:
            raise ConfigurationError("Cannot unset '{}', nested key '{}' does not exist.".format(key, path[-1]))
        del curr[path[-1]]

    def write(self, settings=None):
        """
        Save the current configuration to its file (as given by :code:`self._config_file`).
        Optionally, settings may be passed in to override the current settings before
        writing. Returns :code:`None` if the file could not be written to, either due to
        permissions, or if the :class:`~giraffez.config.Config` object has the :code:`mode`
        'r'.

        :param dict settings: Defaults to :code:`None`, if not :code:`None` this will replace
            `self.settings` prior to writing to the file
        """
        if "r" in self.mode:
            raise ConfigReadOnly("Cannot write Config while in 'r' mode")
        try:
            if settings:
                self.settings = settings
            with open(self._config_file, "w") as f:
                f.write(repr(self))
                return repr(self)
        except OSError:
            return None

    @classmethod
    def write_default(self, conf=None):
        """
        A class method to write a default configuration file structure to a file.
        Note that the contents of the file will be overwritten if it already exists.

        :param str conf: The name of the file to write to. Defaults to :code:`None`, for ~/.girafferc
        :return: The content written to the file
        :rtype: str
        """
        if conf is None:
            conf = home_file(".girafferc")
        contents = yaml.dump(default_config, default_flow_style=False)
        with open(conf, "w") as f:
            f.write(contents)
        os.chmod(conf, 0o600)
        return contents

    def __repr__(self):
        return yaml.dump(self.settings, default_flow_style=False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        pass
