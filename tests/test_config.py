# -*- coding: utf-8 -*-

import os
import platform

import pytest
import yaml

import giraffez
from giraffez.constants import *
from giraffez.errors import *
from giraffez.types import Columns
from giraffez.utils import *


@pytest.mark.usefixtures('config', 'tmpfiles')
class TestConfig(object):
    def test_get_set_list_value(self, tmpfiles):
        with giraffez.Config(tmpfiles.conf, "w", tmpfiles.key) as config:
            value = config.get_value("test")
            assert value == {}

            value = config.get_value("connections.default")
            assert value == "db1"

            config.set_value("connections.default", "db2")

            value = config.get_value("connections.default")
            assert value == "db2"

            value = config.list_value(decrypt=False)

    def test_get_multi_value(self, tmpfiles):
        with giraffez.Config(tmpfiles.conf, "w", tmpfiles.key) as config:
            value = config.get_value("connections")

    def test_get_trailing_dot(self, tmpfiles):
        with giraffez.Config(tmpfiles.conf, "w", tmpfiles.key) as config:
            value1 = config.get_value("connections")
            value2 = config.get_value("connections.")
            assert value1 == value2

    def test_unset_value(self, tmpfiles):
        expected_dsn = "db2"
        with giraffez.Config(tmpfiles.conf, "w", tmpfiles.key) as config:
            config.unset_value("connections.db1")
            value = config.get_value("connections.db1")
            assert value == {}

    def test_read_only(self, tmpfiles):
        with pytest.raises(ConfigReadOnly):
            with giraffez.Config(tmpfiles.conf, "r", tmpfiles.key) as config:
                config.set_value("connections.default", "db2")
                config.write()

    def test_config_conf_missing(self, tmpfiles):
        with pytest.raises(ConfigNotFound):
            with giraffez.Config("None", "r", tmpfiles.key) as config:
                pass

    def test_config_key_missing(self, tmpfiles):
        with pytest.raises(KeyNotFound):
            with giraffez.Config(tmpfiles.conf, "r", "None") as config:
                pass

    def test_config_conf_bad_permissions(self, tmpfiles):
        # Tests for permissions on linux or unix-like system only. Windows
        # requires the use of Windows-only APIs to determine and set the
        # permissions on files.
        if platform.system() == 'Windows':
            return
        with pytest.raises(ConfigurationError):
            os.chmod(tmpfiles.conf, 0o655)
            with giraffez.Config(tmpfiles.conf, "r", tmpfiles.key) as config:
                pass

        os.chmod(tmpfiles.conf, 0o600)

    def test_config_key_bad_permissions(self, tmpfiles):
        # Tests for permissions on linux or unix-like system only. Windows
        # requires the use of Windows-only APIs to determine and set the
        # permissions on files.
        if platform.system() == 'Windows':
            return
        with pytest.raises(ConfigurationError):
            os.chmod(tmpfiles.key, 0o655)
            with giraffez.Config(tmpfiles.conf, "r", tmpfiles.key) as config:
                pass

        os.chmod(tmpfiles.key, 0o400)

    def test_config_connections(self, tmpfiles):
        with giraffez.Config(tmpfiles.conf, "r", tmpfiles.key) as config:
            connections = config.connections
            dsn = config.get_connection("db1")
            assert dsn.get("host") == None

    def test_config_lock(self, tmpfiles):
        with giraffez.Config(tmpfiles.conf, "r", tmpfiles.key) as config:
            giraffez.Config.lock_connection(tmpfiles.conf, "db1", key=tmpfiles.key)
            giraffez.Config.lock_connection(tmpfiles.conf, "db1", key=tmpfiles.key)
            with pytest.raises(ConnectionLock):
                giraffez.Config.lock_connection(tmpfiles.conf, "db1", key=tmpfiles.key)
            config.reload()
            lock_value = config.get_value("connections.db1.lock")
            assert lock_value == 2
            giraffez.Config.unlock_connection(tmpfiles.conf, "db1", key=tmpfiles.key)
            config.reload()
            lock_value = config.get_value("connections.db1.lock")
            assert lock_value == {}

    def test_secret_decrypt(self, tmpfiles):
        expected_username = "user123"
        expected_password = "pass456"
        with giraffez.Config(tmpfiles.conf, "w", tmpfiles.key) as config:
            config.set_value("connections.db1.username", expected_username)
            config.set_value("connections.db1.password", expected_password)
            config.write()
        with giraffez.Secret(tmpfiles.conf, "r", tmpfiles.key) as secret:
            username, password = secret("connections.db1.username, connections.db1.password")
            assert expected_username == username
            assert expected_password == password
        with giraffez.Secret(tmpfiles.conf, "w", tmpfiles.key) as secret:
            secret.set("db1.username", expected_username)
            secret.set("db1.password", expected_password)
            username, password = secret("db1.username, db1.password")
            assert expected_username == username
            assert expected_password == password
