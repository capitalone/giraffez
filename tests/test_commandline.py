# -*- coding: utf-8 -*-

import os
import platform
import pytest

from giraffez.__main__ import main
from giraffez.core import MainCommand
from giraffez.errors import *
from giraffez.encrypt import *


@pytest.mark.usefixtures('tmpfiles')
class TestCommandLine(object):
    def test_print_help(self, mocker):
        # Should raise a SystemExit and cause help to print
        with pytest.raises(SystemExit):
            main()

    @pytest.mark.usefixtures('config')
    def test_cmd_error(self, mocker, tmpfiles):
        # Windows throws a PermissionError when trying to reopen this
        # file. On other unix-like systems this does not fail because
        # the underlying directory entry is removed but the file
        # remains until no longer in use. For now, this test should
        # be skipped on Windows systems.
        if platform.system() == 'Windows':
            return
        os.remove(tmpfiles.key)
        create_key_file(tmpfiles.key)
        with pytest.raises(TeradataError):
            MainCommand().run(test_args=["cmd", "select * from dbc.dbcinfo", "--conf", tmpfiles.conf, "--key", tmpfiles.key])

    def test_config_not_found(self, mocker, tmpfiles):
        mock_prompt = mocker.patch('giraffez.core.prompt_bool')
        mock_prompt.return_value = False
        mock_prompt.side_effect = ConfigNotFound("Did it!")
        with pytest.raises(ConfigNotFound):
            MainCommand().run(test_args=["cmd", "select * from dbc.dbcinfo", "--conf", tmpfiles.noconf])
