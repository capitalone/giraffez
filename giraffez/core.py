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

import sys

from .constants import *
from .errors import *

from . import _teradata
from . import _teradatapt
from .config import Config, message_write_default
from .encrypt import create_key_file
from .io import home_file
from .logging import colors, log
from .parser import Argument, Command
from .utils import prompt_bool


class MainCommand(Command):
    name = "giraffez"
    description = ascii_giraffe
    usage = "giraffez [args] <command> [options]"

    global_arguments = [
        Argument("-D", "--dsn"),
        Argument("-c", "--conf"),
        Argument("-k", "--key"),
        Argument("-v", "--verbose", action="count", default=0, dest="log_level"),
        Argument("-h", "--help", default=False, help="Print usage"),
    ]

    arguments = [
        Argument("-V", "--version", action="version", help="Print version"),
        Argument("--protect", default=False, help=("Use connection locking to protect Teradata "
            "accounts from locking from bad attempts")),
    ]

    commands = [
        "commandline.CmdCommand",
        "commandline.ConfigCommand",
        "commandline.ExportCommand",
        "commandline.FmtCommand",
        "commandline.InsertCommand",
        "commandline.LoadCommand",
        "commandline.RunCommand",
        "commandline.ShellCommand",
        "commandline.SecretCommand",
    ]

    default = False

    def run(self, test_args=None):
        args = self.parse_args(test_args)
        log.level = args.log_level
        log.debug(sys.version.replace("\n", ""))
        if args.module_name is None:
            self.print_help()
            self.exit()
        if args.module_name == "secret" or (args.module_name == "config" and args.init):
            args.run(args)
            return
        try:
            with Config(conf=args.conf, key_file=args.key) as config:
                colorful = config.get_value("colorful", default=True)
                if not colorful:
                    colors.is_colorful = False
            args.run(args)
        except ConfigNotFound as error:
            log.debug(error)
            if args.conf is None:
                args.conf = home_file(".girafferc")
            if prompt_bool(("Configuration file '{}' not found, would you like to create one now "
                    "using the default template?").format(args.conf), default=True):
                result = Config.write_default(args.conf)
                if result:
                    log.write(colors.green(result))
                    log.write(message_write_default.format(args.conf))
                    self.run()
                else:
                    log.write(colors.fail("Was not successful"))
        except KeyNotFound as error:
            log.debug(error)
            if args.key is None:
                args.key = home_file(".giraffepg")
            if prompt_bool("Key file '{}' not found, would you like to create one now?".format(args.key),
                    default=True):
                create_key_file(args.key)
                log.write("Key file '{}' created successfully.".format(args.key))
                self.run()
            else:
                raise error
        except (_teradata.InvalidCredentialsError, _teradatapt.InvalidCredentialsError) as error:
            if args.protect:
                Config.lock_connection(args.conf, args.dsn, args.key)
            raise error
