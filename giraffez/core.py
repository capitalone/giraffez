# -*- coding: utf-8 -*-

import sys

GIRAFFE_NOT_FOUND = False
TDCLI_NOT_FOUND = False
TPT_NOT_FOUND = False
try:
    from . import _encoder
    try:
        from ._cli import Cmd
    except ImportError:
        TDCLI_NOT_FOUND = True
    try:
        from ._tpt import Export
        from ._tpt import Load
    except ImportError:
        TPT_NOT_FOUND = True
except ImportError:
    GIRAFFE_NOT_FOUND = True


from .config import *
from .constants import *
from .encrypt import *
from .errors import *
from .io import *
from .logging import *
from .parser import *
from .utils import *


__all__ = ['GIRAFFE_NOT_FOUND', 'MainCommand', 'TDCLI_NOT_FOUND', 'TPT_NOT_FOUND']


class MainCommand(Command):
    name = "giraffez"
    description = ascii_giraffe
    usage = "giraffez [args] <command> [options]"

    global_arguments = [
        Argument("-D", "--dsn", default=None, group="global options"),
        Argument("-c", "--conf", default=None, group="global options"),
        Argument("-k", "--key", default=None, group="global options"),
        Argument("-v", "--verbose", action="count", default=0, dest="log_level",
            group="global options"),
        Argument("-h", "--help", default=False, help="Print usage", group="global options"),
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
        "commandline.LoadCommand", 
        "commandline.MLoadCommand", 
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
                    log.write(MESSAGE_WRITE_DEFAULT.format(args.conf))
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
        except InvalidCredentialsError as error:
            if args.protect:
                Config.lock_connection(args.conf, args.dsn)
            raise error
