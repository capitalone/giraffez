# -*- coding: utf-8 -*-

import os
import sys
import argparse

from .constants import *
from .errors import *
from .utils import *

from ._compat import *


__all__ = ["Command", "Argument"]


class Argument(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.help = kwargs.get("help", "")
        self.nargs = kwargs.get("nargs", None)
        self.default = kwargs.get("default", None)
        self.action = kwargs.get("action", "store")
        self.type = kwargs.get("type", None)
        self.const = kwargs.get("const", None)
        self.version = kwargs.get("version", "")
        self.group = kwargs.get("group", None)
        self.dest = kwargs.get("dest", None)
        if self.default is False:
            self.action = "store_true"
        if self.default is True:
            self.action = "store_false"


class ArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(ArgumentParser, self).__init__(*args, **kwargs)
        for group in self._action_groups:
            if group.title == "optional arguments":
                group.title = "options"

    def add_or_get_group(self, group_name):
        for group in self._action_groups:
            if group_name == group.title:
                return group
        if group_name == "mutual":
            for group in self._mutually_exclusive_groups:
                return group
            group = self.add_mutually_exclusive_group(required=True)
            return group
        else:
            return self.add_argument_group(group_name)

    def error(self, message):
        if "too few arguments" not in message:
            sys.stderr.write("error: {}\n".format(message))
        self.print_help()
        sys.exit(2)


class Command(ArgumentParser):
    required_arguments = []
    optional_arguments = []

    name = None
    description = None
    usage = None
    help = None

    arguments = []
    global_arguments = []
    commands = []

    default = "run"

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(self.name, description=self.description, usage=self.usage,
            formatter_class=HelpFormatter, add_help=False, conflict_handler='resolve')
        self.has_subcommands = False
        self.register_args(self.arguments)
        self.register_args(self.global_arguments)
        self.set_defaults(name=self.name)
        if self.default:
            if isinstance(self.default, basestring):
                self.set_defaults(run=getattr(self, self.default))
            else:
                self.set_defaults(run=self.default)
        self.set_defaults(required=self.required_arguments)
        self.register_commands()

    def add_command(self, parent):
        if self.has_subcommands is False:
            self.sub_parser = self.add_subparsers(title="commands", dest="module_name",
                metavar=None)
            self.sub_parser._parser_class = ArgumentParser
            self.has_subcommands = True
        new_parser = self.sub_parser.add_parser(parent.prog, usage=parent.usage, help=parent.help,
            description=parent.description, parents=[parent], formatter_class=HelpFormatter,
            add_help=False, conflict_handler='resolve')

    def register_args(self, arguments):
        for argument in arguments:
            if argument.group:
                parser = self.add_or_get_group(argument.group)
            else:
                parser = self
            if argument.default:
                argument.help = "{} [default: '{}']".format(argument.help, argument.default)
            kwargs = {
                'help': argument.help,
                'default': argument.default,
                'action': argument.action
            }

            if argument.action == "store":
                kwargs['nargs'] = argument.nargs
                if argument.type:
                    kwargs['type'] = argument.type
                    kwargs['const'] = argument.const
            elif argument.action == "version":
                kwargs['version'] = " version ".join(get_version_info())
            if argument.dest:
                kwargs['dest'] = argument.dest
            parser.add_argument(
                *argument.args,
                **kwargs
            )

    def register_commands(self):
        for command in self.commands:
            if isinstance(command, Command):
                self.add_command(command)
            elif isinstance(command, basestring):
                try:
                    command = import_string(command)
                    command.global_arguments = self.global_arguments
                    c = command()
                    self.add_command(c)
                except ImportError as error:
                    log.debug(error)
            else:
                command.global_arguments = self.global_arguments
                c = command()
                self.add_command(c)


class HelpFormatter(argparse.RawDescriptionHelpFormatter):
    def _format_action(self, action):
        parts = super(HelpFormatter, self)._format_action(action)

        # pop off subparser header line
        if isinstance(action, argparse._SubParsersAction):
            parts = "\n".join(parts.split('\n')[1:])
        return parts

    def _format_args(self, action, default_metavar):
        result = super(HelpFormatter, self)._format_args(action, default_metavar)
        return "<{}>".format(result.lower())
