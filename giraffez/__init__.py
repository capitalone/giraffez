# -*- coding: utf-8 -*-

from __future__ import absolute_import

"""
giraffez
~~~~~~~~~~~~

A Teradata client making use of CLIv2 and TPT API. It is made to be
user-friendly and very fast.

:copyright: (c) 2016 by Capital One Services, LLC. See AUTHORS for more details.
:license: Apache 2.0, see LICENSE for more details.

"""

__title__ = 'giraffez'
__version__ = '1.0.0'
__authors__ = ['Christopher Marshall', 'Kyle Travis']
__license__ = 'Apache 2.0'
__all__     = ['Export', 'MLoad', 'Load', 'Cmd', 'Config', 'Secret']


from .cmd import TeradataCmd as Cmd
from .config import Config
from .constants import SILENCE, VERBOSE, DEBUG, INFO
from .errors import (
    GeneralError,
    GiraffeError,
    MultiLoadError,
    TeradataError,
    GiraffeTypeError,
    GiraffeEncodeError,
    InvalidCredentialsError,
    ConnectionLock
)
from .export import TeradataExport as Export
from .io import (
    Reader,
    Writer
)
from .load import TeradataLoad as Load
from .logging import log, setup_logging
from .mload import TeradataMLoad as MLoad
from .secret import Secret


"""
Module that monkey-patches json module when it's imported so
JSONEncoder.default() automatically checks for a special "to_json()"
method and uses it to encode the object if found. This is done in 
__init__ to ensure that the patch is applied before the JSONEncoder
is used. This is mainly to work with the custom date types when
being serialized into json.
"""
from json import JSONEncoder

def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)

_default.default = JSONEncoder().default  # Save unmodified default.
JSONEncoder.default = _default # replacement

# Set default logging handler to avoid "No handler found" warnings.
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())
