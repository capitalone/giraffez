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
__version__ = '2.0.15'
__authors__ = ['Christopher Marshall', 'Kyle Travis']
__license__ = 'Apache 2.0'
__all__     = ['BulkExport', 'BulkLoad', 'Cmd', 'Config', 'Secret']


try:
    from . import _teradata
    from . import _teradatapt
except ImportError as error:
    raise Exception("""{}.

This indicates that either the giraffez C extensions did not compile
correctly, or more likely, there is an issue with the environment or
installation of the Teradata dependencies. Both the Teradata Call-Level
Interface Version 2 and Teradata Parallel Transporter API require
several environment variables to be set to find the shared library
files and error message catalog.

For more information, refer to this section in the giraffez
documentation:
    http://www.capitalone.io/giraffez/intro.html#environment.
""".format(error))

from ._teradata import TeradataError
from ._teradatapt import (
    EncoderError,
    TeradataError as TeradataPTError
)
from .cmd import Cmd
from .config import Config
from .constants import SILENCE, VERBOSE, DEBUG, INFO
from .errors import (
    GiraffeError,
    GiraffeTypeError,
    InvalidCredentialsError,
    ConnectionLock
)
from .encoders import TeradataEncoder as Encoder
from .export import BulkExport
from .io import (
    Reader,
    Writer
)
from .load import BulkLoad
from .logging import log, setup_logging
from .secret import Secret
from .types import Column, Columns, Date, Decimal, Time, Timestamp
from .utils import register_graceful_shutdown_signal


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
    return getattr(obj.__class__, "__json__", _default.default)(obj)

_default.default = JSONEncoder().default  # Save unmodified default.
JSONEncoder.default = _default # replacement

# Set default logging handler to avoid "No handler found" warnings.
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())
