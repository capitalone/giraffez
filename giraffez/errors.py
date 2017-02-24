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

from .utils import suppress_context


class GiraffeError(Exception):
    """
    Baseclass for all giraffez errors.
    """

class GeneralError(GiraffeError):
    """
    General giraffez error.
    """

class GiraffeNotFound(GiraffeError):
    """
    Raised when giraffez C modules not found.
    """

class TeradataCLIv2NotFound(GiraffeNotFound):
    """
    Unable to import giraffez._cli.  This indicates that either the
    giraffez C extensions did not compile correctly, or more likely,
    there is an issue with the environment or installation of the
    Teradata dependencies.  The Teradata Call-Level Interface Version 2
    requires several environment variables to be set to find the shared
    library files and error message catalog.

    For more information, refer to this section in the giraffez
    documentation:
        http://www.capitalone.io/giraffez/intro.html#environment.
    """

class TeradataPTAPINotFound(GiraffeNotFound):
    """
    Unable to import giraffez._tpt.  This indicates that either the
    giraffez C extensions did not compile correctly, or more likely,
    there is an issue with the environment or installation of the
    Teradata dependencies.  The Teradata Parallel Transporter API
    requires several environment variables to be set to find the shared
    library files and error message catalog.

    For more information, refer to this section in the giraffez
    documentation:
        http://www.capitalone.io/giraffez/intro.html#environment.
    """

class GiraffeTypeError(GiraffeError):
    """
    Baseclass for all giraffez type errors.
    """

class GiraffeEncodeError(GiraffeError):
    """
    Raised when unable to encode the provided object.
    """

class ConfigurationError(GiraffeError): 
    """
    For use with configuration file handling. 
    """

class FileNotFound(GiraffeError): 
    """
    Raised when file does not exist.
    """

class ConfigNotFound(ConfigurationError, FileNotFound):
    """
    Raised when the specified configuration file does not exist.
    """

class KeyNotFound(ConfigurationError, FileNotFound):
    """
    Raised when the specified configuration file does not exist.
    """

class ConfigReadOnly(ConfigurationError):
    """
    Raised when a write is attempted on a configuration file was opened
    in read mode.
    """

class ConnectionLock(ConfigurationError):
    """
    Raised when connection is locked by invalid attempts and the
    'protect' feature is being used.
    """

    def __init__(self, dsn):
        super(ConnectionLock, self).__init__(("Connection {0} is currently locked. please update "
            "credentials and run:\n\tgiraffez config --unlock {0}").format(dsn))

class TeradataError(Exception): 
    """
    Baseclass for all Teradata errors. This exception represents any
    error that originates from Teradata CLIv2/TPTAPI.

    This will attempt to parse error codes from the message in the case
    that the error is a Teradata CLIv2 error.
    """

    def __init__(self, message, code=None):
        super(TeradataError, self).__init__(message)

        if isinstance(message, Exception):
            #message = getattr(message, 'message', '')
            message = str(message)


        if code is None:
            if isinstance(message, (list, tuple)):
                code, message = message[0], message[1]
            else:
                if ":" in message:
                    parts = [x.strip() for x in message.split(":", 1)]
                    try:
                        code = int(parts[0])
                        message = parts[1]
                    except ValueError as error:
                        code = None
                        message = "Unable to parse CLI error code/message"
                else:
                    code = None
                    message = "Unable to parse CLI error code/message"
            if code == ObjectDoesNotExist.code:
                raise suppress_context(ObjectDoesNotExist(message, code))
            elif code == ObjectNotTable.code:
                raise suppress_context(ObjectNotTable(message, code))
            elif code == ObjectNotView.code:
                raise suppress_context(ObjectNotView(message, code))
            elif code == TransactionAborted.code:
                raise suppress_context(TransactionAborted(message, code))
            elif code == CannotReleaseMultiLoad.code:
                raise suppress_context(CannotReleaseMultiLoad(message, code))
            elif code == MultiLoadTableExists.code:
                raise suppress_context(MultiLoadTableExists(message, code))
            elif code == MultiLoadWorkTableNotFound.code:
                raise suppress_context(MultiLoadWorkTableNotFound(message, code))
            elif code == InvalidCredentialsError.code:
                raise suppress_context(InvalidCredentialsError(message, code))

        self.message = message
        self.code = code

    def __repr__(self):
        return "{}: {}".format(self.code, self.message)

class ObjectDoesNotExist(TeradataError):
    """
    Teradata object does not exist.
    """
    code = 3807

class ObjectNotTable(TeradataError):
    """
    Teradata object not a table.
    """
    code = 3853

class ObjectNotView(TeradataError):
    """
    Teradata object not a view.
    """
    code = 3854

class TransactionAborted(TeradataError):
    """
    Teradata transaction aborted.
    """
    code = 2631

class MultiLoadError(TeradataError):
    """
    General MultiLoad error class.
    """

class CannotReleaseMultiLoad(MultiLoadError):
    """
    Raised when MultiLoad cannot be released because it is in the
    apply phase.
    """
    code = 2572

class MultiLoadTableExists(MultiLoadError):
    """
    Raised when MultiLoad worktables exist.
    """
    code = 2574

class MultiLoadLocked(MultiLoadError):
    """
    Raised when MultiLoad appears to be locked. Used to determine when
    MultiLoad should be released.
    """

class MultiLoadWorkTableNotFound(MultiLoadLocked):
    """
    Raised when MultiLoad worktable is missing during restart.
    """
    code = 2583

class InvalidCredentialsError(TeradataError):
    """
    Raised when connection credentials are incorrect.
    """
    code = 8017
