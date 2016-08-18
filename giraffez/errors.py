# -*- coding: utf-8 -*-

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
    Raised when giraffez C module not found.
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
            message = getattr(message, 'message', '')

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
                raise ObjectDoesNotExist(message, code)
            elif code == ObjectNotTable.code:
                raise ObjectNotTable(message, code)
            elif code == ObjectNotView.code:
                raise ObjectNotView(message, code)
            elif code == TransactionAborted.code:
                raise TransactionAborted(message, code)
            elif code == CannotReleaseMultiLoad.code:
                raise CannotReleaseMultiLoad(message, code)
            elif code == MultiLoadTableExists.code:
                raise MultiLoadTableExists(message, code)
            elif code == MultiLoadWorkTableNotFound.code:
                raise MultiLoadWorkTableNotFound(message, code)
            elif code == InvalidCredentialsError.code:
                raise InvalidCredentialsError(message, code)

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
