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

class GiraffeError(Exception):
    """
    Baseclass for all giraffez errors.
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
class InvalidCredentialsError(ConfigurationError):
    """
    Raised when connection credentials are incorrect.
    """

class ConnectionLock(ConfigurationError):
    """
    Raised when connection is locked by invalid attempts and the
    'protect' feature is being used.
    """

    def __init__(self, dsn):
        super(ConnectionLock, self).__init__(("Connection {0} is currently locked. please update "
            "credentials and run:\n\tgiraffez config --unlock {0}").format(dsn))
