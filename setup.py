import os
import sys
import logging
import re
import platform
import traceback
from setuptools import setup, Extension as _Extension, find_packages
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsExecError, DistutilsPlatformError


WIN = sys.platform.startswith('win')
PY3 = sys.version_info[0] == 3

if not PY3:
    FileNotFoundError = (IOError, OSError)


class GiraffeBuildError(Exception):
    """
    Baseclass for all build errors.
    """

class PlatformNotSupported(GiraffeBuildError):
    """
    Raised when the installation platform is unsupported or unknown.
    """

class TeradataNotFound(GiraffeBuildError):
    """
    Raised when unable to automatically find either Teradata CLIv2 or
    Teradata PT API files.
    """


def fix_compile(remove_flags):
    """
    Monkey-patch compiler to allow for removal of default compiler flags.
    """
    import distutils.ccompiler

    def _fix_compile(self, sources, output_dir=None, macros=None, include_dirs=None, debug=0,
            extra_preargs=None, extra_postargs=None, depends=None):
        for flag in remove_flags:
            if flag in self.compiler_so:
                self.compiler_so.remove(flag)
        macros, objects, extra_postargs, pp_opts, build = self._setup_compile(output_dir, macros,
                include_dirs, sources, depends, extra_postargs)
        cc_args = self._get_cc_args(pp_opts, debug, extra_preargs)
        for obj in objects:
            try:
                src, ext = build[obj]
            except KeyError:
                continue
            self._compile(obj, src, ext, cc_args, extra_postargs, pp_opts)
        return objects

    distutils.ccompiler.CCompiler.compile = _fix_compile

# strict-prototypes is only valid for C code so causes invalid warnings
# with C++.
# unreachable-code is considered to be a broken warning and has been
# removed from newer versions of gcc
remove_flags = ['-Wstrict-prototypes', '-Wunreachable-code']
fix_compile(remove_flags)

def get_teradata_home():
    """
    Attempts to find the Teradata install directory with the defaults
    for a given platform.
    """
    if WIN:
        if is_64bit():
            return get_teradata_version("C:/Program Files/Teradata/Client")
        else:
            return get_teradata_version("C:/Program Files (x86)/Teradata/Client")
    else:
        return get_teradata_version("/opt/teradata/client")

def get_teradata_version(search_directory):
    """
    Returns the highest version number in a directory
    """
    try:
        directories = []
        for d in os.listdir(search_directory):
            if re.search("\d+\.\d+", d):
                directories.append(d)
        if not directories:
            return None
        return os.path.join(search_directory, max(directories))
    except FileNotFoundError:
        sys.stderr.write("Teradata FileNotFound.")
        return None

def get_version():
    version_regex = re.compile(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', re.MULTILINE)

    with open('giraffez/__init__.py', 'r') as f:
        return version_regex.search(f.read()).group(1)

def is_64bit():
    if sys.maxsize > 2**32:
        return True
    return False


TERADATA_HOME = os.environ.get('TERADATA_HOME', get_teradata_home())

class Extension(_Extension):
    name = None

    objects = []

    depends_on = []

    success = False

    def __init__(self, *args, **kwargs):
        _Extension.__init__(self, self.name, self.sources, **kwargs)

        # Add local source files to include_dirs
        self.include_dirs.append(os.path.join(os.getcwd(), "src"))

        # Windows compatbility
        if WIN:
            self.define_macros.append(('_WIN32', 1))
        else:
            self.extra_compile_args = ['-Wfatal-errors']

    @classmethod
    def set_objects(cls, objects):
        cls.objects = objects

    @property
    def language(self):
        if any(f.endswith(".cc") for f in self.sources):
            return "c++"
        return "c"

    @language.setter
    def language(self, value):
        pass

    def setup(self):
        pass


class EncoderExtension(Extension):
    name = "giraffez._encoder"

    sources = [
        "src/encoder/columns.c",
        "src/encoder/convert.c",
        "src/encoder/indicator.c",
        "src/encoder/unpack.c",
        "src/encoder/unpack_dict.c",
        "src/encoder/unpack_str.c",
        "src/encoder/util.c",
        "src/teradata/stmt_info.c",
        "giraffez/encoderobject.c",
        "giraffez/encodermodule.c",
    ]

    depends = [
        "src/encoder/columns.h",
        "src/encoder/convert.h",
        "src/encoder/indicator.h",
        "src/encoder/unpack.h",
        "src/encoder/util.h",
        "src/teradata/stmt_info.h",
        "giraffez/encoderobject.h",
        "giraffez/encodermodule.c",
    ]


class CLIExtension(Extension):
    name = "giraffez._cli"

    sources = [
        "src/teradata/cmd.c",
        "giraffez/cmdobject.c",
        "giraffez/climodule.c"
    ]

    depends = [
        "src/teradata/cmd.h",
        "giraffez/cmdobject.h",
        "giraffez/climodule.c"
    ]

    cli_include_dir = None
    cli_library_dir = None

    def setup(self):
        if TERADATA_HOME is None:
            raise TeradataNotFound("Unable to find the Teradata files.")

        if platform.system() == 'Windows':
            if is_64bit():
                tdcli_inc = os.path.join(TERADATA_HOME, "CLIv2/inc")
                tdcli_lib = os.path.join(TERADATA_HOME, "CLIv2/lib")
            else:
                tdcli_inc = os.path.join(TERADATA_HOME, "CLIv2/inc")
                tdcli_lib = os.path.join(TERADATA_HOME, "CLIv2/lib")
        elif platform.system() == 'Linux':
            if is_64bit():
                tdcli_inc = os.path.join(TERADATA_HOME, "include")
                tdcli_lib = os.path.join(TERADATA_HOME, "lib64")
            else:
                tdcli_inc = os.path.join(TERADATA_HOME, "include")
                tdcli_lib = os.path.join(TERADATA_HOME, "lib")
        elif platform.system() == 'Darwin':
            if is_64bit():
                tdcli_inc = os.path.join(TERADATA_HOME, "include")
                tdcli_lib = os.path.join(TERADATA_HOME, "lib64")
            else:
                tdcli_inc = os.path.join(TERADATA_HOME, "include")
                tdcli_lib = os.path.join(TERADATA_HOME, "lib")
        else:
            raise PlatformNotSupported("This following platform is unsupported or unknown: '{}'".format(platform.system()))

        if os.path.isdir(tdcli_inc):
            self.cli_include_dir = tdcli_inc
        if os.path.isdir(tdcli_lib):
            self.cli_library_dir = tdcli_lib

        if not self.cli_include_dir or not self.cli_library_dir:
            raise TeradataNotFound("Cannot find the Teradata CLIv2 files.")

        self.include_dirs.append(self.cli_include_dir)
        self.library_dirs.append(self.cli_library_dir)

        # Set libraries
        if platform.system() == 'Windows':
            self.libraries.append("wincli32")
        elif platform.system() == 'Linux':
            self.libraries.append("cliv2")


class TPTExtension(Extension):
    name = "giraffez._tpt"

    sources = [
        "src/teradata/export.cc",
        "src/teradata/load.cc",
        "giraffez/exportobject.cc",
        "giraffez/loadobject.cc",
        "giraffez/tptmodule.cc"
    ]

    depends = [
        "src/teradata/export.h",
        "src/teradata/load.h",
        "giraffez/exportobject.h",
        "giraffez/loadobject.h",
        "giraffez/tptmodule.cc"
    ]

    depends_on = [EncoderExtension]

    tpt_include_dir = None
    tpt_library_dir = None

    def setup(self):
        if TERADATA_HOME is None:
            raise Exception("Unable to find the teradata.")

        if platform.system() == 'Windows':
            if is_64bit():
                tptapi_inc = os.path.join(TERADATA_HOME, "Teradata Parallel Transporter/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "Teradata Parallel Transporter/bin64")
            else:
                tptapi_inc = os.path.join(TERADATA_HOME, "Teradata Parallel Transporter/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "Teradata Parallel Transporter/bin")
        elif platform.system() == 'Linux':
            if is_64bit():
                tptapi_inc = os.path.join(TERADATA_HOME, "tbuild/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "tbuild/lib64")
            else:
                tptapi_inc = os.path.join(TERADATA_HOME, "tbuild/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "tbuild/lib")
        elif platform.system() == 'Darwin':
            if is_64bit():
                tptapi_inc = os.path.join(TERADATA_HOME, "tbuild/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "tbuild/lib64")
            else:
                tptapi_inc = os.path.join(TERADATA_HOME, "tbuild/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "tbuild/lib")
        else:
            raise PlatformNotSupported("This following platform is unsupported or unknown: '{}'".format(platform.system()))

        if os.path.isdir(tptapi_inc):
            self.tpt_include_dir = tptapi_inc
        if os.path.isdir(tptapi_lib):
            self.tpt_library_dir = tptapi_lib

        if not self.tpt_include_dir or not self.tpt_library_dir:
            raise TeradataNotFound("Cannot find the Teradata Parallel Transporter API files.")

        self.include_dirs.append(self.tpt_include_dir)
        self.library_dirs.append(self.tpt_library_dir)

        if platform.system() == 'Windows':
            self.libraries.append("telapi")
        elif platform.system() == 'Linux':
            self.libraries.append("telapi")
            self.libraries.append("pxicu")


class BuildExt(build_ext):
    attempted = False
    errors = []

    def run(self):
        BuildExt.attempted = True
        try:
            build_ext.run(self)
        except DistutilsPlatformError as error:
            BuildExt.errors.append(traceback.format_exc())

    def get_inplace_path(self, ext_name):
        fullname = self.get_ext_fullname(ext_name)
        modpath = fullname.split('.')
        filename = self.get_ext_filename(modpath[-1])

        # the inplace option requires to find the package directory
        # using the build_py command for that
        package = '.'.join(modpath[0:-1])
        build_py = self.get_finalized_command('build_py')
        package_dir = os.path.abspath(build_py.get_package_dir(package))

        # returning
        #   package_dir/filename
        fullpath = os.path.join(package_dir, filename)
        return fullpath

    def build_extension(self, ext):
        try:
            ext.setup()
            if ext.depends_on:
                for dep in ext.depends_on:
                    objs = getattr(dep, 'objects', [])
                    ext.extra_objects += objs 
            build_ext.build_extension(self, ext)
            if getattr(self, '_built_objects', None):
                ext.set_objects(self._built_objects)

            # This ensures that the shared objects are *also* built
            # in the path to avoid provides with path resolution when
            # working with local files.
            src = self.get_ext_fullpath(ext.name) 
            dst = self.get_inplace_path(ext.name)
            self.mkpath(os.path.dirname(dst))
            self.copy_file(src, dst, preserve_mode=False)

            ext.success = True
        except (CCompilerError, DistutilsExecError, DistutilsPlatformError, GiraffeBuildError) as error:
            BuildExt.errors.append(traceback.format_exc())


if __name__ == '__main__':
    ext_modules = [EncoderExtension(), CLIExtension(), TPTExtension()]

    with open('requirements.txt') as f:
        requirements = f.read().splitlines()

    setup(
        name="giraffez",
        description="a user-friendly and fast Teradata client for Python",
        long_description=open("README.rst").read(),
        license="Apache 2.0",
        url="https://github.com/capitalone/giraffez",
        version=get_version(),
        packages=find_packages(exclude=['tests*']),
        ext_modules=ext_modules,
        entry_points={
            'console_scripts': [
                'giraffez = giraffez.__main__:main'
            ]
        },
        zip_safe=False,
        package_dir={'giraffez': 'giraffez'},
        cmdclass={'build_ext': BuildExt},
        install_requires=requirements,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Programming Language :: C',
            'Programming Language :: C++',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.1',
            'Programming Language :: Python :: 3.2',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'Intended Audience :: System Administrators',
            'Intended Audience :: End Users/Desktop',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: POSIX :: Linux',
            'Topic :: Database',
            'Topic :: Software Development',
            'Topic :: Utilities'
        ]
    )

    if BuildExt.attempted:
        sys.stderr.write("\n  {:25s}| Installed\n".format("C-Extension"))
        sys.stderr.write("  =========================================\n")
        for ext in ext_modules:
            sys.stderr.write("  {:25s}|   {}\n".format(ext.__class__.__name__, ext.success))
        if any(x.success is False for x in ext_modules):
            sys.stderr.write("\ngiraffez was not successfully installed ...\n\nBUILD ERRORS:\n")
            for err in BuildExt.errors:
                sys.stderr.write(err)
                sys.stderr.write("\n")
