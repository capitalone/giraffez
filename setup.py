import os
import sys
import io
import logging
import re
import platform
import traceback
import multiprocessing
from setuptools import setup, Extension as _Extension, find_packages
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsExecError, DistutilsPlatformError

develop = False
if len(sys.argv) > 1 and sys.argv[1] == "develop":
    develop = True

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
# removed from newer versions of gcc.
remove_flags = ['-Wstrict-prototypes', '-Wunreachable-code']
fix_compile(remove_flags)

def is_64bit():
    if sys.maxsize > 2**32:
        return True
    return False

def find_teradata_home():
    """
    Attempts to find the Teradata install directory with the defaults
    for a given platform.  Should always return `None` when the defaults
    are not present and the TERADATA_HOME environment variable wasn't
    explicitly set to the correct install location.
    """
    if platform.system() == 'Windows':
        # The default installation path for Windows is split between the
        # Windows directories for 32-bit/64-bit applications.  It is
        # worth noting that Teradata archiecture installed should match
        # the architecture of the Python architecture being used (i.e.
        # TTU 32-bit is required /w Python 32-bit and TTU 64-bit is
        # required for Python 64-bit).
        if is_64bit():
            return latest_teradata_version("C:/Program Files/Teradata/Client")
        else:
            return latest_teradata_version("C:/Program Files (x86)/Teradata/Client")
    elif platform.system() == 'Linux':
        return latest_teradata_version("/opt/teradata/client")
    elif platform.system() == 'Darwin':
        return latest_teradata_version("/Library/Application Support/teradata/client")
    else:
        # In the case nothing is found, the default for Linux is
        # attempted as a last effort to find the correct install
        # directory.
        return latest_teradata_version("/opt/teradata/client")

def latest_teradata_version(search_directory):
    """
    Attempts to find the latest version of Teradata installed based upon
    the default Teradata installation directory structure.
    """
    try:
        directories = []
        for d in os.listdir(search_directory):
            if re.match("[\d\.]+", d):
                directories.append(d)
        if not directories:
            return None
        return os.path.join(search_directory, max(directories))
    except FileNotFoundError:
        sys.stderr.write("Teradata FileNotFound.")
        return None

def giraffez_version():
    version_regex = re.compile(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', re.MULTILINE)

    with open('giraffez/__init__.py', 'r') as f:
        return version_regex.search(f.read()).group(1)


TERADATA_HOME = os.environ.get('TERADATA_HOME', find_teradata_home())


class Extension(_Extension):
    name = None

    objects = []

    depends_on = []

    compiled = False
    success = False
    shared_object = True

    def __init__(self, *args, **kwargs):
        _Extension.__init__(self, self.name, self.sources, **kwargs)

        # Add local source files to include_dirs
        self.include_dirs.append(os.path.join(os.getcwd(), "giraffez"))

        # Windows compatbility
        if platform.system() == 'Windows':
            if is_64bit():
                self.define_macros.append(('WIN64', 1))
            else:
                self.define_macros.append(('WIN32', 1))
        else:
            self.extra_compile_args = ['-Wfatal-errors']
        if develop:
            self.define_macros.append(("DEBUG_LOGGING", 1))

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


class TeradataExtension(Extension):
    name = "giraffez._teradata"

    sources = [
        "giraffez/src/buffer.c",
        "giraffez/src/columns.c",
        "giraffez/src/convert.c",
        "giraffez/src/encoder.c",
        "giraffez/src/errors.c",
        "giraffez/src/row.c",
        "giraffez/src/teradata.c",
        "giraffez/_teradatamodule.c",
    ]

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
                tdcli_lib = os.path.join(TERADATA_HOME, "lib")
            else:
                tdcli_inc = os.path.join(TERADATA_HOME, "include")
                tdcli_lib = os.path.join(TERADATA_HOME, "lib")
        else:
            raise PlatformNotSupported("This following platform is unsupported or unknown: '{}'".format(platform.system()))

        if not os.path.isdir(tdcli_inc) or not os.path.isdir(tdcli_lib):
            raise TeradataNotFound("Cannot find the Teradata CLIv2 files.")

        self.include_dirs.append(tdcli_inc)
        self.library_dirs.append(tdcli_lib)

        # Set libraries
        if platform.system() == 'Windows':
            self.libraries.append("wincli32")
        elif platform.system() == 'Linux':
            self.libraries.append("cliv2")
        elif platform.system() == 'Darwin':
            self.libraries.append("cliv2")


class TeradataPTExtension(Extension):
    name = "giraffez._teradatapt"

    sources = [
        "giraffez/_teradataptmodule.cc",
    ]

    depends_on = [TeradataExtension]

    def setup(self):
        if TERADATA_HOME is None:
            raise TeradataNotFound("Unable to find the Teradata files.")

        if platform.system() == 'Windows':
            if is_64bit():
                tptapi_inc = os.path.join(TERADATA_HOME, "Teradata Parallel Transporter/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "Teradata Parallel Transporter/bin64")
            else:
                tptapi_inc = os.path.join(TERADATA_HOME, "Teradata Parallel Transporter/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "Teradata Parallel Transporter/bin")
            if not os.path.isdir(tptapi_lib):
                tptapi_lib = os.path.join(TERADATA_HOME, "bin")
        elif platform.system() == 'Linux':
            if is_64bit():
                tptapi_inc = os.path.join(TERADATA_HOME, "tbuild/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "tbuild/lib64")
                # version 16.10 appears to have yet again change the location
                if not os.path.isdir(tptapi_lib):
                    tptapi_lib = os.path.join(TERADATA_HOME, "lib64")
            else:
                tptapi_inc = os.path.join(TERADATA_HOME, "tbuild/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "tbuild/lib")
                if not os.path.isdir(tptapi_lib):
                    tptapi_lib = os.path.join(TERADATA_HOME, "lib")
        elif platform.system() == 'Darwin':
            if is_64bit():
                tptapi_inc = os.path.join(TERADATA_HOME, "tbuild/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "tbuild/lib")
            else:
                tptapi_inc = os.path.join(TERADATA_HOME, "tbuild/tptapi/inc")
                tptapi_lib = os.path.join(TERADATA_HOME, "tbuild/lib")
        else:
            raise PlatformNotSupported("This following platform is unsupported or unknown: '{}'".format(platform.system()))

        if not os.path.isdir(tptapi_inc) or not os.path.isdir(tptapi_lib):
            raise TeradataNotFound("Cannot find the Teradata Parallel Transporter API files.")

        self.include_dirs.append(tptapi_inc)
        self.library_dirs.append(tptapi_lib)

        if platform.system() == 'Windows':
            self.libraries.append("telapi")
            self.libraries.append("wincli32")
        elif platform.system() == 'Linux':
            self.libraries.append("telapi")
            self.libraries.append("pxicu")
            self.libraries.append("cliv2")
        elif platform.system() == 'Darwin':
            self.libraries.append("telapi")
            self.libraries.append("pxicu")
            self.libraries.append("cliv2")


class BuildExt(build_ext):
    cache = {}

    def run(self):
        # Disabling parallel build for now. It causes issues on multiple
        # platforms with concurrent file access causing odd build errors
        #self.parallel = multiprocessing.cpu_count()
        build_ext.run(self)

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

    def compile(self, ext):
        objects = self.compiler.compile(ext.sources,
            output_dir=os.path.join(os.getcwd(), "build"),
            macros=ext.define_macros,
            include_dirs=ext.include_dirs,
            extra_postargs=ext.extra_compile_args,
            depends=ext.depends)
        self.cache[ext.name] = ext
        return objects

    def build_extension(self, ext):
        ext.setup()
        if ext.depends_on:
            for dep in ext.depends_on:
                if dep.name not in self.cache:
                    d = dep()
                    ext.include_dirs += d.include_dirs
                    ext.library_dirs += d.library_dirs
                    ext.libraries += d.libraries
                    ext.objects += self.compile(d)
                else:
                    d = self.cache[dep.name]
                    ext.include_dirs += d.include_dirs
                    ext.library_dirs += d.library_dirs
                    ext.libraries += d.libraries
                    ext.objects += d.objects
                ext.include_dirs = list(set(ext.include_dirs))
                ext.library_dirs = list(set(ext.library_dirs))
                ext.libraries = list(set(ext.libraries))
                ext.objects = list(set(ext.objects))
        ext.objects += self.compile(ext)
        ext_path = self.get_ext_fullpath(ext.name)

        # Return early when a shared object is not being created. This
        # is useful when dealing with Extensions that need to have
        # object files built for another Extension that will be creating
        # a shared object.
        if not ext.shared_object:
            return

        self.compiler.link_shared_object(
            ext.objects,
            ext_path,
            libraries=self.get_libraries(ext),
            library_dirs=ext.library_dirs,
            runtime_library_dirs=ext.runtime_library_dirs,
            extra_postargs=ext.extra_link_args,
            export_symbols=self.get_export_symbols(ext),
            debug=self.debug,
            build_temp=self.build_temp,
            target_lang=ext.language)

        # This ensures that the shared objects are *also* built
        # in the path to avoid problems with path resolution when
        # working with local files.
        src = self.get_ext_fullpath(ext.name)
        dst = self.get_inplace_path(ext.name)
        self.mkpath(os.path.dirname(dst))
        self.copy_file(src, dst, preserve_mode=False)


if __name__ == '__main__':
    ext_modules = [TeradataExtension(), TeradataPTExtension()]

    with open('requirements.txt') as f:
        requirements = f.read().splitlines()

    with io.open("README.rst", encoding="utf-8") as f:
        long_description = f.read()
        long_description = long_description.replace('logo.png', 'logo-md.png')

    setup(
        name="giraffez",
        description="a user-friendly and fast Teradata client for Python",
        long_description=long_description,
        license="Apache 2.0",
        url="https://github.com/capitalone/giraffez",
        version=giraffez_version(),
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
