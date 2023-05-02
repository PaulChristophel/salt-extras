"""
This resolves an issue with cifs mounts on windows where os.walk treats symbolic links
that point to directories as files.
https://github.com/python/cpython/issues/102503
"""
import os
import inspect
import salt.utils.data
import salt.utils.stringutils
from salt.fileserver import roots as _roots

_orig_fn = {
    "os_walk": _roots.salt.utils.path.os_walk,
}


def _decoration(fn):
    def wrapper(*args, **kwargs):
        _roots.__opts__ = __opts__
        _roots.__salt_loader__ = __salt_loader__
        _roots.salt.utils.path.os_walk = _os_walk
        try:
            return fn(*args, **kwargs)
        finally:
            _roots.salt.utils.path.os_walk = _orig_fn["os_walk"]

    return wrapper


for _attr in dir(_roots):
    if not _attr.startswith("_") and inspect.isfunction(getattr(_roots, _attr)):
        exec("{0} = _decoration(_roots.{0})".format(_attr))


def _os_walk(top, *args, **kwargs):
    top_query = salt.utils.stringutils.to_str(top)
    for cwd, dirs, files in os.walk(top_query, *args, **kwargs):
        i = len(files) - 1
        while i >= 0:
            full_path = os.path.join(cwd, files[i])
            if os.path.isdir(full_path):
                dirs.append(files.pop(i))
            i -= 1
        yield salt.utils.data.decode((cwd, dirs, files), preserve_tuples=True)
