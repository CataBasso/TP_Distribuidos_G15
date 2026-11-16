import ctypes
import gc
import logging

log = logging.getLogger(__name__)


def release_memory():
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
        return True
    except (OSError, AttributeError):
        return False
