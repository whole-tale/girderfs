import logging
import os
import pathlib
import sys

from dateutil.parser import parse as tparse

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
stdout_handler = logging.StreamHandler(sys.stdout)
if os.environ.get("GIRDERFS_DEBUG"):
    stdout_handler.setLevel(logging.DEBUG)
else:
    stdout_handler.setLevel(logging.WARNING)
custom_log_format = logging.Formatter("%(asctime)-15s %(levelname)s:%(message)s")
stdout_handler.setFormatter(custom_log_format)
logger.addHandler(stdout_handler)

ROOT_PATH = pathlib.Path("/")


def _lstrip_path(path):
    path_obj = pathlib.Path(path).resolve()
    if path_obj.is_absolute():
        return pathlib.Path(*path_obj.parts[1:])
    else:
        return path_obj


def _convert_time(strtime):
    return tparse(strtime).timestamp()
