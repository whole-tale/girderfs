import os
import stat

from fs.base import FS
from fs.enums import ResourceType
from fs.error_tools import convert_os_errors
from fs.errors import FileExpected
from fs.info import Info
from fs.mode import Mode
from fs.path import basename
from fs.permissions import Permissions
from girder_client import GirderClient

from girderfs.dms import WtDmsGirderFS


class DMSFS(FS):

    STAT_TO_RESOURCE_TYPE = {
        stat.S_IFDIR: ResourceType.directory,
        stat.S_IFCHR: ResourceType.character,
        stat.S_IFBLK: ResourceType.block_special_file,
        stat.S_IFREG: ResourceType.file,
        stat.S_IFIFO: ResourceType.fifo,
        stat.S_IFLNK: ResourceType.symlink,
        stat.S_IFSOCK: ResourceType.socket,
    }

    def __init__(self, session_id, api_url, api_key):
        super().__init__()
        self.session_id = session_id
        gc = GirderClient(apiUrl=api_url)
        gc.authenticate(apiKey=api_key)
        self._fs = WtDmsGirderFS(session_id, gc)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.session_id)

    # Required
    def getinfo(self, path, namespaces=None):
        self.check()
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _stat = self._fs.getinfo(_path)

        info = {
            "basic": {"name": basename(_path), "is_dir": stat.S_ISDIR(_stat["st_mode"])}
        }

        if "details" in namespaces:
            info["details"] = {
                "_write": ["accessed", "modified"],
                "accessed": _stat["st_atime"],
                "modified": _stat["st_mtime"],
                "size": _stat["st_size"],
                "type": int(
                    self.STAT_TO_RESOURCE_TYPE.get(
                        stat.S_IFMT(_stat["st_mode"]), ResourceType.unknown
                    )
                ),
            }
        if "stat" in namespaces:
            info["stat"] = _stat

        if "access" in namespaces:
            info["access"] = {
                "permissions": Permissions(mode=_stat["st_mode"]).dump(),
                "uid": 1000,  # TODO: fix
                "gid": 100,  # TODO: fix
            }

        return Info(info)

    def listdir(self, path):
        return self._fs.listdir(path)

    def openbin(self, path, mode="r", buffering=-1, **options):
        _mode = Mode(mode)
        _mode.validate_bin()
        self.check()
        _path = self.validatepath(path)
        if _path == "/":
            raise FileExpected(path)
        with convert_os_errors("openbin", path):
            fd = self._fs.open(_path, os.O_RDONLY)
            fdict = self._fs.openFiles[path]
            self._fs._ensure_region_available(path, fdict, fd, 0, fdict["obj"]["size"])
            return open(fdict["path"], "r+b")

    def makedir(self, path, permissions=None, recreate=False):
        pass

    def remove(self, path):
        pass

    def removedir(self, path):
        pass

    def setinfo(self, path, info):
        pass
