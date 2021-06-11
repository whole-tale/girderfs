import pathlib
from errno import EISDIR, EPERM
from typing import Union

import requests
from fuse import FuseOSError

from .cache import CacheEntry
from .dms import DownloadThread, WtDmsGirderFS
from .utils import ROOT_PATH, _lstrip_path, logger


class WtVersionsFS(WtDmsGirderFS):
    def __init__(self, tale_id, gc):
        self.tale_id = tale_id
        self.root_id = tale_id
        WtDmsGirderFS.__init__(
            self, tale_id, gc, default_file_perm=0o444, default_dir_perm=0o555
        )
        self.session_id = None  # TODO: fixme
        self.cached_versions = {}

    @property
    def _resource_name(self):
        return "version"

    def _load_object(self, id: str, model: str, path: pathlib.Path):
        if id == self.tale_id:
            tale = self.girder_cli.get("tale/%s" % self.tale_id)
            return self._add_model("tale", tale)
        else:
            return super()._load_object(id, model, path)

    def _girder_get_listing(self, obj: dict, path: pathlib.Path):
        if str(obj["_id"]) == self.root_id:
            return {
                "folders": self.girder_cli.get(
                    "%s?taleId=%s" % (self._resource_name, self.tale_id)
                ),
                "files": [],
            }
        elif str(obj["parentId"]) == self.root_id:
            # preload and cache session
            sid = str(obj["_id"])
            try:
                return self.cache[sid + "-data"].listing
            except KeyError:
                pass
            # a version; use this opportunity to pre-populate the cache with information
            # from the session
            params = {
                "name": "data",
                "parentId": str(obj["_id"]),
                "parentType": "folder",
            }
            data_folder = self.girder_cli.get("folder", parameters=params)[0]
            data_set = self.girder_cli.get(
                "%s/%s/dataSet" % (self._resource_name, obj["_id"])
            )

            dfid = str(data_folder["_id"])
            entry = CacheEntry(data_folder, pinned=True)
            self.cache[dfid] = entry

            for dse in data_set:
                self._add_session_entry(dfid, dse)
            listing = super()._girder_get_listing(obj, path)
            self.cache[sid + "-data"] = CacheEntry(data_folder, listing=listing)
            return listing
        else:
            return super()._girder_get_listing(obj, path)

    def _is_mutable_dir(self, obj: dict, path: pathlib.Path):
        if path == ROOT_PATH:
            # root dir
            return True
        else:
            return False

    def _is_version(self, path: Union[pathlib.Path, str]) -> bool:
        if isinstance(path, str):
            path = pathlib.Path(path)
        assert path.is_absolute()
        return len(path.parts) == 2

    def _is_external_file(self, path: Union[pathlib.Path, str]) -> bool:
        if isinstance(path, str):
            path = pathlib.Path(path)
        assert path.is_absolute()
        return len(path.parts) > 3 and path.parts[2] == "data"

    def rename(self, old, new):
        old = pathlib.Path(old)
        if not self._is_version(old):
            raise FuseOSError(EPERM)
        new = pathlib.Path(new)
        if not self._is_version(new):
            raise FuseOSError(EPERM)
        obj, _ = self._get_object_from_root(_lstrip_path(old))
        del self.cache[self.root_id]
        if new == "/.trash":
            # We can't really implement removal of versions using normal delete operations,
            # since that is done by recursive removal of files and directories, files and
            # directories which cannot otherwise be removed or modified individually. Instead,
            # removal is implemented as a single operation consisting of moving a version to
            # a special directory. Incidentally, the version delete operation does, in fact,
            # move the deleted version to a .trash directory. Undelete is not currently exposed,
            # but reasonable design dictates that we eventually do expose it.
            self.girder_cli.delete("%s/%s" % (self._resource_name, obj["_id"]))
        else:
            self.girder_cli.put(
                "%s/%s" % (self._resource_name, obj["_id"]), {"newName": new.parts[-1]}
            )

    def _get_perm(self, path, is_dir):
        if is_dir and path == "/":
            # allow write on the root folder, but only to the extent that we can rename and delete
            # versions
            return 0o755
        else:
            # use defaults passed to the super constructor
            return super()._get_perm(path, is_dir)

    def _open(self, path: str, fd: int, obj: dict) -> None:
        if self._is_external_file(path):
            super()._open(path, fd, obj)
        else:
            self._start_nondms_download(path)

    def _start_nondms_download(self, spath: str) -> None:
        path = _lstrip_path(spath)
        file, model = self._get_object_from_root(path)
        if model not in ["file", "item"]:
            raise FuseOSError(EISDIR)
        with self.flock:
            fdict = self._ensure_fdict(spath, file)
            fdict["cached.locally"] = True
            downloaded = fdict["downloaded"]
            downloading = fdict["downloading"]
            # set downloading flag with lock held
            if not downloading and not downloaded:
                fdict["downloading"] = True
                fdict["written"] = 0
        if downloaded:
            # file is here, so no need to download again
            pass
        else:
            if downloading:
                return
            else:
                req = requests.get(
                    "%sfile/%s/download" % (self.girder_cli.urlBase, file["_id"]),
                    headers={"Girder-Token": self.girder_cli.token},
                    stream=True,
                )
                thread = DownloadThread(spath, req, fdict, self.flock, self)
                thread.start()

    def _is_mutable_file(self, path: pathlib.Path) -> bool:
        return False

    def _reload_file(self, file: dict) -> dict:
        logger.debug("-> _reload({})".format(file["_id"]))
        return self.girder_cli.get("file/%s" % file["_id"])

    def _wait_for_file(self, fdict):
        if "cached.locally" in fdict:
            return
        else:
            super()._wait_for_file(fdict)

    def _lock(self, path, obj):
        resp = self.girder_cli.post(
            "dm/lock?sessionId=%s&itemId=%s" % (self.session_id, obj["_id"])
        )
        return resp["_id"]

    # By default, these may throw EROFS, but we don't want to confuse the user, since
    # some rather limited things can be done with top level directories. So instead of
    # saying EROFS (read-only filesystem), we say EPERM (operation not permitted)
    def chmod(self, path, mode):
        raise FuseOSError(EPERM)

    def chown(self, path, uid, gid):
        raise FuseOSError(EPERM)

    def create(self, path, mode, fi=None):
        raise FuseOSError(EPERM)

    def link(self, target, source):
        raise FuseOSError(EPERM)

    def mkdir(self, path, mode):
        raise FuseOSError(EPERM)

    def mknod(self, path, mode, dev):
        raise FuseOSError(EPERM)

    def rmdir(self, path):
        raise FuseOSError(EPERM)

    def symlink(self, target, source):
        raise FuseOSError(EPERM)

    def truncate(self, path, length, fh=None):
        raise FuseOSError(EPERM)

    def unlink(self, path):
        raise FuseOSError(EPERM)

    def write(self, path, data, offset, fh):
        raise FuseOSError(EPERM)
