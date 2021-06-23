import logging
import os
import pathlib
from errno import EISDIR
from stat import S_IFLNK
from typing import Tuple, Union

import requests
from fuse import FuseOSError

from .cache import CacheEntry, CacheFile
from .dms import WtDmsGirderFS
from .utils import ROOT_PATH, _lstrip_path, logger
from .versions import WtVersionsFS


class WtRunsFS(WtVersionsFS):
    def __init__(self, tale_id, gc, versions_mountpoint):
        WtVersionsFS.__init__(self, tale_id, gc)
        versions_path = pathlib.Path(versions_mountpoint)
        if not versions_path.is_absolute():
            # we'll use this from Runs/<run>/, so move this up a bit
            self.versions_mountpoint = ("../.." / versions_path).as_posix()
        else:
            self.versions_mountpoint = versions_mountpoint

    @property
    def _resource_name(self):
        return "run"

    def _girder_get_listing(self, obj: dict, path: pathlib.Path):
        # override to bypass the session caching stuff from the versions FS
        return WtDmsGirderFS._girder_get_listing(self, obj, path)

    def _get_object_from_root(self, path: pathlib.Path) -> Tuple[dict, str]:
        logger.debug("-> _get_object_from_root({})".format(path))
        (obj, type) = self._get_object_by_path(self.root_id, path)
        if type in ["file", "item"]:
            # most of these are obtained through listings, so this is an appropriate place
            # to check for mutability
            if self._is_mutable_file(path):
                logger.debug("-> _get_object_from_root({}) - is mutable".format(path))
                id = str(obj["_id"])
                try:
                    entry = self.cache[id]
                    self._object_is_current(entry, path)
                    return entry.obj, type
                except KeyError:
                    # get the most recent version; let some other part worry about caching this
                    return self._load_object(id, type, path), type

        return obj, type

    def _start_nondms_download(self, spath: str) -> None:
        # we're not actually stating a download in this implementation
        path = _lstrip_path(spath)
        file, model = self._get_object_from_root(path)
        if model not in ["file", "item"]:
            raise FuseOSError(EISDIR)
        with self.flock:
            self._ensure_fdict(spath, file)

    def _invalidate_cache(self, fh: int, fdict: dict, file: dict):
        self.flock.assertLocked()
        if "cache" in fdict:
            cache = fdict["cache"]
            cache.invalidate()
        cache = CacheFile(file["size"])
        fdict["cache"] = cache
        fdict["path"] = cache.name
        fdict["updated"] = file["updated"]

        if fh in self.fobjs:
            file = self.fobjs[fh]
            file.close()
            del self.fobjs[fh]
        return cache

    def _ensure_region_available(
        self, path: str, fdict: dict, fh: int, offset: int, size: int
    ):
        obj = self._get_current_object(fdict["obj"])
        with self.flock:
            cache = fdict.get("cache", None)
            updated = fdict.get("updated", None)
            if cache is None or obj["updated"] != updated:
                logger.debug(
                    "-> _ensure_region_available({}) - invalidating cache".format(path)
                )
                cache = self._invalidate_cache(fh, fdict, obj)

        offset, size = cache.adjust_and_start_download(fh, offset, size)
        if size == 0:
            return
        else:
            try:
                with open(cache.name, "r+b") as f:
                    f.seek(offset, os.SEEK_SET)
                    req = requests.get(
                        "%sfile/%s/download?offset=%s&endByte=%s"
                        % (
                            self.girder_cli.urlBase,
                            fdict["obj"]["_id"],
                            offset,
                            offset + size,
                        ),
                        headers={"Girder-Token": self.girder_cli.token},
                        stream=True,
                    )
                    for chunk in req.iter_content(chunk_size=65536):
                        f.write(chunk)

            finally:
                cache.end_download(fh)

    def _is_mutable_dir(self, obj: dict, path: pathlib.Path):
        if path == ROOT_PATH:
            # root dir
            return True
        if len(path.parts) == 1:
            # the run itself (.stdout, .stderr files might pop in)
            return True
        if len(path.parts) == 2 and path.parts[1] == "results":
            # results dir; result files may appear!
            return True
        return False

    def _is_mutable_file(self, path: pathlib.Path) -> bool:
        if len(path.parts) == 2:
            return True
        if len(path.parts) >= 3 and path.parts[1] == "results":
            return True
        return False

    def _is_external_file(self, path: Union[pathlib.Path, str]) -> bool:
        return False

    def _getattr(self, path: pathlib.Path, obj: dict, obj_type: str) -> dict:
        stat = super()._getattr(path, obj, obj_type)
        if "isLink" in obj and obj["isLink"]:
            stat["st_mode"] = stat["st_mode"] | S_IFLNK
        return stat

    def readlink(self, spath: str):
        logger.debug("-> readlink({})".format(spath))
        path = _lstrip_path(spath)
        obj, obj_type = self._get_object_from_root(path)
        target = obj["linkTarget"]
        if target.startswith("../../Versions/"):
            versionId = target[len("../../Versions/") :]
            version = self._timed_cache(versionId, path, self._get_version, versionId)
            return self.versions_mountpoint + "/" + version["name"]
        else:
            return target

    def _get_version(self, id):
        obj = self.girder_cli.get("version/%s" % id)
        obj["_modelType"] = "folder"
        return obj

    def _timed_cache(self, id: str, path: pathlib.Path, method, *args):
        # I don't like this method. Or maybe I don't like the other method that kinda does a
        # similary thing.
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("-> _timed_cache({}, {}, {})".format(id, method, args))
        try:
            entry = self.cache[id]
            if not self._object_is_current(entry, path):
                obj = method(*args)
                entry.obj = obj
            return entry.obj
        except KeyError:
            obj = method(*args)
            self.cache[id] = CacheEntry(obj)
            return obj
