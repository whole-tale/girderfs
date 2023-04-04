# -*- coding: utf-8 -*-
"""
Core classes for FUSE based filesystem handling Girder's resources
"""
import datetime
import logging
import os
import pathlib
import shutil
import tempfile
import time
from errno import ENOENT, ENOTDIR
from stat import S_IFDIR, S_IFLNK, S_IFREG
from typing import Tuple

import diskcache
from fuse import FuseOSError, LoggingMixIn, Operations

from .cache import CACHE_ENTRY_STALE_TIME, CacheEntry, CacheWrapper
from .utils import ROOT_PATH, _convert_time, _lstrip_path, logger


class GirderFS(LoggingMixIn, Operations):
    """
    Base class for handling Girder's folders

    :param folder_id: Folder id
    :type folder_id: unicode
    :param girder_cli: Authenticated instance of GirderClient
    :type girder_cli: girder_client.GriderClient
    """

    _cachedir = None
    _cache = None

    def __init__(
        self,
        root_id,
        girder_cli,
        root_model="folder",
        default_file_perm=0o644,
        default_dir_perm=0o755,
    ):
        super(GirderFS, self).__init__()
        self.root_id = root_id
        self.root_model = root_model
        self.girder_cli = girder_cli
        self.fd = 0
        self.default_file_perm = default_file_perm
        self.default_dir_perm = default_dir_perm
        self.root = self._load_object(self.root_id, root_model, None)

    @property
    def cachedir(self):
        if not self._cachedir:
            self._cachedir = tempfile.mkdtemp(prefix="wtdm")
        return self._cachedir

    @property
    def cache(self):
        if self._cache is None:
            self._cache = CacheWrapper(diskcache.Cache(self.cachedir))
        return self._cache

    def _load_object(self, id: str, model: str, path: pathlib.Path):
        if model is None and id == self.root_id:
            model = self.root_model
        return self._add_model(model, self.girder_cli.get("/%s/%s" % (model, id)))

    def _add_model(self, model, obj):
        obj["_modelType"] = model
        return obj

    def _get_object_from_root(self, path: pathlib.Path) -> Tuple[dict, str]:
        logger.debug("-> _get_object_from_root({})".format(path))
        return self._get_object_by_path(self.root_id, path)

    def _get_object_by_path(
        self, obj_id: str, pathFromObj: pathlib.Path
    ) -> Tuple[dict, str]:
        logger.debug("-> _get_object_by_path({}, {})".format(obj_id, pathFromObj))

        raw_listing = self._get_listing(obj_id, None)

        obj = self._find(pathFromObj.parts[0], raw_listing)

        if obj is None:
            raise FuseOSError(ENOENT)

        isFile = obj["_modelType"] in ["file", "item"]

        if len(pathFromObj.parts) == 1:
            return obj, obj["_modelType"]
        else:
            if isFile:
                raise FuseOSError(ENOTDIR)
            return self._get_object_by_path(
                str(obj["_id"]), pathlib.Path(*pathFromObj.parts[1:])
            )

    def _find(self, name, d: dict):
        if name in d:
            return d[name].obj
        else:
            return None

    def _get_listing(self, obj_id: str, path: pathlib.Path) -> dict:
        logger.debug("-> _get_listing({}, {})".format(obj_id, path))
        try:
            entry = self.cache[obj_id]
            if not self._listing_is_current(entry, path):
                self._store_listing(entry, self._girder_get_listing(entry.obj, path))
        except KeyError:
            try:
                obj = self._load_object(obj_id, None, path)
                entry = CacheEntry(obj)
                self.cache[obj_id] = entry
                self._store_listing(entry, self._girder_get_listing(obj, path))
            except Exception:
                logger.error("Error updating cache", exc_info=True, stack_info=True)
                raise
        except Exception:
            logger.error("Error getting cached listing", exc_info=True, stack_info=True)
            raise
        finally:
            return self.cache[obj_id].listing

    def _store_listing(self, entry: CacheEntry, listing: dict):
        current_listing = {}
        for obj_type in ["folders", "files", "links"]:
            if obj_type in listing:
                single_listing = listing[obj_type]
                for girder_obj in single_listing:
                    _id = str(girder_obj["_id"])
                    girder_obj["_modelType"] = obj_type[:-1]
                    try:
                        existing = self.cache[_id]
                        existing.obj = girder_obj
                    except KeyError:
                        existing = CacheEntry(girder_obj)
                        self.cache[_id] = existing
                    current_listing[girder_obj["name"]] = existing
        entry.listing = current_listing

    def _listing_is_current(self, entry: CacheEntry, path: pathlib.Path):
        if entry.listing is None:
            logger.debug(
                "-> _listing_is_current({}, {}) - "
                "no listing in entry".format(entry.obj["_id"], path)
            )
            return False
        if path is not None and self._is_mutable_dir(entry.obj, path):
            logger.debug(
                "-> _listing_is_current({}, {}) - "
                "dir is mutable".format(entry.obj["_id"], path)
            )
            if not self._object_is_current(entry, path):
                logger.debug(
                    "-> _listing_is_current({}, {}) - "
                    "object not current".format(entry.obj["_id"], path)
                )
                return False
        logger.debug(
            "-> _listing_is_current({}, {}) - " "current".format(entry.obj["_id"], path)
        )
        return True

    def _get_current_object(self, existing: dict):
        id = str(existing["_id"])
        try:
            entry = self.cache[id]
            self._object_is_current(entry, None)
            return entry.obj
        except KeyError:
            obj = self._load_object(id, existing["_modelType"])
            self.cache[id] = CacheEntry(obj)
            return obj

    def _object_is_current(self, entry: CacheEntry, path: pathlib.Path):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("-> _object_is_current({})".format(entry.obj))
        now = time.time()
        if now - entry.loadTime < CACHE_ENTRY_STALE_TIME:
            logger.debug("-> _object_is_current({}) - too soon".format(path))
            return True
        obj = entry.obj
        newObj = self._load_object(str(obj["_id"]), obj["_modelType"], path)
        if self._object_changed(newObj, obj):
            entry.obj = newObj
            logger.debug("-> _object_is_current({}) - changed".format(path))
            return False
        else:
            logger.debug("-> _object_is_current({}) - current".format(path))
            return True

    def _is_mutable_dir(self, obj: dict, path: pathlib.Path):
        return False

    def _object_changed(self, newObj, oldObj):
        logger.debug(
            "-> _object_changed({}, {})".format(newObj["updated"], oldObj["updated"])
        )
        return newObj["updated"] != oldObj["updated"]

    def _girder_get_listing(self, obj: dict, path: pathlib.Path):
        return self.girder_cli.get("dm/fs/%s/listing" % obj["_id"])

    def getattr(self, spath: str, fh=None):
        logger.debug("-> getattr({})".format(spath))
        if spath == "/":
            now = _convert_time(str(datetime.datetime.now()))
            return dict(
                st_mode=(S_IFDIR | self._get_perm(spath, True)),
                st_nlink=2,
                st_ctime=now,
                st_atime=now,
                st_mtime=now,
            )
        obj, obj_type = self._get_object_from_root(_lstrip_path(spath))

        return self._getattr(spath, obj, obj_type)

    def _getattr(self, path: pathlib.Path, obj: dict, obj_type: str) -> dict:
        if obj_type == "folder":
            stat = dict(st_mode=(S_IFDIR | self._get_perm(path, True)), st_nlink=2)
        elif obj_type == "link":
            stat = dict(st_mode=(S_IFLNK | self._get_perm(path, True)), st_nlink=1)
        else:
            stat = dict(st_mode=(S_IFREG | self._get_perm(path, False)), st_nlink=1)
        ctime = _convert_time(obj["created"])
        try:
            mtime = _convert_time(obj["updated"])
        except KeyError:
            mtime = ctime
        stat.update(
            dict(
                st_ctime=ctime,
                st_mtime=mtime,
                st_blocks=1,
                st_size=obj["size"],
                st_atime=time.time(),
            )
        )
        return stat

    def _get_perm(self, path, is_dir):
        if is_dir:
            return self.default_dir_perm
        else:
            return self.default_file_perm

    def read(self, path, size, offset, fh):
        raise NotImplementedError

    def readdir(self, path, fh):
        logger.debug("-> readdir({})".format(path))
        dirents = [".", ".."]
        raw_listing = self._get_listing_by_path(path)
        logger.debug("raw_listing: %s" % raw_listing)
        dirents += raw_listing.keys()
        return dirents

    def _get_listing_by_path(self, spath):
        logger.debug("-> _get_listing_by_path({})".format(spath))
        if spath == "/":
            return self._get_listing(self.root_id, ROOT_PATH)
        else:
            path = _lstrip_path(spath)
            obj, obj_type = self._get_object_from_root(path)
            return self._get_listing(str(obj["_id"]), path)

    def getinfo(self, path):
        logger.debug("-> getinfo({})".format(path))
        """Pyfilesystem essential method"""
        if not path.startswith("/"):
            path = "/" + path
        return self.getattr(path)

    def listdir(
        self,
        path="./",
        wildcard=None,
        full=False,
        absolute=False,
        dirs_only=False,
        files_only=False,
    ):
        logger.debug("-> listdir({})".format(path))

        if not path.startswith("/"):
            path = "/" + path
        list_dir = self.listdirinfo(
            path,
            wildcard=wildcard,
            full=full,
            absolute=absolute,
            dirs_only=dirs_only,
            files_only=files_only,
        )
        return [_[0] for _ in list_dir]

    def listdirinfo(
        self,
        path="./",
        wildcard=None,
        full=False,
        absolute=False,
        dirs_only=False,
        files_only=False,
    ):
        """
        Pyfilesystem non-essential method

        Retrieves a list of paths and path info under a given path.

        This method behaves like listdir() but instead of just returning the
        name of each item in the directory, it returns a tuple of the name and
        the info dict as returned by getinfo.

        This method may be more efficient than calling getinfo() on each
        individual item returned by listdir(), particularly for network based
        filesystems.
        """

        logger.debug("-> listdirinfo({})".format(path))
        return [
            (name, self.getattr(os.path.join(path, name)))
            for name in self._get_listing_by_path(path).keys()
        ]

    def isdir(self, path):
        """Pyfilesystem essential method"""
        logger.debug("-> isdir({})".format(path))
        attr = self.getattr(path)
        return attr["st_nlink"] == 2

    def isfile(self, path):
        """Pyfilesystem essential method"""
        logger.debug("-> isfile({})".format(path))
        attr = self.getattr(path)
        return attr["st_nlink"] == 1

    def destroy(self, private_data):
        logger.debug("-> destroy()")
        not_removed = True
        while not_removed:
            not_removed = self.cache.clear()
        self.cache.close()
        shutil.rmtree(self.cachedir)

    # Disable unused operations:
    access = None
    flush = None
    # getxattr = None
    # listxattr = None
    opendir = None
    open = None
    release = None
    releasedir = None
    statfs = None
