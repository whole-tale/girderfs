import logging
import os
import pathlib
import re
import sys
import tempfile
import threading
import time
import traceback
import uuid
from errno import EIO, EPERM
from typing import List

import requests
from fuse import FuseOSError

from .cache import CacheEntry, CacheWrapper
from .core import GirderFS
from .utils import ROOT_PATH, _lstrip_path, logger


class DownloadThread(threading.Thread):
    def __init__(self, path, stream, fdict, lock, fs):
        threading.Thread.__init__(self)
        fdict["downloadThread"] = self
        self.path = path
        self.stream = stream
        self.fdict = fdict
        self.lock = lock
        self.limit = sys.maxsize
        self.fs = fs

    def run(self):
        with tempfile.NamedTemporaryFile(prefix="wtdm", delete=False) as tmp:
            self.fdict["path"] = tmp.name
            #  print self.stream.__dict__
            for chunk in self.stream.iter_content(chunk_size=65536):
                tmp.write(chunk)
                self.fdict["written"] += len(chunk)
                if self.fdict["written"] > self.limit:
                    tmp.truncate(self.limit)
                    break

            with self.lock:
                self.fdict["downloaded"] = True
                self.fdict["downloading"] = False
                if self.fdict["written"] > self.limit:
                    # truncate if we downloaded too much
                    tmp.truncate(self.limit)
                del self.fdict["downloadThread"]
        self.fs.downloadCompleted(self.path, self.fdict)

    def setLimit(self, length):
        self.lock.assertLocked()
        self.limit = length
        if self.fdict["downloaded"]:
            # download done, so run() can't truncate if necessary
            f = open(self.fdict["path"], "a+b")
            f.truncate(length)
            f.close()


class MLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.thread = None

    def acquire(self):
        self.lock.acquire()
        if self.thread is not None:
            traceback.print_stack()
            raise Exception("Not a re-entrant lock")
        self.thread = threading.currentThread()

    def release(self):
        self.thread = None
        self.lock.release()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def __enter__(self):
        self.acquire()

    def __repr__(self):
        if self.thread is None:
            return "Lock - not locked"
        else:
            return "Lock - locked by " + str(self.thread)

    def assertLocked(self):
        if not self.lock.locked():
            raise Exception("Lock assertion failed")


class WtDmsGirderFS(GirderFS):
    """
    Filesystem for locally mounting a remote Girder folder

    :param sessionId: WT DMS session id
    :type sessoinId: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def __init__(self, sessionId, gc, default_file_perm=0o644, default_dir_perm=0o755):
        GirderFS.__init__(
            self,
            str(sessionId),
            gc,
            root_model="session",
            default_file_perm=default_file_perm,
            default_dir_perm=default_dir_perm,
        )
        self.sessionId = sessionId
        self.flock = MLock()
        self.openFiles = {}
        self.locks = {}
        self.fobjs = {}
        # call _girder_get_listing to pre-populate cache with mount point structure
        self._girder_get_listing(self.root, None)

    def _init_cache(self):
        self.ctime = time.ctime(time.time())
        self.cache = CacheWrapper()

    def _load_object(self, id: str, model: str, path: pathlib.Path):
        if id == self.root_id:
            return self._add_model(
                "folder", self.girder_cli.get("dm/session/%s" % self.root_id)
            )
        else:
            return super()._load_object(id, model, path)

    def _girder_get_listing(self, obj: dict, path: pathlib.Path):
        if obj["_id"] == self.root_id:
            root = self.girder_cli.get("dm/session/%s?loadObjects=true" % self.root_id)
            self.cache[self.root_id] = CacheEntry(root, listing={})
            self._populate_mount_points(root["dataSet"])
            return self._add_model("folder", root)
        return super()._girder_get_listing(obj, path)

    def _populate_mount_points(self, dataSet: dict) -> None:
        # mount points can have arbitrary depth, so pre-populate the cache with
        # the necessary directory structure
        for entry in dataSet:
            self._add_session_entry(self.root_id, entry)

    def _fake_obj(self, fs_type: str, name=None, id=None, ctime=None, size=0):
        if ctime is None:
            ctime = self.ctime
        if id is None:
            id = uuid.uuid1()
        return {
            "_id": id,
            "name": name,
            "created": ctime,
            "_modelType": fs_type,
            "size": size,
        }

    def _add_session_entry(self, id: str, dse, prefix: List[str] = None):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("-> _add_session_entry({}, {}, {})".format(id, dse, prefix))
        # assume and strip root slash
        path = list(pathlib.PurePath(dse["mountPath"]).parts)[1:]
        if prefix is not None:
            path = prefix + path
        entry = self._cache_mkdirs(id, path[:-1])
        obj = dse["obj"]
        obj["_modelType"] = dse["type"]
        subEntry = CacheEntry(obj)
        self.cache[str(obj["_id"])] = subEntry
        entry.add_to_listing(subEntry)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("-> _add_session_entry updated entry is {}".format(entry))

    def _cache_mkdirs(self, id: str, path: List[str]):
        logger.debug("-> _cache_mkdirs({}, {})".format(id, path))
        entry = self.cache[id]
        if len(path) == 0:
            return entry
        crt = path[0]

        if entry.listing and crt in entry.listing:
            subEntry = entry.listing[crt]
            obj = subEntry.obj
        else:
            obj = self._fake_obj(fs_type="folder", name=crt)
            subEntry = CacheEntry(obj)
        entry.add_to_listing(subEntry)
        self.cache[str(obj["_id"])] = subEntry

        return self._cache_mkdirs(str(obj["_id"]), path[1:])

    def _is_mutable_dir(self, obj: dict, path: pathlib.Path):
        return path == ROOT_PATH

    def _object_changed(self, newObj, oldObj):
        if "seq" in oldObj and "updated" not in oldObj:
            # if oldObj['_modelType'] == 'session':
            # the line above doesn't work since the object type is overriden to
            # "folder"
            oldSeq = oldObj["seq"] if "seq" in oldObj else None
            newSeq = newObj["seq"] if "seq" in newObj else None
            return oldSeq != newSeq
        else:
            return super()._object_changed(newObj, oldObj)

    def read(self, path, size, offset, fh):
        logger.debug("-> read({}, offset={}, size={})".format(path, offset, size))

        fdict = self.openFiles[path]

        self._ensure_region_available(path, fdict, fh, offset, size)

        if fh not in self.fobjs:
            self.fobjs[fh] = open(fdict["path"], "r+b")
        fp = self.fobjs[fh]

        with self.flock:
            fp.seek(offset)
            return fp.read(size)

    def _ensure_region_available(self, path, fdict, fh, offset, size):
        self._wait_for_file(fdict)

        if not fdict["downloaded"]:
            download = False
            with self.flock:
                if not fdict["downloading"]:
                    download = True
                    fdict["downloading"] = True
            if download:
                try:
                    lockId = self.locks[fh]
                except KeyError:
                    raise FuseOSError(EIO)  # TODO: debug why it happens
                self._start_download(path, lockId)

        self._wait_for_region(path, fdict, offset, size)

    def _wait_for_file(self, fdict):
        # Waits for the file to be downloaded/cached by the DMS
        obj = fdict["obj"]
        timeout = 0.1
        while True:
            try:
                if obj["dm"]["cached"]:
                    return obj
            except KeyError:
                pass

            time.sleep(timeout)
            timeout = min(timeout * 2, 1.0)
            obj = self._get_item_unfiltered(obj["_id"])
            fdict["obj"] = obj

            try:
                if obj["dm"]["transferError"]:
                    if re.match("^40[13] Client Error:", obj["dm"]["transferErrorMessage"]):
                        raise FuseOSError(EPERM)
                    raise OSError(EIO, os.strerror(EIO))
            except KeyError:
                pass

    def _wait_for_region(self, path, fdict, offset, size):
        # Waits until enough of the file has been downloaded locally
        # to be able to read the necessary chunk of bytes
        logger.debug("-> wait_for_region({}, {}, {})".format(path, offset, size))
        while True:
            if fdict["downloaded"]:
                return
            limit = offset + size
            if limit > 0 and fdict["written"] >= limit:
                return
            time.sleep(0.1)

    def _get_item_unfiltered(self, id):
        return self.girder_cli.get("dm/fs/%s/raw" % id)

    def open(self, path, mode=os.O_RDONLY, **kwargs):
        fd = self._next_fd()
        logger.debug("-> open({}, {})".format(path, fd))
        obj, _ = self._get_object_from_root(_lstrip_path(path))
        obj = self._get_item_unfiltered(self._get_id(obj))
        self._open(path, fd, obj)
        return fd

    def _get_id(self, obj):
        if "itemId" in obj:
            return obj["itemId"]
        else:
            return obj["_id"]

    def _next_fd(self):
        with self.flock:
            fd = self.fd
            self.fd += 1
        return fd

    def _open(self, path, fd, obj):
        # By default, FUSE uses multiple threads. In our case, single threads are undesirable,
        # since a single slow open() could prevent any other FS operations from happening.
        # So make sure shared state is properly synchronized
        lockId = self._lock(path, obj)
        try:
            available = obj["dm"]["cached"]
        except KeyError:
            available = False
        with self.flock:
            fdict = self._ensure_fdict(path, obj)
            downloaded = fdict["downloaded"]
            downloading = fdict["downloading"]
            # set downloading flag with lock held
            if available and not downloading and not downloaded:
                fdict["downloading"] = True
            self.locks[fd] = lockId
        if not available:
            # we would need to wait, so delay the waiting until read() is invoked
            return
        if downloaded:
            # file is here, so no need to download again
            pass
        else:
            if downloading:
                return
            else:
                self._start_download(path, self.locks[fd])

    def _ensure_fdict(self, path: str, obj):
        if path not in self.openFiles:
            self.openFiles[path] = {
                "obj": obj,
                "written": 0,
                "downloading": False,
                "downloaded": False,
                "path": None,
            }
        fdict = self.openFiles[path]
        fdict["obj"] = obj
        return fdict

    def _start_download(self, path, lockId):
        with self.flock:
            fdict = self.openFiles[path]
            fdict["downloading"] = True
            fdict["written"] = 0

        req = requests.get(
            "%sdm/lock/%s/download" % (self.girder_cli.urlBase, lockId),
            headers={"Girder-Token": self.girder_cli.token},
            stream=True,
        )
        thread = DownloadThread(path, req, self.openFiles[path], self.flock, self)
        thread.start()

    def _lock(self, path, obj):
        resp = self.girder_cli.post(
            "dm/lock?sessionId=%s&itemId=%s" % (self.root_id, obj["_id"])
        )
        return resp["_id"]

    def _unlock(self, lockId):
        self.girder_cli.delete("dm/lock/%s" % (lockId))

    def destroy(self, private_data):
        GirderFS.destroy(self, private_data)

        for path in list(self.openFiles.keys()):
            fdict = self.openFiles[path]
            try:
                if "path" in fdict:
                    fpath = fdict["path"]
                    logger.debug("-> destroy: removing {}".format(fpath))
                    os.remove(fpath)
                else:
                    logger.debug("-> destroy: no physical path for {}".format(path))
            except Exception:
                pass

    def release(self, path, fh):  # pylint: disable=unused-argument
        logger.debug("-> release({}, {})".format(path, fh))

        lockId = None
        toClose = None
        with self.flock:
            self.fd -= 1
            if fh in self.locks:
                lockId = self.locks[fh]
                del self.locks[fh]

            if fh in self.fobjs:
                toClose = self.fobjs[fh]
                del self.fobjs[fh]

        if toClose is not None:
            toClose.close()

        if lockId is not None:
            self._unlock(lockId)

        return fh

    def downloadCompleted(self, path, fdict):
        pass
