import datetime
import logging
import os
import tempfile
import threading
import time
from errno import EISDIR, ENOENT, EPERM
from queue import Queue
from stat import S_IFDIR, S_IFREG

from fuse import FuseOSError

from .dms import WtDmsGirderFS
from .utils import _convert_time, _lstrip_path, logger


class UploadThread(threading.Thread):
    def __init__(self, queue, fs, cli):
        threading.Thread.__init__(self)
        self.daemon = True
        self.queue = queue
        self.fs = fs
        self.cli = cli

    def run(self):
        while True:
            (path, fdict) = self.queue.get(True)
            self._upload(path, fdict)

    def _upload(self, path, fdict):
        obj = fdict["obj"]
        # this will break if anything writes to the file during upload
        # same if anything deletes the file. For now, let it fail with an error.
        # TODO: cancel uploads when a file is opened w/a
        fp = open(fdict["path"], "a+b")
        fp.seek(0, os.SEEK_END)
        size = fp.tell()
        fp.seek(0, os.SEEK_SET)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("-> _upload({}, size={})".format(obj, size))
        if self._is_item(obj):
            obj = self._get_file(obj)
        self.cli.uploadFileContents(obj["_id"], fp, size)
        fp.close()
        self.cli.get("dm/fs/%s/evict" % fdict["obj"]["_id"])

    def _is_item(self, obj):
        return "folderId" in obj

    def _get_file(self, obj):
        files = list(self.cli.listFile(obj["_id"]))
        if len(files) != 1:
            raise Exception("Expected a single file in item %s" % obj["_id"])
        return files[0]


class WtHomeGirderFS(WtDmsGirderFS):
    # At this point this is a mess and should be re-implemented as a write-back cache
    # for both data and metadata

    def __init__(self, folderId, gc):
        WtDmsGirderFS.__init__(self, folderId, gc)
        # override root cache entry built by the DMS FS
        self._invalidate_root(folderId)
        self.uploadQueue = Queue()
        self.uploadThread = UploadThread(self.uploadQueue, self, self.girder_cli)
        self.uploadThread.start()

    def _invalidate_root(self, id: str):
        if not self.cache.delete(id):
            logger.error('Object not in cache "%s" (%s)' % (id, type(id)))

    def _load_root(self):
        return None

    def _get_root_listing(self):
        return None

    def statfs(self, path):
        # default results in a read-only fs
        return {"f_flag": os.ST_NOATIME + os.ST_NODEV + os.ST_NODIRATIME + os.ST_NOSUID}

    def chmod(self, path, mode):
        logger.debug("-> chmod({}, {})".format(path, mode))

        path = _lstrip_path(path)
        self._set_metadata(path, {"permissions": mode})

    def _set_metadata(self, path, dict):
        obj, objType = self._get_object_from_root(path)
        self._set_object_metadata(obj, objType, dict)

    def _set_object_metadata(self, obj, objType, dict):
        self.girder_cli.put("dm/fs/%s/setProperties" % obj["_id"], json=dict)
        obj.update(dict)
        return obj

    def getattr(self, path, fh=None):
        logger.debug("-> getattr({})".format(path))

        if path == "/":
            now = _convert_time(str(datetime.datetime.now()))
            return dict(
                st_mode=(S_IFDIR | self._get_perm(path, True)),
                st_nlink=2,
                st_ctime=now,
                st_atime=now,
                st_mtime=now,
            )
        obj, objType = self._get_object_from_root(_lstrip_path(path))

        if objType == "folder":
            mode = S_IFDIR | self._get_prop(
                obj, "permissions", self._get_perm(path, True)
            )
            nlinks = 2
        else:
            mode = S_IFREG | self._get_prop(
                obj, "permissions", self._get_perm(path, False)
            )
            nlinks = 1
        stat = dict(st_mode=mode, st_nlink=nlinks)
        ctime = _convert_time(obj["created"])
        try:
            mtime = _convert_time(obj["updated"])
        except KeyError:
            mtime = ctime
        size = obj["size"]
        with self.flock:
            if path in self.openFiles:
                fdict = self.openFiles[path]
                if "localsize" in fdict:
                    # assume local file is always the most current if it exists
                    size = fdict["localsize"]
        stat.update(
            dict(
                st_ctime=ctime,
                st_mtime=mtime,
                st_blocks=1,
                st_size=size,
                st_atime=time.time(),
            )
        )
        return stat

    def _get_prop(self, obj, name, default):
        if name in obj:
            return obj[name]
        else:
            return default

    def create(self, pathstr, mode, fi=None):
        logger.debug("-> create({}, {})".format(pathstr, mode))

        path = _lstrip_path(pathstr)
        parentId = self._get_parent_id(path)
        # See atomicity comment on mkdir()

        try:
            obj, objType = self._get_object_from_root(path)
        except FuseOSError as ex:
            if ex.errno == ENOENT:
                objType = None
            else:
                raise

        if objType == "folder":
            raise FuseOSError(EISDIR)
        # TODO: fix race

        if objType == "file":
            # Object exists and is a file. Truncate.
            return self._truncate(path, 0, close=False)
        else:
            # item does not exist
            obj = self._create(path, parentId, mode)
            self._cache_add_file(obj, path)
            # Confusingly, "mode" is used both for permissions (as in mkdir) and
            # for open type (as in open)
            # I'm assuming here that create() is meant to implement what creat() does
            # and therefore it implies open(..., O_CREAT|O_WRONLY|O_TRUNC).
            #
            # create an empty file locally
            fdict = self._ensure_fdict(pathstr, obj)
            with tempfile.NamedTemporaryFile(prefix="wtdm", delete=False) as tmp:
                fdict["path"] = tmp.name
            self._mark_dirty(pathstr)
            return self.open(pathstr, mode=os.O_CREAT + os.O_WRONLY + os.O_TRUNC)

    def _get_object_id_by_path(self, obj_id, path):
        if len(path.parts) == 0:
            return self.folder_id
        obj, _ = self._get_object_id_by_path(path)
        return obj["_id"]

    def _cache_get_parent_id(self, path):
        return self._get_object_id_by_path(path.parent)

    def _cache_add_file(self, obj, path):
        self._cache_add_obj(obj, path, "files")

    def _cache_add_dir(self, obj, path):
        self._cache_add_obj(obj, path, "folders")

    def _cache_add_obj(self, obj, path, type):
        with self.flock:
            parentId = str(self._get_parent_id(path))
            dict = self.cache[parentId]
            lst = dict[type]
            lst.append(obj)
            self.cache[parentId] = dict

    def _cache_remove_dir(self, path):
        self._cache_remove_obj(path, "folders")

    def _cache_remove_file(self, path):
        self._cache_remove_obj(path, "files")

    def _cache_remove_obj(self, path, type):
        with self.flock:
            parentId = str(self._get_parent_id(path))
            obj = self.cache[parentId]
            lst = obj[type]
            for i in range(len(lst)):
                if lst[i]["name"] == path.name:
                    lst.pop(i)
                    self.cache[parentId] = obj
                    return
            raise Exception("Could not remove object from cache: %s" % path)

    def _mark_dirty(self, path):
        # sets a flag that will trigger an upload when the file is closed
        fdict = self.openFiles[path]
        fdict["dirty"] = True

    def _create(self, path, parentId, perms):
        logger.debug("-> _create({}, {}, {})".format(path, parentId, perms))
        item = self.girder_cli.createItem(parentId, path.name)
        print(item)
        file = self.girder_cli.post(
            "file",
            parameters={
                "parentType": "item",
                "parentId": item["_id"],
                "name": path.name,
                "size": 0,
            },
        )
        self._set_object_metadata(file, "file", {"permissions": perms})
        return item

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    def fsyncdir(self, path, datasync, fh):
        return 0

    def mkdir(self, path, mode):
        logger.debug("-> mkdir({}, {})".format(path, mode))

        path = _lstrip_path(path)
        parentId = self._get_parent_id(path)

        # It's somewhat silly that you can't set other folder parameters with this call.
        # The two-step solution is a race waiting to happen and possibly a security issue
        # since there is no way to atomicaly create a locked-down directory.
        # The better thing may be to abandon REST and add an API call that allows this to be
        # done in one step. Although I suspect this will be a small problem once we
        # think about having multiple clients syncing to the same home-dir.
        obj = self.girder_cli.post(
            "folder", parameters={"parentId": parentId, "name": path.name}
        )
        self._set_metadata(path, {"permissions": mode})
        self._cache_add_dir(obj, path)

    def _get_parent_id(self, path):
        if len(path.parent.parts) == 0:
            return self.folder_id
        else:
            obj, objType = self._get_object_from_root(path.parent)
            return obj["_id"]

    def rmdir(self, path):
        logger.debug("-> rmdir({})".format(path))

        path = _lstrip_path(path)
        if len(path.parts) == 0:
            raise FuseOSError(EPERM)

        obj, objType = self._get_object_from_root(path)
        # should probably check if it's empty
        self.girder_cli.delete("%s/%s" % (objType, obj["_id"]))
        self._cache_remove_dir(path)

    def mknod(self, path, mode, dev):
        raise FuseOSError(EPERM)

    def rename(self, old, new):
        logger.debug("-> rename({}, {})".format(old, new))

        path = _lstrip_path(old)
        if len(path.parts) == 0:
            raise FuseOSError(EPERM)

        obj, objType = self._get_object_from_root(path)
        self.girder_cli.put("%s/%s" % (objType, obj["_id"]), parameters={"name": new})
        obj["name"] = new

    def truncate(self, path, length, fh=None):
        logger.debug("-> truncate({}, {}, {})".format(path, length, fh))
        # so fh=None means truncate() whereas fh != None means ftruncate

        # the basic workflow is to do i/o locally and commit when all of the following are true:
        #  - no active downloads
        #  - the file is not open

        pathObj = _lstrip_path(path)
        obj, objType = self._get_object_from_root(pathObj)

        if objType == "folder":
            raise FuseOSError(EISDIR)

        if fh is None:
            self._truncate(path, length)
        else:
            self._ftruncate(path, fh, length)
            self._mark_dirty(path)

    def _truncate(self, path, length, close=True):
        fh = self.open(path, mode=os.O_RDWR + os.O_APPEND)
        self._ftruncate(path, fh, length)
        self._mark_dirty(path)
        if close:
            self.release(path, fh)
        else:
            return fh

    def _ftruncate(self, path, fh, length):
        logger.debug("-> _ftruncate({}, {}, {})".format(path, fh, length))
        fp = None
        with self.flock:
            fdict = self.openFiles[path]
            self._update_size(path, fdict, length)
            if fdict["downloading"]:
                # simply stop downloading after the limit
                fdict["downloadThread"].setLimit(length)
                self._mark_dirty(path)
                return
            else:
                if fh in self.fobjs:
                    fp = self.fobjs[fh]
        if fp is None:
            fp = open(fdict["path"], "r+b")
        fp.truncate(length)
        self._mark_dirty(path)

    def unlink(self, path):
        logger.debug("-> unlink({})".format(path))
        path = _lstrip_path(path)
        obj, objType = self._get_object_from_root(path)
        # should be the parent item
        self.girder_cli.delete("item/%s" % (obj["_id"]))
        self._cache_remove_file(path)

    def write(self, path, data, offset, fh):
        logger.debug("-> write({}, {}, {})".format(path, fh, offset))
        fdict = self.openFiles[path]
        size = len(data)

        if fdict["downloading"]:
            self._ensure_region_available(path, fdict, fh, offset, size)

        if fh not in self.fobjs:
            self.fobjs[fh] = open(fdict["path"], "a+b")
        fp = self.fobjs[fh]

        # should probably lock this to prevent seek+write sequences from racing
        fp.seek(offset)
        fp.write(data)

        fp.seek(0, os.SEEK_END)
        size = fp.tell()
        self._update_size(path, fdict, size)

        self._mark_dirty(path)
        return len(data)

    def _update_size(self, path, fdict, sz):
        fdict["localsize"] = sz

    def release(self, path, fh):  # pylint: disable=unused-argument
        logger.debug("-> release2({}, {})".format(path, fh))
        with self.flock:
            writers = self._remove_writer(path, fh)
            fdict = self.openFiles[path]
            if not fdict["downloading"] and writers == 0:
                self._commit(path, fdict)
            else:
                # still downloading
                pass

        return WtDmsGirderFS.release(self, path, fh)

    def downloadCompleted(self, path, fdict):
        logger.debug("-> downloadCompleted(path=%s)" % (path))
        with self.flock:
            fdict["cached.locally"] = True
            if len(fdict["fds"]) == 0:
                self._commit(path, fdict)

    def open(self, path, mode=os.O_RDONLY, **kwargs):
        logger.debug("-> open(path=%s, mode=%s)" % (path, mode))
        fd = WtDmsGirderFS.open(self, path, mode=mode, **kwargs)
        fdict = self.openFiles[path]
        if "fds" not in fdict:
            fdict["fds"] = set()
        return fd

    def _add_writer(self, path, fd):
        with self.flock:
            fdict = self.openFiles[path]
            fdict["fds"].add(fd)

    def _remove_writer(self, path, fd):
        self.flock.assertLocked()
        fdict = self.openFiles[path]
        if fd in fdict["fds"]:
            fdict["fds"].remove(fd)
        else:
            # no writes actually happened in this open/close session
            pass
        return len(fdict["fds"])

    def _commit(self, path, fdict):
        logger.debug("-> _commit({}, {}".format(path, fdict))
        fdict = self.openFiles[path]
        if "dirty" in fdict and fdict["dirty"]:
            self.flock.assertLocked()
            fdict["uploading"] = True
            self.uploadQueue.put((path, fdict))
            fdict["dirty"] = False

    def _wait_for_file(self, fdict):
        # Override because, except for the first download, the local
        # copy is always considered to be the latest
        if "cached.locally" in fdict:
            return
        else:
            WtDmsGirderFS._wait_for_file(self, fdict)
