# -*- coding: utf-8 -*-
"""
Core classes for FUSE based filesystem handling Girder's resources
"""
import base64
import datetime
import logging
import os
import pathlib
import pprint
import shutil
import sys
import tempfile
import threading
import time
import traceback
import uuid
from errno import ENOENT, EPERM, EISDIR, EIO, ENOTDIR
from queue import Queue
from stat import S_IFDIR, S_IFREG, S_IFLNK
from typing import Union, Tuple, List

import diskcache
import requests
import six
from dateutil.parser import parse as tparse
from fuse import Operations, LoggingMixIn, FuseOSError
from girder_client import GirderClient

# http://stackoverflow.com/questions/9144724/

logging.basicConfig(format='%(asctime)-15s %(levelname)s:%(message)s', level=logging.ERROR)

CACHE_ENTRY_STALE_TIME = 1.0 # seconds
ROOT_PATH = pathlib.Path('/')

def _lstrip_path(path):
    path_obj = pathlib.Path(path).resolve()
    if path_obj.is_absolute():
        return pathlib.Path(*path_obj.parts[1:])
    else:
        return path_obj


if sys.version_info[0] == 2:
    def _convert_time(strtime):
        from backports.datetime_timestamp import timestamp
        return timestamp(tparse(strtime))
else:
    def _convert_time(strtime):
        return tparse(strtime).timestamp()


class DictCache(dict):
    def close(self):
        pass


class CacheEntry:
    def __init__(self, obj: dict, listing: dict = None, pinned=False):
        self.obj = obj
        self.listing = listing
        self.loadTime = time.time()
        self.pinned = pinned

    def add_to_listing(self, entry: 'CacheEntry'):
        if self.listing is None:
            self.listing = {}
        self.listing[entry.obj['name']] = entry

    def __str__(self):
        return 'CacheEntry[obj_id=%s, listing=%s]' % (self.obj['_id'], self.listing)


class CacheWrapper:
    """
    A wrapper cache class that coerces keys to str in order to avoid unicode/str
    lookup errors.

    :param cache: The wrapped cache class
    :type cache: diskcache.Cache
    """
    def __init__(self, cache):
        self.cache = cache

    def __getitem__(self, item: str) -> CacheEntry:
        assert isinstance(item, str)
        return self.cache.__getitem__(item)

    def __setitem__(self, key: str, value: CacheEntry):
        assert isinstance(key, str)
        assert isinstance(value, CacheEntry)
        assert '_modelType' in value.obj, 'No _modelType for %s' % value.obj
        return self.cache.__setitem__(key, value)

    def __delitem__(self, key: str):
        assert isinstance(key, str)
        return self.cache.__delitem__(key)

    def clear(self):
        self.cache.clear()

    def close(self):
        self.cache.close()


class GirderFS(LoggingMixIn, Operations):
    """
    Base class for handling Girder's folders

    :param folder_id: Folder id
    :type folder_id: unicode
    :param girder_cli: Authenticated instance of GirderClient
    :type girder_cli: girder_client.GriderClient
    """
    def __init__(self, root_id, girder_cli, root_model='folder',
                 default_file_perm=0o644, default_dir_perm=0o755):
        super(GirderFS, self).__init__()
        self.root_id = root_id
        self.root_model = root_model
        self.girder_cli = girder_cli
        self.fd = 0
        self.default_file_perm = default_file_perm
        self.default_dir_perm = default_dir_perm
        self.cachedir = tempfile.mkdtemp(prefix='wtdm')
        self.cache = CacheWrapper(diskcache.Cache(self.cachedir))
        self.root = self._load_object(self.root_id, root_model, None)

    def _load_object(self, id: str, model: str, path: pathlib.Path):
        if model is None and id == self.root_id:
            model = self.root_model
        return self._add_model(model, self.girder_cli.get('/%s/%s' % (model, id)))

    def _add_model(self, model, obj):
        obj['_modelType'] = model
        return obj

    def _get_object_from_root(self, path: pathlib.Path) -> Tuple[dict, str]:
        logging.debug("-> _get_object_from_root({})".format(path))
        return self._get_object_by_path(self.root_id, path)

    def _get_object_by_path(self, obj_id: str, pathFromObj: pathlib.Path) -> Tuple[dict, str]:
        logging.debug("-> _get_object_by_path({}, {})".format(obj_id, pathFromObj))

        raw_listing = self._get_listing(obj_id, None)

        obj = self._find(pathFromObj.parts[0], raw_listing)

        if obj is None:
            raise FuseOSError(ENOENT)

        isFile = obj['_modelType'] in ['file', 'item']

        if len(pathFromObj.parts) == 1:
            return obj, obj['_modelType']
        else:
            if isFile:
                raise FuseOSError(ENOTDIR)
            return self._get_object_by_path(str(obj['_id']), pathlib.Path(*pathFromObj.parts[1:]))

    def _find(self, name, d: dict):
        if name in d:
            return d[name].obj
        else:
            return None

    def _get_listing(self, obj_id: str, path: pathlib.Path) -> dict:
        logging.debug("-> _get_listing({}, {})".format(obj_id, path))
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
            except:
                logging.error('Error updating cache', exc_info=True, stack_info=True)
                raise
        except Exception as ex:
            logging.error('Error getting cached listing', exc_info=True, stack_info=True)
            raise
        finally:
            return self.cache[obj_id].listing

    def _store_listing(self, entry: CacheEntry, listing: dict):
        l = {}
        for type in ['folders', 'files', 'links']:
            if type in listing:
                lst = listing[type]
                for de in lst:
                    id = str(de['_id'])
                    de['_modelType'] = type[:-1]
                    try:
                        existing = self.cache[id]
                        existing.obj = de
                    except KeyError:
                        existing = CacheEntry(de)
                        self.cache[id] = existing
                    l[de['name']] = existing
        entry.listing = l

    def _listing_is_current(self, entry: CacheEntry, path: pathlib.Path):
        if entry.listing is None:
            logging.debug("-> _listing_is_current({}, {}) - "
                          "no listing in entry".format(entry.obj['_id'], path))
            return False
        if path is not None and self._is_mutable_dir(entry.obj, path):
            logging.debug("-> _listing_is_current({}, {}) - "
                          "dir is mutable".format(entry.obj['_id'], path))
            if not self._object_is_current(entry, path):
                logging.debug("-> _listing_is_current({}, {}) - "
                              "object not current".format(entry.obj['_id'], path))
                return False
        logging.debug("-> _listing_is_current({}, {}) - "
                      "current".format(entry.obj['_id'], path))
        return True

    def _get_current_object(self, existing: dict):
        id = str(existing['_id'])
        try:
            entry = self.cache[id]
            self._object_is_current(entry, None)
            return entry.obj
        except KeyError:
            obj = self._load_object(id, existing['_modelType'])
            self.cache[id] = CacheEntry(obj)
            return obj

    def _object_is_current(self, entry: CacheEntry, path: pathlib.Path):
        if logging.isEnabledFor(logging.DEBUG):
            logging.debug("-> _object_is_current({})".format(entry.obj))
        now = time.time()
        if now - entry.loadTime < CACHE_ENTRY_STALE_TIME:
            logging.debug("-> _object_is_current({}) - too soon".format(path))
            return True
        obj = entry.obj
        newObj = self._load_object(str(obj['_id']), obj['_modelType'], path)
        if self._object_changed(newObj, obj):
            entry.obj = newObj
            logging.debug("-> _object_is_current({}) - changed".format(path))
            return False
        else:
            logging.debug("-> _object_is_current({}) - current".format(path))
            return True

    def _is_mutable_dir(self, obj: dict, path: pathlib.Path):
        return False

    def _object_changed(self, newObj, oldObj):
        logging.debug("-> _object_changed({}, {})".format(newObj['updated'], oldObj['updated']))
        return newObj['updated'] != oldObj['updated']

    def _girder_get_listing(self, obj: dict, path: pathlib.Path):
        return self.girder_cli.get('dm/fs/%s/listing' % obj['_id'])

    def getattr(self, spath: str, fh=None):
        logging.debug("-> getattr({})".format(spath))
        if spath == '/':
            now = _convert_time(str(datetime.datetime.now()))
            return dict(st_mode=(S_IFDIR | self._get_perm(spath, True)), st_nlink=2,
                        st_ctime=now, st_atime=now, st_mtime=now)
        obj, obj_type = self._get_object_from_root(_lstrip_path(spath))

        return self._getattr(spath, obj, obj_type)

    def _getattr(self, path: pathlib.Path, obj: dict, obj_type: str) -> dict:
        if obj_type == 'folder':
            stat = dict(st_mode=(S_IFDIR | self._get_perm(path, True)), st_nlink=2)
        elif obj_type == 'link':
            stat = dict(st_mode=(S_IFLNK | self._get_perm(path, True)), st_nlink=1)
        else:
            stat = dict(st_mode=(S_IFREG | self._get_perm(path, False)), st_nlink=1)
        ctime = _convert_time(obj["created"])
        try:
            mtime = _convert_time(obj["updated"])
        except KeyError:
            mtime = ctime
        stat.update(dict(st_ctime=ctime, st_mtime=mtime, st_blocks=1,
                         st_size=obj["size"], st_atime=time.time()))
        return stat

    def _get_perm(self, path, is_dir):
        if is_dir:
            return self.default_dir_perm
        else:
            return self.default_file_perm

    def read(self, path, size, offset, fh):
        raise NotImplementedError

    def readdir(self, path, fh):
        logging.debug("-> readdir({})".format(path))
        dirents = ['.', '..']
        raw_listing = self._get_listing_by_path(path)
        logging.debug('raw_listing: %s' % raw_listing)
        dirents += raw_listing.keys()
        return dirents

    def _get_listing_by_path(self, spath):
        logging.debug("-> _get_listing_by_path({})".format(spath))
        if spath == '/':
            return self._get_listing(self.root_id, ROOT_PATH)
        else:
            path = _lstrip_path(spath)
            obj, obj_type = self._get_object_from_root(path)
            return self._get_listing(str(obj['_id']), path)

    def getinfo(self, path):
        logging.debug("-> getinfo({})".format(path))
        '''Pyfilesystem essential method'''
        if not path.startswith('/'):
            path = '/' + path
        return self.getattr(path)

    def listdir(self, path='./', wildcard=None, full=False, absolute=False,
                dirs_only=False, files_only=False):
        logging.debug("-> listdir({})".format(path))

        if not path.startswith('/'):
            path = '/' + path
        list_dir = self.listdirinfo(path, wildcard=wildcard, full=full,
                                    absolute=absolute, dirs_only=dirs_only,
                                    files_only=files_only)
        return [_[0] for _ in list_dir]

    def listdirinfo(self, path='./', wildcard=None, full=False, absolute=False,
                    dirs_only=False, files_only=False):
        '''
        Pyfilesystem non-essential method

        Retrieves a list of paths and path info under a given path.

        This method behaves like listdir() but instead of just returning the
        name of each item in the directory, it returns a tuple of the name and
        the info dict as returned by getinfo.

        This method may be more efficient than calling getinfo() on each
        individual item returned by listdir(), particularly for network based
        filesystems.
        '''

        logging.debug("-> listdirinfo({})".format(path))

        def _get_stat(obj):
            ctime = _convert_time(obj["created"])
            try:
                mtime = _convert_time(obj["updated"])
            except KeyError:
                mtime = ctime
            return dict(st_ctime=ctime, st_mtime=mtime,
                        st_size=obj["size"], st_atime=time.time())

        listdir = []
        raw_listing = self._get_listing_by_path(path)

        for obj in raw_listing['files']:
            stat = dict(st_mode=(S_IFREG | self.default_file_perm), st_nlink=1)
            stat.update(_get_stat(obj))
            listdir.append((obj['name'], stat))

        for obj in raw_listing['folders']:
            stat = dict(st_mode=(S_IFDIR | self.default_dir_perm), st_nlink=2)
            stat.update(_get_stat(obj))
            listdir.append((obj['name'], stat))

        return listdir

    def isdir(self, path):
        '''Pyfilesystem essential method'''
        logging.debug("-> isdir({})".format(path))
        attr = self.getattr(path)
        return attr['st_nlink'] == 2

    def isfile(self, path):
        '''Pyfilesystem essential method'''
        logging.debug("-> isfile({})".format(path))
        attr = self.getattr(path)
        return attr['st_nlink'] == 1

    def destroy(self, private_data):
        logging.debug("-> destroy()")
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


class RESTGirderFS(GirderFS):
    """
    Filesystem for locally mounting a remote Girder folder

    :param folder_id: Folder id
    :type folder_id: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def read(self, path, size, offset, fh):
        logging.debug("-> read({})".format(path))
        obj, _ = self._get_object_from_root(_lstrip_path(path))
        cacheKey = '#'.join((obj['_id'], obj.get('updated', obj['created'])))
        fp = self.cache.get(cacheKey, read=True)
        if fp:
            logging.debug(
                '-> hitting cache {} {} {}'.format(path, size, offset))
            fp.seek(offset)
            return fp.read(size)
        else:
            logging.debug('-> downloading')
            req = requests.get(
                '%sitem/%s/download' % (self.girder_cli.urlBase, obj["_id"]),
                headers={'Girder-Token': self.girder_cli.token}, stream=True)
            with tempfile.NamedTemporaryFile(prefix='wtdm', delete=False) as tmp:
                for chunk in req.iter_content(chunk_size=65536):
                    tmp.write(chunk)
            with open(tmp.name, 'rb') as fp:
                self.cache.set(cacheKey, fp, read=True)
                os.remove(tmp.name)
                fp.seek(offset)
                return fp.read(size)

    def open(self, path, mode="r", **kwargs):
        logging.debug("-> open({}, {})".format(path, self.fd))
        self.fd += 1
        return self.fd

    def destroy(self, private_data):
        GirderFS.destroy(self, private_data)

    def release(self, path, fh):  # pylint: disable=unused-argument
        logging.debug("-> release({}, {})".format(path, self.fd))
        self.fd -= 1
        return self.fd


class DownloadThread(threading.Thread):
    def __init__(self, path, stream, fdict, lock, fs):
        threading.Thread.__init__(self)
        fdict['downloadThread'] = self
        self.path = path
        self.stream = stream
        self.fdict = fdict
        self.lock = lock
        self.limit = six.MAXSIZE
        self.fs = fs

    def run(self):
        with tempfile.NamedTemporaryFile(prefix='wtdm', delete=False) as tmp:
            self.fdict['path'] = tmp.name
            #  print self.stream.__dict__
            for chunk in self.stream.iter_content(chunk_size=65536):
                tmp.write(chunk)
                self.fdict['written'] += len(chunk)
                if self.fdict['written'] > self.limit:
                    tmp.truncate(self.limit)
                    break

            with self.lock:
                self.fdict['downloaded'] = True
                self.fdict['downloading'] = False
                if self.fdict['written'] > self.limit:
                    # truncate if we downloaded too much
                    tmp.truncate(self.limit)
                del self.fdict['downloadThread']
        self.fs.downloadCompleted(self.path, self.fdict)

    def setLimit(self, length):
        self.lock.assertLocked()
        self.limit = length
        if self.fdict['downloaded']:
            # download done, so run() can't truncate if necessary
            f = open(self.fdict['path'], 'a+b')
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
            raise Exception('Not a re-entrant lock')
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
            return 'Lock - not locked'
        else:
            return 'Lock - locked by ' + str(self.thread)

    def assertLocked(self):
        if not self.lock.locked():
            raise Exception('Lock assertion failed')


class WtDmsGirderFS(GirderFS):
    """
    Filesystem for locally mounting a remote Girder folder

    :param sessionId: WT DMS session id
    :type sessoinId: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def __init__(self, sessionId, gc, default_file_perm=0o644, default_dir_perm=0o755):
        GirderFS.__init__(self, six.text_type(sessionId), gc, root_model='session',
                          default_file_perm=default_file_perm, default_dir_perm=default_dir_perm)
        self.sessionId = sessionId
        self.flock = MLock()
        self.openFiles = {}
        self.locks = {}
        self.fobjs = {}
        self.ctime = int(time.time())
        self.cache = CacheWrapper(DictCache())

    def _load_object(self, id: str, model: str, path: pathlib.Path):
        if id == self.root_id:
            root = self.girder_cli.get('dm/session/%s?loadObjects=true' % self.root_id)
            self._populate_mount_points(root['dataSet'])
            return self._add_model('folder', root)
        else:
            return super()._load_object(id, model, path)

    def _populate_mount_points(self, dataSet: dict) -> None:
        # mount points can have arbitrary depth, so pre-populate the cache with
        # the necessary directory structure
        for entry in dataSet:
            self._add_session_entry(self.root_id, entry)

    def _fake_obj(self, fs_type: str, name=None, id=None, ctime=None):
        if ctime is None:
            ctime = self.ctime
        if id is None:
            id = uuid.uuid1()
        return {'_id': id, 'name': name, 'created': ctime}

    def _add_session_entry(self, id: str, dse, prefix: List[str] = None):
        if logging.isEnabledFor(logging.DEBUG):
            logging.debug("-> _add_session_entry({}, {}, {})".format(id, dse, prefix))
        # assume and strip root slash
        path = list(pathlib.PurePath(dse['mountPath']).parts)[1:]
        if prefix is not None:
            path = prefix + path
        entry = self._cache_mkdirs(id, path[:-1])
        obj = dse['obj']
        obj['_modelType'] = dse['type']
        subEntry = CacheEntry(obj)
        self.cache[str(obj['_id'])] = subEntry
        entry.add_to_listing(subEntry)
        if logging.isEnabledFor(logging.DEBUG):
            logging.debug("-> _add_session_entry updated entry is {}".format(entry))

    def _cache_mkdirs(self, id: str, path: List[str]):
        logging.debug("-> _cache_mkdirs({}, {})".format(id, path))
        entry = self.cache[id]
        if len(path) == 0:
            return entry
        crt = path[0]

        obj = self._fake_obj(fs_type='folder', name=crt)
        subEntry = CacheEntry(obj)
        entry.add_to_listing(subEntry)
        self.cache[str(obj['_id'])] = subEntry

        return self._cache_mkdirs(str(obj['_id']), path[1:])

    def _is_mutable_dir(self, obj: dict, path: pathlib.Path):
        return path == ROOT_PATH

    def _object_changed(self, newObj, oldObj):
        if oldObj['_modelType'] == 'session':
            oldSeq = oldObj['seq'] if 'seq' in oldObj else None
            newSeq = newObj['seq'] if 'seq' in newObj else None
            return oldSeq != newSeq
        else:
            return super()._object_changed(newObj, oldObj)

    def read(self, path, size, offset, fh):
        logging.debug("-> read({}, offset={}, size={})".format(path, offset, size))

        fdict = self.openFiles[path]

        self._ensure_region_available(path, fdict, fh, offset, size)

        if fh not in self.fobjs:
            self.fobjs[fh] = open(fdict['path'], 'r+b')
        fp = self.fobjs[fh]

        fp.seek(offset)
        return fp.read(size)

    def _ensure_region_available(self, path, fdict, fh, offset, size):
        self._wait_for_file(fdict)

        if not fdict['downloaded']:
            download = False
            with self.flock:
                if not fdict['downloading']:
                    download = True
                    fdict['downloading'] = True
            if download:
                try:
                    lockId = self.locks[fh]
                except KeyError:
                    raise FuseOSError(EIO)  # TODO: debug why it happens
                self._start_download(path, lockId)

        self._wait_for_region(path, fdict, offset, size)

    def _wait_for_file(self, fdict):
        # Waits for the file to be downloaded/cached by the DMS
        obj = fdict['obj']
        while True:
            try:
                if obj['dm']['cached']:
                    return obj
            except KeyError:
                pass

            time.sleep(1.0)
            obj = self._get_item_unfiltered(obj['_id'])
            fdict['obj'] = obj

            try:
                if obj['dm']['transferError']:
                    raise OSError(EIO, os.strerror(EIO))
            except KeyError:
                pass

    def _wait_for_region(self, path, fdict, offset, size):
        # Waits until enough of the file has been downloaded locally
        # to be able to read the necessary chunk of bytes
        logging.debug("-> wait_for_region({}, {}, {})".format(path, offset, size))
        while True:
            if fdict['downloaded']:
                return
            if fdict['written'] >= offset + size:
                return
            time.sleep(0.1)

    def _get_item_unfiltered(self, id):
        return self.girder_cli.get('dm/fs/%s/raw' % id)

    def open(self, path, mode=os.O_RDONLY, **kwargs):
        fd = self._next_fd()
        logging.debug("-> open({}, {})".format(path, fd))
        obj, _ = self._get_object_from_root(_lstrip_path(path))
        obj = self._get_item_unfiltered(self._get_id(obj))
        self._open(path, fd, obj)
        return fd

    def _get_id(self, obj):
        if 'itemId' in obj:
            return obj['itemId']
        else:
            return obj['_id']

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
            available = obj['dm']['cached']
        except KeyError:
            available = False
        with self.flock:
            fdict = self._ensure_fdict(path, obj)
            downloaded = fdict['downloaded']
            downloading = fdict['downloading']
            # set downloading flag with lock held
            if available and not downloading and not downloaded:
                fdict['downloading'] = True
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
            self.openFiles[path] = {'obj': obj, 'written': 0, 'downloading': False,
                                    'downloaded': False, 'path': None}
        fdict = self.openFiles[path]
        fdict['obj'] = obj
        return fdict

    def _start_download(self, path, lockId):
        with self.flock:
            fdict = self.openFiles[path]
            fdict['downloading'] = True
            fdict['written'] = 0

        req = requests.get(
            '%sdm/lock/%s/download' % (self.girder_cli.urlBase, lockId),
            headers={'Girder-Token': self.girder_cli.token}, stream=True)
        thread = DownloadThread(path, req, self.openFiles[path], self.flock, self)
        thread.start()

    def _lock(self, path, obj):
        resp = self.girder_cli.post('dm/lock?sessionId=%s&itemId=%s' % (self.root_id, obj['_id']))
        return resp['_id']

    def _unlock(self, lockId):
        self.girder_cli.delete('dm/lock/%s' % (lockId))

    def destroy(self, private_data):
        GirderFS.destroy(self, private_data)

        for path in list(self.openFiles.keys()):
            fdict = self.openFiles[path]
            try:
                if 'path' in fdict:
                    fpath = fdict['path']
                    logging.debug('-> destroy: removing {}'.format(fpath))
                    os.remove(fpath)
                else:
                    logging.debug('-> destroy: no physical path for {}'.format(path))
            except:
                pass

    def release(self, path, fh):  # pylint: disable=unused-argument
        logging.debug("-> release({}, {})".format(path, fh))

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
        obj = fdict['obj']
        # this will break if anything writes to the file during upload
        # same if anything deletes the file. For now, let it fail with an error.
        # TODO: cancel uploads when a file is opened w/a
        fp = open(fdict['path'], 'a+b')
        fp.seek(0, os.SEEK_END)
        size = fp.tell()
        fp.seek(0, os.SEEK_SET)
        if logging.isEnabledFor(logging.DEBUG):
            logging.debug('-> _upload({}, size={})'.format(obj, size))
        if self._is_item(obj):
            obj = self._get_file(obj)
        self.cli.uploadFileContents(obj['_id'], fp, size)
        fp.close()
        self.cli.get('dm/fs/%s/evict' % fdict['obj']['_id'])

    def _is_item(self, obj):
        return 'folderId' in obj

    def _get_file(self, obj):
        files = list(self.cli.listFile(obj['_id']))
        if len(files) != 1:
            raise Exception('Expected a single file in item %s' % obj['_id'])
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
            logging.error('Object not in cache "%s" (%s)' % (id, type(id)))

    def _load_root(self):
        return None

    def _get_root_listing(self):
        return None

    def statfs(self, path):
        # default results in a read-only fs
        return {'f_flag': os.ST_NOATIME + os.ST_NODEV + os.ST_NODIRATIME + os.ST_NOSUID}

    def chmod(self, path, mode):
        logging.debug("-> chmod({}, {})".format(path, mode))

        path = _lstrip_path(path)
        self._set_metadata(path, {'permissions': mode})

    def _set_metadata(self, path, dict):
        obj, objType = self._get_object_from_root(path)
        self._set_object_metadata(obj, objType, dict)

    def _set_object_metadata(self, obj, objType, dict):
        self.girder_cli.put('dm/fs/%s/setProperties' % obj['_id'], json=dict)
        obj.update(dict)
        return obj

    def getattr(self, path, fh=None):
        logging.debug("-> getattr({})".format(path))

        if path == '/':
            now = _convert_time(str(datetime.datetime.now()))
            return dict(st_mode=(S_IFDIR | self._get_perm(path, True)), st_nlink=2,
                        st_ctime=now, st_atime=now, st_mtime=now)
        obj, objType = self._get_object_from_root(_lstrip_path(path))

        if objType == 'folder':
            stat = dict(st_mode=(S_IFDIR | self._get_prop(obj, 'permissions',
                                                          self._get_perm(path, True))), st_nlink=2)
        else:
            stat = dict(st_mode=(S_IFREG | self._get_prop(obj, 'permissions',
                                                          self._get_perm(path, False))), st_nlink=1)
        ctime = _convert_time(obj['created'])
        try:
            mtime = _convert_time(obj['updated'])
        except KeyError:
            mtime = ctime
        size = obj['size']
        with self.flock:
            if path in self.openFiles:
                fdict = self.openFiles[path]
                if 'localsize' in fdict:
                    # assume local file is always the most current if it exists
                    size = fdict['localsize']
        stat.update(dict(st_ctime=ctime, st_mtime=mtime, st_blocks=1,
                         st_size=size, st_atime=time.time()))
        return stat

    def _get_prop(self, obj, name, default):
        if name in obj:
            return obj[name]
        else:
            return default

    def create(self, pathstr, mode, fi=None):
        logging.debug("-> create({}, {})".format(pathstr, mode))

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

        if objType == 'folder':
            raise FuseOSError(EISDIR)
        # TODO: fix race

        if objType == 'file':
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
            with tempfile.NamedTemporaryFile(prefix='wtdm', delete=False) as tmp:
                fdict['path'] = tmp.name
            self._mark_dirty(pathstr)
            return self.open(pathstr, mode=os.O_CREAT + os.O_WRONLY + os.O_TRUNC)

    def _get_object_id_by_path(self, obj_id, path):
        if len(path.parts) == 0:
            return self.folder_id
        obj, _ = self._get_object_id_by_path(path)
        return obj['_id']

    def _cache_get_parent_id(self, path):
        return self._get_object_id_by_path(path.parent)

    def _cache_add_file(self, obj, path):
        self._cache_add_obj(obj, path, 'files')

    def _cache_add_dir(self, obj, path):
        self._cache_add_obj(obj, path, 'folders')

    def _cache_add_obj(self, obj, path, type):
        with self.flock:
            parentId = str(self._get_parent_id(path))
            dict = self.cache[parentId]
            lst = dict[type]
            lst.append(obj)
            self.cache[parentId] = dict

    def _cache_remove_dir(self, path):
        self._cache_remove_obj(path, 'folders')

    def _cache_remove_file(self, path):
        self._cache_remove_obj(path, 'files')

    def _cache_remove_obj(self, path, type):
        with self.flock:
            parentId = str(self._get_parent_id(path))
            obj = self.cache[parentId]
            lst = obj[type]
            for i in range(len(lst)):
                if lst[i]['name'] == path.name:
                    lst.pop(i)
                    self.cache[parentId] = obj
                    return
            raise Exception('Could not remove object from cache: %s' % path)

    def _mark_dirty(self, path):
        # sets a flag that will trigger an upload when the file is closed
        fdict = self.openFiles[path]
        fdict['dirty'] = True

    def _create(self, path, parentId, perms):
        logging.debug("-> _create({}, {}, {})".format(path, parentId, perms))
        item = self.girder_cli.createItem(parentId, path.name)
        print(item)
        file = self.girder_cli.post('file', parameters={'parentType': 'item',
                                                        'parentId': item['_id'],
                                                        'name': path.name, 'size': 0})
        self._set_object_metadata(file, 'file', {'permissions': perms})
        return item

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    def fsyncdir(self, path, datasync, fh):
        return 0

    def mkdir(self, path, mode):
        logging.debug("-> mkdir({}, {})".format(path, mode))

        path = _lstrip_path(path)
        parentId = self._get_parent_id(path)

        # It's somewhat silly that you can't set other folder parameters with this call.
        # The two-step solution is a race waiting to happen and possibly a security issue
        # since there is no way to atomicaly create a locked-down directory.
        # The better thing may be to abandon REST and add an API call that allows this to be
        # done in one step. Although I suspect this will be a small problem once we
        # think about having multiple clients syncing to the same home-dir.
        obj = self.girder_cli.post(
            'folder', parameters={'parentId': parentId, 'name': path.name})
        self._set_metadata(path, {'permissions': mode})
        self._cache_add_dir(obj, path)

    def _get_parent_id(self, path):
        if len(path.parent.parts) == 0:
            return self.folder_id
        else:
            obj, objType = self._get_object_from_root(path.parent)
            return obj['_id']

    def rmdir(self, path):
        logging.debug("-> rmdir({})".format(path))

        path = _lstrip_path(path)
        if len(path.parts) == 0:
            raise FuseOSError(EPERM)

        obj, objType = self._get_object_from_root(path)
        # should probably check if it's empty
        self.girder_cli.delete('%s/%s' % (objType, obj['_id']))
        self._cache_remove_dir(path)

    def mknod(self, path, mode, dev):
        raise FuseOSError(EPERM)

    def rename(self, old, new):
        logging.debug("-> rename({}, {})".format(old, new))

        path = _lstrip_path(old)
        if len(path.parts) == 0:
            raise FuseOSError(EPERM)

        obj, objType = self._get_object_from_root(path)
        self.girder_cli.put('%s/%s' % (objType, obj['_id']), parameters={'name': new})
        obj['name'] = new

    def truncate(self, path, length, fh=None):
        logging.debug("-> truncate({}, {}, {})".format(path, length, fh))
        # so fh=None means truncate() whereas fh != None means ftruncate

        # the basic workflow is to do i/o locally and commit when all of the following are true:
        #  - no active downloads
        #  - the file is not open

        pathObj = _lstrip_path(path)
        obj, objType = self._get_object_from_root(pathObj)

        if objType == 'folder':
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
        logging.debug("-> _ftruncate({}, {}, {})".format(path, fh, length))
        fp = None
        with self.flock:
            fdict = self.openFiles[path]
            self._update_size(path, fdict, length)
            if fdict['downloading']:
                # simply stop downloading after the limit
                fdict['downloadThread'].setLimit(length)
                self._mark_dirty(path)
                return
            else:
                if fh in self.fobjs:
                    fp = self.fobjs[fh]
        if fp is None:
            fp = open(fdict['path'], 'r+b')
        fp.truncate(length)
        self._mark_dirty(path)

    def unlink(self, path):
        logging.debug("-> unlink({})".format(path))
        path = _lstrip_path(path)
        obj, objType = self._get_object_from_root(path)
        # should be the parent item
        self.girder_cli.delete('item/%s' % (obj['_id']))
        self._cache_remove_file(path)

    def write(self, path, data, offset, fh):
        logging.debug("-> write({}, {}, {})".format(path, fh, offset))
        fdict = self.openFiles[path]
        size = len(data)

        if fdict['downloading']:
            self._ensure_region_available(path, fdict, fh, offset, size)

        if fh not in self.fobjs:
            self.fobjs[fh] = open(fdict['path'], 'a+b')
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
        fdict['localsize'] = sz

    def release(self, path, fh):  # pylint: disable=unused-argument
        logging.debug("-> release2({}, {})".format(path, fh))
        with self.flock:
            writers = self._remove_writer(path, fh)
            fdict = self.openFiles[path]
            if not fdict['downloading'] and writers == 0:
                self._commit(path, fdict)
            else:
                # still downloading
                pass

        return WtDmsGirderFS.release(self, path, fh)

    def downloadCompleted(self, path, fdict):
        logging.debug("-> downloadCompleted(path=%s)" % (path))
        with self.flock:
            fdict['cached.locally'] = True
            if len(fdict['fds']) == 0:
                self._commit(path, fdict)

    def open(self, path, mode=os.O_RDONLY, **kwargs):
        logging.debug("-> open(path=%s, mode=%s)" % (path, mode))
        fd = WtDmsGirderFS.open(self, path, mode=mode, **kwargs)
        fdict = self.openFiles[path]
        if 'fds' not in fdict:
            fdict['fds'] = set()
        return fd

    def _add_writer(self, path, fd):
        with self.flock:
            fdict = self.openFiles[path]
            fdict['fds'].add(fd)

    def _remove_writer(self, path, fd):
        self.flock.assertLocked()
        fdict = self.openFiles[path]
        if fd in fdict['fds']:
            fdict['fds'].remove(fd)
        else:
            # no writes actually happened in this open/close session
            pass
        return len(fdict['fds'])

    def _commit(self, path, fdict):
        logging.debug("-> _commit({}, {}".format(path, fdict))
        fdict = self.openFiles[path]
        if 'dirty' in fdict and fdict['dirty']:
            self.flock.assertLocked()
            fdict['uploading'] = True
            self.uploadQueue.put((path, fdict))
            fdict['dirty'] = False

    def _wait_for_file(self, fdict):
        # Override because, except for the first download, the local
        # copy is always considered to be the latest
        if 'cached.locally' in fdict:
            return
        else:
            WtDmsGirderFS._wait_for_file(self, fdict)


class LocalGirderFS(GirderFS):
    """
    Filesystem for mounting local Girder's FilesystemAssetstore

    :param folder_id: Folder id
    :type folder_id: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def open(self, path, mode="r", **kwargs):
        logging.debug("-> open({})".format(path))
        obj, _ = self._get_object_from_root(_lstrip_path(path))
        return os.open(obj['path'], os.O_RDONLY)
        # return open(obj['path'], mode)   # pyfilesystem

    def read(self, path, size, offset, fh):
        logging.debug("-> read({})".format(path))
        obj, _ = self._get_object_from_root(_lstrip_path(path))
        fh = os.open(obj['path'], os.O_RDONLY)
        os.lseek(fh, offset, 0)
        return os.read(fh, size)

    def release(self, path, fh):  # pylint: disable=unused-argument
        logging.debug("-> release({})".format(path))
        return os.close(fh)


def _getSessionIdFromInstanceId(instanceId: str, gc: GirderClient) -> str:
    instance = gc.get('instance/%s' % instanceId)
    return instance['sessionId']


# These are copied and pasted from virtual_resources. There should probably be a better way
def _generate_id(path, root_id):
    if isinstance(path, pathlib.Path):
        path = path.as_posix()
    path += "|" + str(root_id)
    return "wtlocal:" + base64.b64encode(path.encode()).decode()


def _path_from_id(object_id):
    decoded = base64.b64decode(object_id[8:]).decode()
    path, root_id = decoded.split("|")
    return pathlib.Path(path), root_id


class WtVersionsFS(WtDmsGirderFS):

    def __init__(self, tale_id, gc):
        self.tale_id = tale_id
        WtDmsGirderFS.__init__(self, tale_id, gc,
                               default_file_perm=0o444, default_dir_perm=0o555)
        self.session_id = None  # TODO: fixme
        self.cached_versions = {}

    @property
    def _resource_name(self):
        return 'version'

    def _load_object(self, id: str, model: str, path: pathlib.Path):
        if id == self.root_id:
            versions_folder = self.girder_cli.get(
                f"{self._resource_name}/root", parameters={"taleId": self.tale_id}
            )
            self.root_id = str(versions_folder['_id'])
            return self._add_model('folder', versions_folder)
        else:
            return super()._load_object(id, model, path)

    def _girder_get_listing(self, obj: dict, path: pathlib.Path):
        if str(obj['_id']) == self.root_id:
            return {
                'folders': self.girder_cli.get('%s?rootId=%s' %
                                               (self._resource_name, self.root_id)),
                'files': []
            }
        elif str(obj['parentId']) == self.root_id:
            # preload and cache session
            sid = str(obj['_id'])
            try:
                return self.cache[sid + '-data'].listing
            except KeyError:
                pass
            # a version; use this opportunity to pre-populate the cache with information
            # from the session
            params = {'name': 'data', 'parentId': str(obj['_id']), 'parentType': 'folder'}
            data_folder = self.girder_cli.get('folder', parameters=params)[0]
            data_set = self.girder_cli.get('%s/%s/dataSet' % (self._resource_name, obj['_id']))

            dfid = str(data_folder['_id'])
            entry = CacheEntry(data_folder, pinned=True)
            self.cache[dfid] = entry

            for dse in data_set:
                self._add_session_entry(dfid, dse)
            listing = super()._girder_get_listing(obj, path)
            self.cache[sid + '-data'] = CacheEntry(data_folder, listing=listing)
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
        return len(path.parts) > 3 and path.parts[2] == 'data'

    def rename(self, old, new):
        old = pathlib.Path(old)
        if not self._is_version(old):
            raise FuseOSError(EPERM)
        new = pathlib.Path(new)
        if not self._is_version(new):
            raise FuseOSError(EPERM)
        obj, _ = self._get_object_from_root( _lstrip_path(old))
        del self.cache[self.root_id]
        if new == '/.trash':
            # We can't really implement removal of versions using normal delete operations,
            # since that is done by recursive removal of files and directories, files and
            # directories which cannot otherwise be removed or modified individually. Instead,
            # removal is implemented as a single operation consisting of moving a version to
            # a special directory. Incidentally, the version delete operation does, in fact,
            # move the deleted version to a .trash directory. Undelete is not currently exposed,
            # but reasonable design dictates that we eventually do expose it.
            self.girder_cli.delete('%s/%s' % (self._resource_name, obj['_id']))
        else:
            self.girder_cli.put('%s/%s' % (self._resource_name, obj['_id']),
                                {'newName': new.parts[-1]})

    def _get_perm(self, path, is_dir):
        if is_dir and path == '/':
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
        if model not in ['file', 'item']:
            raise FuseOSError(EISDIR)
        with self.flock:
            fdict = self._ensure_fdict(spath, file)
            fdict['cached.locally'] = True
            downloaded = fdict['downloaded']
            downloading = fdict['downloading']
            # set downloading flag with lock held
            if not downloading and not downloaded:
                fdict['downloading'] = True
                fdict['written'] = 0
        if downloaded:
            # file is here, so no need to download again
            pass
        else:
            if downloading:
                return
            else:
                req = requests.get(
                    '%sfile/%s/download' % (self.girder_cli.urlBase, file['_id']),
                    headers={'Girder-Token': self.girder_cli.token}, stream=True)
                thread = DownloadThread(spath, req, fdict, self.flock, self)
                thread.start()

    def _is_mutable_file(self, path: pathlib.Path) -> bool:
        return False

    def _reload_file(self, file: dict) -> dict:
        logging.debug("-> _reload({})".format(file['_id']))
        return self.girder_cli.get('file/%s' % file['_id'])

    def _wait_for_file(self, fdict):
        if 'cached.locally' in fdict:
            return
        else:
            super()._wait_for_file(fdict)

    def _lock(self, path, obj):
        resp = self.girder_cli.post('dm/lock?sessionId=%s&itemId=%s' %
                                    (self.session_id, obj['_id']))
        return resp['_id']

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


class ActiveDownload:
    def __init__(self, start: int, len: int):
        self.start = start
        self.len = len
        self.cond = threading.Condition()
        self.done = False

    def overlaps(self, start, len):
        s = max(self.start, start)
        e = min(self.start + self.len, start + len)
        return s < e

class CacheFile:
    BLOCK_SIZE = 4096
    block_size_check_done = False

    def __init__(self, size: int):
        if not CacheFile.block_size_check_done:
            self._run_block_size_check()
        f = tempfile.NamedTemporaryFile(delete=False)
        self.name = f.name
        self.size = size
        f.truncate(size)
        f.close()
        self.active_downloads = {}
        self.openers = {}
        self.invalid = False
        self.lock = threading.Lock()

    def _run_block_size_check(self):
        # When using sparse files, data chunks get written in multiples of the file block size.
        # For example, let's say we have an empty sparse file. We write a single byte at the
        # beginning of the file. If we wanted to use SEEK_HOLE/SEEK_DATA to figure out if that
        # second byte was cached, we would check that seek(1, SEEK_DATA) == 1 and
        # seek(1, SEEK_HOLE) > 1, which should not happen if we only wrote the first byte. However,
        # it does, since the FS writes a block at a time, so writing one byte is indistinguishable
        # using SEEK_DATA/SEEK_HOLE from writing n bytes, as long as the n bytes stay within one
        # block. This means we need to cache BLOCK_SIZE bytes at a time (except for the end of
        # the file). But this behavior isn't clearly documented, as far as I can tell. So we need
        # to run a test and make sure it works that way.
        with tempfile.NamedTemporaryFile() as f:
            f.truncate(CacheFile.BLOCK_SIZE * 10)
            f.seek(0, os.SEEK_SET)
            # write a zero byte and make sure it actually triggers a data block to be created
            f.write(b'\x00')
            assert f.seek(10, os.SEEK_DATA) == 10, \
                'Sparse file block check #1 failed'
            assert f.seek(10, os.SEEK_HOLE) == CacheFile.BLOCK_SIZE, \
                'Sparse file block check #2 failed'
        CacheFile.block_size_check_done = True

    def adjust_and_start_download(self, fh: int, start: int, len: int):
        logging.debug("-> adjust_and_start_download({}, {}, {}, {})".format(self.name, fh, start, len))
        while True:
            # wait for overlapping transfers
            w = None
            logging.debug("-> adjust_and_start_download - wait for lock")
            with self.lock:
                for d in self.active_downloads.values():
                    if d.overlaps(start, len):
                        logging.debug("-> adjust_and_start_download - overlaps {}".format(d))
                        w = d
                if w is None:
                    logging.debug("-> adjust_and_start_download - no overlapping downloads")
                    start, len = self._adjust_download(fh, start, len)
                    if len > 0:
                        self._begin_download(fh, start, len)
                    return start, len
            # w is not None
            with w.cond:
                while not w.done:
                    logging.debug("-> adjust_and_start_download - waiting for {}".format(w))
                    w.cond.wait()


    def _adjust_download(self, fh: int, start: int, len: int):
        assert self.lock.locked()
        logging.debug("-> _adjust_download({}, {}, {}, {})".format(self.name, fh, start, len))
        f = open(self.name, 'rb')
        end = start + len
        # read() might be invoked with a size that gets us past the end of file, so adjust
        # accordingly
        if end > self.size:
            end = self.size
            len = end - start
        try:
            r = f.seek(start, os.SEEK_DATA)
        except OSError as ex:
            if ex.errno == 6:
                # no data in this file
                r = -1
            else:
                # not something we'd normally expect
                raise
        if r == start:
            # the beginning is already cached, so find out where it starts not being cached
            r = f.seek(start, os.SEEK_HOLE)
            if r >= end:
                # contiguous data until (or after) the end, so all is cached
                return 0, 0
            else:
                # hole starts before the end, so that's our new start
                start = r
                # now, find out where we should stop
                r = f.seek(end, os.SEEK_HOLE)
                if r == end:
                    # end is in a hole, so no need to adjust
                    return self._align(start, end)
                else:
                    # find the hard way
                    end = self._left_seek_data(f, start + 1, end)
                    return self._align(start, end)
        else:
            # r != start, so we were in a hole; we must start at start
            if end == self.size:
                return self._align(start, end)

            r = f.seek(end, os.SEEK_HOLE)
            if r == end:
                # end is also in a hole, so download everything
                return self._align(start, end)
            else:
                # end is not in a hole; hard way again
                end = self._left_seek_data(f, start + 1, end)
                return self._align(start, end)

    def _align(self, start, end):
        r = start % CacheFile.BLOCK_SIZE
        start -= r

        r = end % CacheFile.BLOCK_SIZE
        if r != 0:
            end += CacheFile.BLOCK_SIZE - r
        if end > self.size:
            end = self.size

        return start, end - start

    def _left_seek_data(self, f, start, end):
        crtmod = os.SEEK_DATA
        crtpos = start
        while crtpos < end:
            last = crtpos
            crtpos = f.lseek(crtpos, crtmod)
            if crtmod == os.SEEK_HOLE:
                crtmod = os.SEEK_DATA
            else:
                crtmod = os.SEEK_HOLE
        return last

    def _begin_download(self, fh: int, start: int, len: int):
        assert self.lock.locked()
        self.active_downloads[fh] = ActiveDownload(start, len)

    def end_download(self, fh):
        logging.debug("-> _end_download({}, {})".format(self.name, fh))
        delete = True
        with self.lock:
            d = self.active_downloads[fh]
            del self.active_downloads[fh]
            if not self.invalid:
                delete = False
            if len(self.active_downloads) > 0:
                delete = False

        with d.cond:
            d.done = True
            d.cond.notify_all()
        if delete:
            os.remove(self.name)

    def invalidate(self):
        with self.lock:
            self.invalid = True
            if len(self.active_downloads) > 0:
                return
        os.remove(self.name)

    def __str__(self, start: int = -1, len: int = -1, char: str = 'O'):
        with self.lock:
            return self.__xstr__(start, len, char)

    def __xstr__(self, start: int = -1, len: int = -1, char: str = 'O'):
        s = '['
        assert self.lock.locked()
        with open(self.name, 'rb') as f:
            p = 0
            while p < self.size:
                if start <= p < start + len:
                    s += char
                elif p == f.seek(p, os.SEEK_DATA):
                    s += '#'
                else:
                    s += '_'
                p += CacheFile.BLOCK_SIZE
        s += ']'
        return s


class WtRunsFS(WtVersionsFS):

    def __init__(self, tale_id, gc, versions_mountpoint):
        WtVersionsFS.__init__(self, tale_id, gc)
        versions_path = pathlib.Path(versions_mountpoint)
        if not versions_path.is_absolute():
            # we'll use this from Runs/<run>/, so move this up a bit
            self.versions_mountpoint = ('../..' / versions_path).as_posix()
        else:
            self.versions_mountpoint = versions_mountpoint

    @property
    def _resource_name(self):
        return 'run'

    def _girder_get_listing(self, obj: dict, path: pathlib.Path):
        # override to bypass the session caching stuff from the versions FS
        return WtDmsGirderFS._girder_get_listing(self, obj, path)

    def _get_object_from_root(self, path: pathlib.Path) -> Tuple[dict, str]:
        logging.debug("-> _get_object_from_root({})".format(path))
        (obj, type) = self._get_object_by_path(self.root_id, path)
        if type in ['file', 'item']:
            # most of these are obtained through listings, so this is an appropriate place
            # to check for mutability
            if self._is_mutable_file(path):
                logging.debug("-> _get_object_from_root({}) - is mutable".format(path))
                id = str(obj['_id'])
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
        if model not in ['file', 'item']:
            raise FuseOSError(EISDIR)
        with self.flock:
            fdict = self._ensure_fdict(spath, file)

    def _invalidate_cache(self, fh: int, fdict: dict, file: dict):
        self.flock.assertLocked()
        if 'cache' in fdict:
            cache = fdict['cache']
            cache.invalidate()
        cache = CacheFile(file['size'])
        fdict['cache'] = cache
        fdict['path'] = cache.name
        fdict['updated'] = file['updated']

        if fh in self.fobjs:
            file = self.fobjs[fh]
            file.close()
            del self.fobjs[fh]
        return cache

    def _ensure_region_available(self, path: str, fdict: dict, fh: int, offset: int, size: int):
        obj = self._get_current_object(fdict['obj'])
        with self.flock:
            cache = fdict.get('cache', None)
            updated = fdict.get('updated', None)
            if cache is None or obj['updated'] != updated:
                logging.debug("-> _ensure_region_available({}) - invalidating cache".format(path))
                cache = self._invalidate_cache(fh, fdict, obj)

        offset, size = cache.adjust_and_start_download(fh, offset, size)
        if size == 0:
            return
        else:
            try:
                with open(cache.name, 'r+b') as f:
                    f.seek(offset, os.SEEK_SET)
                    req = requests.get(
                        '%sfile/%s/download?offset=%s&endByte=%s' %
                        (self.girder_cli.urlBase, fdict['obj']['_id'], offset, offset + size),
                        headers={'Girder-Token': self.girder_cli.token}, stream=True)
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
        if len(path.parts) == 2 and path.parts[1] == 'results':
            # results dir; result files may appear!
            return True
        return False

    def _is_mutable_file(self, path: pathlib.Path) -> bool:
        if len(path.parts) == 2:
            return True
        if len(path.parts) >= 3 and path.parts[1] == 'results':
            return True
        return False

    def _is_external_file(self, path: Union[pathlib.Path, str]) -> bool:
        return False

    def _getattr(self, path: pathlib.Path, obj: dict, obj_type: str) -> dict:
        stat = super()._getattr(path, obj, obj_type)
        if 'isLink' in obj and obj['isLink']:
            stat['st_mode'] = stat['st_mode'] | S_IFLNK
        return stat

    def readlink(self, spath: str):
        logging.debug("-> readlink({})".format(spath))
        path = _lstrip_path(spath)
        obj, obj_type = self._get_object_from_root(path)
        target = obj['linkTarget']
        if target.startswith('../../Versions/'):
            versionId = target[len('../../Versions/'):]
            version = self._timed_cache(versionId, path, self._get_version, versionId)
            return self.versions_mountpoint + '/' + version['name']
        else:
            return target

    def _get_version(self, id):
        obj = self.girder_cli.get('version/%s' % id)
        obj['_modelType'] = 'folder'
        return obj

    def _timed_cache(self, id: str, path: pathlib.Path, method, *args):
        # I don't like this method. Or maybe I don't like the other method that kinda does a
        # similary thing.
        if logging.isEnabledFor(logging.DEBUG):
            logging.debug("-> _timed_cache({}, {}, {})".format(id, method, args))
        now = time.time()
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

