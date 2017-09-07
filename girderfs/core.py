# -*- coding: utf-8 -*-
"""
Core classes for FUSE based filesystem handling Girder's resources
"""

import os
import time
import pathlib
import logging
import sys
import six
import shutil
import tempfile
from stat import S_IFDIR, S_IFREG
from errno import ENOENT, EPERM, EISDIR
# http://stackoverflow.com/questions/9144724/
import encodings.idna  # NOQA pylint: disable=unused-import
from encodings import hex_codec  # NOQA pylint: disable=unused-import

from dateutil.parser import parse as tparse
import diskcache
from fuse import Operations, LoggingMixIn, FuseOSError
import requests
import threading
from bson import objectid
from queue import Queue
import traceback

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.WARNING)


def _lstrip_path(path):
    path_obj = pathlib.Path(path)
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

class CacheWrapper:
    """
    A wrapper cache class that coerces keys to str in order to avoid unicode/str
    lookup errors.

    :param cache: The wrapped cache class
    :type cache: diskcache.Cache
    """
    def __init__(self, cache):
        self.cache = cache

    def get(self, key, *args, **kwargs):
        return self.cache.get(str(key), *args, **kwargs)

    def set(self, key, *args, **kwargs):
        return self.cache.set(str(key), *args, **kwargs)

    def delete(self, key):
        return self.cache.delete(str(key))

    def __getitem__(self, item):
        return self.cache.__getitem__(str(item))

    def __setitem__(self, key, value):
        return self.cache.__setitem__(str(key), value)

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
    def __init__(self, folder_id, girder_cli):
        super(GirderFS, self).__init__()
        self.folder_id = folder_id
        self.girder_cli = girder_cli
        self.fd = 0
        self.cachedir = tempfile.mkdtemp(prefix='wtdm')
        self.cache = CacheWrapper(diskcache.Cache(self.cachedir))

    def _get_object_by_path(self, obj_id, path):
        logging.debug("-> _get_object_by_path({}, {})".format(obj_id, path))
        raw_listing = self._get_listing(obj_id)
        folder = self._find(path.parts[0], raw_listing['folders'])

        if folder is not None:
            if len(path.parts) == 1:
                return folder, "folder"
            else:
                return self._get_object_by_path(folder["_id"],
                                                pathlib.Path(*path.parts[1:]))

        _file = self._find(path.parts[0], raw_listing['files'])

        if _file is not None:
            return _file, "file"

        raise FuseOSError(ENOENT)

    def _find(self, name, list):
        return next((item for item in list if item['name'] == name), None)

    def _get_listing(self, obj_id):
        try:
            return self.cache[obj_id]
        except KeyError:
            self.cache[obj_id] = self.girder_cli.get('dm/fs/%s/listing' % obj_id)
        finally:
            return self.cache[obj_id]

    def getattr(self, path, fh=None):
        logging.debug("-> getattr({})".format(path))
        if path == '/':
            return dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
        obj, obj_type = self._get_object_by_path(
            self.folder_id, _lstrip_path(path))

        if obj_type == 'folder':
            stat = dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
        else:
            stat = dict(st_mode=(S_IFREG | 0o644), st_nlink=1)
        ctime = _convert_time(obj["created"])
        try:
            mtime = _convert_time(obj["updated"])
        except KeyError:
            mtime = ctime
        stat.update(dict(st_ctime=ctime, st_mtime=mtime, st_blocks=1,
                         st_size=obj["size"], st_atime=time.time()))
        return stat

    def read(self, path, size, offset, fh):
        raise NotImplementedError

    def readdir(self, path, fh):
        logging.debug("-> readdir({})".format(path))
        dirents = ['.', '..']
        raw_listing = self._get_listing_by_path(path)

        for obj_type in list(raw_listing.keys()):
            dirents += [_["name"] for _ in raw_listing[obj_type]]
        return dirents

    def _get_listing_by_path(self, path):
        if path == '/':
            return self._get_listing(self.folder_id)
        else:
            obj, obj_type = self._get_object_by_path(
                self.folder_id, _lstrip_path(path))
            return self._get_listing(obj["_id"])

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
            stat = dict(st_mode=(S_IFREG | 0o644), st_nlink=1)
            stat.update(_get_stat(obj))
            listdir.append((obj['name'], stat))

        for obj in raw_listing['folders']:
            stat = dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
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
        obj, _ = self._get_object_by_path(
            self.folder_id, _lstrip_path(path))
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
            #print self.stream.__dict__
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

    def __init__(self, sessionId, gc):
        GirderFS.__init__(self, six.text_type(sessionId), gc)
        self.sessionId = sessionId
        self.flock = MLock()
        self.openFiles = {}
        self.locks = {}
        self.fobjs = {}
        self.ctime = int(time.time())
        self.session = self._load_session_info()
        # pre-cache the session listing so that base class methods
        # don't try to do a listing on a folder with the same id
        self.cache[sessionId] = self._get_session_listing()

    def _load_session_info(self):
        return self.girder_cli.get('dm/session/%s?loadObjects=true' % self.sessionId)

    def _get_session_listing(self):
        # mount points can have arbitrary depth, so pre-populate the cache with
        # the necessary directory structure
        dict = self._make_dict('/', self.sessionId)
        dataSet = self.session['dataSet']

        for entry in dataSet:
            self._add_session_entry(dict, entry)

        return dict['listing']

    def _make_dict(self, name, id=None):
        if id is None:
            id = objectid.ObjectId()
        return {'obj': {'_id': id, 'name': name, 'created': self.ctime},
                'listing': {'folders': [], 'files':[]},
                'dirmap': {}}

    def _add_session_entry(self, dict, entry):
        # assume and strip root slash
        path = list(pathlib.PurePath(entry['mountPath']).parts)[1:]
        dirdict = self._mkdirs(dict, path[:-1])
        if entry['type'] == 'folder':
            dirdict['listing']['folders'].append(entry['obj'])
        else:
            dirdict['listing']['files'].append(entry['obj'])

    def _mkdirs(self, dict, path):
        if len(path) == 0:
            return dict
        crt = path[0]
        if crt not in dict['dirmap']:
            cdict = self._make_dict(crt)
            dict['listing']['folders'].append(cdict)
            self.cache[cdict['obj']['_id']] = cdict['listing']
            dict['dirmap'][crt] = cdict
        else:
            cdict = dict['dirmap'][crt]
        return self._mkdirs(cdict, path[1:])

    def read(self, path, size, offset, fh):
        logging.debug("-> read({})".format(path))

        fdict = self.openFiles[path]

        self._ensure_region_available(path, fdict, fh, offset, size)

        if fh not in self.fobjs:
            self.fobjs[fh] = open(fdict['path'], 'r+b')
        fp = self.fobjs[fh]

        fp.seek(offset)
        return fp.read(size)

    def _ensure_region_available(self, path, fdict, fh, offset, size):
        obj = fdict['obj']
        obj = self._wait_for_file(fdict)

        if not fdict['downloaded']:
            download = False
            with self.flock:
                if not fdict['downloading']:
                    download = True
                    fdict['downloading'] = True
            if download:
                lockId = self.locks[fh]
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
                time.sleep(1.0)
                obj = self._get_item_unfiltered(obj['_id'])

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
        obj, _ = self._get_object_by_path(self.folder_id, _lstrip_path(path))
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
        lockId = self._lock(obj)
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

    def _ensure_fdict(self, path, obj):
        if path not in self.openFiles:
            self.openFiles[path] = {'obj': obj, 'written': 0, 'downloading': False,
                                    'downloaded': False, 'path': None}
        return self.openFiles[path]

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

    def _lock(self, obj):
        resp = self.girder_cli.post('dm/lock?sessionId=%s&itemId=%s' %
                                    (self.sessionId, obj['_id']))
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
        # TODO: cancel uploads when a file is opened w/a
        fp = open(fdict['path'], 'a+b')
        fp.seek(0, os.SEEK_END)
        size = fp.tell()
        fp.seek(0, os.SEEK_SET)
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

    def _invalidate_root(self, id):
        if not self.cache.delete(id):
            logging.error('Object not in cache "%s" (%s)' % (id, type(id)))

    def _load_session_info(self):
        return None

    def _get_session_listing(self):
        return None

    def statfs(self, path):
        # default results in a read-only fs
        return {'f_flag': os.ST_NOATIME + os.ST_NODEV + os.ST_NODIRATIME + os.ST_NOSUID}

    def chmod(self, path, mode):
        logging.debug("-> chmod({}, {})".format(path, mode))

        path = _lstrip_path(path)
        self._set_metadata(path, {'permissions': mode})

    def _set_metadata(self, path, dict):
        obj, objType = self._get_object_by_path(self.folder_id, path)
        self._set_object_metadata(obj, objType, dict)

    def _set_object_metadata(self, obj, objType, dict):
        self.girder_cli.put('dm/fs/%s/setProperties' % obj['_id'], json=dict)
        obj.update(dict)
        return obj

    def getattr(self, path, fh=None):
        logging.debug("-> getattr({})".format(path))

        if path == '/':
            return dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
        obj, objType = self._get_object_by_path(
            self.folder_id, _lstrip_path(path))

        if objType == 'folder':
            stat = dict(st_mode=(S_IFDIR | self._get_prop(obj, 'permissions', 0o755)), st_nlink=2)
        else:
            stat = dict(st_mode=(S_IFREG | self._get_prop(obj, 'permissions', 0o644)), st_nlink=1)
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
            obj, objType = self._get_object_by_path(self.folder_id, path)
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
            fdict = self._ensure_fdict(path, obj)
            with tempfile.NamedTemporaryFile(prefix='wtdm', delete=False) as tmp:
                fdict['path'] = tmp.file
            self._mark_dirty(path)
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
            parentId = self._get_parent_id(path)
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
            parentId = self._get_parent_id(path)
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
        obj = self.girder_cli.post('folder', parameters = {'parentId': parentId, 'name': path.name})
        self._set_metadata(path, {'permissions': mode})
        self._cache_add_dir(obj, path)

    def _get_parent_id(self, path):
        if len(path.parent.parts) == 0:
            return self.folder_id
        else:
            obj, objType = self._get_object_by_path(self.folder_id, path.parent)
            return obj['_id']

    def rmdir(self, path):
        logging.debug("-> rmdir({})".format(path))

        path = _lstrip_path(path)
        if len(path.parts) == 0:
            raise FuseOSError(EPERM)

        obj, objType = self._get_object_by_path(self.folder_id, path)
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

        obj, objType = self._get_object_by_path(self.folder_id, path)
        self.girder_cli.put('%s/%s' % (objType, obj['_id']), parameters={'name': new})
        obj['name'] = new

    def truncate(self, path, length, fh=None):
        logging.debug("-> truncate({}, {}, {})".format(path, length, fh))
        # so fh=None means truncate() whereas fh != None means ftruncate

        # the basic workflow is to do i/o locally and commit when all of the following are true:
        #  - no active downloads
        #  - the file is not open

        pathObj = _lstrip_path(path)
        obj, objType = self._get_object_by_path(self.folder_id, pathObj)

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
        obj, objType = self._get_object_by_path(self.folder_id, path)
        # should be the parent item
        self.girder_cli.delete('item/%s' % (obj['_id']))
        self._cache_remove_file(path)

    def write(self, path, data, offset, fh):
        logging.debug("-> write({}, {}, {})".format(path, fh, offset))
        fdict = self.openFiles[path]
        size = len(data)

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
        obj, _ = self._get_object_by_path(
            self.folder_id, _lstrip_path(path))
        return os.open(obj['path'], os.O_RDONLY)
        # return open(obj['path'], mode)   # pyfilesystem

    def read(self, path, size, offset, fh):
        logging.debug("-> read({})".format(path))
        obj, _ = self._get_object_by_path(
            self.folder_id, _lstrip_path(path))
        fh = os.open(obj['path'], os.O_RDONLY)
        os.lseek(fh, offset, 0)
        return os.read(fh, size)

    def release(self, path, fh):  # pylint: disable=unused-argument
        logging.debug("-> release({})".format(path))
        return os.close(fh)
