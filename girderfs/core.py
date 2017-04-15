# -*- coding: utf-8 -*-
"""
Core classes for FUSE based filesystem handling Girder's resources
"""

import os
import time
import pathlib
import logging
import sys
import tempfile
from stat import S_IFDIR, S_IFREG
from errno import ENOENT
# http://stackoverflow.com/questions/9144724/
import encodings.idna  # NOQA pylint: disable=unused-import
from encodings import hex_codec  # NOQA pylint: disable=unused-import

from dateutil.parser import parse as tparse
import diskcache
from fuse import Operations, LoggingMixIn, FuseOSError
import girder_client
import requests
import threading
from bson import objectid

# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


def _lstrip_path(path):
    path_obj = pathlib.Path(path)
    return pathlib.Path(*path_obj.parts[1:])


if sys.version_info[0] == 2:
    def _convert_time(strtime):
        from backports.datetime_timestamp import timestamp
        return timestamp(tparse(strtime))
else:
    def _convert_time(strtime):
        return tparse(strtime).timestamp()


class GirderFS(LoggingMixIn, Operations):
    """
    Base class for handling Girder's folders

    :param folder_id: Folder id
    :type folder_id: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def __init__(self, folder_id, girder_cli):
        super(GirderFS, self).__init__()
        self.folder_id = folder_id
        self.girder_cli = girder_cli
        self.fd = 0
        self.cache = diskcache.Cache(tempfile.mkdtemp())

    def _get_object_by_path(self, obj_id, path):
        raw_listing = self._get_listing(obj_id)
        folder = next((item for item in raw_listing['folders']
                       if item["name"] == path.parts[0]), None)

        if folder is not None:
            if len(path.parts) == 1:
                return folder, "folder"
            else:
                return self._get_object_by_path(folder["_id"],
                                                pathlib.Path(*path.parts[1:]))

        _file = next((item for item in raw_listing['files']
                      if item["name"] == path.parts[0]), None)
        if _file is not None:
            return _file, "file"

        raise FuseOSError(ENOENT)

    def _get_listing(self, obj_id):
        try:
            return self.cache[obj_id]
        except KeyError:
            try:
                self.cache[obj_id] = self.girder_cli.get(
                    'folder/%s/listing' % obj_id)
            except girder_client.HttpError:
                self.cache[obj_id] = self.girder_cli.get(
                    'item/%s/listing' % obj_id)
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
        if path == '/':
            raw_listing = self._get_listing(self.folder_id)
        else:
            obj, obj_type = self._get_object_by_path(
                self.folder_id, _lstrip_path(path))
            raw_listing = self._get_listing(obj["_id"])

        for obj_type in raw_listing.keys():
            dirents += [_["name"] for _ in raw_listing[obj_type]]
        return dirents

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
        if path == '/':
            raw_listing = self._get_listing(self.folder_id)
        else:
            obj, obj_type = self._get_object_by_path(
                self.folder_id, _lstrip_path(path))
            raw_listing = self._get_listing(obj["_id"])

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
                '%sfile/%s/download' % (self.girder_cli.urlBase, obj["_id"]),
                headers={'Girder-Token': self.girder_cli.token}, stream=True)
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
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
        self.cache.clear()
        self.cache.close()

    def release(self, path, fh):  # pylint: disable=unused-argument
        logging.debug("-> release({}, {})".format(path, self.fd))
        self.fd -= 1
        return self.fd

class DownloadThread(threading.Thread):
    def __init__(self, stream, fdict, lock):
        threading.Thread.__init__(self)
        self.stream = stream
        self.fdict = fdict
        self.lock = lock

    def run(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            self.fdict['path'] = tmp.name
            for chunk in self.stream.iter_content(chunk_size=65536):
                tmp.write(chunk)
                self.fdict['written'] += len(chunk)
        with self.lock:
            self.fdict['downloaded'] = True
            self.fdict['downloading'] = False


class WtDmsGirderFS(GirderFS):
    """
    Filesystem for locally mounting a remote Girder folder

    :param sessionId: WT DMS session id
    :type sessoinId: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def __init__(self, sessionId, gc):
        GirderFS.__init__(self, sessionId, gc)
        self.sessionId = sessionId
        self.flock = threading.Lock()
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
        obj = fdict['obj']
        obj = self._wait_for_file(obj)

        if not fdict['downloaded']:
            download = False
            with self.flock:
                if not fdict['downloading']:
                    download = True
                    fdict['downloading'] = True
            if download:
                self._start_download(path, obj)

        self._wait_for_region(path, fdict, offset, size)
        if fh not in self.fobjs:
            self.fobjs[fh] = open(fdict['path'], 'rb')
        fp = self.fobjs[fh]

        fp.seek(offset)
        return fp.read(size)

    def _wait_for_file(self, obj):
        # Waits for the file to be downloaded/cached by the DMS
        while True:
            if obj['dm']['cached']:
                return obj
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
        return self.girder_cli.get('dm/session/%s/item/%s' % (self.sessionId, id))

    def open(self, path, mode="r", **kwargs):
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
        available = obj['dm']['cached']
        downloaded = False
        downloading = False
        with self.flock:
            if path not in self.openFiles:
                self.openFiles[path] = {'obj': obj, 'written': 0, 'downloading': False,
                                        'downloaded': False, 'path': None}
            else:
                downloaded = self.openFiles[path]['downloaded']
                downloading = self.openFiles[path]['downloading']
                # set downloading flag with lock held
                if available and not downloading:
                    self.openFiles[path]['downloading'] = True
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

    def _start_download(self, path, lockId):
        with self.flock:
            self.openFiles[path]['downloading'] = True
        req = requests.get(
            '%sdm/lock/%s/download' % (self.girder_cli.urlBase, lockId),
            headers={'Girder-Token': self.girder_cli.token}, stream=True)
        thread = DownloadThread(req, self.openFiles[path], self.flock)
        thread.start()

    def _lock(self, obj):
        resp = self.girder_cli.post('dm/lock?sessionId=%s&itemId=%s' %
                                   (self.sessionId, obj['_id']))
        return resp['_id']

    def _unlock(self, lockId):
        self.girder_cli.delete('dm/lock/%s' % (lockId))

    def destroy(self, private_data):
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
