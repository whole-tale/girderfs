import datetime
import io
import logging
import tempfile
import time
from stat import S_IFDIR, S_IFREG

import diskcache
from fuse import LoggingMixIn, Operations

from .core import _convert_time


class WholeTaleMetadataFS(LoggingMixIn, Operations):
    """
    Class for handling Tale's metadata.

    :param girder_cli: Authenticated instance of GirderClient
    :type girder_cli: girder_client.GriderClient
    """

    def __init__(self, tale_id, girder_cli):
        super(WholeTaleMetadataFS, self).__init__()
        self.girder_cli = girder_cli
        self.tale_id = tale_id
        self.fd = 0
        self.cachedir = tempfile.mkdtemp(prefix="wtmeta")
        self.cache = diskcache.Cache(self.cachedir)

    def read(self, path, size, offset, fh):
        logging.warning(
            "path = {}\nsize = {}\noffset = {}\nfh = {}\n".format(
                path, size, offset, fh
            )
        )
        if path == "/metadata.json":
            fp = self._get_manifest()
            fp.seek(offset)
            return fp.read(size)
        raise NotImplementedError

    def readdir(self, path, fh):
        dirents = [".", "..", "metadata.json", "image_build.log"]
        return dirents

    def getinfo(self, path):
        """Pyfilesystem essential method."""
        logging.debug("-> getinfo({})".format(path))
        if not path.startswith("/"):
            path = "/" + path
        return self.getattr(path)

    def getattr(self, path, fh=None):
        logging.warning("-> getattr({})".format(path))
        now = _convert_time(str(datetime.datetime.now()))

        stat = dict(st_ctime=now, st_atime=now, st_mtime=now)

        if path == "/":
            stat.update(dict(st_mode=(S_IFDIR | 0o755), st_nlink=2))
        elif path == "/metadata.json":
            manifest = self._get_manifest()
            stat.update(
                dict(
                    st_mode=(S_IFREG | 0o644),
                    st_nlink=1,
                    st_blocks=1,
                    st_size=manifest.seek(0, io.SEEK_END),
                    st_atime=time.time(),
                )
            )
        else:
            stat.update(
                dict(
                    st_mode=(S_IFREG | 0o644),
                    st_nlink=1,
                    st_blocks=1,
                    st_size=1024 ** 2,
                    st_atime=time.time(),
                )
            )
        return stat

    def _get_manifest(self):
        fp = self.cache.get("manifest", read=True)
        if not fp:
            resp = self.girder_cli.sendRestRequest(
                "get",
                "tale/{}/manifest".format(self.tale_id),
                stream=False,
                jsonResp=False,
            )
            fp = io.BytesIO(resp.content)
            self.cache.set("manifest", fp, read=True, expire=15)
        return fp

    def open(self, path, mode="r", **kwargs):
        logging.debug("-> open({}, {})".format(path, self.fd))
        self.fd += 1
        return self.fd

    def release(self, path, fh):  # pylint: disable=unused-argument
        logging.debug("-> release({}, {})".format(path, self.fd))
        self.fd -= 1
        return self.fd
