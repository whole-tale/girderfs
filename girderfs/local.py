import os

from .core import GirderFS
from .utils import _lstrip_path, logger


class LocalGirderFS(GirderFS):
    """
    Filesystem for mounting local Girder's FilesystemAssetstore

    :param folder_id: Folder id
    :type folder_id: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def open(self, path, mode="r", **kwargs):
        logger.debug("-> open({})".format(path))
        obj, _ = self._get_object_from_root(_lstrip_path(path))
        return os.open(obj["path"], os.O_RDONLY)
        # return open(obj['path'], mode)   # pyfilesystem

    def read(self, path, size, offset, fh):
        logger.debug("-> read({})".format(path))
        obj, _ = self._get_object_from_root(_lstrip_path(path))
        fh = os.open(obj["path"], os.O_RDONLY)
        os.lseek(fh, offset, 0)
        return os.read(fh, size)

    def release(self, path, fh):  # pylint: disable=unused-argument
        logger.debug("-> release({})".format(path))
        return os.close(fh)
