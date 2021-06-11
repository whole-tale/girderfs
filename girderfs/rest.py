import os
import tempfile

import requests

from .core import GirderFS
from .utils import _lstrip_path, logger


class RESTGirderFS(GirderFS):
    """
    Filesystem for locally mounting a remote Girder folder

    :param folder_id: Folder id
    :type folder_id: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def read(self, path, size, offset, fh):
        logger.debug("-> read({})".format(path))
        obj, _ = self._get_object_from_root(_lstrip_path(path))
        cacheKey = "#".join((obj["_id"], obj.get("updated", obj["created"])))
        fp = self.cache.get(cacheKey, read=True)
        if fp:
            logger.debug("-> hitting cache {} {} {}".format(path, size, offset))
            fp.seek(offset)
            return fp.read(size)
        else:
            logger.debug("-> downloading")
            req = requests.get(
                "%sitem/%s/download" % (self.girder_cli.urlBase, obj["_id"]),
                headers={"Girder-Token": self.girder_cli.token},
                stream=True,
            )
            with tempfile.NamedTemporaryFile(prefix="wtdm", delete=False) as tmp:
                for chunk in req.iter_content(chunk_size=65536):
                    tmp.write(chunk)
            with open(tmp.name, "rb") as fp:
                self.cache.set(cacheKey, fp, read=True)
                os.remove(tmp.name)
                fp.seek(offset)
                return fp.read(size)

    def open(self, path, mode="r", **kwargs):
        logger.debug("-> open({}, {})".format(path, self.fd))
        self.fd += 1
        return self.fd

    def destroy(self, private_data):
        super().destroy(self, private_data)

    def release(self, path, fh):  # pylint: disable=unused-argument
        logger.debug("-> release({}, {})".format(path, self.fd))
        self.fd -= 1
        return self.fd
