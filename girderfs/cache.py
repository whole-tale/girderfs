import os
import tempfile
import threading
import time

from .utils import logger

CACHE_ENTRY_STALE_TIME = 1.0  # seconds


class DictCache(dict):
    def close(self):
        pass


class CacheEntry:
    def __init__(self, obj: dict, listing: dict = None, pinned=False):
        self.obj = obj
        self.listing = listing
        self.loadTime = time.time()
        self.pinned = pinned

    def add_to_listing(self, entry: "CacheEntry"):
        if self.listing is None:
            self.listing = {}
        self.listing[entry.obj["name"]] = entry

    def __str__(self):
        return "CacheEntry[obj_id=%s, listing=%s]" % (self.obj["_id"], self.listing)


class CacheWrapper:
    """
    A wrapper cache class that coerces keys to str in order to avoid unicode/str
    lookup errors.

    :param cache: The wrapped cache class
    :type cache: diskcache.Cache
    """

    def __init__(self, cache=None):
        if cache is None:
            cache = DictCache()
        self.cache = cache

    def __getitem__(self, item: str) -> CacheEntry:
        assert isinstance(item, str)
        return self.cache.__getitem__(item)

    def __setitem__(self, key: str, value: CacheEntry):
        assert isinstance(key, str)
        assert isinstance(value, CacheEntry)
        assert "_modelType" in value.obj, "No _modelType for %s" % value.obj
        return self.cache.__setitem__(key, value)

    def __delitem__(self, key: str):
        assert isinstance(key, str)
        return self.cache.__delitem__(key)

    def clear(self):
        self.cache.clear()

    def close(self):
        self.cache.close()


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
            f.write(b"\x00")
            assert f.seek(10, os.SEEK_DATA) == 10, "Sparse file block check #1 failed"
            assert (
                f.seek(10, os.SEEK_HOLE) == CacheFile.BLOCK_SIZE
            ), "Sparse file block check #2 failed"
        CacheFile.block_size_check_done = True

    def adjust_and_start_download(self, fh: int, start: int, len: int):
        logger.debug(
            "-> adjust_and_start_download({}, {}, {}, {})".format(
                self.name, fh, start, len
            )
        )
        while True:
            # wait for overlapping transfers
            w = None
            logger.debug("-> adjust_and_start_download - wait for lock")
            with self.lock:
                for d in self.active_downloads.values():
                    if d.overlaps(start, len):
                        logger.debug(
                            "-> adjust_and_start_download - overlaps {}".format(d)
                        )
                        w = d
                if w is None:
                    logger.debug(
                        "-> adjust_and_start_download - no overlapping downloads"
                    )
                    start, len = self._adjust_download(fh, start, len)
                    if len > 0:
                        self._begin_download(fh, start, len)
                    return start, len
            # w is not None
            with w.cond:
                while not w.done:
                    logger.debug(
                        "-> adjust_and_start_download - waiting for {}".format(w)
                    )
                    w.cond.wait()

    def _adjust_download(self, fh: int, start: int, len: int):
        assert self.lock.locked()
        logger.debug(
            "-> _adjust_download({}, {}, {}, {})".format(self.name, fh, start, len)
        )
        f = open(self.name, "rb")
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
        logger.debug("-> _end_download({}, {})".format(self.name, fh))
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

    def __str__(self, start: int = -1, len: int = -1, char: str = "O"):
        with self.lock:
            return self.__xstr__(start, len, char)

    def __xstr__(self, start: int = -1, len: int = -1, char: str = "O"):
        s = "["
        assert self.lock.locked()
        with open(self.name, "rb") as f:
            p = 0
            while p < self.size:
                if start <= p < start + len:
                    s += char
                elif p == f.seek(p, os.SEEK_DATA):
                    s += "#"
                else:
                    s += "_"
                p += CacheFile.BLOCK_SIZE
        s += "]"
        return s
