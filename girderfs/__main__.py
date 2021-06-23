# -*- coding: utf-8 -*-
import argparse
import os
import subprocess
from ctypes import cdll

from fuse import FUSE
from girder_client import GirderClient

from .dms import WtDmsGirderFS
from .local import LocalGirderFS
from .rest import RESTGirderFS
from .runs import WtRunsFS
from .versions import WtVersionsFS

_libc = cdll.LoadLibrary("libc.so.6")
_setns = _libc.setns
CLONE_NEWNS = 0x00020000


def setns(fd, nstype):
    if hasattr(fd, "fileno"):
        fd = fd.fileno()
    _setns(fd, nstype)


def main(args=None):
    parser = argparse.ArgumentParser(description="Mount Girder filesystem assetstore.")
    parser.add_argument(
        "--api-url",
        required=True,
        default=None,
        help="full URL to the RESTful API of Girder server",
    )
    parser.add_argument("--username", required=False, default=None)
    parser.add_argument("--password", required=False, default=None)
    parser.add_argument("--api-key", required=False, default=None)
    parser.add_argument("--token", required=False, default=None)
    parser.add_argument("--foreground", dest="foreground", action="store_true")
    parser.add_argument("--hostns", dest="hostns", action="store_true")
    parser.add_argument(
        "--versions-mountpoint",
        dest="versions_mountpoint",
        required=False,
        help="Mountpoint for the versions FS. If relative, then it should be "
        "relative to the runs mountpoint",
        default="Versions",
    )
    parser.add_argument(
        "-c",
        default="remote",
        help="type of filesystem to mount",
        choices=[
            "remote",
            "direct",
            "wt_dms",
            "wt_home",
            "wt_work",
            "wt_versions",
            "wt_runs",
        ],
    )
    parser.add_argument("local_folder", help="path to local target folder")
    parser.add_argument(
        "remote_folder",
        help="Girder's folder id, a DM session id (for wt_dms), or a tale instance"
        "ID (for wt_versions)",
    )

    args = parser.parse_args()

    gc = GirderClient(apiUrl=args.api_url)
    if args.token:
        gc.token = args.token
    elif args.api_key:
        gc.authenticate(apiKey=args.api_key)
    elif args.username and args.password:
        gc.authenticate(username=args.username, password=args.password)
    else:
        raise RuntimeError("You need to specify apiKey or user/pass")

    if args.hostns:
        targetns = os.path.join(os.environ.get("HOSTDIR", "/"), "proc/1/ns/mnt")
        with open(targetns) as fd:
            setns(fd, CLONE_NEWNS)

    if args.c == "remote":
        FUSE(
            RESTGirderFS(args.remote_folder, gc),
            args.local_folder,
            foreground=args.foreground,
            ro=True,
            allow_other=True,
        )
    elif args.c == "direct":
        FUSE(
            LocalGirderFS(args.remote_folder, gc),
            args.local_folder,
            foreground=args.foreground,
            ro=True,
            allow_other=True,
        )
    elif args.c == "wt_dms":
        FUSE(
            WtDmsGirderFS(args.remote_folder, gc),
            args.local_folder,
            foreground=args.foreground,
            ro=True,
            allow_other=True,
        )
    elif args.c == "wt_work":
        user = gc.get("/user/me")
        args = {
            "user": user["login"],
            "pass": "token:{}".format(gc.token),
            "dest": args.local_folder,
            "tale": args.remote_folder,
            "opts": "-o uid=1000,gid=100",  # FIXME
            "url": gc.urlBase.replace("api/v1", "tales").rstrip("/"),  # FIXME
        }
        cmd = 'echo "{user}\n{pass}" | mount.davfs {opts} {url}/{tale} {dest}'
        cmd = cmd.format(**args)
        subprocess.check_output(cmd, shell=True)  # FIXME
    elif args.c == "wt_home":
        user = gc.get("/user/me")
        args = {
            "user": user["login"],
            "pass": "token:{}".format(gc.token),
            "dest": args.local_folder,
            "opts": "-o uid=1000,gid=100",  # FIXME
            "url": gc.urlBase.replace("api/v1", "homes").rstrip("/"),  # FIXME
        }
        cmd = 'echo "{user}\n{pass}" | mount.davfs {opts} {url}/{user} {dest}'
        cmd = cmd.format(**args)
        subprocess.check_output(cmd, shell=True)  # FIXME
    elif args.c == "wt_versions":
        FUSE(
            WtVersionsFS(args.remote_folder, gc),
            args.local_folder,
            foreground=args.foreground,
            ro=False,
            allow_other=True,
        )
    elif args.c == "wt_runs":
        FUSE(
            WtRunsFS(args.remote_folder, gc, args.versions_mountpoint),
            args.local_folder,
            foreground=args.foreground,
            ro=False,
            allow_other=True,
        )
    else:
        print("No implementation for command %s" % args.c)


if __name__ == "__main__":
    main()
