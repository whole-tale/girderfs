# Basic tornado server that handles POST and DELETE requests
import json
import os
import subprocess

import jsonschema
import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from girder_client import GirderClient
from tornado.options import define, options

from .schemas import MountProtocols, MountTypes, MountValidator

define("port", default=8888, help="run on the given port", type=int)


mount = {}


class MainHandler(tornado.web.RequestHandler):
    _gc = None

    def initialize(self, state):
        self.state = state

    def post(self):
        try:
            data = tornado.escape.json_decode(self.request.body)
            MountValidator(data).validate()
        except jsonschema.exceptions.ValidationError as e:
            self.set_status(400)
            self.write(e.message)
            return
        except ValueError as e:
            self.set_status(400)
            self.write(e.message)
            return
        self.state.update(data)
        self.state["root"] = os.path.join(
            os.environ["WT_VOLUMES_PATH"], "mountpoints", data["root"]
        )
        if not os.path.exists(self.state["root"]):
            os.makedirs(self.state["root"])
        self.get_girder_objects()
        self.execute()

    @property
    def gc(self):
        if self._gc is None:
            self._gc = GirderClient(apiUrl=self.state["girderApiUrl"])
            self._gc.authenticate(apiKey=self.state["girderApiKey"])
        return self._gc

    def get_girder_objects(self):
        if "taleId" in self.state:
            self.state["tale"] = self.gc.get(f"/tale/{self.state['taleId']}")
        if "userId" in self.state:
            self.state["user"] = self.gc.get(f"/user/{self.state['userId']}")
        else:
            self.state["user"] = self.gc.get("/user/me")
        if "runId" in self.state:
            self.state["run"] = self.gc.get(f"/run/{self.state['runId']}")
            if "tale" not in self.state:
                runs_root = self.gc.get(f"/folder/{self.state['run']['parentId']}")
                self.state["tale"] = self.gc.get(f"/tale/{runs_root['meta']['taleId']}")
            self.state["run"]["taleId"] = self.state["tale"]["_id"]
        if "sessionId" in self.state:
            self.state["session"] = self.gc.get(
                f"/dm/session/{self.state['sessionId']}"
            )

    def execute(self):
        for mount in self.state["mounts"]:
            destination = os.path.join(self.state["root"], mount["location"])
            if not os.path.exists(destination):
                os.makedirs(destination)

            gcObj, fs_type = self.mounttype_to_fs(mount["protocol"], mount["type"])

            if mount["protocol"] == MountProtocols.webdav:
                girder_url = self.gc.urlBase.replace("api/v1", "").rstrip("/")
                args = {
                    "user": self.state["user"]["login"],
                    "pass": "token:{}".format(self.gc.token),
                    "destination": destination,
                    "opts": "-o uid=1000,gid=100,file_mode=0600,dir_mode=2700",
                    "source": girder_url + self.webdav_url(mount["type"]),
                }
                cmd = (
                    'echo "{user}\n{pass}" | mount.davfs {opts} {source} {destination}'
                )
                cmd = cmd.format(**args)
            elif mount["protocol"] == MountProtocols.girderfs:
                cmd = (
                    f"girderfs -c {fs_type} "
                    f"--api-url {self.state['girderApiUrl']} "
                    f"--api-key {self.state['girderApiKey']} "
                    f"{destination} {gcObj['_id']}"
                )
            elif mount["protocol"] == MountProtocols.bind:
                source = os.path.join(
                    os.environ["WT_VOLUMES_PATH"], self.source_path_bind(mount["type"])
                )
                cmd = f"sudo mount --bind {source} {destination}"
            elif mount["protocol"] == MountProtocols.passthrough:
                cmd = (
                    "passthrough-fuse -o allow_other "
                    f"--girder-url={self.state['girderApiUrl']}/folder/{gcObj['_id']}/listing "
                    f"--token={self.gc.token} {destination}"
                )
            elif mount["protocol"] == MountProtocols.overlay:
                lowerdir = os.path.join(
                    os.environ["WT_VOLUMES_PATH"], self.source_path_overlay(mount["type"], "lower")
                )
                upperdir = os.path.join(
                    os.environ["WT_VOLUMES_PATH"], self.source_path_overlay(mount["type"], "upper")
                )
                workdir = f"{upperdir}_workdir"
                cmd = (
                    "sudo mount -t overlay overlay -o "
                    f"lowerdir={lowerdir},upperdir={upperdir},workdir={workdir}"
                    ",index=off,xino=off,redirect_dir=off,metacopy=off "
                    f"{destination}"
                )
                self.state["remove_dirs"] = [os.path.join(workdir, "work"), workdir]
                os.makedirs(workdir, exist_ok=True)
                os.makedirs(upperdir, exist_ok=True)

            subprocess.check_output(cmd, shell=True)

    def delete(self):
        errmsg = ""
        for mount in self.state["mounts"]:
            destination = os.path.join(self.state["root"], mount["location"])
            if mount["protocol"] == MountProtocols.webdav:
                cmd = f"umount {destination}"
            elif mount["protocol"] == MountProtocols.girderfs:
                cmd = f"fusermount -u {destination}"
            elif mount["protocol"] in (
                MountProtocols.bind, MountProtocols.overlay, MountProtocols.passthrough
            ):
                cmd = f"sudo umount {destination}"
            try:
                subprocess.check_output(cmd, shell=True)
                os.rmdir(destination)
            except (subprocess.CalledProcessError, OSError):
                errmsg += "Failed to unmount {} \n".format(destination)
                pass
        try:
            for entry in self.state["remove_dirs"]:
                try:
                   os.rmdir(entry)
                except OSError:
                    errmsg += "Failed to remove {} \n".format(entry)
                    pass
        except KeyError:
            pass

        try:
            os.rmdir(self.state["root"])
        except OSError:
            errmsg += "Failed to remove {} \n".format(self.state["root"])
            pass

        if self.state.get("session"):
            try:
                self.gc.delete(f"dm/session/{self.state['session']['_id']}")
            except Exception:
                errmsg += f"Failed to delete session {self.state['session']['_id']} \n"
                pass
        self.state.clear()
        if errmsg:
            self.set_status(500)
            self.write(errmsg)

    def source_path_bind(self, mount_type):
        if mount_type == MountTypes.home:
            login = self.state["user"]["login"]
            return f"homes/{login[0]}/{login}"
        elif mount_type == MountTypes.run:
            run = self.state["run"]
            return f"runs/{run['taleId'][0:2]}/{run['taleId']}/{run['_id']}/workspace"
        elif mount_type == MountTypes.workspace:
            tale = self.state["tale"]
            return f"workspaces/{tale['_id'][0]}/{tale['_id']}"

    def source_path_overlay(self, mount_type, layer="lower"):
        if mount_type == MountTypes.run:
            if layer == "upper":
                return self.source_path_bind(mount_type)
            elif layer == "lower":
                run = self.state["run"]
                return os.path.join(
                    "versions", run["taleId"][0:2], run["taleId"], run["runVersionId"], "workspace"
                )

    def webdav_url(self, mount_type):
        if mount_type == MountTypes.home:
            return f"/homes/{self.state['user']['_id']}"
        elif mount_type == MountTypes.run:
            return f"/runs/{self.state['run']['_id']}"
        elif mount_type == MountTypes.workspace:
            return f"/tales/{self.state['tale']['_id']}"

    def mounttype_to_fs(self, protocol, mount_type):
        if mount_type == MountTypes.versions:
            return self.state["tale"], "wt_versions"
        elif mount_type == MountTypes.data:
            dataset = None
            if "run" in self.state:
                dataset = self.gc.get(
                    f"version/{self.state['run']['runVersionId']}/dataSet"
                )
            else:
                dataset = self.state["tale"]["dataSet"]

            if protocol == MountProtocols.girderfs and "session" not in self.state:
                params = {"dataSet": json.dumps(dataset)}
                if "run" not in self.state:
                    params["taleId"] = self.state["tale"]["_id"]
                self.state["session"] = self.gc.post("dm/session", parameters=params)
                return self.state["session"], "wt_dms"
            elif protocol == MountProtocols.passthrough:
                # We are assuming that dataset is a single folder
                return self.gc.get(f"/folder/{dataset[0]['itemId']}"), ""
        elif mount_type == MountTypes.runs:
            return self.state["tale"], "wt_runs"
        return None, None


def main():
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r"/", MainHandler, {"state": mount})])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
