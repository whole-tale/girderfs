import os
import json
import subprocess
from girder_client import GirderClient
from .schemas import MountTypes, MountValidator, MountProtocols


class MountHandler:
    _gc = None

    def __init__(self, state):
        MountValidator(state).validate()
        self.state = state

    @classmethod
    def from_environment_variable(cls):
        """
        Create an instance of the class using the state defined in the GIRDERFS_DEF environment variable.

        Returns:
            An instance of the class initialized with the state from the environment variable.

        Raises:
            json.JSONDecodeError: If the JSON in the environment variable is invalid.
            TypeError: If the GIRDERFS_DEF environment variable is not set.
        """
        try:
            state = json.loads(os.environ.get("GIRDERFS_DEF"))
            return cls(state)
        except json.JSONDecodeError:
            print("Invalid JSON in GIRDERFS_DEF environment variable.")
        except TypeError:
            print("GIRDERFS_DEF environment variable not set.")

    @property
    def gc(self):
        if self._gc is None:
            self._gc = GirderClient(apiUrl=self.state["girderApiUrl"])
            try:
                self._gc.token = self.state["girderToken"]
            except KeyError:
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

    def webdav_url(self, mount_type):
        if mount_type == MountTypes.home:
            return f"/homes/{self.state['user']['login']}"
        elif mount_type == MountTypes.run:
            return f"/runs/{self.state['run']['_id']}"
        elif mount_type == MountTypes.workspace:
            return f"/tales/{self.state['tale']['_id']}"

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

    def mount(self, mount_def) -> None:
        destination = os.path.join(self.state["root"], mount_def["location"])
        if not os.path.exists(destination):
            os.makedirs(destination)

        gcObj, fs_type = self.mounttype_to_fs(mount_def["protocol"], mount_def["type"])

        if mount_def["protocol"] == MountProtocols.webdav:
            girder_url = self.gc.urlBase.replace("api/v1", "").rstrip("/")
            args = {
                "user": self.state["user"]["login"],
                "pass": "token:{}".format(self.gc.token),
                "destination": destination,
                "opts": "-o uid=1000,gid=100,file_mode=0600,dir_mode=2700",
                "source": girder_url + self.webdav_url(mount_def["type"]),
            }
            cmd = 'echo "{user}\n{pass}" | sudo mount.davfs {opts} {source} {destination}'
            cmd = cmd.format(**args)
        elif mount_def["protocol"] == MountProtocols.girderfs:
            cmd = (
                f"girderfs -c {fs_type} "
                f"--api-url {self.state['girderApiUrl']} "
                f"--token {self.gc.token} "
                f"{destination} {gcObj['_id']}"
            )
        elif mount_def["protocol"] == MountProtocols.bind:
            source = os.path.join(
                os.environ["WT_VOLUMES_PATH"], self.source_path_bind(mount_def["type"])
            )
            cmd = f"sudo mount --bind {source} {destination}"
        elif mount_def["protocol"] == MountProtocols.passthrough:
            cmd = (
                "passthrough-fuse -o allow_other "
                f"--girder-url={self.state['girderApiUrl']}/tale/{gcObj['_id']}/listing "
                f"--token={self.gc.token} {destination}"
            )

        subprocess.check_output(cmd, shell=True)

    def mount_all(self):
        self.get_girder_objects()
        for mount_def in self.state["mounts"]:
            self.mount(mount_def)

    def umount(self, mount_def):
        errmsg = ""
        destination = os.path.join(self.state["root"], mount_def["location"])
        if mount_def["protocol"] in (
            MountProtocols.webdav,
            MountProtocols.bind,
            MountProtocols.passthrough,
        ):
            cmd = f"sudo umount {destination}"
        elif mount_def["protocol"] == MountProtocols.girderfs:
            cmd = f"fusermount -u {destination}"
        try:
            subprocess.check_output(cmd, shell=True)
            os.rmdir(destination)
        except (subprocess.CalledProcessError, OSError):
            errmsg += "Failed to unmount {} \n".format(destination)
            pass
        return errmsg

    def umount_all(self):
        errmsg = ""
        self.get_girder_objects()
        for mount_def in self.state["mounts"]:
            errmsg += self.umount(mount_def)
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
        return errmsg


def mount():
    MountHandler.from_environment_variable().mount_all()


def umount():
    MountHandler.from_environment_variable().umount_all()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python mounter.py [mount|umount]")
    elif sys.argv[1] == "mount":
        mount()
    elif sys.argv[1] == "umount":
        umount()
    else:
        print("Invalid command. Available commands: mount, umount")
