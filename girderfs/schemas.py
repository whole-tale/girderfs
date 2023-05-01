import json

import jsonschema
import requests


class MountTypes:
    data = "data"
    home = "home"
    workspace = "workspace"
    run = "run"
    runs = "runs"
    versions = "versions"


class MountProtocols:
    bind = "bind"
    girderfs = "girderfs"
    passthrough = "passthrough"
    webdav = "webdav"


class MountValidator:
    def __init__(self, data):
        self.data = data

    def validate(self):
        jsonschema.validate(self.data, mountsSchema)
        for mount in self.data["mounts"]:
            if mount["type"] == MountTypes.data:
                if mount["protocol"] not in [
                    MountProtocols.girderfs,
                    MountProtocols.passthrough,
                ]:
                    raise ValueError(
                        f"Invalid protocol for {MountTypes.data} mount: {mount['protocol']}"
                    )
            elif mount["type"] == MountTypes.home:
                if mount["protocol"] not in [
                    MountProtocols.bind,
                    MountProtocols.webdav,
                ]:
                    raise ValueError(
                        f"Invalid protocol for {MountTypes.home} mount: {mount['protocol']}"
                    )
            elif mount["type"] == MountTypes.workspace:
                if mount["protocol"] not in [
                    MountProtocols.bind,
                    MountProtocols.webdav,
                ]:
                    raise ValueError(
                        f"Invalid protocol for {MountTypes.workspace} mount: {mount['protocol']}"
                    )
            elif mount["type"] == MountTypes.run:
                if mount["protocol"] not in [
                    MountProtocols.bind,
                    MountProtocols.webdav,
                ]:
                    raise ValueError(
                        f"Invalid protocol for {MountTypes.run} mount: {mount['protocol']}"
                    )
            elif mount["type"] == MountTypes.runs:
                if mount["protocol"] not in [MountProtocols.girderfs]:
                    raise ValueError(
                        f"Invalid protocol for {MountTypes.runs} mount: {mount['protocol']}"
                    )
            elif mount["type"] == MountTypes.versions:
                if mount["protocol"] not in [MountProtocols.girderfs]:
                    raise ValueError(
                        f"Invalid protocol for {MountTypes.versions} mount: {mount['protocol']}"
                    )
            else:
                raise ValueError(f"Invalid mount type: {mount['type']}")


mountSchema = {
    "title": "mount",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "A schema representing a single Whole Tale FUSE mount type",
    "type": "object",
    "properties": {
        "type": {
            "description": "The type of mount",
            "type": "string",
            "enum": [
                MountTypes.data,
                MountTypes.home,
                MountTypes.workspace,
                MountTypes.run,
                MountTypes.runs,
                MountTypes.versions,
            ],
        },
        "protocol": {
            "description": "The protocol to use for the mount",
            "type": "string",
            "enum": [
                MountProtocols.bind,
                MountProtocols.girderfs,
                MountProtocols.passthrough,
                MountProtocols.webdav,
            ],
        },
        "location": {
            "description": (
                "The location of the mount relative to the root of the mounts collection"
            ),
            "type": "string",
        },
    },
    "required": ["type", "protocol", "location"],
    "additionalProperties": False,
}

mountsSchema = {
    "title": "mounts",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "A schema representing a collection of Whole Tale FUSE mounts",
    "type": "object",
    "properties": {
        "mounts": {
            "description": "A collection of mounts",
            "type": "array",
            "items": mountSchema,
        },
        "girderApiUrl": {
            "description": "The url of the girder api",
            "type": "string",
        },
        "girderApiKey": {
            "description": "The api key for the girder api",
            "type": "string",
        },
        "root": {
            "description": "The root of the mounts collection",
            "type": "string",
        },
        "taleId": {
            "description": (
                "The id of the Tale. Required for workspace, versions, and runs mounts "
                "and optional for data mount."
            ),
            "type": "string",
        },
        "userId": {
            "description": (
                "The id of the user. Required for home mount. If not provided, the user "
                "will be derived using girderApiKey."
            ),
            "type": "string",
        },
        "runId": {
            "description": "The id of the run. Required for run mount.",
            "type": "string",
        },
        "sessionId": {
            "description": (
                "The id of the session. Required for data mount. If not provided, "
                "the session will be created from Tale's dataSet."
            ),
            "type": "string",
        },
    },
    "required": ["mounts", "girderApiUrl", "girderApiKey", "root"],
    "additionalProperties": False,
}

# Write example curl POST utilizing mountsSchema
# Path: mounts_example.py

if __name__ == "__main__":
    mounts = {
        "mounts": [
            {
                "type": "data",
                "protocol": "girderfs",
                "location": "/mnt/girderfs",
                "girderObject": {
                    "_modelType": "folder",
                    "_id": "5d9b1b9b2f4a9c0001e9b1a6",
                },
            },
            {
                "type": "home",
                "protocol": "bind",
                "location": "/mnt/home",
                "girderObject": {
                    "_modelType": "user",
                    "_id": "5d9b1b9b2f4a9c0001e9b1a6",
                },
            },
            {
                "type": "workspace",
                "protocol": "bind",
                "location": "/mnt/workspace",
                "girderObject": {
                    "_modelType": "tale",
                    "_id": "5d9b1b9b2f4a9c0001e9b1a6",
                },
            },
            {
                "type": "runs",
                "protocol": "bind",
                "location": "/mnt/runs",
                "girderObject": {
                    "_modelType": "tale",
                    "_id": "5d9b1b9b2f4a9c0001e9b1a6",
                },
            },
            {
                "type": "versions",
                "protocol": "bind",
                "location": "/mnt/versions",
                "girderObject": {
                    "_modelType": "tale",
                    "_id": "5d9b1b9b2f4a9c0001e9b1a6",
                },
            },
        ],
        "girderApiUrl": "https://girder.dandiarchive.org/api/v1",
        "girderApiKey": "5d9b1b9b2f4a9c0001e9b1a6",
    }
    response = requests.post(
        "http://localhost:8888/",
        data=json.dumps(mounts),
        headers={"Content-Type": "application/json"},
    )
    print(response.text)
