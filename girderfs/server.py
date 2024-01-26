# Basic tornado server that handles POST and DELETE requests
import os

import jsonschema
import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options
from .mounter import MountHandler

define("port", default=8888, help="run on the given port", type=int)


store = {}


class MainHandler(tornado.web.RequestHandler):
    def post(self):
        global store
        try:
            data = tornado.escape.json_decode(self.request.body)
            self.mh = MountHandler(data)
        except jsonschema.exceptions.ValidationError as e:
            self.set_status(400)
            self.write(e.message)
            return
        except ValueError as e:
            self.set_status(400)
            self.write(str(e))
            return
        self.mh.state["root"] = os.path.join(
            os.environ["WT_VOLUMES_PATH"], "mountpoints", data["root"]
        )
        if not os.path.exists(self.mh.state["root"]):
            os.makedirs(self.mh.state["root"])
        self.mh.mount_all()
        store = self.mh.state.copy()

    def delete(self):
        global store
        self.mh = MountHandler(store)
        errmsg = self.mh.umount_all()
        print(f"E: '{errmsg}'")
        if errmsg:
            self.set_status(500)
            self.write(errmsg)


def main():
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r"/", MainHandler, {})])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
