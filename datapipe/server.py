
import tornado
import tornado.web
import tornado.ioloop
import tornado.httpserver

def serve():
    app = tornado.web.Application([
        ('/', tornado.web.RedirectHandler, {'url': '/index.html'}),
        (r'/(index\.html)', tornado.web.StaticFileHandler, {'path': 'datapipe/server/'}),
        (r'/js/(.*)', tornado.web.StaticFileHandler, {'path': 'datapipe/server/js'}),
    ], debug=True, static_path='datapipe/server')
    app.listen(8080)
    tornado.ioloop.IOLoop.instance().start()

def stop():
    tornado.ioloop.IOLoop.instance().stop()

