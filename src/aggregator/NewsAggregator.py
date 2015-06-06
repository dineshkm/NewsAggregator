from socket import SHUT_RDWR, error as _SocketException
import sys
import json
import threading
import urlparse
import BaseHTTPServer
from SocketServer import ThreadingMixIn
import signal
import NewsCollector
import NewsService
import storage

service = None
clientConnections = []

PORT = 8800


def getPort():
    global PORT
    return int(PORT)

#//////////////////////////////////////////////////////////////////////////////
# One handler == one connection, so for HTTP 1.0 == one request, but not for http 1.1
#//////////////////////////////////////////////////////////////////////////////
class Handler(BaseHTTPServer.BaseHTTPRequestHandler):

    def handle(self):
        try:
            clientConnections.append(self.request)
            BaseHTTPServer.BaseHTTPRequestHandler.handle(self)
        except _SocketException as e:
            print('Socket error serving request:', str(e))
        finally:
            print('Removing connection from:', self.client_address)
            clientConnections.remove(self.request)

    def sendResponse(self, response):
        self.send_response(200, "Ok")
        if response is not None:
            self.send_header("Content-type", "application/json")
            self.send_header("Content-Length", str(len(response)))
        self.end_headers()

        if response is not None:
            self.wfile.write(response)
        return

    def sendError(self, errorCode, errorMessage):
        self.send_response(errorCode, errorMessage)
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-Length", str(len(errorMessage)))
        self.end_headers()
        self.wfile.write(errorMessage)
        return

    def do_GET(self):
        (scm, netloc, path, params, query, fragment) = urlparse.urlparse(self.path, 'http')
        print path
        if path.find("techcrunch")>0:
            resp = json.dumps(service.fecthSiteNews("TechCrunch"))
            self.sendResponse(resp)
        elif path.find("techgig")>0:
            resp = json.dumps(service.fecthSiteNews("TechGig"))
            self.sendResponse(resp)
        return
       

def httpdShutdown():
    try:
        print('Trying to stop httpd server (server socket)')
        httpd.shutdown()
        httpd.socket.close()
        print('httpd shutdown has succeeded')
    except:
        sys.exit(0)


def on_SIGINT(signal, frame):
    td = threading.Thread(target=httpdShutdown, name='httpd stopper')
    td.setDaemon(1)
    print("Shutdown initiated by Ctrl-C from user")
    td.start()


# class ThreadedHTTPServer(BaseHTTPServer.HTTPServer):
class ThreadedHTTPServer(ThreadingMixIn, BaseHTTPServer.HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        BaseHTTPServer.HTTPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate)


def main(args):
    global service
    expireTime = 30 # 30 days of news only maintained in the application.
    
    signal.signal(signal.SIGINT, on_SIGINT)

    db = storage.StorageCassandra(
        ['%s:%s' % ("127.0.0.1", "9160")], expireTime)
    service = NewsService.Service(db)
    collector = NewsCollector.Collector(db)
    collector.start()

    Handler.close_connection = 0
    Handler.protocol_version = 'HTTP/1.1'
    global httpd
    httpd = ThreadedHTTPServer(("", getPort()), Handler)
    httpd.daemon_threads = True
    print("serving at port:", getPort())

    try:
        httpd.serve_forever()
    finally:
        print("News Aggregator is shutting down. Need to close %s client connections", len(clientConnections))
        for conn in clientConnections:
            try:
                conn.shutdown(SHUT_RDWR)
                conn.close()
            except Exception as e:
                print('Problems shutting down client connection:', str(e))
        sys.exit(0)

#//////////////////////////////////////////////////////////////////////////////
#//                             M A I N
#//////////////////////////////////////////////////////////////////////////////
if __name__ == "__main__":
    main(sys.argv)
