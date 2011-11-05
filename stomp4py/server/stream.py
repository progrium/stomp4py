import socket

from stomp4py.frame import StompFrame
from stomp4py.parser import StompParser
from stomp4py.server.base import BaseHandler

class StompHandler(BaseHandler):
    def __init__(self, socket, broker):
        self.socket = socket
        self.parser = StompParser()
        super(StreamHandler, self).__init__(broker)
    
    def _recv(self):
        self.parser.feed(self.socket.recv(1024))
        return self.parser.getFrame()
    
    def _send(self, command, headers, body=None):
        self.socket.sendall(StompFrame(command, headers, body).pack())
    
    def _close(self):
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except:
            pass