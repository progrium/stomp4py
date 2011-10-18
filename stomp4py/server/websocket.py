import json

from stomp4py import Frame
from stomp4py.server import BaseHandler
from stomp4py.server.stream import StreamParser, StreamFrame

class WebSocketHandler(BaseHandler):
    """ We're assuming a bit about the websocket API that will be used """
    
    def __init__(self, websocket, broker):
        self.websocket = websocket
        self.parser = StreamParser()
        super(WebSocketHandler, self).__init__(broker)
    
    def _recv(self):
        message = self.websocket.receive()
        self.parser.feed(str(message))
        return self.parser.getFrame()
    
    def _send(self, command, headers, body=None):
        self.websocket.send(StreamFrame(command, headers, body).pack())
    
    def _close(self):
        self.websocket.close()