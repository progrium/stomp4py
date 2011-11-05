from stomp4py.frame import StompFrame
from stomp4py.parser import StompParser
from stomp4py.server.base import BaseHandler

class StompHandler(BaseHandler):
    """ We're assuming a bit about the websocket API that will be used """
    
    def __init__(self, websocket, broker):
        self.websocket = websocket
        self.parser = StompParser()
        super(StompHandler, self).__init__(broker)
    
    def _recv(self):
        message = self.websocket.receive()
        self.parser.feed(str(message))
        return self.parser.getFrame()
    
    def _send(self, command, headers, body=None):
        self.websocket.send(StompFrame(command, headers, body).pack())
    
    def _close(self):
        self.websocket.close()