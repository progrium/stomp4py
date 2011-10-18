import json

from stomp4py import Frame
from stomp4py.server import BaseHandler

class JsonFrame(Frame):
    
    def __init__(self, json_string):
        obj = json.parse(json_string)
        super(JsonFrame, self).__init__(**obj)
    
    def pack(self):
        return json.dumps({
            'command': self.command, 
            'headers': self.headers, 
            'body': self.body,})

class WebSocketHandler(BaseHandler):
    """ We're assuming a bit about the websocket API that will be used """
    
    def __init__(self, socket, broker):
        self.socket = socket
        super(WebSocketHandler, self).__init__(broker)
    
    def _recv(self):
        message = self.socket.receive()
        if message:
            return JsonFrame(message)
    
    def _send(self, data):
        self.socket.send(frame.pack())
    
    def _close(self):
        try:
            self.socket.close()
        except:
            pass