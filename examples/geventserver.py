import collections
import uuid

import gevent.server

from ws4py.server.geventserver import WebSocketServer
from stomp4py.server.websocket import StompHandler

class StompBroker(object):
    """ Queue-less channel pubsub broker """
    
    def __init__(self):
        self.channels = collections.defaultdict(set)
        self.subscriber_counts = collections.Counter()
    
    def __call__(self, frame, payload):
        if frame is None or frame.command == 'DISCONNECT':
            for subscriber in payload:
                self.unsubscribe(subscriber.destination, subscriber)
        elif frame.command == 'SUBSCRIBE':
            self.subscribe(frame.destination, payload)
        elif frame.command == 'UNSUBSCRIBE':
            self.unsubscribe(frame.destination, payload)
        elif frame.command == 'SEND':
            self.send(frame.destination, payload)
        elif frame.command == 'COMMIT':
            for part in payload:
                self.send(part.destination, part.body)
    
    def subscribe(self, destination, subscriber):
        self.subscriber_counts[destination] += 1
        self.channels[destination].add(subscriber)
    
    def unsubscribe(self, destination, subscriber):
        self.subscriber_counts[destination] -= 1
        self.channels[destination].remove(subscriber)
        
        # Clean up counts and channels with no subscribers
        self.subscriber_counts += collections.Counter()
        if not self.subscriber_counts[destination]:
            del self.channels[destination]
    
    def send(self, destination, message):
        for subscriber in self.channels[destination]:
            subscriber.send(message)

if __name__ == '__main__':
    broker = StompBroker()
    
    def websocket_handler(websocket, environ):
        if environ.get('PATH_INFO') == '/stomp':
            StompHandler(websocket, broker).serve()
        else:
            websocket.close()
    
    server = WebSocketServer(('127.0.0.1', 9000), websocket_handler)
    print "Starting STOMP WebSocket server on 9000..."
    server.serve_forever()