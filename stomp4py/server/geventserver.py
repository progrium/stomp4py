import collections
import uuid

import gevent.server

from stomp4py.server import ServerHandler

class SimpleChannelBroker(object):
    """ Queue-less channel pubsub broker """
    
    def __init__(self):
        self.channels = collections.defaultdict(set)
        self.subscriber_counts = collections.Counter()
    
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
        message_id = uuid.uuid4().hex
        for subscriber in self.channels[destination]:
            subscriber(destination, message_id, message)

if __name__ == '__main__':
    broker = SimpleChannelBroker()
    
    def handle(socket, address):
        ServerHandler(socket, broker).serve()

    server = gevent.server.StreamServer(('127.0.0.1', 1234), handle) # creates a new server
    print "Starting Stomp server on 1234..."
    server.serve_forever()