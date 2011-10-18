import uuid
import time
import socket

class BaseHandler(object):
    
    def __init__(self, broker):
        self.broker = broker
        self.connected = True
        self.subscriptions = {}
        self.transactions = {}
    
    def serve(self):
        while self.connected:
            try:
                frame = self._recv()
                if frame:
                    self._dispatch(frame)
            except IOError:
                self.connected = False
        self._close()

    def _close(self):
        raise NotImplemented()

    def _recv(self):
        raise NotImplemented()

    def _send(self, command, headers, body=None):
        raise NotImplemented()
    
    def _send_receipt(self, frame):
        if frame.error is not None:
            self._send("ERROR", {'receipt-id': frame.receipt, 'message': frame.error})
        else:
            self._send("RECEIPT", {'receipt-id': frame.receipt})
    
    def _send_message(self, destination, message_id, body):
        self._send("MESSAGE", {'destination': destination, 'message-id': message_id}, body)

    def _dispatch(self, frame):
        handler = 'handle_%s' % frame.command.lower()
        if hasattr(self, handler):
            getattr(self, handler)(frame)
            if frame.receipt:
                self._send_receipt(frame)
        else:
            print "Unknown command: %s" % frame.command

    def handle_connect(self, frame):
        self.session_id = uuid.uuid4().hex
        self._send("CONNECTED", {'session': self.session_id})
    
    def handle_begin(self, frame):
        self.transactions[frame.transaction] = []
    
    def handle_commit(self, frame):
        for frame in self.transactions.pop(frame.transaction, []):
            self.broker.send(frame.destination, frame.body)

    def handle_abort(self, frame):
        del self.transactions[frame.transaction]

    def handle_send(self, frame):
        if frame.transaction:
            self.transactions[frame.transaction].append(frame)
        else:
            self.broker.send(frame.destination, frame.body)

    def handle_subscribe(self, frame):
        if frame.ack == 'auto':
            self.subscriptions[frame.destination] = self._send_message
            self.broker.subscribe(frame.destination, self.subscriptions[frame.destination])
        else:
            frame.error = "Ack mode 'auto' is the only supported mode"

    def handle_unsubscribe(self, frame):
        if frame.destination in self.subscriptions:
            self.broker.unsubscribe(frame.destination, self.subscriptions.pop(frame.destination))
        else:
            frame.error = "Subscription does not exist"
        
    def handle_disconnect(self, frame):
        self.connected = False
