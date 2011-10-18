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
        for sub in self.subscriptions:
            self.broker.unsubscribe(*self.subscriptions[sub])
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
    
    def _send_message(self, destination, subscription, message_id, body):
        self._send("MESSAGE", {
            'destination': destination, 
            'message-id': message_id,
            'subscription': subscription}, body)

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
            sub_id = frame.id or uuid.uuid4().hex
            cb = lambda id, body: self._send_message(frame.destination, sub_id, id, body)
            self.subscriptions[sub_id] = (frame.destination, cb)
            self.broker.subscribe(frame.destination, self.subscriptions[sub_id][1])
        else:
            frame.error = "Ack mode 'auto' is the only supported mode"

    def handle_unsubscribe(self, frame):
        if frame.subscription in self.subscriptions:
            sub = self.subscriptions.pop(frame.subscription)
            self.broker.unsubscribe(*sub)
        else:
            frame.error = "Subscription does not exist"
        
    def handle_disconnect(self, frame):
        self.connected = False
