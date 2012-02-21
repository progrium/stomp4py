import collections
import uuid
import time
import socket

class Subscription(object):
    def __init__(self, handler, frame):
        self.handler = handler
        self.id = frame.id or uuid.uuid4().hex
        self.destination = frame.destination
        self.active = True
    
    def send(self, body, headers=None):
        if self.active:
            self.handler._send_message(self, body, headers)
        else:
            raise IOError("Subscription no longer active")
    
    def cancel(self):
        self.active = False

class BaseHandler(object):
    subscription_class = Subscription
    
    def __init__(self, app=None):
        if app:
            self.app = app
        else:
            self.app = lambda f,p: None
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
                self.handle_disconnect()
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
    
    def _send_message(self, subscription, body, headers=None):
        if headers is None:
            headers = {}
        headers['destination'] = subscription.destination
        headers['subscription'] = subscription.id
        if 'message-id' not in headers:
            headers['message-id'] = uuid.uuid4().hex
        self._send("MESSAGE", headers, body)

    def _dispatch(self, frame):
        frame.conn_id = id(self)
        handler = 'handle_%s' % frame.command.lower()
        if hasattr(self, handler):
            getattr(self, handler)(frame)
            if frame.receipt:
                self._send_receipt(frame)
        else:
            print "Unknown command: %s" % frame.command

    def handle_connect(self, frame):
        headers = {'session': uuid.uuid4().hex}
        self.app(frame, headers)
        self._send("CONNECTED", headers)
    
    def handle_begin(self, frame):
        self.transactions[frame.transaction] = []
    
    def handle_commit(self, frame):
        self.app(frame, self.transactions.pop(frame.transaction, []))

    def handle_abort(self, frame):
        del self.transactions[frame.transaction]

    def handle_send(self, frame):
        if frame.transaction:
            self.transactions[frame.transaction].append(frame)
        else:
            self.app(frame, frame.body)

    def handle_subscribe(self, frame):
        if frame.ack == 'auto':
            subscription = self.subscription_class(self, frame)
            self.subscriptions[subscription.id] = subscription
            self.app(frame, subscription)
        else:
            frame.error = "Ack mode 'auto' is the only supported mode"

    def handle_unsubscribe(self, frame):
        if frame.subscription in self.subscriptions:
            subscription = self.subscriptions.pop(frame.subscription)
            subscription.cancel()
            self.app(frame, subscription)
        else:
            frame.error = "Subscription does not exist"
        
    def handle_disconnect(self, frame=None):
        self.app(frame, self.subscriptions.values())
        self.subscriptions = {}
        self.connected = False
