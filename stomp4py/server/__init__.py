import uuid
import time
import socket

class BaseHandler(object):
    
    def __init__(self, broker):
        self.broker = broker
        self.connected = True
    
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
        else:
            print "Unknown command: %s" % frame.command

    def handle_connect(self, frame):
        self.session_id = uuid.uuid4().hex
        self._send("CONNECTED", {'session': self.session_id})

    def handle_send(self, frame):
        self.broker.send(frame.destination, frame.body)
        if frame.receipt:
            self._send_receipt(frame)

    def handle_subscribe(self, frame):
        if frame.ack != 'auto':
            frame.error = "Ack mode 'auto' is the only supported mode"
        else:
            self.broker.subscribe(frame.destination, self._send_message)
        if frame.receipt:
            self._send_receipt(frame)

    def handle_unsubscribe(self, frame):
        self.broker.unsubscribe(frame.destination, self._send_message)
        if frame.receipt:
            self._send_receipt(frame)
        
    def handle_disconnect(self, frame):
        if frame.receipt:
            self._send_receipt(frame)
        self.connected = False
