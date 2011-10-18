import collections
import uuid
import time
import socket

from stomp4py.parser import Parser, Frame

class ServerHandler(object):

    def __init__(self, socket, broker):
        self.socket = socket
        self.broker = broker
        self.parser = Parser()
        self.connected = True
    
    def serve(self):
        while self.connected:
            try:
                self.parser.feed(self.socket.recv(1024))
                frame = self.parser.getFrame()
                if frame:
                    self._dispatch(frame)
            except IOError:
                self.connected = False
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except:
            pass
        self.broker.destroy_session(self.session_id)

    def _send(self, frame):
        self.socket.sendall(frame.pack())
    
    def _send_receipt(self, frame):
        if frame.error is not None:
            self._send(Frame("ERROR", {'receipt-id': frame.receipt, 'message': frame.error}))
        else:
            self._send(Frame("RECEIPT", {'receipt-id': frame.receipt}))
    
    def _send_message(self, destination, message_id, body):
        self._send(Frame("MESSAGE", {'destination': destination, 'message-id': message_id}, body))

    def _dispatch(self, frame):
        handler = 'handle_%s' % frame.command.lower()
        if hasattr(self, handler):
            getattr(self, handler)(frame)
        else:
            print "Unknown command: %s" % frame.command

    def handle_connect(self, frame):
        self.session_id = uuid.uuid4().hex
        self._send(Frame("CONNECTED", {'session': self.session_id})

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