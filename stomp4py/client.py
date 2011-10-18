import uuid
import time

from stomp4py.parser import StompParser

# WORK IN PROGRESS
# Borrowed from https://github.com/coopernurse/radiator as a baseline
# Haven't gotten to integrating it with the parser

class StompClient(object):

    def __init__(self, conn, on_error=None, write_timeout=60):
        self.f = conn
        self.write_timeout = write_timeout
        self.on_error = on_error or on_error_default
        self.callbacks = { }
        self.receipts  = [ ]
        self.connect()

    def connect(self):
        self.connected = True
        self._write_frame("CONNECT")
        f = self._read_frame(100)
        if f["command"] == "CONNECTED":
            self.session_id = f["headers"]["session"]
        else:
            raise IOError("Invalid frame after CONNECT: %s" % str(f))

    def disconnect(self):
        self._write_frame("DISCONNECT")
        self.f.close()
        self.connected = False
        self.f = None

    def call(self, dest_name, body, timeout=60, call_sleep_sec=0.005,
             headers=None, receipt=False):
        resp_body = [ ]
        def on_msg(s, m_id, r_body):
            resp_body.append(r_body)
            
        reply_to_id = uuid.uuid4().hex
        if not headers:
            headers = { }
        headers['reply-to'] = reply_to_id
        self.callbacks[reply_to_id] = on_msg
        self.send(dest_name, body, headers, receipt)
        timeout_sec = time.time() + timeout
        while time.time() < timeout_sec and len(resp_body) == 0:
            time.sleep(self.call_sleep_sec)
        if resp_body:
            return resp_body[0]
        else:
            raise RadiatorTimeout("No response to message: %s within timeout: %.1f" % \
                                  (body, timeout))

    def send(self, dest_name, body, headers=None, receipt=False):
        headers_arr = [ "destination:%s" % dest_name ]
        if headers:
            for k,v in headers.items():
                headers_arr.append("%s:%s" % (k, v))
        r = None
        if receipt:
            r = self._create_receipt(headers)
        self._write_frame("SEND", headers=headers_arr, body=body)
        if receipt: self._wait_for_receipt(r)

    def subscribe(self, dest_name, callback, auto_ack=True, receipt=False):
        ack = "client"
        if auto_ack: ack = "auto"
        headers = [ "destination:%s" % dest_name, "ack:%s" % ack ]
        headers = self._create_headers(receipt, headers)
        self._write_frame("SUBSCRIBE", headers=headers)
        if receipt: self._wait_for_receipt(headers['receipt'])
        self.callbacks[dest_name] = callback

    def unsubscribe(self, dest_name, receipt=False):
        headers = self._create_headers(receipt, ["destination:%s" % dest_name])
        self._write_frame("UNSUBSCRIBE", headers=headers)
        if receipt: self._wait_for_receipt(headers['receipt'])
        self.drain(timeout=0.1)
        if self.callbacks.has_key(dest_name):
            del(self.callbacks[dest_name])

    def ack(self, msg_id, receipt=False):
        headers = self._create_headers(receipt, ["message-id:%s" % msg_id])
        self._write_frame("ACK", headers=headers)
        if receipt: self._wait_for_receipt(headers['receipt'])

    def _dispatch(self, frame):
        headers = frame["headers"]
        cmd = frame["command"]
        if   cmd == "MESSAGE"  : self._on_message(frame)
        elif cmd == "RECEIPT"  : self.receipts.append((headers["receipt"]))
        elif cmd == "ERROR"    : self.on_error(headers["message"],
                                               dict_get(frame, "body", ""))
        else:
            print "Unknown command: %s" % cmd

    def _on_message(self, frame):
        dest_name  = frame["headers"]["destination"]
        message_id = frame["headers"]["message-id"]
        body = dict_get(frame, "body", "")
        if self.callbacks.has_key(dest_name):
            self.callbacks[dest_name](self, message_id, body)
        else:
            self.on_error("No subscriber registered for destination: " +
                          "%s - but got message: %s %s" %
                          (dest_name, message_id, body))
            
    def _create_headers(self, receipt, headers):
        if receipt:
            headers.append("receipt:%s" % uuid.uuid4().hex)
        return headers

    def _create_receipt(self, headers):
        receipt = uuid.uuid4().hex
        headers.append("receipt:%s" % receipt)
        return receipt

    def _wait_for_receipt(self, receipt, timeout_sec=60):
        timeout = time.time() + timeout_sec
        while time.time() > timeout:
            self.drain(timeout=.05)
            if receipt in self.receipts:
                self.receipts.remove(receipt)
                return
        raise IOError("No receipt %s received after %d seconds" %
                      (receipt, timeout_sec))
