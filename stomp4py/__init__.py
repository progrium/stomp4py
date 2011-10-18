class Frame(object):
    """A frame of STOMP protocol
    
    """
    def __init__(self, command, headers=None, body=None):
        self.command = command
        self.headers = headers
        if self.headers is None:
            self.headers = {}
        self.body = body or ''
        self.error = None
    
    @property
    def destination(self):
        """ SEND, SUBSCRIBE, MESSAGE """
        return self.headers.get('destination')
    
    @property
    def transaction(self):
        """ SEND, ACK, NACK, BEGIN, COMMIT, ABORT """
        return self.headers.get('transaction')
    
    @property
    def id(self):
        """ SUBSCRIBE, UNSUBSCRIBE """
        return self.headers.get('id')
    
    @property
    def ack(self):
        """ SUBSCRIBE """
        return self.headers.get('ack', 'auto')
    
    @property
    def subscription(self):
        """ ACK, NACK, MESSAGE """
        return self.headers.get('subscription')
    
    @property
    def message_id(self):
        """ ACK, NACK, MESSAGE """
        return self.headers.get('message-id')
    
    @property
    def receipt(self):
        """ SEND, SUBSCRIBE, UNSUBSCRIBE, ACK, NACK, BEGIN, COMMIT, ABORT """
        return self.headers.get('receipt')
    
    @property
    def receipt_id(self):
        """ RECEIPT, ERROR """
        return self.headers.get('receipt-id')
    
    def pack(self):
        raise NotImplemented()