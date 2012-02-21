class StompFrame(object):
    """A frame of STOMP protocol
    """
    newline = '\n'
    
    def __init__(self, command, headers=None, body=None):
        self.command = command
        self.headers = headers
        if self.headers is None:
            self.headers = {}
        self.body = body or ''
        self.conn_id = 0
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
    
    def __repr__(self):
        return "<StompFrame:%s %s \"%s\">" % (self.command, self.headers, self.body) 
    
    def pack(self):
        """Pack the frame as a string
        
        """
        if '\0' in self.body:
            self.headers['content-length'] = len(self.body)
         
        headers = [self.command]
        for key, value in self.headers.iteritems():
            line = '%s:%s' % (key, value)
            if isinstance(line, unicode):
                line = line.encode('utf8')
            headers.append(line)
        
        return self.newline.join(headers) + self.newline*2 + self.body + '\0'