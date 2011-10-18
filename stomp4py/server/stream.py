'''
Created on 2010/8/22

@author: Victor-mortal
'''
# Based on https://bitbucket.org/victorlin/gevent_stomp

from stomp4py import Frame
from stomp4py.server import BaseHandler

class StreamParser(object):
    """Parser for parsing STOMP frames from a stream
    
    """
    
    # Note: The STOMP standard didn't say anything about what new line character 
    # it should be, it said that's an HTTP school style protocol, therefore, it 
    # should be \r\n, but however, we saw other STOMP library use \n as the
    # newline character. That's why we set it as a variable here 
    newline = '\n'
    
    # Note: The STOMP standard didn't mention that should we strip the key/value
    # of the header, set it to True if you don't want it strips the headers
    stripHeaders = False
    
    # phase for header
    headerPhase = 0
    # phase for body
    bodyPhase = 1
    
    def __init__(self, remain=''):
        self.buffer = [remain]
        
        # what phase we are in
        self._phase = self.headerPhase
        self._command = None
        self._headers = None
        
    def feed(self, data):
        """Feed data to parser
        
        """
        if data == '':
            raise IOError("Connection lost")
        self.buffer.append(data)
        
    def getFrame(self):
        """Get a frame from buffer, if there is no complete frame, return None
        
        """
        if len(self.buffer) > 1:
            self.buffer = [''.join(self.buffer)]
        data = self.buffer[0]
        
        frame = None
        if self._phase == self.headerPhase:
            # read header
            splitter = self.newline*2
            if splitter in data:
                i = data.find(splitter)
                headerLines = data[:i].split(self.newline)
                data = data[i+len(splitter):]
                
                self._command = headerLines[0]
                self._headers = {}
                for line in headerLines[1:]:
                    key, value = line.split(':', 1)
                    key = key
                    value = value
                    if self.stripHeaders:
                        key = key.strip()
                        value = value.strip()
                    self._headers[key.strip()] = value.strip()
                self._phase = self.bodyPhase
        if self._phase == self.bodyPhase:
            # the index of \0 character to read in body
            i = None
            # read content-length bytes from buffer
            if 'content-length' in self._headers:
                length = int(self._headers['content-length'])
                if len(data) >= length:
                    i = length
            # read until null character
            elif '\0' in data:
                i = data.index('\0')
                
            if i is not None:
                body = data[:i]
                data = data[i+1:]
                frame = StreamFrame(self._command, self._headers, body)
                self._command = None
                self._headers = None
                self._phase = self.headerPhase
        
        self.buffer = [data]
        return frame
            
class StreamFrame(Frame):
    """A frame of STOMP protocol
    
    """
    
    newline = '\n'
    
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

class StreamHandler(BaseHandler):
    def __init__(self, socket, broker):
        self.socket = socket
        self.parser = StreamParser()
        super(StreamHandler, self).__init__(broker)
    
    def _recv(self):
        self.parser.feed(self.socket.recv(1024))
        return self.parser.getFrame()
    
    def _send(self, data):
        self.socket.sendall(frame.pack())
    
    def _close(self):
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        except:
            pass