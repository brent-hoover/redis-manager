#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import socket
from itertools import chain, imap
from redis.connection import Connection
from redis.exceptions import (
    RedisError,
    ConnectionError,
    ResponseError,
    InvalidResponse,
    AuthenticationError
)


try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

try:
    import hiredis
    hiredis_available = True
except ImportError:
    hiredis_available = False

class PythonParser(object):
    "Plain Python parsing class"
    MAX_READ_LENGTH = 1000000
    encoding = None

    def __init__(self):
        self._fp = None

    def __del__(self):
        try:
            self.on_disconnect()
        except:
            pass

    def on_connect(self, connection):
        "Called when the socket connects"
        self._fp = connection._sock.makefile('rb')
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        "Called when the socket disconnects"
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def read(self, length=None):
        """
        Read a line from the socket is no length is specified,
        otherwise read ``length`` bytes. Always strip away the newlines.
        """
        try:
            if length is not None:
                bytes_left = length + 2 # read the line ending
                if length > self.MAX_READ_LENGTH:
                    # apparently reading more than 1MB or so from a windows
                    # socket can cause MemoryErrors. See:
                    # https://github.com/andymccurdy/redis-py/issues/205
                    # read smaller chunks at a time to work around this
                    try:
                        buf = StringIO()
                        while bytes_left > 0:
                            read_len = min(bytes_left, self.MAX_READ_LENGTH)
                            buf.write(self._fp.read(read_len))
                            bytes_left -= read_len
                        buf.seek(0)
                        return buf.read(length)
                    finally:
                        buf.close()
                return self._fp.read(bytes_left)[:-2]

            # no length, read a full line
            return self._fp.readline()[:-2]
        except (socket.error, socket.timeout), e:
            raise ConnectionError("Error while reading from socket: %s" %\
                                  (e.args,))

    def read_response(self):
        response = self.read()
        if not response:
            raise ConnectionError("Socket closed on remote end")

        byte, response = response[0], response[1:]

        if byte not in ('-', '+', ':', '$', '*'):
            raise InvalidResponse("Protocol Error")

        # server returned an error
        if byte == '-':
            if response.startswith('ERR '):
                response = response[4:]
                return ResponseError(response)
            if response.startswith('LOADING '):
                # If we're loading the dataset into memory, kill the socket
                # so we re-initialize (and re-SELECT) next time.
                raise ConnectionError("Redis is loading data into memory")
        # single value
        elif byte == '+':
            pass
        # int value
        elif byte == ':':
            response = long(response)
        # bulk response
        elif byte == '$':
            length = int(response)
            if length == -1:
                return None
            response = self.read(length)
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for i in xrange(length)]
        if isinstance(response, str) and self.encoding:
            response = response.decode(self.encoding)
        return response

class HiredisParser(object):
    "Parser class for connections using Hiredis"
    def __init__(self):
        if not hiredis_available:
            raise RedisError("Hiredis is not installed")

    def __del__(self):
        try:
            self.on_disconnect()
        except:
            pass

    def on_connect(self, connection):
        self._sock = connection._sock
        kwargs = {
            'protocolError': InvalidResponse,
            'replyError': ResponseError,
            }
        if connection.decode_responses:
            kwargs['encoding'] = connection.encoding
        self._reader = hiredis.Reader(**kwargs)

    def on_disconnect(self):
        self._sock = None
        self._reader = None

    def read_response(self):
        if not self._reader:
            raise ConnectionError("Socket closed on remote end")
        response = self._reader.gets()
        while response is False:
            try:
                buffer = self._sock.recv(4096)
            except (socket.error, socket.timeout), e:
                raise ConnectionError("Error while reading from socket: %s" %\
                                      (e.args,))
            if not buffer:
                raise ConnectionError("Socket closed on remote end")
            self._reader.feed(buffer)
            # proactively, but not conclusively, check if more data is in the
            # buffer. if the data received doesn't end with \n, there's more.
            if not buffer.endswith('\n'):
                continue
            response = self._reader.gets()
        return response

if hiredis_available:
    DefaultParser = HiredisParser
else:
    DefaultParser = PythonParser


class TornadoConnection(Connection):
    "Manages TCP communication to and from a Redis server"


    def connect(self):
        """Connects to the Redis server if not already connected
        Slightly modified to return a socket.fileno() """
        if self._sock:
            return self._sock.fileno()
        try:
            sock = self._connect()
        except socket.error, e:
            raise ConnectionError(self._error_message(e))

        self._sock = sock
        self.on_connect()
        return self._sock.fileno()

    def get_fileno(self):
        return self._sock.fileno()


    def on_connect(self):
        "Initialize the connection, authenticate and select a database"
        self._parser.on_connect(self)

        # if a password is specified, authenticate
        if self.password:
            self.send_command('AUTH', self.password)
            if self.read_response() != 'OK':
                raise AuthenticationError('Invalid Password')

        # if a database is specified, switch to it
        if self.db:
            self.send_command('SELECT', self.db)
            if self.read_response() != 'OK':
                raise ConnectionError('Invalid Database')


    def disconnect(self):
        "Disconnects from the Redis server"
        self._parser.on_disconnect()
        if self._sock is None:
            return
        try:
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"
        if not self._sock:
            self.connect()
        try:
            self._sock.sendall(command)
        except socket.error, e:
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." %\
                                  (_errno, errmsg))
        except:
            self.disconnect()
            raise

    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = self._parser.read_response()
        except:
            self.disconnect()
            raise
        if response.__class__ == ResponseError:
            raise response
        return response

