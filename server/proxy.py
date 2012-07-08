#!/usr/bin/env python
#-*- coding:utf-8 -*-
from tornado.ioloop import IOLoop

from tornado.netutil import TCPServer, bind_sockets, add_accept_handler
from tornado.process import fork_processes

def proxy_connection(conn, addr):
    print(conn)
    print(addr)

class RedisProxy(TCPServer):

    def __init__(self):
        sockets = bind_sockets(6379)


    def read_cb(self, data):
        print(data)



    def handle_stream(self, stream, address):
        print('handling stream')
        stream.read_until(':', self.read_cb)



def main():
    sockets = bind_sockets(8888)
    fork_processes(0)
    server = RedisProxy()
    server.add_sockets(sockets)

    IOLoop.instance().start()



if __name__ == '__main__':
    main()