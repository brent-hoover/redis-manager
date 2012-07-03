#!/usr/bin/env python
#-*- coding:utf-8 -*-

import errno
import functools
import json
from tornado import ioloop
from tornado.ioloop import PeriodicCallback
import socket
from connection import Connection

def parse_redis_config(redis_config):
    rconfig = redis_config.split('\r\n')
    rconfig_dict = dict()
    for rc in rconfig:
        if len(rc) != 0:
            x, y = rc.split(':')
            rconfig_dict[x] = y
    return rconfig_dict

def load_config():
    with open('config/config.json', 'rt') as config_file:
        config = json.loads(config_file.read())
        return config

def ping_hosts(server_config):
    for x, y in server_config.items():
        print('pinging')

class Watcher(PeriodicCallback):

    def __init__(self, ping_hosts, server_config, callback_time=1000, io_loop=None):
        callback = ping_hosts(server_config)
        super(Watcher, self).__init__(callback, callback_time=1000, io_loop=io_loop)


class ServerManager(object):
    """ Keeps track of which server is Master and which is
    slave and handles any changes necessary
    """

    def connection_ready(self, conn):
        print(io_loop._handlers)
        self.slave_servers = list()
        server_config = dict()
        conn.send_command("PING")
        response = conn.read_response()
        if response == "PONG":
            print('Server: %s is alive' % conn.host)
        conn.send_command("INFO")
        response = conn.read_response()
        server_config[conn.host] = parse_redis_config(response)
        if server_config[conn.host]['role'] == "master":
            server_config['master'] = server_config[conn.host]

        if server_config[conn.host]['role'] == "slave":
            print('Server %s is Slave' % conn.host)
            self.slave_servers.append(conn.host)
            server_config['slaves'] = self.slave_servers
        self.server_config = server_config
        io_loop.remove_handler(conn._handlers)
        callback = ping_hosts
        server_watcher = Watcher(callback, server_config, callback_time=1000, io_loop=io_loop)
        server_watcher.start()



config = load_config()
conn = dict()
for host in config['hosts']:
    conn[host] = Connection(host=host)
    print('Setting up connection to: %s' % host)

io_loop = ioloop.IOLoop.instance()

#add callback for connection ready
sm = ServerManager()
for host in config['hosts']:
    callback = sm.connection_ready(conn[host])
    io_loop.add_handler(conn[host].connect(), callback, 3)
    print(type(io_loop._handlers))
io_loop.start()

