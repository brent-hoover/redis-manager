#!/usr/bin/env python
#-*- coding:utf-8 -*-

import errno
import functools
import json
from tornado import ioloop
from tornado.ioloop import PeriodicCallback
import socket
from connection import Connection



def load_config():
    with open('config/config.json', 'rt') as config_file:
        config = json.loads(config_file.read())
        return config

def ping_hosts(conn):
        print('pinging: %s' % conn.host)

class Watcher(PeriodicCallback):

    def __init__(self, callback, conn, callback_time=1000, io_loop=None):
        print(callback)
        super(Watcher, self).__init__(callback(conn), callback_time=callback_time, io_loop=io_loop)


class ServerManager(object):
    """ Keeps track of which server is Master and which is
    slave and handles any changes necessary
    """

    def __init__(self):
        self.server_watchers = list()

    def parse_redis_config(self, redis_config):
        rconfig_dict = dict()
        rconfig = redis_config.split('\r\n')
        for rc in rconfig:
            if len(rc) != 0:
                x, y = rc.split(':')
                rconfig_dict[x] = y
        return rconfig_dict

    def send_and_parse_command(self, conn, command):
        conn.send_command(command)
        response = conn.read_response()
        return response

    def check_server_alive(self, conn):
        conn.send_command("PING")
        response = conn.read_response()
        if response == "PONG":
            return True
        else:
            return False

    def get_config_info(self, conn):
        conn.send_command("INFO")
        response = conn.read_response()
        return self.parse_redis_config(response)


    def connection_ready(self, conn):
        """
        Verify server is up and check if it is a master or a slave
        """
        self.servers = list()
        self.slave_servers = list()
        self.master_servers = list()
        self.server_config = dict()
        if self.check_server_alive(conn):
            self.servers.append(conn.host)
        self.server_config[conn.host] = self.get_config_info(conn)
        self.server_config[conn.host]['conn'] = conn
        if self.server_config[conn.host]['role'] == "master":
            print('Server %s is Master' % conn.host)
            self.master_servers.append(self.server_config[conn.host])

        if self.server_config[conn.host]['role'] == "slave":
            print('Server %s is Slave' % conn.host)
            self.slave_servers.append(self.server_config[conn.host])

        periodic_callback = ping_hosts
        server_watcher = Watcher(periodic_callback, conn, callback_time=1000, io_loop=io_loop)
        self.server_watchers.append(server_watcher)

    def start_watchers(self):
        for x in self.server_watchers:
            x.start




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
io_loop.start()

