#!/usr/bin/env python
#-*- coding:utf-8 -*-


import functools
import json
from datetime import datetime
import sys
from tornado import ioloop
from tornado.ioloop import PeriodicCallback

from connection import TornadoConnection
from redis.client import StrictRedis
from redis.exceptions import ConnectionError




def handle_exception():
    print(sys.exc_info())
    io_loop.stop()
    io_loop.close()

def load_config():
    with open('config/config.json', 'rt') as config_file:
        config = json.loads(config_file.read())
        return config



def restart_watchers():
    import pdb; pdb.set_trace()
    callback = sm.connection_ready(conn[sm.master_server])
    io_loop.add_handler(conn[sm.master_server].connect(), callback, 3)
    for host in sm.slave_servers:
        callback = sm.connection_ready(conn[host])
        io_loop.add_handler(conn[host].connect(), callback, 3)
    sm.start_watchers()
    io_loop.start()








class Watcher(PeriodicCallback):

    def __init__(self, callback=None, callback_time=1000, io_loop=None):
        super(Watcher, self).__init__(callback=callback, callback_time=callback_time, io_loop=io_loop)


class ServerManager(object):
    """ Keeps track of which server is Master and which is
    slave and handles any changes necessary
    """

    def __init__(self, io_loop):
        self.io_loop = io_loop
        self.server_watchers = list()
        self.servers = list()
        self.server_fds = dict()
        self.slave_servers = list()
        self.master_server = None
        self.server_config = dict()
        self.server_connections = list()
        self.conn = dict()
        self.config = load_config()
        stats_host = self.config['stats']['host']
        stats_port = self.config['stats']['port']
        stats_db = self.config['stats']['db']
        self.stats_server = StrictRedis(host=stats_host, port=stats_port, db=stats_db)

    def ping_hosts(self, conn):
        """
        The main function that the Periodic Watcher loops over, checking each server
        """
        self.write_logs(message='pinging host %s' % conn.host)
        try:
            conn.send_command('PING')
            response = conn.read_response()
            if response != "PONG":
                self.handler_server_failure(conn)
                raise ConnectionError('RESPONSE NOT PONG')
            self.update_last_checked(conn.host)
        except ConnectionError, e:
            self.write_logs(message='Server Failed Connect:%s' % e)
            self.handler_server_failure(conn)

    def write_logs(self, channel="log", message=None):
        """ Send log statements to a Pub/Sub Channel """
        self.stats_server.publish(channel=channel, message=message)

    def update_master_server(self, master_server):
        """ Update the Master Server records in the stats server """
        self.stats_server.set('master_server', master_server)

    def update_slave_servers(self, slave_server):
        self.stats_server.rpush('slave_servers', slave_server)

    def update_last_checked(self, server):
        self.stats_server.set('server_checked:%s' % server, datetime.now())

    def promote_master(self, master_server):

        self.write_logs(message='Preparing to promote master server: %s' % master_server)
        master_conn = StrictRedis(host=master_server, port=6379, db=0)
        response = master_conn.execute_command("SLAVEOF", "NO", "ONE")
        if response:
            self.write_logs(message='[INFO] New Master successfully promoted')
        self.master_server = master_server
        self.slave_servers.remove(master_server)
        if len(self.slave_servers) == 0:
            self.write_logs(message='[WARN] No remaining Slave Servers')
        else:
            self.reslave_servers()

    def reslave_servers(self):
        reslave = True
        for serv in self.slave_servers:
            temp_conn = StrictRedis(serv, port=6379, db=0)
            response = temp_conn.execute_command("SLAVEOF", self.master_server, 6379)
            if response:
                print('Server %s successfully reslaved to %s' % (serv, self.master_server))
            else:
                reslave = False
        return reslave

    def open_connections(self):
        for host in self.config['hosts']:
            self.conn[host] = TornadoConnection(host=host)
            self.write_logs(message='Setting up connection to: %s' % host)

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
        self.write_logs(message='[DEBUG] Pinging: %s' % conn.host)
        if self.check_server_alive(conn):
            self.servers.append(conn.host)
            self.server_connections.append(conn)
        self.server_config[conn.host] = self.get_config_info(conn)
        self.server_config[conn.host]['conn'] = conn

        if self.server_config[conn.host]['role'] == "master":
            self.write_logs('Server %s:Master' % conn.host)
            self.master_server = conn.host

        if self.server_config[conn.host]['role'] == "slave":
            self.write_logs('Server %s:Slave' % conn.host)
            self.slave_servers.append(conn.host)
            self.write_logs('Current list of Slave servers: %s' % self.slave_servers)

        periodic_callback = functools.partial(self.ping_hosts, conn)
        server_watcher = Watcher(callback=periodic_callback, callback_time=1000, io_loop=self.io_loop)
        #Add each watcher to a list of servers to watch, but don't start them yet
        self.server_watchers.append(server_watcher)
        self.update_slave_servers(conn.host)

    def start_watchers(self):
        self.write_logs(message='stating periodic watchers')
        for x in self.server_watchers:
            x.start()

    def handler_server_failure(self, conn):
        self.stop_watchers()
        if conn.host == self.master_server:
            self.write_logs(message='[ERROR]: Master Server down')
            new_master_server = self.slave_servers[0]
            self.promote_master(new_master_server)
        elif conn.host in self.slave_servers:
            write_logs(message='[ERROR] Slave Server down')

    def stop_watchers(self):
        """
        Shut down watchers while we swap servers around
        """
        for x, y in self.server_fds.items():
            self.io_loop.remove_handler(y)
        self.io_loop.stop()
        for x in self.server_watchers:
            x.stop()
        print('failover handled: youre welome')



def start_tornado():
    io_loop = ioloop.IOLoop.instance()

    #add callback for connection ready
    sm = ServerManager(io_loop)
    sm.open_connections()
    for host in sm.config['hosts']:
        callback = sm.connection_ready(sm.conn[host])
        sm.server_fds[host] = sm.conn[host].connect()
        io_loop.add_handler(sm.server_fds[host], callback, 3)
    sm.start_watchers()
    try:
        io_loop.start()
    except TypeError, e:
        sm.write_logs(message='got Type Error:%s' % e)
        io_loop.stop()

if __name__ == '__main__':
    start_tornado()


