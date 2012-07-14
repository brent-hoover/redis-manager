#!/usr/bin/env python
#-*- coding:utf-8 -*-


import functools
import json
from datetime import datetime
import os
from tornado import ioloop
from tornado.ioloop import PeriodicCallback
from redis.client import StrictRedis
from redis.exceptions import ConnectionError

project_root = os.path.abspath(os.path.dirname(__file__))

from connection import TornadoConnection

def load_config():
    """ load global config from json file
    """
    with open(os.path.join(project_root, 'config/config.json'), 'rt') as config_file:
        config = json.loads(config_file.read())
        return config

def load_config_fromredis():
    """
    On reload after failure use the updated parameters now stored locally in Redis
    """

    temp_conn = StrictRedis()
    master_server = temp_conn.get("master_server")
    slave_servers = temp_conn.lrange("slave_servers", 0, -1)
    hosts = slave_servers
    hosts.append(master_server)

    config = load_config()
    config['hosts'] = hosts
    return config


class Watcher(PeriodicCallback):
    """
    These watch each server, waiting to handle a failure
    """

    def __init__(self, callback=None, callback_time=1000, io_loop=None):
        super(Watcher, self).__init__(callback=callback, callback_time=callback_time, io_loop=io_loop)


class ServerManager(object):
    """ Keeps track of which server is Master and which is
    slave and handles any changes necessary
    """

    def __init__(self, config, io_loop):
        self.config = config
        self.io_loop = io_loop
        self.server_watchers = list()
        self.servers = list()
        self.server_fds = dict()
        self.slave_servers = list()
        self.master_server = None
        self.server_config = dict()
        self.server_connections = list()
        self.conn = dict()
        stats_host = self.config['stats']['host']
        stats_port = self.config['stats']['port']
        stats_db = self.config['stats']['db']
        self.stats_server = StrictRedis(host=stats_host, port=stats_port, db=stats_db)
        self.stats_server.flushdb()

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

    def notify_nagios(self, *args, **kwargs):
        """ Stub for now """
        pass


    def update_slave_servers(self, server, action):
        """
        Post-recovery, take a stock of which Slave servers we have and realign them
        """
        if action == "remove":
            self.write_logs(message="Removing Slave server: %s" % server)
            self.stats_server.lrem("slave_servers", 0, server)
        elif action == "add":
            self.write_logs(message="Adding Slave server: %s" % server)
            self.stats_server.rpush('slave_servers', server)

    def get_new_master(self):
        """ Grad the first slave and make it the master """
        return self.stats_server.lpop("slave_servers")

    def update_last_checked(self, server):
        """ Update timestamps to pub/sub server for when we checked each server """
        self.stats_server.set('server_checked:%s' % server, datetime.now())

    def promote_master(self, master_server):
        """ Given our new master server, make it the master """

        self.write_logs(message='Preparing to promote master server: %s' % master_server)
        master_conn = StrictRedis(host=master_server, port=6379, db=0)
        response = master_conn.execute_command("SLAVEOF", "NO", "ONE")
        if response:
            self.write_logs(message='[INFO] New Master successfully promoted')
        self.master_server = master_server
        self.stats_server.set("master_server", self.master_server)
        self.update_slave_servers(master_server, "remove")


        if not self.stats_server.llen("slave_servers"):
            self.write_logs(message='[WARN] No remaining Slave Servers')
        else:
            self.reslave_servers()


    def reslave_servers(self):
        """ Make sure all non-master servers are now slaves of the new master
        """
        reslave = True
        for serv in self.slave_servers:
            temp_conn = StrictRedis(serv, port=6379, db=0)
            response = temp_conn.execute_command("SLAVEOF", self.master_server, 6379)
            if response:
                self.write_logs(message='Server %s successfully reslaved to %s' % (serv, self.master_server))
                self.update_slave_servers(serv, "add")
            else:
                reslave = False
        return reslave

    def open_connections(self):
        """ Use our modified connection object, that looks like a socket to Tornado
        """
        for host in self.config['hosts']:
            self.conn[host] = TornadoConnection(host=host)
            self.write_logs(message='Setting up connection to: %s' % host)

    def parse_redis_config(self, redis_config):
        """ Read the verbose output from the INFO command """
        rconfig_dict = dict()
        rconfig = redis_config.split('\r\n')
        for rc in rconfig:
            if len(rc):
                x, y = rc.split(':')
                rconfig_dict[x] = y
        return rconfig_dict

    def send_and_parse_command(self, conn, command):
        """ Basically a clone of the same method in the redis driver """
        conn.send_command(command)
        response = conn.read_response()
        return response

    def check_server_alive(self, conn):
        """ Send server a PING """
        conn.send_command("PING")
        response = conn.read_response()
        if response == "PONG":
            return True
        else:
            return False

    def get_config_info(self, conn):
        """ Send INFO command, get response """
        conn.send_command("INFO")
        response = conn.read_response()
        return self.parse_redis_config(response)


    def connection_ready(self, conn):
        """
        Callback from when connection first made for a server
        Verify server is up and check if it is a master or a slave
        Add a periodic callback to it
        """
        self.write_logs(message='[DEBUG] Bringing Up Server: %s' % conn.host)
        if self.check_server_alive(conn):
            self.servers.append(conn.host)
        host_config = self.get_config_info(conn)
        if host_config['role'] == "master":
            self.write_logs(message='MASTER: %s' % conn.host)
            self.master_server = conn.host
            self.stats_server.set("master_server", conn.host)

        if host_config['role'] == "slave":
            self.write_logs('SLAVE: %s' % conn.host)
            self.update_slave_servers(conn.host, "add")


        periodic_callback = functools.partial(self.ping_hosts, conn)
        server_watcher = Watcher(callback=periodic_callback, callback_time=1000, io_loop=self.io_loop)
        #Add each watcher to a list of servers to watch, but don't start them yet
        self.server_watchers.append(server_watcher)


    def start_watchers(self):
        """ After being attached, watchers still need to be started, but we wait until they are all in place """
        self.write_logs(message='starting periodic watchers')
        for x in self.server_watchers:
            x.start()

    def stop_watchers(self):
        """
        Shut down watchers while we swap servers around
        """

        for x, y in self.server_fds.items():
            self.io_loop.remove_handler(y)
        for x in self.server_watchers:
            x.stop()


    def handler_server_failure(self, conn):
        """ The main function called to do the important stuff. The hardest thing is to get Tornado to shut up about
        the failure to handle it
        """
        self.stop_watchers()
        if conn.host == self.master_server:
            self.write_logs(message='[ERROR]: Master Server down')
            new_master_server = self.get_new_master()
            self.promote_master(new_master_server)
        else:
            self.write_logs(message='[ERROR] Slave Server down')
            self.notify_nagios()
        self.write_logs(message='failover handled: youre welome')
        config = load_config_fromredis()
        self.write_logs(message="[INFO] Starting Tornado over again")
        start_tornado(config, self.io_loop)


def start_tornado(config, io_loop):
    """ Launch our event loop
    """

    #add callback for connection ready
    sm = ServerManager(config, io_loop)
    sm.open_connections()
    for host in sm.config['hosts']:
        callback = sm.connection_ready(sm.conn[host])
        sm.server_fds[host] = sm.conn[host].connect()
        io_loop.add_handler(sm.server_fds[host], callback, 3)
    sm.start_watchers()

    io_loop.start()
    io_loop.stop()

if __name__ == '__main__':
    #When first starting we load from config file, from then on we use local redis
    #db to find out which servers to watch
    config = load_config()
    io_loop = ioloop.IOLoop.instance()
    start_tornado(config, io_loop)


