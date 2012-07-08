#!/usr/bin/env python
#-*- coding:utf-8 -*-

import unittest
import redis

import watcher


class TestStatServerMethods(unittest.TestCase):

    def setUp(self):
        self.r = redis.StrictRedis()
        self.r.flushdb()
        self.r.set("master_server", "server1")
        self.r.rpush("slave_servers", "server2")

    def test_that_setup_correct(self):
        master_server = self.r.get("master_server")
        self.assertEqual(master_server, "server1")

    def test_update_slave(self):
        config= watcher.load_config()
        io_loop = None
        sm = watcher.ServerManager(config, io_loop)
        sm.update_slave_servers("server3", "add")
        s_servers = self.r.lrange("slave_servers", 0, -1)
        self.assertEqual(s_servers, ['server2', 'server3'])

    def test_remove_slave(self):
        config= watcher.load_config()
        io_loop = None
        sm = watcher.ServerManager(config, io_loop)
        sm.update_slave_servers("server2", "remove")
        s_servers = self.r.lrange("slave_servers", 0, -1)
        self.assertEqual(s_servers, [])

    def test_promote_master(self):
        config= watcher.load_config()
        io_loop = None
        sm = watcher.ServerManager(config, io_loop)
        sm.slave_servers.append("localhost")
        sm.promote_master("localhost")
        ms = self.r.get("master_server")
        self.assertEqual(ms, "localhost")
