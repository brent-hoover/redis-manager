#!/usr/bin/env python
#-*- coding:utf-8 -*-

from redis.client import StrictRedis



def main():
    r = StrictRedis(host='localhost', port=6379, db=0)

    ps = r.pubsub()

    ps.subscribe("logs")
    data = ps.listen()
    print(data)

if __name__ == '__main__':
    main()