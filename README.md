#Redis Manager

This project is a standalone manager that handles Master/Slave failover for Redis. It's meant to run eternally in the
background.

This functionality is now largely supplanted by the built-in Sentinel module from Redis. However, if you wanted
something you could extend, I have been running this in production for months w/o issue.

This project is built around the [Tornado](http://www.tornadoweb.org/) EventLoop and I think is actually a pretty good example of using the EventLoop
in a non-webserver manner to get NodeJs/Twisted style event-based async processing.

My eventual goal with this project is to create a proxy which provides sharding. I have not added that part as it's
very raw still (i.e. it doesn't work)

###Watching the Stats

The Redis Manager write a constant stream of updates to a pub-sub channel that can be used for monitoring. To view
these logs at the console just run logger.py