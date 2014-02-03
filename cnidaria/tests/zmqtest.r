require(zmqc)

pub <- zmqc("ipc:///tmp/0mq.sock", "w")
sub <- zmqc("ipc:///tmp/0mq.sock", "r")
## subscriptions make take a bit to setup so
## we add a small delay to make sure the subscriber
## is ready
Sys.sleep(1)
serialize(1:10, pub)
close(pub)
#writeLines("hello, world!", pub)
#writeLines("I'm publishing ...", pub)

unserialize(sub)

#readLines(sub, 1)
close(sub)
## clean up the socket since we won't be re-using it
## if you plan to re-use the socket, it is actually
## possble to keep it to allow subscriptions before
## the publisher is started
unlink("/tmp/0mq.sock")


pub <- zmqc("tcp:///localhost:3141", "w")
sub <- zmqc("tcp:///localhost:3141", "r")
## subscriptions make take a bit to setup so
## we add a small delay to make sure the subscriber
## is ready
Sys.sleep(1)
serialize(1:10, pub)
#writeLines("hello, world!", pub)
#writeLines("I'm publishing ...", pub)

unserialize(sub)

#readLines(sub, 1)
close(sub)
close(pub)
## clean up the socket since we won't be re-using it
## if you plan to re-use the socket, it is actually
## possble to keep it to allow subscriptions before
## the publisher is started
unlink("/tmp/0mq.sock")



