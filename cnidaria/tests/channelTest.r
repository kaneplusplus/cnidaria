require(rzmq)
source("cnidaria/R/channel.r")
context <- init.context()
get.zmq.context <- function() context
get.zmq.address <- function() "tcp://127.0.0.1"

zmqc <- zmqChannel("4141")
send(zmqc, 'test')

recv(zmqc)
