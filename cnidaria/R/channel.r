
# This will need to be abstracted to hadle different key value databases.
#' @export
cluster.write <- function(resource, packet) {
  redisLPush(resource, packet)
}

#' @export
cluster.read <- function(resource, timeout=0) {
  redisBRPop(resource, timeout=timeout)
}

# note that a packet is a named list of type (push/pull), 
# expr (character/expression), and retq (the name of the return channel).
#' @export
send <- function(channel, packet) {
  UseMethod("send", channel)  
}

#' @export
recv <- function(channel) {
  UseMethod("recv", channel)  
}

#' @export
localChannel <- function(expr, type, resultHandle=NULL) {
  # TODO: return a future object.
  # type needs to be push or pull.
  if (type == "pull") {
    ret <- pull(get.local.con(), expr)
  }
  else if (type == "push") {
    if (is.null(resultHandle))
      stop("A result handle must be specified for a local channel")
    ret <- push(get.local.con(), expr, resultHandle)
  }
  else {
    stop("Unknown localChannel type")
  }
  ret <- list(ret=ret)
  class(ret) <- c("localChannel", "channel")
  ret
}

#' @export
recv.localChannel <- function(channel) {
  channel$ret
#  serviceAll()
#  ret <- NULL
#  if (channel$type == "pull")
#    ret <- pull(get.local.con(), channel$expr)
#  else if (channel$type == "push")
#    ret <- push(get.local.con(), channel$expr)
#  else
#    stop("Unknown localChannel type")
#  ret
}

# This will only be used for getting the result of a push.
#' @export
clusterChannel <- function(resource, timeout=0, broadcast=FALSE) {
  ret <- list(resource=resource)
  class(ret) <- c("clusterChannel", "channel")
}

# resource is the fully qualified zeromq channel.
# comType should be ZMQ_PUSH or ZMQ_PULL
# con should be bind.socket or connect.socket.
#' @export
zmqChannel <- function(resource, con=bind.socket, comType="ZMQ_PUSH", 
  timeout=0, broadcast=FALSE) {
  if (broadcast)
    stop("Broadcasting for rzmq channels has not been implemented yet.") 
  context <- get.zmq.context()
  socket <- init.socket(context, comType)
  #zmqAddress <- paste(get.zmq.address(), ":", $resource, sep="")
  if (!con(socket, resource)) {
    ret <- NULL
  } else {
    set.linger(socket, as.integer(0))
    ret <- list(socket=socket)
    class(ret) <- c("zmqChannel", "channel")
  }
  # This sucks but we have to give the socket a chance to actually be created.
  # It would be better if it didn't return until it is actually bound to
  # the socket.
  Sys.sleep(0.1)
  ret
}

#' @export
send.zmqChannel <- function(channel, packet) {
  send.socket(channel$socket, packet)
}

#' @export
recv.zmqChannel <- function(channel) {
  # TODO: fix this so that it polls and times out.
  numTries <- 100
  haveResp <- FALSE
  for (i in 1:numTries) {
    if (poll.socket(list(channel$socket), list("read"), timeout=0L)[[1]]$read) {
      haveResp <- TRUE
      break
    }
    #serviceAll(timeout=0.1)
    service(timeout=0)
    Sys.sleep(0.1)
  }
  if (!haveResp)
    stop("No response found")
  receive.socket(channel$socket)
}

#' @export
recv.clusterChannel <- function(channel) {
  cluster.read(channel)
}


