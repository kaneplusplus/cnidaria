require(rzmq)
require(rredis)

dist.worker.init <- function(qid=guid(), allq="all", host="localhost",
  port=6379, zmqAddress="tcp://127.0.0.1") {
  if (!(".dist.env" %in% names(options()))) {
    if (is.null(qid))
      qid <- guid()
    .dist.env <- new.env(parent=emptyenv())
    .dist.env$qid <- qid
    .dist.env$redis.host <- host
    .dist.env$redis.port <- port
    .dist.env$allq <- allq
    .dist.env$zmqAddress <- zmqAddress
    .dist.env$zmqContext <- init.context()
    .dist.env$redisAggQ <- makeRAQ(ip=host, port=port)
    addPop(.dist.env$redisAggQ, allq)
    addPop(.dist.env$redisAggQ, qid)
    options(.dist.env=.dist.env)
    redisConnect(host=host, port=port)
  }
  invisible()
}

get.raq <- function() {
  dist.worker.init()
  options()$.dist.env$redisAggQ
}

get.qid <- function() {
  dist.worker.init()
  options()$.dist.env$qid
}

get.zmq.context <- function() {
  dist.worker.init()
  options()$.dist.env$zmqContext
}

get.zmq.address <- function() {
  dist.worker.init()
  options()$.dist.env$zmqAddress
}

get.dist.q <- function() {
  dist.worker.init()
  options()$.dist.env$qid
}

get.all.q <- function() {
  dist.worker.init()
  options()$.dist.env$allq
}

get.redis.host <- function() {
  options()$.redis.host
}

get.redis.port <- function() {
  options()$.redis.port
}

#' @export
pull.character <- function(w, expr) {
  # Is w a local resource?
  localWorker <- get.local.con()
  ret <- invisible()
  if (w %in% c(get.qid(), ls(envir=localWorker$env))) {
    # If so then execute the expression locally.
    #ret <- pull(localWorker, expr)
    ret <- localChannel(expr, "pull")
  } else {
    #retResource <- paste("tcp://localhost:", cnidaria:::guid(), sep="")
    ret <- NULL
    tries <- 4
    try <- 0
    while(is.null(ret) && try < tries) {
      retResource <- paste(get.zmq.address(), ":",
        as.character(sample(100000, 1)+100000), sep="")
      #ret <- zmqChannel(retResource, bind.socket, "ZMQ_PULL")
      ret <- zmqChannel(retResource, bind.socket, "ZMQ_REP")
      if (is.null(ret)) {
        try <- try + 1
        Sys.sleep(0.1)
      }
    }
    if (is.null(ret)) {
      stop("pull was unsuccessful")
    }
    packet <- list(type="pull", expr=expr, retq=retResource)
    print(packet)
    cluster.write(w, packet)
  }
  ret
}

# TODO: register a backend or default to this.
# W is the name of the resource to execute on.
#' @export
push.character <- function(w, expr, resultHandle=guid()) {
  # Is w a local resource?
  localWorker <- get.local.con()
  ret <- invisible()
  if (w %in% c(get.qid(), ls(envir=localWorker$env))) {
    # If so then execute the expression locally.
    #push(localWorker, expr, resultHandle)
    ret <- localChannel(expr, "push", resultHandle)
  } else {
    # Otherwise, push it out to be consumed on the cluster.
    ret <- NULL
    retq <- NULL
    tries <- 4
    try <- 0
    while (is.null(ret) && try < tries) {
      retq <- paste(get.zmq.address(), ":",
            as.character(sample(100000, 1)+100000), sep="")
      ret <- zmqChannel(retq, bind.socket, "ZMQ_REP")
      if (is.null(ret)) {
        try <- try + 1
        Sys.sleep(0.1)
      } 
    }
    if (is.null(ret)) {
      stop("push was unsuccessful")
    }
    packet <- list(type="push", expr=expr, resourceName=resultHandle, retq=retq)
    cluster.write(w, packet)
    #ret <- zmqChannel(retq, bind.socket, "ZMQ_PULL")
  }
  ret
}

#' @export
serviceAll <- function(redisAggQ=get.raq(), w=get.local.con(), log=stdout(),
  verbosity=1, p2p="zeromq", timeout=0.1) {
  moreJobs <- TRUE
  while(moreJobs) {
    #moreJobs <- service(redisAggQ, w, log, verbosity, p2p, timeout)
    moreJobs <- service(timeout=timeout)
    if (moreJobs)
      cat("Job serviced\n")
  }
  TRUE
}

#' Service a task from a coordinator
#'
#' The service function blocks on a redis queue, performs a specified pull or
#' push task and returns the result.
#'
#' @param redisAggQ The redis aggregate queue
#' @param w The worker responsible for servicing the task
#' @param log A log file to print to (stdout by default)
#' @param verbosity 0, 1, 2 to log nothing, errors, or everything. Defaults to
#' @param timeout how long should we wait on redis to return something?
#' 1.
#' @param tries how many times should we try to connect to the return endpoint?
#' @param pause how long should we pause between tries?
#' @return TRUE If a request was serviced FALSE otherwise
#' @export
service <- function(redisAggQ=get.raq(), w=get.local.con(), log=stdout(), 
  verbosity=1, p2p="zeromq", timeout=10, tries=3, pause=0.2) {
  packet <- nextRAQMessage(redisAggQ, timeout)
  if (length(packet) == 0) {
    return(FALSE)
  }
  msg <- unserialize(packet[[1]])
  #print(msg)
  if (verbosity > 1) {
    cat("Packet received\n", file=log)
    #print(msg)
  }
  if (msg$type == "pull") {
    #print(msg)
    r <- try(pull(w, msg$expr))
    if (inherits(r, "try-error")) {
      warning("Problem with pull: Trying again")
      r <- try(pull(w, msg$expr))
    }
    if (!is.null(msg$retq)) {
      cat("sending return on channel", msg$retq, "\n")
      channel <- NULL
      try <- 0
      while (is.null(channel) && try < tries) {
        #channel <- zmqChannel(msg$retq, connect.socket, "ZMQ_PUSH")
        channel <- zmqChannel(msg$retq, connect.socket, "ZMQ_REQ")
        if (is.null(channel)) {
          warning("couldn't connect to channel")
          print(msg)
          try <- try + 1
          Sys.sleep(pause)
        }
      }
      if (try == tries || is.null(channel)) {
        warning("Failed to service request")
        return(FALSE)
      } else {
        send(channel, r)
      }
    } 
  } else if (msg$type == "push") {
    push(w, msg$expr, msg$resourceName)
    channel <- NULL
    try <- 0
    while (is.null(channel) && try < tries) {
      #channel <- zmqChannel(msg$retq, connect.socket, "ZMQ_PUSH")
      channel <- zmqChannel(msg$retq, connect.socket, "ZMQ_REQ")
      if (is.null(channel)) {
        warning("couldn't connect to channel")
        print(msg)
        try <- try + 1
        Sys.sleep(pause)
      }
    }
    if (try == tries || is.null(channel)) {
      warning("Failed to service request")
      return(FALSE)
    } else {
      send(channel, msg$resourceName)
    }
    #if (verbosity > 1) 
    #  cat("Resource", newResource, "created \n", file=log)
    #cluster.write(msg$retq, msg$retq)
  } else if(verbosity > 0) {
    cat("Unknown message type", file=log)
  }
  return(TRUE)
}

#' Run a worker
#' 
#' The run.redis.worker function sets up the redis aggregate queue,
#' sets up the worker, and services tasks indefinitely.
#' 
#' @param workerq A unique identifier for the worker.
#' @param host The host address of the redis server.
#' @param port The port of the redis server.
#' @param verbosity 0, 1, 2 to log nothing, errors, or everything. Defaults to
#' 1
#' @return TRUE If a request was serviced FALSE otherwise
#' @export
run.dist.worker <- function(workerq, allq, host="localhost", port=6379, 
  log=stdout(), verbosity=1) {
#  require(rredis)
#  redisConnect()
  dist.worker.init(workerq, allq, host, port)
  redisConnect(host, port)
  redisAggQ <- makeRAQ(host, port)
  #if (verbosity > 1)
  #  cat("Worker connecting to allq", allq, "and worker queue", workerq, "\n")
  addPop(redisAggQ, workerq)
  addPop(redisAggQ, allq)
  local.worker.init()
  while(1) {
    service(redisAggQ, get.local.con(), verbosity=verbosity)
    Sys.sleep(0.1)
  }
}

#' Start distributed workers
#'
#' The start.dist.worker function starts a specified number of distributed
#' workers on the local machine as separate processes.
#' @param workerq A unique character identifier for each worker.
#' @param allq The character identifier for the queue that services tasks that 
#' are not associated with a resource.
#' @param host The host address of the redis server.
#' @param port The port of the redis server.
#' @param log The stream to write log messages to.
#' @param verbosity 0, 1, 2 to log nothing, errors, or everything. Defaults to
#' 1
#' @param rbin The R binary run the workers with.
#' @export
start.dist.worker <- function(workerq=guid(), allq="all", host="localhost", 
  port=6379, log=stdout(), verbosity=1, 
  rbin=paste(R.home(component='bin'),"R",sep="/")) {
  m <- match.call()
  l <- m$log
  if (is.null(l)) l <- "stdout()"
  for (wq in workerq) {
    if (verbosity > 1)
      cat('New worker created with id "', wq, '"\n', sep="", file=log)
    cmd <- paste("require(cnidaria); run.dist.worker(workerq='", wq,
      "', allq='", allq, "',host='", host, "',port=", port, ", log=", l, 
      ", verbosity=", verbosity, ")", sep="")
    args <- c("--slave -e", paste('"', cmd, '"', sep=""))
    system(paste(c(rbin, args), collapse=" "), intern=FALSE, wait=FALSE)
  }
  invisible(TRUE)
}

