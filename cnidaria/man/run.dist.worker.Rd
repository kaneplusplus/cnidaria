\name{run.dist.worker}
\alias{run.dist.worker}
\title{Run a worker}
\usage{
  run.dist.worker(workerq, allq, host = "localhost",
    port = 6379, log = stdout(), verbosity = 1)
}
\arguments{
  \item{workerq}{A unique identifier for the worker.}

  \item{host}{The host address of the redis server.}

  \item{port}{The port of the redis server.}

  \item{verbosity}{0, 1, 2 to log nothing, errors, or
  everything. Defaults to 1}
}
\value{
  TRUE If a request was serviced FALSE otherwise
}
\description{
  The run.redis.worker function sets up the redis aggregate
  queue, sets up the worker, and services tasks
  indefinitely.
}

