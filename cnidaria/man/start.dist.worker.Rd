\name{start.dist.worker}
\alias{start.dist.worker}
\title{Start distributed workers}
\usage{
\method{start}{dist.worker}(workerq = guid(), allq = "all",
  host = "localhost", port = 6379, log = stdout(), verbosity = 1,
  rbin = paste(R.home(component = "bin"), "R", sep = "/"))
}
\arguments{
  \item{workerq}{A unique character identifier for each
  worker.}

  \item{allq}{The character identifier for the queue that
  services tasks that are not associated with a resource.}

  \item{host}{The host address of the redis server.}

  \item{port}{The port of the redis server.}

  \item{log}{The stream to write log messages to.}

  \item{verbosity}{0, 1, 2 to log nothing, errors, or
  everything. Defaults to 1}

  \item{rbin}{The R binary run the workers with.}
}
\description{
The start.dist.worker function starts a specified number of
distributed workers on the local machine as separate
processes.
}

