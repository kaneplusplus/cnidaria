\name{service}
\alias{service}
\title{Service a task from a coordinator}
\usage{
service(redisAggQ = get.raq(), w = get.local.con(), log = stdout(),
  verbosity = 1, p2p = "zeromq", timeout = 10)
}
\arguments{
  \item{redisAggQ}{The redis aggregate queue}

  \item{w}{The worker responsible for servicing the task}

  \item{log}{A log file to print to (stdout by default)}

  \item{verbosity}{0, 1, 2 to log nothing, errors, or
  everything. Defaults to}

  \item{timeout}{how long should we wait on redis to return
  something? 1.}
}
\value{
TRUE If a request was serviced FALSE otherwise
}
\description{
The service function blocks on a redis queue, performs a
specified pull or push task and returns the result.
}

