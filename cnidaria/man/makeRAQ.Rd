\name{makeRAQ}
\alias{makeRAQ}
\title{Create a redis aggregate queue.}
\usage{
  makeRAQ(ip, port)
}
\arguments{
  \item{ip}{The host address of the redis server.}

  \item{port}{The port the redis server is listening on.}
}
\value{
  An external pointer to an instance of type
  RedisAggregateQueue.
}
\description{
  Create a redis aggregate queue.
}

