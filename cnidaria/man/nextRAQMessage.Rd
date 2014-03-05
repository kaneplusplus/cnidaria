\name{nextRAQMessage}
\alias{nextRAQMessage}
\title{Block for the next message on the redis aggregate queue}
\usage{
nextRAQMessage(raq, timeout)
}
\arguments{
  \item{raq}{The redis aggregate queue}

  \item{timeout}{The number of seconds to wait for the next
  message}
}
\value{
NULL if the timeout is reached otherwise the next message
}
\description{
Block for the next message on the redis aggregate queue
}

