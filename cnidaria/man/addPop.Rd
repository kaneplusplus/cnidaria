\name{addPop}
\alias{addPop}
\title{Add a resource queue to listen on.}
\usage{
  addPop(raq, key)
}
\arguments{
  \item{raq}{The pointer to the RedisAggregateQueue}

  \item{key}{The key of the list to listen on}
}
\value{
  TRUE if the operation was successful FALSE otherwise
}
\description{
  Add a resource queue to listen on.
}

