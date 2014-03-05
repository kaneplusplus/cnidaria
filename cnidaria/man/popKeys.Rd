\name{popKeys}
\alias{popKeys}
\title{Get the keys for the lists that the redis aggregate queue is listening on}
\usage{
popKeys(raq)
}
\arguments{
  \item{raq}{The redis aggregate queue}
}
\value{
The keys corresponding to the redis lists that the raq is
listening on
}
\description{
Get the keys for the lists that the redis aggregate queue
is listening on
}

