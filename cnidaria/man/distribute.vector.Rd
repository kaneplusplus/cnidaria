\name{distribute.vector}
\alias{distribute.vector}
\title{Create a distributed vector from a native R vector}
\usage{
distribute.vector(vec, block.size = 1000)
}
\arguments{
  \item{vec}{The vector to distribute}

  \item{block.size}{The size of a block of a distributed
  vector}

  \item{constructor}{A constructor for the vector type to
  create}
}
\value{
a new dist.vector object
}
\description{
This function allows you to easily create new distributed
vectors from a local one.
}

