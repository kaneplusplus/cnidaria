\name{distribute.data.frame}
\alias{distribute.data.frame}
\title{Create a distributed data frame from a native R data.frame}
\usage{
distribute.data.frame(df, row.chunk.size = 1000,
  constructor = dist.data.frame)
}
\arguments{
  \item{df}{The data frame to distriubte}

  \item{row.chunk.size}{The size of a row block of a
  distribute data frame}

  \item{constructor}{A constructor for the data frame type
  to create}
}
\value{
a new dist.data.frame object
}
\description{
This function allows you to easily create new distributed
vectors from local ones.
}

