\name{local.block.vector}
\alias{local.block.vector}
\title{Create a new local.block.vector object}
\usage{
local.block.vector(blocks = vector(mode = "character"), length = 0,
  type = "double", gc = TRUE)
}
\arguments{
  \item{blocks}{The blocks that will be managed by the new
  object}

  \item{gc}{If TRUE then the blocks are removed when the
  object is destroyed}
}
\description{
The local.dist.vector function is a constructor to create
objects of the same name
}

