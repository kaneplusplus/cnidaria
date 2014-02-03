\name{push}
\alias{push}
\title{Push an expression to a worker and create a new block.}
\usage{
  push(w, expr, resultHandle)
}
\arguments{
  \item{w}{The worker to push the expression to.}

  \item{expr}{The expression to execute or the variable to
  put to the worker side. Note that with pull you can pass
  an character expression, with push you cannot do this.}
}
\description{
  Push an expression to a worker and create a new block.
}

