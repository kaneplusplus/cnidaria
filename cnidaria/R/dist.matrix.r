
#' @export
setClass("dist.block.matrix", representation(env="environment"),
  contains="block.matrix")

#' @export
dist.block.matrix <- function(blocks=matrix(mode="character"), 
  gc=TRUE) {
  stop("not implemented")
}

#' @export
block.names.dist.block.matrix <- function(x) {
  stop("not implemented")
}

#' @export
`block.names<-.dist.block.matrix` <- function(x, value) {
  stop("not implemented")
}

#' @export
con.dist.block.matrix <- function(x, r=NA) {
  stop("not implemented")
}

#' @export 
dim.dist.block.matrix <- function(x) dim.block.vector(x)

#' @export
typeof.dist.block.matrix <- function(x) typeof.block.matrix(x)

#' @export
setMethod("[", signature(x="dist.block.matrix", i="missing", j="missing"),
  function(x) emerge.matrix(x))

