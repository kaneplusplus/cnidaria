
#' local.block.vector class
#'
#' The local.block.vector class allows users to create distributed vectors
#' that are stored in an environment in the calling R session. Please note
#' that this class is mostly for testing purposes and provides an
#' set-up that is easy to debug in.
#'
#' @name local.block.vector-class
#' @rdname local.block.vector-class
#' @exportClass local.block.vector
setClass("local.block.vector", representation(env="environment",
  length="numeric", type="character"), contains="block.vector")

# @export
#setMethod("block.names", signature(x="local.dist.vector"),
#  function(x) x@env$blocks)
#' @export 
block.names.local.block.vector <- function(x) {
  x@env$blocks
}

# S3 method dispatch doesn't work for inherited classes so we'll have
# to write it ourselves.
#' @export
typeof.local.block.vector <- function(x) typeof.block.vector(x) 

#' @export
length.local.block.vector <- function(x) length.block.vector(x) 

# @export
#setMethod("block.names<-",
#  signature(x="local.dist.vector", value="character"),
#  function(x, value) {
#    x@env$blocks <- value
#    x
#  })
`block.names<-.local.block.vector` <- function(x, value) {
  if (!is.character(value)) 
    stop("Blocks should have character type.")
  x@env$blocks <- value
  x
}

#' @export
ibv.local.block.vector <- function(x, n=1000) {
  x <- x
  it <- isplitIndices(length(x), n)
  nextEl <- function() {
    newInds <- nextElem(it)
    foreach(itt=iconvertIndex(x, newInds, x@length), .combine=c) %do% {
      indices <- paste(itt[[2]], collapse=",")
      pull(con(x, itt[[1]]),
        parse(text=paste(itt[[1]], "[c(", indices, ")]")))
    }
  }
  object <- list(nextElem=nextEl)
  class(object) <- c("abstractiter", "iter")
  object
}

#' Create a new local.block.vector object
#'
#' The local.dist.vector function is a constructor to create objects of the
#' same name
#'
#' @param blocks The blocks that will be managed by the new object
#' @param gc If TRUE then the blocks are removed when the object is destroyed
#' @export
local.block.vector <- function(blocks=vector(mode="character"), 
  length=0, type="double", gc=TRUE) {
  if (!is.character(blocks))
    stop("The blocks parameter should have character type")
  if (!is.logical(gc)) 
    stop("The gc parameter should have logical type")

  ret <- new("local.block.vector", env=new.env())
  ret@env$blocks <- blocks
  ret@env$gc <- gc
  ret@env$length <- length
  ret@env$type <- type
# TODO: fix this
#  reg.finalizer(ret@env,
#    function(e) {
#      if (e$gc) {
#        rs <- paste(e$blocks, collapse=",")
#        s <- paste("rm(", rs, ",envir=get.local.con()$env)")
#        eval(parse(text=s))
#      }
#    },
#    onexit=FALSE)
  ret
}

#' @export
con.local.block.vector <- function(x, r="") {
  get.local.con()
}

#' @export
setMethod("[", signature(x="local.block.vector", i="missing"),
  function(x) emerge.vector(x))

#' @export
setMethod("[", signature(x="local.block.vector", i="numeric"),
  function(x, i) get.block.vector.numeric.index(x, i))

#' @export
setMethod("[",
  signature(x="local.block.vector", i="local.block.vector"),
  function(x, i) get.block.vector.block.vector.index(x, i))

