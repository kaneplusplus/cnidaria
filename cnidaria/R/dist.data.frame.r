# source("util.r")
#source("dist.data.structure.r")
#source("dist.vector.r")

#' The dist.data.frame class
#' 
#' The dist.data.frame class inherits from the block.data.frame class and
#' implements distributed data frames in the cnidaria framework.
#'
#' @name dist.data.frame-class
#' @rdname dist.data.frame-class
#' @exportClass dist.data.frame
setClass("dist.data.frame", representation(env="environment",
  nrow="numeric", ncol="numeric"), contains="block.data.frame")

#' @export
con.dist.data.frame <- function(x, r=NA) if (!is.na(r)) r else get.all.q()

#' @export
dist.data.frame <- function(blocks=vector(mode="character"), nrow=0, 
  ncol=0, gc=TRUE) {
  if (!is.character(blocks)) 
    stop("The blocks parameters should have character type")
  if (!is.logical(gc)) 
    stop("The gc parameter should have logical type")
  ret <- new("dist.data.frame", env=new.env())
  ret@env$blocks <- blocks
  ret@env$gc <- gc
  ret@nrow=nrow
  ret@ncol=ncol
#  reg.finalizer(ret@env,
#    function(e) {
#      if (e$gc == TRUE) {
#        r <- e$blocks
#        #localWorker <- list(type="pull", expr=NULL, retq=NULL)
#        localWorker <- get.local.con()
#        for (i in 1:length(r)) {  
#          if (r[i] %in% ls(envir=localWorker$env)) {
#            pull(localWorker, paste("rm(", r[i], ")", sep=""))
#          } else {  
#            pull(r[i], paste("rm(", r[i], ")"))
#          }
#        }
#        e$blocks <- NULL
#      } else {
#      }
#      TRUE
#    }, onexit=TRUE)
  ret
}

#' @export
block.names.dist.data.frame <- function(x) x@env$blocks

#' @export
`block.names<-.dist.data.frame` <- function(x, value) {
  if (!is.character(value))
    stop("Blocks should have character type")
  x@env$blocks<- value
  x
}

#setMethod("dimnames", signature(x="dist.data.frame"),
#  function(x) {
#    rn <- foreach(name=block.names(x), .combine=c) %do% {
#      pull(con(x), parse(text=paste("rownames(", name, ")")))
#    cn <- pull(con(x), parse(text=paste("colnames(",block.names(x)[1],")")))
#    list(rn, cn)
#    }
#  })

#setMethod("names", signature(x="dist.data.frame"),
#  function(x) {
#    pull(con(x), parse(text=paste("colnames(", block.names(x), ")")))
#  })

#' @export
setMethod("$", 
  signature(x="dist.data.frame"),
  function(x, name) {
    x[,paste("'", name, "'", sep="")]
  })

#' @export
setMethod("[",
  signature(x="dist.data.frame", i="missing", j="missing"),
  function(x) {
    emerge.data.frame(x)
  })

# Select a set of column.
#' @export
setMethod("[",
  signature(x = "dist.data.frame", i="missing"),
  function(x, j, drop=TRUE) {
    get.block.data.frame.column.select(x, j, drop)
  })

#' @export
setMethod("[",
  signature(x = "dist.data.frame", i="block.vector", j="missing"),
  function(x, i, drop=TRUE) {
    get.block.data.frame.block.row.select(x, i, j=NULL, drop=drop)
  })

#' @export
setMethod("[",
  signature(x = "dist.data.frame", i="numeric"),
  function(x, i, j="", drop=TRUE) {
    get.block.data.frame.vector.row.select(x, i, j, drop=drop)
  })

#' @export
setMethod("[",
  signature(x = "dist.data.frame", i="block.vector"),
  function(x, i, j, drop=TRUE) {
    get.block.data.frame.block.row.select(x, i, j, drop=drop)
  })

