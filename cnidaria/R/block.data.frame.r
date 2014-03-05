require(foreach)
require(itertools)

#' @export
setGeneric("bdf2dvClass", function(x) standardGeneric("bdf2dvClass"))

#' @export
ibdf <- function(x, ...) {
  UseMethod("ibdf", x)
}

#' The block.data.frame class
#'
#' The block.data.frame class inherits from the block.data.structure class
#' and is the abstract class that all block data frames depend on.
#' 
#' @name block.data.frame-class
#' @rdname block.data.frame-class
#' @exportClass block.data.frame
setClass("block.data.frame")#, contains="block.data.structure")

# TODO: Add numeric indexing and implement ibdf. The way it's done here is
# too slow.

#' @export
ibdf.block.data.frame <- function(x, ...) {
  x <- x
  it <- isplitIndices(nrow(x), ...)
  nextEl <- function() {
    x[nextElem(it),][]
  } 
  object <- list(nextElem=nextEl)
  class(object) <- c("abstractiter", "iter")
  object
}

##' @export
#ncol.block.data.frame <- function(x) {
#  sum(x@ncol)
#}
#
##' @export
#nrow.block.data.frame <- function(x) {
#  sum(x@nrow)
#}

#' @export
dim.block.data.frame <- function(x) {
  c(sum(x@nrow), sum(x@ncol))
}

#' @export
dimnames.block.data.frame <- function(x) {
  stop("This function is currently not supported")
}

#' @export 
typeof.block.data.frame <- function(x) {
  recv(pull(block.names(x)[1], paste("typeof(", block.names(x)[1], ")")))
}

emerge.data.frame <- function(x) {
  dfChannels <- foreach(name=block.names(x)) %do% {
    pull(con(x, name), name)
  }
  foreach(dfc=dfChannels, .combine=rbind) %do% {
    recv(dfc)
  }
}

get.block.data.frame.vector.row.select <- function(x, i, j=NULL, drop=TRUE) {
  if (is.numeric(i)) {
    js <- makeIndexCommandString(j)
    newBlocks <- foreach(itt=iconvertIndex(x, i, x@nrow), .combine=c) %do% {
      indices <- paste(itt[[2]], collapse=",")
      commandString <- paste(itt[[1]], "[c(", indices, "), ", js, ", drop=",
          drop, "]", sep="")
      recv(push(itt[[1]], parse(text=commandString)))
    }
  } else {
    stop("Unsupported vector index type")
  }
  makeDSFromBlocks(newBlocks)
}

get.block.data.frame.block.row.select <- function(x, i, j=NULL, drop=TRUE) {
  if (typeof(i) == "logical") {
    iEmerge <- paste(bv2bvClass(i), "(c('",
      paste(block.names(i), collapse="','", sep=""),
      "'), length=c(",
      paste(i@length, collapse=",", sep=""), "), gc=FALSE)",
      sep="")
    js <- makeIndexCommandString(j)
    newBlocks <- foreach(name=block.names(x),
      row=isplitRows(startEndInds(x, x@nrow), chunkSize=1), 
      .combine=c) %do% {
      commandString <- paste(name, "[recv(pull('", name, "', parse(text=\"",
        iEmerge, "\")))[", row[1, 1], ":", row[1, 2], "][],", 
        js, ",drop=", drop, "]", sep="")
      print(commandString)
      recv(push(name, parse(text=commandString)))
    }
  } else {
    stop("Unsupported index vector type")
  }
  makeDSFromBlocks(newBlocks)
}

get.block.data.frame.column.select <- function(x, j, drop=TRUE) {
  ret <- c()
  if (length(j) > 0) {
    newBlocks <- foreach(name=block.names(x), .combine=c) %do% {
      recv(push(con(x, name), 
        parse(text=paste(name, "[,", j[], ", drop=", drop, "]", sep=""))))
    }
    ret <- makeDSFromBlocks(newBlocks)
  }
  ret
}

# Create a local.dist.data.frame from a list of data.frames.
#' @export
list2lddf <- function(l) {
  lddf <- dist.data.frame()
  block.names(lddf) <- foreach(i=1:length(l), .combine=c) %do% {
    recv(push(con(lddf), l[[i]]))
  }
  if (length(l) >= 1) {
    lddf@ncol <- unlist(ncol(l[[1]]))
    lddf@nrow <- unlist(lapply(l, nrow))
  }
  lddf
}

#' Create a distributed data frame from a native R data.frame
#' 
#' This function allows you to easily create new distributed vectors
#' from local ones.
#' 
#' @param df The data frame to distriubte
#' @param row.chunk.size The size of a row block of a distribute data frame
#' @param constructor A constructor for the data frame type to create
#' @return a new dist.data.frame object
#' @export
distribute.data.frame <- function(df, row.chunk.size=1000, 
  constructor=dist.data.frame) {
  list2lddf(
    foreach(it=isplitRows(df, chunkSize=row.chunk.size)) %do% it)
}

