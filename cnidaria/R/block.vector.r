require(foreach)
require(itertools)

#' Create a block vector iterator
#'
#' Retrieve an block vector using the iterator interface.
#'
#' @export
#' @param x The block vector
#' @param n The iterator chunksize
#' @return An iterator
ibv <- function(x, ...) {
  UseMethod("ibv", x)
}

#setGeneric("dv2dvClass", function(x) standardGeneric("dv2dvClass"))

#' @export
bv2bvClass <- function(x) {
  UseMethod("bv2bvClass", x)
}

#' @export
bv2bvClass <- function(x) {
  class(x)[1]
}

#' The block.vector class
#'
#' The block.vector class inherits from the block.data.structure class and
#' is the abstract class that all block vectors depend on.
#
#' @name block.vector-class
#' @rdname block.vector-class
#' @exportClass block.vector
setClass("block.vector", contains="block.data.structure")

#' @export
length.block.vector <- function(x) {
  sum(x@length)
}

#' @export
typeof.block.vector <- function(x) {
  recv(pull(block.names(x)[1], paste("typeof(", block.names(x)[1], ")")))
}

#' @export
ibv.block.vector <- function(x, ...) {
  x <- x
  it <- isplitIndices(length(x), ...)
  nextEl <- function() {
    x[nextElem(it)][]
  }
  object <- list(nextElem=nextEl)
  class(object) <- c("abstractiter", "iter")
  object
}

#emerge.vector <- function(x, ...) {
#  ret <- c()
#  blockClass <- pull(block.names(x)[1], 
#    parse(text=paste("class(", block.names(x)[1], ")")))
#  if (blockClass == "factor") {
#    # We are going to assume that the factor levels are normalized across
#    # blocks.
#    levels <- pull(con(x, block.names(x)[1]), 
#      parse(text=paste("levels(", block.names(x)[1], ")")))
#    retChannels <- foreach(name=block.names(x), .combine=c) %do% {
#      # Pull the blocks as strings.
#      pull(con(x, name), parse(text=paste("as.character(", name, ")")))
#    }
#    ret <- foreach(r=retChannels, .combine=c) %do% recv(r)
#    ret <- factor(ret, levels=levels)
#  } else {
#    retChannels <- foreach(name=block.names(x), .combine=c) %do% {
#      # Pull the blocks as strings.
#      pull(con(x, name), name)
#    }
#    ret <- foreach(r=retChannels, .combine=c) %do% recv(r)
#  }
#  ret
#}

logical.block.block.comparison<- function(e1, e2, op) {
  # We are going to emerge e2 blocks in the workers where the e1 blocks
  # reside and perform the logical comparisons.
  if (length(e1) != length(e2)) {
    stop("Distributed vectors must have the same size for logical comparisons.")
  }

#  e2Emerge <- paste(bv2bvClass(e2), "(c('",
  e2Emerge <- paste("dist.vector(c('",
    paste(block.names(e2), collapse="','", sep=""), "'), c(",
    paste(e2@length, collapse=",", sep=""), "), gc=FALSE)",
    sep="")

  newBlocks <- foreach(name=block.names(e1),
    row=isplitRows(startEndInds(e1, e1@length), chunkSize=1), 
    .combine=c) %do% {
    commandString <- paste(name, " ", op, " ", 
      e2Emerge, "[", row[1, 1], ":", row[1, 2], "][]", sep="")
    recv(push(name, parse(text=commandString)))
  }
  #newBlockChannels <- foreach(name=block.names(e1),
  #  row=isplitRows(startEndInds(e1, e1@length), chunkSize=1)) %do% {
  #  commandString <- paste(name, " ", op, " ", 
  #    e2Emerge, "[", row[1, 1], ":", row[1, 2], "][]", sep="")
  #  push(name, parse(text=commandString))
  #}
  #newBlocks <- foreach(nbc=newBlockChannels, .combine=c) %do% {
  #  recv(newBlocks)
  #}
  #ret <- eval(parse(text=paste(bv2bvClass(e1), "(gc=TRUE)")))
  ret <- dist.vector(gc=TRUE)
  block.names(ret) <- newBlocks
  retLengthChannels <- foreach(name=block.names(ret)) %do% {
    pull(name, parse(text=paste("length(", name, ")")))
  }
  ret@length <- foreach(rlc=retLengthChannels, .combine=c) %do% {
    recv(rlc)
  }
  ret
}

logical.block.vector.comparison <- function(e1, e2, op) {
  # For this function e1 is always a block.vector and e2 is not.
  ret <- NULL
  if (length(e2) == 1) {
    newBlocks <- foreach(name=block.names(e1), .combine=c) %do% {
      recv(push(name, parse(text=paste(name, op, e2))))
    }
    #newBlockChannels <- foreach(name=block.names(e1)) %do% {
    #  push(name, parse(text=paste(name, op, e2)))
    #}
    #newBlocks <- foreach(nbc=newBlockChannels, .combine=c) %do% {
    #  recv(nbc)
    #}
    #ret <- eval(parse(text=paste(bv2bvClass(e1), "(gc=TRUE)", sep="")))
    ret <- dist.vector(gc=TRUE)
    block.names(ret) <- newBlocks
    ret@length <- e1@length
  } else {
    if (length(e1) != length(e2)) 
      stop("Length mismatch in binary logical comparison operator")
    else
      stop("Upcasting of vectors not supported yet.")
  }
  ret
}

#' @export
setMethod("==", signature(e1="block.vector", e2="block.vector"),
  function(e1, e2) {
    logical.block.block.comparison(e1, e2, "==")
  })

#' @export
setMethod("==", signature(e1="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e1, e2, "==")
  })

#' @export
setMethod("==", signature(e2="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e2, e1, "==")
  })

#' @export
setMethod(">", signature(e1="block.vector", e2="block.vector"),
  function(e1, e2) {
    logical.block.block.comparison(e1, e2, ">")
  })

#' @export
setMethod(">", signature(e1="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e1, e2, ">")
  })

#' @export
setMethod(">", signature(e2="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e2, e1, ">")
  })

#' @export
setMethod(">=", signature(e1="block.vector", e2="block.vector"),
  function(e1, e2) {
    logical.block.block.comparison(e1, e2, ">=")
  })

#' @export
setMethod(">=", signature(e1="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e1, e2, ">=")
  })

#' @export
setMethod(">=", signature(e2="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e2, e1, ">=")
  })

#' @export
setMethod("<", signature(e1="block.vector", e2="block.vector"),
  function(e1, e2) {
    logical.block.block.comparison(e1, e2, "<")
  })

#' @export
setMethod("<", signature(e1="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e1, e2, "<")
  })

#' @export
setMethod("<", signature(e2="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e2, e1, "<")
  })

#' @export
setMethod("<=", signature(e1="block.vector", e2="block.vector"),
  function(e1, e2) {
    logical.block.block.comparison(e1, e2, "<=")
  })

#' @export
setMethod("<=", signature(e1="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e1, e2, "<=")
  })

#' @export
setMethod("<=", signature(e2="block.vector"),
  function(e1, e2) {
    logical.block.vector.comparison(e2, e1, "<=")
  })

list2bv <- function(l) {
  ldv <- dist.vector(gc=TRUE)
  block.names(ldv) <- foreach(i=1:length(l), .combine=c) %do% {
    recv(push(con(ldv), l[[i]], guid()))
  }
  #ldvChannels <- foreach(i=1:length(l)) %do% {
  #  push(con(ldv), l[[i]], guid())
  #}
  #block.names(ldv) <- foreach(ldvc=ldvChannels, .combine=c) %do% {
  #  recv(ldvc)
  #}
  if (length(l) >=1 ) {
    ldv@length <- unlist(lapply(l, length))
  }
  ldv
}

#' Create a distributed vector from a native R vector
#' 
#' This function allows you to easily create new distributed vectors
#' from a local one.
#'
#' @param vec The vector to distribute
#' @param block.size The size of a block of a distributed vector
#' @param constructor A constructor for the vector type to create
#' @return a new dist.vector object
#' @export
distribute.vector <- function(vec, block.size=1000) {
  list2bv(foreach(it=ichunk(vec, block.size)) %do% unlist(it))
}
