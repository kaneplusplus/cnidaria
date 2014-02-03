# TODO: The following function should be Rcpp'ified.
convertIndex <- function(ddf, ind, rowLens ) { #sizechar="nrow") {
#  rowLens <- foreach(name=block.names(ddf), .combine=c) %do% {
#    pull(con(ddf, name), parse(text=paste(sizechar, "(", name, ")")))
#  }
  cRowLens <- c(0, cumsum(rowLens)) + 1
  ret <- foreach(i=ind, .combine=rbind) %do% {
    li <- tail(which(i >= cRowLens), 1)
    remainder <- i-cRowLens[li]+1
    c(li, remainder)
  }
  if (class(ret) != "matrix")
    ret <- matrix(ret, ncol=2, nrow=1)
  ret
}

iconvertIndex <- function(ddf, ind, rowLens) { #sizechar="nrow") {
  ici <- convertIndex(ddf=ddf, ind=ind, rowLens=rowLens)
  rownames(ici) <- NULL
  breaks <- c(which(diff(ici[,1]) != 0), nrow(ici))
  from <- 1
  nextEl <- function() {
    if (from > nrow(ici)) 
      stop("StopIteration")
    to <- breaks[breaks >= from][1]
    ret <- list(block.names(ddf)[ici[from,1]], as.vector(ici[from:to,2]))
    from <<- to+1
    ret
  }
  object <- list(nextElem=nextEl)
  class(object) <- c("abstractiter", "iter")
  object
}

# Get the absolute start and end indices for each block.
startEndInds <- function(dds, lens) { #sizechar="nrow") {
  end <- cumsum(lens)
#    foreach(name=block.names(dds), .combine=c) %do% {
#      pull(con(dds, name), parse(text=paste(sizechar, "(", name, ")")))
#    })
  start <- c(1, end[-length(end)]+1)
  cbind(start, end)
}

#' @title Create a global unique identifier (guid) for a new block.
#' 
#' @param len the length of the guid
guid <- function(len=7) {
  paste(sample(letters, len, replace=TRUE), collapse="")
}

#' @title Creat a port number for transferring things over zeromq channels.
#' 
#' @param start The starting port
#' @param end The ending port
rand.port <- function(start=50000, end=500000) {
  sample.int( end-start, 1) + start
}

# TODO: This needs to be better.
inet.interface <- function(int="eth0") {
  interface <- system(paste("ifconfig", int), intern=TRUE)
  lineNum <- grep("inet addr", interface)
  s1 <- unlist(strsplit(interface[lineNum], ":"))[2]
  s1 <- gsub(" ", "", s1)
  gsub("[A-Za-z]", "", s1)
}

makeDSFromBlocks <- function(newBlocks) {
  testClass <- recv(pull(newBlocks[1],
    parse(text=paste("class(", newBlocks[1], ")"))))
  if (testClass %in% c("numeric", "integer", "double", "logical", "complex",
    "character", "raw", "factor")) {
    #dimExtent <- foreach(block=newBlocks, .combine=c) %do% {
    #  pull(block, parse(text=paste("length(", block, ")")))
    #}
    dimExtentChannel <- foreach(block=newBlocks) %do% {
      pull(block, parse(text=paste("length(", block, ")")))
    }
    dimExtent <- foreach(dec=dimExtentChannel, .combine=c) %do% {
      recv(dec)
    }
    ret <- dist.vector(newBlocks, dimExtent, gc=TRUE)
  } else if (testClass == "data.frame") {
    ncol <- recv(pull(newBlocks[1],
      parse(text=paste("ncol(", newBlocks[1], ")"))))
    nrowChannel <- foreach(name=newBlocks) %do% {
      pull(name, parse(text=paste("nrow(", name, ")")))
    }
    nrow <- foreach(nrc=nrowChannel, .combine=c) %do% {
      recv(nrc)
    }
    ret <- dist.data.frame(newBlocks, nrow=nrow, ncol=ncol, gc=TRUE)
  } else {
    stop("Unsupported distibuted data structure type")
  }
  ret
}

makeIndexCommandString <- function(j) {
  if (missing(j)) {
    js <- ""
  } else if (is.character(j)) {
    js <- paste("c('", paste(j[], collapse="','"), "')", sep="")
  } else if (is.numeric(j)) {
    js <- paste("c(", paste(j[], collapse=","), ")", sep="")
  } else {
    js <- ""
  }
  js
}

# For now we are going to assume that we are only substituting (bquoting)
# for a single variable. This can change later with a ... argument.
# Note that expression arguments will need. to use the convention 
# .(dds) for the substitution

# TODO: This should probably be written as an iterator later, in case
# there are a lot of blocks.

#' @export
blockSubstitute <- function(expr, rs, subVar) {
  if (is.expression(expr)) {
    expr <- as.character(expr)
  }
  foreach (r=rs, .combine=c) %do% {
    subString <- as.character(enquote(eval(parse(text=paste("substitute(", 
      expr, ", list(", subVar, "='", r, "'))", sep=""))))[2])
    # Now get rid of the quotes around the block. 
    gsub(paste('"', r, '"', sep=""), r, subString)
  }
}

cnApply <- function(op, expr, rs, subVar, .combine=c) {
  retChannels <- foreach (r=rs,
    it=blockSubstitute(expr, rs, subVar)) %do% {
      op(r, it)
  }
  foreach(rc=retChannels, .combine=.combine) %do% {
    recv(rc)
  }
}

#' @export
pullApply <- function(expr, rs, subVar, .combine=c) {
  cnApply(pull, expr, rs, subVar, .combine=.combine)
}

#' @export
pushApply <- function(expr, rs, subVar, .combine=c) {
  # Push the tasks out to the cluster.
  # TODO: Should we verify that the corresponding computations happened?
  cnApply(push, expr, rs, subVar, .combine=.combine)
}

