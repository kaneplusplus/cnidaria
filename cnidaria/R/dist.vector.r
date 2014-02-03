#' The dist.vector class
#' 
#' The dist.vector class inherits from the block.vector class and implents
#' distributed vectors in the cnidaria framework.
#' 
#' @name dist.vector-class
#' @rdname dist.vector-class
#' @exportClass dist.vector
setClass("dist.vector", representation(env="environment", 
  length="numeric"), contains="block.vector")

#' TODO: Support the garbage collection option.
#' @export
dist.vector <- function(blocks=vector(mode="character"), length=0, gc=FALSE) {
  if (!is.character(blocks)) 
    stop("The blocks parameter should have character type")
  if (!is.logical(gc)) 
    stop("The gc parameter should have logical type")
  #if (gc) print("gc is true") else print("gc is false")
  ret <- new("dist.vector", env=new.env())
  ret@env$blocks <- blocks
  ret@env$gc <- gc
  ret@length <- length
#  reg.finalizer(ret@env,
#    function(e) {
#        if (e$gc == TRUE) {
#          r <- e$blocks
#          localWorker <- get.local.con()
#          packet <- list(type="pull", expr=NULL, retq=NULL)
#          for (i in 1:length(r)) {
#            if (r[i] %in% ls(envir=localWorker$env)) {
#              pull(localWorker, paste("rm(", r[i], ")", sep=""))
#            } else {
#              pull(r[i], paste("rm(", r[i], ")"))
#            }
#          }
#          e$blocks <- NULL
#        } else {
#          #print("no garbage collection")
#        }
#        TRUE
        #invisible()
#    }, onexit=TRUE)
  ret
  # TODO: get broadcasting working and then broadcast garbage collection.
}

#' @export
block.names.dist.vector <- function(x) x@env$blocks

#' @export
`block.names<-.dist.vector` <- function(x, value) {
  if (!is.character(value))
    stop("Blocks should have character type")
  x@env$blocks <- value
  x
}

#' @export
con.dist.vector <- function(x, r=NA) if (!is.na(r)) r else get.all.q()

#' @export
length.dist.vector <- function(x) length.block.vector(x)

#' @export
typeof.dist.vector <- function(x) typeof.block.vector(x) 

#' @export
class.dist.vector <- function(x) class.block.vector(x)

#' @export
`[.dist.vector` <- function(x, i, ...) {
  ret <- c()
  if (missing(i)) {
    bch <- pull(block.names(x)[1],
      parse(text=paste("class(", block.names(x)[1], ")")))
    blockClass <- recv(bch)
    if (blockClass == "factor") {
      # We are going to assume that the factor levels are normalized across
      # blocks.
      lh <- pull(block.names(x)[1],
        parse(text=paste("levels(", block.names(x)[1], ")")))
      levels <- recv(lh)
      retChannels <- foreach(name=block.names(x)) %do% {
        pull(name, parse(text=paste("as.character(", name, ")")))
      }
      
      ret <- foreach(r=retChannels, .combine=c) %do% {
        recv(r)
      }
      ret <- factor(ret, levels=levels)
    } else {
      retChannels <- foreach(name=block.names(x)) %do% {
        # Pull the blocks as strings.
        pull(name, name)
      }
      ret <- foreach(r=retChannels, .combine=c) %do% {
        recv(r)
      }
    }
  } else if (is.numeric(i)) {
    # This could be made more efficient with a single pass.
    newBlocks <- foreach(itt=iconvertIndex(x, i, x@length), .combine=c) %do% {
      indices <- paste(itt[[2]], collapse=",")
      recv(push(itt[[1]],
        parse(text=paste(itt[[1]], "[c(", indices, ")]"))))
    }
    #newBlockChannels <- foreach(itt=iconvertIndex(x, i, x@length)) %do% {
    #  indices <- paste(itt[[2]], collapse=",")
    #  #push(con(x, itt[[1]]),
    #  push(itt[[1]],
    #    parse(text=paste(itt[[1]], "[c(", indices, ")]")))
    #}
    #newBlocks <- foreach(nbc=newBlockChannels, .combine=c) %do% {
    #  recv(nbc)
    #}
    lengths <- foreach(itt=iconvertIndex(x, i, x@length), .combine=c) %do% {
      length(itt[[2]])
    }
    ret <- dist.vector(gc=TRUE)
    ret@length <- lengths
    block.names(ret) <- newBlocks
  } else if (class(i) == "dist.vector") {
    if (typeof(i) == "character") {
      stop(paste("You cannot index vectors with character vectors"))
    }
    # Emerge the distributed vector on the
    # worker side. Since it is making use of previously allocated
    # blocks, make sure it doesn't try to garbage collect them.
    if (typeof(i) == "numeric" || typeof(i) == "integer" || 
      typeof(i) == "double") {
      # Emerge the calling dist.vector to the numeric index 
      # local vector. Note that we are handling this function
      # "inside out" which will minimize sharding and data movement.
      
      newBlocks <- foreach(name=block.names(i), .combine=c) %do% {
        commandString <- paste("dist.vector(c('",
          paste(block.names(x), collapse="','", sep=''), "'),length=c(",
          paste(x@length, collapse=",", sep=''),
          "),gc=FALSE)[", "recv(pull('", name, "','", name, "'))][]", sep='')
        print(commandString)
        recv(push(name, parse(text=commandString)))
      }
      #newBlockChannels <- foreach(name=block.names(i)) %do% {
      #  commandString <- paste("dist.vector(c('",
      #    paste(block.names(x), collapse="','", sep=''), "'),length=c(",
      #    paste(x@length, collapse=",", sep=''),
      #    "),gc=FALSE)[", "recv(pull('", name, "','", name, "'))][]", sep='')
      #  print(commandString)
      #  push(name, parse(text=commandString))
      #}
      #newBlocks <- foreach(nbc=newBlockChannels, .combine=c) %do% {
      #  recv(nbc)
      #}
      
      #ret <- eval(parse(text=paste(bv2bvClass(x), "(gc=TRUE)")))
      ret <- dist.vector(gc=TRUE)
      block.names(ret) <- newBlocks
      ret@length <- i@length
      return(ret)
    } else if (typeof(i) == "logical") {
      # Emerge the logical indices on the worker side.
      # We'll need to iterate over the blocks and the absolute
      # indices of the distributed data structures.
  #    iEmerge <- paste(bv2bvClass(i), "(c('",
      iEmerge <- paste("dist.vector(c('",
        paste(block.names(i), collapse="','", sep=""), 
        "'), length=c(",
        paste(i@length, collapse=",", sep=""),"), gc=FALSE)", 
        sep="")
      # OPTIMIZATION: Logical indexing currently requires 2 calls out to the
      # cluster. One to do the indexing and one to get the lengths of the
      # newly created block vector. This could be made into a single call
      # out to the cluster if we could send multiple pushes/pulls with a single
      # call out.
      newBlocks <- foreach( name=block.names(x),
        row=isplitRows(startEndInds(x,x@length),chunkSize=1 ),
        .combine=c) %do% {
        commandString <- paste(name, "[", 
          iEmerge, "[", row[1, 1], ":", row[1, 2], "][]]", sep="")
        print(commandString)
        recv(push(name, parse(text=commandString)))
      }
      #newBlockChannels <- foreach( name=block.names(x),
      #  row=isplitRows(startEndInds(x,x@length),chunkSize=1 )) %do% {
      #  commandString <- paste(name, "[", 
      #    iEmerge, "[", row[1, 1], ":", row[1, 2], "][]]", sep="")
      #  print(commandString)
      #  push(name, parse(text=commandString))
      #}
      #newBlocks <- foreach(nbc=newBlockChannels, .combine=c) %do% {
      #  recv(nbc)
      #}
      #ret <- eval(parse(text=paste(bv2bvClass(x), "(gc=TRUE)")))
      ret <- dist.vector(gc=TRUE)
      block.names(ret) <- newBlocks
      lengthChannels <- foreach( name=block.names(ret)) %do% {
        commandString <- paste("length(", name, ")")
        pull(name, parse(text=commandString))
      }
      ret@length <- foreach(channel=lengthChannels, .combine=c) %do% {
        recv(channel)
      }
#    ret@length <- i@length
    } else {
      stop("Unsupported index vector type")
    }
  } else {
    stop("Unsupported index vector type")
  }
  ret
}

##' @export
#setMethod("[", signature(x="dist.vector", i="missing"),
#  function(x, ...) emerge.vector(x, ...))
#
##' @export
#setMethod("[", signature(x="dist.vector", i="numeric"),
#  function(x, i) get.block.vector.numeric.index(x, i))
#
##' @export
#setMethod("[",
#  signature(x="dist.vector", i="dist.vector"),
#  function(x, i) get.block.vector.block.vector.index(x, i))

