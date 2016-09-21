library(foreach)

source("convert_indices.r")

# TODO: Add better checking to see if the chunks make a rectangular
# matrix.
# The list has to include the absolute coordinates for the 
# top-left most element of the chunk, and the matrix chunk.
dmatrix = function(l, chunk_constructor) {
  if (missing(chunk_constructor))
    chunk_constructor = options()$default_chunk_constructor
  if (any(!sapply(l, function(x) is.matrix(x[[2]]))))
    stop("All supplied objects must be matrices")
  
  start_row = sapply(l, function(x) x[[1]][1])
  start_col = sapply(l, function(x) x[[1]][2])
  end_row = start_row + sapply(l, function(x) nrow(x[[2]])) - 1
  end_col = start_col + sapply(l, function(x) ncol(x[[2]])) - 1
  ret = list(chunks=lapply(l, function(x) as_chunk(x[[2]], chunk_constructor)),
             chunk_locs=cbind(start_row, end_row, start_col, end_col))
  class(ret) = c("dmatrix", class(ret))
  ret
}

dim.dmatrix = function(x) {
  c(max(x$chunk_locs[,"end_row"]), max(x$chunk_locs[,"end_col"]))
}

`[.dmatrix` = function(x, i, j, ..., drop=TRUE) {
  
  if (missing(i) && missing(j)) {
    # Emerge... we should do some checking to make sure there aren't 
    # too many elements.
    ret = matrix(NA, nrow=nrow(x), ncol=ncol(x))
    for (i in 1:nrow(x$chunk_locs)) {
      ret[x$chunk_locs[i, "start_row"]:x$chunk_locs[i, "end_row"],
          x$chunk_locs[i, "start_col"]:x$chunk_locs[i, "end_col"]] = 
        get_values(x$chunks[[i]])
    }
    ret
  } else if (missing(i) & !missing(j)) {
  } else if (!missing(i) & missing(j)) {
  } else if (!missing(i) & !missing(j)) {
  } else {
    stop("Parameters not supported yet")
  }
  ret
}

source("disk-chunk.r")

# We'll use doSEQ as a parallel execution engine.
library(foreach)
registerDoSEQ()

init_ddr_disk_chunk()

# Chunks for an irregular matrix.
l = list(list(c(1, 1), matrix(rnorm(25), nrow=5, ncol=5)),
         list(c(1, 6), matrix(rnorm(36), nrow=6, ncol=6)),
         list(c(6, 1), matrix(rnorm(5), ncol=5, nrow=1)))

dm = dmatrix(l)
