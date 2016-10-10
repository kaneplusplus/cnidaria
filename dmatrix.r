library(foreach)

source("convert-indices.r")

setClass('dmatrix', representation(e='environment'))

dmatrix = function(parts, start_rows, end_rows, start_cols, end_cols) {
  num_rows = end_rows - start_rows + 1
  num_cols = end_cols - start_cols + 1
  part_locs = cbind(start_rows, end_rows, num_rows, start_cols, end_cols, num_cols)
  e = new.env(parent=emptyenv())
  assign("parts", parts, envir=e)
  assign("part_locs", part_locs, envir=e)
  new('dmatrix', e=e)
}

# TODO: Add better checking to see if the parts make a rectangular
# matrix.
# The list has to include the absolute coordinates for the 
# top-left most element of the part, and the matrix part 
dmatrix_from_matrices = function(l, start_rows, start_cols, part_constructor) {
  if (missing(part_constructor))
    part_constructor = options()$default_part_constructor

  parts = lapply(l, function(x) as_part(x, part_constructor))
  end_rows = start_rows + sapply(l, function(x) nrow(x)) - 1
  end_cols = start_cols + sapply(l, function(x) ncol(x)) - 1
  dmatrix(parts, start_rows, end_rows, start_cols, end_cols)
}

setMethod("dim", signature(x="dmatrix"),
  function(x) {
    c(max(x@e$part_locs[,"end_rows"]), max(x@e$part_locs[,"end_cols"]))
  })

setMethod("[", signature(x="dmatrix", i="missing", j="missing"),
  function(x) {
    # Emerge... we should do some checking to make sure there aren't 
    # too many elements.
    ret = matrix(NA, nrow=nrow(x), ncol=ncol(x))
    for (i in 1:nrow(x@e$part_locs)) {
      ret[x@e$part_locs[i, "start_rows"]:x@e$part_locs[i, "end_rows"],
          x@e$part_locs[i, "start_cols"]:x@e$part_locs[i, "end_cols"]] = 
        get_values(x@e$parts[[i]])
    }
    ret
  })

setMethod("[", signature(x="dmatrix", i="numeric", j="missing", drop="ANY"),
  function(x, i) {
    x[i, 1:ncol(x), drop]
  })

setMethod("[", signature(x="dmatrix", i="missing", j="numeric", drop="ANY"),
  function(x, j) {
    x[1:nrow(x), j, drop]
  })

setMethod("[", signature(x="dmatrix", i="numeric", j="numeric", drop="ANY"),
  function(x, i, j) {
    ret = matrix( NA, nrow=length(unique(i)), ncol=length(unique(j)) )
    if (!is.null(nrow(x)))
      rownames(ret) = rownames(x)[i]
    if (!is.null(colnames(x)))
      colnames(ret) = colnames(x)[j]

    ret_vals = convert_coord2d(x@e$part_locs, i, j)
    if (any(ret_vals[,"i"] > nrow(ret)) || any(ret_vals[,"j"] > ncol(ret)))
      stop("subscript out of bounds")

    # We'll use 1-dimensional indexing based on the ret_vals.
    # Get the number of rows for each part in ret_vals.
    ret_vals = cbind(ret_vals, 
      ret_vals[,"i"] + length(i)*(ret_vals[,"j"]-1),
      ret_vals[,"rel_i"] + 
        x@e$part_locs[ret_vals[,"part"],"num_rows"]*(ret_vals[,"rel_j"]-1))
    colnames(ret_vals)[c(6,7)] = c("offset", "rel_offset")
    for(part_num in unique(ret_vals[,"part"])) {
      part_rows = which(ret_vals[,"part"] == part_num)
      ret[ret_vals[part_rows,"offset"]] = 
        get_values(x@e$parts[[part_num]], ret_vals[part_rows, "rel_offset"])
    }
    ret
  })

options(ddr_size_prop=2)

setMethod("Arith", signature(e1='dmatrix', e2='numeric'),
  function(e1, e2) {
    op = .Generic[[1]]
    if (length(e1) == 1) {
#      parts = foreach(part = e1@e$parts) %dopar% {
#        as_part(do.call(op, list(get_values(part), e2)))
#      }
    } else {
      if ((inherits(e2, "matrix") || inherits(e2, "Matrix")) && all(dim(e1)==dim(e2)))
        e1[] + e2
      else
        stop("arithmetic operation not defined for these shapes")
    }
  })
 
setMethod("%*%", signature(x="dmatrix",  y="dmatrix"),
  function(x, y) {
    if (ncol(x) != nrow(y))
      stop("non-conformable arguments")

    # Get the absolute coordinates that will define resulting parts.
    # Note that this is only one (possibly dumb) way of defining the resulting
    # parts. 

    i_starts = seq(1, nrow(x), by=floor(nrow(x)/ options()$ddr_size_prop))
    i_ends = c(i_starts[-1]-1, nrow(x))
    # If the ddr_size_prop divides nrow(x)+1 then we get an extra element.
    # Remove it.
    if (length(i_starts) > length(i_ends)) i_starts=i_starts[-length(i_starts)]

    j_starts = seq(1, ncol(y), by=floor(ncol(y)/ options()$ddr_size_prop))
    j_ends = c(j_starts[-1]-1, ncol(y))
    if (length(j_starts) > length(j_ends)) j_starts=j_starts[-length(j_starts)]

    k_starts = seq(1, ncol(x), by=floor(ncol(x)/ options()$ddr_size_prop))
    k_ends = c(k_starts[-1]-1, ncol(x))
    if (length(k_starts) > length(k_ends)) k_starts=k_starts[-length(k_starts)]

    # Should we localize row parts to the same process instead?
    # Or should we do the entire outer product of matrix part multiplications
    # and then add up terms?
    matrix_parts = foreach (i=1:length(i_starts), .combine=c) %:% 
      foreach (j=1:length(j_starts)) %dopar% {
        # start_row, start_col, num_rows, num_cols, end_row, end_col
        list(part_loc=c(i_starts[i], j_starts[j], i_ends[i], j_ends[j]),
          part=as_part(foreach (k=1:length(k_starts), .combine=`+`) %do% {
            x[i_starts[i]:i_ends[i], k_starts[k]:k_ends[k]] %*%
              y[k_starts[k]:k_ends[k], j_starts[j]:j_ends[j]]
          }))
    }
    part_locs=foreach(i=1:length(matrix_parts), .combine=rbind) %do% {
      matrix_parts[[i]]$part_loc
    }
    colnames(part_locs) = c("start_rows", "start_cols", "end_rows", "end_cols")
    dmatrix(parts=lapply(matrix_parts, function(x) x[[2]]),
            part_locs[,"start_rows"], part_locs[,"end_rows"], 
            part_locs[,"start_cols"], part_locs[,"end_cols"])
  })

