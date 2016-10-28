library(foreach)
library(Matrix)

source("convert-indices.r")
source("params.r")
source("dvector.r")

setClass('dmatrix', representation(e='environment'))

dmatrix = function(parts, start_rows, end_rows, start_cols, end_cols) {
  num_rows = end_rows - start_rows + 1
  num_cols = end_cols - start_cols + 1
  part_locs = cbind(start_rows, end_rows, num_rows, start_cols, end_cols, 
                    num_cols)
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
    ret = Matrix(0, nrow=nrow(x), ncol=ncol(x))
    for (i in 1:nrow(x@e$part_locs)) {
      ret[x@e$part_locs[i, "start_rows"]:x@e$part_locs[i, "end_rows"],
          x@e$part_locs[i, "start_cols"]:x@e$part_locs[i, "end_cols"]] = 
        get_values(x@e$parts[[i]])
    }
    ret
  })

setMethod('[', signature(x='dmatrix', i='numeric', j='missing', drop='logical'),
  function(x, i, drop) {
    if (ncol(x) <= options()$col_part_size) {
      # It's small enough to return as a Matrix.
      x[i, 1:ncol(x)]
    } else {
      j_starts = seq(1, ncol(x), by=options()$col_part_size)
      j_ends = c(j_starts[-1]-1, ncol(x))
      if (length(j_starts) > length(j_ends)) 
        j_starts = j_starts[-length(j_starts)]

      start_rows = rep(1, length(j_starts))
      end_rows = rep(length(i), length(j_starts))
      if (length(i) > 1 || drop==FALSE) {
        dmatrix(
          parts=foreach(j=1:length(j_starts)) %dopar% {
            as_part(x[i, j_starts[j]:j_ends[j]])
          },
          start_rows,
          end_rows,
          j_starts,
          j_ends)
      } else {
        dvector(
          parts=foreach(j=1:length(j_starts)) %dopar% {
            as_part(as.vector(x[i, j_starts[j]:j_ends[j]]))
          },
          lengths=i_ends - i_starts + 1)
      }
    }
  })

setMethod('[', signature(x='dmatrix', i='numeric', j='missing', drop='missing'),
  function(x, i) x[i, drop=TRUE])


setMethod("[", signature(x="dmatrix", i="missing", j="numeric", drop="missing"),
  function(x, j) x[,j,drop=TRUE])

setMethod("[", signature(x="dmatrix", i="missing", j="numeric", drop="logical"),
  function(x, j, drop) {
    if (nrow(x) <= options()$row_part_size) {
      x[1:nrow(x), j]
    } else {
      i_starts = seq(1, nrow(x), by=options()$row_part_size)
      i_ends = c(i_starts[-1]-1, nrow(x))
      if (length(i_starts) > length(i_ends))
        i_starts = i_starts[-length(i_starts)]

      start_cols = rep(1, length(i_starts))
      end_cols = rep(length(j), length(i_starts))
      if (length(j) > 1 || drop == FALSE) {
        dmatrix(
          parts=foreach(i=1:length(i_starts)) %dopar% {
            as_part(x[i_starts[i]:i_ends[i], j])
          },
          i_starts,
          i_ends,
          start_cols,
          end_cols)
      } else {
        dvector(
          parts=foreach(i=1:length(i_starts)) %dopar% {
            as_part(as.vector(x[i_starts[i]:i_ends[i], j]))
          },
          lengths=i_ends - i_starts + 1)
      }
    }
  })

setMethod("[", signature(x="dmatrix", i="numeric", j="numeric", drop="missing"),
  function(x, i, j) x[i,j,drop=TRUE])

setMethod("[", signature(x="dmatrix", i="numeric", j="numeric", drop="logical"),
  function(x, i, j, drop) {
    ret = Matrix(as.numeric(NA), nrow=length(unique(i)), 
                 ncol=length(unique(j)) )
    if (!is.null(rownames(x)))
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
    if (drop && (length(i) == 1 || length(j) == 1)) ret = as.numeric(drop)
    ret
  })

setMethod("Arith", signature(e1='dmatrix', e2='numeric'),
  function(e1, e2) {
    op = .Generic[[1]]
    if (length(e2) == 1) {
      parts = foreach(part = e1@e$parts) %dopar% {
        as_part(do.call(op, list(get_values(part), e2)))
      }
      dmatrix(parts, e1@e$part_locs[,"start_rows"], e1@e$part_locs[,"end_rows"], 
              e1@e$part_locs[,"start_cols"], e1@e$part_locs[,"end_cols"])
    } else {
      if ((inherits(e2, "matrix") || inherits(e2, "Matrix")) && 
           all(dim(e1)==dim(e2))) {
        e1[] + e2
      } else {
        stop("arithmetic operation not defined for these shapes")
      }
    }
  })
 
setMethod("Arith", signature(e1='dmatrix', e2='dmatrix'),
  function(e1, e2) {
    op = .Generic[[1]]
    if (!all(dim(e1) == dim(e2)))
      stop("non-conformable arrays")

    # We'll use e1's partitioning scheme
    part_locs = e1@e$part_locs
    parts = foreach(i=1:nrow(part_locs)) %dopar% {
      part_rows = part_locs[i,"start_rows"]:part_locs[i,"end_rows"]
      part_cols = part_locs[i,"start_cols"]:part_locs[i,"end_cols"]
      as_part(do.call(op, 
        list(e1[part_rows, part_cols], e2[part_rows, part_cols])))
    }
    dmatrix(parts, part_locs[,"start_rows"], part_locs[,"end_rows"],
            part_locs[,"start_cols"], part_locs[,"end_cols"])
  })

setMethod("%*%", signature(x="dmatrix", y="numeric"),
  function(x, y) {
    if (ncol(x) != length(y)) stop("non-conformable arguments")
    # We should check to see if we can emerge something that's
    # options()$row_part_size x length(y)
    i_starts = seq(1, nrow(x), by=options()$row_part_size)
    i_ends = c(i_starts[-1]-1, nrow(x))
    if (length(i_starts) > length(i_ends)) i_starts=i_starts[-length(i_starts)]

    foreach(i=1:length(i_starts), .combine=rbind) %dopar% {
      x[i_starts[i]:i_ends[i], 1:ncol(x)] %*% y
    }
  }) 

setMethod("%*%", signature(x="numeric", y="dmatrix"),
  function(x, y) {
    if (length(x) != nrow(y)) stop("non-conformable arguments")

    # We should check to see if we can emerge something that's
    # options()$row_part_size x length(y)
    j_starts = seq(1, ncol(y), by=options()$col_part_size)
    j_ends = c(j_starts[-1]-1, ncol(y))
    if (length(j_starts) > length(j_ends)) j_starts=j_starts[-length(j_starts)]
    foreach(j=1:length(j_starts), .combine=cbind) %dopar% {
      x %*% y[1:nrow(y), j_starts[j]:j_ends[j]] 
    }
  }) 

setMethod("%*%", signature(x="dmatrix",  y="dmatrix"),
  function(x, y) {
    if (ncol(x) != nrow(y)) stop("non-conformable arguments")

    # Get the absolute coordinates that will define resulting parts.
    # Note that this is only one (possibly dumb) way of defining the resulting
    # parts. 

    i_starts = seq(1, nrow(x), by=options()$row_part_size)
    i_ends = c(i_starts[-1]-1, nrow(x))
    # If we get an extra element, remove it.
    if (length(i_starts) > length(i_ends)) i_starts=i_starts[-length(i_starts)]

    j_starts = seq(1, ncol(y), by=options()$col_part_size)
    j_ends = c(j_starts[-1]-1, ncol(y))
    if (length(j_starts) > length(j_ends)) j_starts=j_starts[-length(j_starts)]

    k_starts = seq(1, ncol(x), by=options()$col_part_size)
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

setMethod("%*%", signature(x="dmatrix", y="dvector"),
  function(x, y) {
    if (ncol(x) != length(y)) stop("non-conformable arguments")
    # Construct a dmatrix from y and use the already-defined matrix-multiply
    # operator.
    i_starts = seq(1, nrow(x), by=options()$row_part_size)
    i_ends = c(i_starts[-1]-1, nrow(x))
    # If we get an extra element, remove it.
    if (length(i_starts) > length(i_ends)) i_starts=i_starts[-length(i_starts)]
    j_starts = rep(1, length(i_starts))
    j_ends = rep(1, length(i_starts))

    k_starts = seq(1, ncol(x), by=options()$col_part_size)
    k_ends = c(k_starts[-1]-1, ncol(x))
    # Should we localize row parts to the same process instead?
    # Or should we do the entire outer product of matrix part multiplications
    # and then add up terms?
    matrix_parts = foreach (i=1:length(i_starts), .combine=c) %:% 
      foreach (j=1:length(j_starts)) %dopar% {
        # start_row, start_col, num_rows, num_cols, end_row, end_col
        list(part_loc=c(i_starts[i], j_starts[j], i_ends[i], j_ends[j]),
          part=as_part(foreach (k=1:length(k_starts), .combine=`+`) %do% {
            x[i_starts[i]:i_ends[i], k_starts[k]:k_ends[k]] %*%
              y[k_starts[k]:k_ends[k]]
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

setMethod("%*%", signature(x="dvector", y="dmatrix"),
  function(x, y) {
    if (length(x) != nrow(y)) stop("non-conformable arguments")

    j_starts = seq(1, ncol(y), by=options()$col_part_size)
    j_ends = c(j_starts[-1]-1, ncol(y))
    if (length(j_starts) > length(j_ends)) j_starts=j_starts[-length(j_starts)]

    i_starts = rep(1, length(j_starts))
    i_ends = i_starts

    k_starts = seq(1, nrow(y), by=options()$col_part_size)
    k_ends = c(k_starts[-1]-1, nrow(y))
    if (length(k_starts) > length(k_ends)) k_starts=k_starts[-length(k_starts)]

    # Should we localize row parts to the same process instead?
    # Or should we do the entire outer product of matrix part multiplications
    # and then add up terms?
    matrix_parts = foreach (i=1:length(i_starts), .combine=c) %:% 
      foreach (j=1:length(j_starts)) %dopar% {
        # start_row, start_col, num_rows, num_cols, end_row, end_col
        list(part_loc=c(i_starts[i], j_starts[j], i_ends[i], j_ends[j]),
          part=as_part(foreach (k=1:length(k_starts), .combine=`+`) %do% {
            x[k_starts[k]:k_ends[k]] %*%
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


