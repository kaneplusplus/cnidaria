library(foreach)

source("convert-indices.r")

setClass('dmatrix', representation(e='environment'))

# TODO: Add better checking to see if the parts make a rectangular
# matrix.
# The list has to include the absolute coordinates for the 
# top-left most element of the part, and the matrix part 
dmatrix = function(l, part_constructor) {
  if (missing(part_constructor))
    part_constructor = options()$default_part_constructor
  if (any(!sapply(l, function(x) is.matrix(x[[2]]))))
    stop("All supplied objects must be matrices")
  
  start_row = sapply(l, function(x) x[[1]][1])
  start_col = sapply(l, function(x) x[[1]][2])
  num_rows = sapply(l, function(x) nrow(x[[2]]))
  num_cols = sapply(l, function(x) ncol(x[[2]]))
  end_row = start_row + num_rows - 1
  end_col = start_col + num_cols - 1
  e = new.env(parent=emptyenv())
  assign("parts", lapply(l, function(x) as_part(x[[2]], part_constructor)),
         envir=e)
  assign("part_locs", cbind(start_row, end_row, start_col, end_col,
                              num_rows, num_cols),
         envir=e)
  new('dmatrix', e=e)
#  ret = list(parts=lapply(l, function(x) as_part(x[[2]], part_constructor)),
#             part_locs=cbind(start_row, end_row, start_col, end_col,
#                              num_rows, num_cols))
#  class(ret) = c("dmatrix", class(ret))
#  ret
}

setMethod("dim", signature(x="dmatrix"),
  function(x) {
    c(max(x@e$part_locs[,"end_row"]), max(x@e$part_locs[,"end_col"]))
  })
#dim.dmatrix = function(x) {
#  c(max(x$part_locs[,"end_row"]), max(x$part_locs[,"end_col"]))
#}

setMethod("[", signature(x="dmatrix", i="missing", j="missing"),
  function(x) {
    # Emerge... we should do some checking to make sure there aren't 
    # too many elements.
    ret = matrix(NA, nrow=nrow(x), ncol=ncol(x))
    for (i in 1:nrow(x@e$part_locs)) {
      ret[x@e$part_locs[i, "start_row"]:x@e$part_locs[i, "end_row"],
          x@e$part_locs[i, "start_col"]:x@e$part_locs[i, "end_col"]] = 
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
        list(part_loc=c(i_starts[i], j_starts[j], i_ends[i]-i_starts[i]+1,
                        j_ends[j]-j_starts[j]+1, i_ends[i], j_ends[j]),
          part=as_part(foreach (k=1:length(k_starts), .combine=`+`) %do% {
            x[i_starts[i]:i_ends[i], k_starts[k]:k_ends[k]] %*%
              y[k_starts[k]:k_ends[k], j_starts[j]:j_ends[j]]
          }))
    }
    part_locs=foreach(i=1:length(matrix_parts), .combine=rbind) %do% {
      matrix_parts[[i]]$part_loc
    }
    dimnames(part_locs) = list(NULL, c("start_row", "start_col", "num_rows",
                                         "num_cols", "end_row", "end_col"))
    e = new.env(parent=emptyenv())
    assign("parts", lapply(matrix_parts, function(x) x[[2]]), envir=e)
    assign("part_locs", part_locs, envir=e)
    new('dmatrix', e=e)
  })

