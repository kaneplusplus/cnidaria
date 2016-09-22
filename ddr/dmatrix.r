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
  num_rows = sapply(l, function(x) nrow(x[[2]]))
  num_cols = sapply(l, function(x) ncol(x[[2]]))
  end_row = start_row + num_rows - 1
  end_col = start_col + num_cols - 1
  ret = list(chunks=lapply(l, function(x) as_chunk(x[[2]], chunk_constructor)),
             chunk_locs=cbind(start_row, end_row, start_col, end_col,
                              num_rows, num_cols))
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
    return(ret)
  } 
  
  if (missing(i) & !missing(j)) {
    # For now we'll just fill in j.
    j = 1:ncol(x)
  } else if (!missing(i) & missing(j)) {
    # For now we'll just fill in i.
    i = 1:row(x)
  } 

  if (!missing(i) & !missing(j)) {
    ret = matrix( NA, nrow=length(unique(i)), ncol=length(unique(j)) )
    if (!is.null(nrow(x)))
      rownames(ret) = rownames(x)[i]
    if (!is.null(colnames(x)))
      colnames(ret) = colnames(x)[j]

    ret_vals = convert_coord2d(x$chunk_locs, i, j)
    if (any(ret_vals[,"i"] > nrow(ret)) || any(ret_vals[,"j"] > ncol(ret)))
      stop("subscript out of bounds")

    # We'll use 1-dimensional indexing based on the ret_vals.
    
    # Get the number of rows for each chunk in ret_vals.
    ret_vals = cbind(ret_vals, 
      ret_vals[,"i"] + length(i)*(ret_vals[,"j"]-1),
      ret_vals[,"rel_i"] + 
        x$chunk_locs[ret_vals[,"chunk"],"num_rows"]*(ret_vals[,"rel_j"]-1))
    colnames(ret_vals)[c(6,7)] = c("offset", "rel_offset")
    for(chunk_num in unique(ret_vals[,"chunk"])) {
      chunk_rows = which(ret_vals[,"chunk"] == chunk_num)
      ret[ret_vals[chunk_rows,"offset"]] = 
        get_values(x$chunks[[chunk_num]], ret_vals[chunk_rows, "rel_offset"])
    }
    ret
  } else {
    stop("Parameters not supported yet")
  } 
  ret
}

