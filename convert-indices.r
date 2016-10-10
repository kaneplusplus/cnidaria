
# TODO: Optimize these functions.

# Convert and absolute index to a part and relative index.
convert_indices = function(lens, abs_indexes) {
  index_ranges = matrix(c(cumsum(lens)-lens+1, cumsum(lens)), ncol=2, 
    dimnames=list(NULL, c("start", "end")))
  ret = matrix(NA, nrow=length(abs_indexes), ncol=2,
    dimnames=list(NULL, c("part", "index")))
  for (i in 1:nrow(index_ranges)) {
    ret_rows = which(abs_indexes >= index_ranges[i, "start"] &
                     abs_indexes <= index_ranges[i, "end"])
    ret[ret_rows, "part"] = i
    ret[ret_rows, "index"] = abs_indexes[ret_rows]-index_ranges[i, "start"]+1
  }
  ret
}

convert_coord2d = function(locs, i, j) {
  ret = matrix(NA, nrow=length(i) * length(j), ncol=5, 
               dimnames=list(NULL, c("i", "j", "part", "rel_i", "rel_j")))
  ret[,"i"] = rep(i, each=length(j))
  ret[,"j"] = j
  for (k in 1:nrow(locs)) {
    ret_rows = which(ret[,"i"] >= locs[k,"start_row"] &
                     ret[,"i"] <= locs[k,"end_row"] &
                     ret[,"j"] >= locs[k,"start_col"] &
                     ret[,"j"] <= locs[k,"end_col"])
    if (length(ret_rows) > 0) {
      ret[ret_rows, "part"] = k
      ret[ret_rows, "rel_i"] = ret[ret_rows, "i"] - locs[k,"start_row"] + 1
      ret[ret_rows, "rel_j"] = ret[ret_rows, "j"] - locs[k,"start_col"] + 1
    }
  }
  # There is a better way to go from the i and j values in the 
  # matrix we're querying to the i and j values in the resulting matrix.
  # However, it will take more time.
  i_lookup = 1:length(unique(ret[,"i"]))
  names(i_lookup) = as.character(unique(ret[,"i"]))
  j_lookup = 1:length(unique(ret[,"j"]))
  names(j_lookup) = as.character(unique(ret[,"j"]))
  ret[,"i"] = i_lookup[as.character(ret[,"i"])]
  ret[,"j"] = j_lookup[as.character(ret[,"j"])]
  ret
}
