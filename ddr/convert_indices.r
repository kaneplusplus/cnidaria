
# TODO: Optimize these functions.

# Convert and absolute index to a chunk and relative index.
convert_indices = function(lens, abs_indexes) {
  index_ranges = matrix(c(cumsum(lens)-lens+1, cumsum(lens)), ncol=2, 
    dimnames=list(NULL, c("start", "end")))
  ret = matrix(NA, nrow=length(abs_indexes), ncol=2,
    dimnames=list(NULL, c("chunk", "index")))
  for (i in 1:nrow(index_ranges)) {
    ret_rows = which(abs_indexes >= index_ranges[i, "start"] &
                     abs_indexes <= index_ranges[i, "end"])
    ret[ret_rows, "chunk"] = i
    ret[ret_rows, "index"] = abs_indexes[ret_rows]-index_ranges[i, "start"]+1
  }
  ret
}
