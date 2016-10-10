library(foreach)

source("convert_indices.r")

# TODO: Add code to make sure factors levels are normalized across
# the passed vectors.
dvector = function(l, part_constructor) {
  if (missing(part_constructor)) 
    part_constructor = options()$default_part_constructor
  if (any(!sapply(l, is.vector))) 
    stop("All supplied objects must be vectors.")

  lens = sapply(l, length)
  len = sum(lens)
  ret = list(parts=lapply(l, as_part, part_constructor), 
             length=len, lengths=lens)
  class(ret) = c("dvector", class(ret))
  ret
}

`[.dvector` = function(x, i) {
  if (missing(i)) {
    # TODO: we need a warning that the user probably doesn't want to 
    # emerge the entire distributed vector if the size is above a threshold.
    unlist(sapply(x$parts, get_values))
  } else if (is.numeric(i)) {
    ret = rep(NA, length(i))
    ci = convert_indices(x$lengths, i)
    for (cn in unique(ci[,"part"])) {
      ret[ci[,"part"] == cn] = get_values(x$parts[[cn]], 
        ci[ci[,"part"]==cn,"index"])
    }
    ret
  } else {
    stop("Index type not supported.")
  }
}

length.dvector = function(x) {
  x$length
}

# Arithmetic operators require an execution engine. We'll use foreach.

Ops.dvector = function(e1, e2) {
  FUN = get(.Generic, envir = parent.frame(), mode = "function")
  if (class(e2) == "numeric" && length(e2) == 1)
    dvector(foreach(part = e1$parts) %dopar% FUN(get_values(part), e2))
}

