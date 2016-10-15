library(foreach)

source("convert-indices.r")


setClass('dvector', representation(e='environment'))

dvector = function(parts, lengths) {
  e = new.env(parent=emptyenv())
  assign('parts', parts, envir=e)
  assign('lengths', lengths, envir=e)
  assign('length', sum(lengths), envir=e)
  new('dvector', e=e)
}

# TODO: Add code to make sure factors levels are normalized across
# the passed vectors.
dvector_from_vectors = function(l, part_constructor) {
  if (missing(part_constructor)) 
    part_constructor = options()$default_part_constructor
  if (any(!sapply(l, function(x) inherits(x, "numeric") || 
                                 inherits(x, "sparseVector")))) {
    stop("All supplied objects must be vectors.")
  }
  dvector(lapply(l, as_part, part_constructor), sapply(l, length))
}

setMethod("[",
  signature(x='dvector', i="missing"),
  function(x) {
    # TODO: we need a warning that the user probably doesn't want to 
    # emerge the entire distributed vector if the size is above a threshold.
    unlist(sapply(x@e$parts, function(x) as.vector(get_values(x))))
  })

setMethod("[",
  signature(x='dvector', i="numeric"),
  function(x, i) {
    ret = rep(NA, length(i))
    ci = convert_indices(x@e$lengths, i)
    for (cn in unique(ci[,"part"])) {
      ret[ci[,"part"] == cn] = as.vector(get_values(x@e$parts[[cn]], 
        ci[ci[,"part"]==cn,"index"]))
    }
    ret
  })

#`[.dvector` = function(x, i) {
#  if (missing(i)) {
#    # TODO: we need a warning that the user probably doesn't want to 
#    # emerge the entire distributed vector if the size is above a threshold.
#    unlist(sapply(x$parts, get_values))
#  } else if (is.numeric(i)) {
#    ret = rep(NA, length(i))
#    ci = convert_indices(x$lengths, i)
#    for (cn in unique(ci[,"part"])) {
#      ret[ci[,"part"] == cn] = get_values(x$parts[[cn]], 
#        ci[ci[,"part"]==cn,"index"])
#    }
#    ret
#  } else {
#    stop("Index type not supported.")
#  }
#}

setMethod("length", signature(x='dvector'), function(x) x@e$length)

#length.dvector = function(x) {
#  x$length
#}

# Arithmetic operators require an execution engine. We'll use foreach.

setMethod("Arith", signature(e1='dvector', e2='numeric'),
  function(e1, e2) {
    op = .Generic[[1]]
    if (length(e2) == 1) {
      parts = foreach(part = e1@e$parts) %dopar% {
        as_part(do.call(op, list(get_values(part), e2)))
      }
      e=new.env(parent=emptyenv())
      assign("parts", parts, envir=e)
      assign("length", e1@e$length, envir=e)
      assign("lengths", e1@e$lengths, envir=e)
      new('dvector', e=e)
    } else if (length(e2) == e1) {
      e2[] + e1
    }
  })

#Ops.dvector = function(e1, e2) {
#  FUN = get(.Generic, envir = parent.frame(), mode = "function")
#  if (class(e2) == "numeric" && length(e2) == 1)
#    dvector(foreach(part = e1$parts) %dopar% FUN(get_values(part), e2))
#}

