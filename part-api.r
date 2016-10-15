#' @title Create a global unique identifier (guid) for a new part.
#' 
#' @param len the length of the guid
guid <- function(len=7) {
  paste(sample(letters, len, replace=TRUE), collapse="")
}

#####################
# Method definitions.
#####################

# The get_ prefix denotes we are getting something from the distributed
# part. This could be cached by the part object.

get_values = function(part, i, ...) UseMethod("get_values", part)

get_attributes = function(part, labels) UseMethod("get_attributes", part)

get_object_size = function(part) UseMethod("get_object_size", part)

get_typeof = function(part) UseMethod("get_typeof", part)

get_class = function(part) UseMethod("get_class", part)


############################
# Chunk constructor function
############################

as_part = function(x, part_constructor) {
  if (missing(part_constructor)) 
    part_constructor = options()$default_part_constructor
  
  part_constructor(x)
}
