#' @title Create a global unique identifier (guid) for a new chunk.
#' 
#' @param len the length of the guid
guid <- function(len=7) {
  paste(sample(letters, len, replace=TRUE), collapse="")
}

#####################
# Method definitions.
#####################

# The get_ prefix denotes we are getting something from the distributed
# chunk. This could be cached by the chunk object.

get_values = function(chunk, i, j, ...) UseMethod("get_values", chunk)

get_attributes = function(chunk, labels) UseMethod("get_attributes", chunk)

get_object_size = function(chunk) UseMethod("get_object_size", chunk)

get_typeof = function(chunk) UseMethod("get_typeof", chunk)

get_class = function(chunk) UseMethod("get_class", chunk)


############################
# Chunk constructor function
############################

as_chunk = function(x, chunk_constructor) {
  if (missing(chunk_constructor)) 
    chunk_constructor = options()$default_chunk_constructor
  
  chunk_constructor(x)
}
