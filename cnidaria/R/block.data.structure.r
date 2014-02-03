
#' block.data.structure-class 
#' 
#' The abstract block data structure class all data
#' structures in the package inherit from.
#'
#' @name block.data.structure-class
#' @rdname block.data.structure-class
#' @exportClass block.data.structure
setClass("block.data.structure")

#' @export
typeof <- function(x) {
  UseMethod("typeof", x)
}

#' @export
typeof.default <- function(x) {
  .Internal(typeof(x))
}

##' @export
#class <- function(x) {
#  UseMethod("class", x)
#}

##' @export
#class.default <- function(x) {
#  .Primitive("class")(x)
#}

#' @export
con <- function(x, r) {
  UseMethod("con", x)
}

#' @export
block.names <- function(x) {
  UseMethod("block.names", x)
}

#' @export
`block.names<-` <- function(x, value) {
  UseMethod("block.names<-", x)
}

