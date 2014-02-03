#' @useDynLib cnidaria

local.worker.init <- function() {
  if (!(".local.resource.env" %in% names(options()))) {
    e <- new.env(parent=emptyenv())
    e$w <- worker()
    pull(e$w, parse(text="require(cnidaria)"))
    options(.local.resource.env=e)
  }
}

get.local.con <- function() {
  local.worker.init()
  options()$.local.resource.env$w
}

#' @title Pull the result of an expression from a worker.
#'
#' @param w The worker to pull the result from.
#' @param expr The expression that retrieves the desired data
#' @export
pull <- function(w, expr) {
  UseMethod("pull", w)
}

#' @title Push an expression to a worker and create a new block. 
#' 
#' @param w The worker to push the expression to.
#' @param expr The expression to execute or the variable to put to the worker
#' side. Note that with pull you can pass an character expression, with
#' push you cannot do this.
#' @export
push <- function(w, expr, resultHandle) {
  UseMethod("push", w)
}

#' @title Constructor creating an uninitialized worker.
#' 
#' @seealso \code{\link{worker-class}}
#' @export
worker <- function() {
  ret <- list(env=new.env())
  class(ret) <- "worker"
  ret
}

#' @export
pull.worker <- function(w, expr) {
  ret <- invisible()
  if (is.expression(expr)) {
    ret <- eval(expr, envir=w$env)
  } else if(is.character(expr)) {
    ret <- eval(parse(text=expr), envir=w$env)
  }
  ret
}

#' @export
push.worker <- function(w, expr, resultHandle) {
  # Create the new block name and assign it to the result of the push
  # expression inside the worker's environment.
  addPop(get.raq(), resultHandle)
  if (is.expression(expr)) {
    #assign(newBlock, eval(expr, envir=w$env), envir=w$env)
    #w$env[[newBlock]] <- eval(expr, envir=w$env)
    r <- try(eval(expr, envir=w$env), silent=TRUE)
    if (inherits(r, "try-error")) {
      #print(r)
      print("trying again")
      r <- try(eval(expr, envir=w$env, enclos=NULL), silent=TRUE)
    }
    #print(r)
   } else {
    #assign(newBlock, expr, envir=w$env)
    #w$env[[newBlock]] <- expr
    r <- expr
  }
  w$env[[resultHandle]] <- r
  resultHandle
}

