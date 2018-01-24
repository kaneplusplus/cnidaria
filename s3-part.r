library(aws.s3)

source("part-api.r")

init_s3_part <- function(bucket_name) {
  options(default_part_constructor=as_s3_part)
  options(s3_bucket_name=bucket_name)
  invisible(TRUE)
}

# This should be changed so parts can be stored at different urls.
as_s3_part <- function(x, ...) {
  ret <- list(bucket=options()$s3_bucket_name, file_name=guid())
  class(ret) <- c(class(ret), "s3.part")
  put_object(serialize(x, NULL), ret$file_name, ret$bucket, ...)
  ret
}

s3_retrieve <- function(file_name, bucket_name, ...) {
  unserialize(get_object(file_name, bucket_name, ...))
}

get_value.s3.part <- function(part, i) {
  # TODO: add other relevant parameters.
  x <- s3_retrieve(part$key, part$buket_name)
  if (!missing(i) && !missing(...)) {
    x[i, ...]
  } else if (missing(i) && !missing(...)) {
    x[...]
  } else if (!missing(i) && missing(...)) {
    x[i]
  } else {
    x
  }
}

get_attributes.s3.part <- function(part, labels) {
  x <- s3_retrieve(part$key, part$bucket_name)
  if (missing(labels)) {
    attributes(x)
  } else {
    attributes(x)[[labels]]
  }
}

get_object_size.s3.part <- function(part) {
  object.size(s3_retrieve(part$key, part$bucket_name))
}

get_typeof.s3.part <- function(part) {
  typeof(s3_retrieve(part$key, part$bucket_name))
}

get_class.s3.part <- function(part) {
  class(s3_retrieve(part$key, part$bucket_name))
}

delete_part.s3.part <- function(part) {
  delete_object(part$key, part$bucket_name)
}


