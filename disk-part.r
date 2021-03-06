source("part-api.r")

#######################################
# The disk.part specific function.
#######################################

# This will be a method that implements as_part.
as_disk_part <- function(x) {
  ret <- list(file_path=file.path(options()$disk_part_dir, guid()))
  con <- file(ret$file_path, open="wb", blocking=FALSE)
  serialize(x, con)
  close(con)
  class(ret) <- c(class(ret), "disk.part")
  ret
}

init_disk_part <- function(part_dir=tempdir()) {
  # Create a new environment if it doesn't already exist.
  options(disk_part_dir=part_dir)
  options(default_part_constructor=as_disk_part)
  TRUE
}

########################################
# The disk.part method definitions.
########################################

get_values.disk.part <- function(part, i, ...) {
  con <- file(part$file_path, open="rb", blocking=FALSE)
  x <- unserialize(con)
  close(con)
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

get_attributes.disk.part <- function(part, labels) {
  con <- file(part$file_path, open="rb", blocking=FALSE)
  x <- unserialize(con)
  close(con)
  if (missing(labels)) {
    attributes(x)
  } else {
    attributes(x)[[labels]] 
  }
}

get_object_size.disk.part <- function(part) {
  con <- file(part$file_path, open="rb", blocking=FALSE)
  x <- unserialize(con)
  close(con)
  object.size(x)
}

get_typeof.disk.part <- function(part) {
  con <- file(part$file_path, open="rb", blocking=FALSE)
  x <- unserialize(con)
  close(con)
  typeof(x)
}

get_class.disk.part <- function(part) {
  con <- file(part$file_path, open="rb", blocking=FALSE)
  x <- unserialize(con)
  close(con)
  class(x)
}

delete_part.disk.part <- function(part) {
  file.remove(part$file_path)
}
