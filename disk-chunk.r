source("chunk-api.r")

#######################################
# The ddr_disk_chunk specific function.
#######################################

# This will be a method that implements as_chunk.
as_ddr_disk_chunk = function(x) {
  ret = list(file_path=file.path(options()$ddr_disk_chunk_dir, guid()))
  con = file(ret$file_path, open="wb", blocking=FALSE)
  serialize(x, con)
  close(con)
  class(ret) = c(class(ret), "ddr_disk_chunk")
  ret
}

init_ddr_disk_chunk = function(chunk_dir=tempdir()) {
  # Create a new environment if it doesn't already exist.
  options(ddr_disk_chunk_dir=chunk_dir)
  options(default_chunk_constructor=as_ddr_disk_chunk)
  TRUE
}

########################################
# The ddr_disk_chunk method definitions.
########################################

get_values.ddr_disk_chunk = function(chunk, i, ...) {
  con = file(chunk$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  x[i, ...]
}

get_attributes.ddr_disk_chunk = function(chunk, labels) {
  con = file(chunk$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  if (missing(labels)) {
    attributes(x)
  } else {
    attributes(x)[[labels]] 
  }
}

get_object_size.ddr_disk_chunk = function(chunk) {
  con = file(chunk$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  object.size(x)
}

get_typeof.ddr_disk_chunk = function(chunk) {
  con = file(chunk$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  typeof(x)
}

get_class.ddr_disk_chunk = function(chunk) {
  con = file(chunk$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  class(x)
}

