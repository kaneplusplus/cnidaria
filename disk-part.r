source("part-api.r")

#######################################
# The ddr_disk_part specific function.
#######################################

# This will be a method that implements as_part.
as_ddr_disk_part = function(x) {
  ret = list(file_path=file.path(options()$ddr_disk_part_dir, guid()))
  con = file(ret$file_path, open="wb", blocking=FALSE)
  serialize(x, con)
  close(con)
  class(ret) = c(class(ret), "ddr_disk_part")
  ret
}

init_ddr_disk_part = function(part_dir=tempdir()) {
  # Create a new environment if it doesn't already exist.
  options(ddr_disk_part_dir=part_dir)
  options(default_part_constructor=as_ddr_disk_part)
  TRUE
}

########################################
# The ddr_disk_part method definitions.
########################################

get_values.ddr_disk_part = function(part, i, ...) {
  con = file(part$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  x[i, ...]
}

get_attributes.ddr_disk_part = function(part, labels) {
  con = file(part$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  if (missing(labels)) {
    attributes(x)
  } else {
    attributes(x)[[labels]] 
  }
}

get_object_size.ddr_disk_part = function(part) {
  con = file(part$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  object.size(x)
}

get_typeof.ddr_disk_part = function(part) {
  con = file(part$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  typeof(x)
}

get_class.ddr_disk_part = function(part) {
  con = file(part$file_path, open="rb", blocking=FALSE)
  x = unserialize(con)
  close(con)
  class(x)
}

