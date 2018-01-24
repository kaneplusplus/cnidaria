
delete_finalizer = function(e) {
  lapply(dv@e$parts, delete_part)
}
