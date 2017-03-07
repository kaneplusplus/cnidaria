source("part-api.r")

cass_insert(key, value) {
  iq = casslite::cl.prepare("insert into test.block (key, v) values (?, ?)")
  insert = function(key, value) casslite::cl.execute(iq, 0, key, value)
  insert(key, value)
}

cass_retrieve(key) {
  q = casslite::cl.prepare("select v from test.block where key=?")
  query = function(key) casslite::cl.execute(q, -1, key)
  query(part)
}

as_cass_part = function(x) {
  ret = list(key=guid())
  class(ret) = c(class(ret), "cassandra_part")
  cass_insert(ret$key, serialize(x))
  ret
}

get_values.cassandra_part = function(part, i, ...) {
  x = unserialize(cass_retrieve(part$key))
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

get_attributes.cassandra_part = function(part, labels) {
  x = unserialize(cass_retrieve(part$key))
  if (missing(labels)) {
    attributes(x)
  } else {
    attributes(x)[[labels]] 
  }
}

get_object_size.cassandra_part = function(part) {
  x = unserialize(cass_retrieve(part$key))
  object.size(x)
}

get_typeof.cassandra_part = function(part) {
  x = unserialize(cass_retrieve(part$key))
  typeof(x)
}

get_class.cassandra_part = function(part) {
  x = unserialize(cass_retrieve(part$key))
  class(x)
}

