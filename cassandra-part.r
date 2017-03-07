source("part-api.r")

q = casslite::cl.prepare("select v from test.block where key=?")

query = function(key) casslite::cl.execute(q, -1, key)

as_cass_part = function(x) {
  ret = list(key=guid())
  class(ret) = c(class(ret), "cassandra_part")
  iq = casslite::cl.prepare("insert into test.block (key, v) values (?, ?)")
  insert = function(key, value) casslite::cl.execute(iq, 0, key, value)
  insert(ret$key, serialize(x))
  ret
}

get_values.cassandra_part = function(part, i, ...) {
  q = casslite::cl.prepare("select v from test.block where key=?")
  query = function(key) casslite::cl.execute(q, -1, key)
  x = unserialize(query(part))
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
  q = casslite::cl.prepare("select v from test.block where key=?")
  query = function(key) casslite::cl.execute(q, -1, key)
  x = unserialize(query(part))
  if (missing(labels)) {
    attributes(x)
  } else {
    attributes(x)[[labels]] 
  }
}

get_object_size.cassandra_part = function(part) {
  q = casslite::cl.prepare("select v from test.block where key=?")
  query = function(key) casslite::cl.execute(q, -1, key)
  x = unserialize(query(part))
  object.size(x)
}

get_typeof.cassandra_part = function(part) {
  q = casslite::cl.prepare("select v from test.block where key=?")
  query = function(key) casslite::cl.execute(q, -1, key)
  x = unserialize(query(part))
  typeof(x)
}

get_class.cassandra_part = function(part) {
  q = casslite::cl.prepare("select v from test.block where key=?")
  query = function(key) casslite::cl.execute(q, -1, key)
  x = unserialize(query(part))
  class(x)
}

