library(cnidaria)
redisConnect()
workerIds <- "worker1"
constructor <- redis.dist.vector
formals(constructor)$ids <- workerIds
d <- rnorm(1000)
dv <- distribute.vector(d, 79, constructor)

typeof(dv)

a <- dv[c(1:10, 40:30)]
dv2 <- distribute.vector(sample(1:1000, 300), 112, constructor)
b <- dv[dv2]

any( (b[] == dv[][dv2[]]) == FALSE)

bv <- distribute.vector(sample(c(TRUE, FALSE), 1000, replace=TRUE), 101, 
  constructor)


# TODO: This is deadlocking... fix it.
 tv <- dv[bv]
any( (tv[] == dv[][bv[]]) == FALSE)
