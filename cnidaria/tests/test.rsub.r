require(cnidaria)
require(testthat)

tryRedis <- TRUE
if (tryRedis) {
  redis <- TRUE
  trc <- try(redisConnect(), silent=TRUE)
  if (inherits(trc, "try-error")) {
    redis <- FALSE
    constructor <- local.block.vector
  } else {
    # It looks like we have a working redis connection.
    workerIds <- paste("worker", 1:2, sep="")
#    start.dist.worker(workerIds, verbosity=2)
    Sys.sleep(1)
    constructor <- dist.vector
  }
} else {
  constructor <- local.block.vector
}

# 
dv <- distribute.vector(rnorm(1000), 79)

print(resource.names(dv))

resourceSubstitute("pull", parse(text="ls(dv)"), dv)

