require(cnidaria)
require(rredis)
require(testthat)

redisConnect()

rl <- makeRAQ("127.0.0.1", 6379)
expect_that(addPop(rl, "test1"), equals(TRUE))
expect_that(popKeys(rl), equals("test1"))

timeout <- 3
expect_that(
  round(as.vector(system.time(a <- nextRAQMessage(rl, timeout))[3])), 
  equals(timeout))

tm1 <- "test message 1"
redisRPush("test1", tm1)
msg <- nextRAQMessage(rl, timeout)
expect_that(names(msg), equals("test1"))
expect_that(unserialize(msg$test), equals(tm1))

tm2 <- "test message 2"
redisRPush("test2", tm2)

expect_that(addPop(rl, "test2"), equals(TRUE))
expect_that(popKeys(rl), equals(c("test1", "test2")))
expect_that(remPop(rl, "test1"), equals(TRUE))

a <- nextRAQMessage(rl, timeout)
expect_that(names(a), equals("test2"))
expect_that(unserialize(a$test2), equals(tm2))


