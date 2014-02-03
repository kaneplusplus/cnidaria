require(Rcpp)
require(rredis)

Sys.setenv(PKG_LIBS=paste("-lhiredis", Rcpp:::RcppLdFlags()))
Sys.setenv(PKG_CPPFLAGS=
  sprintf("-Wall -std=gnu++0x -I%s %s", getwd(),Sys.getenv("PKG_CPPFLAGS")))

sourceCpp("../../src/RAQRcpp.cpp", rebuild=TRUE, verbose=TRUE)

rl <- makeRAQ("127.0.0.1", 6379)
addPop(rl, "test")
system.time(a <- nextRAQMessage(rl, 10))



