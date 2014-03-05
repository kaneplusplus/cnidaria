require(cnidaria)
require(testthat)

# hadoop17
#cnidaria:::dist.worker.init(zmqAddress="tcp://135.207.240.57")
cnidaria:::dist.worker.init()
constructor <- dist.vector

# 
dv <- distribute.vector(rnorm(1000), 79)
a <- foreach(b=block.names(dv) ) %do% {
  pull(b, paste("length(", b, ")", sep=""))
}
foreach (b=a, .combine=c) %do% {
  print(recv(b))
  NULL
}

blockLens <- pullApply(parse(text="length(dv)"), block.names(dv), "dv")

expect_that(typeof(dv), equals("double"))
expect_that(length(dv), equals(sum(blockLens)))

# numeric indexing.
a <- dv[c(1:10, 78:79)]
b <- dv[][c(1:10, 78:79)]
expect_that(a[], equals(b))

# numeric dist vector indexing
# TODO: Start here!
iv <- distribute.vector(sample(1:length(dv), 35, replace=TRUE), 14)
a <- dv[iv]
expect_that(dv[][iv[]], equals(a[]))
# logical indexing, including distributed vector indexing.
bs <- sample(c(TRUE, FALSE), 1000, replace=TRUE)
bv <- distribute.vector(bs, 101)

expect_that(bv[], equals(bs))
tv <- dv[bv]
expect_that(tv[], equals(dv[][bv[]]))
expect_that(dv[bv][] ,equals(dv[][bv[]]))

# iterators
itBv <- ibv(bv, chunkSize=71)
it <- isplitIndices(length(bs), chunkSize=71)
expect_that(nextElem(itBv), equals(bs[nextElem(it)]))
expect_that(nextElem(itBv), equals(bs[nextElem(it)]))
expect_that(nextElem(itBv), equals(bs[nextElem(it)]))

# distributed factors.
dfv <- distribute.vector(iris$Species, 35)
expect_that(dfv[], equals(iris$Species))


