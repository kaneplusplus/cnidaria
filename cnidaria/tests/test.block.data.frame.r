require(cnidaria)
require(testthat)
require(stockPortfolio)

redisConnect()

tickerInfo <- getReturns("DORM")$full[["DORM"]]

ddf <- distribute.data.frame(tickerInfo, 83)

expect_that(ddf[], equals(tickerInfo))

expect_that(ddf[1:10,][], equals(tickerInfo[1:10,][]))

for (j in 1:ncol(ddf)) {
  expect_that(ddf[,j, drop=TRUE][], equals(tickerInfo[,j, drop=TRUE]))
  expect_that(ddf[,j, drop=FALSE][], equals(tickerInfo[,j, drop=FALSE]))
}
 
# Dollar-sign operator
expect_that(ddf$Date[], equals(tickerInfo$Date))
expect_that(ddf$Open[], equals(tickerInfo$Open))
expect_that(ddf$Close[], equals(tickerInfo$Close))
expect_that(ddf$High[], equals(tickerInfo$High))
expect_that(ddf$Low[], equals(tickerInfo$Low))
expect_that(ddf$Volume[], equals(tickerInfo$Volume))
expect_that(ddf$Adj.Close[], equals(tickerInfo$Adj.Close))


it <- ibdf(ddf, chunkSize=47)
isi <- isplitIndices(nrow(tickerInfo), chunkSize=47)
expect_that(nextElem(it), equals(tickerInfo[nextElem(isi),]))

