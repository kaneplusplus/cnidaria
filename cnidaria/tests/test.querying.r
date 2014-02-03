require(cnidaria)
require(testthat)
require(stockPortfolio)

redisConnect()

tickerInfo <- getReturns("DORM")$full[["DORM"]]
ddf <- distribute.data.frame(tickerInfo, 83, data.frame.constructor)

expect_that((ddf$Open == ddf$Open)[], equals(rep(TRUE, nrow(ddf))))
expect_that((ddf$Open <= ddf$Open)[], equals(rep(TRUE, nrow(ddf))))
expect_that((ddf$Open >= ddf$Open)[], equals(rep(TRUE, nrow(ddf))))

upDays <- ddf$Close > ddf$Open
expect_that(upDays[], equals(tickerInfo$Close > tickerInfo$Open))

expect_that(ddf[ddf$Close > ddf$Open,][], 
  equals(tickerInfo[tickerInfo$Close > tickerInfo$Open,]))

expect_that(ddf[ddf$Close > ddf$Open, c("Volume", "Adj.Close")][], 
  equals(
    tickerInfo[tickerInfo$Close > tickerInfo$Open, c("Volume", "Adj.Close")]))

a <- ddf[ddf$Volume > 136400,][]
b <- tickerInfo[tickerInfo$Volume > 136400,]
for (j in 1:ncol(ddf))
  expect_that(a[,j], equals(b[,j]))

