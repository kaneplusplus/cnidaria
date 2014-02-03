require(cnidaria)
#options(error=recover)
cnidaria:::dist.worker.init(host="135.207.240.57", zmqAddress="135.207.240.65")
while(1) {
  ret <- service()
  if (!ret)
    Sys.sleep(0.5)
  print(ret)
}
