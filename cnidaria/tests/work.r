require(cnidaria)
#options(error=recover)
#cnidaria:::dist.worker.init(host="135.207.240.57", zmqAddress="135.207.240.65")
cnidaria:::dist.worker.init(host="127.0.0.1", zmqAddress="tcp://127.0.0.1")
while(1) {
  ret <- service()
  if (!ret)
    Sys.sleep(0.5)
  print(ret)
}
