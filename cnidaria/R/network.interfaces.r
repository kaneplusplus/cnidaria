require(foreach)

inet.sep.strings <- function() {
  if (.Platform$OS.type != "unix")
    stop("ifconfig is only supported on the unix platform.")
  ret <- NULL
  if (Sys.info()['sysname'] == "Linux") {
    ret <- c(interface=" ", field=":")
  } else if (Sys.info()['sysname'] == "Darwin") {
    ret <- c(interface=":", field=" ")
  } else {
    stop("Unknown system")
  }
  ret
}

extract.inet.addr <- function(chunk, seps) {
  ret <- NA
  if (Sys.info()['sysname'] == "Darwin") {
    ln <- grep(paste("inet", seps['field'], sep=""), chunk)
    if (length(ln) > 0) {
      ss <- unlist(strsplit(chunk[ln], seps['field']))
      ret <- ss[grep("inet", unlist(strsplit(chunk[ln], seps['field'])))+1]
    }
  } else if (Sys.info()['sysname'] == "Linux") {
    ln <- grep(paste("inet addr", seps['field'], sep=""), chunk)
    if (length(ln) > 0) {
      ss <- unlist(strsplit(chunk[ln], seps['field']))
      ret <- unlist(strsplit(ss[2], " "))[1]
    }
  }
  ret
}

#' @export
inet.addr <- function(interface.name=NULL) {
  # Use the inet sep.string to check the system and get the separator, 
  # if needed.
  inetSeps <- inet.sep.strings()
  
  ifs <- system("/sbin/ifconfig", intern=TRUE)

  ifStartLines <- setdiff(1:length(ifs), grep("^\\s", ifs, perl=TRUE))
  ifStartLines <- ifStartLines[ifs[ifStartLines] != ""]
  ss <- strsplit(ifs[ifStartLines], inetSeps['interface'])
  allInterfaces <- unlist(Map( function(x) x[1], ss ))
  ifEndLines <- c(ifStartLines[-1]-1, length(ifs))
  idf <- data.frame(list(interface=allInterfaces, start.line=ifStartLines, 
    end.line=ifEndLines))

  if (is.null(interface.name))
    interface.name <- allInterfaces

  ret <- foreach(interfaceName=interface.name, .combine=rbind) %do% {
    fret <- c(interfaceName, NA)
    ind <- which(idf$interface == interfaceName)
    if (length(ind) > 0) {
      fret <- c(interfaceName, 
        extract.inet.addr(ifs[idf$start.line[ind]:idf$end.line[ind]], inetSeps))
    }
    fret
  }
  if (is.vector(ret)) {
    ret <- data.frame(interface=ret[1], address=ret[2])
  } else {
    dimnames(ret) <- list(NULL, c("interface", "address"))
    ret <- data.frame(ret)
  }
  ret
}

