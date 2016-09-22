# Load the data representation.
source("disk-chunk.r")

# We'll use doSEQ as a parallel execution engine.
library(foreach)
registerDoSEQ()

# We'll test the distributed vector.
source("dvector.r")

init_ddr_disk_chunk()

dv = dvector(list(rnorm(10), rnorm(20), rnorm(15)))

inds = sample.int(length(dv), 10, replace=TRUE)

any(dv[inds] != dv[][inds])

dv2 = dv + 1

any(dv2[] != (dv+1)[])



