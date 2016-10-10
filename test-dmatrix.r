source("disk-part.r")
source("dmatrix.r")

# We'll use doSEQ as a parallel execution engine.
library(foreach)
registerDoSEQ()

# and disk parts for the data manager.
init_ddr_disk_part()

# Chunks for an irregular matrix.
l = list(list(c(1, 1), matrix(rnorm(25), nrow=5, ncol=5)),
         list(c(1, 6), matrix(rnorm(36), nrow=6, ncol=6)),
         list(c(6, 1), matrix(rnorm(5), ncol=5, nrow=1)))

# Add the irregular parts to a distributed matrix.
dm = dmatrix(l)

# Make sure that emerging the entire matrix is the same as
# only emerging the speified elements.
all(dm[][1:3,4:5] == dm[1:3,4:5])

# Chunks for an irregular matrix.
l = list(list(c(1, 1), matrix(rnorm(33), nrow=11, ncol=3)),
         list(c(1, 4), matrix(rnorm(33), nrow=11, ncol=3)))

# Add the irregular parts to a distributed matrix.
dm2 = dmatrix(l)

sum( (dm %*% dm2)[] - dm[] %*% dm2[]) < 1e-10
