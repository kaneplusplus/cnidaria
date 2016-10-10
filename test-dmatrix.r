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
l = list(matrix(rnorm(25), nrow=5, ncol=5),
         matrix(rnorm(36), nrow=6, ncol=6),
         matrix(rnorm(5), ncol=5, nrow=1))

start_rows = c(1, 1, 6)
start_cols = c(1, 6, 1)
# Add the irregular parts to a distributed matrix.
dm = dmatrix_from_matrices(l, start_rows, start_cols)

# Make sure that emerging the entire matrix is the same as
# only emerging the speified elements.
all(dm[][1:3,4:5] == dm[1:3,4:5])

# Chunks for an irregular matrix.
l2 = list(matrix(rnorm(33), nrow=11, ncol=3),
         matrix(rnorm(33), nrow=11, ncol=3))
start_rows2 = c(1, 1)
start_cols2 = c(1, 4)

# Add the irregular parts to a distributed matrix.
dm2 = dmatrix_from_matrices(l2, start_rows2, start_cols2)

sum( (dm %*% dm2)[] - dm[] %*% dm2[]) < 1e-10
