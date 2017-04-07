library(testthat)

#source("disk-part.r")
source("cassandra-part.r")
source("dmatrix.r")

# We'll use doSEQ as a parallel execution engine.
library(foreach)
#library(doMC)
#registerDoMC()
registerDoSEQ()

# and disk parts for the data manager.
#init_disk_part()
init_cassandra_part()

# Chunks for an irregular matrix.
l = list(matrix(rnorm(25), nrow=5, ncol=5),
         matrix(rnorm(36), nrow=6, ncol=6),
         matrix(rnorm(5), ncol=5, nrow=1))

start_rows = c(1, 1, 6)
start_cols = c(1, 6, 1)
# Add the irregular parts to a distributed matrix.
dm = dmatrix_from_matrices(l, start_rows, start_cols)

# Make sure that emerging the entire matrix is the same as
# only emerging the speified elements.
all.equal(as.matrix(dm[][1:3,4:5]),  as.matrix(dm[1:3, 4:5]))

# Chunks for an irregular matrix.
l2 = list(matrix(rnorm(33), nrow=11, ncol=3),
         matrix(rnorm(33), nrow=11, ncol=3))
start_rows2 = c(1, 1)
start_cols2 = c(1, 4)

# Add the irregular parts to a distributed matrix.
dm2 = dmatrix_from_matrices(l2, start_rows2, start_cols2)

# Matrix-multiplication and addition.
all.equal( (dm %*% dm2)[], dm[] %*% dm2[])

all.equal( (dm + dm)[], dm[] + dm[])

dv = dvector_from_vectors(list(rnorm(3), rnorm(3), rnorm(5)))

all.equal( as.matrix((dm %*% dv)[]), as.matrix(dm[] %*% dv[]))

# The irlba
library(irlba)

a = irlba(dm, nv=2, nu=2, mult=`%*%`)
b = irlba(dm[], nv=2, nu=2, mult=`%*%`)

all.equal(a$d, b$d)
all.equal(abs(a$u), abs(b$u))
all.equal(abs(a$v), abs(b$v))
# These are slightly different but the result is the same.
#all.equal(a$iter, b$iter)
#all.equal(a$mprod, b$mprod)
