library(testthat)
library(Matrix)

# Load the data representation.
source("disk-part.r")

# We'll use doSEQ as a parallel execution engine.
library(foreach)
registerDoSEQ()

# Test the distributed vector.
source("dvector.r")

# Initialize disk parting.
init_ddr_disk_part()

# Create the vector.
dv = dvector_from_vectors(list(rnorm(10), rnorm(20), rnorm(15)))
class(dv)

dv_sparse = dvector_from_vectors(list(rnorm(10), 
  sparseVector(1:2, 4:5, 15), rnorm(20)))

# Sample a few element positions.
inds = sample.int(length(dv), 10, replace=TRUE)

# Emerge the vector at the specified indices and see if it's
# the same same as emerging the entire vector and then
# subsetting by indices.
any(dv[inds] != dv[][inds])

# Add one to each element of the vector.
dv2 = dv + 1

# Make sure it's the same as emerging dv and then adding 1.
any(dv2[] != (dv+1)[])



