source("disk-part.r")

# Initialize ddr disk parting.
init_ddr_disk_part()

# Create a new part from a matrix.
a = as_part(matrix(1:10, ncol=2))

# Get rows 1, 3, 4.
get_values(a, c(1, 3, 4))

get_attributes(a)

