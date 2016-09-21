source("disk-chunk.r")

# Initialize ddr disk chunking.
init_ddr_disk_chunk()

# Create a new chunk from a matrix.
a = as_chunk(matrix(1:10, ncol=2))

# Get rows 1, 3, 4.
get_values(a, c(1, 3, 4))

get_attributes(a)

