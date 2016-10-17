
# The default partition sizes.
options(row_part_size=4)
options(col_part_size=4)

options(vector_part_size = options()$row_part_size * options()$col_part_size)
