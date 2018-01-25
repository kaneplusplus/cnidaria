# Make sure Mike's patched version of aws.s3 is installed.
# devtools::install_github("kaneplusplus/aws.s3")
library(aws.s3)

# Set up connectivity and create a bucket to hold objects.
minio_keys <- read.csv("~/.minio.keys", header=FALSE, stringsAsFactors=FALSE)
key <- minio_keys[1, 2]
secret <- minio_keys[2, 2] 

Sys.setenv("AWS_ACCESS_KEY_ID" = key, "AWS_SECRET_ACCESS_KEY" = secret)
set_base_url("rcloud-2.research.att.com:9000")

if (!bucket_exists("mikes-test-bucket")) {
  put_bucket("mikes-test-bucket")
}

source("s3-part.r")

# Initialize S3 storage.
init_s3_part("mikes-test-bucket")

# Create a new part from a matrix.
a <- as_part(matrix(1:10, ncol=2))

# Get rows 1, 3, 4.
get_values(a, c(1, 3, 4))

get_attributes(a)

rm(a)

