rm -rf /tmp/R*
redis-cli flushdb
redis-cli flushall
#ps aux | grep work\\.r | awk '{print $2}' | xargs kill
Rscript roxygenize.r
R CMD INSTALL cnidaria
Rscript cnidaria/tests/work.r &
Rscript cnidaria/tests/work.r 

