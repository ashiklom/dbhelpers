library(dbhelpers)
library(dplyr)
library(testthat)
#devtools::document('../..')
#devtools::load_all('../..')

test_db <- tempfile()
sqlite <- src_sqlite(test_db, create = TRUE)

data(iris)

iris$num <- rep(1:11, length.out = nrow(iris))
iris$char <- rep(letters, length.out = nrow(iris))
#distinct(iris, num, char)

# Create fake database consisting of subset to test merge
initdb <- iris[1:10,]
initdb$id <- as.integer(1:10)
db_insert_into(con = sqlite$con, table = 'iris', values = initdb)

db <- sqlite
table <- 'iris'
values <- iris
by <- c('num', 'char')
id_colname <- 'id'
backend <- 'insert'

mrg <- db_merge_into(db = sqlite, 
                     table = 'iris', 
                     values = iris[1:50,],
                     by = c('num', 'char'),
                     id_colname = 'id')

mrg2 <- db_merge_into(db = sqlite,
                      table = 'iris',
                      values = iris,
                      by = c('num', 'char'),
                      id_colname = 'id',
                      backend = 'sqlite_import')

mrg3 <- db_merge_into(db = sqlite,
                      table = 'iris',
                      values = iris,
                      by = c('num', 'char'),
                      id_colname = 'id')

mrg4 <- db_merge_into(db = sqlite,
                      table = 'iris',
                      values = iris,
                      by = c('num', 'char'),
                      id_colname = 'id',
                      backend = 'sqlite_import')

out <- tbl(sqlite, 'iris') %>% collect() 

test_that('Extra rows have not been added', 
          {
              expect_equal(count(out)$n, nrow(iris))
          })

test_that('Merge output has id column',
          {
              expect_true('id' %in% colnames(out))
          })

