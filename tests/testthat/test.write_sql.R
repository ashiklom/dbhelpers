library(dbhelpers)
library(dplyr)
library(testthat)

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

db_insert_into(sqlite$con, 
               sql('SELECT "Sepal.Length" FROM "iris"'),
               values = data.frame(Sepal.Length = c(-1, -1)))

#db <- sqlite
#table <- 'iris'
#values <- iris
#by <- c('num', 'char')
#id_colname <- 'id'
#backend <- 'db_insert_into'

mrg <- db_merge_into(db = sqlite, 
                     table = 'iris', 
                     values = iris,
                     by = c('num', 'char'),
                     id_colname = 'id')

mrg2 <- db_merge_into(db = sqlite,
                      table = 'iris',
                      values = iris,
                      by = c('num', 'char'),
                      id_colname = 'id')

out <- tbl(sqlite, 'iris') %>% collect() 

test_that('Extra rows have not been added', 
          {
              expect_equal(count(out)$n, nrow(iris))
          })

test_that('Merge output has id column',
          {
              expect_true('id' %in% colnames(out))
          })

