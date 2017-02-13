#' Merge data into SQL database
#'
#' @author Alexey Shiklomanov <ashiklom@bu.edu>
#' @param db dplyr \code{src_*} object
#' @param table Name of table, as character
#' @param values \code{data.frame}
#' @param by Character vector of column names to use for the merge
#' @param id_colname Column name for the database ID
#' @param backend Which backend to use for copy operation. Default is "insert".
#' @param ... Additional arguments to \code{backend_*} function
#' @export
#' @examples
#' data(iris)
#' tablename <- 'projects'
#' input <- data.table(
#'   code = 'test',
#'   description = 'example')
#' key <- 'code'
#' dbname <- 'leaf_spectra'
#' merge_with_sql(input, tablename, key = 'code')
db_merge_into <- function(db, table, values, by, id_colname,
                          backend = 'insert', return = TRUE, ...) {
    supported_backends <- c('insert', 'psql_copy', 'pg_bulkload', 'sqlite_import')
    if (!backend %in% supported_backends) {
        stop('Backend "', backend, '" not supported. ',
             'Please select one of the following: ', 
             paste(supported_backends, collapse = ', '))
    }

    sql_table <- dplyr::tbl(db, table)
    sql_cols <- dplyr::tbl_vars(sql_table)

    # Select only those columns that are in target table
    input_cols <- colnames(values)
    keep_col_logical <- input_cols %in% sql_cols
    keep_cols <- input_cols[keep_col_logical]
    drop_cols <- input_cols[!keep_col_logical]
    if (length(drop_cols) > 0) {
        message('The following columns were not in the src and will be dropped: ',
                paste(drop_cols, collapse = ', '))
    }
    input_sel <- dplyr::select_(values, .dots=keep_cols)

    sql_nrow <- dplyr::collect(dplyr::count(sql_table))[['n']]
    if (sql_nrow > 0) {
        sql_keys <- dplyr::collect(dplyr::distinct_(sql_table, .dots = by))
        if (data.table::is.data.table(input_sel)) {
            data.table::setDT(sql_keys)
        }
        input_sub <- dplyr::anti_join(input_sel, sql_keys)
    } else {
        message('Writing into empty table. All values will be used.')
        input_sub <- input_sel
    }
    n_added <- nrow(input_sub)
    if (n_added > 0) {
        if (backend == 'insert') {
            insert <- backend_insert(db, table, input_sub, id_colname, ...)
        } else if (backend == 'psql_copy') {
            data.table::setDT(input_sub)
            insert <- backend_psql_copy(db, table, input_sub, id_colname, ...)
        } else if (backend == 'pg_bulkload') {
            data.table::setDT(input_sub)
            insert <- backend_pg_bulkload(db, table, input_sub, id_colname, ...)
        } else if (backend == 'sqlite_import') {
            data.table::setDT(input_sub)
            insert <- backend_sqlite_import(db, table, input_sub, id_colname, ...)
        }
        message('Added ', n_added, ' rows to table ', table)
    } else {
        message('All rows in input already in table ', table)
    }
    if (isTRUE(return)) {
        new_sql_keys <- dplyr::collect(dplyr::distinct_(sql_table, .dots = c(id_colname, by)))
        if (data.table::is.data.table(values)) {
            data.table::setDT(new_sql_keys)
        }
        result <- dplyr::left_join(values, new_sql_keys)
        return(result)
    } else {
        return(NULL)
    }
} # db_merge_into
