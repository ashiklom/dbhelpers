#' Merge data into SQL database
#'
#' @author Alexey Shiklomanov <ashiklom@bu.edu>
#' @param db dplyr \code{src_*} object
#' @param table Name of table, as character
#' @param values \code{data.frame}
#' @param by Character vector of column names to use for the merge
#' @param id_colname Column name for the database ID
#' @param backend Which backend to use for copy operation. Default is "db_insert_into".
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
                          backend = 'db_insert_into', ...) {
    supported_backends <- c('db_insert_into', 'psql_copy')
    if (!backend %in% supported_backends) {
        stop('Backend "', backend, '" not supported. ',
             'Please select one of the following: ', 
             paste(supported_backends, collapse = ', '))
    }

    sql_table <- dplyr::tbl(db, table)
    sql_cols <- dplyr::tbl_vars(sql_table)

    # Select only those columns that are in input
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
        input_sub <- dplyr::anti_join(input_sel, sql_keys)
    } else {
        message('Writing into empty table. All values will be used.')
        input_sub <- input_sel
    }
    n_added <- nrow(input_sub)
    if (n_added > 0) {
        if (backend == 'db_insert_into') {
            insert <- backend_db_insert_into(db, table, input_sub, id_colname, ...)
        } else if (backend == 'psql_copy') {
            insert <- backend_psql_copy(db, table, input_sub, id_colname, ...)
        }
        message('Added ', n_added, ' rows to table ', table)
    } else {
        message('All rows in input already in table ', table)
    }
    new_sql_keys <- dplyr::collect(dplyr::distinct_(sql_table, .dots = c(id_colname, by)))
    result <- dplyr::left_join(values, new_sql_keys)
    return(result)
}

backend_db_insert_into <- function(db, table, values, id_colname, 
                                   add_id = TRUE, update_seq = NULL) {

    if (is.null(update_seq) && inherits(db, 'src_postgres')) {
        update_seq <- TRUE
    }

    sql_table <- dplyr::tbl(db, table)
    sql_cols <- dplyr::tbl_vars(sql_table)
    if (isTRUE(add_id)) {
        last_id <- dplyr::tbl(db, dplyr::build_sql('SELECT', dplyr::ident(id_colname), 
                                                   'FROM', dplyr::ident(table),
                                                   'ORDER BY', dplyr::ident(id_colname),
                                                   'DESC LIMIT 1'))
        last_id <- dplyr::collect(last_id)
        if (nrow(last_id) == 0) {
            i <- 0
        } else {
            i <- unlist(last_id, use.names = FALSE)
        }
        values[[id_colname]] <- as.integer(i + (1:nrow(values)))
    }
    # Order the columns correctly and fill missing columns 
    # (required for `db_insert_into`)
    input_cols <- colnames(values)
    missing_cols <- sql_cols[!sql_cols %in% input_cols]
    if (length(missing_cols) > 0) {
        values[missing_cols] <- NA
    }
    values <- values[sql_cols]
    message('Inserting ', nrow(values), ' values into table "', table, '"...')
    dplyr::db_insert_into(con = db$con, table = table, values = values)

    # Update table serial sequence counter
    if (isTRUE(update_seq)) {
        qry <- dplyr::build_sql('SELECT pg_catalog.setval(pg_get_serial_sequence',
                                '(', dplyr::ident(table), ',', dplyr::ident(id_colname), '),',
                                '(SELECT MAX(', dplyr::ident(id_colname), ') FROM ', ident(table), ') + 1)')
        r <- RPostgreSQL::dbSendQuery(db$con, qry)
        dbClearResult(r)
    }

    return(TRUE)
}

backend_psql_copy <- function(db, table, values,
                              id_colname = 'id', add_id = TRUE, ...) {
    stopifnot(is.data.table(values))
    sql_cols <- RPostgreSQL::dbListFields(db$con, table)
    sql_cols <- sql_cols[!grepl('\\.\\.pg\\.dropped', sql_cols)]

    # Add ID column if missing
    if (isTRUE(add_id)) {
        last_id <- RPostgreSQL::dbGetQuery(db$con, paste('SELECT', id_colname, 
                                         'FROM', table,
                                         'ORDER BY', id_colname,
                                         'DESC LIMIT 1'))
        if (nrow(last_id) == 0) {
            i <- 0
        } else {
            i <- unlist(last_id, use.names = FALSE)
        }
        values <- values[, (id_colname) := as.integer(i+(1:.N))]
    }
    tmp_dir <- '/tmp'
    tmp_fname <- tempfile(pattern = 'dt2sql_', tmpdir=tmp_dir, fileext = '.csv')
    oldscipen <- options(scipen=500)
    message('Writing CSV file...')
    fwrite(values, tmp_fname, quote = TRUE)
    message('Performing pgsql copy operation...')
    cp <- system2('psql', c('-d', db$info$dbname, '-c', 
                            paste0('"', '\\copy ', table,
                                   '(', paste(colnames(values), collapse = ','), ')',
                                   ' FROM ', shQuote(tmp_fname), 
                                   ' WITH CSV HEADER', '"')), stdout = TRUE)
    # TODO: Check 'cp' object for errors and capture.
    message('Copy complete! Check stderr/stdout for errors.')
    file.remove(tmp_fname)

    # Update table serial sequence counter
    r <- RPostgreSQL::dbSendQuery(db$con, paste0("SELECT pg_catalog.setval(pg_get_serial_sequence",
                                                 "('", table,"', '", id_colname, "'),",
                                                 "(SELECT MAX(", id_colname, ") FROM ",table,")+1);"))
    dbClearResult(r)
    return(cp)
}

