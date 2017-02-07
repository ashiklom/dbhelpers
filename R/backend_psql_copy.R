backend_psql_copy <- function(db, table, values,
                              id_colname = 'id', add_id = TRUE, ...) {
    stopifnot(is.data.table(values))
    stopifnot(inherits(db, 'src_postgres'))
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
        values[[id_colname]] <- as.integer(i + (1:nrow(values)))
    }
    tmp_dir <- '/tmp'
    tmp_fname <- tempfile(pattern = 'dt2sql_', tmpdir=tmp_dir, fileext = '.csv')
    oldscipen <- options(scipen=500)
    message('Writing CSV file...')
    fwrite(values, tmp_fname, quote = TRUE)
    message('Performing pgsql copy operation...')
    cp_query <- paste0('COPY ', table,
                       '(', paste(colnames(values), collapse = ','), ')',
                       ' FROM STDIN WITH CSV HEADER')
    cp <- system2('psql', c('-d', db$info$dbname, '-c', shQuote(cp_query)),
                  stdin = tmp_fname)
    file.remove(tmp_fname)
    if (cp != 0) {
        stop('Error in PSQL copy operation')
    }
#    cp <- system2('psql', c('-d', db$info$dbname, '-c', 
#                            paste0('"', '\\copy ', table,
#                                   '(', paste(colnames(values), collapse = ','), ')',
#                                   ' FROM ', shQuote(tmp_fname), 
#                                   ' WITH CSV HEADER', '"')), stdout = TRUE)
    message('Copy complete! Check stderr/stdout for errors.')

    # Update table serial sequence counter
    r <- RPostgreSQL::dbGetQuery(db$con, paste0("SELECT pg_catalog.setval(pg_get_serial_sequence",
                                                 "('", table,"', '", id_colname, "'),",
                                                 "(SELECT MAX(", id_colname, ") FROM ",table,")+1);"))
    #dbClearResult(r)
    return(cp)
}

