backend_psql_copy <- function(db, table, values,
                              id_colname = 'id', add_id = TRUE, ...) {
    stopifnot(inherits(db, 'src_postgres'))
    if (add_id) {
        values <- add_id(db = db, table = table, values = values, 
                         id_colname = id_colname)
    }
    tmp_fname <- write_tmp_file(values = values)

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
    r <- update_psql_counter(db = db, table = table, id_colname = id_colname)
    return(cp)
}

