backend_pg_bulkload <- function(db, table, values,
                                id_colname = NULL, add_id = TRUE, ...) {
    stopifnot(inherits(db, 'src_postgres'))
    if (is.null(id_colname)) {
        add_id = FALSE
    }
    if (add_id) {
        values <- add_id(db = db, table = table, values = values, 
                         id_colname = id_colname)
    }
    values <- fill_cols(db = db, table = table, values = values)
    tmp_dir <- '/tmp'
    tmp_fname <- write_tmp_file(values = values)
    tmp_ctl <- tempfile(pattern = 'csvctl_', tmpdir=tmp_dir, fileext = '.ctl') 
    ctlfile <- c(paste0('OUTPUT = ', table),
                 'TYPE = CSV',
                 'DELIMITER = ","')
    write(x = ctlfile, file = tmp_ctl, ncolumns = 1, append = FALSE)
    message('Using the following ctl file...')
    print(readLines(tmp_ctl))
    message('Performing pg_bulkload operation...')
    file.remove(tmp_fname)
    file.remove(tmp_ctl)
    cp <- system2('pg_bulkload', c('-d ', db$info$dbname, tmp_ctl, '-i stdin'), 
                  stdin = tmp_fname)
    if (cp != 0) {
        stop('Error in pg_bulkload operation')
    }
    # TODO: Check 'cp' object for errors and capture.
    message('Copy complete! Check stderr/stdout for errors.')

    # Update table serial sequence counter
    r <- update_psql_counter(db = db, table = table, id_colname = id_colname)
    return(cp)
}

