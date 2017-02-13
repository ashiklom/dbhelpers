backend_sqlite_import <- function(db, table, values, id_colname, add_id = TRUE) {
    stopifnot(inherits(db, 'src_sqlite'))
    if (add_id) {
        values <- add_id(db = db, table = table, values = values, 
                         id_colname = id_colname)
    }
    values <- fill_cols(db = db, table = table, values = values)
    tmp_dir <- '/tmp'
    tmp_fname <- write_tmp_file(values = values, col.names = FALSE)
    import_statement <- c('PRAGMA foreign_keys = on;',
                          '.mode csv',
                          paste('.import', shQuote(tmp_fname), shQuote(table)))
    import_filename <- tempfile(pattern = 'sqlite_statement_', tmpdir = tmp_dir, fileext = '.sql')
    write(x = import_statement, file = import_filename, ncolumns = 1, append = FALSE)
    message('Running sqlite3 import command')
    import_exec <- system2('sqlite3', c(db$path), stdin = import_filename)
    file.remove(tmp_fname)
    if (import_exec != 0) {
        stop('Error in sqlite import operation')
    }
    return(TRUE)
}
