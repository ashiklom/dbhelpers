write_tmp_file <- function(values, tmp_dir = '/tmp', col.names = FALSE, ...) {
    stopifnot(data.table::is.data.table(values))
    dir.create(path = tmp_dir, showWarnings = FALSE, recursive = TRUE)
    tmp_fname <- tempfile(pattern = 'dt2sql_', tmpdir=tmp_dir, fileext = '.csv')
    oldscipen <- options(scipen=500)
    message('Writing CSV file...')
    data.table::fwrite(values, tmp_fname, quote = TRUE, col.names = col.names, ...)
    return(tmp_fname)
}
