backend_pg_bulkload <- function(db, table, values,
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
    # Align all columns in `values` with SQL
    missing_cols <- sql_cols[!sql_cols %in% colnames(values)]
    for (m in missing_cols) {
        values[[m]] <- NA
    }
    setcolorder(values, sql_cols)

    tmp_dir <- '/tmp'
    dir.create(path = tmp_dir, showWarnings = FALSE, recursive = TRUE)
    tmp_fname <- tempfile(pattern = 'dt2sql_', tmpdir=tmp_dir, fileext = '.csv')
    oldscipen <- options(scipen=500)
    message('Writing CSV file...')
    fwrite(values, tmp_fname, quote = TRUE, col.names = FALSE)
    tmp_ctl <- tempfile(pattern = 'csvctl_', tmpdir=tmp_dir, fileext = '.ctl') 
    ctlfile <- c(paste0('OUTPUT = ', table),
                 'TYPE = CSV',
                 'DELIMITER = ","')
    write(x = ctlfile, file = tmp_ctl, ncolumns = 1, append = TRUE)
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
    r <- RPostgreSQL::dbGetQuery(db$con, paste0("SELECT pg_catalog.setval(pg_get_serial_sequence",
                                                 "('", table,"', '", id_colname, "'),",
                                                 "(SELECT MAX(", id_colname, ") FROM ",table,")+1);"))
    return(cp)
}

