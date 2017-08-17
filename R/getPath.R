#' @export
getPath <- function(db) {
    if (inherits(db, 'src_sql')) {
        db2 <- db$con
    } else {
        db2 <- db
    }
    stopifnot(inherits(db2, 'SQLiteConnection'))
    txt <- capture.output(DBI::show(db2))
    path_rxp <- '(^ +Path: +)(.*)'
    path_string <- grep('^ +Path: .*', txt, value = TRUE)
    path <- gsub(path_rxp, '\\2', path_string)
    return(path)
}
