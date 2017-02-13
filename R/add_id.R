add_id <- function(db, table, values, id_colname) {
    last_id <- DBI::dbGetQuery(db$con, paste('SELECT', id_colname, 
                                             'FROM', table,
                                             'ORDER BY', id_colname,
                                             'DESC LIMIT 1'))
    if (nrow(last_id) == 0) {
        i <- 0
    } else {
        i <- unlist(last_id, use.names = FALSE)
    }
    values[[id_colname]] <- as.integer(i + (1:nrow(values)))
    return(values)
}
