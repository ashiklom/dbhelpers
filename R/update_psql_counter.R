update_psql_counter <- function(db, table, id_colname) {
    RPostgreSQL::dbGetQuery(db$con, paste0("SELECT pg_catalog.setval(pg_get_serial_sequence",
                                           "('", table,"', '", id_colname, "'),",
                                           "(SELECT MAX(", id_colname, ") FROM ",table,")+1);"))
}
