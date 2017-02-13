fill_cols <- function(db, table, values) {
        sql_cols <- DBI::dbListFields(db$con, table)
        sql_cols <- sql_cols[!grepl('\\.\\.pg\\.dropped', sql_cols)]
        missing_cols <- sql_cols[!sql_cols %in% colnames(values)]
        for (m in missing_cols) {
            values[[m]] <- NA
        }
        data.table::setcolorder(values, sql_cols)
        return(values)
}
