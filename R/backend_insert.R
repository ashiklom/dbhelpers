backend_insert <- function(db, table, values, id_colname = NULL, 
                                   add_id = TRUE, update_seq = NULL) {

    if (is.null(update_seq) && inherits(db$con, 'PostgreSQLConnection')) {
        update_seq <- TRUE
    }

    sql_table <- dplyr::tbl(db, table)
    sql_cols <- dplyr::tbl_vars(sql_table)

    if (is.null(id_colname)) {
        add_id <- FALSE
    }
    
    if (isTRUE(add_id)) {
        last_id <- dplyr::tbl(db, dbplyr::build_sql('SELECT', dplyr::ident(id_colname), 
                                                    'FROM', dplyr::ident(table),
                                                    'ORDER BY', dplyr::ident(id_colname),
                                                    'DESC LIMIT 1'))
        last_id <- dplyr::collect(last_id)
        if (nrow(last_id) == 0) {
            i <- 0
        } else {
            i <- unlist(last_id, use.names = FALSE)
        }
        values[[id_colname]] <- i + seq_len(nrow(values))
    }
    input_cols <- colnames(values)
    escaped_values <- lapply(values, dbplyr::escape, collapse = NULL, parens = FALSE, con = db$con)
    values_matrix <- matrix(unlist(escaped_values, use.names = FALSE), nrow = nrow(values))
    rows <- apply(values_matrix, 1, paste0, collapse = ', ')
    input_values <- paste0('(', rows, ')', collapse = '\n, ')
    insert_query <- dbplyr::build_sql(dplyr::sql('INSERT INTO '), dplyr::ident(table),
                                      dplyr::sql(' ('), dplyr::ident(input_cols), dplyr::sql(') '),
                                      dplyr::sql(' VALUES '), dplyr::sql(input_values))

    message('Inserting ', nrow(values), ' values into table "', table, '"...')
    r <- DBI::dbGetQuery(db$con, insert_query)

    # Update table serial sequence counter
    if (isTRUE(update_seq)) {
        r <- update_psql_counter(db = db, table = table, id_colname = id_colname)
    }

    return(TRUE)
}
