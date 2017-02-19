#' Update SQL table based on criteria
#'
#' Function to facilitate updating SQL database.
#'
#' Each item in criteria should be a length 3 list containing:
#'  * The column name, as a character
#'  * The operator (e.g. '=', '<'), as a string
#'  * The value, as the appropriate type
#'
#' @param criteria (list of lists) See details.
#' @param values Named list of values. See details
#' @inherit db_merge_into params
#' @export
db_update_where <- function(db, table, criteria, values) {
    warning('Experimental! May not work as advertised, or at all...')
    where_list <- lapply(criteria, function(x) 
                         dplyr::sql_vector(c(dplyr::ident(x[[1]]),
                                             dplyr::sql(x[[2]]),
                                             dplyr::escape(x[[3]])),
                                           parens = FALSE))
    where_query <- dplyr::sql_vector(where_list, parens = FALSE, collapse = sql(' AND '))
    query <- dplyr::build_sql(
        dplyr::sql('UPDATE '), dplyr::ident(table),
        dplyr::sql(' SET ('), dplyr::ident(names(values)), 
        dplyr::sql(') = ('), dplyr::escape(unlist(values, use.names = FALSE)),
        dplyr::sql(') WHERE '), where_query)
    runquery <- DBI::dbExecute(db$con, query) 
}
