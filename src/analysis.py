import psycopg2 as dbapi

DBNAME = "news"


def get_core_connection():
    """
    Retrieve a connection to the database.
    """
    return dbapi.connect(database=DBNAME)


def perform_query(query):
    """
    Performs a SQL query.
    Returns the result immediately.
    """
    # Get a connection to the DB.
    db_connection = get_core_connection()

    # Get a cursor.
    cursor = db_connection.cursor()

    # Perform the query.
    cursor.execute(query)

    # Store the result
    result = cursor.fetchall()

    # Always close the connection when finished talking to the db.
    db_connection.close()
    return result


def print_results(title, result):
    """
    Print a title and results line by line.
    """
    print(title)
    for record in result:
        print("\t• %s — %s views" % (record[0], "{:,}".format(record[1])))
    print("\n")


def print_error_percentage(title, result):
    """
    Print a title and results as day and percentage.
    """
    print(title)
    for record in result:
        print("\t• %s — %.2f%% errors" % (record[0], float(record[1])))
    print("\n")


def top_three_articles():
    """
    Prints the top three articles in descending order.
    """
    # Declare query.
    query = (
        "SELECT articles.title, COUNT(*) AS num FROM articles "
        "INNER JOIN log ON log.path LIKE CONCAT('%', articles.slug, '%') "
        "AND log.method LIKE '%GET%' "
        "AND log.status LIKE '%200%' "
        "GROUP BY articles.title "
        "ORDER BY num DESC "
        "LIMIT 3 "
    )

    # Run the query.
    result = perform_query(query)

    # Print the result.
    print_results(
        "1. What are the most popular three articles of all time?:", result)


def popular_authors():
    """
    Prints popular authors in descending order.
    """
    # Declare query.
    query = (
        "SELECT authors.name, COUNT(*) as num FROM articles "
        "INNER JOIN log on log.path LIKE CONCAT('%', articles.slug,'%') "
        "INNER JOIN authors ON articles.author = authors.id "
        "AND log.status LIKE '%200%' "
        "GROUP BY authors.name "
        "ORDER BY num DESC "
    )

    # Run the query.
    result = perform_query(query)

    # Print the result.
    print_results("2. Who are the most popular authors of all time?:", result)


def days_with_errors():
    """
    Print which days had >1% of errors.
    """

    # Declare query. The final table should have a day and percentage column.
    # We need (Non-200 requests) / (Total number of requests) PER DAY and
    # then select the ones where the percentage is more than 1.00

    # Going to update this to use
    # <https://www.postgresql.org/docs/9.1/queries-with.html> WITH clauses.
    # Makes it more readable and much faster than my previous query.
    query = (
        # First, lets get the non-200 requests,
        # make sure to convert from timestamp to date with '::'
        # or CAST from <https://www.postgresql.org/docs/7.3/sql-syntax.html>
        "WITH badrequests AS ("
        "SELECT time::date, count(*) AS size "
        "FROM log "
        "WHERE status NOT LIKE '%200%' "
        "GROUP BY time::date "
        "ORDER BY time::date DESC"
        "),"
        # Next, lets get the total number of requests
        " totalrequests AS ("
        "SELECT time::date, count(*) AS size "
        "FROM log "
        "GROUP BY time::date "
        "ORDER BY time::date DESC"
        "),"
        # Last, lets make our result table, one column as the time (or day)
        # and one column as the percentage.
        " resulttable AS ("
        "SELECT totalrequests.time, "
        "badrequests.size::float / totalrequests.size::float * 100"
        "AS percentage "
        "FROM badrequests, totalrequests "
        "WHERE badrequests.time = totalrequests.time"
        ") "
        "SELECT * from resulttable WHERE percentage > 1"
    )

    # Run the query.
    result = perform_query(query)

    # Print the result.
    print_error_percentage(
        "3. On which days did more than 1% of requests lead to errors?",
        result)


# If this file is run w/ python from terminal
if __name__ == "__main__":
    top_three_articles()
    popular_authors()
    days_with_errors()
