import psycopg2 as dbapi
import datetime

DBNAME = "news"

'''
Retrieve a connection to the database. 
'''
def get_core_connection():
    return dbapi.connect(database=DBNAME)

'''
Performs a SQL query. 
Returns the result immediately.
'''
def perform_query(query):
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

'''
Print a title and results line by line.
'''
def print_results(title, result):
    print(title)
    for record in result:
        print("\t• %s — %s views" % (record[0], "{:,}".format(record[1])));
    print("\n")

'''
Print a title and results as day and percentage.
'''
def print_error_percentage(title, result):
    print(title)
    for record in result:
        print("\t• %s — %s errors" % (record[0], "{:,}%".format(record[1])));
    print("\n")

'''
Prints the top three articles in descending order.
'''
def top_three_articles():
    # Declare query.
    query = (
        "SELECT articles.title, COUNT(*) AS num FROM articles " 
        "INNER JOIN log ON log.path LIKE CONCAT('%', articles.slug, '%') "
        "AND log.method = 'GET' "
        "AND log.status = '200 OK' "
        "GROUP BY articles.title "
        "ORDER BY num DESC "
        "LIMIT 3 "
    )

    # Run the query.
    result = perform_query(query)
    
    # Print the result.
    print_results("1. What are the most popular three articles of all time?:", result)

'''
Prints popular authors in descending order.
'''
def popular_authors():
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

'''
Print which days had >1% of errors.
'''
def days_with_errors():
    # Declare query. The final table should have a day and percentage column.
    query = (
        # Select the day and percentage fields from the final table
        "SELECT day, percentage FROM ("
            # Select the day field, and the rounded result of this division from 
            "SELECT day, ROUND("
                # Sum all of the http requests and divide by the total number of requests on that day. Multiply by 100 so we can display it pretty later.
                "(SUM(http_requests)/(SELECT COUNT(*) FROM log WHERE SUBSTRING(CAST(log.time AS TEXT), 0, 11) = day) * 100)," 
                "2"
            ") AS percentage " # Declare the result of this query as a percentage.
            # Need to get all of HTTP requests for use in sum above.
            "FROM ("
                # Select the day, and aggregate and call the result http_request. Only aggregate bad requests.
                "SELECT SUBSTRING(CAST(log.time AS TEXT), 0, 11) as day, COUNT(*) AS http_requests FROM log "
                "WHERE status LIKE '%404%' "
                "GROUP BY day"
            ") AS log_percentage "
            "GROUP BY day "
            "ORDER BY percentage DESC"
        ") "
        "AS my_query "
        # Finally, only show days with more than 1%
        "WHERE percentage >= 1.00"
    )

    # Run the query.
    result = perform_query(query)

    # Print the result.
    print_error_percentage("3. On which days did more than 1% of requests lead to errors?", result)
    

# If this file is run w/ python from terminal
if __name__ == "__main__":
    top_three_articles()
    popular_authors()
    days_with_errors()