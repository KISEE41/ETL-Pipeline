import awswrangler as wr


def redshift_connection() -> wr.redshift.connect:
    """
    Establishes a connection to the Amazon Redshift cluster.

    Returns:
    ------
    con: (redshift connection)
        Redshift connection object.
    """
    try:
        con = wr.redshift.connect("Redshift connection")
        return con
    except Exception as e:
        print("Error connecting to Redshift:", e)
        return


def execute_sql_in_redshift(sql_query: str) -> None:
    """
    Connects to an Amazon Redshift cluster, executes the provided SQL query,
    commits the transaction, and then closes the connection.

    Parameters:
    ------
    sql_query: (str)
        The SQL query to be executed in the Redshift cluster.

    Returns:
    ------
        None
    """
    # initializing the redshift connection
    con = redshift_connection()
    if con:
        try:
            # Create a cursor object using the connection
            cursor = con.cursor()

            # Execute the create table query
            cursor.execute(sql_query)

            # Commit the transaction
            con.commit()

            # Close the cursor
            cursor.close()

            # Close the connection
            con.close()

        except Exception as e:
            print("Error executing SQL query:", e)
    else:
        print("Connection to Redshift failed. SQL query not executed.")
