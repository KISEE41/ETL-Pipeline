import awswrangler as wr
import pandas as pd

from services.aws_services import execute_sql_in_redshift, redshift_connection


def load_data_from_s3_to_redshift(table_name: str, s3_location: str) -> pd.DataFrame:
    """
    Copies data from a CSV file located in an S3 bucket to a table in Amazon Redshift,
    then reads the data from the Redshift table into a pandas DataFrame.

    Parameters:
    ------
    table_name: (str)
         The name of the table to which data will be copied in Redshift.

    s3_location: (str)
        The S3 bucket location where the CSV file is stored.

    database: (str)
        The name of the Redshift database to connect to.

    Returns
    ------
        (pandas.DataFrame): A DataFrame containing the data loaded from the Redshift table.
    """
    # Intialise the redshift connection
    con = redshift_connection

    # SQL query to copy data from csv file to table in Redshift
    copy_data_query = f"""
            COPY public.{table_name}
            FROM '{s3_location}/{table_name}.csv'
            IAM_ROLE 'arn:aws:iam::658349184098:role/service-role/AmazonRedshift-CommandsAccessRole-20240219T164815'
            CSV;
        """

    # SQL query to load data from the table in Redshift
    load_data_query = f"SELECT * FROM public.{table_name};"

    # Execute the copy data query
    execute_sql_in_redshift(copy_data_query)

    # Read data from the table into a DataFrame
    df = wr.redshift.read_sql_query(load_data_query, con=con)

    return df
