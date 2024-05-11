# import libraries
import os
from io import BytesIO

import awswrangler as wr
import boto3
import pandas as pd

# -----------------------------------DATA EXTRACTION FUNCTIONS-------------------#


def execute_sql_in_redshift(sql_query):
    """
    Connects to an Amazon Redshift cluster, executes the provided SQL query,
    commits the transaction, and then closes the connection.

    Parameters:
    - sql_query (str): The SQL query to be executed in the Redshift cluster.

    Returns:
    - None

    """
    con = wr.redshift.connect("Redshift connection")
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


def load_data_from_s3_to_redshift(table_name, s3_location):
    """
    Copies data from a CSV file located in an S3 bucket to a table in Amazon Redshift,
    then reads the data from the Redshift table into a pandas DataFrame.

    Parameters:
    - table_name (str): The name of the table to which data will be copied in Redshift.
    - s3_location (str): The S3 bucket location where the CSV file is stored.
    - database (str): The name of the Redshift database to connect to.

    Returns:
    - pandas.DataFrame: A DataFrame containing the data loaded from the Redshift table.
    """
    # Intialise the redshift connection
    con = wr.redshift.connect("Redshift connection")

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


# --------------------DATA CLEANING AND TRANSFORMATION FUNCTIONS------------------#


def convert_currencies_to_usd(amount, currency):
    """
    Convert a given amount from the specified currency to USD using fixed exchange rates.

    Parameters:
    - amount (float): The amount to be converted.
    - currency (str): The currency of the amount.

    Returns:
    - USD (float): The converted amount in USD.
    """
    # Define fixed exchange rates for each currency to USD
    exchange_rates = {
        "CAD": 0.73,  # 1 CAD = 0.73 USD
        "USD": 1.0,  # 1 USD = 1.0 USD
        "AUD": 0.66,  # 1 AUD = 0.74 USD
        "DKK": 0.14,  # 1 DKK = 0.16 USD
        "GBP": 1.26,  # 1 GBP = 1.32 USD
        "CHF": 1.11,  # 1 CHF = 1.09 USD
        "EUR": 1.08,  # 1 EUR = 1.18 USD
    }

    # Convert amount to USD using the exchange rate
    if currency in exchange_rates:
        return amount * exchange_rates[currency]
    else:
        print(f"Unsupported currency: {currency}. Cannot convert to USD.")
        return None


def split_df_by_start_year(df):
    """
    Split the DataFrame based on the start year.

    Parameters:
    - df (DataFrame): dataframe to split.

    Returns:
    - year_dfs (dict): A dictionary where -keys:years,
    -values: DataFrames containing rows with as per the start year(keys's year)
    """
    # Convert 'start_date' to datetime
    df["startdate"] = pd.to_datetime(df["startdate"])

    # Group the DataFrame by start year
    year_dfs = {year: group for year, group in df.groupby(df["startdate"].dt.year)}
    return year_dfs


def split_dfs_by_outcome(yearly_dfs):
    """
    Split the dataframes based on the 'outcome' column.

    Parameters:
    - yearly_dfs (dict): A dictionary where keys are years and values are dataframes.

    Returns:
    - outcome_dfs (dict): A dictionary where -keys:years,
                                           -values: classified dataframes as per the outcomes
    """
    outcome_dfs = {}
    for year, year_df in yearly_dfs.items():
        # Split the DataFrame based on 'outcome' column
        successful_df = year_df[year_df["outcome"] == "successful"]
        failed_df = year_df[year_df["outcome"] == "failed"]

        # Drop the 'outcome' column
        successful_df = successful_df.drop(columns=["outcome"])
        failed_df = failed_df.drop(columns=["outcome"])

        # Store the DataFrames for successful and failed outcomes in a dictionary
        outcome_dfs[f"{year}_successful"] = successful_df
        outcome_dfs[f"{year}_failed"] = failed_df

    return outcome_dfs


def perform_data_transformations(crowdfunding_df):
    """
    Performs data transformations on the crowdfunding DataFrame.

    Parameters:
    - crowdfunding_df (pandas.DataFrame): DataFrame containing crowdfunding data.

    Returns:
    - pandas.DataFrame: Transformed DataFrame.

    """
    # Convert currency to USD
    crowdfunding_df["goal_usd"] = crowdfunding_df.apply(
        lambda row: convert_currencies_to_usd(row["goal"], row["currency"]), axis=1
    )
    crowdfunding_df["pledged_usd"] = crowdfunding_df.apply(
        lambda row: convert_currencies_to_usd(row["pledged"], row["currency"]), axis=1
    )

    # Split 'category & sub-category' column into two separate columns
    crowdfunding_df[["category", "subcategory"]] = crowdfunding_df["category_subcategory"].str.split("/", expand=True)

    # Drop unnecessary columns
    crowdfunding_df.drop(
        [
            "goal",
            "pledged",
            "currency",
            "category_subcategory",
            "staff_pick",
            "spotlight",
        ],
        axis=1,
        inplace=True,
    )

    # Rearrange column order
    crowdfunding_df = crowdfunding_df[
        [
            "company_name",
            "blurb",
            "category",
            "subcategory",
            "ownername",
            "email",
            "country",
            "outcome",
            "goal_usd",
            "pledged_usd",
            "backers_count",
            "start_date",
            "end_date",
        ]
    ]

    # Rename columns with underscores
    crowdfunding_df.columns = crowdfunding_df.columns.str.replace("_", "")

    return crowdfunding_df


def merge_contact_info(crowdfunding_df, contact_df):
    """
    Merges contact information with the crowdfunding DataFrame.

    Parameters:
    - crowdfunding_df (pandas.DataFrame): DataFrame containing crowdfunding data.
    - contact_df (pandas.DataFrame): DataFrame containing contact information.

    Returns:
    - pandas.DataFrame: Merged DataFrame.

    """
    # Merge contact information with crowdfunding data
    crowdfunding_df = pd.merge(
        crowdfunding_df,
        contact_df[["contact_id", "first_name", "last_name", "email"]],
        on="contact_id",
        how="left",
    )

    # Combine first name and last name into 'ownername'
    crowdfunding_df["ownername"] = crowdfunding_df["first_name"].str.cat(crowdfunding_df["last_name"], sep=" ")

    # Drop unnecessary columns
    crowdfunding_df.drop(columns=["cf_id", "contact_id", "first_name", "last_name"], inplace=True)

    return crowdfunding_df


# -----------------------------------DATA LOAD FUNCTIONS -----------------------------------#


def store_dfs_in_s3(outcome_dfs, bucket_name):
    """
    Store the DataFrames in the outcome_dfs dictionary as Excel files and upload them to Amazon S3.

    Parameters:
    - outcome_dfs (dict):
            A dictionary where keys are tuples (year, outcome) and values are DataFrames.
    - bucket_name (str):
            The name of the S3 bucket.

    Returns:
    - None
    """
    s3 = boto3.client("s3")

    for key, df in outcome_dfs.items():
        # create folder name
        folder_name = f"{key}"
        # Assign filename
        filename = f"{key}.csv"

        # Convert DataFrame to Excel bytes
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Upload file to S3
        s3.put_object(Bucket=bucket_name, Key=f"{folder_name}/{filename}", Body=csv_buffer)
        print(f"{filename} uploaded")


def store_data_athena_tables(s3_location):
    """
    Create separate external tables in Athena for each CSV file in the S3 location.

    Parameters:
    - s3_location (str): The S3 location where CSV files are stored.

    Returns:
    - None
    """
    athena = boto3.client("athena")
    s3 = boto3.client("s3")

    # Extract bucket and prefix from S3 location
    bucket_name = s3_location.split("/")[2]
    prefix = "/".join(s3_location.split("/")[3:])

    # Create database if not exists
    create_database_query = "CREATE DATABASE IF NOT EXISTS crowdfunding_database;"
    athena.start_query_execution(
        QueryString=create_database_query,
        ResultConfiguration={"OutputLocation": "s3://athena-db-40/"},
    )

    # List all folders in the S3 location
    folders = set()
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
    for obj in response.get("CommonPrefixes", []):
        folders.add(obj.get("Prefix"))

    for folder in folders:
        # List all CSV files in the folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        for file in response["Contents"]:
            if file["Key"].endswith(".csv"):
                # Extract table name from file name
                table_name = os.path.splitext(os.path.basename(file["Key"]))[0]

                # SQL statement to create the table
                create_table_query = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS `crowdfunding_database`.`{table_name}` (
                    `company_name` string,
                    `blurb` string,
                    `category` string,
                    `subcategory` string,
                    `ownername` string,
                    `email` string,
                    `country` string,
                    `goalusd` float,
                    `pledgeusd` float,
                    `backerscount` int,
                    `startdate` timestamp,
                    `enddate` string
                    )
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                    LOCATION 's3://cleaned-data-40/{folder}'
                    TBLPROPERTIES ('classification' = 'parquet');
                """

                # Run the create table query
                athena.start_query_execution(
                    QueryString=create_table_query,
                    ResultConfiguration={"OutputLocation": "s3://athena-db-40/"},
                )


# -------------------------------------------------------------------------------------------------#


def main():

    # Initalise parameters
    s3_location = "s3://raw-data-40/"
    crowdfunding_table_name = "crowdfunding"
    contact_table_name = "contact"

    # Define sql query to create table
    crowdfunding_table_query = """
                            CREATE TABLE crowdfunding(
                                cf_id VARCHAR(100),
                                contact_id VARCHAR(100),
                                company_name VARCHAR(100),
                                blurb VARCHAR(MAX),
                                goal VARCHAR(100),
                                pledged VARCHAR(100),
                                outcome VARCHAR(50),
                                backers_count VARCHAR(100),
                                country VARCHAR(50),
                                currency VARCHAR(10),
                                launched_at VARCHAR(50),
                                deadline VARCHAR(50),
                                staff_pick VARCHAR(100),
                                spotlight VARCHAR(100),
                                category_subcategory VARCHAR(100)
                            );
                        """

    contact_table_query = """
                            CREATE TABLE contact (
                                contact_id VARCHAR(100),
                                first_name VARCHAR(100),
                                last_name VARCHAR(100),
                                email VARCHAR(100)
                            );
                        """

    # -----------------------DATA EXTRACTION ----------------------#

    execute_sql_in_redshift(crowdfunding_table_query)
    execute_sql_in_redshift(contact_table_query)

    # Load dataframe from redshift to dataframe
    crowdfunding_df = load_data_from_s3_to_redshift(crowdfunding_table_name, s3_location)
    contact_df = load_data_from_s3_to_redshift(contact_table_name, s3_location)

    # -----------------------DATA CLEANING AND TRANSFORMATION------------------------#

    # Merge contact information with crowdfunding data
    crowdfunding_df = merge_contact_info(crowdfunding_df, contact_df)

    # Perform data transformations
    crowdfunding_df = perform_data_transformations(crowdfunding_df)

    # Split the DataFrame by start year
    yearly_dfs = split_df_by_start_year(crowdfunding_df)

    # Split each DataFrame in yearly_dfs by outcome
    outcome_dfs = split_dfs_by_outcome(yearly_dfs)

    # -------------------------DATA LOAD-----------------------#

    # store transform data in s3
    store_dfs_in_s3(outcome_dfs, "cleaned-data-40")

    # store data in athena table
    store_data_athena_tables("s3://cleaned-data-40/")


if __name__ == "__main__":
    main()
