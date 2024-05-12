import os
from io import BytesIO

import boto3

# Creation of S3 client.
s3 = boto3.client("s3")

# Creation of athena client
athena = boto3.client("athena")


def store_dfs_in_s3(outcome_dfs: dict, bucket_name: str) -> None:
    """
    Store the DataFrames in the outcome_dfs dictionary as Excel files and upload them to Amazon S3.

    Parameters:
    ------
    outcome_dfs: (dict)
        A dictionary where keys are tuples (year, outcome) and values are DataFrames.

    bucket_name: (str)
        The name of the S3 bucket.

    Returns:
    ------
        None
    """
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


def store_data_athena_tables(s3_location: str) -> None:
    """
    Create separate external tables in Athena for each CSV file in the S3 location.

    Parameters:
    ------
        s3_location: (str)
            The S3 location where CSV files are stored.

    Returns:
    ------
        None
    """
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
