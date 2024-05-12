from services.data_extraction import (
    execute_sql_in_redshift,
    load_data_from_s3_to_redshift,
)
from services.data_load import store_data_athena_tables, store_dfs_in_s3
from services.data_transformation import (
    merge_contact_info,
    perform_data_transformations,
    split_df_by_start_year,
    split_dfs_by_outcome,
)

if __name__ == "__main__":
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

    # Executing the sql query to create a table in redshift
    # Perform only once if there is no such table in redshift
    execute_sql_in_redshift(crowdfunding_table_query)
    execute_sql_in_redshift(contact_table_query)

    # Load dataframe from redshift to dataframe
    crowdfunding_df = load_data_from_s3_to_redshift(crowdfunding_table_name, s3_location)
    contact_df = load_data_from_s3_to_redshift(contact_table_name, s3_location)

    # Merge contact information with crowdfunding data
    crowdfunding_df = merge_contact_info(crowdfunding_df, contact_df)

    # Perform data transformations
    crowdfunding_df = perform_data_transformations(crowdfunding_df)

    # Split the DataFrame by start year
    yearly_dfs = split_df_by_start_year(crowdfunding_df)

    # Split each DataFrame in yearly_dfs by outcome
    outcome_dfs = split_dfs_by_outcome(yearly_dfs)

    # store transform data in s3
    store_dfs_in_s3(outcome_dfs, "cleaned-data-40")

    # store data in athena table
    store_data_athena_tables("s3://cleaned-data-40/")
