import pandas as pd


def convert_currencies_to_usd(amount: float, currency: str) -> float:
    """
    Convert a given amount from the specified currency to USD using fixed exchange rates.

    Parameters:
    ------
    amount: (float)
        The amount to be converted.

    currency: (str)
        The currency of the amount.

    Returns:
    ------
    USD: (float)
        The converted amount in USD.
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


def split_df_by_start_year(df: pd.DataFrame) -> dict:
    """
    Split the DataFrame based on the start year.

    Parameters:
    -------
    df: (DataFrame)
        dataframe to split.

    Returns:
    ------
    year_dfs: (dict)
        A dictionary where
        - keys: years,
        - values: DataFrames containing rows with as per
                  the start year(keys's year)
    """
    # Convert 'start_date' to datetime
    df["startdate"] = pd.to_datetime(df["startdate"])

    # Group the DataFrame by start year
    year_dfs = {year: group for year, group in df.groupby(df["startdate"].dt.year)}
    return year_dfs


def split_dfs_by_outcome(yearly_dfs: pd.DataFrame) -> dict:
    """
    Split the dataframes based on the 'outcome' column.

    Parameters:
    ------
    yearly_dfs: (dict)
        A dictionary where keys are years and values are dataframes.

    Returns:
    ------
    outcome_dfs: (dict)
        A dictionary where
        - keys: years,
        - values: classified dataframes as per the outcomes
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


def perform_data_transformations(crowdfunding_df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs data transformations on the crowdfunding DataFrame.

    Parameters:
    -------
    crowdfunding_df: (pandas.DataFrame)
        DataFrame containing crowdfunding data.

    Returns:
    ------
    (pandas.DataFrame): Transformed DataFrame.

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


def merge_contact_info(crowdfunding_df: pd.DataFrame, contact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges contact information with the crowdfunding DataFrame.

    Parameters:
    -------
    crowdfunding_df: (pandas.DataFrame)
        DataFrame containing crowdfunding data.

    contact_df: (pandas.DataFrame)
        DataFrame containing contact information.

    Returns:
    (pandas.DataFrame): Merged DataFrame.

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
