from rapidfuzz import fuzz, process
import pandas as pd
from joblib import Parallel, delayed
import numpy as np
import time


def fuzzy_match_name(name, choices, threshold=70):
    """
    Function to compare company names of customers and return a match if similar enough
    a
    Parameters:
        name (str): the name to be matched
        choices (list): A list of possible matches
        threshold (int): The minimum score needed for a match to be valid

    Returns:
        stuple: containing best match with score
    """
    if not choices:
        return None, None
    
    match = process.extractOne(name, choices)
    if match:
        best_match, score, _ = match
        if score >= threshold:
            return best_match, score
    return None, None


def match_row(row, source_names, source_name_to_number, threshold):
    """
    This function does the actual fuzzy matching maching process

    Parameters:
        row (pandas.Series): A row from a DataFrame containing 'Customer_Number' and 'Customer_Name'.
        source_names (list): A list of source customer names to match against.
        source_name_to_number (dict): A dictionary mapping source names to their corresponding numbers.
        threshold (float): The minimum similarity score required for a fuzzy match.

    Returns:
        str or int: The matched customer number if a match is found, or the original Customer_Number
            if no match is found or if Customer_Number is not null.
    """
    if pd.isnull(row['Customer_Number']):
        matched_name, matched_score = fuzzy_match_name(row['Customer_Name'], source_names, threshold)
        if matched_name is not None:
            return source_name_to_number.get(matched_name)
    return row['Customer_Number']


def update_customer_info(source_data_df, cleaned_df, threshold=65):
    """
    Update customer information in the cleaned DataFrame using fuzzy matching with source data.

    Parameters:
        source_data_df (pandas.DataFrame): DataFrame containing source customer data with 'Customer_Name' and 'Customer_Number' columns.
        cleaned_df (pandas.DataFrame): DataFrame containing cleaned customer data to be updated.
        threshold (int, optional): The minimum similarity score required for a fuzzy match. Defaults to 65.

    Returns:
        pandas.DataFrame: The updated cleaned DataFrame with matched 'Customer_Number' values.
    """
    # Create dictionaries for faster lookup
    source_name_to_number = dict(zip(source_data_df['Customer_Name'], source_data_df['Customer_Number']))
    source_names = list(source_name_to_number.keys())

    # Apply fuzzy matching in parallel
    matched_numbers = Parallel(n_jobs=-1)(
        delayed(match_row)(row, source_names, source_name_to_number, threshold)
        for _, row in cleaned_df.iterrows()
    )

    cleaned_df['Customer_Number'] = matched_numbers
    return cleaned_df




def check_and_perform_fuzzy_matching(source_data_df, cleaned_df, table, data_source):
    # Check if both Customer_Number and Customer_Name columns exist
    has_customer_number = 'Customer_Number' in cleaned_df.columns
    has_customer_name = 'Customer_Name' in cleaned_df.columns

    if not has_customer_number and not has_customer_name:
        print(f"Skipping fuzzy matching for table '{table}' from data source '{data_source}' as both Customer_Number and Customer_Name columns are missing.")
        return

    # Check if both Customer_Number and Customer_Name are entirely null
    customer_number_all_null = cleaned_df['Customer_Number'].isnull().all() if has_customer_number else True
    customer_name_all_null = cleaned_df['Customer_Name'].isnull().all() if has_customer_name else True

    if customer_number_all_null and customer_name_all_null:
        print(f"Skipping fuzzy matching for table '{table}' from data source '{data_source}' as both Customer_Number and Customer_Name are entirely null.")
        return

    # Check if fuzzy matching is needed
    if has_customer_name and cleaned_df['Customer_Number'].isnull().any():
        print('\n\n ------Processing fuzzy matching------')
        matching_start_time = time.time()
        update_customer_info(source_data_df, cleaned_df)
        matching_end_time = time.time()
        matching_elapsed_time = matching_end_time - matching_start_time
        print(f"Fuzzy matching for table '{table}' from data source '{data_source}' completed in {matching_elapsed_time:.2f} seconds")
    else:
        print(f"Skipping fuzzy matching for table '{table}' from data source '{data_source}' as all customer numbers are present or Customer_Number column is missing.")


#  !! MIGHT BE NEEDED STILL VERIFYING !!
# def check_and_perform_fuzzy_matching(source_data_df, cleaned_df, table, data_source):
#     # Check if both Customer_Number and Customer_Name columns exist
#     has_customer_number = 'Customer_Number' in cleaned_df.columns
#     has_customer_name = 'Customer_Name' in cleaned_df.columns

#     if not has_customer_number and not has_customer_name:
#         print(f"Skipping fuzzy matching for table '{table}' from data source '{data_source}' as both Customer_Number and Customer_Name are missing.")
#         return

#     # Check if fuzzy matching is needed
#     if has_customer_name and cleaned_df['Customer_Number'].isnull().any():
#         print('\n\n ------Processing fuzzy matching------')
#         matching_start_time = time.time()
#         update_customer_info(source_data_df, cleaned_df)
#         matching_end_time = time.time()
#         matching_elapsed_time = matching_end_time - matching_start_time
#         print(f"Fuzzy matching for table '{table}' from data source '{data_source}' completed in {matching_elapsed_time:.2f} seconds")
#     else:
#         print(f"Skipping fuzzy matching for table '{table}' from data source '{data_source}' as all customer numbers are present or Customer_Number column is missing.")
