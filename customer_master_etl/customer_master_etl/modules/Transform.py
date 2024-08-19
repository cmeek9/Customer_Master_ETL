import pandas as pd
import time
import re
 

def clean_email(email):
    """
    'Cleans' a email by bringing everything lowercase, making sure it is a valid email addres and removing it if it is.

    Parameters: 
        email (str): a string containing possible emails.

    Returns: 
        cleaned_email (str):  a cleaned, valid email in the dataframe.
    
    """
    # Convert email to lowercase and remove whitespace
    cleaned_email = email.lower().strip()

    # regex to clean emails from special characters --likely need to improve.
    cleaned_email = re.sub(r'[^a-zA-Z0-9.@]', '', cleaned_email)

    if '@' not in cleaned_email:
        return None
    return cleaned_email

 
def make_emails_clean(df):
    """"
    Helper function to take in the emails in the data frame and drop invalid email columns

    Parameters: 
        df (dataframe): The passed in dataframe with a column contain emails

    retuns:
        df (dataframe): A dataframe with a cleaned up version of emails usable for marketing.
    
    """
    df['Customer_Email'] = df['Customer_Email'].apply(clean_email)

    # # Drop rows with None values in the cleaned_email column
    # df = df.dropna(subset=['Customer_Email'])

    return df



def clean_and_convert_to_number(x):
    """
    Cleans the input and converts it to an integer.

    Parameters:
    x : any
        The input value that you want to clean and convert to an integer.

    Returns:
    int or str
        Returns the integer value if conversion is successful, otherwise returns 'Error NaN'.
    """
    try:
        return int(str(x).strip())
    except ValueError:
        return 'Error NaN'

    

def make_emails_clean(extracted_data):
    """
    Cleans and converts the extracted data into a pandas DataFrame.

    Parameters:
    extracted_data : pandas.DataFrame or any
        The data to be cleaned and converted. This can be a pandas DataFrame, 
        a list of data, or any other structure that can be converted into a DataFrame.

    Returns:
    pandas.DataFrame:
        The cleaned DataFrame. If the input was empty or None, an empty DataFrame is returned.
    """
    # Timing data cleansing process 
    cleaning_start_time = time.time()

    # Process cleaning the extracted data
    if isinstance(extracted_data, pd.DataFrame):
        cleaned_df = extracted_data
    elif extracted_data:
        # If it's not a DataFrame but contains data, convert it
        cleaned_df = pd.DataFrame(extracted_data)
    else:
        # If it's empty or None, return an empty DataFrame
        cleaned_df = pd.DataFrame()

    cleaning_end_time = time.time()

    # Cleaning time calculation
    cleaning_time = cleaning_end_time - cleaning_start_time
    print(f"Data cleaning took {cleaning_time:.2f} seconds.")

    return cleaned_df


def find_match(col_name, column_mapping):
    """
    Finds and returns a mapped column name based on a pattern match.

    Parameters:
    col_name : str
        The name of the column to be matched against the patterns.
    column_mapping : dict
        A dictionary where keys are regex patterns and values are the new column names.

    Returns:
    str
        The new column name if a pattern match is found; otherwise, the original column name.
    """
    for pattern, new_name in column_mapping.items():
        if re.search(pattern, col_name):
            return new_name
    # If no match found, return the original name
    return col_name


def create_column_mapping():
    """
    Creates and returns a dictionary for column name mapping using regex patterns.

    Returns:
    dict
        A dictionary mapping regex patterns to standardized column names.
        For example:
        - 'First_Name' for column names matching 'first name' or 'first_name'.
        - 'Customer_Email' for any column name containing 'email'.
        - 'Customer_Phone_Number' for any column name containing 'phone'.
    """
    return {
        r'(?i)^first[\s_]?name$': 'First_Name',
        r'(?i)^last[\s_]?name$': 'Last_Name',
        r'(?i)^(customer|company)[\s_]?name$': 'Customer_Name',
        r'(?i)^name$': 'Customer_Name',
        r'(?i).*email.*': 'Customer_Email',
        r'(?i).*phone.*': 'Customer_Phone_Number',
        r'(?i).*address.*': 'Customer_Street_Address',
        r'(?i).*city.*': 'Customer_City',
        r'(?i).*state.*': 'Customer_State',
        r'(?i).*zip.*': 'Customer_ZipCode',
        r'(?i).*country.*': 'Customer_Country',
        r'(?i).*customer[\s_]?number.*': 'Customer_Number',
        r'(?i).*PARTSTORE DCNs*': 'DCN',
        # r'(?i).*Ccid*': 'CAT_CCID',
        # r'(?i).*Ccid_Name*': 'CAT_Customer_Name',
    }

def map_column_names(columns, column_mapping):
    """
    Maps a list of column names to new names based on a provided mapping dictionary.

    Parameters:
    columns : list of str
        A list of column names that need to be mapped to new names.
    column_mapping : dict
        A dictionary where keys are regex patterns and values are the new column names.

    Returns:
    list of str
        A list of column names where matching columns have been replaced by the corresponding
        new names from the mapping dictionary. Columns that do not match any pattern remain unchanged.
    """
    new_columns = []
    for col in columns:
        matched = False
        for pattern, new_name in column_mapping.items():
            if re.match(pattern, col, re.IGNORECASE):
                new_columns.append(new_name)
                matched = True
                break
        if not matched:
            new_columns.append(col)
    return new_columns


def ensure_unique_column_names(columns):
    """
    Ensures that a list of column names are unique by appending a suffix to duplicates.

    Parameters:
    columns : list of str
        A list of column names that need to be checked for uniqueness.

    Returns:
    list of str
        A list of column names where duplicates have been renamed with a numeric suffix to ensure uniqueness.
    """
    seen = {}
    unique_columns = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            unique_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            unique_columns.append(col)
    return unique_columns


def add_missing_columns(df, required_columns):
    """
    Adds missing columns (customer number and customer name to a DataFrame with specified default values.
    This is for the purpose of getting the necessary components to use if fuzzy matching is needed.

    Parameters:
    df : pandas.DataFrame
        The DataFrame to which missing columns need to be added.
    required_columns : list of str
        A list of column names that are required in the DataFrame.

    Returns:
    pandas.DataFrame
        The DataFrame with all required columns, with missing columns added and 
        populated with default values.
    """
    for col in required_columns:
        if col not in df.columns:
            if col == 'Customer_Number':
                df[col] = None
            else:
                df[col] = ''
    return df


def reformat_excel_column_headers(df):
    """
    Reformats the column headers of a DataFrame to standardize names and ensure uniqueness.

    This function performs several steps to reformat the column headers of the input DataFrame `df`:
    1. Maps the existing column names to standardized names using a predefined column mapping.
    2. Ensures all column names are unique by appending numeric suffixes to duplicates.
    3. Adds any missing required columns ('Customer_Name', 'Customer_Number') with default values.

    Parameters:
    df : pandas.DataFrame
        The DataFrame whose column headers need to be reformatted.

    Returns:
    pandas.DataFrame
        The DataFrame with reformatted, unique, and complete column headers.
    """
    column_mapping = create_column_mapping()
    new_columns = map_column_names(df.columns, column_mapping)
    unique_columns = ensure_unique_column_names(new_columns)
    df.columns = unique_columns
    df = add_missing_columns(df, ['Customer_Name', 'Customer_Number'])
    return df



def update_additional_data(cleaned_df, source_data_df):
    # Merge cleaned_df with source_data_df on Customer_Number
    merged_df = pd.merge(cleaned_df, 
                         source_data_df, 
                         on='Customer_Number', 
                         how='left', 
                         suffixes=('', '_source'))

    return merged_df



def update_cat_data(cleaned_df, cat_data_df):
    # Merge cleaned_df with cat_data_df on Customer_Number and CAT_DCN
    merged_df = pd.merge(cleaned_df, cat_data_df, 
                         left_on='Customer_Number', 
                         right_on='CAT_DCN', 
                         how='left', 
                         suffixes=('', '_cat'))
    

    return merged_df



def clean_customer_columns_for_matching(df, df_name="DataFrame"):
    """
    Cleans the specified customer columns by converting to lowercase and stripping whitespace.
    
    Parameters:
    df (pandas.DataFrame): The DataFrame containing the customer data.
    df_name (str): The name of the DataFrame (for debugging purposes).
    
    Returns:
    pandas.DataFrame: The cleaned DataFrame.
    """
    columns_to_clean = ['Customer_Name', 'Customer_Number', 'CAT_DCN']
    
    for column in columns_to_clean:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: x.lower().strip() if isinstance(x, str) else x)
        else:
            print(f"Warning: Column '{column}' not found in the {df_name} DataFrame.")
    
    return df
