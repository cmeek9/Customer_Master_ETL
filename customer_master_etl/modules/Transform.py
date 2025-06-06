import pandas as pd
import time
import re
from customer_master_etl.modules import ExtractCustomerData
from customer_master_etl.src.config import logging

def clean_email(email):
    """
    'Cleans' a email by bringing everything lowercase, making sure it is a valid email addres and removing it if it is.

    Parameters: 
        email (str): a string containing possible emails.

    Returns: 
        cleaned_email (str):  a cleaned, valid email in the dataframe.
    
    """
    try:
        # Convert email to lowercase and remove whitespace
        cleaned_email = email.lower().strip()

        # regex to clean emails from special characters --likely need to improve.
        cleaned_email = re.sub(r'[^a-zA-Z0-9.@]', '', cleaned_email)

        if '@' not in cleaned_email:
            return None
        logging.info(f'Emails cleaned succcessfully.')
        return cleaned_email
    except Exception as e:
        logging.error(f"An error occurred while cleaning emails, Error: {str(e)}")
        return None

 
def make_emails_clean(df):
    """"
    Helper function to take in the emails in the data frame and drop invalid email columns

    Parameters: 
        df (dataframe): The passed in dataframe with a column contain emails

    retuns:
        df (dataframe): A dataframe with a cleaned up version of emails usable for marketing.
    
    """
    try:
        df['Customer_Email'] = df['Customer_Email'].apply(clean_email)
        logging.info(f'helper function to apply clean emails function successful.')
        return df
    except Exception as e:
        logging.error(f" helper function to clean emails applied failed,Error: {str(e)}")
        return None



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
        logging.info(f"number converted to integer")
        return int(str(x).strip())
    except Exception as e:
        logging.error(f"'Error NaN', Error: {str(e)}")
        return None

    

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
    try:
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
        logging.info(f"Data cleaning took {cleaning_time:.2f} seconds.")
        return cleaned_df
    except Exception as e:
        logging.error(f"Data cleaning timer failed, Error: {str(e)}")
        return None


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
    try:
        logging.info(f"Attempting to find a match for column: {col_name}.")
        
        for pattern, new_name in column_mapping.items():
            if re.search(pattern, col_name):
                logging.info(f"Pattern matched: {pattern} for column: {col_name}. Mapped to: {new_name}.")
                return new_name
        
        logging.info(f"No pattern match found for column: {col_name}. Returning the original column name.")
        return col_name
    except Exception as e:
        logging.error(f"An error occurred while finding a match for column: {col_name}. Error: {str(e)}")
        raise


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
    try:
        logging.info("Creating column mapping dictionary.")
        column_mapping = {
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
        }
        logging.info("Column mapping dictionary created successfully.")
        return column_mapping
    except Exception as e:
        logging.error(f"An error occurred while creating the column mapping dictionary. Error: {str(e)}")
        raise

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
    try:
        logging.info("Starting column name mapping process.")
        new_columns = []

        for col in columns:
            matched = False
            for pattern, new_name in column_mapping.items():
                if re.match(pattern, col, re.IGNORECASE):
                    logging.info(f"Column '{col}' was mapped to '{new_name}'.")
                    new_columns.append(new_name)
                    matched = True
                    break
            if not matched:
                logging.info(f"No match found for column '{col}'. Keeping the original name.")
                new_columns.append(col)

        logging.info("Column name mapping process completed successfully.")
        return new_columns
    except Exception as e:
        logging.error(f"An error occurred during column name mapping. Error: {str(e)}")
        raise


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
    try:
        logging.info("Ensuring unique column names.")
        seen = {}
        unique_columns = []

        for col in columns:
            if col in seen:
                seen[col] += 1
                new_name = f"{col}_{seen[col]}"
                logging.info(f"Duplicate column name '{col}' detected. Renamed to '{new_name}'.")
                unique_columns.append(new_name)
            else:
                seen[col] = 0
                unique_columns.append(col)

        logging.info("Column name uniqueness ensured successfully.")
        return unique_columns
    except Exception as e:
        logging.error(f"An error occurred while ensuring unique column names. Error: {str(e)}")
        raise


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
    try:
        for col in required_columns:
            if col not in df.columns:
                if col == 'Customer_Number':
                    df[col] = None
                else:
                    df[col] = ''
        logging.info(f"addeding missing customer column needed for fuzzy matching and dataframe joins.")
        return df
    except Exception as e:
        logging.error(f"An error occurred while adding the customer number column into dataframe. Error: {str(e)}")
        raise

# Excel orchestrator 
# needed for Excel sources
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
    try:
        column_mapping = create_column_mapping()
        new_columns = map_column_names(df.columns, column_mapping)
        unique_columns = ensure_unique_column_names(new_columns)
        df.columns = unique_columns
        df = add_missing_columns(df, ['Customer_Name', 'Customer_Number'])
        logging.info(f"Successfully reformatted the column headers.")
        return df
    except Exception as e:
        logging.error(f"An error occurred while reformatted the column headers, Error: {str(e)}")
        raise



def update_additional_data(cleaned_df, source_data_df):
    """
    Updates the cleaned DataFrame with additional data from the source DataFrame.

    Parameters:
        cleaned_df (pd.DataFrame): The cleaned DataFrame containing initial customer data.
        source_data_df (pd.DataFrame): The source DataFrame containing additional customer data.

    Returns:
        pd.DataFrame: An updated DataFrame where customer information is supplemented with data from the source DataFrame.
    """
    try:
        # Merge cleaned_df with source_data_df on Customer_Number
        logging.info("Merging cleaned_df with source_data_df on 'Customer_Number'.")
        merged_df = pd.merge(
            cleaned_df,
            source_data_df,
            on='Customer_Number',
            how='left',
            suffixes=('', '_source')
        )

        # Create a new DataFrame to build the updated results
        updated_df = merged_df.copy()

        # Ensure Customer_Name is taken from source_data_df (if available)
        if 'Customer_Name_source' in merged_df.columns:
            logging.info("Updating 'Customer_Name' from source data where available.")
            updated_df['Customer_Name'] = updated_df['Customer_Name_source'].fillna(updated_df['Customer_Name'])

        # Remove '_source' columns used in the merge
        source_columns = [col for col in updated_df.columns if col.endswith('_source')]
        logging.info(f"Removing temporary columns: {source_columns}.")
        updated_df = updated_df.drop(columns=source_columns)

        # Remove exact duplicates across all columns
        logging.info("Removing exact duplicates from the updated DataFrame.")
        updated_df = updated_df.drop_duplicates()

        logging.info("Additional data update process completed successfully.")
        return updated_df
    except Exception as e:
        logging.error(f"An error occurred during the update of additional data. Error: {str(e)}")
        raise




def update_cat_data(cleaned_df, cat_data_df):
    """
    Updates the cleaned DataFrame with CAT data by merging on Customer_Number and CAT_DCN.

    Parameters:
        cleaned_df (pd.DataFrame): The cleaned DataFrame containing customer data.
        cat_data_df (pd.DataFrame): The CAT DataFrame containing CAT-specific data.

    Returns:
        pd.DataFrame: A merged DataFrame with additional CAT data added to the cleaned DataFrame.
    """
    try:
        
        cleaned_df['Customer_Number'] = cleaned_df['Customer_Number'].astype(str).str.strip()
        cat_data_df['CAT_DCN'] = cat_data_df['CAT_DCN'].astype(str).str.strip()

        # Merge cleaned_df with cat_data_df on Customer_Number and CAT_DCN
        logging.info("Merging cleaned_df with cat_data_df on 'Customer_Number' and 'CAT_DCN'.")
        merged_df = pd.merge(
            cleaned_df,
            cat_data_df,
            left_on='Customer_Number',
            right_on='CAT_DCN',
            how='left',
            suffixes=('', '_cat')
        )

        logging.info("CAT data update process completed successfully.")
        return merged_df
    except Exception as e:
        logging.error(f"An error occurred during the update of CAT data. Error: {str(e)}")
        raise



def clean_customer_columns_for_matching(df, df_name="DataFrame"):
    """
    Cleans the specified customer columns by converting to lowercase and stripping whitespace.
    
    Parameters:
    df (pandas.DataFrame): The DataFrame containing the customer data.
    df_name (str): The name of the DataFrame (for debugging purposes).
    
    Returns:
    pandas.DataFrame: The cleaned DataFrame.
    """
    try:
        columns_to_clean = ['Customer_Name', 'Customer_Number', 'CAT_DCN', 'CAT_UCID']
        
        for column in columns_to_clean:
            if column in df.columns:
                logging.info(f"Cleaning column '{column}' in {df_name}")
                df[column] = df[column].apply(
                    lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (float, int)) 
                    else (str(x).strip().lower() if pd.notnull(x) else None)
                )
            else:
                logging.info(f"Warning: Column '{column}' not found in the {df_name} DataFrame.")
        
        return df
    except Exception as e:
        logging.error(f"Failed to clean columns for customer matching in {df_name}. Error: {str(e)}")
        raise

def extract_and_clean_cat_data(cat_conxn_string):
    """
    Extract and clean CAT data.

    Parameters:
        cat_conxn_string (str): Database connection string for CAT data.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing CAT data.
    """
    try:
        logging.info("Extracting CAT data.")
        cat_data = ExtractCustomerData.get_cat_data(cat_conxn_string)
        cat_data_df = pd.DataFrame(cat_data)
        
        logging.info("Cleaning CAT data columns for matching.")
        cat_data_df = clean_customer_columns_for_matching(cat_data_df, df_name="cat_data_df")
        
        logging.info("CAT data extracted and cleaned successfully.")
        return cat_data_df
    except Exception as e:
        logging.error(f"An error occurred while extracting and cleaning CAT data. Error: {str(e)}")
        raise

def clean_and_transform_customer_data(extracted_data, customer_master_string):
    """
    Clean and transform customer data for matching.

    Parameters:
        extracted_data (pd.DataFrame): Extracted customer data to be cleaned and transformed.
        customer_master_string (str): Database connection string for customer master data.

    Returns:
        tuple: A tuple containing:
            - pd.DataFrame: Cleaned extracted data.
            - pd.DataFrame: Cleaned source customer master data.
    """
    try:
        logging.info("Cleaning extracted customer data.")
        cleaned_df = make_emails_clean(extracted_data)

        logging.info("Extracting and cleaning source customer master data.")
        source_data = ExtractCustomerData.get_source_data(customer_master_string)
        source_data_df = pd.DataFrame(source_data)
        source_data_df = clean_customer_columns_for_matching(source_data_df, df_name="source_data_df")
        
        logging.info("Cleaning extracted data columns for matching.")
        cleaned_df = clean_customer_columns_for_matching(cleaned_df, df_name="cleaned_df")
        
        logging.info("Customer data cleaned and transformed successfully.")
        return cleaned_df, source_data_df
    except Exception as e:
        logging.error(f"An error occurred while cleaning and transforming customer data. Error: {str(e)}")
        raise
