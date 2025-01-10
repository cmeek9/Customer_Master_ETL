from sqlalchemy import create_engine, text
import pandas as pd
import os
import re
import time
from datetime import datetime
from customer_master_etl.modules import SEQLogging, Transform


def get_data(connection_string, data_source, table_name=None):
    '''
    Gets raw data from database source using pre-defined SQLs.

    parameters:
        - connection_string: Database connection string
        - data_source: The data source directory
        - table_name: Optional parameter to specify a specific table

    returns:
        list: a list of data from table.
    '''
    # try:
    engine = create_engine(connection_string, echo=False, fast_executemany=True)

    # Get SQL file based on the data source and table name
    if table_name:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file = os.path.join(base_dir, "..", "data_sources", data_source, "SQLs", f"{table_name}.sql")
        print("SQL file path:", sql_file)  # Debug print statement
        if not os.path.exists(sql_file):
            print(f"SQL file for table '{table_name}' not found at: {sql_file}")
            return None
    else:
        print("Please provide a table name.")
        return None

    # Read the SQL query/queries from the file
    with open(sql_file, "r") as file:
        query = file.read()

    # Compile the SQL query into a SQLAlchemy using text the input as text
    compiled_query = text(query)

    # Execute the query and get data
    with engine.connect() as conn:
        data = conn.execute(compiled_query).fetchall()
    return data

    # except Exception as e:
    #     seq_logger = SEQLogging()
    #     seq_logger.error(f'An error has occurred: {str(e)}')
    #     return None


def get_source_data(connection_string):
    '''
    Gets source data of customer master from the specified data source as a means for comparing in fuzzing matching.

    parameters:
        connection_string: Database connection string
        data_source: The data source directory
    '''
    # try:
        # Connect to the database
    engine = create_engine(connection_string, echo=False, fast_executemany=True)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file = os.path.join(base_dir, "..", "data_sources", "CustomerMaster", "SQLs", "CustomerMaster.sql")

    if not os.path.exists(sql_file):
        print(f"SQL file for Customer Master data not found at: {sql_file}")
        return None

    # Read the SQL query/queries from the file
    with open(sql_file, "r") as file:
        query = file.read()

    # Compile the SQL query into a SQLAlchemy using text the input as text
    compiled_query = text(query)

    # Execute the query and get data
    with engine.connect() as conn:
        data = conn.execute(compiled_query).fetchall()
    return data

    # except Exception as e:
    #     seq_logger = SEQLogging()
    #     seq_logger.error(f'An error has occurred: {str(e)}')
    #     return None


def get_cat_data(connection_string):
    '''
    Gets source data of customer master from the specified data source as a means for comparing in fuzzing matching.

    parameters:
        connection_string: Database connection string
        data_source: The data source directory
    '''
    try:
        # Connect to the database
        engine = create_engine(connection_string, echo=False, fast_executemany=True)

        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file = os.path.join(base_dir, "..", "data_sources", "CAT", "SQLs", "CAT_DCN.sql")

        if not os.path.exists(sql_file):
            print(f"SQL file for Customer Master data not found at: {sql_file}")
            return None

        # Read the SQL query/queries from the file
        with open(sql_file, "r") as file:
            query = file.read()

        # Compile the SQL query into a SQLAlchemy using text the input as text
        compiled_query = text(query)

        # Execute the query and get data
        with engine.connect() as conn:
            data = conn.execute(compiled_query).fetchall()
        return data

    except Exception as e:
        seq_logger = SEQLogging()
        seq_logger.error(f'An error has occurred: {str(e)}')
        return None


def extract_data(connection_string, data_source, table):
    """
    Generalized process to getting data from a given source of SQL.

    Parameters:
        connection_string: .
        data_source: Name of SQL DB its from.
        table: name of SQL table in DB.
        
    Returns:
        object: this is a object containing the extracted data.
    """
    extraction_start_time = time.time()
    extracted_data = get_data(connection_string, data_source, table)
    extraction_end_time = time.time()
    # Extraction time calculation
    extraction_time = extraction_end_time - extraction_start_time

    print(f"Data extraction for table '{table}' from data source '{data_source}' completed in {extraction_time:.2f} seconds")

    return extracted_data



def is_excel_source(data_source):
    """
    A function to check if the data source is an excel file.

    Parameters:
        data_source: an excel data source.
        
    Returns:
         file path: the path to which the excel lives.

    """
    excel_folder = os.path.join('../data_sources', data_source, 'Excel')
    return os.path.exists(excel_folder) and len(os.listdir(excel_folder)) > 0



def process_excel_source(data_source, table):
    """
    This is the main function for handling a excel data source.

    Parameters:
        data_source: an excel data source.
        table: a table structure, but more a spreadsheet which the excel lives on.
        
    Returns:
         df (dataframe): conveted into a dataframe for later processing.
    """
    excel_folder = os.path.join('../data_sources', data_source, 'Excel')
    excel_file = os.path.join(excel_folder, f"{table}.xlsx")
    
    if not os.path.exists(excel_file):
        print(f"Error: Excel file for table '{table}' not found in {excel_folder}")
        return None

    # Read the Excel file
    df = pd.read_excel(excel_file)
    
    # Perform Excel-specific operations
    if data_source == 'PCC':
        df = process_pcc_excel(df)
    elif data_source == 'VisionLink':
        df = process_visionlink_excel(df)
    
    return df

def format_column_name(col):
    """
    This takes in a column from a data frame and formats it to the destination table formatting.

    Parameters:
        col: column to format.
        
    Returns:
         col: a formatted column.
    """
    # Remove any non-alphanumeric characters (except underscores) and split
    words = re.findall(r'\w+', col)

    words = [word.capitalize() for word in words]
    # Join with underscores
    return '_'.join(words)


def extract_before_colon(text):
    """
    This function is used for PCC excel data.  It needs to get the customer number (CUNO) if available to limit fuzzy matching.

    Parameters:
        text: String in a column that contains or possibly contains the customer number.
        
    Returns:
         text: Hopefully returns the customer number else there is nothing to parse.
    """
    if isinstance(text, str):
        return text.split('::')[0]
    return text  # or return an empty string or any default value you prefer


def process_pcc_excel(df):
    """
    This function is processing PCC link data.  Dropping un-needed columns formating & adding source details

    Parameters:
        df (dataframe): Takes in a dataframe to process the data within.

    Returns:
        df (dataframe): Returns a more cleaned up df, extracted version that can be further processed later.
    """
    # Columns to keep
    columns_to_keep = [
        'LAST NAME', 'FIRST NAME', 'WORK PHONE', 'EMAIL', 'COMPANY NAME', 
        'COMPANY CITY', 'COMPANY STATE', 'COMPANY ZIP', 'COMPANY COUNTRY', 'PARTSTORE DCNs']
    
    # Select only the columns we need
    df = df[columns_to_keep]

    # Extract DCN from PARTSTORE DCNs
    df['Customer_Number'] = df['PARTSTORE DCNs'].apply(extract_before_colon)

    # Drop the 'Partstore_Dcns' column
    df = df.drop(columns=['PARTSTORE DCNs'])

    df['Source_DB'] = 'PCC'
    df['Source_Table'] = 'Excel_File'
    df['Source_Timestamp'] = datetime.now()
    
    # Format column names: make proper case and replace spaces with underscores
    df.columns = [format_column_name(col) for col in df.columns]

    df = df.rename(columns={'Ucid': 'CAT_UCID'})

    return df


def process_visionlink_excel(df):
    """
    This function is processing vision link data.  Dropping un-needed columns formating & adding source details

    Parameters:
        df (dataframe): Takes in a dataframe to process the data within.

    Returns:
        df (dataframe): Returns a more cleaned up df, extracted version that can be further processed later.
    """
    # Drop specified columns
    columns_to_drop = [
        'CWS ID + CCID', 'Login Count   ', 'First Login    ', 'Last Login    ', 
        'Total Assets', 'Subscribed Assets', 'VL Access', 'Asset Security ','CCID',
        'Account Created Date ', 'Account Created By', 'Application Type', ' CWS ID '
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # Replace spaces with underscores in column names
    df.columns = [format_column_name(col) for col in df.columns]

    df = df.rename(columns={'Ccid_Name': 'Customer_Name'})
    df = df.rename(columns={'Ccid': 'CAT_UCID'})

    df['Source_DB'] = 'VisionLink'
    df['Source_Table'] = 'Excel_File'
    df['Source_Timestamp'] = datetime.now()
    
    return df


def extract_and_process_data(data_source, table, is_excel, connection_string=None):
    if is_excel:
        extracted_data = process_excel_source(data_source, f"{table}")
        extracted_data = Transform.reformat_excel_column_headers(extracted_data)
    else:
        extracted_data = extract_data(connection_string, data_source, table)
        extracted_data = pd.DataFrame(extracted_data)
    
    return extracted_data


# ExtractCustomerData.py

def get_tables_for_data_source(data_sources_dir, data_source, is_excel):
    """
    Returns a list of tables for the given data source, either from Excel or SQL files.
    """
    if is_excel:
        # Get all Excel tables
        folder = os.path.join(data_sources_dir, data_source, 'Excel')
        tables = [os.path.splitext(file)[0] for file in os.listdir(folder) if file.endswith('.xlsx')]
    else:
        # Get all SQL tables
        folder = os.path.join(data_sources_dir, data_source, 'SQLs')
        tables = [os.path.splitext(file)[0] for file in os.listdir(folder) if file.endswith('.sql')]
    
    return tables
