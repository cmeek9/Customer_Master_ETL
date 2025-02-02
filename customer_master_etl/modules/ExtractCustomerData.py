from sqlalchemy import create_engine, text
import pandas as pd
import os
import re
import time
from datetime import datetime
from customer_master_etl.modules import Transform
from customer_master_etl.src.config import logging


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
    try:
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
        logging.info(f"Successfully extracted data from data_source: {data_source}, table_name: {table_name}.")
        return data

    except Exception as e:
        logging.error(f"An error occurred while processing data_source: {data_source}, table_name: {table_name}. Error: {str(e)}")
        return None


def get_source_data(connection_string):
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
        logging.info(f"Successfully extracted data from CIPNAME0, DBS old customer master.")
        return data

    except Exception as e:
        logging.error(f'An error has occurred when extracting data from CIPNAME0, Error: {str(e)}')
        return None


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
        logging.info(f"Successfully extracted CAT data from CatData.E250_UCID_DCN_Report in Pord1 Wagner_ODS.")
        return data

    except Exception as e:
        logging.error(f'Error occured while trying to extract data from CatData.E250_UCID_DCN_Report: {str(e)}')
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
    try:
        # look at completion time
        extraction_start_time = time.time()
        extracted_data = get_data(connection_string, data_source, table)
        extraction_end_time = time.time()
        # Extraction time calculation
        extraction_time = extraction_end_time - extraction_start_time

        logging.info(f"Data extraction for table '{table}' from data source '{data_source}' completed in {extraction_time:.2f} seconds")
        return extracted_data
    except Exception as e:
        logging.error(f'An error occured in extraction time calculation: {str(e)}')
        return None


def is_excel_source(data_source):
    """
    A function to check if the data source is an excel file.

    Parameters:
        data_source: an excel data source.
        
    Returns:
         file path: the path to which the excel lives.

    """
    try:
        project_root = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
        excel_folder = os.path.join(project_root, '..', 'data_sources', data_source, 'Excel')

        # Log the full path
        logging.info(f"Checking Excel folder path: {excel_folder}")

        # Check if the folder exists and contains files
        folder_exists = os.path.exists(excel_folder)
        has_files = len(os.listdir(excel_folder)) > 0 if folder_exists else False

        logging.info(f"Excel folder exists: {folder_exists}, contains files: {has_files}")
        return folder_exists and has_files
    except Exception as e:
        logging.error(f'An error ocurred in determining excel source, Error: {str(e)}')
        return None


def process_excel_source(data_source, table):
    """
    This is the main function for handling an Excel data source.

    Parameters:
        data_source: an Excel data source.
        table: A table structure, specifically a spreadsheet within the Excel file.

    Returns:
        df (DataFrame): Converted into a DataFrame for later processing.
    """
    try:
        # Construct the full path to the Excel folder
        project_root = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
        excel_folder = os.path.join(project_root, '..', 'data_sources', data_source, 'Excel')
        excel_file = os.path.join(excel_folder, f"{table}.xlsx")

        # Log the constructed paths
        logging.info(f"Checking Excel folder path: {excel_folder}")
        logging.info(f"Checking Excel file path: {excel_file}")

        # Check if the file exists
        if not os.path.exists(excel_file):
            logging.error(f"Error: Excel file for table '{table}' not found in {excel_folder}")
            return None

        # Read the Excel file
        df = pd.read_excel(excel_file)
        logging.info(f"Successfully read Excel file: {excel_file}")

        # Perform Excel-specific operations
        if data_source == 'PCC':
            df = process_pcc_excel(df)
            logging.info(f"Processed Excel file for PCC source.")
        elif data_source == 'VisionLink':
            df = process_visionlink_excel(df)
            logging.info(f"Processed Excel file for VisionLink source.")

        logging.info(f"Finished processing Excel source.")
        return df
    except Exception as e:
        logging.error(f"An error occurred in processing Excel source. Error: {str(e)}")
        return None

def format_column_name(col):
    """
    This takes in a column from a data frame and formats it to the destination table formatting.

    Parameters:
        col: column to format.
        
    Returns:
         col: a formatted column.
    """
    try:
        # Remove any non-alphanumeric characters (except underscores) and split
        words = re.findall(r'\w+', col)

        words = [word.capitalize() for word in words]
        # Join with underscores
        logging.info(f'formatted column {col} successfully.')
        return '_'.join(words)
    except Exception as e:
        logging.error(f'could not format columns, Error: {str(e)}')
        return None


def extract_before_colon(text):
    """
    This function is used for PCC excel data.  It needs to get the customer number (CUNO) if available to limit fuzzy matching.

    Parameters:
        text: String in a column that contains or possibly contains the customer number.
        
    Returns:
         text: Hopefully returns the customer number else there is nothing to parse.
    """
    try:
        if isinstance(text, str):
            return text.split('::')[0]
        return text
    except Exception as e:
        logging.error(f'could not extract customer number before colon, Error: {str(e)}')
        return None

def process_pcc_excel(df):
    """
    This function is processing PCC link data.  Dropping un-needed columns formating & adding source details

    Parameters:
        df (dataframe): Takes in a dataframe to process the data within.

    Returns:
        df (dataframe): Returns a more cleaned up df, extracted version that can be further processed later.
    """
    try:
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
        logging.info(f'Processed Excel PCC (Parts ordering website) successfully.')
        return df
    except Exception as e:
        logging.error(f'could not process PPC source, Error: {str(e)}')
        return None


def process_visionlink_excel(df):
    """
    This function is processing vision link data.  Dropping un-needed columns formating & adding source details

    Parameters:
        df (dataframe): Takes in a dataframe to process the data within.

    Returns:
        df (dataframe): Returns a more cleaned up df, extracted version that can be further processed later.
    """
    try:
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
        logging.info(f'Processed VisionLink Excel successfully.')
        return df
    except Exception as e:
        logging.error(f'could not process VisionLink Excel, Error: {str(e)}')
        return None

def extract_and_process_data(data_source, table, is_excel, connection_string=None):
    '''
    Extracts and processes data from the specified source, either an Excel file or a database.

    Parameters:
        - data_source (str): The path to the data source directory or file.
        - table (str): The table name or identifier.
        - is_excel (bool): Flag indicating whether the data source is an Excel file.
        - connection_string (str, optional): The connection string for database access (required if not using Excel).

    Returns:
        pd.DataFrame: A processed DataFrame containing the extracted data.
    '''
    try:
        logging.info(f"Starting data extraction and processing for data_source: {data_source}, table: {table}, is_excel: {is_excel}.")

        if is_excel:
            # Process Excel source
            logging.info(f"Processing Excel source: {data_source}, table: {table}.")
            extracted_data = process_excel_source(data_source, f"{table}")
            extracted_data = Transform.reformat_excel_column_headers(extracted_data)
        else:
            # Process database source
            logging.info(f"Extracting data from database source: {data_source}, table: {table}.")
            if not connection_string:
                raise ValueError("Connection string is required for database sources.")
            extracted_data = extract_data(connection_string, data_source, table)
            extracted_data = pd.DataFrame(extracted_data)

        logging.info(f"Successfully processed data for data_source: {data_source}, table: {table}.")
        return extracted_data

    except Exception as e:
        logging.error(f"An error occurred during data extraction and processing for data_source: {data_source}, table: {table}. Error: {str(e)}")
        return None


def get_tables_for_data_source(data_sources_dir, data_source, is_excel):
    """
    Returns a list of tables for the given data source, either from Excel or SQL files.

    Parameters:
        - data_sources_dir (str): The base directory for data sources.
        - data_source (str): The specific data source directory.
        - is_excel (bool): Flag indicating whether the data source is Excel-based.

    Returns:
        list: A list of table names (without extensions).
    """
    try:
        logging.info(f"Fetching tables for data_source: {data_source}, is_excel: {is_excel}.")
        
        folder = os.path.join(data_sources_dir, data_source, 'Excel' if is_excel else 'SQLs')
        logging.info(f"Looking in folder: {folder}.")
        
        if not os.path.exists(folder):
            logging.error(f"Folder does not exist: {folder}.")
            return []
        
        extension = '.xlsx' if is_excel else '.sql'
        tables = [os.path.splitext(file)[0] for file in os.listdir(folder) if file.endswith(extension)]
        
        logging.info(f"Found tables: {tables}.")
        return tables

    except Exception as e:
        logging.error(f"An error occurred while fetching tables for data_source: {data_source}. Error: {str(e)}")
        return []
