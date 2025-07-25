from sqlalchemy import create_engine, text
import pandas as pd
import os
import re
import time
from datetime import datetime
from customer_master_etl.modules import Transform
from customer_master_etl.src.config import logging

def get_data(connection_string, data_source, table_name):
    """
    Gets raw data from database source using pre-defined SQLs.
    """
    try:
        engine = create_engine(connection_string, echo=False, fast_executemany=True)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file = os.path.join(base_dir, "..", "data_sources", data_source, "SQLs", f"{table_name}.sql")
        logging.info(f"Looking for SQL file at: {sql_file}")
        if not os.path.exists(sql_file):
            logging.error(f"SQL file for table '{table_name}' not found at: {sql_file}")
            return None

        with open(sql_file, "r") as file:
            query = file.read()
        compiled_query = text(query)

        with engine.connect() as conn:
            data = conn.execute(compiled_query).fetchall()
        logging.info(f"Successfully extracted data from data_source: {data_source}, table_name: {table_name}.")
        return data
    except Exception as e:
        logging.error(f"An error occurred while processing data_source: {data_source}, table_name: {table_name}. Error: {str(e)}")
        return None

def get_source_data(connection_string):
    """
    Gets source data of customer master from the specified data source for fuzzy matching.
    """
    try:
        engine = create_engine(connection_string, echo=False, fast_executemany=True)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file = os.path.join(base_dir, "..", "data_sources", "CustomerMaster", "SQLs", "CustomerMaster.sql")
        logging.info(f"Looking for CustomerMaster SQL at: {sql_file}")
        if not os.path.exists(sql_file):
            logging.error(f"SQL file for Customer Master data not found at: {sql_file}")
            return None

        with open(sql_file, "r") as file:
            query = file.read()
        compiled_query = text(query)

        with engine.connect() as conn:
            data = conn.execute(compiled_query).fetchall()
        logging.info(f"Successfully extracted data from CustomerMaster.")
        return data
    except Exception as e:
        logging.error(f'An error has occurred when extracting data from CustomerMaster: {str(e)}')
        return None

def get_cat_data(connection_string):
    """
    Gets CAT DCN data for fuzzy matching.
    """
    try:
        engine = create_engine(connection_string, echo=False, fast_executemany=True)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file = os.path.join(base_dir, "..", "data_sources", "CAT", "SQLs", "CAT_DCN.sql")
        logging.info(f"Looking for CAT DCN SQL at: {sql_file}")
        if not os.path.exists(sql_file):
            logging.error(f"SQL file for CAT DCN data not found at: {sql_file}")
            return None

        with open(sql_file, "r") as file:
            query = file.read()
        compiled_query = text(query)

        with engine.connect() as conn:
            data = conn.execute(compiled_query).fetchall()
        logging.info(f"Successfully extracted CAT data from CatData.E250_UCID_DCN_Report.")
        return data
    except Exception as e:
        logging.error(f'Error occurred while extracting CAT data: {str(e)}')
        return None

def extract_data(connection_string, data_source, table):
    """
    Generalized process to getting data from a given source of SQL.
    """
    try:
        extraction_start_time = time.time()
        extracted_data = get_data(connection_string, data_source, table)
        extraction_end_time = time.time()
        extraction_time = extraction_end_time - extraction_start_time
        logging.info(f"Data extraction for table '{table}' from data source '{data_source}' completed in {extraction_time:.2f} seconds")
        return extracted_data
    except Exception as e:
        logging.error(f'An error occurred in extraction time calculation: {str(e)}')
        return None

def is_excel_source(data_source):
    """
    Checks if the data source is an Excel source.
    """
    try:
        project_root = os.path.dirname(os.path.abspath(__file__))
        excel_folder = os.path.join(project_root, '..', 'data_sources', data_source, 'Excel')
        logging.info(f"Checking Excel folder path: {excel_folder}")
        folder_exists = os.path.exists(excel_folder)
        has_files = len(os.listdir(excel_folder)) > 0 if folder_exists else False
        logging.info(f"Excel folder exists: {folder_exists}, contains files: {has_files}")
        return folder_exists and has_files
    except Exception as e:
        logging.error(f'An error occurred in determining excel source: {str(e)}')
        return None

def process_excel_source(data_source, table):
    """
    Handles an Excel data source.
    """
    try:
        project_root = os.path.dirname(os.path.abspath(__file__))
        excel_folder = os.path.join(project_root, '..', 'data_sources', data_source, 'Excel')
        excel_file = os.path.join(excel_folder, f"{table}.xlsx")
        logging.info(f"Checking Excel file path: {excel_file}")

        if not os.path.exists(excel_file):
            logging.error(f"Excel file for table '{table}' not found in {excel_folder}")
            return None

        df = pd.read_excel(excel_file)
        logging.info(f"Successfully read Excel file: {excel_file}")

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
    Formats a column name to destination table formatting.
    """
    try:
        words = re.findall(r'\w+', col)
        words = [word.capitalize() for word in words]
        logging.info(f'Formatted column {col} successfully.')
        return '_'.join(words)
    except Exception as e:
        logging.error(f'Could not format column: {col}, Error: {str(e)}')
        return None

def extract_before_colon(text):
    """
    For PCC excel data: get the customer number (CUNO) if available.
    """
    try:
        if isinstance(text, str):
            return text.split('::')[0]
        return text
    except Exception as e:
        logging.error(f'Could not extract customer number before colon, Error: {str(e)}')
        return None

def process_pcc_excel(df):
    """
    Processes PCC link data: drops unneeded columns, formats, adds source details.
    """
    try:
        columns_to_keep = [
            'LAST NAME', 'FIRST NAME', 'WORK PHONE', 'EMAIL', 'COMPANY NAME', 
            'COMPANY CITY', 'COMPANY STATE', 'COMPANY ZIP', 'COMPANY COUNTRY', 'PARTSTORE DCNs'
        ]
        df = df[columns_to_keep]
        df['Customer_Number'] = df['PARTSTORE DCNs'].apply(extract_before_colon)
        df = df.drop(columns=['PARTSTORE DCNs'])
        df['Source_DB'] = 'PCC'
        df['Source_Table'] = 'Excel_File'
        df['Source_Timestamp'] = datetime.now()
        df.columns = [format_column_name(col) for col in df.columns]
        df = df.rename(columns={'Ucid': 'CAT_UCID'})
        logging.info(f'Processed Excel PCC (Parts ordering website) successfully.')
        return df
    except Exception as e:
        logging.error(f'Could not process PCC source, Error: {str(e)}')
        return None

def process_visionlink_excel(df):
    """
    Processes VisionLink Excel data: drops unneeded columns, formats, adds source details.
    """
    try:
        columns_to_drop = [
            'CWS ID + CCID', 'Login Count   ', 'First Login    ', 'Last Login    ', 
            'Total Assets', 'Subscribed Assets', 'VL Access', 'Asset Security ','CCID',
            'Account Created Date ', 'Account Created By', 'Application Type', ' CWS ID '
        ]
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        df.columns = [format_column_name(col) for col in df.columns]
        df = df.rename(columns={'Ccid_Name': 'Customer_Name'})
        df = df.rename(columns={'Ccid': 'CAT_UCID'})
        df['Source_DB'] = 'VisionLink'
        df['Source_Table'] = 'Excel_File'
        df['Source_Timestamp'] = datetime.now()
        logging.info(f'Processed VisionLink Excel successfully.')
        return df
    except Exception as e:
        logging.error(f'Could not process VisionLink Excel, Error: {str(e)}')
        return None

def extract_and_process_data(data_source, table, is_excel, connection_string=None):
    """
    Extracts and processes data from the specified source, either an Excel file or a database.
    """
    try:
        logging.info(f"Starting data extraction and processing for data_source: {data_source}, table: {table}, is_excel: {is_excel}.")
        if is_excel:
            logging.info(f"Processing Excel source: {data_source}, table: {table}.")
            extracted_data = process_excel_source(data_source, f"{table}")
            if extracted_data is not None:
                extracted_data = Transform.reformat_excel_headers(extracted_data)
        else:
            logging.info(f"Extracting data from database source: {data_source}, table: {table}.")
            if not connection_string:
                raise ValueError("Connection string is required for database sources.")
            extracted_data = extract_data(connection_string, data_source, table)
            if extracted_data is not None:
                extracted_data = pd.DataFrame(extracted_data)
        logging.info(f"Successfully processed data for data_source: {data_source}, table: {table}.")
        return extracted_data
    except Exception as e:
        logging.error(f"An error occurred during data extraction and processing for data_source: {data_source}, table: {table}. Error: {str(e)}")
        return None

def get_tables_for_data_source(data_sources_dir, data_source, is_excel):
    """
    Returns a list of tables for the given data source, either from Excel or SQL files.
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