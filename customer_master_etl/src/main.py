import configparser
import pandas as pd
import os
from customer_master_etl.modules import ExtractCustomerData, Load, Transform, FuzzyMatching, SelectData

def main():
    config = configparser.ConfigParser()

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root_dir, 'config.ini')
    config.read(config_path)

    # Argument parser for specifying the data source and table
    args = SelectData.parse_arguments()

    data_sources_dir = os.path.join(root_dir, 'data_sources')

    data_sources = []
    if args.data_source:
        data_sources = [args.data_source]
    else:
        # No data source specified, get all data sources
        data_sources = [name for name in os.listdir(data_sources_dir) if os.path.isdir(os.path.join(data_sources_dir, name))]

    for data_source in data_sources:
        print(f"Processing data source: {data_source}")

        # Get the connection string based on the data source
        connection_string_key = f'{data_source}'
        if connection_string_key in config['DataSources']:
            connection_string = config['DataSources'][connection_string_key]
        else:
            print(f"Error: No connection string found for data source '{data_source}'.")
            continue

        # Determine if the data source is an Excel source
        is_excel = ExtractCustomerData.is_excel_source(data_source)
        
        # Determine tables to process
        tables = []
        if args.table and args.data_source:
            tables = [args.table]
        else:
            if is_excel:
                # Get all Excel tables
                excel_folder = os.path.join(data_sources_dir, data_source, 'Excel')
                tables = [os.path.splitext(file)[0] for file in os.listdir(excel_folder) if file.endswith('.xlsx')]
            else:
                # Get all SQL tables
                sql_dir = os.path.join(data_sources_dir, data_source, 'SQLs')
                tables = [os.path.splitext(file)[0] for file in os.listdir(sql_dir) if file.endswith('.sql')]

        for table in tables:
            print(f"Processing table: {table}")
            # Perform extraction
            if is_excel:
                # Extract & Transform
                extracted_data = ExtractCustomerData.process_excel_source(data_source, f"{table}")
                extracted_data = Transform.reformat_excel_column_headers(extracted_data)
            else:
                extracted_data = ExtractCustomerData.extract_data(connection_string, data_source, table)
                extracted_data = pd.DataFrame(extracted_data)

            # Cleaning and transforming
            cleaned_df = Transform.make_emails_clean(extracted_data)
            CustomerMasterString = config['CustomerMaster']['connection_string']
            source_data = ExtractCustomerData.get_source_data(CustomerMasterString)
            source_data_df = pd.DataFrame(source_data)
            source_data_df = Transform.clean_customer_columns_for_matching(source_data_df, df_name="source_data_df")
            cleaned_df = Transform.clean_customer_columns_for_matching(cleaned_df, df_name="cleaned_df")

            # Check if fuzzy matching is needed
            FuzzyMatching.check_and_perform_fuzzy_matching(source_data_df, cleaned_df, table, data_source)

            if cleaned_df['Customer_Number'].isnull().all() and cleaned_df['Customer_Name'].isnull().all():
                # If both Customer_Number and Customer_Name columns are entirely null, load the results as is. Used for prospective customers
                Load.load_results(cleaned_df)
            else:
                # Backfill customer data with CIPNAME0 after getting fuzzy matching to get other customer info.
                updated_cleaned_df = Transform.update_additional_data(cleaned_df, source_data_df)

                # Extract CAT data and clean for matching
                cat_conxn_string = config['CAT']['cat_conxn_str']
                cat_data = ExtractCustomerData.get_cat_data(cat_conxn_string)
                cat_data_df = pd.DataFrame(cat_data)
                cat_data_df = Transform.clean_customer_columns_for_matching(cat_data_df, df_name="cat_data_df")

                # Update the raw customer data with the CAT data
                raw_customer_df = Transform.update_cat_data(updated_cleaned_df, cat_data_df)

                # Load the final results
                load_conxn_str = config['Database']['ResConxnString']
                Load.load_results(raw_customer_df,load_conxn_str)
                
                # Stored proc to de-dup & update?
            
if __name__ == '__main__':
    main()