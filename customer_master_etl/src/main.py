import configparser
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
            # Get tables for the data source (Excel or SQL)
            tables = ExtractCustomerData.get_tables_for_data_source(data_sources_dir, data_source, is_excel)

            # Exclude the 'CustomerMaster' SQL when processing the 'DBS' data source
            if data_source == 'DBS':
                tables = [table for table in tables if table != 'CustomerMaster']

        for table in tables:
            print(f"Processing table: {table}")

            # Extract and process data
            extracted_data = ExtractCustomerData.extract_and_process_data(data_source, table, is_excel, connection_string)

            # Cleaning and transforming
            CustomerMasterString = config['CustomerMaster']['connection_string']
            cleaned_df, source_data_df = Transform.clean_and_transform_customer_data(extracted_data, CustomerMasterString)

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
                cat_data_df = Transform.extract_and_clean_cat_data(cat_conxn_string)

                # Update the raw customer data with the CAT data
                raw_customer_df = Transform.update_cat_data(updated_cleaned_df, cat_data_df)

                # Load the final results
                # load_conxn_str = config['Database']['ResConxnString']
                # Load.load_results(raw_customer_df,load_conxn_str)
                
                # Stored proc to de-dup & update?
            
if __name__ == '__main__':
    main()