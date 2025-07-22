from customer_master_etl.modules import SelectData
from customer_master_etl.orchestrator.task_orchestrator import TaskOrchestrator

def main():
    args = SelectData.parse_arguments()
    orchestrator = TaskOrchestrator()
    orchestrator.run(args)

if __name__ == '__main__':
    main()






# import os
# from customer_master_etl.modules import ExtractCustomerData, Load, Transform, FuzzyMatching, SelectData, CleanUp
# from customer_master_etl.src.config import config, logging

# def main():
#     root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#     data_sources_dir = os.path.join(root_dir, 'data_sources')

#     # Argument parser for specifying the data source and table
#     args = SelectData.parse_arguments()

#     if isinstance(args.data_source, list):
#         data_sources  = args.data_source
#         is_weekly_update = True
#     else:
#         is_weekly_update = False
#         # Special handling for CustomerMaster
#         if args.data_source == 'CustomerMaster':
#             data_sources = ['CustomerMaster']
#         else:
#             # Get all data sources except CustomerMaster
#             if args.data_source:
#                 # If a specific data source is requested (not CustomerMaster)
#                 data_sources = [args.data_source]
#             else:
#                 # Get all data sources except CustomerMaster
#                 data_sources = [
#                     name for name in os.listdir(data_sources_dir)
#                     if os.path.isdir(os.path.join(data_sources_dir, name)) 
#                     and name != 'CustomerMaster'
#                 ]
#     # if you call weekly udpate it cleans out the sources before the ETL getting new data for them
#     if is_weekly_update:
#         logging.info("Running bronze deduplication for weekly update...")
#         CleanUp.run_cleanup("bronze_dedup")

#         logging.info("Running weekly update source cleanup...")
#         CleanUp.run_cleanup("weekly_update_clean_up")

#     for data_source in data_sources:
#         logging.info(f"Processing data source: {data_source}")

#         # Get the connection string based on the data source
#         connection_string = config.get_datasource_connection(data_source)
#         if not connection_string:
#             continue

#         # Determine if the data source is an Excel source
#         is_excel = ExtractCustomerData.is_excel_source(data_source)
        
#         # Determine tables to process
#         if args.table:
#             tables = [args.table]
#         else:
#             # Get tables for the data source (Excel or SQL)
#             tables = ExtractCustomerData.get_tables_for_data_source(
#                 data_sources_dir, data_source, is_excel
#             )

#         for table in tables:
#             logging.info(f"Processing table: {table}")

#             # Extract and process data
#             extracted_data = ExtractCustomerData.extract_and_process_data(
#                 data_source, table, is_excel, connection_string
#             )

#             CustomerMasterString = config.get_customer_master_connection()
#             cleaned_df, source_data_df = Transform.clean_and_transform_customer_data(
#                 extracted_data, CustomerMasterString
#             )

#             FuzzyMatching.check_and_perform_fuzzy_matching(
#                 source_data_df, cleaned_df, table, data_source
#             )
#             print('code is working')
#             # if cleaned_df['Customer_Number'].isnull().all() and cleaned_df['Customer_Name'].isnull().all() or data_source == 'CustomerMaster':
#             #     load_conxn_str = config.get_database_connection()
#             #     Load.load_results(cleaned_df, load_conxn_str)
#             # else:
#             #     updated_cleaned_df = Transform.update_additional_data(cleaned_df, source_data_df)
#             #     cat_conxn_string = config.get_cat_connection()
#             #     cat_data_df = Transform.extract_and_clean_cat_data(cat_conxn_string)
#             #     raw_customer_df = Transform.update_cat_data(updated_cleaned_df, cat_data_df)
#             #     load_conxn_str = config.get_database_connection()
#             #     Load.load_results(raw_customer_df, load_conxn_str)

# if __name__ == '__main__':
#     main()