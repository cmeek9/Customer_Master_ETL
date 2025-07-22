import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from customer_master_etl.modules import ExtractCustomerData, Transform, FuzzyMatching, CleanUp, Load
from customer_master_etl.src.config import config, logging

class TaskOrchestrator:
    def __init__(self):
        self.root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_sources_dir = os.path.join(self.root_dir, 'data_sources')

    def run(self, args):
        # Cleanup if weekly update
        if isinstance(args.data_source, list):
            logging.info("Running bronze deduplication for weekly update...")
            CleanUp.run_cleanup("bronze_dedup")
            logging.info("Running weekly update source cleanup...")
            CleanUp.run_cleanup("weekly_update_clean_up")

        # Cache CustomerMaster ONCE
        customer_master_string = config.get_customer_master_connection()
        source_data = ExtractCustomerData.get_source_data(customer_master_string)
        source_data_df = None
        if source_data is not None:
            source_data_df = Transform.clean_customer_columns_for_matching(
                pd.DataFrame(source_data), df_name="source_data_df"
            )

        # Determine data sources
        if isinstance(args.data_source, list):
            data_sources = args.data_source
        else:
            if args.data_source == 'CustomerMaster':
                data_sources = ['CustomerMaster']
            elif args.data_source:
                data_sources = [args.data_source]
            else:
                data_sources = [
                    name for name in os.listdir(self.data_sources_dir)
                    if os.path.isdir(os.path.join(self.data_sources_dir, name)) 
                    and name != 'CustomerMaster'
                ]

        for data_source in data_sources:
            logging.info(f"Processing data source: {data_source}")
            connection_string = config.get_datasource_connection(data_source)
            if not connection_string:
                continue
            is_excel = ExtractCustomerData.is_excel_source(data_source)
            if args.table:
                tables = [args.table]
            else:
                tables = ExtractCustomerData.get_tables_for_data_source(
                    self.data_sources_dir, data_source, is_excel
                )

            # --- Parallelize table processing if desired ---
            # with ThreadPoolExecutor(max_workers=4) as executor:
            #     futures = [
            #         executor.submit(self.process_table, data_source, table, is_excel, connection_string, source_data_df)
            #         for table in tables
            #     ]
            #     for future in as_completed(futures):
            #         future.result()
            # --- Or just process serially: ---
            for table in tables:
                self.process_table(data_source, table, is_excel, connection_string, source_data_df)

    def process_table(self, data_source, table, is_excel, connection_string, source_data_df):
        logging.info(f"Processing table: {table}")
        extracted_data = ExtractCustomerData.extract_and_process_data(
            data_source, table, is_excel, connection_string
        )
        if extracted_data is None:
            logging.warning(f"No data extracted for {data_source}.{table}")
            return

        cleaned_df = Transform.transform_pipeline(extracted_data)
        cleaned_df = Transform.clean_customer_columns_for_matching(
            cleaned_df, df_name="cleaned_df"
        )

        if source_data_df is not None:
            FuzzyMatching.check_and_perform_fuzzy_matching(
                source_data_df, cleaned_df, table, data_source
            )
        # Uncomment when ready to load:
        Load.load_results(cleaned_df, config.get_database_connection())
        logging.info(f"Finished processing {data_source}.{table}")