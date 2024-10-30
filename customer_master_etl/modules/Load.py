from sqlalchemy import create_engine, inspect
from customer_master_etl.modules import SEQLogging


def load_results(df, connection_string):
    '''
    Loads result set from ETL process.

    parameters:
        - df: the DataFrame to be loaded into SQL
        - connection_string: Database connection string

    returns:
        None
    '''
    try:
        engine = create_engine(connection_string, fast_executemany=True)

        # ensure the inserted columns match the target table
        inspector = inspect(engine)
        table_columns = inspector.get_columns('BronzeCustomerMaster')
        sql_column_names = [col['name'] for col in table_columns]

        # Filter the DataFrame to include only columns that exist in the SQL table
        df_filtered = df[df.columns.intersection(sql_column_names)]

        # # Truncate string data in the DataFrame to match the column sizes in the SQL table
        # for col in table_columns:
        #     if col['type'].__class__.__name__ == 'VARCHAR':
        #         max_length = col['type'].length
        #         if max_length:
        #             df_filtered[col['name']] = df_filtered[col['name']].apply(lambda x: str(x)[:max_length] if isinstance(x, str) else x)

        # Load data into the SQL table
        df_filtered.to_sql('Bronze_Customer_Master', con=engine, schema='Customer', if_exists='append', index=False)
        print("Data loaded successfully.")
    
    except Exception as e:
        seq_logger = SEQLogging()
        seq_logger.error(f'An error has occurred: {str(e)}')
        return None


# TEST for truncation issue, may be useful in the future.
# def load_results(df):
#     '''
#     Loads result set from ETL process and logs columns where string truncation would occur.

#     Parameters:
#         - df: the DataFrame to be loaded into SQL
#         - connection_string: Database connection string

#     Returns:
#         None
#     '''

#     engine = create_engine(connection_string, fast_executemany=True)

#     # Get the column names from the SQL table
#     inspector = inspect(engine)
#     table_columns = inspector.get_columns('BronzeCustomerMaster')
#     sql_column_names = [col['name'] for col in table_columns]

#     # Filter the DataFrame to include only columns that exist in the SQL table
#     df_filtered = df[df.columns.intersection(sql_column_names)]

#     # Check for columns where string truncation would occur and log the issue
#     for col in table_columns:
#         if col['type'].__class__.__name__ == 'VARCHAR':
#             max_length = col['type'].length
#             if max_length:
#                 # Find rows where the string length exceeds the column length
#                 too_long = df_filtered[col['name']].apply(lambda x: len(str(x)) > max_length if isinstance(x, str) else False)
                
#                 if too_long.any():
#                     # Get the longest value in the column for comparison
#                     max_value_length = df_filtered[col['name']].apply(lambda x: len(str(x)) if isinstance(x, str) else 0).max()
#                     print(f"Column '{col['name']}' is overwhelmed. Max allowed length: {max_length}, Max data length: {max_value_length}.")
                    
#                     # Optionally print or log the rows where truncation would occur
#                     print(f"Rows with issues in column '{col['name']}':\n{df_filtered[too_long][col['name']]}\n")
    
#     # Load data into the SQL table
#     # df_filtered.to_sql('BronzeCustomerMaster', con=engine, if_exists='append', index=False)
#     print("finished with the code.")
