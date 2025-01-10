from sqlalchemy import create_engine, inspect
import pandas as pd
from customer_master_etl.modules.SEQLogging import SeqLog


def load_results(df, connection_string):
    '''
    Loads result set from ETL process.

    parameters:
        - df: the DataFrame to be loaded into SQL
        - connection_string: Database connection string

    returns:
        None
    '''
    engine = create_engine(connection_string, fast_executemany=True)
    
    # Clean the DataFrame
    cleaned_df = clean_string_columns(df, engine, 'Bronze_Customer_Master')
    
    # Filter columns to match SQL table
    inspector = inspect(engine)
    table_columns = inspector.get_columns('Bronze_Customer_Master', schema='Customer')
    sql_column_names = [col['name'] for col in table_columns]
    df_filtered = cleaned_df[cleaned_df.columns.intersection(sql_column_names)]
    
    # Load data
    try:
        df_filtered.to_sql(
            'Bronze_Customer_Master', 
            con=engine, 
            schema='Customer', 
            if_exists='append', 
            index=False
        )
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        print("\nDataFrame info:")
        print(df_filtered.info())
        raise
    
    # except Exception as e:
    #     seq_logger = SeqLog()
    #     seq_logger.error(f'An error has occurred: {str(e)}')
    #     return None



def clean_string_columns(df, engine, table_name, schema='Customer'):
    """
    Cleans all string columns in DataFrame by:
    1. Stripping whitespace
    2. Truncating to match SQL column lengths
    3. Converting None/NaN to empty string for string columns
    
    Parameters:
        df: DataFrame to clean
        engine: SQLAlchemy engine
        table_name: Target SQL table name
        schema: SQL schema name
    
    Returns:
        Cleaned DataFrame
    """
    inspector = inspect(engine)
    table_columns = inspector.get_columns(table_name, schema=schema)
    
    # Create a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    for col in table_columns:
        if col['name'] in cleaned_df.columns:
            # If it's a string/VARCHAR column
            if col['type'].__class__.__name__ in ('VARCHAR', 'String', 'NVARCHAR'):
                max_length = col['type'].length
                
                # Clean the column
                cleaned_df[col['name']] = cleaned_df[col['name']].apply(
                    lambda x: (str(x).strip()[:max_length] 
                             if isinstance(x, str) and x is not None 
                             else '') if pd.notna(x) else ''
                )
                
                # Log if any values were truncated
                original_lengths = df[col['name']].astype(str).apply(len)
                cleaned_lengths = cleaned_df[col['name']].apply(len)
                if (original_lengths > cleaned_lengths).any():
                    print(f"Warning: Some values in column '{col['name']}' were truncated to {max_length} characters")
    
    return cleaned_df




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
