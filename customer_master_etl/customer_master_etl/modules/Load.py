from sqlalchemy import create_engine, inspect
from customer_master_etl.modules.SEQLogging import SeqLog 
import configparser


config = configparser.ConfigParser()
config.read('../config.ini')

connection_string = config['Database']['ResConxnString']

def load_results(df):
    '''
    Loads result set from ETL process.

    parameters:
        - df: the DataFrame to be loaded into SQL
        - connection_string: Database connection string

    returns:
        None
    '''

    engine = create_engine(connection_string, fast_executemany=True)

    # Get the column names from the SQL table
    inspector = inspect(engine)
    table_columns = inspector.get_columns('BronzeCustomerMaster')
    sql_column_names = [col['name'] for col in table_columns]

    # Filter the DataFrame to include only columns that exist in the SQL table
    df_filtered = df[df.columns.intersection(sql_column_names)]

    # Load data into the SQL table
    df_filtered.to_sql('BronzeCustomerMaster', con=engine, if_exists='append', index=False)
    print("Data loaded successfully.")


