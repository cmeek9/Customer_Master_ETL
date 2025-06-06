import os
from sqlalchemy import create_engine, text
from customer_master_etl.src.config import config, logging

# Mapping of task name to SQL file
CLEANUP_TASKS = {
    'bronze_dedup': 'DedupBronzeMaster.sql',
    'remove_nulls': 'RemoveNullsAndSpaces.sql',
    'weekly_update_clean_up': 'WeeklyUpdateCleanUp.sql'
}


def run_cleanup(task_name: str):
    """
    Executes a specified cleanup task by running the corresponding SQL script and logs affected rows if applicable.
    """
    try:
        if task_name not in CLEANUP_TASKS:
            raise ValueError(f"Invalid cleanup task: {task_name}. Valid options: {list(CLEANUP_TASKS.keys())}")

        logging.info(f"Starting cleanup task: {task_name}")

        # Load SQL script
        sql_file = CLEANUP_TASKS[task_name]
        sql_path = os.path.join(os.path.dirname(__file__), '..', 'cleanup_scripts', sql_file)

        with open(sql_path, 'r') as f:
            cleanup_sql = f.read()

        # Connect and run with row count tracking
        connection_string = config.get_database_connection()
        engine = create_engine(connection_string, echo=False, fast_executemany=True)

        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(cleanup_sql)
            affected_rows = cursor.rowcount
            conn.commit()
        finally:
            conn.close()

        logging.info(f"Cleanup task '{task_name}' executed successfully. Rows affected: {affected_rows}")

    except Exception as e:
        logging.error(f"Error running cleanup task '{task_name}': {str(e)}")
        raise




# def run_cleanup(task_name: str):
#     """
#     Executes a specified cleanup task by running the corresponding SQL script.

#     Parameters:
#     task_name (str): The name of the cleanup task to execute. Must be one of the keys in the CLEANUP_TASKS dictionary.

#     Returns:
#     None: This function does not return a value. It executes the SQL script associated with the given task name.
#     """
#     try:
#         if task_name not in CLEANUP_TASKS:
#             raise ValueError(f"Invalid cleanup task: {task_name}. Valid options: {list(CLEANUP_TASKS.keys())}")

#         logging.info(f"Starting cleanup task: {task_name}")

#         # Get SQL file path
#         sql_file = CLEANUP_TASKS[task_name]
#         sql_path = os.path.join(os.path.dirname(__file__), '..', 'cleanup_sql', sql_file)

#         # Read SQL
#         with open(sql_path, 'r') as f:
#             cleanup_sql = f.read()

#         # Execute SQL using SQLAlchemy
#         connection_string = config.get_database_connection()
#         engine = create_engine(connection_string, echo=False, fast_executemany=True)
#         with engine.begin() as connection:
#             connection.execute(text(cleanup_sql))

#         logging.info(f"Cleanup task '{task_name}' executed successfully.")

#     except Exception as e:
#         logging.error(f"Error running cleanup task '{task_name}': {str(e)}")
#         raise
