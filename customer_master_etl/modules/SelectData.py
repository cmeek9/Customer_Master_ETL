import argparse
from customer_master_etl.src.config import logging

# this function would be used if somebody would pick from a possible GUI.
#  else we'd need bat scripts picking each possible option.

# can add to groupings as this gets move advanced and/or defined.
# Weekly updates will include all SQL sources for now.
WEEKLY_UPDATE_SOURCES = ['CODA', 'DBS', 'CloudLink']

def select_option(options, prompt):
    """
    Display a list of options and prompt the user to select one.

    Parameters:
        options (list): A list of options to display for selection.
        prompt (str): A message to display to the user before showing the options.

    Returns:
        str: The selected option from the list.
    """
    try:
        logging.info("Displaying options to the user.")
        print(prompt)
        for i, option in enumerate(options, start=1):
            print(f"{i}. {option}")
        choice = int(input("Select an option: ")) - 1

        if choice < 0 or choice >= len(options):
            raise ValueError(f"Invalid option selected: {choice + 1}. Must be between 1 and {len(options)}.")
        
        logging.info(f"User selected option: {options[choice]}.")
        return options[choice]
    except Exception as e:
        logging.error(f"An error occurred in select_option. Error: {str(e)}")
        raise

def parse_arguments():
    """
    Parse command-line arguments for data extraction.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.
            The object has two attributes:
            - data_source (str or None): The specified data source, if any.
            - table (str or None): The specified table, if any.
    """
    try:
        logging.info("Parsing command-line arguments.")

        parser = argparse.ArgumentParser(description='Extract data from a specific table or extract all from the database')
        parser.add_argument(
            'data_source', 
            metavar='DATA_SOURCE', 
            nargs='?', 
            type=str, 
            help='Enter the data source you wish to extract from, if any.'
        )
        parser.add_argument(
            'table', 
            metavar='TABLE', 
            nargs='?', 
            type=str, 
            help='Enter the table you wish to extract, if any.'
        )

        args = parser.parse_args()

        if args.data_source and args.data_source.lower() == 'weeklyupdate':
            args.data_source = WEEKLY_UPDATE_SOURCES
            args.table = None

        logging.info(f"Parsed arguments: data_source={args.data_source}, table={args.table}.")
        return args

    except Exception as e:
        logging.error(f"An error occurred while parsing arguments. Error: {str(e)}")
        raise