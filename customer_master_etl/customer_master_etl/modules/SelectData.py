import argparse

def select_option(options, prompt):
    """
    Display a list of options and prompt the user to select one.

    Parameters:
    options (list): A list of options to display for selection.
    prompt (str): A message to display to the user before showing the options.

    Returns:
    str: The selected option from the list.
    """
    print(prompt)
    for i, option in enumerate(options, start=1):
        print(f"{i}. {option}")
    choice = int(input("Select an option: ")) - 1
    return options[choice]




def parse_arguments():
    """
    Parse command-line arguments for data extraction.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.
            The object has two attributes:
            - data_source (str or None): The specified data source, if any.
            - table (str or None): The specified table, if any.
    """
    parser = argparse.ArgumentParser(description='Extract data from a specific table or extract all from the database')
    parser.add_argument('data_source', metavar='DATA_SOURCE', nargs='?', type=str, help='Enter the data source you wish to extract from, if any.')
    parser.add_argument('table', metavar='TABLE', nargs='?', type=str, help='Enter the table you wish to extract, if any.')
    return parser.parse_args()