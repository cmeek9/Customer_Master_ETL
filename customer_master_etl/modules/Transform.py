import pandas as pd
import time
import re
from customer_master_etl.modules import ExtractCustomerData
from customer_master_etl.src.config import logging

# --- Column Mapping Utilities ---

DEFAULT_COLUMN_MAPPING = {
    r'(?i)^first[\s_]?name$': 'First_Name',
    r'(?i)^last[\s_]?name$': 'Last_Name',
    r'(?i)^(customer|company)[\s_]?name$': 'Customer_Name',
    r'(?i)^name$': 'Customer_Name',
    r'(?i).*email.*': 'Customer_Email',
    # More specific phone patterns - order matters!
    r'(?i).*mobile[\s_]?phone.*': 'Customer_Mobile_Phone',
    r'(?i).*phone.*mobile.*': 'Customer_Mobile_Phone',
    r'(?i).*cell[\s_]?phone.*': 'Customer_Mobile_Phone',
    r'(?i).*phone.*cell.*': 'Customer_Mobile_Phone',
    r'(?i).*phone.*': 'Customer_Phone_Number',  # Keep this last as it's most general
    r'(?i).*address.*': 'Customer_Street_Address',
    r'(?i).*city.*': 'Customer_City',
    r'(?i).*state.*': 'Customer_State',
    r'(?i).*zip.*': 'Customer_ZipCode',
    r'(?i).*country.*': 'Customer_Country',
    r'(?i).*customer[\s_]?number.*': 'Customer_Number',
    r'(?i).*PARTSTORE DCNs*': 'DCN',
}

REQUIRED_COLUMNS = ['Customer_Name', 'Customer_Number']

def map_columns(df, mapping=DEFAULT_COLUMN_MAPPING):
    """Rename columns in df using regex mapping."""
    try:
        def map_col(col):
            for pattern, new_name in mapping.items():
                if re.match(pattern, col, re.IGNORECASE):
                    logging.info(f"Column '{col}' mapped to '{new_name}'.")
                    return new_name
            return col
        df = df.rename(columns=map_col)
        logging.info(f"map_columns functions successful.")
        return df
    except Exception as e:
        logging.error(f"Error in map_columns: {str(e)}")
        raise

def ensure_required_columns(df, required=REQUIRED_COLUMNS):
    """Add missing required columns with default values."""
    try:
        for col in required:
            if col not in df.columns:
                if col == 'Customer_Number':
                    df[col] = None
                else:
                    df[col] = ''
                logging.info(f"Added missing required column '{col}'.")
        return df
    except Exception as e:
        logging.error(f"Error in ensure_required_columns: {str(e)}")
        raise

def ensure_unique_columns(df):
    """Ensure column names are unique."""
    try:
        seen = {}
        new_cols = []
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_col = f"{col}_{seen[col]}"
                new_cols.append(new_col)
            else:
                seen[col] = 0
                new_cols.append(col)
        df.columns = new_cols
        return df
    except Exception as e:
        logging.error(f"Error in ensure_unique_columns: {str(e)}")
        raise

# --- Cleaning Functions ---

def clean_email(email):
    """Lowercase, strip, and validate email."""
    try:
        if not isinstance(email, str):
            return None
        cleaned_email = email.lower().strip()
        cleaned_email = re.sub(r'[^a-zA-Z0-9.@]', '', cleaned_email)
        if '@' not in cleaned_email:
            return None
        return cleaned_email
    except Exception as e:
        logging.error(f"Error cleaning email '{email}': {str(e)}")
        return None

def clean_email_column(df, col='Customer_Email'):
    """Clean email column in DataFrame."""
    try:
        if col in df.columns:
            df[col] = df[col].apply(clean_email)
        return df
    except Exception as e:
        logging.error(f"Error in clean_email_column: {str(e)}")
        raise

def clean_customer_columns(df):
    """Lowercase and strip key customer columns."""
    try:
        for col in ['Customer_Name', 'Customer_Number', 'CAT_DCN', 'CAT_UCID']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.lower()
        return df
    except Exception as e:
        logging.error(f"Error in clean_customer_columns: {str(e)}")
        raise

def clean_and_convert_to_number(x):
    """Cleans the input and converts it to an integer, or returns None."""
    try:
        result = int(str(x).strip())
        return result
    except Exception as e:
        logging.error(f"Error converting '{x}' to integer: {str(e)}")
        return None

# --- Orchestrator Functions ---

def reformat_excel_headers(df):
    """Standardize and clean Excel headers."""
    try:
        df = map_columns(df)
        df = ensure_unique_columns(df)
        df = ensure_required_columns(df)
        logging.info("Excel headers reformatted.")
        return df
    except Exception as e:
        logging.error(f"Error in reformat_excel_headers: {str(e)}")
        raise

def transform_pipeline(df):
    """Apply all cleaning steps to a DataFrame."""
    try:
        df = reformat_excel_headers(df)
        df = clean_email_column(df)
        df = clean_customer_columns(df)
        logging.info("Transform pipeline completed.")
        return df
    except Exception as e:
        logging.error(f"Error in transform_pipeline: {str(e)}")
        raise

# --- Merge/Update Functions ---

def update_additional_data(cleaned_df, source_data_df):
    """Update cleaned_df with additional data from source_data_df on Customer_Number."""
    try:
        merged_df = pd.merge(
            cleaned_df,
            source_data_df,
            on='Customer_Number',
            how='left',
            suffixes=('', '_source')
        )
        if 'Customer_Name_source' in merged_df.columns:
            merged_df['Customer_Name'] = merged_df['Customer_Name_source'].fillna(merged_df['Customer_Name'])
            logging.info("Customer_Name updated from source data.")
            merged_df = merged_df.drop(columns=['Customer_Name_source'])
        # Drop any other _source columns
        source_columns = [col for col in merged_df.columns if col.endswith('_source')]
        if source_columns:
            merged_df = merged_df.drop(columns=source_columns)
            logging.info(f"Dropped source columns: {source_columns}")
        merged_df = merged_df.drop_duplicates()
        logging.info("update_additional_data completed.")
        return merged_df
    except Exception as e:
        logging.error(f"Error in update_additional_data: {str(e)}")
        raise

def update_cat_data(cleaned_df, cat_data_df):
    """Merge cleaned_df with cat_data_df on Customer_Number and CAT_DCN."""
    try:
        cleaned_df['Customer_Number'] = cleaned_df['Customer_Number'].astype(str).str.strip()
        cat_data_df['CAT_DCN'] = cat_data_df['CAT_DCN'].astype(str).str.strip()
        merged_df = pd.merge(
            cleaned_df,
            cat_data_df,
            left_on='Customer_Number',
            right_on='CAT_DCN',
            how='left',
            suffixes=('', '_cat')
        )
        logging.info("update_cat_data completed.")
        return merged_df
    except Exception as e:
        logging.error(f"Error in update_cat_data: {str(e)}")
        raise

def clean_customer_columns_for_matching(df, df_name="DataFrame"):
    """Clean customer columns for matching (lowercase, strip, handle NaN)."""
    try:
        columns_to_clean = ['Customer_Name', 'Customer_Number', 'CAT_DCN', 'CAT_UCID']
        for column in columns_to_clean:
            if column in df.columns:
                df[column] = df[column].apply(
                    lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (float, int)) 
                    else (str(x).strip().lower() if pd.notnull(x) else None)
                )
                logging.info(f"Column '{column}' cleaned for matching in {df_name}.")
            else:
                logging.info(f"Column '{column}' not found in {df_name}.")
        return df
    except Exception as e:
        logging.error(f"Error in clean_customer_columns_for_matching: {str(e)}")
        raise

def extract_and_clean_cat_data(cat_conxn_string):
    """Extract and clean CAT data for matching."""
    try:
        cat_data = ExtractCustomerData.get_cat_data(cat_conxn_string)
        cat_data_df = pd.DataFrame(cat_data)
        cat_data_df = clean_customer_columns_for_matching(cat_data_df, df_name="cat_data_df")
        logging.info("extract_and_clean_cat_data completed.")
        return cat_data_df
    except Exception as e:
        logging.error(f"Error in extract_and_clean_cat_data: {str(e)}")
        raise

def clean_and_transform_customer_data(extracted_data, customer_master_string):
    """
    Clean and transform customer data for matching.
    Returns cleaned extracted data and cleaned source customer master data.
    """
    try:
        cleaned_df = transform_pipeline(extracted_data)
        logging.info("Extracted customer data cleaned.")
        source_data = ExtractCustomerData.get_source_data(customer_master_string)
        source_data_df = pd.DataFrame(source_data)
        source_data_df = clean_customer_columns_for_matching(source_data_df, df_name="source_data_df")
        cleaned_df = clean_customer_columns_for_matching(cleaned_df, df_name="cleaned_df")
        logging.info("Source and extracted data cleaned for matching.")
        return cleaned_df, source_data_df
    except Exception as e:
        logging.error(f"Error in clean_and_transform_customer_data: {str(e)}")
        raise