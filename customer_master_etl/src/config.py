import os
import configparser
from customer_master_etl.modules.SEQLogging import SeqLog

class Config:
    def __init__(self):
        self.config = configparser.ConfigParser(interpolation=None)
        self._load_config()
        self._init_logging()

    def _load_config(self):
        """Load the configuration file using the same path logic as main.py"""
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(root_dir, 'config.ini')
        self.config.read(config_path)

    def _init_logging(self):
        """Initialize SEQ logging"""
        self.seq_url = self.config['Seq']['url']
        self.seq_key = self.config['Seq']['key']
        self.logging = SeqLog(self.seq_url, self.seq_key)

    def get_datasource_connection(self, data_source):
        """Get connection string for a data source from DataSources section"""
        try:
            return self.config['DataSources'][data_source]
        except KeyError:
            self.logging.info(f"Error: No connection string found for data source '{data_source}'.")
            return None

    def get_customer_master_connection(self):
        """Get CustomerMaster connection string"""
        return self.config['CustomerMaster']['connection_string']

    def get_database_connection(self):
        """Get Database ResConxnString"""
        return self.config['Database']['ResConxnString']

    def get_cat_connection(self):
        """Get CAT connection string"""
        return self.config['CAT']['cat_conxn_str']

    def get_all_datasources(self):
        """Get all available data sources"""
        return list(self.config['DataSources'].keys())

# Create instance
config = Config()
logging = config.logging