import threading
import configparser
from pathlib import Path
from pymongo import MongoClient
from utils.logger import SingletonLogger


class MongoDBConnectionSingleton:
    #get connection
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(MongoDBConnectionSingleton, cls).__new__(cls)
                    cls._instance._initialize_connection()
        return cls._instance

    def _initialize_connection(self):
      self.logger = SingletonLogger.get_logger('dbLogger')
      try:
          # Load connection details from config
          project_root = Path(__file__).resolve().parents[1]
          config_path = project_root / 'config' / 'core_config.ini'
          config = configparser.ConfigParser()
          config.read(str(config_path))

          if 'environment' not in config or 'current' not in config['environment']:
              raise KeyError("Missing [environment] section or 'current' key in core_config.ini")

          env = config['environment']['current'].lower()
          section = f'mongo_database_{env}'

          if section not in config:
              raise KeyError(f"Missing section [{section}] in config file.")

          mongo_uri = config[section].get('MONGO_HOST', '')
          mongo_dbname = config[section].get('MONGO_DATABASE', '')

          if not mongo_uri or not mongo_dbname:
              raise ValueError(f"MongoDB URI or database name missing in [{section}] configuration.")

          self.client = MongoClient(mongo_uri)
          self.database = self.client[mongo_dbname]  # Correctly set the Database object

          self.logger.info("MongoDB connection established successfully.")

      except KeyError as key_err:
          self.logger.error(f"Configuration error: {key_err}")
          self.client = None
          self.database = None
      except Exception as err:
          self.logger.error(f"Error connecting to MongoDB: {err}")
          self.client = None
          self.database = None

          
    def get_database(self):
        return self.database

    def close_connection(self):
        if self.client:
            try:
                self.client.close()
                self.logger.info("MongoDB connection closed.")
                self.client = None
                self.database = None
                MongoDBConnectionSingleton._instance = None
            except Exception as err:
                self.logger.error(f"Error closing MongoDB connection: {err}")

    def __enter__(self):
        return self.get_database()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()


