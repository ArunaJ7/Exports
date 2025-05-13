import configparser
import os
import logging
import sys

logger = logging.getLogger('excel_data_writer')

def get_config():
    """
    Load and return the configuration from the Config.ini file.
    """
    try:
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), '../Config/Config.ini'))
        if not config.sections():
            logger.error("Configuration file is empty or not found.")
            sys.exit(1)
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)