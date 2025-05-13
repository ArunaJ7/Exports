from pymongo import MongoClient
import logging

logger = logging.getLogger('excel_data_writer')

def get_db_connection(config):
    """
    Connect to the MongoDB database using the provided configuration.
    """
    try:
        # Retrieve values from the config
        mongo_uri = config['DATABASE'].get('MONGO_URI', '').strip()
        db_name = config['DATABASE'].get('DB_NAME', '').strip()

        if not mongo_uri or not db_name:
            logger.error("Missing MONGO_URI or DB_NAME in the configuration.")
            return None

        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        logger.info(f"Connected to MongoDB successfully | Database name: {db_name}")
        return db
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        return None