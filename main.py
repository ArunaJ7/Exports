import logging
import logging.config
from export.task_processor import process_tasks

# Load logger configuration
logging.config.fileConfig('config/logger/loggers.ini')
logger = logging.getLogger('excel_data_writer')

def main():
    """Main entry point to run task processing"""
    logger.info("Starting task processing script (single execution)...")
    try:
        process_tasks()
        logger.info("Task processing completed successfully")
    except Exception as e:
        logger.error(f"Task processing failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    logger.debug("Entering main execution block")
    main()