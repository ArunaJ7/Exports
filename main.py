from manipulation.task_manager import TaskManager
from utils.logger import SingletonLogger
from tasks.config_loader import ConfigLoaderSingleton

SingletonLogger.configure()

logger = SingletonLogger.get_logger('appLogger')
db_logger = SingletonLogger.get_logger('dbLogger')

def main():
    """Main entry point to run task processing"""
    logger.info("Starting task processing script (single execution)...")
    try:
        task_manager = TaskManager()
        task_manager.execute_tasks()
        logger.info("Task processing completed successfully")
    except Exception as e:
        logger.error(f"Task processing failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    logger.debug("Entering main execution block")
    main()