from logging import getLogger
import traceback
from utils.connectionMongo import MongoDBConnectionSingleton
import os

logger = getLogger()



logger = get_logger("db_operations")


def get_open_tasks():
    upload_task_number = config.get("upload_task_number")
    system_tasks_inprogress_collection = db["System_tasks_Inprogress"]
    
    
    logger.info(f"Cleaning up completed/failed tasks from System_tasks_Inprogress...")

    try:
        # Delete completed or failed tasks
        cleanup_result = system_tasks_inprogress_collection.delete_many({
            "task_status": {"$in": ["Completed", "Failed"]}
        })
        logger.info(f"Deleted {cleanup_result.deleted_count} completed/failed tasks from System_tasks_Inprogress.")
    except Exception as e:
        logger.exception("Failed to clean up System_tasks_Inprogress.")
  
    logger.info(f"Fetching open tasks with Template task ID {upload_task_number} && task_status = 'Open'...")
