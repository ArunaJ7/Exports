'''
Purpose: This module manages the execution of system tasks by fetching open tasks from MongoDB and delegating them to appropriate handlers.
Created Date: 2025-01-18
Created By: Aruna Jayaweera (ajayaweerau@gmail.com)
Last Modified Date: 2024-02-20
Modified By: Aruna Jayaweera (ajayaweerau@gmail.com)
Version: Python 3.12
Dependencies: 
    - pymongo (MongoDB driver)
    - logging (for error handling and tracking)
Related Files: 
    - config_loader.py (for task configuration)
    - connectionMongo.py (for database connection)
    - task_handler.py (for task execution logic)
    - logger.py (for logging functionality)

Program Description:
1. Initialization:
    - Establishes MongoDB connection using MongoDBConnectionSingleton
    - Loads template task IDs from configuration using ConfigLoaderSingleton
    - Initializes TaskHandlers for executing specific tasks

2. Task Execution Flow:
    a. Retrieves all open tasks from System_tasks collection where:
        - Template_Task_Id matches configured template IDs
        - task_status equals "open"
    
    b. For each matching task:
        - Extracts task parameters
        - Finds corresponding handler method (handle_task_{template_id})
        - Executes the handler with task parameters
    
3. Error Handling:
    - Validates presence of template task IDs
    - Checks for handler method availability
    - Logs detailed errors for failed task executions

4. Data Flow:
    - Tasks are read from System_tasks collection
    - Handlers process tasks according to their Template_Task_Id
    - Execution results are logged via appLogger

MongoDB Collections:
    - System_tasks (primary collection for task management)
    - Other collections accessed through individual task handlers

Task Types Supported:
    - All task types defined in core_config.ini under [template_task_id_{environment}]
    - Each task type requires a corresponding handler method in TaskHandlers class
'''

# task_manager.py
from utils.config_loader import ConfigLoaderSingleton
from tasks.task_handler import TaskHandlers
import logging
from utils.connectionMongo import MongoDBConnectionSingleton

logger = logging.getLogger('appLogger')

class TaskManager:
    def __init__(self):
        self.template_ids = ConfigLoaderSingleton().get_template_task_ids()
        
    def execute_tasks(self):
        if not self.template_ids:
            logger.error("No template task IDs found in config.")
            return

        task_handlers = TaskHandlers()

        try:
            with MongoDBConnectionSingleton() as db:
                system_tasks_collection = db['System_tasks_inprogress']
                
                # Query for tasks with matching template IDs and open status
                query = {
                    "Template_Task_Id": { "$in": self.template_ids },
                    "task_status": { "$in": ["open"] }
                }

                for task in system_tasks_collection.find(query):
                    try:
                        template_id = task.get("Template_Task_Id")
                        params = task.get("parameters", {})
                        
                        # Call the unified handler with template_id and parameters
                        task_handlers.handle_task(template_id, **params)
                        logger.info(f"Successfully executed task {template_id}")
                        
                    except Exception as task_error:
                        logger.error(f"Task {task.get('_id')} failed: {task_error}", exc_info=True)
                        # Optionally update task status to 'failed' here

        except Exception as db_error:
            logger.error(f"Database operation failed: {db_error}", exc_info=True)