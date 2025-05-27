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


# task_manager.py (Simplified)
from tasks.config_loader import ConfigLoaderSingleton
from tasks.task_handler import TaskHandlers
import logging
from utils.connectionMongo import MongoDBConnectionSingleton

logger = logging.getLogger('appLogger')

class TaskManager:
    def __init__(self):
        self.template_ids = ConfigLoaderSingleton().get_template_task_ids()
        
    def execute_tasks(self):
        """Simplified task processing in a single method"""
        if not self.template_ids:
            logger.error("No template task IDs found in config.")
            return

        try:
            with MongoDBConnectionSingleton() as db:
                task_handlers = TaskHandlers()
                system_tasks_collection = db['System_tasks']
                
                for task_id in self.template_ids:
                    try:
                        # Get all open tasks for this template ID
                        for task in system_tasks_collection.find({
                            "Template_Task_Id": task_id,
                            "task_status": "open"
                        }):
                            try:
                                # Process each task
                                template_id = task.get("Template_Task_Id")
                                params = task.get("parameters", {})
                                
                                # Find and execute the handler
                                handler = getattr(task_handlers, f"handle_task_{template_id}", None)
                                if not handler:
                                    logger.error(f"No handler for template {template_id}")
                                    continue
                                    
                                handler(**params)
                                logger.info(f"Successfully executed task {template_id}")
                                
                            except Exception as task_error:
                                logger.error(f"Task {task.get('_id')} failed: {task_error}", exc_info=True)
                                
                    except Exception as template_error:
                        logger.error(f"Error processing template {task_id}: {template_error}", exc_info=True)
                        
        except Exception as db_error:
            logger.error(f"Database error: {db_error}", exc_info=True)


