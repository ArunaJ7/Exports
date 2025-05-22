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



# task_manager.py (Main Task Manager)
from tasks.config_loader import ConfigLoaderSingleton
from tasks.task_handler import TaskHandlers
import logging

logger = logging.getLogger('appLogger')

class TaskManager:
    def __init__(self):
        from utils.connectionMongo import MongoDBConnectionSingleton
        self.db_client = MongoDBConnectionSingleton().get_database()
        self.template_ids = ConfigLoaderSingleton().get_template_task_ids()
        self.task_handlers = TaskHandlers(self.db_client)

    def execute_tasks(self):
        if not self.template_ids:
            logger.error("No template task IDs found in config.")
            return

        system_task_collection = self.db_client['System_tasks']

        for task_id in self.template_ids:
            tasks = system_task_collection.find({"Template_Task_Id": task_id, "task_status": "open"})
            for task in tasks:
                self.process_task(task)

    def process_task(self, task):
        template_id = task.get("Template_Task_Id")
        params = task.get("parameters", {})

        try:
            if hasattr(self.task_handlers, f"handle_task_{template_id}"):
                handler = getattr(self.task_handlers, f"handle_task_{template_id}")
                handler(**params)
                logger.info(f"Task {template_id} executed successfully.")
            else:
                logger.error(f"No handler found for template_task_id {template_id}")
        except Exception as e:
            logger.error(f"Error executing task {template_id}: {e}", exc_info=True)
