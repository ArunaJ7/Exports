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
