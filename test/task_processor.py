import logging
import configparser
from importlib import import_module
from logging import getLogger

logger = getLogger()

# Load task configuration from ini file
config_parser = configparser.ConfigParser()
config_parser.read('Config/coreConfig.ini')

# Create Task_list from coreconfig.ini (only task IDs)
Task_list = [task_id for task_id in config_parser['Tasks'].keys()]

def process_tasks():
    """Process tasks by calling functions specified in coreConfig.ini"""
    try:
        task_index = 0

        while task_index < len(Task_list):
            task_id = Task_list[task_index]
            task_section = f"Task_{task_id}"

            # Check if task section exists in tasks.ini
            if task_section not in config_parser:
                logger.warning(f"No configuration found for Task_Id {task_id}")
                task_index += 1
                continue

            # Get task details
            try:
                task_config = config_parser[task_section]
                function_name = task_config.get('function_name')
                module_path = task_config.get('module_path')

                if not function_name or not module_path:
                    logger.warning(f"Missing function_name or module_path for Task_Id {task_id}")
                    task_index += 1
                    continue

                # Import the module and get the function
                module = import_module(module_path)
                task_function = getattr(module, function_name)

                # Parse parameters
                params = {}
                for key, value in task_config.items():
                    if key not in ['function_name', 'module_path']:
                        # Convert 'None' string to None, handle other values
                        if value.lower() == 'none':
                            params[key] = None
                        else:
                            params[key] = value

                # Log and execute the task
                logger.info(f"Processing Task_Id {task_id} with function {function_name} and params {params}")
                success = task_function(**params)

                if success:
                    logger.info(f"Task {task_id} processed successfully")
                else:
                    logger.warning(f"Task {task_id} processing failed or no data found")

            except Exception as e:
                logger.error(f"Task {task_id} processing failed: {str(e)}", exc_info=True)
                success = False

            task_index += 1

    except Exception as e:
        logger.error(f"Task processing failed: {str(e)}", exc_info=True)
        raise