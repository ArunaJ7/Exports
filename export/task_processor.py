import logging
import configparser
from importlib import import_module
from logging import getLogger
import traceback
from utils.connectionMongo import MongoDBConnectionSingleton
import os
from datetime import datetime

logger = getLogger()

# Load task configuration from ini file
config_parser = configparser.ConfigParser()
config_parser.read('config/core_config.ini')

def process_tasks():
    """Process tasks from System_tasks collection based on coreConfig.ini"""
    logger.info("Starting task processing from System_tasks collection")
    
    total_error_count = 0
    environment = config_parser.get('environment', 'current', fallback='development')
    
    try:
        # Get template task IDs and names for current environment
        template_section = f'template_task_id_{environment}'
        if template_section not in config_parser:
            raise Exception(f"Missing template task ID section for environment: {environment}")
        
        # Create mapping of template IDs to task names
        template_tasks = {
            tid: name for tid, name in config_parser[template_section].items()
        }
        
        # Get all export task IDs (all defined template tasks are considered potential exports)
        export_task_ids = list(template_tasks.keys())
        logger.info(f"Processing these template tasks: {template_tasks}")

        # Get all open system tasks that have matching template_task_ids
        system_tasks = MongoDBConnectionSingleton.System_tasks.find({
            "Template_Task_Id": {"$in": export_task_ids},
            "task_status": "Open"
        })
        
        task_list = list(system_tasks)
        logger.info(f"Found {len(task_list)} tasks to process")

        for task in task_list:
            task_error_count = 0
            template_task_id = task["Template_Task_Id"]
            task_id = task["Task_Id"]
            task_name = template_tasks.get(template_task_id, f"Unknown Task {template_task_id}")

            try:
                logger.info(f"Processing {task_name} (ID: {task_id})")
                MongoDBConnectionSingleton.System_tasks.update_one(
                    {"Task_Id": task_id},
                    {"$set": {"task_status": "InProgress"}}
                )

                # Check if this is an export task (all defined tasks are considered exports)
                if template_task_id in template_tasks:
                    try:
                        # Import the appropriate export function
                        export_function = get_export_function(template_task_id)
                        
                        # Get parameters from task document
                        params = task.get("parameters", {})
                        
                        # Execute the export
                        success = export_function(**params)
                        
                        if not success:
                            logger.warning(f"{task_name} returned unsuccessful status")
                            task_error_count += 1
                        
                        # Update task with export info
                        update_data = {
                            "task_status": "Complete" if success else "Failed",
                            "task_description": f"{task_name} completed with {task_error_count} errors"
                        }
                        
                        # Add export file info if available
                        export_dir = "exports"
                        latest_export = get_latest_export(export_dir, template_task_id)
                        if latest_export:
                            update_data.update({
                                "export_path": os.path.abspath(latest_export),
                                "export_status": "Generated" if success else "Failed",
                                "export_filename": os.path.basename(latest_export)
                            })
                        
                        MongoDBConnectionSingleton.System_tasks.update_one(
                            {"Task_Id": task_id},
                            {"$set": update_data}
                        )
                        
                    except Exception as e:
                        logger.error(f"{task_name} failed: {str(e)}")
                        task_error_count += 1
                        raise

                logger.info(f"{task_name} completed with {task_error_count} errors")

            except Exception as task_error:
                MongoDBConnectionSingleton.System_tasks.update_one(
                    {"Task_Id": task_id},
                    {"$set": {
                        "task_status": "Failed",
                        "task_description": f"{task_name} failed: {str(task_error)}"
                    }}
                )
                logger.error(f"Error processing {task_name}: {str(task_error)}\n{traceback.format_exc()}")
                task_error_count += 1
            finally:
                total_error_count += task_error_count

        logger.info(f"Processed {len(task_list)} tasks with {total_error_count} total errors")
        return total_error_count == 0

    except Exception as e:
        logger.error(f"Task processing failed: {str(e)}\n{traceback.format_exc()}")
        raise

def get_export_function(template_task_id):
    """Dynamically import and return the appropriate export function"""
    # Map template task IDs to their respective functions
    export_functions = {
        '20': 'excel_incident_detail',
        '21': 'approval_list_export',
        '22': 'drc_summary_export',
        '23': 'drc_summary_rtom_export',
        '24': 'cpe_export',
        '25': 'rejected_case_export',
        '26': 'direct_lod_export'
    }
    
    function_name = export_functions.get(template_task_id)
    if not function_name:
        raise ValueError(f"No export function defined for template task ID {template_task_id}")
    
    try:
        module = import_module('exports.excel_exports')  # Assuming all exports are in this module
        return getattr(module, function_name)
    except Exception as e:
        raise ImportError(f"Could not import export function {function_name}: {str(e)}")

def get_latest_export(directory, template_task_id):
    """Find the most recent export file for a specific task type"""
    try:
        if not os.path.exists(directory):
            return None
            
        # Mapping of template task IDs to their file prefixes
        file_prefixes = {
            '20': "incidents_details_",
            '21': "approval_list_",
            '22': "drc_summary_",
            '23': "drc_summary_rtom_",
            '24': "cpe_export_",
            '25': "rejected_export_",
            '26': "direct_lod_"
        }
        
        prefix = file_prefixes.get(template_task_id, "export_")
        files = [
            os.path.join(directory, f) 
            for f in os.listdir(directory) 
            if f.startswith(prefix) and f.endswith(".xlsx")
        ]
                
        return max(files, key=os.path.getmtime) if files else None
        
    except Exception as e:
        logger.error(f"Error finding latest export: {str(e)}")
        return None