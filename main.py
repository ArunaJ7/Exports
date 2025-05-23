'''
Purpose: This script serves as the main entry point for executing scheduled tasks in the system
Created Date: 2025-03-18
Created By: Aruna Jayaweera (ajayaweerau@gmail.com)
Last Modified Date: 2024-04-20
Modified By: Aruna Jayaweera (ajayaweerau@gmail.com)
Version: Python 3.12

Dependencies:
    - manipulation.task_manager (for task execution logic)
    - utils.logger (for logging configuration)

Related Files:
    - task_manager.py (contains core task execution logic)
    - logger.py (handles logging configuration)
    - All task-specific modules called by the TaskManager

Program Description:
1. Core Functionality:
    - Initializes and configures the application logging system
    - Creates and executes the TaskManager instance
    - Handles top-level execution flow and error handling
    - Provides logging for the entire task execution lifecycle

2. Execution Flow:
    - Configures logging via SingletonLogger on startup
    - Initializes TaskManager instance
    - Executes all pending tasks via TaskManager
    - Provides success/failure logging for the entire process

3. Key Features:
    - Centralized logging configuration
    - Comprehensive error handling
    - Clean separation of concerns between main execution and task logic
    - Support for both direct execution and module import

4. Configuration:
    - Logging configured via SingletonLogger
    - Task execution handled by TaskManager
    - Two logger instances:
        * appLogger: General application logging
        * dbLogger: Database-specific logging

5. Integration Points:
    - Primary interface for scheduled task execution
    - Works with TaskManager to coordinate all system tasks
    - Utilizes centralized logging system

Technical Specifications:
    - Entry Point: main() function
    - Error Handling: Catches and logs all exceptions
    - Logging: 
        - INFO level for normal operation
        - ERROR level for failures
        - DEBUG level for execution tracing
    - Execution Mode: Designed for both single runs and scheduled execution

Usage:
    - Run directly via Python interpreter for one-time execution
    - Can be called by scheduling systems (cron, Windows Task Scheduler)
    - Importable as a module for integration with larger systems

Error Handling:
    - Catches all exceptions at top level
    - Logs full error details including stack trace
    - Propagates exceptions after logging
'''

from manipulation.task_manager import TaskManager
from utils.logger import SingletonLogger

SingletonLogger.configure()

logger = SingletonLogger.get_logger('appLogger')


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
    main()