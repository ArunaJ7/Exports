'''
Purpose: This module contains task handler implementations for processing specific system tasks, starting with Incident Export (Task ID 20)
Created Date: 2025-01-18  
Created By: Aruna Jayaweera (ajayaweerau@gmail.com)
Last Modified Date: 2024-02-20
Modified By: Aruna Jayaweera (ajayaweerau@gmail.com)
Version: Python 3.12
Dependencies:
    - logging (for execution tracking)
    - datetime (for date handling)
    - openpyxl (for Excel export functionality)
    - pymongo (for database operations)
Related Files:
    - incident_list.py (primary export implementation)
    - style_loader.py (Excel formatting styles)
    - task_manager.py (task executor)
    - connectionMongo.py (database connection)

Program Description:
1. Task Handler Structure:
    - Initializes with MongoDB client connection
    - Contains dedicated methods for each task type (handle_task_XX)
    - Follows consistent naming convention (handle_task_{template_id})

2. Incident Export Task (ID 20):
    a. Parameters Accepted:
        - action_type: Type of incident action (e.g., "collect arrears")
        - status: Incident status filter (e.g., "Incident Open")
        - from_date/to_date: Date range filter (YYYY-MM-DD format)
    
    b. Execution Flow:
        1. Logs task initiation
        2. Delegates to excel_incident_detail() in incident_list.py
        3. Handles success/failure responses
        4. Provides detailed logging

3. Error Handling:
    - Logs export failures with error details
    - Returns success status to calling function
    - Maintains consistent logging format

4. Data Flow:
    - Receives parameters from TaskManager
    - Passes parameters to incident_list.py
    - Returns boolean status to caller

Integration Points:
    - Called by TaskManager.process_task()
    - Utilizes excel_incident_detail() for core functionality
    - Shares logger with main application

Future Extensibility:
    - Additional task handlers can be added following same pattern
    - New handle_task_XX() methods automatically discovered by TaskManager
    - Shared MongoDB client minimizes connection overhead
'''

import logging
from datetime import datetime
import os
from utils.style_loader import STYLES
from export.incident_list import excel_incident_detail
from export.incident_open_for_distribution import excel_incident_open_distribution

logger = logging.getLogger('appLogger')

class TaskHandlers:
    

    def handle_task_20(self, action_type=None, status=None, from_date=None, to_date=None):
        """Handles Incident Export Task (Task ID 20)"""
        logger.info("Executing Incident Export Task (Task ID 20)...")
        
        success = excel_incident_detail(action_type, status, from_date, to_date)

        if success:
            logger.info("Incident report successfully exported.")
        else:
            logger.error("Failed to export incident report.")


