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


# task_handler.py
import logging
from datetime import datetime
from utils.connectionMongo import MongoDBConnectionSingleton
from export.incident_list import excel_incident_detail
from export.incident_open_for_distribution import excel_incident_open_distribution
from export.pending_reject_list import excel_pending_reject_incident
from export.cpe_list import excel_cpe_detail
from export.direct_lod import excel_direct_lod_detail
from export.rejected_list import excel_rejected_detail
from export.case_distribution_drc_transaction import excel_case_distribution_detail
from export.case_distribution_drc_transaction_batch_list import excel_case_distribution_transaction_batch_detail
from export.case_distribution_drc_transaction_batch_list_distribution_array import excel_case_distribution_transaction_batch_distribution_array_detail

logger = logging.getLogger('appLogger')

class TaskHandlers:
    def handle_task(self, template_id: int, **params):
        """Handle tasks using match statement (Python 3.10+ switch-case equivalent)"""
        try:
            match template_id:
                case 20:
                    return excel_incident_detail(
                        params.get('action_type'),
                        params.get('status'),
                        params.get('from_date'),
                        params.get('to_date')
                    )
                case 21:
                    return excel_incident_open_distribution()
                case 22:
                    return excel_pending_reject_incident(
                        params.get('drc_commission_rules'),
                        params.get('from_date'),
                        params.get('to_date')
                    )
                case 23:
                    return excel_direct_lod_detail(
                        params.get('from_date'),
                        params.get('to_date'),
                        params.get('drc_commission_rules')
                    )
                case 24:
                    return excel_cpe_detail(
                        params.get('from_date'),
                        params.get('to_date'),
                        params.get('drc_commission_rules')
                    )
                case 25:
                    return excel_rejected_detail(
                        params.get('action_type'),
                        params.get('drc_commission_rules'),
                        params.get('from_date'),
                        params.get('to_date')
                    )
                case 26:
                    return excel_case_distribution_detail(
                        params.get('arrears_band'),
                        params.get('drc_commission_rules'),
                        params.get('from_date'),
                        params.get('to_date')
                    )
                case 27:
                    return excel_case_distribution_transaction_batch_detail(
                        params.get('case_distribution_batch_id')
                    )
                case 28:
                    return excel_case_distribution_transaction_batch_distribution_array_detail(
                        params.get('case_distribution_batch_id'),
                        params.get('batch_seq')
                    )
                case _:
                    logger.error(f"No handler for template ID: {template_id}")
                    raise ValueError(f"Unknown template_id: {template_id}")
        
        except Exception as e:
            logger.error(f"Error executing task {template_id}: {str(e)}", exc_info=True)
            raise