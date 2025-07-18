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
from export._20_incident import excel_incident_detail
from export._21_incident_open import excel_incident_open_distribution
from export._22_pending_reject import excel_pending_reject_incident
from export._23_direct_lod import excel_direct_lod_detail
from export._24_cpe import excel_cpe_detail
from export._25_rejected import excel_rejected_detail
from export._26_case_distribution_drc_transaction import excel_case_distribution_detail
from export._27_case_distribution_drc_transaction_batch_list import excel_case_distribution_transaction_batch_detail
from export._28_case_distribution_drc_transaction_batch_list_distribution_array import excel_case_distribution_transaction_batch_distribution_array_detail
from export._30_drc_assign_batch_approval_list import excel_drc_assign_batch_approval
from export._33_drc_assign_manager_approval_list import excel_drc_approval_detail
from export._32_case_distribution_drc_summary_drc_id import excel_drc_summary_detail
from export._37_request_log import excel_request_log_detail
from export._38_request_response_log_list import excel_case_detail
from export._39_digital_signatures_relavent_lod import excel_digital_signature_detail
from export._40_each_lod_or_final_remider_case import excel_lod_or_final_reminder_detail
from export._41_proceed_lod_or_final_remider_list import excel_proceed_lod_or_final_reminder_detail

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
                        params.get('current_arrears_band'),
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
                case 30:
                    return excel_drc_assign_batch_approval(
                        params.get('approver_ref')
                    )
                case 33:
                    return excel_drc_approval_detail(
                        params.get('approval_type'),
                        params.get('from_date'),
                        params.get('to_date')
                    )
            
                case 32:
                    return excel_drc_summary_detail(
                        params.get('drc_id'),
                        params.get('drc_name'), 
                        params.get('case_distribution_batch_id')
                    ) 
                case 37:
                    return excel_request_log_detail(
                        params.get('deligate_user_id'),
                        params.get('user_interaction_type'),
                        params.get('drc_id'),
                        params.get('from_date'),
                        params.get('to_date'),
                    )
                case 38:
                    return excel_case_detail(
                        params.get('case_current_status'),
                        params.get('to_date'),
                        params.get('date_from'),
                    )                                                                   
                case 39:
                    return excel_digital_signature_detail(
                        params.get('case_current_status'),
                    )   
                case 40:    
                    return excel_lod_or_final_reminder_detail(
                        params.get('case_current_status'),
                        params.get('current_document_type')
                    )   
                case 41:    
                    return excel_proceed_lod_or_final_reminder_detail( 
                        params.get('case_current_status'),
                        params.get('current_document_type'),
                        params.get('case_count')    
                    )    
                        
                case _:
                    logger.error(f"No handler for template ID: {template_id}")
                    raise ValueError(f"Unknown template_id: {template_id}")
        
        except Exception as e:
            logger.error(f"Error executing task {template_id}: {str(e)}", exc_info=True)
            raise