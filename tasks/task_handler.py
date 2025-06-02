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
    

    def handle_task_20(self, action_type=None, status=None, from_date=None, to_date=None):
        """Handles Incident Export Task (Task ID 20)"""
        logger.info("Executing Incident Export Task (Task ID 20)...")
        
        success = excel_incident_detail(action_type, status, from_date, to_date)

        if success:
            logger.info("Incident report successfully exported.")

            try:
                    # Record the export in MongoDB
                with MongoDBConnectionSingleton() as db:
                    downloads_collection = db['download']
                    
                    record = {
                        "export_type": "incident_report",
                        "action_type": action_type,
                        "status": status,
                        "from_date": from_date,
                        "to_date": to_date,
                        "export_date": datetime.now(),
                        "download_status": "Incident report exported"
                    }

                    result = downloads_collection.insert_one(record)
                    logger.info(f"Export record created with ID: {result.inserted_id}")
                    
            except Exception as e:
                logger.error(f"Failed to record export in database: {e}", exc_info=True)
                # The export itself succeeded, so we still return True
                return True
        else:
            logger.error("Failed to export incident report.")


    def handle_task_21(self):
        """Handles Open Incident Export Task (Task ID 21)"""
        logger.info("Executing Open Incident Export Task (Task ID 21)...")
        
        success = excel_incident_open_distribution()

        if success:
            logger.info("Open Incident report successfully exported.")

            try:
                    # Record the export in MongoDB
                with MongoDBConnectionSingleton() as db:
                    downloads_collection = db['download']
                    
                    record = {
                        "export_type": "incident_open_report",
                        "export_date": datetime.now(),
                        "download_status": "Incident open report exported"
                    }

                    result = downloads_collection.insert_one(record)
                    logger.info(f"Export record created with ID: {result.inserted_id}")
                    
            except Exception as e:
                logger.error(f"Failed to record export in database: {e}", exc_info=True)
                # The export itself succeeded, so we still return True
                return True
        else:
            logger.error("Failed to export open incident report.")



# def handle_task_22(self, drc_commission_rules=None, from_date=None, to_date=None):
#         """Handles pending reject Export Task (Task ID 22)"""
#         logger.info("Executing pending reject Export Task (Task ID 22)...")
        
#         success = excel_pending_reject_incident(drc_commission_rules, from_date, to_date)

#         if success:
#             logger.info("Open pending reject report successfully exported.")

#             try:
#                 # Record the export in MongoDB
#                 with MongoDBConnectionSingleton() as db:
#                     downloads_collection = db['download']
                    
#                     record = {
#                         "export_type": "incident_open_report",
#                         "drc_commission_rules": drc_commission_rules,
#                         "from_date": from_date,
#                         "to_date": to_date,
#                         "export_date": datetime.now(),
#                         "download_status": "Pending Reject report exported"
#                     }

#                     result = downloads_collection.insert_one(record)
#                     logger.info(f"Export record created with ID: {result.inserted_id}")
                    
#             except Exception as e:
#                 logger.error(f"Failed to record export in database: {e}", exc_info=True)
#                 # The export itself succeeded, so we still return True
#                 return True
#         else:
#             logger.error("Failed to export pending reject incident report.")



    # def handle_task_23(self, from_date=None, to_date=None, drc_commision_rule=None):
    #     """Handles direct LOD Export Task (Task ID 23)"""
    #     logger.info("Executing direct LOD Export Task (Task ID 23)...")
        
    #     success = excel_direct_lod_detail(from_date, to_date, drc_commision_rule)

    #     if success:
    #         logger.info("Direct LOD report successfully exported.")

    #         try:
    #                 # Record the export in MongoDB
    #             with MongoDBConnectionSingleton() as db:
    #                 downloads_collection = db['download']
                    
    #                 record = {
    #                     "export_type": "direct_lod_report",
    #                     "from_date": from_date,
    #                     "to_date": to_date,
    #                     "drc_commision_rule": drc_commision_rule,
    #                     "export_date": datetime.now(),
    #                     "download_status": "direct LOD report exported"
    #                 }

    #                 result = downloads_collection.insert_one(record)
    #                 logger.info(f"Export record created with ID: {result.inserted_id}")
                    
    #         except Exception as e:
    #             logger.error(f"Failed to record export in database: {e}", exc_info=True)
    #             # The export itself succeeded, so we still return True
    #             return True
    #     else:
    #         logger.error("Failed to export direct lod report.")



    # def handle_task_24(self, from_date=None, to_date=None, drc_commision_rule=None):
    #     """Handles CPE Export Task (Task ID 24)"""
    #     logger.info("Executing CPE Export Task (Task ID 24)...")
        
    #     success = excel_cpe_detail(from_date, to_date, drc_commision_rule)

    #     if success:
    #         logger.info("CPE report successfully exported.")

    #         try:
    #                 # Record the export in MongoDB
    #             with MongoDBConnectionSingleton() as db:
    #                 downloads_collection = db['download']
                    
    #                 record = {
    #                     "export_type": "cpe_report",
    #                     "from_date": from_date,
    #                     "to_date": to_date,
    #                     "drc_commision_rule": drc_commision_rule,
    #                     "export_date": datetime.now(),
    #                     "download_status": "CPE report exported"
    #                 }

    #                 result = downloads_collection.insert_one(record)
    #                 logger.info(f"Export record created with ID: {result.inserted_id}")
                    
    #         except Exception as e:
    #             logger.error(f"Failed to record export in database: {e}", exc_info=True)
    #             # The export itself succeeded, so we still return True
    #             return True
    #     else:
    #         logger.error("Failed to export CPE report.")




    

    # def handle_task_25(self, actions=None, from_date=None, to_date=None, drc_commision_rule=None):
    #     """Handles Rejected list Export Task (Task ID 25)"""
    #     logger.info("Executing Rejected list Export Task (Task ID 25)...")
        
    #     success = excel_rejected_detail(actions, drc_commision_rule, from_date,to_date)

    #     if success:
    #         logger.info("Rejected report successfully exported.")

    #         try:
    #                 # Record the export in MongoDB
    #             with MongoDBConnectionSingleton() as db:
    #                 downloads_collection = db['download']
                    
    #                 record = {
    #                     "export_type": "rejected_report",
    #                     "actions": actions,
    #                     "drc_commision_rule": drc_commision_rule,
    #                     "from_date": from_date,
    #                     "to_date": to_date,
    #                     "export_date": datetime.now(),
    #                     "download_status": "rejected report exported"
    #                 }

    #                 result = downloads_collection.insert_one(record)
    #                 logger.info(f"Export record created with ID: {result.inserted_id}")
                    
    #         except Exception as e:
    #             logger.error(f"Failed to record export in database: {e}", exc_info=True)
    #             # The export itself succeeded, so we still return True
    #             return True
    #     else:
    #         logger.error("Failed to export rejected report.")





    # def handle_task_26(self, Arrears_band=None, drc_commision_rule=None, from_date=None, to_date=None):
    #     """Handles case distribution drc transaction list Export Task (Task ID 26)"""
    #     logger.info("Executing case distribution drc transaction list Export Task (Task ID 26)...")
        
    #     success = excel_case_distribution_detail(Arrears_band, drc_commision_rule, from_date, to_date)

    #     if success:
    #         logger.info("case distribution drc transaction report successfully exported.")

    #         try:
    #                 # Record the export in MongoDB
    #             with MongoDBConnectionSingleton() as db:
    #                 downloads_collection = db['download']
                    
    #                 record = {
    #                     "export_type": "rejected_report",
    #                     "arrears_band": Arrears_band,
    #                     "drc_commision_rule": drc_commision_rule,
    #                     "from_date": from_date,
    #                     "to_date": to_date,
    #                     "export_date": datetime.now(),
    #                     "download_status": "case distribution drc transaction report exported"
    #                 }

    #                 result = downloads_collection.insert_one(record)
    #                 logger.info(f"Export record created with ID: {result.inserted_id}")
                    
    #         except Exception as e:
    #             logger.error(f"Failed to record export in database: {e}", exc_info=True)
    #             # The export itself succeeded, so we still return True
    #             return True
    #     else:
    #         logger.error("Failed to export case distribution transaction report.")






    # def handle_task_27(self, case_distribution_batch_id=None):
    #     """Handles case distribution drc transaction batch list Export Task (Task ID 27)"""
    #     logger.info("Executing case distribution drc transaction batch list Export Task (Task ID 27)...")
        
    #     success = excel_case_distribution_transaction_batch_detail(case_distribution_batch_id)

    #     if success:
    #         logger.info("case distribution drc transaction batch report successfully exported.")

    #         try:
    #                 # Record the export in MongoDB
    #             with MongoDBConnectionSingleton() as db:
    #                 downloads_collection = db['download']
                    
    #                 record = {
    #                     "export_type": "rejected_report",
    #                     "case_distribution_batch_id": case_distribution_batch_id,
    #                     "export_date": datetime.now(),
    #                     "download_status": "case distribution drc transaction batch report exported"
    #                 }

    #                 result = downloads_collection.insert_one(record)
    #                 logger.info(f"Export record created with ID: {result.inserted_id}")
                    
    #         except Exception as e:
    #             logger.error(f"Failed to record export in database: {e}", exc_info=True)
    #             # The export itself succeeded, so we still return True
    #             return True
    #     else:
    #         logger.error("Failed to export case distribution drc transaction batch  report.")





    # def handle_task_28(self, case_distribution_batch_id=None, batch_seq=None):
    #     """Handles case distribution drc transaction batch list distribution Export Task (Task ID 28)"""
    #     logger.info("Executing case distribution drc transaction batch list Export Task (Task ID 28)...")
        
    #     success = excel_case_distribution_transaction_batch_distribution_array_detail(case_distribution_batch_id, batch_seq)

    #     if success:
    #         logger.info("case distribution drc transaction batch list distribution report successfully exported.")

    #         try:
    #             #Record the export in MongoDB
    #             with MongoDBConnectionSingleton() as db:
    #                 downloads_collection = db['download']
                    
    #                 record = {
    #                     "export_type": "rejected_report",
    #                     "case_distribution_batch_id": case_distribution_batch_id,
    #                     "batch_seq": batch_seq,
    #                     "export_date": datetime.now(),
    #                     "download_status": "case distribution drc transaction batch list distribution report exported"
    #                 }

    #                 result = downloads_collection.insert_one(record)
    #                 logger.info(f"Export record created with ID: {result.inserted_id}")
                    
    #         except Exception as e:
    #             logger.error(f"Failed to record export in database: {e}", exc_info=True)
    #             # The export itself succeeded, so we still return True
    #             return True
    #     else:
    #         logger.error("Failed to export case distribution drc transaction batch list distribution report.")
