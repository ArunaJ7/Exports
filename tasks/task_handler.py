import logging
from datetime import datetime
import os
from utils.style_loader import STYLES
from export.incident_list import excel_incident_detail  # Updated import

logger = logging.getLogger('appLogger')

class TaskHandlers:
    def __init__(self, db_client):
        self.db_client = db_client

    def handle_task_20(self, action_type=None, status=None, from_date=None, to_date=None):
        """Handles Incident Export Task (Task ID 20)"""
        logger.info("Executing Incident Export Task (Task ID 20)...")

        # Call the new excel_incident_detail function
        success = excel_incident_detail(action_type, status, from_date, to_date)

        if success:
            logger.info("Incident report successfully exported.")
        else:
            logger.error("Failed to export incident report.")
