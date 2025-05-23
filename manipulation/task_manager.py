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


# task_manager.py (Simplified)
from tasks.config_loader import ConfigLoaderSingleton
from tasks.task_handler import TaskHandlers
import logging
from utils.connectionMongo import MongoDBConnectionSingleton

logger = logging.getLogger('appLogger')

class TaskManager:
    def __init__(self):
        self.template_ids = ConfigLoaderSingleton().get_template_task_ids()
        
    def execute_tasks(self):
        """Simplified task processing in a single method"""
        if not self.template_ids:
            logger.error("No template task IDs found in config.")
            return

        try:
            with MongoDBConnectionSingleton() as db:
                task_handlers = TaskHandlers()
                system_tasks_collection = db['System_tasks']
                
                for task_id in self.template_ids:
                    try:
                        # Get all open tasks for this template ID
                        for task in system_tasks_collection.find({
                            "Template_Task_Id": task_id,
                            "task_status": "open"
                        }):
                            try:
                                # Process each task
                                template_id = task.get("Template_Task_Id")
                                params = task.get("parameters", {})
                                
                                # Find and execute the handler
                                handler = getattr(task_handlers, f"handle_task_{template_id}", None)
                                if not handler:
                                    logger.error(f"No handler for template {template_id}")
                                    continue
                                    
                                handler(**params)
                                logger.info(f"Successfully executed task {template_id}")
                                
                            except Exception as task_error:
                                logger.error(f"Task {task.get('_id')} failed: {task_error}", exc_info=True)
                                
                    except Exception as template_error:
                        logger.error(f"Error processing template {task_id}: {template_error}", exc_info=True)
                        
        except Exception as db_error:
            logger.error(f"Database error: {db_error}", exc_info=True)



'''from utils.logger import SingletonLogger
from utils.connectionMongo import MongoDBConnectionSingleton
from datetime import datetime

# Configure the logger before using it
SingletonLogger.configure()

# Define loggers at module level
logger = SingletonLogger.get_logger('appLogger')
db_logger = SingletonLogger.get_logger('dbLogger')

def get_next_case_id(collection):
    """Get the next available case_id by finding the maximum existing case_id."""
    logger.info("Entering get_next_case_id to fetch the next case ID")
    
    try:
        max_case = collection.find_one(sort=[("case_id", -1)])
        next_case_id = max_case["case_id"] + 1 if max_case else 1
        logger.info(f"Retrieved next case_id: {next_case_id}")
        return next_case_id
    except Exception as e:
        logger.error(f"Error in get_next_case_id: {str(e)}")
        raise

def initialize_case_details():
    """Initialize a default document structure for case_details."""
    logger.info("Initializing case_details document structure")
    
    try:
        return {
            "case_id": None,
            "incident_id": None,
            "account_num": "",
            "customer_ref": "",
            "created_dtm": None,
            "implemented_dtm": None,
            "area": "",
            "rtom": "",
            "arrears_band": "",
            "bss_arrears_amount": 0.0,
            "current_arrears_amount": 0.0,
            "current_arrears_band": "",
            "action_type": "",
            "drc_commision_rule": "",
            "last_payment_date": None,
            "monitor_months": 6,
            "last_bss_reading_date": None,
            "commission": 0.0,
            "case_current_status": "",
            "filtered_reason": None,
            "contact": [],
            "remark": [],
            "case_status": [],
            "updatedAt": None,
            "current_contact": [],
            "ref_products": [],
            "RO_Customer_Details_Edit": [],
            "Ref_Data_Temp_Permanent": [],
            "Approvals": [],
            "DRC": [],
            "RO": [],
            "RO_Requests": [],
            "RO_Negotiation": [],
            "RO_CPE_Collect": [],
            "Mediation_Board": [],
            "Settlement": [],
            "Money_Transactions": [],
            "Commission_Bill_Payment": [],
            "Bonus": [],
            "FTL_LOD": [],
            "Litigation": [],
            "LOD_Final_Reminder": [],
            "Dispute": [],
            "Abnormal_Abs": []
        }
    except Exception as e:
        logger.error(f"Error initializing case_details: {str(e)}")
        raise

def assign_case_details_values(case_details, incident_data, current_time):
    """Assign values to the initialized case_details dictionary, including contacts and case status."""
    logger.info("Assigning values to case_details")
    
    try:
        # Assign core case details (excluding case_id)
        case_details["incident_id"] = incident_data.get("Incident_Id")
        case_details["account_num"] = incident_data.get("Account_Num", "")
        case_details["customer_ref"] = incident_data.get("Product_Details", [{}])[0].get("Customer_Ref", "CUST-REF-UNKNOWN")
        case_details["created_dtm"] = current_time
        case_details["implemented_dtm"] = current_time
        case_details["area"] = incident_data.get("Product_Details", [{}])[0].get("Province", "Unknown")
        case_details["arrears_band"] = incident_data.get("Arrears_Band", "AB-UNKNOWN")
        case_details["bss_arrears_amount"] = incident_data.get("Arrears", 0.0)
        case_details["current_arrears_amount"] = incident_data.get("Arrears", 0.0)
        case_details["current_arrears_band"] = incident_data.get("current_arrears_band", "Default Band")
        case_details["action_type"] = incident_data.get("Actions", "Recovery")
        case_details["drc_commision_rule"] = incident_data.get("drc_commision_rule", "Unknown")
        case_details["last_payment_date"] = incident_data.get("Last_Actions", {}).get("Payment_Created")
        case_details["case_current_status"] = incident_data.get("Incident_Status", "Open No Agent")
        case_details["filtered_reason"] = incident_data.get("Filtered_Reason", None)
        case_details["ref_products"] = incident_data.get("Product_Details", [])
        case_details["updatedAt"] = current_time

        # Map contact details directly
        contacts = []
        for contact in incident_data.get("Contact_Details", []):
            contact_type = contact.get("Contact_Type")
            contact_value = contact.get("Contact")
            logger.debug(f"Processing contact: type={contact_type}, value={contact_value}")
            
            if contact_type == "Mob":
                contacts.append({"mob": contact_value, "email": "", "lan": "", "address": ""})
            elif contact_type == "email":
                contacts.append({"mob": "", "email": contact_value, "lan": "", "address": ""})
            elif contact_type == "Land":
                contacts.append({"mob": "", "email": "", "lan": contact_value, "address": ""})
        
        full_address = incident_data.get("Customer_Details", {}).get("Full_Address")
        if full_address:
            logger.debug(f"Found Full_Address in Customer_Details: {full_address}")
            if contacts:
                contacts[0]["address"] = full_address
            else:
                contacts.append({"mob": "", "email": "", "lan": "", "address": full_address})
        
        case_details["contact"] = contacts
        logger.info(f"Successfully mapped {len(contacts)} contact(s)")

        # Add case_status entry
        case_status_entry = {
            "case_status": incident_data.get("Incident_Status", "Open No Agent"),
            "status_reason": incident_data.get("Status_Description", "Pending"),
            "created_dtm": incident_data.get("Incident_Status_Dtm"),
            "created_by": incident_data.get("Created_By", "admin"),
            "notified_dtm": current_time,
            "expire_dtm": None
        }
        case_details["case_status"].append(case_status_entry)
        logger.debug("Appended case_status")

        logger.debug(f"Customer reference: {case_details['customer_ref']}, incident_id: {case_details['incident_id']}")
        logger.info("Successfully assigned values to case_details")
        return case_details
    except Exception as e:
        logger.error(f"Error in assign_case_details_values: {str(e)}")
        raise

def map_incident_to_case_details(incident_data):
    """Map Incident data to Case_details format."""
    logger.info("Entering map_incident_to_case_details")
    
    try:
        # Initialize the case_details document
        case_details = initialize_case_details()
        
        # Assign values to the case_details document, including contacts and case status
        current_time = datetime.now()
        case_details = assign_case_details_values(case_details, incident_data, current_time)
        
        logger.info("Successfully mapped incident to case_details")
        return case_details
    except Exception as e:
        logger.error(f"Error in map_incident_to_case_details: {str(e)}")
        raise

def process_incident_to_case(incident_id):
    """Process a single Incident by ID and map it to Case_details, rejecting if duplicate."""
    logger.info(f"Starting process_incident_to_case for incident_id: {incident_id}")
    
    try:
        # Connect to MongoDB
        with MongoDBConnectionSingleton() as mongo_db:
            if mongo_db is None:
                db_logger.error("Failed to connect to MongoDB.")
                return False, "Failed to connect to MongoDB."

            db_logger.info(f"Connected to MongoDB database: {mongo_db.name}")

            # Fetch the specific incident directly
            incident = mongo_db["Incident"].find_one({"Incident_Id": incident_id})
            if not incident:
                db_logger.error(f"No incident found with Incident_Id: {incident_id}")
                return False, f"No incident found with Incident_Id: {incident_id}"

            try:
                # Check for existing case
                logger.debug(f"Checking for existing case with incident_id: {incident_id}")
                existing_case = mongo_db["Case_details"].find_one({"incident_id": incident_id})
                if existing_case:
                    logger.error(f"Duplicate case found for incident_id: {incident_id}")
                    return False, f"Duplicate case found for incident_id: {incident_id}"

                # Map incident to case details
                logger.debug(f"Mapping incident to case details for incident_id: {incident_id}")
                case_data = map_incident_to_case_details(incident)
                
                # Get next case_id just before insert
                logger.debug(f"Fetching next case_id for incident_id: {incident_id}")
                new_case_id = get_next_case_id(mongo_db["Case_details"])
                logger.info(f"Using case_id: {new_case_id} for incident_id: {incident_id}")
                case_data["case_id"] = new_case_id
                
                # Insert into Case_details collection
                logger.debug(f"Inserting new case for incident_id: {incident_id}")
                result = mongo_db["Case_details"].insert_one(case_data)
                logger.info(f"Inserted case with case_id: {new_case_id}, MongoDB ID: {result.inserted_id}")
                return True, f"Successfully inserted case with case_id: {new_case_id}"
                
            except Exception as e:
                logger.error(f"Error processing incident {incident_id}: {str(e)}")
                return False, f"Error processing incident {incident_id}: {str(e)}"

    except Exception as e:
        db_logger.error(f"Error in main process for incident_id {incident_id}: {str(e)}")
        return False, f"Error in main process: {str(e)}"'''
  