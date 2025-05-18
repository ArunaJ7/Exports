"This file indicate fetch incident details acording to relevant parameters and export excel file"


import logging
from datetime import datetime, timedelta
from bson import ObjectId
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from utils.style_loader import STYLES
import os
import logging.config
from pymongo import MongoClient
from utils.connectionMongo import MongoDBConnectionSingleton
from utils.logger import SingletonLogger
from logging import getLogger

logger = getLogger('appLogger')

INCIDENT_HEADERS = [
    "Task_Id", "Incident_Id", "Account_Num", "Incident_Status", "Actions",
    "Monitor_Months", "Created_By", "Created_Dtm", "Source_Type"
]

def excel_incident_detail(action_type, status, from_date, to_date):

    """Fetch and export incidents with a fixed Task_Id of 20 based on validated parameters"""

   
    try:   

            incident_log_collection = MongoDBConnectionSingleton["Incident_log"]
            query = {} 

            # Check each parameter and build query
            # Check action_type
            if action_type is not None:
                if action_type == "collect arrears and CPE":
                    query["Actions"] = {"$regex": f"^{action_type}$"}
                elif action_type == "collect arrears":
                    query["Actions"] = action_type
                elif action_type == "collect CPE":
                    query["Actions"] = action_type
                else:
                    raise ValueError(f"Invalid action_type '{action_type}'. Must be 'collect arrears and CPE', 'collect arrears', or 'collect CPE'")
            

            # Check status
            if status is not None:
                if status == "Incident Open":
                    query["Incident_Status"] = {"$regex": f"^{status}$"}
                elif status == "Incident close":
                    query["Incident_Status"] = status
                elif status == "Incident reject":
                    query["Incident_Status"] = status
                else:
                    raise ValueError(f"Invalid status '{status}'. Must be 'Incident Open', 'Incident Close', or 'Incident Reject'")



            # Check date range
            if from_date is not None and to_date is not None:
                try:
                    # Check if from_date and to_date are in correct YYYY-MM-DD format
                    from_dt = datetime.strptime(from_date, '%Y-%m-%d')
                    to_dt = datetime.strptime(to_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
                    
                    # Validate date range
                    if to_dt < from_dt:
                        raise ValueError("to_date cannot be earlier than from_date")
                    
                   # Construct query                  
                    query["Created_Dtm"] = {"$gte": from_dt, "$lte": to_dt}

                except ValueError as ve:
                    if str(ve).startswith("to_date"):
                        raise
                    raise ValueError(f"Invalid date format. Use 'YYYY-MM-DD'. Error: {str(ve)}")

            
            # Log and execute query
            logger.info(f"Executing query: {query}")
            incidents = list(incident_log_collection.find(query))  # Fetch data into an array
            logger.info(f"Found {len(incidents)} matching incidents")

           

            # Export to Excel even if no incidents are found
            output_dir = "exports"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"incidents_details_{timestamp}.xlsx"
            filepath = os.path.join(output_dir, filename)
            os.makedirs(output_dir, exist_ok=True)

            wb = Workbook()
            wb.remove(wb.active)

            if not create_incident_table(wb, incidents, {
                "action": action_type,
                "status": status,
                "date_range": (from_dt if from_date is not None else None, to_dt if to_date is not None else None)
            }):
                raise Exception("Failed to create incident sheet")

            wb.save(filepath)
            if not incidents:
                print("No incidents found matching the selected filters. Exported empty table to: {filepath}")
            else:
                print(f"\nSuccessfully exported {len(incidents)} records to: {filepath}")
            return True

            


    except ValueError as ve:
        logger.error(f"Validation error: {str(ve)}")
        print(f"Error: {str(ve)}")
        return False
    except Exception as e:
        logger.error(f"Export failed: {str(e)}", exc_info=True)
        print(f"\nError during export: {str(e)}")
        return False
    


def create_incident_table(wb, data, filters=None):
    """Create formatted Excel sheet with filtered incident data, including headers even if no data"""
    try:
        ws = wb.create_sheet(title="INCIDENT REPORT")
        row_idx = 1
        
        # Main Header
        ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=len(INCIDENT_HEADERS))
        main_header = ws.cell(row=row_idx, column=1, value="INCIDENT REPORT")
        main_header.font = STYLES['MainHeader_Style']['font']
        main_header.fill = STYLES['MainHeader_Style']['fill']
        main_header.alignment = STYLES['MainHeader_Style']['alignment']
        row_idx += 1
        
        # Display Active Filters
        if filters:
            row_idx += 1
            
            if filters.get('action'):
                ws.cell(row=row_idx, column=2, value="Action:").font = STYLES['FilterParam_Style']['font']
                ws.cell(row=row_idx, column=2).fill = STYLES['FilterParam_Style']['fill']
                ws.cell(row=row_idx, column=2).alignment = STYLES['FilterParam_Style']['alignment']
                ws.cell(row=row_idx, column=3, value=filters['action']).font = STYLES['FilterValue_Style']['font']
                ws.cell(row=row_idx, column=3).fill = STYLES['FilterValue_Style']['fill']
                ws.cell(row=row_idx, column=3).alignment = STYLES['FilterValue_Style']['alignment']
                row_idx += 1
            
            if filters.get('status'):
                ws.cell(row=row_idx, column=2, value="Status:").font = STYLES['FilterParam_Style']['font']
                ws.cell(row=row_idx, column=2).fill = STYLES['FilterParam_Style']['fill']
                ws.cell(row=row_idx, column=2).alignment = STYLES['FilterParam_Style']['alignment']
                ws.cell(row=row_idx, column=3, value=filters['status']).font = STYLES['FilterValue_Style']['font']
                ws.cell(row=row_idx, column=3).fill = STYLES['FilterValue_Style']['fill']
                ws.cell(row=row_idx, column=3).alignment = STYLES['FilterValue_Style']['alignment']
                row_idx += 1
            
            if filters.get('date_range') and any(filters['date_range']):
                start, end = filters['date_range']
                ws.cell(row=row_idx, column=2, value="Date Range:").font = STYLES['FilterParam_Style']['font']
                ws.cell(row=row_idx, column=2).fill = STYLES['FilterParam_Style']['fill']
                ws.cell(row=row_idx, column=2).alignment = STYLES['FilterParam_Style']['alignment']
                date_str = f"{start.strftime('%Y-%m-%d') if start else 'Beginning'} to {end.strftime('%Y-%m-%d') if end else 'Now'}"
                ws.cell(row=row_idx, column=3, value=date_str).font = STYLES['FilterValue_Style']['font']
                ws.cell(row=row_idx, column=3).fill = STYLES['FilterValue_Style']['fill']
                ws.cell(row=row_idx, column=3).alignment = STYLES['FilterValue_Style']['alignment']
                row_idx += 1
            
            row_idx += 1
        
        # Data Table Headers
        header_row = row_idx
        for col_idx, header in enumerate(INCIDENT_HEADERS, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=header.replace('_', ' ').title())
            cell.font = STYLES['SubHeader_Style']['font']
            cell.fill = STYLES['SubHeader_Style']['fill']
            cell.border = STYLES['SubHeader_Style']['border']
            cell.alignment = STYLES['SubHeader_Style']['alignment']
            ws.column_dimensions[get_column_letter(col_idx)].width = 20
        
        # Data Rows (only if data exists)
        if data:
            for record in data:
                row_idx += 1
                for col_idx, header in enumerate(INCIDENT_HEADERS, 1):
                    value = record.get(header, "")
                    if header == "Incident_Id" and isinstance(value, ObjectId):
                        value = str(value)
                    if header == "Created_Dtm" and isinstance(value, datetime):
                        value = value.strftime('%Y-%m-%d %H:%M:%S')
                    cell = ws.cell(row=row_idx, column=col_idx, value=value)
                    cell.font = STYLES['Border_Style']['font']
                    cell.border = STYLES['Border_Style']['border']
                    cell.alignment = STYLES['Border_Style']['alignment']
        
        # Add AutoFilter to headers
        last_col_letter = get_column_letter(len(INCIDENT_HEADERS))
        ws.auto_filter.ref = f"{get_column_letter(1)}{header_row}:{last_col_letter}{header_row}"
        
        # Auto-adjust columns based on headers (and data if present)
        for col_idx in range(1, len(INCIDENT_HEADERS) + 1):
            col_letter = get_column_letter(col_idx)
            max_length = max(
                len(str(cell.value)) if cell.value else 0
                for cell in ws[col_letter]
            )
            adjusted_width = (max_length + 2) * 1.2
            ws.column_dimensions[col_letter].width = max(adjusted_width, 20)
        
        return True
    
    except Exception as e:
        logger.error(f"Error creating sheet: {str(e)}", exc_info=True)
        return False