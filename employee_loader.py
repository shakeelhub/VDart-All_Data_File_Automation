import pandas as pd
import numpy as np
import mysql.connector
from datetime import date, datetime
import os

def load_employee_data():
    """
    Load employee data from Excel file into database
    Returns: dict with success status and message
    """
    try:
        # Check if employee file exists
        employee_file = "uploads/employee_list.xlsx"
        if not os.path.exists(employee_file):
            return {
                'success': False,
                'message': f"❌ Employee file not found: {employee_file}. Please upload employee_list.xlsx first."
            }
        
        print("📋 Loading employee data from Excel...")
        
        # Database connection
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Admin@123",
        )
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS All_Data_File;")
        cursor.execute("USE All_Data_File;")
        
        # Create employee_data table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS employee_data (
            emp_id VARCHAR(20),
            bu VARCHAR(100),
            client VARCHAR(100),
            track VARCHAR(100),
            name VARCHAR(100),
            email_id VARCHAR(100),
            role VARCHAR(100),
            cal_add VARCHAR(50),
            dm_dal VARCHAR(50),
            tl_lead_rec VARCHAR(100),
            reporting_to VARCHAR(100),
            team_name VARCHAR(100),
            zoom_no VARCHAR(50),
            mode_of_hire VARCHAR(50),
            work_location VARCHAR(100),
            mobile_no VARCHAR(30),
            doj DATE,
            date_of_deployment DATE,
            extension VARCHAR(50),
            status VARCHAR(50),
            exit_date DATE,
            internal_transfer_date DATE,
            tenure VARCHAR(50),
            lead_or_non_lead VARCHAR(50),
            new_deployed_batch VARCHAR(100)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("✅ Employee table created/verified")
        
        # Clear existing data
        cursor.execute("TRUNCATE TABLE employee_data;")
        conn.commit()
        print("🗑️ Existing employee data cleared")
        
        # Read Excel file with error handling
        try:
            df = pd.read_excel(employee_file)
            print(f"📊 Read {len(df)} employee records from Excel")
        except Exception as e:
            cursor.close()
            conn.close()
            return {
                'success': False,
                'message': f"❌ Error reading Excel file: {str(e)}. Please check file format."
            }
        
        # Clean Exit Date column
        df['Exit Date'] = df['Exit Date'].replace(['', ' ', 'N/A', 'NA', '--'], pd.NA)
        df['Exit Date'] = pd.to_datetime(df['Exit Date'], errors='coerce')
        df['Exit Date'] = df['Exit Date'].apply(lambda x: x.date() if pd.notnull(x) else None)
        
        # Rename columns to match database schema
        df.rename(columns={
            'Emp ID': 'emp_id',
            'BU': 'bu',
            'Client': 'client',
            'Track': 'track',
            'Ceipal Name': 'name',
            'Email ID': 'email_id',
            'Role': 'role',
            'CAL/ADD': 'cal_add',
            'DM/DAL': 'dm_dal',
            'TL/Lead Rec': 'tl_lead_rec',
            'Reporting To': 'reporting_to',
            'Team Name': 'team_name',
            'Zoom No': 'zoom_no',
            'Mode Of Hire': 'mode_of_hire',
            'Work Location': 'work_location',
            'Mobile No': 'mobile_no',
            'DOJ': 'doj',
            'Date of Deployment': 'date_of_deployment',
            'Extension': 'extension',
            'Status': 'status',
            'Exit Date': 'exit_date',
            'Internal Transfer Date': 'internal_transfer_date',
            'Tenure': 'tenure',
            'Lead OR Non -Lead': 'lead_or_non_lead',
            'New Deployed Batch': 'new_deployed_batch'
        }, inplace=True)
        
        # Clean date columns
        date_columns = ['doj', 'date_of_deployment', 'exit_date', 'internal_transfer_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].replace(['', ' ', 'N/A', 'NA', '--'], pd.NA)
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].apply(lambda x: x.date() if pd.notnull(x) else None)
        
        # Handle NaN values
        df = df.where(pd.notnull(df), None)
        
        # Prepare data for insertion
        data_to_insert = df[[
            'emp_id', 'bu', 'client', 'track', 'name', 'email_id', 'role', 'cal_add', 'dm_dal', 'tl_lead_rec',
            'reporting_to', 'team_name', 'zoom_no', 'mode_of_hire', 'work_location', 'mobile_no',
            'doj', 'date_of_deployment', 'extension', 'status', 'exit_date',
            'internal_transfer_date', 'tenure', 'lead_or_non_lead', 'new_deployed_batch'
        ]].values.tolist()
        
        # Insert data
        insert_query = """
        INSERT INTO employee_data (
            emp_id, bu, client, track, name, email_id, role, cal_add, dm_dal, tl_lead_rec,
            reporting_to, team_name, zoom_no, mode_of_hire, work_location, mobile_no,
            doj, date_of_deployment, extension, status, exit_date,
            internal_transfer_date, tenure, lead_or_non_lead, new_deployed_batch
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        """
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"✅ {len(data_to_insert)} employee records inserted")
        
        # Apply name corrections
        print("🔧 Applying name corrections...")
        corrections = [
            ("Bhuvaneswaran s", "12917"),
            ("VijayaKannan S", "12568"),
            ("Kirupakaran P", "12615")
        ]
        
        for name, emp_id in corrections:
            cursor.execute("UPDATE employee_data SET name = %s WHERE emp_id = %s", (name, emp_id))
        
        conn.commit()
        print(f"✅ Applied {len(corrections)} name corrections")
        
        cursor.close()
        conn.close()
        
        return {
            'success': True,
            'message': f"✅ Employee data loaded successfully: {len(data_to_insert)} records processed with {len(corrections)} corrections applied",
            'records_count': len(data_to_insert)
        }
        
    except mysql.connector.Error as db_err:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
        except:
            pass
        return {
            'success': False,
            'message': f"❌ Database error: {str(db_err)}"
        }
        
    except Exception as e:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
        except:
            pass
        return {
            'success': False,
            'message': f"❌ Error loading employee data: {str(e)}"
        }

# For testing
if __name__ == "__main__":
    result = load_employee_data()
    print(result['message'])