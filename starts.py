import pandas as pd
import numpy as np
import mysql.connector

def process_starts():
    """
    Process starts data and update master_table
    Returns a result dictionary for Flask integration
    """
    try:
        print("🔄 Starting starts processing...")
        
        # Read starts data
        cl = pd.read_excel("uploads/starts.xlsx")
        print(f"📊 Loaded {len(cl)} starts records")
        
        # Database connection
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Admin@123",
        )
        cursor = conn.cursor()
        cursor.execute("use All_Data_File")
        
        # Create starts table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS starts (
          Year INT,
          Sector VARCHAR(255),
          BU VARCHAR(255),
          `Deal_Type` VARCHAR(255),
          PlacementCode VARCHAR(255),
          Candidate_Name VARCHAR(255),
          Candidate_EmailID VARCHAR(255),
          Candidate_Contact_No VARCHAR(255),
          LinkedIn_URL VARCHAR(255),
          Term VARCHAR(255),
          Month VARCHAR(255),
          Start_Date DATE,
          Project_Contract_Duration VARCHAR(255),
          Client VARCHAR(255),
          Client_Track VARCHAR(255),
          GEO VARCHAR(255),
          End_Client VARCHAR(255),
          Industry_Catg VARCHAR(255),
          CAL_ADD VARCHAR(255),
          DM_DAL VARCHAR(255),
          TL_Lead_Rec VARCHAR(255),
          CAL VARCHAR(255),
          Associate_Director_Delivery VARCHAR(255),
          Delivery_Manager VARCHAR(255),
          Delivery_Account_Lead VARCHAR(255),
          Team_Lead VARCHAR(255),
          Lead_Rec VARCHAR(255),
          Recruiter_Name VARCHAR(255),
          Employee_ID VARCHAR(255),
          Client_Manager VARCHAR(255),
          Job_Location VARCHAR(255),
          Job_Title VARCHAR(255),
          Primary_Skill VARCHAR(255),
          Secondary_Skill VARCHAR(255),
          Candidate_Source VARCHAR(255),
          Business_Track VARCHAR(255),
          Project_End_Date DATE,
          Status VARCHAR(255),
          Actual_Source VARCHAR(255),
          Margin varchar(102),
          Week INT,
          PRIMARY KEY (Employee_ID)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_table_sql)
        print("✅ Starts table created/verified")

        # Column mapping for renaming
        mapping = {
            'Deal Type ':                    'Deal_Type',
            'Candidate Name':                'Candidate_Name',
            'Candidate Contact No':          'Candidate_Contact_No',
            'LinkedIn URL':                  'LinkedIn_URL',
            'Project/Contract Duration':     'Project_Contract_Duration',
            'Client Track':                  'Client_Track',
            'End Client':                    'End_Client',
            'Industry Catg.':                'Industry_Catg',
            'CAL / ADD':                     'CAL_ADD',
            'DM / DAL':                      'DM_DAL',
            'TL / Lead Rec':                 'TL_Lead_Rec',
            'Associate Director Delivery':   'Associate_Director_Delivery',
            'Delivery Manager':              'Delivery_Manager',
            'Delivery Account Lead':         'Delivery_Account_Lead',
            'Team Lead':                     'Team_Lead',
            'Lead Rec':                      'Lead_Rec',
            'Recruiter Name':                'Recruiter_Name',
            'Employee ID':                   'Employee_ID',
            'Client Manager':                'Client_Manager',
            'Job Location':                  'Job_Location',
            'Job Title':                     'Job_Title',
            'Primary Skill':                 'Primary_Skill',
            'Secondary Skill':               'Secondary_Skill',
            'Candidate Source':              'Candidate_Source',
            'Business Track':                'Business_Track',
            'Project End Date':              'Project_End_Date',
            'Actual Source':                 'Actual_Source',
        }

        # Rename columns
        cl.rename(columns=mapping, inplace=True)
        print(f"📝 Renamed columns: {list(mapping.keys())}")

        # Debug: Print available columns
        print(f"🔍 Available columns after rename: {cl.columns.tolist()}")

        # Convert date columns to MySQL friendly format - ROBUST VERSION
        date_columns_to_check = ['Start_Date', 'Project_End_Date']
        
        for col in date_columns_to_check:
            if col in cl.columns:
                print(f"🔄 Processing date column: {col}")
                try:
                    # Handle different date formats and timestamps
                    cl[col] = pd.to_datetime(cl[col], errors='coerce')
                    # Check if we have any valid dates
                    valid_dates = cl[col].notna().sum()
                    print(f"   - Found {valid_dates} valid dates out of {len(cl)} records")
                    
                    # Convert to date only (remove time component) 
                    cl[col] = cl[col].dt.date
                    # Convert to string format for MySQL, handle NaT/None values
                    cl[col] = cl[col].astype(str)
                    cl[col] = cl[col].replace('NaT', None)
                    cl[col] = cl[col].replace('None', None)
                    print(f"✅ Successfully converted {col} to MySQL date format")
                except Exception as e:
                    print(f"⚠️ Error converting {col}: {e}")
                    # If conversion fails, set column to None
                    cl[col] = None
            else:
                print(f"⚠️ Column {col} not found in data")

        # Clean column names for MySQL compatibility
        cl.columns = (
            cl.columns
              .str.strip()                            
              .str.replace(r'[^\w]', '_', regex=True)  
              .str.replace(r'__+', '_',    regex=True) 
              .str.strip('_')                          
        )
        print(f"🧹 Cleaned column names: {cl.columns.tolist()}")

        # Convert all data to string/None for MySQL insertion
        print("🔄 Converting data for MySQL insertion...")
        for col in cl.columns:
            cl[col] = cl[col].astype(str)
            cl[col] = cl[col].replace('nan', None)
            cl[col] = cl[col].replace('NaT', None)
            cl[col] = cl[col].replace('None', None)

        # Insert starts data
        columns = cl.columns.tolist()
        col_list = ', '.join(f'`{c}`' for c in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"REPLACE INTO starts ({col_list}) VALUES ({placeholders})"

        data = cl.values.tolist()
        print(f"🔄 Inserting {len(data)} records into database...")
        cursor.executemany(insert_sql, data)
        conn.commit()
        print(f"✅ Inserted {len(data)} starts records into database")

        # First, set all starts values to 0 (default)
        print("🔄 Setting default starts values to 0...")
        default_sql = "UPDATE master_table SET starts = 0;"
        cursor.execute(default_sql)
        
        # Then update with actual starts counts where they exist
        print("🔄 Updating master_table with actual starts counts...")
        update_sql = """
        UPDATE master_table mt
        INNER JOIN (
            SELECT
              `Employee_ID`,
              DATE(`Start_Date`)   AS date,
              COUNT(*)             AS starts_count
            FROM starts
            WHERE `Start_Date` IS NOT NULL
            GROUP BY `Employee_ID`, DATE(`Start_Date`)
        ) s
          ON mt.emp_id = s.`Employee_ID`
         AND mt.date   = s.date
        SET mt.starts = s.starts_count;
        """

        cursor.execute(update_sql)
        affected_rows = cursor.rowcount
        conn.commit()
        print(f"✅ Updated {affected_rows} rows in master_table with starts data")

        # Close database connection
        cursor.close()
        conn.close()
        print("🔌 Database connection closed")

        return {
            'success': True,
            'message': f"✅ Starts processing completed successfully! Processed {len(data)} records and updated master_table.",
            'records_processed': len(data),
            'master_table_updates': affected_rows
        }

    except FileNotFoundError:
        error_msg = "❌ Starts file 'starts.xlsx' not found"
        print(error_msg)
        return {
            'success': False,
            'message': error_msg,
            'records_processed': 0
        }
        
    except mysql.connector.Error as db_error:
        error_msg = f"❌ Database error during starts processing: {str(db_error)}"
        print(error_msg)
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
        except:
            pass
        return {
            'success': False,
            'message': error_msg,
            'records_processed': 0
        }
        
    except Exception as e:
        error_msg = f"❌ Error during starts processing: {str(e)}"
        print(error_msg)
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
        except:
            pass
        return {
            'success': False,
            'message': error_msg,
            'records_processed': 0
        }

# For testing without Flask
if __name__ == "__main__":
    result = process_starts()
    print(result)