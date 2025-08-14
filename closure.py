import pandas as pd
import numpy as np
import mysql.connector

def process_closure():
    """
    Process closure data and update master_table
    Returns a result dictionary for Flask integration
    """
    try:
        print("🔄 Starting closure processing...")
        
        # Read closure data
        cl = pd.read_excel("uploads/closure.xlsx")
        print(f"📊 Loaded {len(cl)} closure records")
        
        # Database connection
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Admin@123",
        )
        cursor = conn.cursor()
        cursor.execute("use All_Data_File")
        
        # Create closure table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS closure (
            Sector VARCHAR(255),
            Year YEAR,
            Date DATE,
            Week TINYINT,
            `PLC Code` VARCHAR(255),
            `Applicant ID` VARCHAR(255),
            `Candidate Name` VARCHAR(255),
            `Candidate Email ID` VARCHAR(255),
            `Candidate Contact No` TEXT,
            `Candidate LinkedIn URL` VARCHAR(255),
            `Work Authorization` VARCHAR(255),
            `Paperwork Source` VARCHAR(255),
            `Job Location` TEXT,
            GEO VARCHAR(255),
            Certification TEXT,
            `Job title` VARCHAR(255),
            `Primary Skill` TEXT,
            `Secondary Skill` TEXT,
            Duration VARCHAR(255),
            Term VARCHAR(255),
            Client VARCHAR(255),
            `Client Track` VARCHAR(255),
            `SOT/Non SOT` VARCHAR(255),
            `End Client` VARCHAR(255),
            `Client Manager` VARCHAR(255),
            `BR No.` VARCHAR(255),
            `CAL / ADD` VARCHAR(255),
            `DM /DAL` VARCHAR(255),
            `TL / Lead Rec` VARCHAR(255),
            `Recruiters Name` VARCHAR(255),
            `Employee ID` INT,
            `Tentative Start Date` DATE,
            Proof VARCHAR(255),
            `Rate Card Adherence` VARCHAR(255),
            `Reason for Deviation` TEXT,
            Entity VARCHAR(255),
            `Client rate` VARCHAR(255),
            `Pay Rate` VARCHAR(255),
            Margin VARCHAR(255),
            Cents VARCHAR(255),
            `Vendor Status` VARCHAR(255),
            `Vendor Company Name` TEXT,
            `Vendor name` VARCHAR(255),
            `Vendor Contact No` TEXT,
            `Vendor Email ID` VARCHAR(255),
            `Bench Sales Recruiter Name` VARCHAR(255),
            `Bench Sales Recruiter Contact No` TEXT,
            `Bench Sales Recruiter Mail ID` VARCHAR(255),
            `H1B Verified` VARCHAR(255),
            `V-Validate Status` VARCHAR(255),
            Month VARCHAR(255),
            BU VARCHAR(255),
            Type VARCHAR(255),
            Status VARCHAR(255),
            `CAL/CP` VARCHAR(255),
            `Associate Director Delivery` VARCHAR(255),
            `Delivery Manager` VARCHAR(255),
            `Delivery Account Lead` VARCHAR(255),
            `Team Lead` VARCHAR(255),
            `Lead Rec` VARCHAR(255),
            Industry VARCHAR(255),
            `Own/ Collaboration` VARCHAR(255),
            `Own Team Name (Account)` TEXT,
            `Collaborated Team Name` TEXT,
            `Origin Source` VARCHAR(255),
            `Location Status` VARCHAR(255),
            City VARCHAR(255),
            State VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("✅ Closure table created/verified")

        # Convert date columns to MySQL friendly format
        for col in ['Date', 'Tentative Start Date']:
            if col in cl.columns:
                cl[col] = pd.to_datetime(cl[col], errors='coerce').dt.strftime('%Y-%m-%d')
                print(f"📅 Converted {col} to MySQL date format")

        # Insert closure data
        columns = cl.columns.tolist()
        col_list = ', '.join(f'`{c}`' for c in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"REPLACE INTO closure ({col_list}) VALUES ({placeholders})"

        data = cl.where(pd.notnull(cl), None).values.tolist()
        cursor.executemany(insert_sql, data)
        conn.commit()
        print(f"✅ Inserted {len(data)} closure records into database")

        # First, set all closure values to 0 (default)
        default_sql = "UPDATE master_table SET closure = 0;"
        cursor.execute(default_sql)
        
        # Then update with actual closure counts where they exist
        update_sql = """
        UPDATE master_table mt
        INNER JOIN (
            SELECT `Employee ID`, `Date`, COUNT(*) AS closure_count
            FROM closure
            GROUP BY `Employee ID`, `Date`
        ) c
          ON mt.emp_id = c.`Employee ID`
         AND mt.date  = c.`Date`
        SET mt.closure = c.closure_count;
        """

        cursor.execute(update_sql)
        affected_rows = cursor.rowcount
        conn.commit()
        print(f"✅ Updated {affected_rows} rows in master_table with closure data")

        # Close database connection
        cursor.close()
        conn.close()
        print("🔌 Database connection closed")

        return {
            'success': True,
            'message': f"✅ Closure processing completed successfully! Processed {len(data)} records and updated master_table.",
            'records_processed': len(data),
            'master_table_updates': affected_rows
        }

    except FileNotFoundError:
        error_msg = "❌ Closure file 'closure.xlsx' not found"
        print(error_msg)
        return {
            'success': False,
            'message': error_msg,
            'records_processed': 0
        }
        
    except mysql.connector.Error as db_error:
        error_msg = f"❌ Database error during closure processing: {str(db_error)}"
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
        error_msg = f"❌ Error during closure processing: {str(e)}"
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
    result = process_closure()
    print(result)
