import pandas as pd
import numpy as np
import mysql.connector
from datetime import date
from datetime import datetime
import sys
import os
from openpyxl import load_workbook
import pythoncom
from win32com.client import DispatchEx
import chardet  # Add this import for encoding detection
from employee_loader import load_employee_data

# Initialize COM
pythoncom.CoInitialize()

def detect_file_encoding(filepath):
    """
    Detect the encoding of a file
    """
    with open(filepath, 'rb') as file:
        raw_data = file.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def read_csv_with_encoding_detection(filepath, **kwargs):
    """
    Read CSV file with automatic encoding detection
    """
    try:
        # First try UTF-8 (most common)
        print(f"🔍 Trying UTF-8 encoding for {filepath}...")
        return pd.read_csv(filepath, encoding='utf-8', **kwargs)
    except UnicodeDecodeError:
        try:
            # Try to detect encoding
            print(f"🔍 Detecting encoding for {filepath}...")
            detected_encoding = detect_file_encoding(filepath)
            print(f"📋 Detected encoding: {detected_encoding}")
            
            if detected_encoding:
                return pd.read_csv(filepath, encoding=detected_encoding, **kwargs)
            else:
                raise Exception("Could not detect file encoding")
                
        except Exception as e:
            print(f"❌ Encoding detection failed: {e}")
            # Try common encodings as fallback
            encodings_to_try = ['latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'cp437']
            
            for encoding in encodings_to_try:
                try:
                    print(f"🔄 Trying {encoding} encoding...")
                    return pd.read_csv(filepath, encoding=encoding, **kwargs)
                except Exception as enc_error:
                    print(f"❌ {encoding} failed: {enc_error}")
                    continue
            
            # If all encodings fail, try with error handling
            try:
                print("🔄 Trying UTF-8 with error handling...")
                return pd.read_csv(filepath, encoding='utf-8', errors='replace', **kwargs)
            except Exception as final_error:
                raise Exception(f"All encoding attempts failed. Last error: {final_error}")

def clean_excel_file(filepath):
    """
    Programmatically 'clean' an Excel file using Excel automation (Save As to same location)
    This mimics the manual Save As process that fixes XML corruption
    """
    try:
        print(f"🧹 Cleaning Excel file with Excel automation: {filepath}")
        import win32com.client as win32
        
        # Start Excel (hidden)
        excel = win32.gencache.EnsureDispatch('Excel.Application')
        excel.Visible = False
        excel.DisplayAlerts = False  # Suppress all prompts
        
        # Open the file
        workbook = excel.Workbooks.Open(filepath)
        
        # Save As to same location (this is exactly what manual Save As does)
        workbook.SaveAs(filepath)  # Same filepath - replaces original
        
        # Close everything
        workbook.Close()
        excel.Quit()
        
        print(f"✅ Excel file cleaned successfully: {filepath}")
        return filepath  # Return same path since we replaced it
        
    except Exception as e:
        print(f"❌ Failed to clean with Excel automation: {e}")
        print("🔄 Falling back to openpyxl method...")
        
        # Fallback to openpyxl method
        try:
            wb = load_workbook(filepath, data_only=True)
            wb.save(filepath)  # Save to same location
            print(f"✅ Fallback cleaning successful: {filepath}")
            return filepath
        except Exception as e2:
            print(f"❌ Fallback also failed: {e2}")
            return filepath  # Return original path if all cleaning fails

def read_excel_with_cleaning(filepath, **kwargs):
    """
    Read Excel file with automatic cleaning if corruption is detected
    """
    # First, always try to clean the Excel file (mimics manual Save As process)
    print(f"🔧 Pre-cleaning Excel file: {filepath}")
    cleaned_filepath = clean_excel_file(filepath)
    
    try:
        # Try reading the cleaned file
        return pd.read_excel(cleaned_filepath, **kwargs)
    except Exception as e:
        print(f"❌ Failed to read even after cleaning: {e}")
        # Try with openpyxl engine
        try:
            print(f"🔄 Trying openpyxl engine for {cleaned_filepath}...")
            return pd.read_excel(cleaned_filepath, engine='openpyxl', **kwargs)
        except Exception as e2:
            print(f"❌ All reading attempts failed: {e2}")
            raise e2

def process_data(input_callback=None):
    print("🔄 Loading employee data from Excel...")
    employee_result = load_employee_data()
    
    if not employee_result['success']:
        if input_callback:
            return {
                'success': False,
                'message': employee_result['message'],
                'missing_members': []
            }
        else:
            raise Exception(employee_result['message'])
    
    
    
    search_column = ["Search Type","Created At","Searched By","Search String"]
    Activity_Columns=['Team-Lead Name','Team Member','Assigned Jobs','Submissions','Internal Rejections','Pending Feedback','Client Submissions','Interviewer Schedules']
    list1=['Direction','From','To','Forward_to','Device','Time','Result','Path','Duration','Client_Code','Site','Department','Cost_Center','Charge','Type','EndtoEnd_Encryption','omit']
    list2=['Direction','From','To','Forward_to','Device','Time','Result','Path','Duration','Client_Code','Site','Department','Cost_Center','Charge','Type','EndtoEnd_Encryption']

    # File paths
    searchstring_filepath = "uploads/searchstring.xlsx"
    activityreport_filepath = "uploads/activityreport2.xlsx"
    usagereport_filepath = "uploads/usage_report_sample.xlsx"
    emailstatistics_filepath = "uploads/email_statistics.xlsx"
    zoomdata_filepath= "uploads/zoomus_call_log_2025-06-11.csv"
    attendance_filepath="uploads/attendance_sample.xlsx"

    # Read all files with error handling and auto-cleaning
    try:
        print("📖 Reading search string file...")
        search_string = read_excel_with_cleaning(searchstring_filepath, header=0, names=search_column, skiprows=1)
    except Exception as e:
        print(f"❌ Error reading search string file with cleaning: {e}")
        try:
            print("🔄 Trying to read without custom names...")
            search_string = read_excel_with_cleaning(searchstring_filepath, skiprows=1)
            if len(search_string.columns) >= len(search_column):
                search_string.columns = search_column[:len(search_string.columns)]
        except Exception as e3:
            print(f"❌ All methods failed: {e3}")
            if input_callback:
                return {
                    'success': False,
                    'message': f"❌ Cannot read search string file. The file appears to be corrupted beyond repair. Error: {str(e)}",
                    'missing_members': []
                }
            else:
                raise Exception("Cannot read search string file")

    try:
        print("📖 Reading email statistics file...")
        email_statistics = read_excel_with_cleaning(emailstatistics_filepath, header=None)
    except Exception as e:
        print(f"❌ Error reading email statistics: {e}")
        if input_callback:
            return {'success': False, 'message': f"❌ Cannot read email statistics file: {str(e)}", 'missing_members': []}
        else:
            raise Exception(f"Cannot read email statistics file: {e}")

    try:
        print("📖 Reading activity report file...")
        dataact = read_excel_with_cleaning(activityreport_filepath, skiprows=1, header=0, names=Activity_Columns)
    except Exception as e:
        print(f"❌ Error reading activity report with cleaning: {e}")
        try:
            print("🔄 Trying to read activity report without custom names...")
            dataact = read_excel_with_cleaning(activityreport_filepath, skiprows=1)
            if len(dataact.columns) >= len(Activity_Columns):
                dataact.columns = Activity_Columns[:len(dataact.columns)]
        except Exception as e3:
            print(f"❌ All methods failed for activity report: {e3}")
            if input_callback:
                return {'success': False, 'message': f"❌ Cannot read activity report file. Please check the file: {str(e)}", 'missing_members': []}
            else:
                raise Exception(f"Cannot read activity report file: {e}")

    try:
        print("📖 Reading usage report file...")
        usage_report = read_excel_with_cleaning(usagereport_filepath, header=None)
    except Exception as e:
        print(f"❌ Error reading usage report: {e}")
        if input_callback:
            return {'success': False, 'message': f"❌ Cannot read usage report file: {str(e)}", 'missing_members': []}
        else:
            raise Exception(f"Cannot read usage report file: {e}")

    try:
        print("📖 Reading zoom data file...")
        # Use the new encoding detection function for CSV
        data = read_csv_with_encoding_detection(zoomdata_filepath, header=0, names=list1)
    except Exception as e:
        print(f"❌ Error reading zoom data: {e}")
        if input_callback:
            return {'success': False, 'message': f"❌ Cannot read zoom data file: {str(e)}", 'missing_members': []}
        else:
            raise Exception(f"Cannot read zoom data file: {e}")

    try:
        print("📖 Reading attendance file...")
        att = read_excel_with_cleaning(attendance_filepath)
    except Exception as e:
        print(f"❌ Error reading attendance file: {e}")
        if input_callback:
            return {'success': False, 'message': f"❌ Cannot read attendance file: {str(e)}", 'missing_members': []}
        else:
            raise Exception(f"Cannot read attendance file: {e}")

    print("🔄 Processing data...")
    data = data.iloc[:, :-1]

    data_clean = dataact.dropna(subset=['Team Member'])
    data_clean = data_clean.drop(columns=['Team-Lead Name'])
    first_row = pd.read_excel(activityreport_filepath, header=None, nrows=1)
    period_str = str(first_row.iloc[0, 0])
    date_value = period_str.split()[1]
    date_obj = datetime.strptime(date_value, "%m/%d/%y")
    formatted_date = date_obj.strftime("%d-%b-%y")
    print("Extracted date:", formatted_date)

    mysql_date = datetime.strptime(formatted_date, '%d-%b-%y').strftime('%Y-%m-%d')
    data_clean.insert(0, 'Date', formatted_date)

    names_to_remove = ['Vinay Kumar', 'Priscilla R', 'Swathy Muthukumar', 'David Sexton']
    data_clean = data_clean[~data_clean['Team Member'].isin(names_to_remove)]
    print("Rows removed from DataFrame.")

    # ========== SINGLE DATABASE CONNECTION FOR EVERYTHING ==========
    print("🔗 Connecting to database...")
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Admin@123",
    )
    cursor = conn.cursor()
    cursor.execute("use All_Data_File")
    print("✅ Connected to All_Data_File database")

    ############################################ Check missing members ###############################################
    print("🔍 Checking for missing members...")
    names = data_clean['Team Member'] 
    cursor.execute("SELECT name FROM employee_data")
    db_names = set(row[0] for row in cursor.fetchall() if row[0])

    missing = []
    for member in names:
        if pd.isna(member):
            continue
        if member not in db_names:
            print(f"❌ Missing (exact): [{member}]")
            missing.append(member)

    if missing:
        print(f"\n⚠️ {len(missing)} member(s) are missing from the database.")
        
        if input_callback:
            user_input = input_callback(
                prompt=f"⚠️ {len(missing)} member(s) are missing from the database.\n❓ Do you want to continue execution and delete the missing members from the file?",
                missing_count=len(missing),
                missing_members=missing
            )
        else:
            user_input = input("❓ Do you want to continue execution and delete the missing members from the file? (yes/no): ").strip().lower()
        
        if user_input != 'yes':
            cursor.close()
            conn.close()
            if input_callback:  
                return {
                    'success': False,
                    'message': "🔒 Execution stopped as per user's choice due to missing members.",
                    'missing_members': missing
                }
            else:  
                import sys
                sys.exit("🔒 Execution stopped as per user's choice due to missing members.")
        else:
            print("✅ Continuing execution as per user approval...")
    else:
        print("✅ All members in Excel are present in the database.")

    # Remove missing members from data
    data_clean = data_clean[~data_clean['Team Member'].isin(missing)]

    ############################################# Continue with data processing #######################################
    search_string["Created At"] = mysql_date

    usage_report = usage_report.drop(index=[0, 1]).reset_index(drop=True)
    usage_cols = ["User Name", "Career Builder", "Monster", "Dice", "Internal DB", "Total Views"]
    usage_report.columns = usage_cols
    usage_report.insert(0, "Date", formatted_date)
    usage_report["Total Views"] = pd.to_numeric(usage_report["Total Views"], errors='coerce').fillna(0)
    usage_report = usage_report[usage_report["Total Views"] != 0]

    email_statistics = email_statistics.drop(index=[0, 1]).reset_index(drop=True)
    email_cols = ["User Name", "Sent Count", "Open Count", "Bounce Count", "New Contacts Added","Total Contacts Owned"]
    email_statistics.columns = email_cols
    email_statistics.insert(0, "Date", formatted_date)

    activity_columns = ["Sent Count", "Open Count", "Bounce Count", "New Contacts Added", "Total Contacts Owned"]
    for col in activity_columns:
        email_statistics[col] = pd.to_numeric(email_statistics[col], errors='coerce').fillna(0)

    email_statistics = email_statistics[(email_statistics[activity_columns] != 0).any(axis=1)]

    ############################# Processing of zoom data ######################################################
    dataout=[data.loc[(data['Path'] == 'PSTN') & (data['Direction'] == 'Outbound') ]]
    dataout = np.array(dataout)
    dataout = dataout.reshape(-1, dataout.shape[-1])
    dataout = dataout.reshape(-1, dataout.shape[-1])
    dataout=pd.DataFrame(dataout,columns=list2)
    dataout.insert(3,'Extension',dataout['From'])
    dataout['From'] = dataout['From'].str.split('-').str[0]
    dataout['Extension']=dataout['Extension'].str.extract(r'\D*(\d{4})')
    dataout.insert(0, 'Date',formatted_date )

    datain=[data.loc[(data['Path'] == 'PSTN') & (data['Direction'] == 'Inbound') & (data['Duration'] != ' --') ]]
    datain = np.array(datain)
    datain = datain.reshape(-1, datain.shape[-1])
    datain=pd.DataFrame(datain,columns=list2)
    datain.insert(4,'Extension',datain['To'])
    datain['To'] = datain['To'].str.split('-').str[0]
    datain['Extension']=datain['Extension'].str.extract(r'\D*(\d{4})')
    datain.insert(0, 'Date', formatted_date)

    # Process attendance data
    att = att.replace({pd.NaT: None, np.nan: None})
    for col in ['Date', 'In Time', 'Out Time', 'Shift In Time', 'Shift Out Time']:
        if col in att.columns:
            if col == 'Date':
                att[col] = pd.to_datetime(att[col], errors='coerce').dt.date
            else:
                att[col] = att[col].apply(lambda x: x.time() if pd.notnull(x) and isinstance(x, pd.Timestamp) else None if pd.isnull(x) else x)

    # Get employee mapping (using same connection)
    print("👥 Getting employee mapping...")
    cursor.execute("SELECT emp_id, name FROM employee_data")
    rows = cursor.fetchall()
    emp_map = {name: emp_id for emp_id, name in rows}

    # Get extension mapping (using same connection)
    cursor.execute("SELECT emp_id, extension FROM employee_data")
    rows = cursor.fetchall()
    ext_map = {extension: emp_id for emp_id, extension in rows}

    # Prepare all data for insertion
    print("🗂️ Preparing data for database insertion...")
    
    # Prepare activity report data
    data_clean['empid'] = data_clean['Team Member'].map(emp_map)
    cols = data_clean.columns.tolist()
    cols.insert(0, cols.pop(cols.index('empid')))
    data_clean = data_clean[cols]
    data_clean['Date'] = pd.to_datetime(data_clean['Date'], errors='coerce').dt.date

    # Prepare search string data
    search_string['empid'] = search_string['Searched By'].map(emp_map)
    cols = search_string.columns.tolist()
    cols.insert(0, cols.pop(cols.index('empid')))
    search_string = search_string[cols]

    search_string = search_string.loc[:, [col for col in search_string.columns if pd.notna(col) and str(col).strip().lower() != 'nan' and str(col).strip() != '']]

    print("search_string columns after cleaning:", search_string.columns.tolist())

    # Prepare email statistics data
    email_statistics['empid'] = email_statistics['User Name'].map(emp_map)
    cols = email_statistics.columns.tolist()
    cols.insert(0, cols.pop(cols.index('empid')))
    email_statistics = email_statistics[cols]
    email_statistics["Date"] = mysql_date

    # Prepare usage report data
    usage_report['empid'] = usage_report['User Name'].map(emp_map)
    cols = usage_report.columns.tolist()
    cols.insert(0, cols.pop(cols.index('empid')))
    usage_report = usage_report[cols]
    usage_report["Date"] = mysql_date

    # Prepare zoom outbound data
    dataout['empid'] = dataout['Extension'].map(ext_map)
    cols = dataout.columns.tolist()
    cols.insert(0, cols.pop(cols.index('empid')))
    dataout = dataout[cols]
    dataout['Date'] = mysql_date
    dataout['Time'] = pd.to_datetime(dataout['Time'], format='%m/%d/%Y %H:%M', errors='coerce')
    dataout['Time'] = dataout['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    dataout['Duration'] = dataout['Duration'].replace(['--', ' --', None], '00:00:00')
    dataout['Duration'] = pd.to_timedelta(dataout['Duration'], errors='coerce')
    dataout['Duration'] = dataout['Duration'].fillna(pd.Timedelta(seconds=0))
    dataout['Duration'] = dataout['Duration'].astype(str)
    dataout['Duration'] = dataout['Duration'].apply(lambda x: x.split(' ')[-1])

    # Prepare zoom inbound data
    datain['empid'] = datain['Extension'].map(ext_map)
    cols = datain.columns.tolist()
    cols.insert(0, cols.pop(cols.index('empid')))
    datain = datain[cols]
    datain['Date'] = mysql_date
    datain['Time'] = pd.to_datetime(datain['Time'], format='%m/%d/%Y %H:%M', errors='coerce')
    datain['Time'] = datain['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    datain['Duration'] = datain['Duration'].replace(['--', ' --', None], '00:00:00')
    datain['Duration'] = pd.to_timedelta(datain['Duration'], errors='coerce')
    datain['Duration'] = datain['Duration'].fillna(pd.Timedelta(seconds=0))
    datain['Duration'] = datain['Duration'].astype(str)
    datain['Duration'] = datain['Duration'].apply(lambda x: x.split(' ')[-1])

    print("search_string DataFrame columns:", search_string.columns.tolist())
    for idx, row in enumerate(search_string.head(10).values.tolist()):
        print(f"Record {idx}: {row}")

    # ========== ALL DATABASE INSERTIONS USING SAME CONNECTION ==========
    print("💾 Inserting all data into database...")
    
    try:
        # Insert activity report
        print("📝 Inserting activity report...")
        print(data_clean.head(10))

        insert_query = """
        INSERT INTO activity_report (
            empid, Date, `Team_Member`, `Assigned_Jobs`, Submissions,
            `Internal_Rejections`, `Pending_Feedback`,
            `Client_Submissions`, `Interviewer_Schedules`
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        records = data_clean[['empid', 'Date', 'Team Member', 'Assigned Jobs', 'Submissions',
                             'Internal Rejections', 'Pending Feedback', 'Client Submissions',
                             'Interviewer Schedules']].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"✅ Inserted {len(records)} activity report records")

        # Insert search string
        search_string = search_string.where(pd.notnull(search_string), None)

        print("🔍 Inserting search string data...")
        insert_query = """
        INSERT INTO search_string_report (emp_id,search_type,created,searched_by,search_string)
        VALUES (%s, %s, %s, %s, %s)
        """
        records = search_string[['empid', 'Search Type', 'Created At', 'Searched By', 'Search String']].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"✅ Inserted {len(records)} search string records")

        # Insert email statistics
        print("📧 Inserting email statistics...")
        email_statistics = email_statistics.where(pd.notnull(email_statistics), None)

        insert_query = """
        INSERT INTO email_statistics (emp_id,date,user_name,sent_count,open_count,bounce_count,new_contacts_added,total_contacts_owned)
        VALUES (%s, %s, %s, %s, %s,%s,%s,%s)
        """
        records = email_statistics[['empid', 'Date', 'User Name', 'Sent Count', 'Open Count',
                                   'Bounce Count','New Contacts Added','Total Contacts Owned']].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"✅ Inserted {len(records)} email statistics records")

        usage_report.drop_duplicates(subset=['empid', 'Date'], keep='last', inplace=True)

        # Insert usage report
        print("📊 Inserting usage report...")
        usage_report = usage_report.where(pd.notnull(usage_report), None)

        insert_query = """
        INSERT INTO usage_report (emp_id,date,user_name,career_builder,monster,dice,internal_db,total_views)
        VALUES (%s, %s, %s, %s, %s,%s,%s,%s)
        """
        records = usage_report[['empid', 'Date', 'User Name', 'Career Builder', 'Monster',
                               'Dice','Internal DB','Total Views']].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"✅ Inserted {len(records)} usage report records")

        # Insert zoom outbound data
        print("📞 Inserting zoom outbound data...")
        dataout = dataout.where(pd.notnull(dataout), None)
        datain = datain.where(pd.notnull(datain), None)

        insert_query = """
        INSERT INTO zoom_out_bound (
            emp_id, call_date, direction, call_from, call_to, extension,
            forward_to, device, call_time, result, path, duration,
            client_code, site, department, cost_center, charge, type,
            end_to_end_encryption
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        records = dataout[['empid', 'Date', 'Direction', 'From', 'To', 'Extension', 'Forward_to',
                          'Device', 'Time', 'Result', 'Path', 'Duration', 'Client_Code',
                          'Site', 'Department', 'Cost_Center', 'Charge', 'Type', 'EndtoEnd_Encryption']].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"✅ Inserted {len(records)} zoom outbound records")

        # Insert zoom inbound data
        print("📞 Inserting zoom inbound data...")
        insert_query = """
        INSERT INTO zoom_in_bound (
            emp_id, call_date, direction, call_from, call_to, extension,
            forward_to, device, call_time, result, path, duration,
            client_code, site, department, cost_center, charge, type,
            end_to_end_encryption
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        records = datain[['empid', 'Date', 'Direction', 'From', 'To', 'Extension', 'Forward_to',
                         'Device', 'Time', 'Result', 'Path', 'Duration', 'Client_Code',
                         'Site', 'Department', 'Cost_Center', 'Charge', 'Type', 'EndtoEnd_Encryption']].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"✅ Inserted {len(records)} zoom inbound records")

        # Insert attendance data
        print("🏢 Inserting attendance data...")
        insert_query = """
        INSERT INTO attendance_logs (
            employee_code, employee_name, attendance_date, type1, portion,
            type2, late_hrs, ot_hrs, in_time, out_time,
            total_hrs, shift_in_time, shift_out_time, latemark
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        records = att[['Employee Code', 'Employee Name', 'Date', 'Type1', 'Portion',
                      'Type2', 'Late Hrs', 'Ot Hrs', 'In Time', 'Out Time',
                      'Total\nHrs', 'Shift In Time', 'Shift Out Time', 'Latemark']].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"✅ Inserted {len(records)} attendance records")

        # Commit all changes
        conn.commit()
        print("✅ All database insertions completed successfully!")

    except mysql.connector.Error as err:
        print(f"❌ Database error: {err}")
        cursor.close()
        conn.close()
        if input_callback:
            return {'success': False, 'message': f"❌ Database error during insertion: {str(err)}", 'missing_members': []}
        else:
            raise Exception(f"Database error: {err}")

    # Close connection only at the very end
    cursor.close()
    conn.close()
    print("🔌 Database connection closed")

    # Export data to CSV files
    print("📤 Exporting data to CSV files...")
    search_string["Created At"] = formatted_date
    email_statistics["Date"] = formatted_date
    usage_report["Date"] = formatted_date
    dataout['Date'] = formatted_date
    datain['Date'] = formatted_date
    
    folder_name = f"all_data_file_{formatted_date}"
    os.makedirs(folder_name, exist_ok=True)
    
    outbound = os.path.join(folder_name, f"zoom_outbound_{formatted_date}.csv")
    inbound = os.path.join(folder_name, f"zoom_inbound_{formatted_date}.csv")
    searchstr = os.path.join(folder_name, f"search_string_{formatted_date}.csv")
    email = os.path.join(folder_name, f"email_statistics_{formatted_date}.csv")
    usage = os.path.join(folder_name, f"usage_report_{formatted_date}.csv")
    act = os.path.join(folder_name, f"activity_report_{formatted_date}.csv")
    atd = os.path.join(folder_name, f"attendance_report_{formatted_date}.csv")

    dataout.to_csv(outbound, index=False)
    datain.to_csv(inbound, index=False)
    search_string.to_csv(searchstr, index=False)
    email_statistics.to_csv(email, index=False)
    usage_report.to_csv(usage, index=False)
    data_clean.to_csv(act, index=False)
    att.to_csv(atd, index=False)
    
    print(f"✅ All files exported to: {folder_name}")

    # Return statement for Flask integration
    if input_callback:
        return {
            'success': True,
            'message': f"✅ The Data Has been Stored in the Database and the files have been exported to folder: {folder_name}",
            'missing_members': missing if 'missing' in locals() else [],
            'export_folder': folder_name
        }
    else:
        print(f"✅ Processing completed! Files exported to: {folder_name}")

# For testing without Flask
if __name__ == "__main__":
    process_data()
