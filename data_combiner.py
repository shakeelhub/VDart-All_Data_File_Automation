import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime

def combine_data():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Admin@123",
        )
        cursor = conn.cursor()
        cursor.execute("use All_Data_File")

        def fetch_table(table_name, query=None):
            cursor = conn.cursor()
            if not query:
                query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)

        print("📊 Fetching data from database tables...")
        
        activity_report     = fetch_table("activity_report")
        employee_data       = fetch_table("employee_data")
        email_statistics    = fetch_table("email_statistics")
        search_string       = fetch_table("search_string_report")  # Get all columns
        usage_report        = fetch_table("usage_report")
        zoom_in             = fetch_table("zoom_in_bound")
        zoom_out            = fetch_table("zoom_out_bound")
        attendance_log      = fetch_table("attendance_logs")

        # Get the date from the most recent activity_report records (latest insertion)
        # Assuming the most recent records are the ones just inserted by cleaning script
        latest_date = activity_report['date'].iloc[-1]  # Last record's date
        print(f"🗓️ Processing data for date: {latest_date}")

        print("🔄 Processing and aggregating data...")

        # Filter all tables by the latest date before aggregation
        activity_filtered = activity_report[activity_report['date'] == latest_date]
        email_filtered = email_statistics[email_statistics['date'] == latest_date]
        search_filtered = search_string[search_string['created'] == latest_date]
        usage_filtered = usage_report[usage_report['date'] == latest_date]
        zoom_in_filtered = zoom_in[zoom_in['call_date'] == latest_date]
        zoom_out_filtered = zoom_out[zoom_out['call_date'] == latest_date]
        attendance_filtered = attendance_log[attendance_log['attendance_date'] == latest_date]

        # Search string aggregation (using filtered data)
        search_agg = (
            search_filtered
            .groupby('emp_id')
            .size()  
            .reset_index(name='search_string_count')
            .rename(columns={'searched_by': 'emp_id'})
        )

        # Usage report aggregation (using filtered data)
        usage_agg = usage_filtered.groupby('emp_id')[['career_builder', 'monster', 'dice', 'internal_db', 'total_views']].sum().reset_index()

        # Zoom inbound aggregation (using filtered data)
        zoom_in_filtered['duration_sec'] = pd.to_timedelta(zoom_in_filtered['duration']).dt.total_seconds()
        zoom_in_agg = zoom_in_filtered.groupby('emp_id').agg(
            total_inbound_calls=('emp_id', 'count'),
            total_inbound_duration=('duration_sec', lambda x: pd.to_timedelta(x.sum(), unit='s')),
            inbound_calls_gt_1min=('duration_sec', lambda x: (x > 60).sum())
        ).reset_index()

        # Zoom outbound aggregation (using filtered data)
        zoom_out_filtered['duration_sec'] = pd.to_timedelta(zoom_out_filtered['duration']).dt.total_seconds()
        zoom_out_agg = zoom_out_filtered.groupby('emp_id').agg(
            total_outbound_calls_connected=('result', lambda x: (x == 'Call Connected').sum()),
            total_outbound_calls_not_connected=('result', lambda x: (x != 'Call Connected').sum()),
            total_outbound_duration=('duration_sec', lambda x: pd.to_timedelta(x.sum(), unit='s')),
            outbound_calls_gt_1min=('duration_sec', lambda x: (x > 60).sum()),
            outbound_duration_gt_1min=('duration_sec', lambda x: pd.to_timedelta(x[x > 60].sum(), unit='s'))
        ).reset_index()

        zoom_in_agg['total_inbound_duration'] = zoom_in_agg['total_inbound_duration'].astype(str).str.split().str[-1]
        zoom_out_agg['total_outbound_duration'] = zoom_out_agg['total_outbound_duration'].astype(str).str.split().str[-1]
        zoom_out_agg['outbound_duration_gt_1min'] = zoom_out_agg['outbound_duration_gt_1min'].astype(str).str.split().str[-1]

        # Attendance data (using filtered data)
        print("🔧 Processing attendance data to avoid duplicates...")
        attendance_trim = (
            attendance_filtered[['employee_code', 'type1']]
            .groupby('employee_code')
            .agg({
                'type1': lambda x: 'A' if 'A' in x.values else x.iloc[0]
            })
            .reset_index()
            .rename(columns={
                'employee_code': 'emp_id',
                'type1': 'attendance_type1'
            })
        )
        print(f"✅ Attendance data processed: {len(attendance_trim)} unique employees")

        print("🔗 Merging all data tables...")

        activity_filtered.rename(columns={'empid': 'emp_id'}, inplace=True)
        attendance_trim.rename(columns={'employee_code': 'emp_id'}, inplace=True)

        activity_filtered.rename(columns={
            'Team Member': 'employee_name',
            'Assigned Jobs': 'assigned_jobs',
            'Submissions': 'submission',
            'Internal Rejections': 'internal_rejections',
            'Client Submissions': 'client_submissions',
            'Interviewer Schedules': 'interviewer_schedules',
            'date': 'date'
        }, inplace=True)
        activity_filtered.rename(columns={'Date': 'date'}, inplace=True)

        attendance_trim.rename(columns={'employee_code': 'emp_id'}, inplace=True)

        # Create master table by merging all data (using filtered datasets)
        master = (
            activity_filtered.merge(employee_data, on='emp_id', how='left')
                           .merge(email_filtered,  on=['emp_id', 'date'], how='left')
                           .merge(search_agg, on='emp_id', how='left')
                           .merge(usage_agg, on='emp_id', how='left')
                           .merge(zoom_in_agg, on='emp_id', how='left')
                           .merge(zoom_out_agg, on='emp_id', how='left')
                           .merge(attendance_trim, on='emp_id', how='left')
        )
        
        master['closure'] = None
        master['starts'] = None  
        master['week'] = None

        master.rename(columns={'date_x': 'date'}, inplace=True)
        master.rename(columns={'team_member':'employee_name'},inplace=True)
        master.rename(columns={'submissions':'submission'},inplace=True)

        final_cols = [
            'emp_id', 'employee_name', 'date', 'client', 'status', 'track', 'email_id', 'role', 'reporting_to', 'team_name',
            'assigned_jobs', 'submission', 'internal_rejections', 'client_submissions', 'interviewer_schedules',
            'sent_count', 'open_count', 'search_string_count',
            'career_builder', 'monster', 'dice', 'internal_db', 'total_views',
            'total_inbound_calls', 'total_inbound_duration', 'inbound_calls_gt_1min',
            'total_outbound_calls_connected', 'total_outbound_calls_not_connected', 'total_outbound_duration',
            'outbound_calls_gt_1min', 'outbound_duration_gt_1min',
            'closure', 'starts', 'week', 'attendance_type1'
        ]

        master_final = master[final_cols]
        master_final = master[final_cols].drop_duplicates()

        print("🛠️ Processing data types and formatting...")

        master_final = master_final.copy()

        for col in master_final.columns:
            if pd.api.types.is_timedelta64_dtype(master_final[col]):
                master_final[col] = master_final[col].apply(
                    lambda x: str(x) if pd.notnull(x) else None
                )

        for col in master_final.columns:
            if pd.api.types.is_datetime64_any_dtype(master_final[col]):
                master_final[col] = master_final[col].apply(
                    lambda x: x.to_pydatetime() if pd.notnull(x) else None
                )

        if 'date' in master_final.columns:
            master_final.loc[:, 'week'] = master_final['date'].apply(
                lambda x: f"Week {x.isocalendar()[1]}" if pd.notnull(x) else None
            )
        else:
            raise KeyError("❌ 'date' column not found in master_final DataFrame.")

        print("💾 Inserting combined data into master_table...")

        # Create master_table with proper schema
        create_master_table_sql = """
        CREATE TABLE IF NOT EXISTS master_table (
            emp_id VARCHAR(255),
            employee_name VARCHAR(255),
            date DATE,
            client VARCHAR(255),
            status VARCHAR(255),
            track VARCHAR(255),
            email_id VARCHAR(255),
            role VARCHAR(255),
            reporting_to VARCHAR(255),
            team_name VARCHAR(255),
            assigned_jobs INT DEFAULT 0,
            submission INT DEFAULT 0,
            internal_rejections INT DEFAULT 0,
            client_submissions INT DEFAULT 0,
            interviewer_schedules INT DEFAULT 0,
            sent_count INT DEFAULT 0,
            open_count INT DEFAULT 0,
            search_string_count INT DEFAULT 0,
            career_builder INT DEFAULT 0,
            monster INT DEFAULT 0,
            dice INT DEFAULT 0,
            internal_db INT DEFAULT 0,
            total_views INT DEFAULT 0,
            total_inbound_calls INT DEFAULT 0,
            total_inbound_duration VARCHAR(255),
            inbound_calls_gt_1min INT DEFAULT 0,
            total_outbound_calls_connected INT DEFAULT 0,
            total_outbound_calls_not_connected INT DEFAULT 0,
            total_outbound_duration VARCHAR(255),
            outbound_calls_gt_1min INT DEFAULT 0,
            outbound_duration_gt_1min VARCHAR(255),
            closure INT DEFAULT 0,
            starts INT DEFAULT 0,
            week VARCHAR(255),
            attendance_type1 VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        cursor.execute(create_master_table_sql)
        conn.commit()
        print("✅ Master table schema created/verified")

        final_cols = list(master_final.columns)

        def safe_clean(val, col):
            # Int/float columns: replace NaN/None with 0
            int_columns = [
                'assigned_jobs', 'submission', 'internal_rejections', 'client_submissions', 'interviewer_schedules',
                'sent_count', 'open_count', 'search_string_count', 'career_builder', 'monster', 'dice',
                'internal_db', 'total_views', 'total_inbound_calls', 'inbound_calls_gt_1min',
                'total_outbound_calls_connected', 'total_outbound_calls_not_connected', 'outbound_calls_gt_1min',
                'closure', 'starts'
            ]
            if col in int_columns:
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    return 0
                if isinstance(val, str) and val.lower() == 'nan':
                    return 0
                return int(val) if isinstance(val, (int, float)) and not np.isnan(val) else 0
            # Time columns: treat NaN as None
            time_columns = [
                'total_inbound_duration', 'total_outbound_duration', 'outbound_duration_gt_1min'
            ]
            if col in time_columns:
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    return None
                if isinstance(val, str) and val.lower() == 'nan':
                    return None
                return val
            # String columns: treat NaN as None
            if val is None or (isinstance(val, float) and np.isnan(val)):
                return None
            if isinstance(val, str) and val.lower() == 'nan':
                return None
            return val

        cleaned_data = []
        for idx, row in master_final.iterrows():
            cleaned_row = [safe_clean(val, col) for val, col in zip(row, master_final.columns)]
            cleaned_data.append(cleaned_row)
        data = cleaned_data

        print("DataFrame columns:", final_cols)
        print("MySQL columns:")
        cursor.execute("SHOW COLUMNS FROM master_table")
        for col in cursor.fetchall():
            print(col)
        for i, row in enumerate(data[:10]):
            print(f"Row {i}:", row)

        insert_query = f"""
        INSERT INTO master_table ({', '.join(final_cols)})
        VALUES ({', '.join(['%s'] * len(final_cols))})
        """

        cursor.executemany(insert_query, data)
        conn.commit()

        print(f"✅ Inserted {len(data)} records into master_table for date: {latest_date}")

        print("🔄 Updating NULL values to 0...")

        exclude_cols = {'closure', 'starts', 'track', 'emp_id', 'employee_name', 'date', 'client', 'status', 'email_id', 'role', 'reporting_to', 'team_name', 'week', 'attendance_type1', 'total_inbound_duration', 'total_outbound_duration', 'outbound_duration_gt_1min'}
        update_cols = [col for col in master_final.columns if col not in exclude_cols]
        set_clauses = [f"{col} = IFNULL({col}, 0)" for col in update_cols]
        update_sql = f"UPDATE master_table SET {', '.join(set_clauses)}"

        cursor.execute(update_sql)
        conn.commit()
        print("✅ All NULL values (except string columns) updated to 0.")

        print("📤 Generating final output files...")

        try:
            query = "SELECT * FROM master_table;"
            cursor.execute(query)

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            df = pd.DataFrame(rows, columns=columns)
            print(f"📊 DataFrame has {len(df.columns)} columns: {list(df.columns)}")

            # Clean duration columns
            duration_cols = ['total_inbound_duration', 'total_outbound_duration', 'outbound_duration_gt_1min']
            for col in duration_cols:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: str(x).split(" ")[-1] if pd.notnull(x) and str(x) != 'None' else None
                    )

            desired_column_order = [
        'date', 'client', 'emp_id', 'status', 'track', 'employee_name', 'email_id', 
        'role', 'reporting_to', 'team_name', 'assigned_jobs', 'submission', 
        'internal_rejections', 'client_submissions', 'interviewer_schedules', 
        'sent_count', 'open_count', 'search_string_count', 'career_builder', 
        'monster', 'dice', 'internal_db', 'total_views', 'total_inbound_calls', 
        'total_inbound_duration', 'inbound_calls_gt_1min', 'total_outbound_calls_connected', 
        'total_outbound_calls_not_connected', 'total_outbound_duration', 
        'outbound_calls_gt_1min', 'outbound_duration_gt_1min', 'closure', 
        'week', 'attendance_type1', 'starts'
    ]
            
            missing_cols = [col for col in desired_column_order if col not in df.columns]
            extra_cols = [col for col in df.columns if col not in desired_column_order]
            if missing_cols:
                 print(f"⚠️ Missing columns: {missing_cols}")
            if extra_cols:
                print(f"⚠️ Extra columns not in desired order: {extra_cols}")

            
            existing_ordered_cols = [col for col in desired_column_order if col in df.columns]
            final_column_order = existing_ordered_cols + extra_cols
            df = df[final_column_order]
            

            # Save to CSV
            df.to_csv('master_table_final.csv', index=False)
            print(f"✅ Final output file created: master_table_final.csv ({len(df)} records)")
            
        except Exception as csv_error:
            print(f"⚠️ Error creating CSV file: {csv_error}")
            print("✅ Data processing completed successfully, but CSV generation failed.")
        
        cursor.close()
        conn.close()

        return {
            'success': True,
            'message': f"✅ Data combination completed successfully for date {latest_date}! {len(data)} records processed and inserted into master_table. CSV file 'master_table_final.csv' has been created.",
            'records_processed': len(data),
            'processed_date': latest_date
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
            'message': f"❌ Error during data combination: {str(e)}",
            'records_processed': 0
        }

if __name__ == "__main__":
    result = combine_data()
    print(result)