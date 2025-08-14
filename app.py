from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_file
import os
import threading
import queue
from werkzeug.utils import secure_filename
import time
import mysql.connector
import pandas as pd
import tempfile
from cleaning import process_data  
from data_combiner import combine_data   
from closure import process_closure  
from starts import process_starts    

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this-to-something-secure'

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'csv', 'xls'}
MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Create upload directory
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Global variables for handling user input requests
input_queues = {}
processing_status = {}

# File mapping - main 6 files only
FILE_MAPPINGS = {
    
        'activity_report': 'activityreport2.xlsx',
    'zoom_call': 'zoomus_call_log_2025-06-11.csv', 
    'search_string': 'searchstring.xlsx',
    'usage_report': 'usage_report_sample.xlsx',
    'email_stats': 'email_statistics.xlsx',
    'attendance': 'attendance_sample.xlsx'
}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS



def get_mysql_connection():
    """Create MySQL database connection"""
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Admin@123",
            database="All_Data_File"
        )
        return conn
    except mysql.connector.Error as err:
        print(f"❌ Database connection error: {err}")
        return None

@app.route('/')
def index():
    """Home page with navigation options"""
    return render_template('index.html')

@app.route('/upload')
def upload_page():
    """Main upload page for 6 files"""
    return render_template('upload.html')

@app.route('/upload_closure')
def upload_closure_page():
    """Closure upload page"""
    return render_template('upload_closure.html')

@app.route('/upload_starts')
def upload_starts_page():
    """Starts upload page"""
    return render_template('upload_starts.html')

@app.route('/view')
def view_page():
    """View data page"""
    return render_template('view.html')

@app.route('/upload_files', methods=['POST'])
def upload_files():
    """Handle main 6 files upload and start processing"""
    session_id = session.get('session_id', str(time.time()))
    session['session_id'] = session_id
    
    uploaded_files = []
    missing_files = []
    
    # Check each required file (6 files only)
    for field_name, target_filename in FILE_MAPPINGS.items():
        if field_name not in request.files:
            missing_files.append(field_name.replace('_', ' ').title())
            continue
            
        file = request.files[field_name]
        if file.filename == '':
            missing_files.append(field_name.replace('_', ' ').title())
            continue
            
        if file and allowed_file(file.filename):
            # Save with the target filename
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], target_filename)
            file.save(filepath)
            uploaded_files.append(target_filename)
            print(f"✅ Uploaded: {file.filename} → {target_filename}")
        else:
            flash(f'Invalid file type for {field_name.replace("_", " ").title()}. Please upload Excel (.xlsx) or CSV files only.')
            return redirect(url_for('upload_page'))
    
    if missing_files:
        flash(f'Please upload all required files. Missing: {", ".join(missing_files)}')
        return redirect(url_for('upload_page'))
    
    if len(uploaded_files) == 6:  # Changed back to 6
        flash(f'Successfully uploaded all {len(uploaded_files)} files!')
        print(f"🚀 Starting main data processing for session {session_id}")
        
        # Start main processing in background (only stages 1-2)
        threading.Thread(target=run_main_data_processing, args=(session_id,), daemon=True).start()
        return redirect(url_for('processing', session_id=session_id))
    else:
        flash('Please upload all 6 required files.')
        return redirect(url_for('upload_page'))



@app.route('/get_full_metrics')
def get_full_metrics():
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                SUM(submission) as total_submissions,
                SUM(client_submissions) as client_submissions, 
                SUM(interviewer_schedules) as interviews_scheduled
            FROM master_table
        """)
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'metrics': {
                'totalSubmissions': result[0] or 0,
                'clientSubmissions': result[1] or 0,
                'interviewsScheduled': result[2] or 0
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/upload_closure_file', methods=['POST'])
def upload_closure_file():
    """Handle closure file upload and processing"""
    if 'closure_file' not in request.files:
        flash('Please select a closure file to upload.')
        return redirect(url_for('upload_closure_page'))
    
    file = request.files['closure_file']
    if file.filename == '':
        flash('Please select a closure file to upload.')
        return redirect(url_for('upload_closure_page'))
    
    if file and allowed_file(file.filename):
        # Save as closure.xlsx
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'closure.xlsx')
        file.save(filepath)
        print(f"✅ Uploaded closure file: {file.filename} → closure.xlsx")
        
        session_id = str(time.time())
        session['closure_session_id'] = session_id
        
        # Start closure processing
        threading.Thread(target=run_closure_processing, args=(session_id,), daemon=True).start()
        return redirect(url_for('closure_processing', session_id=session_id))
    else:
        flash('Invalid file type. Please upload Excel (.xlsx, .xls) files only.')
        return redirect(url_for('upload_closure_page'))
    



@app.route('/upload_starts_file', methods=['POST'])
def upload_starts_file():
    """Handle starts file upload and processing"""
    if 'starts_file' not in request.files:
        flash('Please select a starts file to upload.')
        return redirect(url_for('upload_starts_page'))
    
    file = request.files['starts_file']
    if file.filename == '':
        flash('Please select a starts file to upload.')
        return redirect(url_for('upload_starts_page'))
    
    if file and allowed_file(file.filename):
        # Save as starts.xlsx
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'starts.xlsx')
        file.save(filepath)
        print(f"✅ Uploaded starts file: {file.filename} → starts.xlsx")
        
        session_id = str(time.time())
        session['starts_session_id'] = session_id
        
        # Start starts processing
        threading.Thread(target=run_starts_processing, args=(session_id,), daemon=True).start()
        return redirect(url_for('starts_processing', session_id=session_id))
    else:
        flash('Invalid file type. Please upload Excel (.xlsx, .xls) files only.')
        return redirect(url_for('upload_starts_page'))

@app.route('/processing/<session_id>')
def processing(session_id):
    """Show main processing progress page"""
    return render_template('processing.html', session_id=session_id, process_type="main")

@app.route('/closure_processing/<session_id>')
def closure_processing(session_id):
    """Show closure processing progress page"""
    return render_template('processing.html', session_id=session_id, process_type="closure")

@app.route('/starts_processing/<session_id>')
def starts_processing(session_id):
    """Show starts processing progress page"""
    return render_template('processing.html', session_id=session_id, process_type="starts")

@app.route('/get_status/<session_id>')
def get_status(session_id):
    """Check for input requests or completion status"""
    
    # Check for input request first
    if session_id in input_queues:
        try:
            request_data = input_queues[session_id]['request_queue'].get_nowait()
            return jsonify({
                'status': 'input_required',
                'prompt': request_data['prompt'],
                'missing_count': request_data.get('missing_count', 0),
                'missing_members': request_data.get('missing_members', [])
            })
        except queue.Empty:
            pass
    
    # Check processing status
    status = processing_status.get(session_id, {})
    
    if status.get('complete', False):
        return jsonify({
            'status': 'complete',
            'success': status.get('success', True),
            'message': status.get('message', 'Processing completed!')
        })
    elif status.get('error', False):
        return jsonify({
            'status': 'error',
            'message': status.get('message', 'An error occurred during processing.')
        })
    else:
        return jsonify({
            'status': 'processing',
            'message': status.get('message', 'Processing your files...')
        })

@app.route('/submit_decision/<session_id>', methods=['POST'])
def submit_decision(session_id):
    """Handle user's continue/stop decision"""
    if session_id in input_queues:
        decision = request.json.get('decision', 'no')  # 'yes' or 'no'
        input_queues[session_id]['response_queue'].put(decision)
        print(f"📝 User decision for session {session_id}: {decision}")
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Session not found'})

@app.route('/get_data_preview')
def get_data_preview():
    """Get preview of table data"""
    # Check if table parameter is provided
    table_name = request.args.get('table')
    
    # Add debug logging
    print(f"📊 get_data_preview called with table: {table_name}")
    
    try:
        conn = get_mysql_connection()
        if not conn:
            return jsonify({
                'success': False,
                'error': 'Database connection failed'
            })

        cursor = conn.cursor()
        
        # Determine which table to query
        if table_name == 'closure':
            db_table = 'closure'
            print(f"✅ Loading data from closure table")
        else:
            db_table = 'master_table'
            print(f"✅ Loading data from master_table")
        
        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{db_table}'")
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            print(f"❌ Table {db_table} not found")
            return jsonify({
                'success': False,
                'error': f'{db_table} not found in database. Please run processing first.'
            })

        # Get total record count
        cursor.execute(f"SELECT COUNT(*) FROM {db_table}")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total records in {db_table}: {total_records}")

        # Get column information
        cursor.execute(f"DESCRIBE {db_table}")
        columns_info = cursor.fetchall()
        column_names = [col[0] for col in columns_info]
        print(f"📋 Columns found in {db_table}: {len(column_names)} columns")
        print(f"📋 First 5 columns: {column_names[:5]}")

        # Get sample data (first 500 rows for preview)
        print(f"🔄 Loading sample of 500 records for preview from {db_table}...")
        cursor.execute(f"SELECT * FROM {db_table} LIMIT 500")
        sample_rows = cursor.fetchall()

        # Convert to list of dictionaries
        sample_data = []
        for row in sample_rows:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                value = row[i]
                if value is None:
                    row_dict[col_name] = ''
                elif isinstance(value, (int, float, str)):
                    row_dict[col_name] = str(value)
                elif hasattr(value, 'strftime'):
                    row_dict[col_name] = value.strftime('%Y-%m-%d') if 'date' in col_name.lower() else str(value)
                else:
                    row_dict[col_name] = str(value)
            sample_data.append(row_dict)

        print(f"📋 Sample data prepared: {len(sample_data)} rows from {db_table}")

        # Get date range from ALL records
        date_info = {}
        date_column = None
        
        # Different date columns for different tables
        if table_name == 'closure':
            possible_date_cols = ['Tentative Start Date', 'Date', 'date', 'DATE']
        else:
            possible_date_cols = ['date', 'Date', 'DATE', 'created_at', 'timestamp', 'time']
            
        for col in possible_date_cols:
            if col in column_names:
                date_column = col
                break

        print(f"📅 Date column found: {date_column}")

        if date_column:
            try:
                print(f"🔍 Getting full date range from ALL {total_records} records...")
                cursor.execute(f"""
                    SELECT MIN(`{date_column}`) as min_date, 
                           MAX(`{date_column}`) as max_date,
                           COUNT(`{date_column}`) as date_count
                    FROM {db_table} 
                    WHERE `{date_column}` IS NOT NULL
                """)
                date_result = cursor.fetchone()
                
                if date_result and date_result[0]:
                    min_date = date_result[0]
                    max_date = date_result[1]
                    
                    if hasattr(min_date, 'strftime'):
                        min_date_str = min_date.strftime('%Y-%m-%d')
                        max_date_str = max_date.strftime('%Y-%m-%d')
                    else:
                        min_date_str = str(min_date)
                        max_date_str = str(max_date)
                    
                    date_info = {
                        'min_date': min_date_str,
                        'max_date': max_date_str,
                        'date_count': date_result[2],
                        'date_column': date_column
                    }
                    print(f"📅 Date range: {min_date_str} to {max_date_str}")
            except Exception as e:
                print(f"⚠️ Error getting date range: {e}")

        # Prepare response
        summary = {
            'total_records': total_records,
            'total_columns': len(column_names),
            'columns': column_names,
            'sample_size': len(sample_data),
            'table_name': db_table  # Add this to confirm which table was loaded
        }

        response_data = {
            'success': True,
            'summary': summary,
            'sample_data': sample_data,
            'date_info': date_info,
            'has_more_data': True
        }

        cursor.close()
        conn.close()
        
        print(f"✅ Successfully returned data from {db_table}")
        return jsonify(response_data)

    except Exception as e:
        print(f"❌ Error in get_data_preview: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}'
        })


@app.route('/filter_data', methods=['POST'])
def filter_data():
    try:
        data = request.get_json()
        
        # Check if this is a unique values request (from previous step)
        if data.get('action') == 'get_unique_values':
            # ... keep your existing unique values code here
            pass
        
        # Regular filter request
        table_name = data.get('table')
        column_filters = data.get('column_filters', {})
        
        # For backward compatibility with date filter
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        print(f"🔍 Filtering {table_name} with column filters: {column_filters}")
        
        conn = get_mysql_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        # Determine table
        db_table = 'closure' if table_name == 'closure' else 'master_table'
        
        # Get column information
        cursor.execute(f"DESCRIBE {db_table}")
        columns_info = cursor.fetchall()
        column_names = [col[0] for col in columns_info]
        
        # Build query with multiple filters
        base_query = f"SELECT * FROM {db_table}"
        conditions = []
        params = []
        
        # Handle column filters
        for column, filter_data in column_filters.items():
            filter_type = filter_data.get('type')
            filter_value = filter_data.get('value')
            
            if filter_type == 'checkbox' and filter_value and len(filter_value) > 0:
                # For checkbox filters (multiple values)
                placeholders = ', '.join(['%s'] * len(filter_value))
                conditions.append(f"`{column}` IN ({placeholders})")
                params.extend(filter_value)
                
            elif filter_type == 'text' and filter_value:
                # For text search (contains)
                conditions.append(f"`{column}` LIKE %s")
                params.append(f"%{filter_value}%")
                
            elif filter_type == 'date' and filter_value:
                # For date range
                if filter_value.get('from'):
                    conditions.append(f"`{column}` >= %s")
                    params.append(filter_value['from'])
                if filter_value.get('to'):
                    conditions.append(f"`{column}` <= %s")
                    params.append(filter_value['to'])
                    
            elif filter_type == 'number' and filter_value:
                # For number range
                if filter_value.get('min'):
                    conditions.append(f"`{column}` >= %s")
                    params.append(float(filter_value['min']))
                if filter_value.get('max'):
                    conditions.append(f"`{column}` <= %s")
                    params.append(float(filter_value['max']))
        
        # Handle legacy date filter (backward compatibility)
        if start_date or end_date:
            date_column = data.get('date_column')
            if not date_column:
                # Auto-detect date column
                if table_name == 'closure':
                    possible_date_cols = ['Date', 'date', 'DATE', 'Tentative Start Date']
                else:
                    possible_date_cols = ['date', 'Date', 'DATE']
                    
                for col in possible_date_cols:
                    if col in column_names:
                        date_column = col
                        break
            
            if date_column:
                if start_date:
                    conditions.append(f"`{date_column}` >= %s")
                    params.append(start_date)
                if end_date:
                    conditions.append(f"`{date_column}` <= %s")
                    params.append(end_date)
        
        # Build final query
        if conditions:
            query = f"{base_query} WHERE {' AND '.join(conditions)}"
        else:
            query = base_query
        
        print(f"📊 Query: {query}")
        print(f"📊 Params: {params}")
        
        # Execute query
        cursor.execute(query, params)
        filtered_rows = cursor.fetchall()
        
        print(f"✅ Found {len(filtered_rows)} records matching filters")
        
        # Convert to list of dictionaries
        filtered_data = []
        for row in filtered_rows:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                value = row[i]
                if value is None:
                    row_dict[col_name] = ''
                elif isinstance(value, (int, float, str)):
                    row_dict[col_name] = str(value)
                elif hasattr(value, 'strftime'):
                    row_dict[col_name] = value.strftime('%Y-%m-%d')
                else:
                    row_dict[col_name] = str(value)
            filtered_data.append(row_dict)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': filtered_data,
            'total_filtered': len(filtered_data),
            'filters_applied': len(column_filters)
        })
        
    except Exception as e:
        print(f"❌ Error in filter_data: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Filter error: {str(e)}'}), 500


@app.route('/download/<filename>')
def download_file(filename):
    """Download data from MySQL database as CSV"""
    try:
        # Determine which table to download
        if filename == 'closure_table.csv':
            db_table = 'closure'
            download_name = f'closure_table_complete_{int(time.time())}.csv'
        elif filename == 'master_table_final.csv':
            db_table = 'master_table'
            download_name = f'master_table_complete_{int(time.time())}.csv'
        else:
            return jsonify({'error': 'File not found'}), 404

        conn = get_mysql_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{db_table}'")
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'error': f'{db_table} not found in database'}), 404

        # Get all data
        cursor.execute(f"SELECT * FROM {db_table}")
        rows = cursor.fetchall()

        # Get column names
        cursor.execute(f"DESCRIBE {db_table}")
        columns_info = cursor.fetchall()
        column_names = [col[0] for col in columns_info]

        cursor.close()
        conn.close()

        # Convert to pandas DataFrame
        data = []
        for row in rows:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                value = row[i]
                if value is None:
                    row_dict[col_name] = ''
                elif isinstance(value, (int, float)):
                    row_dict[col_name] = value
                elif hasattr(value, 'strftime'):
                    row_dict[col_name] = value.strftime('%Y-%m-%d') if 'date' in col_name.lower() else str(value)
                else:
                    row_dict[col_name] = str(value)
            data.append(row_dict)

        df = pd.DataFrame(data)

        # Create temporary CSV file
        temp_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False)
        temp_filename = temp_file.name
        temp_file.close()

        # Write CSV
        df.to_csv(temp_filename, index=False)

        # Send file
        response = send_file(
            temp_filename,
            as_attachment=True,
            download_name=download_name,
            mimetype='text/csv'
        )

        response.call_on_close(lambda: os.unlink(temp_filename) if os.path.exists(temp_filename) else None)

        return response

    except Exception as e:
        print(f"❌ Download error: {str(e)}")
        return jsonify({'error': f'Error downloading data: {str(e)}'}), 500
    

    
def get_user_input_from_web(prompt, missing_count=0, missing_members=None):
    """Function that replaces input() - communicates with web UI"""
    session_id = threading.current_thread().session_id
    
    if session_id not in input_queues:
        input_queues[session_id] = {
            'request_queue': queue.Queue(),
            'response_queue': queue.Queue()
        }
    
    print(f"🤔 Requesting user input for session {session_id}")
    
    # Send request to web UI
    input_queues[session_id]['request_queue'].put({
        'prompt': prompt,
        'missing_count': missing_count,
        'missing_members': missing_members or []
    })
    
    # Wait for user response
    print(f"⏳ Waiting for user response...")
    response = input_queues[session_id]['response_queue'].get()
    print(f"✅ User responded: {response}")
    return response

def run_main_data_processing(session_id):
    """Background thread that runs main data processing (stages 1-2 only)"""
    try:
        # Store session_id in thread for the input callback
        threading.current_thread().session_id = session_id
        
        print(f"🔄 Starting Stage 1 for session {session_id}")
        # Stage 1: Initial data processing (cleaning.py)
        processing_status[session_id] = {
            'complete': False,
            'message': 'Stage 1: Processing uploaded files and cleaning data...'
        }
        
        result1 = process_data(input_callback=get_user_input_from_web)
        
        if not result1 or not result1['success']:
            processing_status[session_id] = {
                'complete': True,
                'success': False,
                'message': result1['message'] if result1 else 'Stage 1 failed'
            }
            return
        
        print(f"✅ Stage 1 completed for session {session_id}")
        print(f"🔄 Starting Stage 2 for session {session_id}")
        
        # Stage 2: Data combination and master table creation (data_combiner.py)
        processing_status[session_id] = {
            'complete': False,
            'message': 'Stage 2: Combining data and creating master table...'
        }
        
        result2 = combine_data()
        
        if not result2 or not result2['success']:
            processing_status[session_id] = {
                'complete': True,
                'success': False,
                'message': result2['message'] if result2 else 'Stage 2 failed'
            }
            return
        
        print(f"✅ Stage 2 completed for session {session_id}")
        
        # MAIN PROCESSING COMPLETED (No closure/starts in main flow)
        total_records = result2.get('records_processed', 0) if isinstance(result2, dict) else 0
        processing_status[session_id] = {
            'complete': True,
            'success': True,
            'message': f"""🎉 Main data processing completed successfully!

✅ Stage 1: Data cleaning and processing completed
✅ Stage 2: Master table created with {total_records} records  

Your main data is ready! You can now upload closure and starts files separately if needed."""
        }
        print(f"🎉 Main processing completed successfully for session {session_id}")
            
    except Exception as e:
        print(f"❌ Processing error for session {session_id}: {str(e)}")
        processing_status[session_id] = {
            'complete': True,
            'error': True,
            'success': False,
            'message': f'❌ Error during processing: {str(e)}'
        }
    finally:
        # Clean up
        if session_id in input_queues:
            del input_queues[session_id]

def run_closure_processing(session_id):
    """Background thread for closure processing only"""
    try:
        print(f"🔄 Starting closure processing for session {session_id}")
        
        processing_status[session_id] = {
            'complete': False,
            'message': 'Processing closure data...'
        }
        
        result = process_closure()
        
        if result and result['success']:
            processing_status[session_id] = {
                'complete': True,
                'success': True,
                'message': f"✅ Closure processing completed! Updated {result.get('master_table_updates', 0)} records in master table."
            }
        else:
            processing_status[session_id] = {
                'complete': True,
                'success': False,
                'message': result['message'] if result else 'Closure processing failed'
            }
            
    except Exception as e:
        processing_status[session_id] = {
            'complete': True,
            'error': True,
            'success': False,
            'message': f'❌ Error during closure processing: {str(e)}'
        }

def run_starts_processing(session_id):
    """Background thread for starts processing only"""
    try:
        print(f"🔄 Starting starts processing for session {session_id}")
        
        processing_status[session_id] = {
            'complete': False,
            'message': 'Processing starts data...'
        }
        
        result = process_starts()
        
        if result and result['success']:
            processing_status[session_id] = {
                'complete': True,
                'success': True,
                'message': f"✅ Starts processing completed! Updated {result.get('master_table_updates', 0)} records in master table."
            }
        else:
            processing_status[session_id] = {
                'complete': True,
                'success': False,
                'message': result['message'] if result else 'Starts processing failed'
            }
            
    except Exception as e:
        processing_status[session_id] = {
            'complete': True,
            'error': True,
            'success': False,
            'message': f'❌ Error during starts processing: {str(e)}'
        }

@app.route('/get_column_unique_values', methods=['POST'])
def get_column_unique_values():
    """Get unique values for a specific column"""
    try:
        data = request.get_json()
        table_name = data.get('table')
        column_name = data.get('column')
        
        print(f"📊 Getting unique values for {column_name} in {table_name}")
        
        conn = get_mysql_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'})
        
        cursor = conn.cursor()
        
        # Determine which table to query
        if table_name == 'closure':
            db_table = 'closure'
        else:
            db_table = 'master_table'
        
        # Get unique values - with proper escaping for column names with spaces
        try:
            query = f"""
                SELECT DISTINCT `{column_name}` 
                FROM {db_table} 
                WHERE `{column_name}` IS NOT NULL 
                ORDER BY `{column_name}` 
                LIMIT 1000
            """
            
            print(f"📊 Query: {query}")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Convert to list
            unique_values = [row[0] for row in rows]
            
            # Convert dates/other types to strings if needed
            processed_values = []
            for value in unique_values:
                if hasattr(value, 'strftime'):
                    processed_values.append(value.strftime('%Y-%m-%d'))
                else:
                    processed_values.append(str(value) if value is not None else '')
            
            cursor.close()
            conn.close()
            
            print(f"✅ Found {len(processed_values)} unique values for {column_name}")
            
            return jsonify({
                'success': True,
                'values': processed_values,
                'count': len(processed_values),
                'column': column_name
            })
            
        except mysql.connector.Error as e:
            print(f"❌ MySQL Error: {str(e)}")
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': f'Database error: {str(e)}'})
        
    except Exception as e:
        print(f"❌ Error getting unique values: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})
    


@app.route('/debug_info')
def debug_info():
    """Debug endpoint to check registered routes"""
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            'endpoint': rule.endpoint,
            'methods': list(rule.methods),
            'path': str(rule)
        })
    return jsonify({
        'total_routes': len(routes),
        'routes': sorted(routes, key=lambda x: x['path']),
        'looking_for': '/get_column_unique_values'
    })





if __name__ == '__main__':
    print("🚀 Starting Data Processing Dashboard...")
    print("📝 Upload files at: http://localhost:8080")
    print("📝 Upload files at: http://alldataautomation.vdartinc.com:8080/")
    app.run(debug=True, host='0.0.0.0', port=8080)



