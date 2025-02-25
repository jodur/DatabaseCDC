import pyodbc
import json
import datetime
import binascii

# Database connection details
myDatabase='Test_DB'
myTable='prospects'
server = 'localhost'
username = 'sa' 
password = 'Ikloopdoordestraat1@'

db_conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={myDatabase};UID={username};PWD={password};TrustServerCertificate=YES'

# Function to ensure the sync_tracking table exists
def ensure_tracking_table_exists():
    with pyodbc.connect(db_conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""           
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sync_tracking')
        BEGIN
            DECLARE @from_lsn AS BINARY (10);
            SET @from_lsn = sys.fn_cdc_get_min_lsn('dbo_{myTable}');  
            CREATE TABLE sync_tracking (
                id INT PRIMARY KEY,
                last_lsn BINARY(10),
                last_timestamp DATETIME
            )
            INSERT INTO sync_tracking (id, last_lsn, last_timestamp) VALUES (1, @from_lsn, GETDATE())
        END
        """)
        conn.commit()

# Function to get the last LSN and timestamp from the tracking table
def get_last_sync_info():
    with pyodbc.connect(db_conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT last_lsn, last_timestamp FROM sync_tracking WHERE id = 1")
        row = cursor.fetchone()
        return row.last_lsn, row.last_timestamp

# Function to update the last LSN and timestamp in the tracking table
def update_last_sync_info(last_lsn):

    last_timestamp = datetime.datetime.now()
    with pyodbc.connect(db_conn_str) as conn:      
        cursor = conn.cursor()
        query='''
            declare @last_lsn binary(10) = ?;
            UPDATE sync_tracking SET last_lsn = @last_lsn, last_timestamp = ? WHERE id = 1
            '''
        cursor.execute(query,last_lsn,last_timestamp)
        conn.commit()

# Function to fetch CDC changes from the source database
def fetch_cdc_changes(last_lsn):
    with pyodbc.connect(db_conn_str) as conn:
        cursor = conn.cursor()
        query = f"""
        DECLARE @max_lsn BINARY(10);
        SET @max_lsn =sys.fn_cdc_get_max_lsn();
        DECLARE @last_lsn BINARY(10) = ?;
        -- if the  @last_lsn is the same as the @max_lsn, then return no changes
        IF @last_lsn=@max_lsn 
        BEGIN
          -- Dummy select, alway returns no rows
          SELECT __$start_lsn,__$operation, __$update_mask, * 
          FROM cdc.fn_cdc_get_all_changes_dbo_{myTable}(@last_lsn, @max_lsn, 'all') WHERE 1=0;
        END
        ELSE
            BEGIN
                -- IF @last_lsn not equal to @min_ls, then increment the last_lsn befor query
                -- Only needed for the first entry when the tracking table is created
                IF @last_lsn <> sys.fn_cdc_get_min_lsn('dbo_{myTable}')
                BEGIN
                    SET @last_lsn = sys.fn_cdc_increment_lsn(@last_lsn);  
                END    
            SELECT __$start_lsn,__$operation, __$update_mask, * 
                FROM  cdc.fn_cdc_get_all_changes_dbo_{myTable}(@last_lsn, @max_lsn, 'all');
           END     
        """
        cursor.execute(query, last_lsn)
        rows = cursor.fetchall()
        return rows

# Function to simulate API calls
def simulate_api_call(operation, data):
    
    match operation:
        case 1:  # Delete
            print("POST /api/resource/delete", json.dumps(data, default=str), "/r/n")
        case 2:  # Insert
            print("POST /api/resource/insert", json.dumps(data, default=str), "/r/n")
        case 4:  # Update
            print("PUT /api/resource/update", json.dumps(data, default=str), "/r/n")

# Main function to perform the sync
def perform_sync():
    ensure_tracking_table_exists()
    last_lsn, last_timestamp = get_last_sync_info()
    print ("last lsn :",binascii.hexlify(last_lsn).decode()) 

    changes = fetch_cdc_changes(last_lsn)
    for change in changes:
        operation = change[1]
        data = {column[0]: value for column, value in zip(change.cursor_description[7:], change[7:])}
        try:
            simulate_api_call(operation, data) 
            # Update the last LSN after processing the change
            last_lsn=change[0] 
        except:       
            print("Error in processing the change") 
            break     

    # Update the last LSN and timestamp after processing changes
    update_last_sync_info(last_lsn)

if __name__ == "__main__":
    perform_sync()