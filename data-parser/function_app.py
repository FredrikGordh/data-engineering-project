import logging
import json
import datetime
import os
import psycopg2
from io import StringIO
import csv



import azure.functions as func
from azure.storage.blob import BlobServiceClient
# from azure.identity import ManagedIdentityCredential


app = func.FunctionApp()
# Base Url for the blob
BLOB_URL = 'https://blobstorageaccount987.blob.core.windows.net'
INPUT_CONTAINER_NAME = 'input-data'
BLOB_NAME = 'test_json.json'  
STORAGE_URL = 'https://storagelowlatency.blob.core.windows.net'
LOG_CONTAINER_NAME = 'process-logs'  # Container for storing logs
LOG_BLOB_NAME = 'process_log.csv'  # Blob name for the log file

def get_blob_content(blob_url, container_name, blob_name):
    try:
        # Create the BlobServiceClient, object
        # blob_service_client = BlobServiceClient(account_url=blob_url, credential=os.getenv('BLOB_CREDENTIAL'))
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv('INPUT_BLOB_STORAGE_CONNECTION_STRING'))
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        logging.debug(f'blob_client: {blob_client}')
        blob_data = blob_client.download_blob().readall()

        return (blob_data.decode('utf-8'))
    except Exception as e:
        logging.error(f'Error accessing blob: {e}')
        raise

def get_blob_input_name(container_name, blob_name):
    blob_service_client = BlobServiceClient(account_url=BLOB_URL, credential=os.getenv('BLOB_CREDENTIAL'))
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    properties = blob_client.get_blob_properties()
    return properties['last_modified']

def insert_data_to_postgres(data_list, blob_input_name, event_time, instance_id):
    try:
        if os.getenv('RUN_TEST_ON_VM'):
            # Connect to VM PostgreSQL
            connection = psycopg2.connect(
                dbname=os.getenv('VM_DB_NAME'),
                user=os.getenv('VM_DB_USER'),
                password=os.getenv('VM_DB_PASSWORD'),
                host=os.getenv('VM_DB_HOST'),
                port=os.getenv('VM_DB_PORT')
            )
            logging.debug('Connecting to Postgres on VM')
        else:
            # Connect to Azure PostgreSQL
            connection = psycopg2.connect(
                dbname=os.getenv('POSTGRESQL_DB'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
            logging.debug('Connecting to Postgres')
        cursor = connection.cursor()

        # Insert data into PostgreSQL
        insert_query = """
        INSERT INTO fora_table (business_partner_number_admin, swedish_social_security_number, first_name, last_name, email, phone_number, admin_type, admin_type_code, business_partner_number_organization, valid_from, valid_to)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values_list = [
            (
                row.get('business_partner_number_admin'),
                row.get('swedish_social_security_number'),
                row.get('first_name'),
                row.get('last_name'),
                row.get('email'),
                row.get('phone_number'),
                row.get('admin_type'),
                row.get('admin_type_code'),
                row.get('business_partner_number_organization'),
                row.get('valid_from'),
                row.get('valid_to')
            ) for row in data_list
        ]
        cursor.executemany(insert_query, values_list)

        # Commit the transaction
        connection.commit()
        completion_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Capture with milliseconds
        logging.info(f'Data inserted into database at time: {completion_time}')
        logging.info('Admin data inserted into PostgreSQL successfully')

        if os.getenv('RUN_TEST_ON_VM'):
            # Close the connection
            cursor.close()
            connection.close()
            # Connect to Azure PostgreSQL
            connection = psycopg2.connect(
                dbname=os.getenv('POSTGRESQL_DB'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
            cursor = connection.cursor()


        log_query = """
        INSERT INTO process_log (file_name, event_time, completion_time, processing_duration_in_sec, test_category, no_rows, instance_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Split the name String using underscores as delimiters
        parts = blob_input_name.split("_")
        # Extract T1 and no_rows
        T1 = parts[1]
        no_rows = parts[2]
        logging.debug(f'printing parsed strings, test_category: {T1}, number of rows: {no_rows}')

        # Calculate processing duration
        event_time_dt = datetime.datetime.strptime(event_time, '%Y-%m-%d %H:%M:%S.%f')
        completion_time_dt = datetime.datetime.strptime(completion_time, '%Y-%m-%d %H:%M:%S.%f')
        processing_duration = (completion_time_dt - event_time_dt).total_seconds()
        cursor.execute(log_query, (blob_input_name, event_time, completion_time, processing_duration, T1, no_rows, instance_id))

        # Commit the transaction
        connection.commit()
        
        # Close the connection
        cursor.close()
        connection.close()
        logging.info('Process logs inserted into PostgreSQL successfully')

        
    except Exception as e:
	    logging.error(f'Error inserting data into PostgreSQL: {e}')

def fetch_data_from_postgres():
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            dbname=os.getenv('POSTGRESQL_DB'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        cursor = connection.cursor()
        
        # Fetch data from PostgreSQL
        fetch_query = "SELECT * FROM process_log;"
        cursor.execute(fetch_query)
        records = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return records
    except Exception as e:
        logging.error(f'Error fetching data from PostgreSQL: {e}')
        return []
    
def save_fetched_data_to_csv(records):
    try:
        blob_deploy_service_client = BlobServiceClient(account_url=STORAGE_URL, credential=os.getenv('PROCESS_LOG_STORAGE_CREDENTIAL'))
        container_client = blob_deploy_service_client.get_container_client(LOG_CONTAINER_NAME)
        blob_deploy_client = container_client.get_blob_client(LOG_BLOB_NAME)
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Write headers if the file is new
        if not blob_deploy_client.exists():
            writer.writerow(['upload_time', 'event_time', 'completion_time', 'processing_duration_in_sec'])
        
        # Write records
        for record in records:
            writer.writerow(record)
        
        # Upload the new log content, overwriting the existing blob if necessary
        blob_deploy_client.upload_blob(output.getvalue(), overwrite=True)
        
        logging.info(f'Fetched data logged to blob: {LOG_BLOB_NAME}')
    except Exception as e:
        logging.error(f'Error writing fetched data to log blob: {e}')


@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent, context: func.Context):
    
    try:
        event_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Capture with milliseconds
        instance_id = context.invocation_id
        logging.info(f'Event Grid Triggered: {event_time}')
        # Log Current instance ID
        logging.info(f"Current Instance ID: {instance_id}")
        blob_data = azeventgrid.get_json()
        blob_url = blob_data.get('url')
        
        logging.info(f'Event data: {blob_data}') 
        logging.info(f'Event url: {blob_url}') 
        
        # Extract the account URL, container name, and blob name from the blob URL
        # account_url = blob_url.split('/')[0] + '//' + blob_url.split('/')[2]
        container_name = blob_url.split('/')[3]
        blob_name = '/'.join(blob_url.split('/')[4:])

        logging.info(f'container name: {container_name}')
        logging.info(f'blob name: {blob_name}')
        
        # Get blob content using managed identity
        blob_data = get_blob_content(blob_url, container_name, blob_name)

        # Parse JSON content
        admin_json_data = json.loads(blob_data)
        
        # Insert data into PostgreSQL
        insert_data_to_postgres(admin_json_data, blob_name, event_time, instance_id)

        # Fetch all data from PostgreSQL and log it to a CSV file in Blob Storage
        records = fetch_data_from_postgres()
        save_fetched_data_to_csv(records)
    except Exception as e:
        logging.error(f'Error processing blob: {e}')







