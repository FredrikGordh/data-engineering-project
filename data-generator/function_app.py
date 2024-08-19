import logging
import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient
import os
from faker import Faker
from datetime import datetime

app = func.FunctionApp()

@app.schedule(schedule=os.environ['SCHEDULE'], arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger_load_data(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    # Read number_of_rows and test_category from environment variables
    try:
        number_of_rows = int(os.environ['NUMBER_OF_ROWS'])
    except (KeyError, ValueError):
        logging.error("NUMBER_OF_ROWS environment variable is missing or invalid. Using default value 1.")
        number_of_rows = 1  # Default value if not set or invalid

    try:
        test_category = os.environ['TEST_CATEGORY']
    except (KeyError, ValueError):
        logging.error("TEST_CATEGORY environment variable is missing. Using default value 'T0'.")
        test_category = 'T0'  # Default value if not set

    # Generate example data
    df = generate_data(number_of_rows)

    # Convert DataFrame to JSONS
    json_data = df.to_json(orient='records')

    # Azure Blob Storage connection string
    connect_str = os.environ['AZURE_STORAGE_CONNECTION_STRING']

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Get timestamp
    timestamp = datetime.now().strftime('%y%m%d-%H:%M:%S.%f')

    # Specify the container name and blob name
    container_name = "input-data"
    blob_name = f"admin_{test_category}_{number_of_rows}_{timestamp}.json"

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    try:
        # Upload the JSON
        blob_client.upload_blob(json_data, overwrite=False)
        logging.info(f"JSON file {blob_name} uploaded to container {container_name}.")
    except Exception as e:
        logging.error(f"Failed to upload JSON file: {e}")
    
def generate_data(number_of_rows):
    fake = Faker('sv_SE')  # Swedish locale
    data = []
    for _ in range(number_of_rows):
        first_name = fake.first_name()
        last_name = fake.last_name()
        row = {
            'business_partner_number_admin': str(fake.unique.random_number(digits=10, fix_len=True)),
            'swedish_social_security_number': str(fake.ssn()),
            'first_name': first_name,
            'last_name': last_name,
            'email': f'{first_name.lower()}.{last_name.lower()}@example.com',
            'phone_number': str(fake.phone_number()),
            'admin_type': 'huvudadministratör',
            'admin_type_code': '01',
            'business_partner_number_organization': str(fake.random_number(digits=10, fix_len=True)),
            'valid_from': fake.date_time_this_century(before_now=True, after_now=False).strftime('%Y-%m-%d %H:%M:%S'),
            'valid_to': fake.date_time_this_century(before_now=False, after_now=True).strftime('%Y-%m-%d %H:%M:%S')
        }
        data.append(row)
    df = pd.DataFrame(data)

    return df