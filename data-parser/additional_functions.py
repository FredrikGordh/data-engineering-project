import os
import requests
import json
import psycopg2
import logging

import azure.functions as func


BLOB_URL = 'https://blobstorageaccount987.blob.core.windows.net/input-data'

def parse_blob():
        try:
            # Download blob content using requests
            response = requests.get(BLOB_URL)
            response.raise_for_status()
            blob_data = response.content.decode('utf-8')
            
            logging.info(f'Blob content: {blob_data}')
            logging.error(f'Inside try statement')
            # Parse JSON content
            json_data = json.loads(blob_data)
            
            # Insert data into PostgreSQL
            insert_data_to_postgres(json_data)
        except Exception as e:
            logging.error(f'Error processing blob: {e}')

def insert_data_to_postgres(data):
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
        # Insert data into PostgreSQL
        insert_query = """
        INSERT INTO fora_table (business_partner_number_admin, first_name, last_name, email, phone_number, admin_type, admin_type_code, business_partner_number_organization, ingestion_ts)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            data['business_partner_number_admin'],
            data['first_name'],
            data['last_name'],
            data['email'],
            data['phone_number'],
            data['business_partner_number'],
            data['admin_type'],
            data['admin_type_code'],
            data['business_partner_number_organization'],
            data['ingestion_ts']
        ))
        
        # Commit the transaction
        connection.commit()
        # Close the connection
        cursor.close()
        connection.close()
        logging.info('Data inserted into PostgreSQL')
    except Exception as e:
	    logging.error(f'Error inserting data into PostgreSQL: {e}')