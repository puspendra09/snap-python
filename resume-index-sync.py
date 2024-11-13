import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# MySQL Database Configuration
mysql_config = {
    'host': 'localhost',          # Replace with your MySQL host
    'user': 'root',               # Replace with your MySQL username
    'password': 'yumble',         # Replace with your MySQL password
    'database': 'jumpspire'       # Replace with your database name
}

# Elasticsearch Configuration
es_host = "https://164.152.28.147:9200"  # Replace with your Elasticsearch host
es_username = "snapapp_es"                # Replace with your Elasticsearch username
es_password = "rjPXPR5FD153Issw0rd"       # Replace with your Elasticsearch password

# Connect to MySQL Database
try:
    db_connection = mysql.connector.connect(**mysql_config)
    cursor = db_connection.cursor(dictionary=True)
except mysql.connector.Error as err:
    print(f"Error connecting to MySQL: {err}")
    exit(1)

# Connect to Elasticsearch with authentication
try:
    es = Elasticsearch(
        [es_host],
        http_auth=(es_username, es_password),
        verify_certs=False  # Set to True in production if using valid SSL certificates
    )

    # Test the connection
    if not es.ping():
        print("Connection to Elasticsearch failed!")
        exit(1)
    else:
        print("Connected to Elasticsearch!")

except Exception as e:
    print(f"Error connecting to Elasticsearch: {e}")
    exit(1)

# Function to fetch and upload data in batches
def upload_data_in_batches(batch_size=100):
    offset = 0  # Initialize offset for pagination

    while True:
        # Fetch records from MySQL in batches
        cursor.execute(f"SELECT id, email, s3, created, name, text FROM resume LIMIT {batch_size} OFFSET {offset}")
        data = cursor.fetchall()

        if not data:  # Exit loop if no more records are returned
            break

        # Prepare documents for bulk upload
        index_name = "resume_new"
        actions = [
            {
                "_index": index_name,
                "_id": str(record['id']),  # Use a unique ID if available
                "_source": record
            }
            for record in data
        ]

        # Bulk upload documents to Elasticsearch
        try:
            success, failed = bulk(es, actions)
            print(f"Successfully indexed {success} documents.")
            if failed:
                print(f"Failed to index {len(failed)} documents.")
        except Exception as e:
            print(f"Error during bulk upload: {e}")

        offset += batch_size  # Increment the offset for the next batch

# Call the function to start uploading data in batches
upload_data_in_batches()

# Close connections in a finally block to ensure they are closed properly
try:
    cursor.close()
except NameError:
    pass  # Cursor might not have been created if connection failed

try:
    db_connection.close()
except NameError:
    pass  # Connection might not have been created if connection failed