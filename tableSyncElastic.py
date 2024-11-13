import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# MySQL database connection
db_config = {
    'user': 'root',
    'password': 'yumble',
    'host': 'localhost',
    'database': 'jumpspire'
}

# Elasticsearch configuration with credentials
es = Elasticsearch(
    ['https://snapapp_es:rjPXPR5FD153Issw0rd@164.152.28.147:9200'],
    verify_certs=False,  # Disable SSL certificate verification
    ssl_show_warn=False   # Disable SSL warnings
)
index_name = 'job_title'

# Function to fetch job titles from MySQL database
def get_job_titles():
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM job_title")
    job_titles = cursor.fetchall()
    cursor.close()
    connection.close()
    return job_titles

# Function to prepare documents for Elasticsearch
def prepare_documents(job_titles):
    actions = []
    for job in job_titles:
        action = {
            "_index": index_name,
            "_id": job['id'],  # assuming `id` is the primary key in the table
            "_source": job
        }
        actions.append(action)
    return actions

# Function to insert data into Elasticsearch
def insert_into_elasticsearch(documents):
    success, _ = bulk(es, documents)
    print(f'Successfully indexed {success} documents.')

# Main function
def main():
    job_titles = get_job_titles()
    documents = prepare_documents(job_titles)
    insert_into_elasticsearch(documents)

if __name__ == '__main__':
    main()
