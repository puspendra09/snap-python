import mysql.connector
from datetime import datetime
import time
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import argparse

def get_pjf_history_records(limit):
    try:
        # Establish the database connection
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='yumble',
            database='jumpspire'
        )

        cursor = connection.cursor()

         # Count the total number of distinct records
        count_query = """
        SELECT COUNT(DISTINCT email) FROM Pjf_history pjf 
        WHERE pjf.email NOT IN (SELECT email FROM User)
        AND pjf.email NOT IN (SELECT username FROM User) 
        AND pjf.s3URL IS NOT NULL
        """
        cursor.execute(count_query)
        total_records = cursor.fetchone()[0]
        print(f"Total number of matching records: {total_records}")


        # Define the query to fetch distinct email records with dynamic limit
        select_query = f"""
        SELECT email, MIN(createdAt) AS createdAt, MIN(candidateFullName) AS candidateFullName, 
           MIN(ip) AS ip, MIN(partner) AS partner, MIN(resumeFileName) AS resumeFileName, 
           MIN(s3URL) AS s3URL, MIN(city) AS city, MIN(state) AS state, MIN(country) AS country, 
           MIN(title) AS title
        FROM Pjf_history pjf 
        WHERE pjf.email NOT IN (SELECT email FROM User) 
        AND pjf.email NOT IN (SELECT username FROM User) 
        AND pjf.s3URL IS NOT NULL 
        GROUP BY pjf.email
        LIMIT {limit}
        """

        # Execute the select query
        cursor.execute(select_query)

        # Fetch the records
        records = cursor.fetchall()

        # Define the insert queries for User, Resume, and UserDetails tables
        insert_user_query = """
        INSERT INTO User (email, creation_date, first_name, last_name, modification_date, password, user_type, username, last_ip_address, user_origin, last_login_date, account_expired, account_locked, credentials_expired, account_enabled, password_reseted, resetPassword) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        insert_resume_query = """
        INSERT INTO Resume (name, text, s3, email, created) 
        VALUES (%s, %s, %s, %s, %s)
        """

        insert_user_details_query = """
        INSERT INTO user_details (id, resume_id, city, title, searched_title, modifiedSearchTitleDate, alias) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        s3_client = boto3.client('s3')

        def download_s3_file(s3_url):
            try:
                bucket_name = s3_url.split('/')[2]  # Extract bucket name from URL
                key = '/'.join(s3_url.split('/')[3:])  # Extract key from URL
                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                return response['Body'].read().decode('utf-8')
            except (NoCredentialsError, PartialCredentialsError) as e:
                print(f"AWS credentials error: {e}")
                return ''
            except Exception as e:
                print(f"Error downloading S3 file: {e}")
                return ''

        # Insert the records into the User, Resume, and UserDetails tables
        for record in records:
            email = record[0]
            created_at = record[1] if record[1] else datetime.now()  # Use creation_date or current time
            full_name = record[2]
            first_name, last_name = (full_name.split()[0], full_name.split()[-1]) if full_name else ('', '')
            modification_date = datetime.now()
            password = "7c4a8d09ca3762af61e59520943dc26494f8941b"  # Example hashed password
            user_type = 1
            username = email
            ip_address = record[3]
            user_origin = record[4] + " sync"
            last_login_date = datetime.now()
            account_expired = 0
            account_locked = 0
            credentials_expired = 0
            account_enabled = 1
            resume_file_name = record[5]
            s3_url = record[6]
            city = f"{record[7]}, {record[8]}, {record[9]}"
            title = record[10]
            created = datetime.now()
            alias = f"{first_name}-{last_name}-{int(time.time())}"
            password_reseted = 0
            resetPassword = 0

            # Download and read the content of the resume file from S3
            resume_text = ""
            try:
                # Insert into User table
                cursor.execute(insert_user_query, (email, created_at, first_name, last_name, modification_date, password, user_type, username, ip_address, user_origin, last_login_date, account_expired, account_locked, credentials_expired, account_enabled, password_reseted, resetPassword))
                user_id = cursor.lastrowid

                # Insert into Resume table
                cursor.execute(insert_resume_query, (resume_file_name, resume_text, s3_url, email, created))
                resume_id = cursor.lastrowid

                # Insert into UserDetails table
                cursor.execute(insert_user_details_query, (user_id, resume_id, city, title, title, created, alias))

                print(f"Inserted user with ID: {user_id}, resume with ID: {resume_id}")

            except mysql.connector.Error as insert_error:
                if insert_error.errno == 1062:
                    print(f"Duplicate entry '{email}' for key 'email'. Skipping this record.")
                else:
                    raise insert_error

        # Commit the transaction
        connection.commit()
        print("Records inserted successfully into User, Resume, and UserDetails tables")

    except mysql.connector.Error as error:
        print(f"Error: {error}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Set up argument parser
parser = argparse.ArgumentParser(description='Process Pjf_history records')
parser.add_argument('--limit', type=int, default=1, help='Limit the number of records to fetch')

# Parse arguments
args = parser.parse_args()

# Call the function to get records and insert them
get_pjf_history_records(args.limit)