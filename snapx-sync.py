import mysql.connector
from datetime import datetime
import time
import boto3
import base64
import argparse
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def get_snapx_resume_records(limit):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='yumble',
            database='jumpspire'
        )
        cursor = connection.cursor()

        # Count the total number of distinct records
        count_query = """
        SELECT COUNT(DISTINCT email) FROM snapx_resume sr 
        WHERE sr.email NOT IN (SELECT username FROM User)
        """
        cursor.execute(count_query)
        total_records = cursor.fetchone()[0]
        print(f"Total number of matching records: {total_records}")

        # Select the limited number of distinct records
        select_query = f"""
        SELECT DISTINCT email, createDate, firstName, lastName, location, resumeName, userOrigin, jobTitle FROM snapx_resume sr 
        WHERE (sr.email NOT IN (SELECT email FROM User)) AND (sr.email NOT IN (SELECT username FROM User))
        LIMIT {limit}
        """

        cursor.execute(select_query)
        records = cursor.fetchall()

        insert_user_query = """
        INSERT INTO User (email, creation_date, first_name, last_name, modification_date, password, user_type, username, user_origin, last_login_date, account_expired, account_locked, credentials_expired, account_enabled, password_reseted, resetPassword) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        insert_user_details_query = """
        INSERT INTO user_details (id, resume_id, city, title, searched_title, modifiedSearchTitleDate, alias) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        s3_client = boto3.client('s3')

        for record in records:
            print(f"Processing record for email: {record[0]}")
            email = record[0]
            created_at = record[1] if record[1] else datetime.now()
            first_name = record[2]
            last_name = record[3]
            modification_date = datetime.now()
            password = "7c4a8d09ca3762af61e59520943dc26494f8941b"
            user_type = 1
            username = email
            user_origin = record[6] + " py-sync"
            last_login_date = datetime.now()
            account_expired = 0
            account_locked = 0
            credentials_expired = 0
            account_enabled = 1
            resume_file_name = record[5]
            title = record[7]
            created = datetime.now()
            alias = f"{first_name}-{last_name}-{int(time.time())}"
            city = record[4]
            resetPassword = 0
            password_reseted = 0

            # Fetch the resume content
            cursor.execute("SELECT content FROM Resume WHERE name = %s", (resume_file_name,))
            resume_record = cursor.fetchone()

            if resume_record:
                resume_base64 = resume_record[0]
                resume_content = base64.b64decode(resume_base64)
                file_name = f"applicant-resumes/{resume_file_name}"

                try:
                    cursor.execute(insert_user_query, (email, created_at, first_name, last_name, modification_date, password, user_type, username, user_origin, last_login_date, account_expired, account_locked, credentials_expired, account_enabled, password_reseted, resetPassword))
                    user_id = cursor.lastrowid

                    cursor.execute("SELECT id FROM Resume WHERE name = %s", (resume_file_name,))
                    resume_id = cursor.fetchone()[0]

                    cursor.execute(insert_user_details_query, (user_id, resume_id, city, title, title, created, alias))

                    print(f"Inserted user with ID: {user_id}, resume with ID: {resume_id}")

                except mysql.connector.Error as insert_error:
                    if insert_error.errno == 1062:
                        print(f"Duplicate entry '{email}' for key 'email'. Skipping this record.")
                    else:
                        print(f"Error inserting user: {insert_error}")
            else:
                try:
                    cursor.execute(insert_user_query, (email, created_at, first_name, last_name, modification_date, password, user_type, username, user_origin, last_login_date, account_expired, account_locked, credentials_expired, account_enabled, password_reseted, resetPassword))
                    user_id = cursor.lastrowid
                    cursor.execute(insert_user_details_query, (user_id, None, city, title, title, created, alias))

                except mysql.connector.Error as insert_error:
                    if insert_error.errno == 1062:
                        print(f"Duplicate entry '{email}' for key 'email'. Skipping this record.")
                    else:
                        print(f"Error inserting user: {insert_error}")

        connection.commit()
        print("Records inserted successfully into User and updated in Resume table")

    except mysql.connector.Error as error:
        print(f"Error: {error}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process snapx_resume records')
    parser.add_argument('--limit', type=int, default=1, help='Limit the number of records to fetch')

    args = parser.parse_args()

    get_snapx_resume_records(args.limit)