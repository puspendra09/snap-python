import mysql.connector
import base64
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Database connection details
db_config = {
    'user': 'root',
    'password': 'yumble',
    'host': 'localhost',
    'database': 'jumpspire',
}

# Directory to save the files temporarily
save_directory = '/home/ubuntu/home/python/resume'


# AWS S3 configuration
s3_bucket_name = 'snaprecruit-assets-01'
aws_access_key = 'AKIAI5B6AYHN2LFAU6WQ'
aws_secret_key = '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'
aws_region = 'us-east-1'
s3_folder_name = 'applicant-resumes'  # Folder name in S3 bucket

# Function to convert base64 string to file
def save_base64_to_file(file_path, base64_string):
    with open(file_path, 'wb') as file:
        file.write(base64.b64decode(base64_string))

# Function to upload file to S3 and make it publicly accessible
def upload_to_s3(file_path, file_name):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    try:
        # Include the folder name in the file name
        s3_file_name = f"{s3_folder_name}/{file_name}"
        s3_client.upload_file(file_path, s3_bucket_name, s3_file_name, ExtraArgs={'ACL': 'public-read'})
        s3_url = f"https://{s3_bucket_name}.s3.{aws_region}.amazonaws.com/{s3_file_name}"
        return s3_url
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        return None
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None

def main():
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Query to select the base64 string and file name
        query = """
            SELECT id, resumeData, resumeFileName 
            FROM pjf_history pjf 
            WHERE pjf.email NOT IN (SELECT email FROM User) AND pjf.s3URL IS NULL AND pjf.resumeData IS NOT NULL
        """
        cursor.execute(query)

        # Fetch all the results
        results = cursor.fetchall()
        if results:
            # Ensure the save directory exists
            if not os.path.exists(save_directory):
                os.makedirs(save_directory)

            for result in results:
                print(result[0])
                resume_id = result[0]
                base64_string = result[1]
                file_name = result[2]

                # Create the full file path
                file_path = os.path.join(save_directory, file_name)

                # Convert base64 string to file
                save_base64_to_file(file_path, base64_string)
                print(f"File saved as {file_path}")

                # Upload file to S3 and get the URL
                s3_url = upload_to_s3(file_path, file_name)
                if s3_url:
                    print(f"File uploaded to S3: {s3_url}")

                    # Update the database with the S3 URL
                    update_query = "UPDATE pjf_history SET s3URL = %s WHERE id = %s"
                    cursor.execute(update_query, (s3_url, resume_id))
                    connection.commit()
                    print(f"S3 URL updated in database for ID: {resume_id}")

        else:
            print("No resume data found for the given condition.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    main()
