import pandas as pd
from sqlalchemy import create_engine, text
import os
import boto3
import shutil
import argparse
from PyPDF2 import PdfReader
from docx import Document
import subprocess
import urllib.parse
from elasticsearch import Elasticsearch, ConnectionError
import urllib3

# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Argument parsing
parser = argparse.ArgumentParser(description='Process resumes from MySQL and upload to AWS S3.')
parser.add_argument('--id_start', type=int, required=True, help='Start ID for filtering resumes')
parser.add_argument('--id_end', type=int, required=True, help='End ID for filtering resumes')
args = parser.parse_args()
id_start = args.id_start
id_end = args.id_end

# Configuration and database connection
d100 = 'mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.format("root", "yumble", "localhost", "3306", "jumpspire")
engine = create_engine(d100)
print('Data loading started from DB')

# Load resumes from MySQL using pandas with ID range filter (with email)
query = f"""
SELECT CAST(id AS CHAR) AS R_id, content, name, email, s3 
FROM resume 
WHERE content IS NOT NULL AND name IS NOT NULL AND email IS NOT NULL
AND id BETWEEN {id_start} AND {id_end}
ORDER BY id DESC
"""

resume_df = pd.read_sql(query, engine)
print('Data loaded into Python')

# Establish AWS S3 connection
s3_client = boto3.client('s3')
bucket_name = 'snaprecruit-assets-01'
folder_name = 'applicant-resumes'
print('AWS S3 connection established successfully')

# Create temporary folder to process files from DB
current_directory = os.getcwd()
final_directory = os.path.join(current_directory, 'Resumes/')
if not os.path.exists(final_directory):
    os.makedirs(final_directory)

def write_file(data, filename):
    filename = filename.replace(" ", "-").replace("/", "-")
    file_path = os.path.join(final_directory, filename)
    
    with open(file_path, 'wb') as file:
        file.write(data)
    
    # Upload to S3
    s3_client.upload_file(file_path, bucket_name, f'{folder_name}/{filename}', ExtraArgs={'ACL': 'public-read'})
    
    # Properly encode the filename for the URL
    encoded_filename = urllib.parse.quote(filename)
    return f"https://{bucket_name}.s3.amazonaws.com/{folder_name}/{encoded_filename}"

def extract_text_from_pdf(file_path):
    with open(file_path, 'rb') as file:
        reader = PdfReader(file)
        text = [page.extract_text() for page in reader.pages]
    return '\n'.join(text)

def extract_text_from_docx(file_path):
    doc = Document(file_path)
    text = [para.text for para in doc.paragraphs]
    return '\n'.join(text)

def convert_doc_to_docx(doc_path):
    docx_path = doc_path.replace('.doc', '.docx')
    try:
        subprocess.run(['libreoffice', '--headless', '--convert-to', 'docx', doc_path, '--outdir', os.path.dirname(doc_path)], check=True)
        if os.path.exists(docx_path):
            print(f"Conversion successful: {docx_path}")
        else:
            print(f"Conversion failed: {docx_path} does not exist.")
    except subprocess.CalledProcessError as e:
        print(f"Error during conversion: {e}")
    return docx_path

def extract_text(file_path):
    _, extension = os.path.splitext(file_path)
    try:
        if extension.lower() == '.pdf':
            return extract_text_from_pdf(file_path)
        elif extension.lower() == '.docx':
            return extract_text_from_docx(file_path)
        elif extension.lower() == '.doc':
            docx_path = convert_doc_to_docx(file_path)
            if os.path.exists(docx_path) and os.path.getsize(docx_path) > 0:
                return extract_text_from_docx(docx_path)
            else:
                print(f"Failed to convert DOC to DOCX: {docx_path}")
                return "Conversion failed."
        else:
            print(f"Unsupported file type: {extension}")
            return "Unsupported file type."
    except Exception as e:
        print(f"Error extracting text: {e}")
        return "Text extraction failed."

# Connect to Elasticsearch with enhanced error handling
try:
    es = Elasticsearch(
        ["https://164.152.28.147:9200"],  # Replace with your ES host if different
        http_auth=('snapapp_es', 'rjPXPR5FD153Issw0rd'),
        verify_certs=False,
        timeout=30  # Increase timeout to 30 seconds
    )
except ConnectionError as e:
    print(f"Elasticsearch connection error: {e}")
    exit(1)

print('Applying filter on s3 and content')
resume_filtered = resume_df[(resume_df['content'].notnull()) & (resume_df['name'].notnull())]

if len(resume_filtered) > 0:
    for _, row in resume_filtered.iterrows():
        file_path = write_file(row['content'], f"{row['R_id']}_{row['name']}")
        
        resume_text = extract_text(file_path)
        print(f'Resume ID {row["R_id"]} uploaded to AWS S3 Bucket')

        # Construct the bucket URL
        resume_url = file_path  
        print('Bucket URL:', resume_url)

        print('Started updating the Resume table in MySQL')

        try:
            # Save processed resume into a temporary DataFrame (including email)
            temp_df = pd.DataFrame({
                'R_id': [int(row['R_id'])],
                'Resume_url': [resume_url],
                'Resume_text': [resume_text],
                'Email': [row['email']]
            })
            temp_df.to_sql('temp_table', engine, if_exists='replace', index=False)

            # Update original resume table with S3 Bucket link, resume text, and email
            update_sql = """
            UPDATE resume AS f
            INNER JOIN temp_table AS t ON f.id = t.R_id
            SET f.s3 = t.Resume_url, f.text = t.Resume_text, f.email = t.Email 
            WHERE f.id = t.R_id
            """
            
            with engine.begin() as conn:
                result = conn.execute(text(update_sql))
                print(f"Rows affected by update: {result.rowcount}")
                conn.execute(text('DROP TABLE temp_table'))

                # Update S3 URL in Elasticsearch index based on email match.
                try:
                    existing_doc = es.get(index='resume', id=row['email'])
                    print(f'Existing document: {existing_doc}')

                    # Compare existing values with new values
                    needs_update = False
                    if (existing_doc['_source']['s3'] != resume_url or 
                        existing_doc['_source']['Resume_text'] != resume_text):
                        needs_update = True

                    # Debugging output for comparison
                    print(f'New S3 URL: {resume_url}, Existing S3 URL: {existing_doc["_source"]["s3"]}')
                    print(f'New Resume Text: {resume_text}, Existing Resume Text: {existing_doc["_source"]["Resume_text"]}')

                    if needs_update:
                        response = es.update(
                            index='resume',
                            id=row['email'],  # Assuming email is used as a unique identifier
                            body={
                                "doc": {
                                    's3': "",
                                    'Resume_url': "resume_url",
                                },
                                "doc_as_upsert": True  
                            }
                        )
                        print(f'Updated Elasticsearch index for email: {row["email"]}')
                        print(f'Elasticsearch response: {response}')  # Print the response for debugging
                    else:
                        print(f'No changes detected for email: {row["email"]}, skipping update.')

                except Exception as e:
                    print(f"Error updating Elasticsearch for email {row['email']}: {e}")

        except Exception as e:
            print(f"Error updating Resume table for ID {row['R_id']}: {e}")

# Remove the Resumes directory after processing
shutil.rmtree(final_directory)
print('Resumes directory removed')