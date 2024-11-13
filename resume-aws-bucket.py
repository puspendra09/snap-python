import pandas as pd
from sqlalchemy import create_engine
import os
import boto3
import shutil
import argparse
from PyPDF2 import PdfReader
from docx import Document
import subprocess

# Argument parsing
parser = argparse.ArgumentParser(description='Process resumes from MySQL and upload to AWS S3.')
parser.add_argument('--limit', type=int, required=True, help='Limit the number of resumes to process')
args = parser.parse_args()
limit = args.limit

# Configuration and database connection
d100 = 'mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.format("root", "yumble", "localhost", "3306", "jumpspire")
engine = create_engine(d100)
print('Data loading started from DB')

# Load resumes from MySQL using pandas
query = f"""
SELECT CAST(id AS CHAR) AS R_id, content, name, s3
FROM resume WHERE s3 IS NULL AND content IS NOT NULL AND name is not null order by id desc
LIMIT {limit}
"""
resume_df = pd.read_sql(query, engine)
print('Data loaded into Python')

# Log the resume IDs being loaded
print(f'Resume IDs being loaded: {resume_df["R_id"].tolist()}')

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
    s3_client.upload_file(file_path, bucket_name, f'{folder_name}/{filename}', ExtraArgs={'ACL': 'public-read'})
    return file_path

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

print('Applying filter on s3 and content')
resume_filtered = resume_df[(resume_df['content'].notnull()) & (resume_df['name'].notnull())]

if len(resume_filtered) > 0:
    for _, row in resume_filtered.iterrows():
        file_path = write_file(row['content'], f"{row['R_id']}_{row['name']}")
        
        resume_text = extract_text(file_path)
        print(f'Resume ID {row["R_id"]} uploaded to AWS S3 Bucket')

        # Construct the bucket URL
        resume_url = f"https://{bucket_name}.s3.amazonaws.com/{folder_name}/{row['R_id']}_{row['name']}"
        print('Bucket URL:', resume_url)

        print('Started updating the Resume table in MySQL')

        try:
            # Save processed resume into a temporary DataFrame
            temp_df = pd.DataFrame({
                'R_id': [int(row['R_id'])],
                'Resume_url': [resume_url],
                'Resume_text': [resume_text]
            })
            temp_df.to_sql('temp_table', engine, if_exists='replace', index=False)

            # Update original resume table with S3 Bucket link and resume text
            update_sql = """
            UPDATE resume AS f
            INNER JOIN temp_table AS t ON f.id = t.R_id
            SET f.s3 = t.Resume_url, f.text = t.Resume_text
            """
            with engine.begin() as conn:
                conn.execute(update_sql)
                conn.execute('DROP TABLE temp_table')
        except Exception as e:
            print("")
        print(f'AWS S3 Bucket link and resume text updated in Resume table for ID {row["R_id"]}')
else:
    print('No resumes to process')

# Remove the Resumes directory after processing
shutil.rmtree(final_directory)
print('Resumes directory removed')
