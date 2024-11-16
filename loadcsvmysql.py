import pandas as pd
import mysql.connector

# Read the CSV file
csv_file = '/home/ubuntu/home/blocledEmail.csv'
data = pd.read_csv(csv_file)

# Connect to the database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="yumble",
    database="jumpspire"
)
cursor = conn.cursor()

# Insert data row by row
for index, row in data.iterrows():
    cursor.execute(
        "INSERT INTO mail_subscribe (email, createdAt, source) VALUES (%s, %s, %s)",
        (row['email'], row['createdAt'], row['source'])
    )

# Commit and close
conn.commit()
cursor.close()
conn.close()
