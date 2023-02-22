import mysql.connector
import pandas as pd

# Creating connection object
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Kms@1405",
    database="public"
)

cursor = mydb.cursor()

cursor.execute(
    "CREATE TABLE IF NOT EXISTS test_od (year INT, age INT, ethinic INT,sex INT,area INT, count VARCHAR(255))")

filename = "./sample.csv"

chunksize = 1000 * 5
with pd.read_csv(filename, chunksize=chunksize) as reader:
    for chunk in reader:
        query = 'INSERT INTO test_od(year,age,ethinic,sex,area,count) VALUES( %s, %s, %s, %s, %s, %s)'
        my_data = chunk.values.tolist()
        cursor.executemany(query, my_data)
        mydb.commit()

    mydb.close()
