from sqlalchemy import create_engine
from sqlalchemy import inspect
import tabula
import pandas as pd
import yaml
import psycopg2
import MySQLdb
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import boto3

file = 'db_creds.yaml'
db = DatabaseConnector()
tables = db.list_db_tables(file)
user_data = DataExtractor().read_rds_table('legacy_users', file)
df1 = DataCleaning().clean_user_data(user_data)
table = 'dim_users'
db.upload_to_db(df1, table)

link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
db2 = DatabaseConnector()
link_data = db2.retrieve_pdf_data(link)
df2 = DataCleaning().clean_card_data(link_data)
table2 = 'dim_card_details'
db2.upload_to_db(df2, table2)

header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
store_end = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
np_stores_end = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
dbe = DataExtractor()
num_of_stores = dbe.list_number_of_stores(np_stores_end, header)
df1 = dbe.retrieve_stores_data(np_stores_end, store_end, header)
df3 = DataCleaning().clean_store_data(df1)
table3 = "dim_store_details"
db3 = DatabaseConnector()
db3.upload_to_db(df3, table3)
 
address = 's3://data-handling-public/products.csv'
dbe2 = DataExtractor()
df4 = dbe2.extract_from_s3(address)
df4 = DataCleaning().convert_product_weights(df4)
df4 = DataCleaning().clean_products_data(df4)
print(df4)
table4 = 'dim_products'
# db4 = DatabaseConnector()
# db4.upload_to_db(df4, table4)

db5 = DatabaseConnector()
tables2 = db5.list_db_tables(file)
orders_data = DataExtractor().read_rds_table('orders_table', file)
df5 = DataCleaning().clean_orders_data(orders_data)
table5 = 'orders_table'
db5.upload_to_db(df5, table5)

url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
dbe3 = DataExtractor()
df6 = dbe3.extract_json(url)
table6 = 'dim_date_tiimes'
db6 = DatabaseConnector()
db6.upload_to_db(df6, table6)

conn = psycopg2.connect(
    host = 'localhost',
    user = 'postgres',
    password = 'nazir40297',
    database = 'sales_data')

cur = conn.cursor()

cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'orders_table'")
types = cur.fetchall()
print(types)
cur.execute("SELECT MAX(LENGTH(CAST(card_number as TEXT))) FROM orders_table")
max_command_card_number = cur.fetchone()[0]
cur.execute("SELECT MAX(LENGTH(store_code)) FROM orders_table")
max_command_store_code = cur.fetchone()[0]
cur.execute("SELECT MAX(LENGTH(product_code)) FROM orders_table")
max_command_product_code = cur.fetchone()[0]
alter_command = f"ALTER TABLE orders_table \
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID, \
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID, \
    ALTER COLUMN card_number TYPE VARCHAR({max_command_card_number}), \
    ALTER COLUMN store_code TYPE VARCHAR({max_command_store_code}), \
    ALTER COLUMN product_code TYPE VARCHAR({max_command_product_code})"
cur.execute(alter_command)
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'orders_table'")
types1 = cur.fetchall()
print(types1)
conn.commit()
cur.close()
conn.close()


cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dim_users'")
results = cur.fetchall()
cur.execute("SELECT MAX(LENGTH(country_code)) FROM dim_users")
max_command_country_code = cur.fetchone()[0]
alter_command2 = f"ALTER TABLE dim_users \
    ALTER COLUMN first_name TYPE VARCHAR(255),\
    ALTER COLUMN last_name TYPE VARCHAR(255),\
    ALTER COLUMN date_of_birth TYPE DATE,\
    ALTER COLUMN country_code TYPE VARCHAR({max_command_country_code}),\
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,\
    ALTER COLUMN join_date TYPE DATE"
cur.execute(alter_command2)
conn.commit()
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dim_users'")
results2 = cur.fetchall()
cur.close()
conn.close()
print(results)
print(results2)

cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dim_store_details'")
types2 = cur.fetchall()
cur.execute("\d+ dim_((store_details")
types3 = cur.fetchall()
print(types3)
cur.execute("SELECT MAX(LENGTH(country_code)) FROM dim_store_details")
max_command_country_code2 = cur.fetchone()[0]
cur.execute("SELECT MAX(LENGTH(store_code)) FROM dim_store_details")
max_command_store_code2 = cur.fetchone()[0]
alter_command3 = f"ALTER TABLE dim_store_details\
    ALTER COLUMN longitude TYPE FLOAT,\
    ALTER COLUMN locality TYPE VARCHAR(255),\
    ALTER COLUMN store_code TYPE VARCHAR({max_command_store_code2}),\
    ALTER COLUMN staff_numbers TYPE SMALLINT,\
    ALTER COLUMN opening_date TYPE DATE,\
    ALTER COLUMN store_type TYPE VARCHAR(255), DROP NOT NULL,\
    ALTER COLUMN latitude TYPE FLOAT,\
    ALTER COLUMN country_code TYPE VARCHAR({max_command_country_code2}),\
    ALTER COLUMN continent TYPE VARCHAR(255)"
cur.execute(alter_command3)
conn.commit()
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dim_store_details'")
types3 = cur.fetchall()
cur.close()
conn.close()
print(types2)
print(types3)

