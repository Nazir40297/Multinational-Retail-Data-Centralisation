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
file2 = 'db_creds2.yaml'
db = DatabaseConnector()
tables = db.list_db_tables(file)
user_data = DataExtractor().read_rds_table('legacy_users', file)
df1 = DataCleaning().clean_user_data(user_data)
table = 'dim_users'
db.upload_to_db(file2, df1, table)

link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
db2 = DatabaseConnector()
link_data = db2.retrieve_pdf_data(link)
df2 = DataCleaning().clean_card_data(link_data)
table2 = 'dim_card_details'
db2.upload_to_db(file2, df2, table2)

header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
store_end = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
np_stores_end = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
dbe = DataExtractor()
num_of_stores = dbe.list_number_of_stores(np_stores_end, header)
df1 = dbe.retrieve_stores_data(np_stores_end, store_end, header)
df3 = DataCleaning().clean_store_data(df1)
table3 = "dim_store_details"
db3 = DatabaseConnector()
db3.upload_to_db(file2, df3, table3)
 
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
db5.upload_to_db(file2, df5, table5)

url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
dbe3 = DataExtractor()
df6 = dbe3.extract_json(url)
table6 = 'dim_date_tiimes'
db6 = DatabaseConnector()
db6.upload_to_db(file2, df6, table6)


