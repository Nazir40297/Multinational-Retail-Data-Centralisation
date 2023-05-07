import pandas as pd
from database_utils import DatabaseConnector as DC
import requests
import boto3

class DataExtractor:
    def read_rds_table(self, table_name, file, instance = DC()):
        list_of_tables = instance.list_db_tables(file)
        print(list_of_tables)
        user_data = pd.read_sql_table(table_name, instance.init_db_engine(file))
        return user_data
    
    def list_number_of_stores(self, no_stores_end, header):
        r = requests.get(no_stores_end, headers = header)
        response = r.json()
        num_stores = response['number_stores']
        return num_stores
    
    def retrieve_stores_data(self, no_stores_end, store_end, header):
        store_numbers = []
        num_stores = self.list_number_of_stores(no_stores_end, header)
        for i in range(0,num_stores):
            store_numbers.append(i)

        store_details = []
        for x in store_numbers:
            url = f"{store_end}/{x}"
            p = requests.get(url, headers=header)

            if p.status_code==200:
                store_detail = p.json()
                store_detail['store_number']= x
                store_details.append(store_detail)
            else:
                print("Failed to retrieve store details for store {}. Status code:{}".format(x, p.status_code))

        df = pd.DataFrame(store_details)
        return df

    def extract_from_s3(self, address):
        s32 = address.split(':')[0]
        bucket_name = address.split('/')[2]
        file_name = address.split('/')[-1]
        s3 = boto3.client(s32)
        s3.download_file(bucket_name, file_name, 'Product Details')
        df = pd.read_csv('Product Details', index_col=0)
        return df

    def extract_json(self, url):
        df = pd.read_json(url)
        return df 

