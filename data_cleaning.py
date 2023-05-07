import pandas as pd

class DataCleaning:
    def clean_user_data(self, df):
        # Setting index to the first column
        df.set_index('index', inplace=True)
        # Converting to the same datetime
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed', errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed', errors ='coerce')
        # Removing all NaT values
        df = df.dropna(subset=['date_of_birth'])
        # Changing country codes 'GGB' to 'GB'
        # df['country_code'] = df['country_code'].replace('GGB', 'GB')
        # df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        # Sorting by index
        df = df.sort_values('index')
        return df
    
    def clean_card_data(self, dfs):
        # Setting 'expiry date' and 'date payment confirmed' to datetime columns
        # dfs['expiry_date'] = pd.to_datetime(dfs['expiry_date'], format= '%m/%y', errors='coerce')
        dfs['date_payment_confirmed'] = pd.to_datetime(dfs['date_payment_confirmed'], errors = 'coerce')
        # Removing null values
        dfs = dfs.dropna(subset=['date_payment_confirmed'])
        # dfs = dfs.dropna(subset=['expiry_date'])
        # Changing 'card number' column to numeric
        # dfs['card_number'] = pd.to_numeric(dfs['card_number'], errors="coerce")
        # # Removing null values
        # dfs = dfs.dropna(subset=['card_number'])
        # # Setting data type to integer
        # dfs['card_number'] = dfs['card_number'].astype('int')
        # # Setting data type to string
        # dfs['card_provider'] = dfs['card_provider'].astype('string')
        return dfs
    
    def clean_store_data(self, df):
        df = df.drop("lat", axis = 1)
        df.set_index('index', inplace=True)
        df = df.dropna()
        df['latitude'] = pd.to_numeric(df['latitude'], errors = 'coerce')
        df = df.dropna(subset=['latitude'])
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'
        df[['continent', 'country_code','store_type','locality']] = df[['continent', 'country_code', 'store_type','locality']].astype('category')
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors= 'coerce')
        df = df.dropna(subset=['opening_date'])
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df = df.dropna(subset=['staff_numbers'])
        df['longitude'] = pd.to_numeric(df['longitude'], errors= 'coerce')
        df[['address', 'store_code']] = df[['address', 'store_code']].astype('string')
        return df

    def convert_product_weights(self, df):
        df = df.dropna(subset=['weight'])
        df['weight_kg'] = df['weight'].str.replace(r'[^0-9\.]+?', '',regex=True).astype(float)
        g_mask = df['weight'].str.contains(r'^\d+g$')
        df.loc[g_mask, 'weight_kg'] = df[g_mask]['weight_kg']/1000
        ml_mask = df['weight'].str.contains(r'^\d+ml$')
        df.loc[ml_mask, 'weight_kg'] = df[ml_mask]['weight_kg']/1000
        df['weight'] = df['weight_kg']
        df = df.drop('weight_kg', axis=1)
        return df
    
    def clean_products_data(self, df):
        df['EAN'] = pd.to_numeric(df['EAN'], errors='coerce')
        df = df.dropna(subset='EAN')
        df['EAN'] = df['EAN'].astype('int')
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        df = df.dropna(subset='date_added')
        df[['removed', 'category']] = df[['removed', 'category']].astype('category')
        df['product_price'] = df['product_price'].str.strip('£')
        df['product_price'] = df['product_price'].astype('float')
        df[['product_name', 'uuid', 'product_code']] = df[['product_name', 'uuid', 'product_code']].astype('string')
        return df
    
    def clean_orders_data(self, df):
        df = df.drop(['level_0', 'first_name', 'last_name', '1'], axis = 1)
        df.set_index('index', inplace = True)
        return df  
