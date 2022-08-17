from codecs import ignore_errors
import pandas as pd
import math
import numpy as np
import datetime
from google.cloud import storage
from dispatcher.params import (BUCKET_NAME,
                               RED_DATA_GC_PATH,CLEAN_DATA_GC_PATH,
                               RED_DATA_LOCAL_PATH,CLEAN_DATA_LOCAL_PATH)

class RawData:
    def get_master_data(local=True):
        """
        This function can be used to acces the raw master data.
        It relates to apipll data of the dispacther project except for shipments,
        orders and tickets for which you find individual functions.
        The function returns a Python dict. Its keys are be 'warehouse',
        'warehouse_additional_info', 'carrier' etc...
        Its values are be pandas.DataFrames loaded from google cloud storage.

        Parameters:
        local (default = True):
            Indicates whether the data should be loaded from the local data
            folder. If False it will be loaded from google cloud.

        clean (default = True):
            Indicates what data should be loaded. Either the raw_data or the
            clean data where certain rows and columns have been removed already.
        """
        files = ['carrier', 'country', 'master_mapping_ticket_type',
                 'master_mapping', 'shop_additional_info', 'shop',
                 'ticket_type', 'warehouse', 'warehouse_additional_info']

        if local:
            path = RED_DATA_LOCAL_PATH
        else:
            path = f'gs://{BUCKET_NAME}/{RED_DATA_GC_PATH}'

        data = {}
        for file in files:
            data[file] = pd.read_csv(f'{path}/{file}.csv')

        return data

    def get_table_data(table, local=True, clean=True):
        """
        This function can be used to access individual data sources.
        The function returns a pandas dataframe.

        Parameters:
        table:
            Inidcates the name of the table to be loaded source.
            Can be one of the following:
                'shipment', 'order', 'ticket',
                'carrier', 'country', 'master_mapping_ticket_type',
                'master_mapping', 'shop_additional_info', 'shop',
                'ticket_type', 'warehouse', 'warehouse_additional_info'

        local (default = True):
            Indicates whether the data should be loaded from the local data
            folder. If False it will be loaded from google cloud.

         clean (default = True):
            Indicates what data should be loaded. Either the raw_data or the
            clean data where certain rows and columns have been removed already.
        """
        if table not in ['shipment', 'order', 'ticket']:
            clean = False
        if local:
            path = CLEAN_DATA_LOCAL_PATH if clean else RED_DATA_LOCAL_PATH
        else:
            if clean:
                path = f'gs://{BUCKET_NAME}/{CLEAN_DATA_GC_PATH}'
            else:
                path = f'gs://{BUCKET_NAME}/{RED_DATA_GC_PATH}'

        data = pd.read_csv(f'{path}/{table}.csv')
        return data

def save_clean_ticket_data(local=True):
    """
    Deletes columns and rows from the ticket table that are not needed to
    reduce the size of the files. The obtained file is saved into the 'clean'
    folder within the raw_data folder.
    """
    raw_ticket = RawData.get_table_data('ticket', local=local, clean=False)
    clean_ticket = raw_ticket[['SHIPMENT_ID','ORDER_ID','TYPE_ID','TIME']]
    clean_ticket = clean_ticket[(clean_ticket['TYPE_ID'].isin(
        [12,  53,  57,  58,  59,  60, 106, 17]))]
    if local:
        path = CLEAN_DATA_LOCAL_PATH
    else:
        path = f'gs://{BUCKET_NAME}/{CLEAN_DATA_GC_PATH}'
    clean_ticket.to_csv(f'{path}/ticket.csv',index=False)
    print('Clean ticket file loaded.')
    return None

def save_clean_order_data(local=True):
    """
    Deletes columns and rows from the ticket table that are not needed to
    reduce the size of the files. The obtained file is saved into the 'clean'
    folder within the raw_data folder.
    """
    raw_order = RawData.get_table_data('order', local=local, clean=False)
    clean_order = raw_order[['ID','SHOP_ORDER_DATE','PROMISED_DELIVERY_DATE',
                             'CREATED_AT']]
    if local:
        path = CLEAN_DATA_LOCAL_PATH
    else:
        path = f'gs://{BUCKET_NAME}/{CLEAN_DATA_GC_PATH}'
    clean_order.to_csv(f'{path}/order.csv',index=False)
    print('Clean order file loaded.')
    return None

def save_clean_shipment_data(local=True):
    """
    Deletes columns and rows from the shipment table that are not needed to
    reduce the size of the files. The obtained file is saved into the 'clean'
    folder within the raw_data folder.
    """
    raw_shipment = RawData.get_table_data('shipment', local=local, clean=False)

    clean_shipment = raw_shipment[['ID', 'CARRIER_ID', 'SHOP_ID', 'CUSTOMER_ADDRESS_COUNTRY_ID',
       'CUSTOMER_ADDRESS_ZIP_CODE', 'RETURN_PARCEL', 'PLANNED_PICKUP_TIMESTAMP',
       'CREATED_AT', 'WAREHOUSE_ID',
       'ORDER_ID', 'SLA_DAYS', 'RELATION_ID', 'RELATION_DISTANCE',
       'ORIGIN_ZIP_CODE', 'ORIGIN_COUNTRY', 'DESTINATION_ZIP_CODE',
       'DESTINATION_COUNTRY']]

    clean_shipment = clean_shipment[(clean_shipment['RETURN_PARCEL'] == 0)]
    clean_shipment = clean_shipment.drop(columns='RETURN_PARCEL')

    if local:
        path = CLEAN_DATA_LOCAL_PATH
    else:
        path = f'gs://{BUCKET_NAME}/{CLEAN_DATA_GC_PATH}'
    clean_shipment.to_csv(f'{path}/shipment.csv',index=False)
    print('Clean shipment file loaded.')
    return None

if __name__ == '__main__':
    pass
    #save_warehouse(local=True)
    #number = len(RawData.get_table_data('shop',local=True,clean=True))
    #print(f'The shop table has a size of: {number} rows.')
