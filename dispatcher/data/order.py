import pandas as pd
from dispatcher.data.data import RawData

class Order:
    def get_order_features(self):
        """
        Returns a DataFrame with the data from the order table.
        """
        clean_order = RawData.get_table_data('order', local=True, clean=True)
        clean_order['SHOP_ORDER_DATE'] = pd.to_datetime(clean_order['SHOP_ORDER_DATE'])
        clean_order['PROMISED_DELIVERY_DATE'] = pd.to_datetime(clean_order['PROMISED_DELIVERY_DATE'])
        clean_order['CREATED_AT'] = pd.to_datetime(clean_order['CREATED_AT'])
        clean_order['DIFF_CREATED_PROMISE'] = clean_order['PROMISED_DELIVERY_DATE'] - clean_order['SHOP_ORDER_DATE']
        clean_order['DIFF_CREATED_PROMISE'] = clean_order['DIFF_CREATED_PROMISE'].astype('timedelta64[h]')
        print('order features loaded.')
        return clean_order

if __name__ == '__main__':
    pass
    #print(len(Order.get_order_features(Order)))
