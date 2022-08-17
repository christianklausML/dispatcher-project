from dispatcher.data.ticket import Ticket
from dispatcher.data.order import Order
from dispatcher.data.shipment import Shipment
from dispatcher.data.carrier import Carrier
from dispatcher.data.warehouse import Warehouse
from sklearn.model_selection import train_test_split
from tensorflow.keras import utils
from termcolor import colored
import pandas as pd

class JoinTables:
    '''
    Returns a train test split.
    X_train, X_test, y_train, y_test
    '''
    def __init__(self, my_cols=None):
        if my_cols is None:
            self.my_cols = ['WAREHOUSE_ID', 'RELATION_DISTANCE', 
                            'CARRIER_COMPANY_ID', 'PLANNED_PICKUP_TIMESTAMP', 
                            'CREATED_AT_SHIPMENT', 'DIFF_TRUE', 
                            'SHOP_ID', 'DESTINATION_ZIP_CODE', 
                            'CUSTOMER_ADDRESS_ZIP_CODE', 'CUSTOMER_ADDRESS_COUNTRY_ID', 
                            'Order created']
        else:
            self.my_cols = my_cols

    def join_tables(self):
        '''
        Returns X_train, X_test, y_train, y_test.
        '''
        ticket = Ticket().get_ticket_features().reset_index()
        order = Order().get_order_features()
        carrier = Carrier().get_carrier_features()
        shipment = Shipment().get_shipment_features()
        warehouse = Warehouse().get_warehouse_features()

        carrier_shipment = shipment.merge(carrier, how="left", left_on="CARRIER_ID", right_on="ID")
        carrier_shipment.drop(columns=['ID_y'], inplace=True)
        carrier_shipment.rename(columns={'ID_x':'SHIPMENT_ID'}, inplace=True)
        carrier_shipment_ticket = carrier_shipment.merge(ticket, how = "left", on = "SHIPMENT_ID")
        carrier_shipment_ticket_order = carrier_shipment_ticket.merge(order, how = "left", left_on = "ORDER_ID", right_on = "ID")
        carrier_shipment_ticket_order.drop(columns=['ID'], inplace=True)
        df = carrier_shipment_ticket_order.merge(warehouse, how = "left", left_on = "WAREHOUSE_ID", right_on = "ID")

        print(colored('Tables joined.','green'))
        df = df.drop(columns=['ID'], axis=1)
        df = df.rename({'CREATED_AT_x':'CREATED_AT_SHIPMENT', 'CREATED_AT_y': 'CREATED_AT_ORDER'}, axis='columns')
        df['DIFF_TRUE'] = (df['First hub scan'] - df['CREATED_AT_SHIPMENT']).astype('timedelta64[D]')
        df = df.drop(columns=['First hub scan'], axis=1)
        df = df[df['DIFF_TRUE'].isin([0,1,2,3,4])].copy()
        df = df[self.my_cols]
        df['OC_MIN'] = df['Order created'].dt.minute
        df['OC_HRS'] = df['Order created'].dt.hour
        df['OC_DOW'] = df['Order created'].dt.dayofweek
        df['OC_MONTH'] = df['Order created'].dt.month        
        df['PPU_MIN'] = df['PLANNED_PICKUP_TIMESTAMP'].dt.minute
        df['PPU_HRS'] = df['PLANNED_PICKUP_TIMESTAMP'].dt.hour
        df['PPU_DOW'] = df['PLANNED_PICKUP_TIMESTAMP'].dt.dayofweek
        df['SCR_MIN'] = df['CREATED_AT_SHIPMENT'].dt.minute
        df['SCR_HRS'] = df['CREATED_AT_SHIPMENT'].dt.hour
        df['SCR_DOW'] = df['CREATED_AT_SHIPMENT'].dt.dayofweek
        df['DESTINATION_ZIP_CODE'] = df['DESTINATION_ZIP_CODE'].astype("string")
        df['CUSTOMER_ADDRESS_ZIP_CODE'] = df['CUSTOMER_ADDRESS_ZIP_CODE'].astype("string")
        df.drop(columns=['PLANNED_PICKUP_TIMESTAMP','CREATED_AT_SHIPMENT', 'Order created'], inplace=True)
        df['DESTINATION_ZIP_CODE'] = df['DESTINATION_ZIP_CODE'].fillna(df['DESTINATION_ZIP_CODE'].mode()[0])
        df['RELATION_DISTANCE'] = df['RELATION_DISTANCE'].fillna(df['RELATION_DISTANCE'].mean())
        print(colored('NAs filled. (─‿‿─)', "green"))
        length = df.shape[0]
        print(colored(f'Number of rows: {length}', "green"))
        percent_missing = (df.isnull().sum() * 100 / len(df)).astype(int)
        missing_value_df = pd.DataFrame({'percent_missing': percent_missing})
        missing_value_df.sort_values('percent_missing', ascending=False, inplace=True)
        print(colored(missing_value_df.head(5),'green'))
        df.dropna(inplace=True)
        print(colored(f'{round(((length - df.shape[0]) / length * 100), 1)}% of rows dropped.', "green"))
        X_train, X_test, y_train, y_test = train_test_split(df.drop(columns='DIFF_TRUE'), df['DIFF_TRUE'], test_size=0.2)
        y_train = utils.to_categorical(y=y_train)
        y_test = utils.to_categorical(y=y_test)

        print(colored('train test split ready. ( ◑‿◑)', "green"))

        return X_train, X_test, y_train, y_test
