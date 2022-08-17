import pandas as pd
from dispatcher.data.data import RawData

class Shipment:
    def get_shipment_features(self):
        """
        Returns a DataFrame with the data from the shipment table.
        """
        #TO DO: make generic function for sin/cos thingy
        clean_shipment = RawData.get_table_data('shipment', local=True, clean=True)
        clean_shipment['CREATED_AT'] = pd.to_datetime(clean_shipment['CREATED_AT'])
        clean_shipment['PLANNED_PICKUP_TIMESTAMP'] = pd.to_datetime(clean_shipment['PLANNED_PICKUP_TIMESTAMP'])
        return clean_shipment

        #clean_shipment['CREATED_HR'] = clean_shipment['CREATED_AT'].dt.hour
        #clean_shipment['CREATED_HR_norm'] = 2 * math.pi * clean_shipment['CREATED_HR'] / clean_shipment['CREATED_HR'].max()
        #clean_shipment["cos_CREATED_HR"] = np.cos(clean_shipment['CREATED_HR_norm'])
        #clean_shipment["sin_CREATED_HR"] = np.sin(clean_shipment['CREATED_HR_norm'])

        #clean_shipment['CREATED_DOW'] = clean_shipment['CREATED_AT'].dt.dayofweek
        #clean_shipment['CREATED_DOW_norm'] = 2 * math.pi * clean_shipment['CREATED_DOW'] / clean_shipment['CREATED_DOW'].max()
        #clean_shipment["cos_CREATED_DOW"] = np.cos(clean_shipment['CREATED_DOW_norm'])
        #clean_shipment["sin_CREATED_DOW"] = np.sin(clean_shipment['CREATED_DOW_norm'])
        #clean_shipment = clean_shipment.drop(columns=['CREATED_DOW_norm', 'CREATED_DOW', 'CREATED_HR_norm', 'CREATED_HR', 'CREATED_AT'])

        #clean_shipment['PICKUP_HR'] = clean_shipment['PLANNED_PICKUP_TIMESTAMP'].dt.hour
        #clean_shipment['PICKUP_HR_norm'] = 2 * math.pi * clean_shipment['PICKUP_HR'] / clean_shipment['PICKUP_HR'].max()
        #clean_shipment["cos_PICKUP_HR"] = np.cos(clean_shipment['PICKUP_HR_norm'])
        #clean_shipment["sin_PICKUP_HR"] = np.sin(clean_shipment['PICKUP_HR_norm'])

        #clean_shipment['PICKUP_DOW'] = clean_shipment['PLANNED_PICKUP_TIMESTAMP'].dt.dayofweek
        #clean_shipment['PICKUP_DOW_norm'] = 2 * math.pi * clean_shipment['PICKUP_DOW'] / clean_shipment['PICKUP_DOW'].max()
        #clean_shipment["cos_PICKUP_DOW"] = np.cos(clean_shipment['PICKUP_DOW_norm'])
        #clean_shipment["sin_PICKUP_DOW"] = np.sin(clean_shipment['PICKUP_DOW_norm'])
        #clean_shipment = clean_shipment.drop(columns=['PICKUP_DOW_norm', 'PICKUP_DOW', 'PICKUP_HR_norm', 'PICKUP_HR', 'PLANNED_PICKUP_TIMESTAMP'])

if __name__ == '__main__':
    pass
    #print(len(Shipment.get_shipment_features(Shipment)))
