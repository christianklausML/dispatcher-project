import pandas as pd
from dispatcher.data.data import RawData

class Warehouse:
    def get_warehouse_features(self):
        """
        Returns a DataFrame with:
        'SHIPMENT_ID' and each timestamp for that shipment.
        """
        #warehouse Data
        clean_warehouse = RawData.get_table_data('warehouse', local=True, clean=False)
        clean_warehouse = clean_warehouse[['ID']]
        
        clean_warehouse_additional_info = RawData.get_table_data('warehouse_additional_info', clean=False)
        clean_warehouse_additional_info = clean_warehouse_additional_info.drop(columns=["ORG_ID", "ADDRESS_POSTAL_CODE"])

        # Merging Dataframes
        
        final_warehouse = clean_warehouse.merge(clean_warehouse_additional_info, how='left', left_on='ID', right_on='ID')
        final_warehouse = final_warehouse.drop(final_warehouse[final_warehouse['TIME_TYPE'] == 'Cut-Off Time'].index)
        final_warehouse = final_warehouse.drop(columns="TIME_TYPE")
        final_warehouse['TIME_FROM'] = pd.to_datetime(final_warehouse['TIME_FROM']).dt.time
        final_warehouse['TIME_UNTIL'] = pd.to_datetime(final_warehouse['TIME_UNTIL']).dt.time
        print('warehouse features loaded')
        return final_warehouse

if __name__ == '__main__':
    pass
    #Warehouse.get_warehouse_features(Warehouse)