import pandas as pd
from dispatcher.data.data import RawData

class Carrier:
    def get_carrier_features(self):
        """
        Returns a DataFrame with the data from the carrier table.
        """
        clean_carrier = RawData.get_table_data('carrier', local=True, clean=True)
        clean_carrier = clean_carrier[['ID', 'CARRIER_COMPANY_ID',]]
        print('carrier features loaded.')
        return clean_carrier

if __name__ == '__main__':
    pass
    #print(len(Carrier.get_carrier_features(Carrier)))
