import pandas as pd
from dispatcher.data.data import RawData

class Ticket:
    def get_ticket_features(self):
        """
        Returns a DataFrame with:
        'SHIPMENT_ID' and each timestamp for that shipment.
        """
        #Ticket Data
        clean_ticket = RawData.get_table_data('ticket', local=True, clean=True)
        #Ticket Data per Shipment
        clean_shipment_ticket = clean_ticket[clean_ticket['ORDER_ID'].isna()]
        clean_shipment_ticket = clean_shipment_ticket[['SHIPMENT_ID','TYPE_ID',
                                                       'TIME']]
        #Ticket Data per Order
        order_ticket = clean_ticket[clean_ticket['SHIPMENT_ID'].isna()]
        order_ticket = order_ticket[['ORDER_ID','TYPE_ID','TIME']]
        #Ticket Type Data
        ticket_type = RawData.get_table_data('ticket_type', local=True,
                                             clean=False)
        ticket_type = ticket_type[['ID','NAME']]
        #Shipments & Orders
        shipment_order = RawData.get_table_data('shipment', local=True,
                                                clean=False)
        shipment_order = shipment_order[['ID','ORDER_ID']]

        # Merging Dataframes
        clean_order_ticket = order_ticket.merge(shipment_order, how='left',
                                                on='ORDER_ID')
        clean_order_ticket = clean_order_ticket[['ID','TYPE_ID','TIME']]
        clean_order_ticket.rename(columns={'ID':'SHIPMENT_ID'}, inplace=True)
        merged_ticket = pd.concat([clean_shipment_ticket, clean_order_ticket])
        merged_ticket['TIME'] = pd.to_datetime(merged_ticket['TIME'])
        merged_ticket = merged_ticket.groupby(['SHIPMENT_ID','TYPE_ID']).agg(
            TIME = pd.NamedAgg('TIME','min'))
        merged_ticket.reset_index(inplace=True)
        merged_ticket['SHIPMENT_ID'] = merged_ticket['SHIPMENT_ID']
        final_ticket = merged_ticket.merge(ticket_type, how='left', left_on='TYPE_ID', right_on='ID')
        final_ticket = final_ticket[['SHIPMENT_ID','NAME','TIME']]
        final_ticket = final_ticket.pivot(columns='NAME', values='TIME', index='SHIPMENT_ID')
        final_ticket = final_ticket[final_ticket['First hub scan'].notna()]
        print('ticket features loaded')
        return final_ticket

if __name__ == '__main__':
    pass
    #Ticket.get_ticket_features(Ticket)
