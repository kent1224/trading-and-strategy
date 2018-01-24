# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 08:10:49 2017

@author: 14224
"""

import pandas as pd
import numpy as np
import json

import oandapyV20
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.instruments as instruments
import oandapyV20.endpoints.trades as trades
import oandapyV20.endpoints.positions as positions
import oandapyV20.endpoints.orders as orders


access_token="284e07e015a8b1ffe388a9990ef267da-e12a1d6816d3164b6fccc29fc120c6b2"
accountID = "101-011-3037533-001"
client = oandapyV20.API(access_token=access_token)


"""
Strategy 1
"""
str1_product_list = {'EUR_USD':'0','USD_JPY':'0','GBP_USD':'0',
                     'XAU_USD':'0','BCO_USD':'0',
                     'USB02Y_USD':'0','USB30Y_USD':'0',
                     'SOYBN_USD':'0','CORN_USD':'0',
                     'JP225_USD':'0','US30_USD':'0','AU200_AUD':'0','CN50_USD':'0'}#
#print(len(product_list))

r_ad = accounts.AccountDetails(accountID)
client.request(r_ad)

while True:
    #product, ptime = product_list.pop()
    #print(product, ptime)
    for product in str1_product_list:
        #print(product)            
        params = {'count': 2, 'granularity': 'M15'}
        r_ic = instruments.InstrumentsCandles(instrument = product, params = params)
        client.request(r_ic)
        #print(r_ic.response['candles'][1]['time'][:16])
        if (float(r_ic.response['candles'][1]['mid']['l']) < float(r_ic.response['candles'][0]['mid']['l'])) & (float(r_ic.response['candles'][1]['mid']['c']) < float(r_ic.response['candles'][0]['mid']['c'])):        
            if r_ic.response['candles'][1]['time'][:16] != str1_product_list[product]:
                if float(r_ad.response['account']['marginAvailable']) >= 50000:
                    #print('aa')
                    #order_price = str(1)#float(r_ic.response['candles'][1]['mid']['c'])*2
                    data = {"order": {"instrument": product,"units": "1","type": "MARKET","positionFill": "DEFAULT"}}#"price": order_price,"timeInForce": "GTC",
                    r_oc = orders.OrderCreate(accountID, data=data)
                    client.request(r_oc)
                    print(r_ic.response['instrument'])
                    print(r_ic.response['candles'][1]['time'][:16])
                    print(str1_product_list[product])

                    #print (r_oc.response)
                    str1_product_list[product] = r_ic.response['candles'][1]['time'][:16]
                if float(r_ad.response['account']['marginAvailable']) < 50000:
                    r_op = positions.PositionDetails(accountID, instrument = product)#
                    client.request(r_op)
                    
                    if float(r_op.response['position']['short']['units']) > 0:
                        r_pc = positions.PositionClose(accountID, instrument = product, data = {'shortUnits':'1'})
                        client.request(r_pc)
                        
                        str1_product_list[product] = r_ic.response['candles'][1]['time'][:16]
                        print(r_pc.response)
        if (float(r_ic.response['candles'][1]['mid']['h']) > float(r_ic.response['candles'][0]['mid']['h'])) & (float(r_ic.response['candles'][1]['mid']['c']) > float(r_ic.response['candles'][0]['mid']['c'])):                
            if r_ic.response['candles'][1]['time'][:16] != str1_product_list[product]:
                if float(r_ad.response['account']['marginAvailable']) >= 50000:
                    #print('aa')
                    #order_price = str(1)#float(r_ic.response['candles'][1]['mid']['c'])*2
                    data = {"order": {"instrument": product,"units": "-1","type": "MARKET","positionFill": "DEFAULT"}}#"price": order_price,"timeInForce": "GTC",
                    r_oc = orders.OrderCreate(accountID, data=data)
                    client.request(r_oc)
                    print(r_ic.response['instrument'])
                    print(r_ic.response['candles'][1]['time'][:16])
                    print(str1_product_list[product])

                    #print (r_oc.response)
                    str1_product_list[product] = r_ic.response['candles'][1]['time'][:16]
                if float(r_ad.response['account']['marginAvailable']) < 50000:
                    r_op = positions.PositionDetails(accountID, instrument = product)#
                    client.request(r_op)
                    
                    if float(r_op.response['position']['long']['units']) > 0:

                        r_pc = positions.PositionClose(accountID, instrument = product, data = {'longUnits':'1'})
                        client.request(r_pc)
                        
                        str1_product_list[product] = r_ic.response['candles'][1]['time'][:16]
                        #print(r_pc.response)
        
"""

"""

r = positions.OpenPositions(accountID)
client.request(r)
print(r.response)

product_pos = pd.DataFrame(r.response['positions'])

print(product_pos['long'])
print (len(r.response['positions']))

fx = []
bond = []
index = []

for i in np.arange(0,len(r.response['positions'])):
    print (r.response['positions'][i]['instrument'])

a=0
if r.response['positions'][0]

#r = trades.TradesList(accountID)
## show the endpoint as it is constructed for this call
#print("REQUEST:{}".format(r))
#rv = api.request(r)
#print("RESPONSE:\n{}".format(json.dumps(rv, indent=2)))


data = {
"order": {
"price": "1.168",
"stopLossOnFill": {
"timeInForce": "GTC",
"price": "1.15"
},
"timeInForce": "GTC",
"instrument": "EUR_USD",
"units": "100",
"type": "LIMIT",
"positionFill": "DEFAULT"
}
}


r1 = orders.OrderDetails(accountID=accountID, orderID=r.response['lastTransactionID'])
client.request(r1)
print (r1.response)


import json
from oandapyV20 import API
from oandapyV20.contrib.factories import InstrumentsCandlesFactory

client = API(access_token=access_token)
instrument, granularity = "EUR_USD", "M15"
_from = "2017-08-01T00:00:00Z"
params = {
"from": _from,
"granularity": granularity,
"count": 2500
}
with open("/tmp/{}.{}".format(instrument, granularity), "w") as OUT:
    # The factory returns a generator generating consecutive
    # requests to retrieve full history from date 'from' till 'to'
    for r in InstrumentsCandlesFactory(instrument=instrument,params=params):
        client.request(r)
        OUT.write(json.dumps(r.response.get('candles'), indent=2))
