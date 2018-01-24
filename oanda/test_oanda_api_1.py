#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 29 01:29:35 2017

@author: kentchung
"""

import oandapyV20
import oandapyV20.endpoints.orders as orders

""" account information """
accountID = '101-011-3037533-001'
access_token = '7ab23e5bf8fc363b01e90b1c45f4e56c-5febdf006318b1de35b4d5e8a4fd6b0d'
client = oandapyV20.API(access_token = access_token)

""" actions """
r = orders.OrderList(accountID = accountID)
client.request(r)
print (r.response)

r1 = orders.OrderCancel(accountID = accountID, orderID = r.response['orders'][0]['id'])
client.request(r1)
print (r1.response)

data = {
"order": {
"price": "1.1824",
"stopLossOnFill": {
"timeInForce": "GTC",
"price": "1.18"
},
"timeInForce": "GTC",
"instrument": "EUR_USD",
"units": "100",
"type": "LIMIT",
"positionFill": "DEFAULT"
}
}

#r2 = orders.OrderCreate(accountID = accountID, data=data)
#client.request(r2)
#print (r2.response)


from oandapyV20 import API
import json
import numpy as np
import oandapyV20.endpoints.pricing as pricing
import pandas as pd
api = API(access_token=access_token)
params ={"instruments": "EUR_USD"}

for i in np.arange(0,100):
    r = pricing.PricingInfo(accountID=accountID, params=params)
    rv = api.request(r)
    #df = pd.DataFrame(r.response['prices'])
    print(r.response['prices'][0]['asks'])
    if float(r.response['prices'][0]['asks'][0]['price']) <= float(data['order']['price']):
        r2 = orders.OrderCreate(accountID = accountID, data=data)
        client.request(r2)
        print (r2.response)
        break


from oandapyV20.exceptions import V20Error
from oandapyV20.endpoints.pricing import PricingStream


api = API(access_token=access_token, environment="practice")

instruments = "DE30_EUR,EUR_USD,EUR_JPY"
s = PricingStream(accountID=accountID, params={"instruments":instruments})
try:
    n = 0
    for R in api.request(s):
        print(json.dumps(R, indent=2))
        n += 1
        if n > 10:
            s.terminate("maxrecs received: 10")#{}".format(MAXREC))

except V20Error as e:
    print("Error: {}".format(e))
        

