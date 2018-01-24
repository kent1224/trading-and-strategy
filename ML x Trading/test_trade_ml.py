# -*- coding: utf-8 -*-
"""
Created on Fri Jul  7 17:21:58 2017

@author: 14224
"""

import pyodbc
import pandas as pd
import datetime
import numpy as np
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.cross_validation import KFold, cross_val_score

#######################################################################################################################################################################################
"""資料處理 """

conn = pyodbc.connect('Driver={SQL Server};Server=10.216.8.54;Database=Thai;Trusted_Connection=yes;')

sql = "SELECT [日期] as dt, [股票代號] as ss, [股票名稱] as sn, [成交量] as v, [開盤價] as o, [最高價] as h, [最低價] as l, [收盤價] as c, [漲幅(%)] as m, [漲跌停] as ml"
sql = sql + " FROM [warrant].[dbo].[日收盤表排行]"
sql = sql + " where [日期]>='20150601' and len([股票代號])=4 and left([股票代號],1)<>0"
#sql = sql + " order by 股票代號,日期"

df = pd.io.sql.read_sql(sql,conn)

conn.close()

df = df.sort(['ss','dt']).reset_index().drop('index',axis=1)

#df1 = df[(df['o']>df['c'])].reset_index()
#df2 = df[(df['o']==df['c'])].reset_index()
#df3 = df[(df['o']<df['c'])].reset_index()

#df1['kc'] = 1
#df2['kc'] = 0
#df3['kc'] = -1

#df = pd.concat([df1,df2,df3]).drop('index',axis=1).sort(['ss','dt']).reset_index().drop('index',axis=1)

#df.loc[:,'-1kc']=pd.DataFrame(df['kc'].ix[0:len(df['kc'])-2]).set_index(np.arange(1,len(df['kc']))).rename(columns={'kc':'-1kc'}).fillna(0)
#df.loc[:,'-2kc']=pd.DataFrame(df['kc'].ix[0:len(df['kc'])-3]).set_index(np.arange(2,len(df['kc']))).rename(columns={'kc':'-2kc'}).fillna(0)
#df.loc[:,'-3kc']=pd.DataFrame(df['kc'].ix[0:len(df['kc'])-4]).set_index(np.arange(3,len(df['kc']))).rename(columns={'kc':'-3kc'}).fillna(0)
df.loc[:,'-1m']=pd.DataFrame(df['m'].ix[0:len(df['m'])-2]).set_index(np.arange(1,len(df['m']))).rename(columns={'m':'-1m'}).fillna(0)
df.loc[:,'-2m']=pd.DataFrame(df['m'].ix[0:len(df['m'])-3]).set_index(np.arange(2,len(df['m']))).rename(columns={'m':'-2m'}).fillna(0)
df.loc[:,'-3m']=pd.DataFrame(df['m'].ix[0:len(df['m'])-4]).set_index(np.arange(3,len(df['m']))).rename(columns={'m':'-3m'}).fillna(0)
df.loc[:,'-1ml']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-2]).set_index(np.arange(1,len(df['ml']))).rename(columns={'ml':'-1ml'}).fillna(0)
df.loc[:,'-2ml']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-3]).set_index(np.arange(2,len(df['ml']))).rename(columns={'ml':'-2ml'}).fillna(0)
df.loc[:,'-3ml']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-4]).set_index(np.arange(3,len(df['ml']))).rename(columns={'ml':'-3ml'}).fillna(0)
df.loc[:,'-1c']=pd.DataFrame(df['c'].ix[0:len(df['c'])-2]).set_index(np.arange(1,len(df['c']))).rename(columns={'c':'-1c'}).fillna(0)
df.loc[:,'-1v']=pd.DataFrame(df['v'].ix[0:len(df['v'])-2]).set_index(np.arange(1,len(df['v']))).rename(columns={'v':'-1v'}).fillna(0)


#df.loc[:,'1m'] = pd.DataFrame(df['m'].ix[1:len(df['m'])-1]).set_index(np.arange(0,len(df['m'])-1)).rename(columns={'m':'1m'}).fillna(0)
df.loc[:,'5c'] = pd.DataFrame(df['c'].ix[5:len(df['c'])-1]).set_index(np.arange(0,len(df['c'])-5)).rename(columns={'c':'5c'}).fillna(0)
df['5m'] = (df['5c']-df['c'])/df['c']

df.loc[:,'-3ss']=pd.DataFrame(df['ss'].ix[0:len(df['ss'])-4]).set_index(np.arange(3,len(df['ss']))).rename(columns={'ss':'-3ss'}).fillna(0)
df.loc[:,'5ss'] = pd.DataFrame(df['ss'].ix[5:len(df['ss'])-1]).set_index(np.arange(0,len(df['ss'])-5)).rename(columns={'ss':'5ss'}).fillna(0)


df4 = df[df['5m']>=0.01].reset_index()
df5 = df[df['5m']<0.01].reset_index()
df4['label'] = 1
df5['label'] = 0

df = pd.concat([df4,df5]).drop('index',axis=1).sort(['ss','dt']).reset_index().drop('index',axis=1)

df['h_l'] = (df['h']-df['l'])/df['-1c']
df['h_c'] = (df['h']-df['c'])/df['-1c']
df['h_o'] = (df['h']-df['o'])/df['-1c']
df['o_l'] = (df['o']-df['l'])/df['-1c']
df['o_c'] = (df['o']-df['c'])/df['-1c']

df = df.fillna(20)
df = df[(df['-1m']!=20)&(df['-2m']!=20)&(df['-3m']!=20)&
        (df['-1ml']!=20)&(df['-2ml']!=20)&(df['-3ml']!=20)&
        (df['-1c']!=20)&(df['-1v']!=20)&(df['5m']!=20)&#
        (df['-3ss']==df['ss'])&(df['5ss']==df['ss'])]#(df['-1kc']!=20)&(df['-2kc']!=20)&(df['-3kc']!=20)&
#
df['c_l'] = (df['c']-df['l'])/df['-1c']
df['vm'] = (df['v']-df['-1v'])/df['-1v']

df = df[(df['vm']!=np.inf)&(df['vm']!=np.nan)&(df['vm']!=-1)]

df['amount'] = df['v']*df['c']

df = df.drop(['ss','sn','o','h','l','c','5m','5c','-3ss','5ss'],axis=1).reset_index().drop('index',axis=1)

df['vm'] = df['vm'].fillna(0)
np.isnan(df['vm']).any()
###############################################################################################################################################################
"""分train跟test"""

df_train = df[df['dt']<='20161231']
df_test = df[df['dt']>='20170101']

y = df_train['label'].reset_index().drop('index',axis=1)
x = df_train.drop(['dt','label'],axis=1)
y_test = df_test['label'].reset_index().drop('index',axis=1)
x_test = df_test.drop(['dt','label'],axis=1)

###############################################################################################################################################################
""" Base Accuracy """
y_test['base_0'] = 0
y_test['base_1'] = 1

base_acc0 = metrics.accuracy_score(y_test['base_0'],y_test['label'])
base_acc1 = metrics.accuracy_score(y_test['base_1'],y_test['label'])

###############################################################################################################################################################
#kfold = KFold(len(x), n_folds = 10, random_state = 7)
model = LogisticRegression().fit(x, y)
predicted = model.predict(x_test)

pre_acc = metrics.accuracy_score(predicted, y_test['label'])

print(pre_acc, base_acc0)

dfz = pd.DataFrame({'pre_label':predicted})
dfz['tru_label'] = y_test

#score = cross_val_score(model, x, y, cv = kfold)

writer = pd.ExcelWriter('test_pre.xlsx')
dfz.to_excel(writer)
writer.save()

#print(score.mean(), score.std())