# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 11:18:15 2016

@author: 14224
"""


import pyodbc
import pandas as pd
import datetime
import numpy as np
import matplotlib.pyplot as plt
from functools import reduce
import operator

conn = pyodbc.connect('Driver={SQL Server};Server=10.216.8.54;Database=Thai;Trusted_Connection=yes;')

sql = "SELECT [日期] as dt, [股票代號] as ss, [股票名稱] as sn, [成交量] as v, [開盤價] as o, [最高價] as h, [最低價] as l, [收盤價] as c, [漲幅(%)] as m, [漲跌停] as ml"
sql = sql + " FROM [warrant].[dbo].[日收盤表排行]"
sql = sql + " where [日期]>='20060101' and [日期]<='20170101' and len([股票代號])=4 and left([股票代號],1)<>0"
#sql = sql + " order by 股票代號,日期"

df = pd.io.sql.read_sql(sql,conn)

conn.close()

df = df.sort(['ss','dt']).reset_index().drop('index',axis=1)
df['br'] = np.where(df['c']>df['o'],1,np.where(df['c']==df['o'],0,-1))
df['ul'] = (df['h']-df['c'])/df['h']

df.loc[:,'br1b']=pd.DataFrame(df['br'].ix[0:len(df['br'])-2]).set_index(np.arange(1,len(df['br']))).rename(columns={'br':'br1b'}).fillna(0)
df.loc[:,'br2b']=pd.DataFrame(df['br'].ix[0:len(df['br'])-3]).set_index(np.arange(2,len(df['br']))).rename(columns={'br':'br2b'}).fillna(0)
df.loc[:,'br3b']=pd.DataFrame(df['br'].ix[0:len(df['br'])-4]).set_index(np.arange(3,len(df['br']))).rename(columns={'br':'br3b'}).fillna(0)
df.loc[:,'br4b']=pd.DataFrame(df['br'].ix[0:len(df['br'])-5]).set_index(np.arange(4,len(df['br']))).rename(columns={'br':'br4b'}).fillna(0)
df.loc[:,'br5b']=pd.DataFrame(df['br'].ix[0:len(df['br'])-6]).set_index(np.arange(5,len(df['br']))).rename(columns={'br':'br5b'}).fillna(0)
df.loc[:,'br6b']=pd.DataFrame(df['br'].ix[0:len(df['br'])-7]).set_index(np.arange(6,len(df['br']))).rename(columns={'br':'br6b'}).fillna(0)
df.loc[:,'br7b']=pd.DataFrame(df['br'].ix[0:len(df['br'])-8]).set_index(np.arange(7,len(df['br']))).rename(columns={'br':'br7b'}).fillna(0)
df.loc[:,'ml1b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-2]).set_index(np.arange(1,len(df['ml']))).rename(columns={'ml':'ml1b'}).fillna(0)
df.loc[:,'ml2b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-3]).set_index(np.arange(2,len(df['ml']))).rename(columns={'ml':'ml2b'}).fillna(0)
df.loc[:,'ml3b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-4]).set_index(np.arange(3,len(df['ml']))).rename(columns={'ml':'ml3b'}).fillna(0)
df.loc[:,'ml4b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-5]).set_index(np.arange(4,len(df['ml']))).rename(columns={'ml':'ml4b'}).fillna(0)
df.loc[:,'ml4b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-5]).set_index(np.arange(4,len(df['ml']))).rename(columns={'ml':'ml4b'}).fillna(0)
df.loc[:,'ml5b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-6]).set_index(np.arange(5,len(df['ml']))).rename(columns={'ml':'ml5b'}).fillna(0)
df.loc[:,'ml6b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-7]).set_index(np.arange(6,len(df['ml']))).rename(columns={'ml':'ml6b'}).fillna(0)
df.loc[:,'ml7b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-8]).set_index(np.arange(7,len(df['ml']))).rename(columns={'ml':'ml7b'}).fillna(0)
df.loc[:,'ul1b']=pd.DataFrame(df['ul'].ix[0:len(df['ul'])-2]).set_index(np.arange(1,len(df['ul']))).rename(columns={'ul':'ul1b'}).fillna(0)
df.loc[:,'ss1b']=pd.DataFrame(df['ss'].ix[0:len(df['ss'])-2]).set_index(np.arange(1,len(df['ss']))).rename(columns={'ss':'ss1b'}).fillna(0)
df.loc[:,'c1b']=pd.DataFrame(df['c'].ix[0:len(df['c'])-2]).set_index(np.arange(1,len(df['c']))).rename(columns={'c':'c1b'}).fillna(0)
df.loc[:,'c2b']=pd.DataFrame(df['c'].ix[0:len(df['c'])-3]).set_index(np.arange(2,len(df['c']))).rename(columns={'c':'c2b'}).fillna(0)
df.loc[:,'o1b']=pd.DataFrame(df['o'].ix[0:len(df['o'])-2]).set_index(np.arange(1,len(df['o']))).rename(columns={'o':'o1b'}).fillna(0)
df.loc[:,'v1b']=pd.DataFrame(df['v'].ix[0:len(df['v'])-2]).set_index(np.arange(1,len(df['v']))).rename(columns={'v':'v1b'}).fillna(0)
df['y'] = df['dt'].str[:4] + '0101'

df['om'] = (df['o']-df['c1b'])/df['c1b']
df['dt'] = pd.to_datetime(df['dt'])
df['to'] = np.where(((df['dt']< '2015-06-01')&(abs(df['om'])>=0.06))|((df['dt']>= '2015-06-01')&(abs(df['om'])>=0.09)),1,0)
df['hm'] = (df['h']-df['c1b'])/df['c1b']
df['lm'] = (df['l']-df['c1b'])/df['c1b']
df['th1'] = np.where(((df['dt']< '2015-06-01')&(df['hm']>0.07))|((df['dt']< '2015-06-01')&(df['lm']<-0.07))|((df['dt']>= '2015-06-01')&(df['hm']>0.1))|((df['dt']>= '2015-06-01')&(df['hm']<-0.1)),1,0)

df['th'] = np.where(((df['dt']< '2015-06-01')&(df['hm']>=0.06))|((df['dt']>= '2015-06-01')&(df['hm']>=0.09)),1,0)

####兩天紅k+前一天紅k長上影線，同一支股票，####
df1 = df[(df['br1b']==1)&(df['br2b']==1)&(df['br3b']==1)&(df['br4b']==1)&(df['br5b']==1)&(df['ss']==df['ss1b'])&(df['ml1b']!=1)&((df['ml2b']==1)|(df['ml3b']==1)|(df['ml4b']==1)|(df['ml5b']==1))]#&(df['ul1b']>0.02)&(df['br4b']==1)&(df['br5b']==1)|(df['ml4b']==1)|(df['ml5b']==1)
df1 = df1.reset_index().drop('index',axis=1)

####開盤和收盤非漲停####
df2 = df1[(df1['to']==0)&(df1['th1']==0)]

dff = df2[(df2['c1b']>=10)&(df2['v1b']>=500)]
dff['pl%'] = np.where(dff['th']==1,(((dff['o']-dff['h'])/dff['o'])-0.005),(((dff['o']-dff['c'])/dff['o'])-0.005))
stat = dff.groupby(['dt'])['pl%'].mean().reset_index()
stat['cs'] = stat['pl%'].cumsum()
stat['dt'] = pd.to_datetime(stat['dt'])
stat['cm'] = stat['pl%']+1
statc = dff.groupby(['dt']).count()
staty = dff.groupby(['y'])['pl%'].sum().reset_index()
staty['y'] = pd.to_datetime(staty['y'])
staty['cm'] = staty['pl%']+1

fig = plt.figure(figsize=(15,10))
ax1 = fig.add_subplot(111)
ax1.plot(stat['dt'],stat['cs'])

