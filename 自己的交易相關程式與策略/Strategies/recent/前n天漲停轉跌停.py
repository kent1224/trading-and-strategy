# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 14:41:26 2016

@author: 14224
"""

import pyodbc
import pandas as pd
import datetime
import numpy as np

conn = pyodbc.connect('Driver={SQL Server};Server=10.216.8.54;Database=warrant;Trusted_Connection=yes;')

sql = "SELECT [日期] as dt, [股票代號] as ss, [股票名稱] as sn, [成交量] as v, [開盤價] as o, [最高價] as h, [最低價] as l, [收盤價] as c, [漲幅(%)] as m, [漲跌停] as ml"
sql = sql + " FROM [warrant].[dbo].[日收盤表排行]"
sql = sql + " where [日期]>='20150601' and [日期]<='20171231' and len([股票代號])=4 and left([股票代號],1)<>0"
#sql = sql + " order by 股票代號,日期"

df = pd.io.sql.read_sql(sql,conn)

conn.close()

df = df.sort(['ss','dt']).reset_index().drop('index',axis=1)

df.loc[:,'ml1b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-2]).set_index(np.arange(1,len(df['ml']))).rename(columns={'ml':'ml1b'}).fillna(0)
df.loc[:,'ml2b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-3]).set_index(np.arange(2,len(df['ml']))).rename(columns={'ml':'ml2b'}).fillna(0)
df.loc[:,'ml3b']=pd.DataFrame(df['ml'].ix[0:len(df['ml'])-4]).set_index(np.arange(3,len(df['ml']))).rename(columns={'ml':'ml3b'}).fillna(0)
#df.loc[:,'m1a']=pd.DataFrame(df['m'].ix[1:len(df['m'])-1]).set_index(np.arange(0,len(df['m'])-1)).rename(columns={'m':'m1a'}).fillna(0)
df.loc[:,'ss1b']=pd.DataFrame(df['ss'].ix[0:len(df['ss'])-2]).set_index(np.arange(1,len(df['ss']))).rename(columns={'ss':'ss1b'}).fillna(0)
df.loc[:,'ss2b']=pd.DataFrame(df['ss'].ix[0:len(df['ss'])-3]).set_index(np.arange(2,len(df['ss']))).rename(columns={'ss':'ss2b'}).fillna(0)
df.loc[:,'ss3b']=pd.DataFrame(df['ss'].ix[0:len(df['ss'])-4]).set_index(np.arange(3,len(df['ss']))).rename(columns={'ss':'ss3b'}).fillna(0)
#df.loc[:,'ss1a']=pd.DataFrame(df['ss'].ix[1:len(df['ss'])-1]).set_index(np.arange(0,len(df['ss'])-1)).rename(columns={'ss':'ss1a'}).fillna(0)
#df.loc[:,'c1b']=pd.DataFrame(df['c'].ix[0:len(df['c'])-2]).set_index(np.arange(1,len(df['c']))).rename(columns={'c':'c1b'}).fillna(0)
#df.loc[:,'v1b']=pd.DataFrame(df['v'].ix[0:len(df['v'])-2]).set_index(np.arange(1,len(df['v']))).rename(columns={'v':'v1b'}).fillna(0)
#df['ym'] = df['dt'].str[:6]

df1 = df[(df['ss']==df['ss1b'])&(df['ss']==df['ss2b'])&(df['ss']==df['ss3b'])&(df['h']>df['l'])&(df['ml']==-1)&(df['ml1b']==1)&(df['ml2b']==1)&(df['ml3b']==1)]
df1 = df1[(df1['c']>=5)&(df1['v']>=100)]

writer = pd.ExcelWriter('111.xlsx')
df1.to_excel(writer,'Sheet1')
writer.save()

df10 = df1[df1['ml1b']==1]
df11 = df1[df1['m1b']>=0]
df12 = df1[df1['m1b']<0]

df10['symbol'] = 0
df11['symbol'] = 1
df12['symbol'] = 2

df2 = pd.concat([df10,df11,df12])
df2 = df2.sort(['ss','dt']).reset_index().drop('index',axis=1)
df2 = df2[(df2['c1b']>=10)&(df2['v1b']>=300)]#

df3 = df2.groupby(['dt','ym'])['m1a'].mean().reset_index()
df3a = df2.groupby(['dt','symbol','ym'])['m1a'].mean().reset_index()

#df3['m1a'] = ((df3['m1a']-0.5-1)/100)+1
#df3a['m1a'] = ((df3a['m1a']-0.5-1)/100)+1

df3['m1a'] = df3['m1a']-0.5-1
df3a['m1a'] = df3a['m1a']-0.5-1

df3 = df3.groupby('ym')['m1a'].sum().reset_index()
df3a = df3a.groupby(['ym','symbol'])['m1a'].sum().reset_index()

df30 = df3a[df3a['symbol']==0].reset_index().drop('index',axis=1)
df31 = df3a[df3a['symbol']==1].reset_index().drop('index',axis=1)
df32 = df3a[df3a['symbol']==2].reset_index().drop('index',axis=1)

a=1
b=[]
for i in df30['m1a']:
   a *=i
   b.append(a)

print(a)
b=pd.DataFrame(b)
b.plot()

df4 = df3['m1a'].mul()
df40 = df30['m1a'].sum()
df41 = df31['m1a'].sum()
df42 = df32['m1a'].sum()

####前兩天漲停，同一支股票####
df1 = df[(df['ml1b']==1)&(df['ml2b']==1)&(df['ml3b']==1)&(df['ss']==df['ss1b'])]
df1 = df1.reset_index().drop('index',axis=1)

####開盤和收盤非漲停####
#df2 = df1[(df1['ml']<1)|(df1['c']>df1['o'])]

dff = df1[(df1['c1b']>=10)&(df1['v1b']>=500)]
dff['pl%'] = (((dff['o']-dff['c'])/dff['o'])-0.005)*100
stat = dff.groupby(['ym'])['pl%'].sum()
statc = dff.groupby(['ym']).count()


