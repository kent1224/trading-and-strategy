# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 09:37:29 2016

@author: 14224
"""

import pyodbc
import pandas
import numpy


conn = pyodbc.connect('Driver={SQL Server};Server=10.216.8.54;Database=warrant;Trusted_Connection=yes;')

sql = """
SELECT [日期] as datee, [股票代號] as usymbol, [股票名稱] as uname, [開盤價] as openp
      , [最高價] as highp, [最低價] as lowp, [收盤價] as closep, [漲幅(%)] as magn
      , [漲跌停] as plimit
  FROM [warrant].[dbo].[日收盤表排行]
  where 日期>='20150101' and len(股票代號)=4 and left(股票代號,1)<>0 and 收盤價>=10

"""

df = pandas.io.sql.read_sql(sql,conn)

conn.close()

df1n = df[((df['openp']!=df['highp'])&(df['closep']==df['highp'])&(df['plimit']==1))|
          ((df['openp']!=df['highp'])&(df['closep']!=df['highp'])&(df['plimit']==1))|
          ((df['openp']!=df['highp'])&(df['closep']==df['highp'])&(df['plimit']!=1))|
          ((df['openp']!=df['highp'])&(df['closep']!=df['highp'])&(df['plimit']!=1))|
          ((df['openp']==df['highp'])&(df['closep']!=df['highp'])&(df['plimit']==1))|
          ((df['openp']==df['highp'])&(df['closep']!=df['highp'])&(df['plimit']!=1))|
          ((df['openp']==df['highp'])&(df['closep']==df['highp'])&(df['plimit']!=1))]

df1 = df[(df['openp']==df['highp']) & (df['closep']==df['highp']) & (df['plimit']==1)]
#df1n = df[((df['closep']!=df['highp']) & (df['plimit']==1)) | 
#          ((df['closep']==df['highp']) & (df['plimit']!=1)) |
#          ((df['closep']!=df['highp']) & (df['plimit']!=1)) ]
#df1 = df[(df['closep']==df['highp']) & (df['plimit']==1)]
df1['mark0']=0
df1['mark1']=1
df1['mark2']=2
df1['mark3']=3
df1['mark4']=4
df1['mark5']=5

df2 = pandas.concat([df1n,df1])
df2 = df2.sort(['usymbol','datee'],ascending=[1,1]).reset_index().drop('index',axis=1)

df2.loc[:,'mark1n']=pandas.DataFrame(df2['mark1'].ix[0:len(df2['mark1'])-2]).set_index(numpy.arange(1,len(df2['mark1']))).rename(columns={'mark1':'mark1n'})
df2.loc[:,'mark2n']=pandas.DataFrame(df2['mark2'].ix[0:len(df2['mark2'])-3]).set_index(numpy.arange(2,len(df2['mark2']))).rename(columns={'mark2':'mark2n'})
df2.loc[:,'mark3n']=pandas.DataFrame(df2['mark3'].ix[0:len(df2['mark3'])-4]).set_index(numpy.arange(3,len(df2['mark3']))).rename(columns={'mark3':'mark3n'})
df2.loc[:,'mark4n']=pandas.DataFrame(df2['mark4'].ix[0:len(df2['mark4'])-5]).set_index(numpy.arange(4,len(df2['mark4']))).rename(columns={'mark4':'mark4n'})
df2.loc[:,'mark5n']=pandas.DataFrame(df2['mark5'].ix[0:len(df2['mark5'])-6]).set_index(numpy.arange(5,len(df2['mark5']))).rename(columns={'mark5':'mark5n'})

df2.loc[:,'magn-1']=pandas.DataFrame(df2['magn'].ix[0:len(df2['magn'])-2]).set_index(numpy.arange(1,len(df2['magn']))).rename(columns={'magn':'magn-1'})
df2.loc[:,'magn-2']=pandas.DataFrame(df2['magn'].ix[0:len(df2['magn'])-3]).set_index(numpy.arange(2,len(df2['magn']))).rename(columns={'magn':'magn-2'})
df2.loc[:,'magn-3']=pandas.DataFrame(df2['magn'].ix[0:len(df2['magn'])-4]).set_index(numpy.arange(3,len(df2['magn']))).rename(columns={'magn':'magn-3'})
df2.loc[:,'magn-4']=pandas.DataFrame(df2['magn'].ix[0:len(df2['magn'])-5]).set_index(numpy.arange(4,len(df2['magn']))).rename(columns={'magn':'magn-4'})
df2.loc[:,'magn-5']=pandas.DataFrame(df2['magn'].ix[0:len(df2['magn'])-6]).set_index(numpy.arange(5,len(df2['magn']))).rename(columns={'magn':'magn-5'})


df31 = df2[df2['mark1n']==1]
df31['rt']=(1+df31['magn']/100)
df31p = df31[df31['magn']>0]
df31n = df31[df31['magn']<=0]

df32 = df2[df2['mark2n']==2]
df32['rt']=(1+df32['magn-1']/100)*(1+df32['magn']/100)
df32pp = df32[(df32['magn']>0) & (df32['magn-1']>0)]
df32np = df32[(df32['magn']>0) & (df32['magn-1']<=0)]
df32pn = df32[(df32['magn']<=0) & (df32['magn-1']>0)]

df33 = df2[df2['mark3n']==3]
df33['rt']=(1+df33['magn-2']/100)*(1+df33['magn-1']/100)*(1+df33['magn']/100)
df33ppp = df33[(df33['magn']>0)&(df33['magn-1']>0)&(df33['magn-2']>0)]
df33pnp = df33[(df33['magn']>0)&(df33['magn-1']<=0)&(df33['magn-2']>0)]
df33ppn = df33[(df33['magn']<=0)&(df33['magn-1']>0)&(df33['magn-2']>0)]

df34 = df2[df2['mark4n']==4]
df34['rt']=(1+df34['magn-3']/100)*(1+df34['magn-2']/100)*(1+df34['magn-1']/100)*(1+df34['magn']/100)
df34pppp = df34[(df34['magn']>0)&(df34['magn-1']>0)&(df34['magn-2']>0)&(df34['magn-3']>0)]
df34ppnp = df34[(df34['magn']>0)&(df34['magn-1']<=0)&(df34['magn-2']>0)&(df34['magn-3']>0)]
df34pppn = df34[(df34['magn']<=0)&(df34['magn-1']>0)&(df34['magn-2']>0)&(df34['magn-3']>0)]

df35 = df2[df2['mark5n']==5]
df35['rt']=(1+df35['magn-4']/100)*(1+df35['magn-3']/100)*(1+df35['magn-2']/100)*(1+df35['magn-1']/100)*(1+df35['magn']/100)
df35ppppp = df35[(df35['magn']>0)&(df35['magn-1']>0)&(df35['magn-2']>0)&(df35['magn-3']>0)&(df35['magn-4']>0)]
df35pppnp = df35[(df35['magn']>0)&(df35['magn-1']<=0)&(df35['magn-2']>0)&(df35['magn-3']>0)&(df35['magn-4']>0)]
df35ppppn = df35[(df35['magn']<=0)&(df35['magn-1']>0)&(df35['magn-2']>0)&(df35['magn-3']>0)&(df35['magn-4']>0)]

a = (df35ppppp['rt'].mean()-1)*100
b = (df35ppppn['rt'].mean()-1)*100
c = (df34pppn['rt'].mean()-1)*100
d = (df33ppn['rt'].mean()-1)*100
e = (df32pn['rt'].mean()-1)*100
f = (df31n['rt'].mean()-1)*100
g = (df31p['rt'].mean()-1)*100
h = (df32pp['rt'].mean()-1)*100
i = (df33ppp['rt'].mean()-1)*100
j = (df34pppp['rt'].mean()-1)*100
