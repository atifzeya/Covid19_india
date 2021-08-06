# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 16:42:48 2020

@author: User
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 00:10:42 2020

@author: User
"""

import requests
# to parse json contents
import json
# to parse csv files
import csv

# for numerical operations
import numpy as np
# to store and analysis data in dataframes
import pandas as pd
from datetime import datetime, timedelta

#using api to import data
district_wise_daily = pd.read_csv('https://api.covid19india.org/csv/latest/districts.csv')

#changing date format
district_wise_daily['Date']=pd.to_datetime(district_wise_daily['Date'], format='%Y-%m-%d')
district_wise_daily['Date']=district_wise_daily['Date'].dt.strftime('%d-%m-%Y')  

yday=str(datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y'))      #yesterday
db_yday=str(datetime.strftime(datetime.now() - timedelta(2), '%d-%m-%Y'))    #day before yesterday

df1=district_wise_daily[district_wise_daily['Date'].isin([yday])].iloc[:,0:6]
df2=district_wise_daily[district_wise_daily['Date'].isin([db_yday])].iloc[:,1:6]

df=pd.merge(df1,df2,how='left',on=['State','District'])

df['confirmed']=df['Confirmed_x']-df['Confirmed_y']
df['recovered']=df['Recovered_x']-df['Recovered_y']
df['deceased']=df['Deceased_x']-df['Deceased_y']

dff=df[['Date','State','District','confirmed','recovered','deceased']]

dff.loc[dff['confirmed'] < 0, 'confirmed'] = 0
dff.loc[dff['recovered'] < 0, 'recovered'] = 0
dff.loc[dff['deceased'] < 0, 'deceased'] = 0


df_state=dff.groupby(['Date','State'], as_index=False).sum()
df_state.columns = ['Date','state_name','confirmed_state','recovered_state','deceased_state']
dfstate = pd.read_excel('D:/covid/LGD_code.xlsx', sheet_name='state_code')
df_state=pd.merge(df_state,dfstate,how='left',left_on=df_state['state_name'].str.lower(),right_on=['state_name'])
df_state=df_state[['Date','state_name','state_code','confirmed_state','recovered_state','deceased_state']]

#importing india level data
df_india=pd.read_csv('https://api.covid19india.org/csv/latest/case_time_series.csv')
df_india=df_india[['Date_YMD','Daily Confirmed','Daily Recovered','Daily Deceased']]
df_india.columns = ['Date','confirmed_india','recovered_india','deceased_india']
df_india['Date']=pd.to_datetime(df_india['Date'], format='%Y-%m-%d')
df_india['Date']=df_india['Date'].dt.strftime('%d-%m-%Y')

df_india1=df_india.tail(1)          
df_india1.iloc[0,0]=yday

dff['sd']=dff['State'].str.lower() + dff['District'].str.lower()  #creating an identifier for left join of LGD codes

#joining lgd codes
dfs = pd.read_excel('D:/covid/LGD_code.xlsx', sheet_name='district_code')

d=pd.merge(dff,dfs,how='left',left_on=['sd'],right_on=['sd'])
d.isnull().sum()

#LGD codes for the following not available, hence dropping them
d.drop( d[ d['District'].isin(['Unknown','Foreign Evacuees',
'Other State','Capital Complex',
'Others','Upper Dibang Valley','Gaurela Pendra Marwahi',
'State Pool','Hnahthial','Khawzawl','Saitual',
'BSF Camp','Chengalpattu','Kallakurichi',
'Evacuees','Ranipet','Tenkasi',
'Italians','Tirupathur',
'Airport Quarantine',
'Railway Quarantine']) ].index , inplace=True)

d.isnull().sum()

data=pd.merge(d,df1,how='left',on=['Date','State','District'])

data=data[['Date','State','state_code','district_name','district_code'
         ,'confirmed','recovered','deceased','Confirmed','Recovered','Deceased']]

data.columns=['Date','state_name','state_code','district_name','district_code'
         ,'confirmed_district','recovered_district','deceased_district',
         'total_confirmed_district','total_recovered_district','total_deceased_district']

data=pd.merge(df_state,data,how='left',on=['state_code'])

df_state_total=df1.groupby(['State'],as_index=False).sum()
df_state_total['State']=df_state_total['State'].str.lower()
df_state_total.columns = ['state_name_x','total_confirmed_state','total_recovered_state','total_deceased_state']
data=pd.merge(data,df_state_total,how='left',left_on=['state_name_x'],right_on=['state_name_x'])

data=pd.merge(data,df_india1,how='left',left_on=['Date_x'],right_on=['Date'])

df_india_total=df1.groupby(['Date'],as_index=False).sum()
df_india_total.columns = ['Date_x','total_confirmed_india','total_recovered_india','total_deceased_india']
data=pd.merge(data,df_india_total,how='left',on=['Date_x'])

data=data[['Date_x','state_name_x','state_code','district_name','district_code',
         'confirmed_district','recovered_district','deceased_district',
         'total_confirmed_district','total_recovered_district','total_deceased_district',
         'confirmed_state','recovered_state','deceased_state',
         'total_confirmed_state','total_recovered_state','total_deceased_state',
         'confirmed_india','recovered_india','deceased_india',
         'total_confirmed_india','total_recovered_india','total_deceased_india'
         ]]

data.columns=['Date','state_name','state_code','district_name','district_code',
         'confirmed_district','recovered_district','deceased_district',
         'total_confirmed_district','total_recovered_district','total_deceased_district',
         'confirmed_state','recovered_state','deceased_state',
         'total_confirmed_state','total_recovered_state','total_deceased_state',
         'confirmed_india','recovered_india','deceased_india',
         'total_confirmed_india','total_recovered_india','total_deceased_india'
         ]

data.isnull().sum()
data=data.fillna("") 
data.isnull().sum()

filename='D:/covid/covid_'+yday+'.csv'

data.to_csv(filename,index=False)

