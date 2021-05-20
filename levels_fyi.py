
import pandas as pd
import requests
import numpy as np
import math
from datetime import timedelta
import re

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets

data = requests.get('https://www.levels.fyi/js/salaryData.json').json()
df = pd.DataFrame(data)

# Remove columns that we don't need
df = df.drop(['cityid','rowNumber','dmaid'], axis=1)
df = df.replace("", np.nan)

#convert datatypes
num_cols = ['yearsofexperience','basesalary','bonus','stockgrantvalue',
            'totalyearlycompensation','yearsatcompany']
df[num_cols] = df[num_cols].apply(pd.to_numeric)

#one record without a location, kick it out
df = df[df.location.notnull()]

#round up all of the years of experience even if it is 0.25 years
df['yearsofexperience'] = np.ceil(df.yearsofexperience)
df['yearsatcompany'] = np.ceil(df.yearsatcompany)

#remove records that fall in the top/bottom 95th/5th percentile on totalyearly compensation
#I do this to remove some of the submissions that say they are making $5 million a year or those that are next to nothing
df = df[df['totalyearlycompensation'].between(df['totalyearlycompensation']. \
                                              quantile(.05),df['totalyearlycompensation'].quantile(.95))]

#remove records that are outside of the US. This definition is any location record that has 2 commas or more but keep remote workers
df = df[(df['location'].str.count(',') == 1) | (df['location'].str.contains('remote',flags=re.IGNORECASE, regex=True))]

#change timestampe to date
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['city'] = df['location'].str.split(",").str[0]
df['state'] = df['location'].str[-2:]

#strip any leading or trailing spaces
ob_cols = df.select_dtypes(include=['object']).columns.tolist()

for col in df[ob_cols]:
    df[col] = df[col].str.strip()

#duplicates and fuzzy match company name clean up
company_dict = {'JP Morgan Chase':'JPMorgan Chase','JPMORGAN':'JPMorgan Chase','JP Morgan':'JPMorgan Chase','JPMorgan':'JPMorgan Chase','JP morgan':'JPMorgan Chase',
				'Jp Morgan':'JPMorgan Chase','jp morgan':'JPMorgan Chase', 'Jp morgan chase':'JPMorgan Chase',
				'Ford Motor':'Ford','Ford Motor Company':'Ford',
				'Johnson and Johnson':'Johnson & Johnson',
				'Juniper':'Juniper Networks','juniper':'Juniper Networks',
				'HP':'HP Inc','Hewlett Packard Enterprise':'HPE',
				'Hsbc':'HSBC',
				'Amazon web services':'Amazon',
				'Apple Inc.':'Apple',
				'Bosch Global':'Bosch',
				'Deloitte Advisory':'Deloitte','Deloitte Consulting':'Deloitte','Deloitte consulting':'Deloitte',
				'DISH':'DISH Network','Dish Network':'DISH Network','Dish':'DISH Network',
				'Disney Streaming Services':'Disney','The Walt Disney Company':'Disney',
				'Epic':'Epic Systems',
				'Ernst and Young':'Ernst & Young',
				'Expedia Group':'Expedia',
				'Qualcomm Inc':'Qualcomm',
				'Raytheon Technologies':'Raytheon',
				'MSFT':'Microsoft','Microsoft Corporation':'Microsoft','Msft':'Microsoft','microsoft corporation':'Microsoft',
				'Snapchat':'Snap',
				'Sony Interactive Entertainment':'Sony',
				'Micron':'Micron Technology',
				'Mckinsey & Company':'McKinsey',
				'Jane Street':'Jane Street Capital',
				'EPAM':'EPAM Systems',
				'Costco Wholesale':'Costco',
				'Akamai Technology':'Akamai','Akamai Technologies':'Akamai',
				'Visa inc':'Visa',
				'Wipro Limited':'Wipro',
				'Zoominfo':'Zoom',
				'Zillow Group':'Zillow'}
df['company'] = df['company'].map(company_dict).fillna(df['company'])

#once you have the final dataframe, now it is time to paste it into google sheets
pycred = pygsheets.authorize(service_file='/Users/paul.brown/Documents/Python/credentials.json')
#opening the gsheet and sheet you want to work with
ss = pycred.open_by_key('1CuQDfKALqxxKdYvsudhkRjXelriZktT7QxaDHQGFjeU')[0]
#overwrite what is in the sheet with your df
ss.set_dataframe(df,(1,1))

