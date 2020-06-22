#!/usr/bin/env python
# coding: utf-8

# ## Import packages & dependencies

# In[1]:


#!usr/bin/env python3
import os
#import shutil
import pandas as pd
#import numpy as np
import dateutil #https://www.shanelynn.ie/summarising-aggregation-and-grouping-data-in-python-pandas/
from datetime import datetime
import glob
import time
import gc  #garbage collection to free up memory

# Hide warning messages in notebook
import warnings
warnings.filterwarnings('ignore')


# In[2]:


#set some static parameters
debug_mode = 'n'
csv_header_ind = 'True'
cur_dir = os.getcwd()

# print start timestamp 
scriptStartDateTime = datetime.now()
print(scriptStartDateTime)

# ## Load Data

# In[3]:


if debug_mode == 'y':
    data_year = 9999
    print(data_year)
else:
    for i in range(2013,2021):
        data_year = i  
        print(data_year)
    
        # track execution by year
        yearStartDateTime = datetime.now()
        print(f"Starting to process: {data_year} @{yearStartDateTime}")


        # In[6]:


        raw_dir = os.path.join(cur_dir,'citibike_files','raw', str(data_year))


        # In[7]:


        path = raw_dir

        all_files = sorted(glob.glob(os.path.join(path, "*citibike*")))

        all_df = []

        #parse_dates=['starttime','stoptime'],

        for f in all_files:
            try: 
                file_name = f.split('/')[-1]   
                df = pd.read_csv(f, sep=',',header = 1,skiprows = 1, names=['tripduration','starttime','stoptime','start station id','start station name','start station latitude','start station longitude','end station id','end station name','end station latitude','end station longitude','bikeid','usertype','birth year','gender'])
                row_count = (len(df.index))
                print(f"    try: {file_name} rowcount: {row_count}")
                df['a_file'] = file_name    
                all_df.append(df)
                citibike_df = pd.concat(all_df, ignore_index=True, sort=True)
            except:
                print('except:' + f)


        # ## Pre-processing: Preview data & datatype inspection

        # In[8]:


        citibike_df= pd.DataFrame(citibike_df)


        # In[9]:


        citibike_df.dtypes


        # In[10]:


        # set text columns as categories
        for col in ['gender', 'usertype', 'start station name', 'end station name']:
            citibike_df[col] = citibike_df[col].astype('category')


        # In[11]:


        # set datatypes for numeric columns
        citibike_df['start station id'] = citibike_df['start station id'].fillna(0)
        citibike_df['start station id'] = citibike_df['start station id'].astype(str).astype(float).astype(int)
        citibike_df['start station latitude'] = citibike_df['start station latitude'].astype(float)
        citibike_df['start station latitude'] = citibike_df['start station latitude'].round(decimals=3)
        citibike_df['start station longitude'] = citibike_df['start station longitude'].astype(float)
        citibike_df['start station longitude'] = citibike_df['start station longitude'].round(decimals=3)
        #citibike_df = citibike_df.dropna(subset=['end station id'])
        citibike_df['end station id'] = citibike_df['end station id'].fillna(0)
        citibike_df['end station id'] = citibike_df['end station id'].astype(str).astype(float).astype(int)
        citibike_df['end station latitude'] = citibike_df['end station latitude'].astype(float)
        citibike_df['end station latitude'] = citibike_df['end station latitude'].round(decimals=3)
        citibike_df['end station longitude'] = citibike_df['end station longitude'].astype(float)
        citibike_df['end station longitude'] = citibike_df['end station longitude'].round(decimals=3)


        # In[12]:


        citibike_df.dtypes


        # In[13]:


        citibike_df.head()


        # In[14]:


        citibike_df['birth year'].value_counts()


        # In[15]:


        # Using try block here since data files were not consistent over time
        try:
            if pd.api.types.is_string_dtype:
                citibike_df['birth year'] = citibike_df['birth year'].replace({"\\N":2020})
        except:
            print("skip")


        # In[16]:


        citibike_df['birth year'].fillna(2020,inplace=True)


        # In[17]:


        # Now that all fields are prepped drop nans in dataframe.  This is slow.
        citibike_df.dropna(inplace=True)


        # In[18]:


        # Set birth year datatype the nans dropped
        citibike_df['birth year'] = citibike_df['birth year'].astype(str).astype(float).astype(int)


        # In[19]:


        citibike_df.isnull().sum(axis=0)


        # In[20]:


        #stamp the output files yearmonth to track the source of the data
        citibike_df['yearmonth'] =  citibike_df['a_file'].str[:6].astype(int)


        # ## Analyze by date and starthour

        # In[21]:


        citibike_df.dtypes


        # In[22]:

        citibike_df['tripduration'] = citibike_df['tripduration']/60
        citibike_df[['begindate','begintime']] = citibike_df.starttime.str.split(expand=True) 


        # In[23]:


        # Possible optimzation:  https://stackoverflow.com/questions/50744369/how-to-speed-up-pandas-string-function
        # %timeit [x.split('~', 1)[0] for x in df['facility']]
        # def splittime(x):
        #     test = [x.split(' ', 1)[0] for x in citibike_df['starttime']]
        #     return x.map(test)
        # citibike_df['test2'] = splittime(citibike_df['starttime'])
        # TypeError: list indices must be integers or slices, not str   


        # In[24]:


        # https://github.com/pandas-dev/pandas/issues/11665
        def lookup(s):
            """
            This is an extremely fast approach to datetime parsing.
            For large data, the same dates are often repeated. Rather than
            re-parse these, we store all unique dates, parse them, and
            use a lookup to convert all dates.
            """
            dates = {date:pd.to_datetime(date) for date in s.unique()}
            return s.map(dates)


        # In[25]:


        citibike_df['startdate'] = lookup(citibike_df['begindate'])


        # In[26]:


        citibike_df.head()


        # In[27]:


        citibike_df['starthour'] = citibike_df['begintime'].str.slice(0, 2)


        # In[28]:


        daily_df = citibike_df.groupby(['startdate']).tripduration.agg(['count', 'sum']).reset_index().set_index(['startdate'])
        daily_df = pd.DataFrame(daily_df)
        daily_df.sort_index(axis = 0) 
        daily_df


        # In[29]:


        citibike_daily_bike_csv = os.path.join(cur_dir,'citibike_files','cleansed','citibike_trips_daily.csv')


        # In[30]:


        if debug_mode == 'n':
            if not os.path.isfile(citibike_daily_bike_csv):
                daily_df.to_csv(citibike_daily_bike_csv, header='column_names')
            else: # else it exists so append without writing the header
                daily_df.to_csv(citibike_daily_bike_csv, mode='a', header=False)


        # In[31]:


        # Extend analysis tostart hour
        hourly_df = citibike_df.groupby(['startdate', 'starthour']).tripduration.agg(['count', 'sum']).reset_index()
        hourly_df = pd.DataFrame(hourly_df)
        hourly_df.set_index('startdate', inplace=True)
        hourly_df.sort_index(axis = 0) 
        hourly_df.head()


        # In[32]:


        citibike_hourly_csv = os.path.join(cur_dir,'citibike_files','cleansed','citibike_trips_hourly.csv')


        # In[33]:


        if debug_mode == 'n':
            if not os.path.isfile(citibike_hourly_csv):
                hourly_df.to_csv(citibike_hourly_csv, header='column_names')
            else: # else it exists so append without writing the header
                hourly_df.to_csv(citibike_hourly_csv, mode='a', header=False)


        # ## Analyze customers data

        # In[34]:


        citibike_df['gender'].value_counts()


        # In[35]:


        citibike_df['birth year'].value_counts()


        # In[36]:


        currentYear = datetime.now().year


        # In[37]:


        citibike_df['rider age'] = currentYear - citibike_df['birth year']
        citibike_df


        # In[38]:


        bins = [-1,1,18,25,45,65,100,1000]
        citibike_df['age bracket'] = pd.cut(citibike_df['rider age'],bins)
        citibike_df


        # In[39]:


        customers_df = citibike_df.groupby(['startdate','gender','age bracket','usertype']).tripduration.agg(['count']).reset_index()
        customers_df.sort_index(axis = 0) 
        customers_df


        # In[40]:


        citibike_customers_csv = os.path.join(cur_dir,'citibike_files','cleansed','citibike_customers.csv')


        # In[41]:


        if debug_mode == 'n':
            #https://stackoverflow.com/questions/30991541/pandas-write-csv-append-vs-write
            if not os.path.isfile(citibike_customers_csv):
                customers_df.to_csv(citibike_customers_csv, header='column_names', index=False)
            else: # else it exists so append without writing the header
                customers_df.to_csv(citibike_customers_csv, mode='a', header=False, index=False)


        # ## Analyze bike stations

        # In[42]:


        start_stations_df = citibike_df.drop_duplicates(subset=["start station id", "start station latitude","start station longitude","start station name"], keep='first')
        start_stations_df = start_stations_df[["start station id", "start station latitude","start station longitude","start station name"]]
        start_stations_df = pd.DataFrame(start_stations_df)
        start_stations_df.columns = ["station id", "station latitude","station longitude","station name"]


        # In[43]:


        end_stations_df = citibike_df.drop_duplicates(subset=["end station id", "end station latitude","end station longitude","end station name"], keep='first')
        end_stations_df = end_stations_df[["end station id", "end station latitude","end station longitude","end station name"]]
        end_stations_df = pd.DataFrame(end_stations_df)
        end_stations_df.columns = ["station id", "station latitude","station longitude","station name"]


        # In[44]:


        distinct_stations_df = start_stations_df.append(end_stations_df)
        distinct_stations_df = distinct_stations_df.drop_duplicates(subset=["station id", "station latitude","station longitude","station name"], keep='first')
        distinct_stations_df = distinct_stations_df.set_index('station id', inplace=True)
        #distinct_stations_df = distinct_stations_df.sort_index(axis = 0) 


        # In[45]:


        citibike_distinct_station_csv = os.path.join(cur_dir,'citibike_files','cleansed','citibike_distinct_stations_temp.csv')


        # In[46]:


        if debug_mode == 'n':
            if not os.path.isfile(citibike_distinct_station_csv):
                start_stations_df.to_csv(citibike_distinct_station_csv, header='column_names', index=False)
            else: # else it exists so append without writing the header
                start_stations_df.to_csv(citibike_distinct_station_csv, mode='a', header=False, index=False)


        # In[47]:


        start_station_trips_df = citibike_df.groupby(['startdate','start station id']).tripduration.agg(['count']).reset_index()
        start_station_trips_df = start_station_trips_df.set_index(['startdate'])
        start_station_trips_df = start_station_trips_df.sort_index(axis=0)
        start_station_trips_df


        # In[48]:


        citibike_start_station_csv = os.path.join(cur_dir,'citibike_files','cleansed','citibike_start_stations.csv')


        # In[49]:


        if debug_mode == 'n':
            if not os.path.isfile(citibike_start_station_csv):
                start_station_trips_df.to_csv(citibike_start_station_csv, header='column_names')
            else: # else it exists so append without writing the header
                start_station_trips_df.to_csv(citibike_start_station_csv, mode='a', header=False)


        # ## Analyze bike equipment

        # In[50]:


        bike_equipment_df = citibike_df.groupby(['bikeid']).tripduration.agg(['count','sum']).reset_index()
        bike_equipment_df = bike_equipment_df.set_index('bikeid')
        bike_equipment_df = bike_equipment_df.sort_index(axis = 0)
        bike_equipment_df = pd.DataFrame(bike_equipment_df)


        # In[51]:


        bike_df = citibike_df.groupby(['bikeid']).startdate.agg(['min', 'max']).reset_index()
        bike_df = pd.DataFrame(bike_df)
        bike_df = bike_df.set_index(['bikeid'])
        bike_df = bike_df.sort_index(axis = 0)



        # In[52]:


        bike_merged_df = pd.merge(bike_df, bike_equipment_df, left_index=True, right_index=True)


        # In[53]:


        citibike_bike_equipment_csv = os.path.join(cur_dir,'citibike_files','cleansed','citibike_bikes.csv')


        # In[54]:


        if debug_mode == 'n':
            if not os.path.isfile(citibike_bike_equipment_csv):
                bike_merged_df.to_csv(citibike_bike_equipment_csv, header='column_names')
            else: # else it exists so append without writing the header
                bike_merged_df.to_csv(citibike_bike_equipment_csv, mode='a', header=False)


    # ## Cleanup memory for next loop

    # In[55]:


        del [[citibike_df]]
        del [[start_stations_df, end_stations_df, distinct_stations_df]]
        del [[bike_equipment_df, bike_df, bike_merged_df]]
        gc.collect()
        citibike_df = []
        customer_df = []
        distinct_stations_df = []
        start_stations_df = []
        end_stations_df = []
        bike_equipment_df = []
        bike_date_df = []
        bike_merged_df = []


    # In[56]:

        # track execution by year
        yearEndDateTime = datetime.now()
        print(f"Finshed processing: {data_year} @{yearStartDateTime}")


# In[ ]:

# Post-Processing:  Data is processed by year.  Final aggregation needed to produce a list of distinct stations for all years.
citibike_distinct_station_csv = pd.read_csv(os.path.join(cur_dir, 'citibike_files', 'cleansed', 'citibike_distinct_stations_temp.csv'))
citibike_distinct_station_final_df = pd.DataFrame(citibike_distinct_station_csv)
citibike_distinct_station_final_df = citibike_distinct_station_final_df.drop_duplicates(subset=["station id", "station latitude","station longitude","station name"],keep='first')
citibike_distinct_station_final_df = citibike_distinct_station_final_df[["station id", "station latitude","station longitude","station name"]]
citibike_distinct_station_final_df.columns = ["station id", "station latitude","station longitude","station name"]
citibike_distinct_station_final_df.set_index("station id", inplace=True)
citibike_distinct_station_final_df.sort_values("station id", axis = 0, ascending = True, inplace = True, na_position ='first')
citibike_distinct_station_final_df
citibike_distinct_station_final_csv = os.path.join(cur_dir,'citibike_files','cleansed','citibike_distinct_stations.csv')
if debug_mode == 'n':
    if not os.path.isfile(citibike_distinct_station_final_csv):
        citibike_distinct_station_final_df.to_csv(citibike_distinct_station_final_csv, header='column_names')
    else: # else it exists so append without writing the header
        citibike_distinct_station_final_df.to_csv(citibike_distinct_station_final_csv, mode='a', header=False)
if os.path.exists(os.path.join(cur_dir, 'citibike_files', 'cleansed', 'citibike_distinct_stations_temp.csv')):
    os.remove(os.path.join(cur_dir, 'citibike_files', 'cleansed', 'citibike_distinct_stations_temp.csv'))
else:
    print("The citibike_distinct_stations_temp.csv file does not exist")
  
scriptEndDateTime = datetime.now()
print(f"Started script at: {scriptStartDateTime} Finished at: {scriptEndDateTime}")



# %%
