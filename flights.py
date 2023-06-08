from sqlalchemy import create_engine
from sqlalchemy import inspect
from tabula import read_pdf
from scipy.stats import chi2_contingency
from botocore import UNSIGNED
from botocore.client import Config
from math import floor
from scipy import stats
from scipy.stats import normaltest
from scipy.stats import yeojohnson
from plotly.subplots import make_subplots
from plotly.subplots import go
from statsmodels.graphics.gofplots import qqplot
from matplotlib import pyplot
from sklearn.datasets import fetch_california_housing
from functools import reduce
from sklearn.linear_model import LogisticRegression
import tabula
import numpy as np
import psycopg2
import yaml
import pandas as pd
import requests
import json
import time
import boto3 
import io
import os
import random
import numpy as np
import plotly.express as px
import matplotlib
import seaborn as sns
import statsmodels.formula.api as smf
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.style as style
import dask.dataframe as dd


class FlightsProject:
    def __init__(self):
       pass
    
    def flight_year_1987(self):
        df_year_1987 = "1987.csv" 
        n = sum(1 for line in open(df_year_1987))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1987 = pd.read_csv(df_year_1987, skiprows=skip)
        df_year_1987 = df_year_1987.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1987 = df_year_1987.fillna(0)
        df_year_1987[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1987[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1987[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1987[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1987[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1987[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1987[['UniqueCarrier','TailNum']] = df_year_1987[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1987
        
        
    
    def flight_year_1989(self):
        df_year_1989 = "1989.csv" 
        n = sum(1 for line in open(df_year_1989))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1989 = pd.read_csv(df_year_1989, skiprows=skip)
        df_year_1989 = df_year_1989.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1989 = df_year_1989.fillna(0)
        df_year_1989[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1989[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1989[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1989[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1989[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1989[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1989[['UniqueCarrier','TailNum']] = df_year_1989[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1989
      
    
    def flight_year_1990(self):
        df_year_1990 = "1990.csv" 
        n = sum(1 for line in open(df_year_1990))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1990 = pd.read_csv(df_year_1990, skiprows=skip)
        df_year_1990 = df_year_1990.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1990 = df_year_1990.fillna(0)
        df_year_1990[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1990[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1990[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1990[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1990[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1990[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1990[['UniqueCarrier','TailNum']] = df_year_1990[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1990
    
    def flight_year_1991(self):
        df_year_1991 = "1991.csv" 
        n = sum(1 for line in open(df_year_1991))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1991 = pd.read_csv(df_year_1991, skiprows=skip)
        df_year_1991 = df_year_1991.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1991 = df_year_1991.fillna(0)
        df_year_1991[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1991[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1991[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1991[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1991[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1991[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1991[['UniqueCarrier','TailNum']] = df_year_1991[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1991
        
     
    def flight_year_1992(self):
        df_year_1992 = "1992.csv" 
        n = sum(1 for line in open(df_year_1992))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1992 = pd.read_csv(df_year_1992, skiprows=skip)
        df_year_1992 = df_year_1992.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1992 = df_year_1992.fillna(0)
        df_year_1992[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1992[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1992[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1992[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1992[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1992[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1992[['UniqueCarrier','TailNum']] = df_year_1992[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1992
    
    def flight_year_1993(self):
        df_year_1993 = "1993.csv" 
        n = sum(1 for line in open(df_year_1993))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1993 = pd.read_csv(df_year_1993, skiprows=skip)
        df_year_1993 = df_year_1993.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1993 = df_year_1993.fillna(0)
        df_year_1993[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1993[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1993[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1993[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1993[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1993[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1993[['UniqueCarrier','TailNum']] = df_year_1993[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1993
        
    
    def flight_year_1994(self):
        df_year_1994 = "1994.csv" 
        n = sum(1 for line in open(df_year_1994))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1994 = pd.read_csv(df_year_1994, skiprows=skip)
        df_year_1994 = df_year_1994.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1994 = df_year_1994.fillna(0)
        df_year_1994[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1994[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1994[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1994[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1994[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1994[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1994[['UniqueCarrier','TailNum']] = df_year_1994[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1994
        
    
    def flight_year_1995(self):
        df_year_1995 = "1995.csv" 
        n = sum(1 for line in open(df_year_1995))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1995 = pd.read_csv(df_year_1995, skiprows=skip)
        df_year_1995 = df_year_1995.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1995 = df_year_1995.fillna(0)
        df_year_1995[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1995[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1995[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1995[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1995[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1995[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1995[['UniqueCarrier','TailNum']] = df_year_1995[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1995
        
       
    
    def flight_year_1996(self):
        df_year_1996 = "1996.csv" 
        n = sum(1 for line in open(df_year_1996))-1  
        s = n//10  
        skip = sorted(random.sample(range(1, n+1), n-s)) 
        df_year_1996 = pd.read_csv(df_year_1996, skiprows=skip)
        df_year_1996 = df_year_1996.drop(['CancellationCode','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay'],axis=1)
        df_year_1996 = df_year_1996.fillna(0)
        df_year_1996[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']] = df_year_1996[['Year','Month','DayofMonth','DayOfWeek','Origin','Dest']].astype('category')
        df_year_1996[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']] = df_year_1996[['CRSDepTime','CRSArrTime','FlightNum','Distance','TaxiIn','TaxiOut','Cancelled','Diverted']].astype('int64')
        df_year_1996[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']] = df_year_1996[['ArrTime','ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay','DepDelay','DepTime']].astype('float64')
        df_year_1996[['UniqueCarrier','TailNum']] = df_year_1996[['UniqueCarrier','TailNum']].astype('object')
        return df_year_1996
        
    
    def year_combination(self):
        year_1987 = self.flight_year_1987()
        year_1989 = self.flight_year_1989()
        year_1990 = self.flight_year_1990()
        year_1991 = self.flight_year_1991()
        year_1992 = self.flight_year_1992()
        year_1993 = self.flight_year_1993()
        year_1994 = self.flight_year_1994()
        year_1995 = self.flight_year_1995()
        year_1996 = self.flight_year_1996()

        list_df_combination = [year_1987,year_1989,year_1990,year_1991,year_1992,year_1993,year_1994,year_1995,year_1996] 
        df_combination = reduce(lambda x,y: pd.merge(x,y, how="left"),list_df_combination)
        df_combination.to_csv('combined_data.csv', index=False)
       
    
if __name__ == '__main__':       
    FlightsProject().year_combination()




