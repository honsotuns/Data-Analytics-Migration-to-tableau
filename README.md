# Data Analytics Migration to Tableau
Data Analytics Migration to Tableau project which takes the role of a Data Analyst at Skyscanner that is tasked with ingesting, integrating and cleaning a large dataset containing flight data, loading the cleaned data into a PostgreSQL database, and finally integrating that database with Tableau to create real-time visualisations and dashboards that can be used by top management for strategic decision making.
* Used Pandas to ingest 10 years of flight data containing millions of records each and integrated that data together
* Cleaned the data using various techniques to remove unwanted or incomplete data and saved the output in an integrated CSV file
* Used PostgreSQL on AWS RDS to store the cleaned and transformed data in order to enable data analysis
* Integrated Tableau with the PostgreSQL RDS to visually explore the data and to create visualisations and various real-time charts (bubble, pie, bar and line) using the back-end flights data stored in RDS
* Prepared a presentation to share with top management highlighting the insights and findings observed


1: Setting up the environment

* Created a GitHub repo which was linked to my Vscode

* Setting up AWS


2: Data Wrangling and Cleaning

*  Loaded the csv file into Pandas dataframe

* Turned each csv file into a seperate dataframe. Each method randomly samples 10% of the massively large data, because to my machine memory

* Using Pandas, Printed the numbers of records that contains NULL values

* Removed columns that contains NULL or NA in all of their records

* Replaced any record that still contained NULL with zero

* Integrated the Data by making sure all dataframes have the same number of columns, and each of the column types are the same

* Using pandas, I exported the master dataframe into a file named combined_data.csv

* Copied the file to my AWS S3 bucket using aws s3 cp command

* Downloaded a copy of the file into my local machine from AWS S3



3: PostgreSQL RDS data import and reporting

* From AWS, I copied my endpoint address.

* On my machine, using pgAdmin4 I created a new server called production_server and connected it to my PostgreSQL using the endpoint address

* On the newly created server, I created a database called flights_analytics_database

* Using pgAdmin4, I created a new table called flights

* The table has the same numner of columns and types as combined_data.csv

* Imported the combined_data.csv file into the flights table



4: Integrated Tableau Desktop with PostgreSQL 

* Downloaded and install Tableau Desktop

* Configured the PostgreSQL connector and connected it to the flights_analytics_database RDS



5: Created Tableau reports

* Created a Tableau report for the historical flight origins and destinations

* Created a Tableau report for the average distance travelled

* Created a Tableau report for the most used flight numbers

* Created a Tabeau report for flights delays and cancellation


     * To run: python flights.py



