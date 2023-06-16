# Data Analytics Migration Tableau
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



