# Data Analytics Migration Tableau
1 Setting up the environment
1.1 Created a GitHub repo which was linked to my Vscode
1.2 Setting up AWS

2 Data Wrangling and Cleaning
2.1  Loaded the csv file into Pandas dataframe
2.2 Turned each csv file into a seperate dataframe. Each method randomly samples 10% of the massively large data, because to my machine memory
2.3 Using Pandas, Printed the numbers of records that contains NULL values
2.4 Removed columns that contains NULL or NA in all of their records
2.5 Replaced any record that still contained NULL with zero
2.6 Integrated the Data by making sure all dataframes have the same number of columns, and each of the column types are the same
2.7 Using pandas, I exported the master dataframe into a file named combined_data.csv
2.8 Copied the file to my AWS S3 bucket using aws s3 cp command
2.9 Downloaded a copy of the file into my local machine from AWS S3

3 PostgreSQL RDS data import and reporting
3.1 From AWS, I copied my endpoint address.
3.2 On my machine, using pgAdmin4 I created a new server called production_server and connected it to my PostgreSQL using the endpoint address
3.3 On the newly created server, I created a database called flights_analytics_database
3.4 Using pgAdmin4, I created a new table called flights
3.5 The table has the same numner of columns and types as combined_data.csv
3.6 Imported the combined_data.csv file into the flights table

4 Integrated Tableau Desktop with PostgreSQL 
4.1 Downloaded and install Tableau Desktop
4.2 Configured the PostgreSQL connector and connected it to the flights_analytics_database RDS

5 Created Tableau reports
5.1 Created a Tableau report for the historical flight origins and destinations
5.2 Created a Tableau report for the average distance travelled
5.3 Created a Tableau report for the most used flight numbers
5.4 Created a Tabeau report for flights delays and cancellation



