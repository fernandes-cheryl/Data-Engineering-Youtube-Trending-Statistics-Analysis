# Data-Engineering-Youtube-Trending-Statistics-Analysis

## Introduction
The objective of this project is to clean, automate, and perform analysis on the structured and semi-structured YouTube trending videos data based on video categories and regions.
The pipeline will retrieve data from the dataset, transform it to a proper format(parquet) in AWS Lambda. A Glue Crawler will be used to create a glue catalog which can be queried in AWS Athena.

## Use Case
1. Companies can display ads in these category videos to promote their product or service and get good returns.
2. A person trying to start their YouTube journey can check the video category doing well in the region.
3. A company can mould ads based on categories liked in certain regions.

## Questions Asked
* Regions in analysis(CA, US, GB):
* Which categories perform well and their statistics
* Based on which factors are the categories performing well or not
* Which channels are the most liked in the respective regions


## Youtube Dataset 

This [dataset]([https://www.google.com](https://www.kaggle.com/datasets/datasnaek/youtube-new)) includes several months (and counting) of data on daily trending YouTube videos. Data is included for the US, GB, DE, CA, FR RU, MX, KR, JP and IN (USA, Great Britain, Germany, Canada, and France, Russia, Mexico, South Korea, Japan and India respectively), with up to 200 listed trending videos per day.
Each region’s data is in separate json files segregated by catgory_id. Data(CSV files) includes the video title, channel title, publish time, tags, views, likes and dislikes, description, and comment count.


## Services Utilized
<b>AWS CLI</b><br>
The AWS Command Line Interface (AWS CLI) is a unified tool to manage your AWS services. 

<b>S3</b><br>
Amazon S3 is object storage built to store and retrieve any amount of data from anywhere.

<b>AWS Lambda</b><br>
AWS Lambda is a serverless, event-driven compute service that lets you run code for virtually any type of application or backend service.

<b>CloudWatch</b><br>
Amazon CloudWatch collects and visualizes real-time logs, metrics, and event data in automated dashboards to streamline your infrastructure and application maintenance.

<b>Glue Crawler</b><br>
A crawler is a job defined in Amazon Glue. It crawls through databases and buckets in S3 and then creates tables in Amazon Glue together with their schema. Then, you can perform your data operations in Glue, like ETL.

<b>Amazon Athena</b><br>
Amazon Athena enables users to analyze data in Amazon S3 using Structured Query Language (SQL).

<b>Quicksight</b><br>
Amazon QuickSight is a cloud-scale business intelligence (BI) service that you can use to deliver easy-to-understand insights to people, wherever they are.


## Architecture
![architecture](https://github.com/cherchub/Data-Engineering-Youtube-Trending-Statistics-Analysis/assets/100081376/a84cbfa6-5523-4639-99e2-261c7e67e0a8)


## Process/ Steps Followed
* Load data from local systems to S3 bucket.
* Build a crawler on json files and give required roles. This results in table created in Athena.
* Give output location(S3) in Athena and run required queries.
* Error encountered since json files are not in SerDe format and hence need to be converted to parqet format.
* AWS Lambda is used to convert json files to parquet while also giving S3 role to store resultant files (At first only one json file is tested).
* Create Lambda layer for additional packages like awswrangler. Also create a glue role.
* Check if json file is converted to parquet in S3 (This will be the cleaned version of the raw file).

* Next, build a crawler on CSV files, the resultant of which is a table created in Athena.
* Cleaned json file can be joined with raw CSV file table.
* CSV file is converted to parquet by creating a job to do the same(cleaned version of csv files).

* Create a trigger that is invoked whenever new json files are added.
* Create a job for joining the cleaned json and csv tables.
* Store the resultant data in an S3 bucket that can be further used for analysis.


## Dashboard using QuickSight
![image](https://github.com/cherchub/Data-Engineering-Youtube-Trending-Statistics-Analysis/assets/100081376/9de0d18d-b535-4589-84e7-faa3ff6d0c8d)


## Future Scope
Use encoding to analyse data from other countries.
