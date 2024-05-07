"""
-----------------------------------------------------------------------------------------------------------------------------------------------------
© Copyright 2022, California, Department of Motor Vehicle, all rights reserved.
The source code and all its associated artifacts belong to the California Department of Motor Vehicle (CA, DMV), and no one has any ownership
and control over this source code and its belongings. Any attempt to copy the source code or repurpose the source code and lead to criminal
prosecution. Don't hesitate to contact DMV for further information on this copyright statement.

Release Notes and Development Platform:
The source code was developed on the Google Cloud platform using Google Cloud Functions serverless computing architecture. The Cloud
Functions gen 2 version automatically deploys the cloud function on Google Cloud Run as a service under the same name as the Cloud
Functions. The initial version of this code was created to quickly demonstrate the role of MLOps in the ELP process and to create an MVP. Later,
this code will be optimized, and Python OOP concepts will be introduced to increase the code reusability and efficiency.
____________________________________________________________________________________________________________
Development Platform                | Developer       | Reviewer   | Release  | Version  | Date
____________________________________|_________________|____________|__________|__________|__________________
Google Cloud Serverless Computing   | DMV Consultant  | Ajay Gupta | Initial  | 1.0      | 09/18/2022

-----------------------------------------------------------------------------------------------------------------------------------------------------
"""



from google.cloud import bigquery
import pandas as pd
import datetime
import os

def GetMonthToDateLevelStats():

  vAR_client = bigquery.Client()
  vAR_table_name = "run_googleapis_com_stdout"
  vAR_sql =(
      """
(with processed_records_date as (

select  distinct DATE,CONFIG,RECOMMENDATION from(
select date(timestamp) as DATE,

split(split(textPayload,"KeepingLogsIntoBigqueryWithRequest&Response - ")[1],"###")[0] as CONFIG,

REPLACE(JSON_EXTRACT(split(textPayload,"###")[1],"$.responseCode"),'\"',"") as RECOMMENDATION

from `{}.{}.{}`
where textPayload like '%KeepingLogsIntoBigqueryWithRequest&Response%'
and extract(month from timestamp)=extract(MONTH from current_date()) and extract(year from timestamp)=extract(YEAR from current_date())

order by timestamp desc
) where RECOMMENDATION is not null
)



select DATE,
COUNT(*) AS TOTAL_RECORDS,
COUNTIF(upper(RECOMMENDATION)="APPROVED") AS APPROVED,
COUNTIF(upper(RECOMMENDATION)="DENIED") AS DENIED,
concat(cast (round((COUNTIF(upper(RECOMMENDATION)="APPROVED")/COUNT(*))*100,2) as string),"%") as APPROVED_PERCENTAGE,
concat(cast (round((COUNTIF(upper(RECOMMENDATION)="DENIED")/COUNT(*))*100,2) as string),"%") as DENIED_PERCENTAGE
from processed_records_date
group by DATE)

union all

(with date_range AS (
  SELECT DATE '2023-10-01' AS start_date, DATE '2023-10-15' AS end_date
)

SELECT
  cast(FORMAT_DATE('%Y-%m-%d', DATE_ADD(start_date, INTERVAL n DAY)) as date) AS DATE,0 as TOTAL_RECORDS,0 as APPROVED,0 as DENIED,"0%" as APPROVED_PERCENTAGE,"0%" as DENIED_PERCENTAGE
FROM
  date_range
JOIN
  UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
  where extract(month from current_date())=10 and extract(year from current_date())=2023
ORDER BY
  date)

order by date desc

      """.format(os.environ["GCP_PROJECT_ID"],os.environ["GCP_BQ_API_LOGS_SCHEMA"],vAR_table_name)
  )

  vAR_df = vAR_client.query(vAR_sql).to_dataframe()

  return vAR_df


def GetMonthLevelStats():

  vAR_client = bigquery.Client()
  vAR_table_name = "run_googleapis_com_stdout"
  vAR_sql =(
      """
      (with processed_records_month as (

select distinct YEAR,MONTH,MONTH_NUM,CONFIG,RECOMMENDATION from(
select FORMAT_DATETIME("%B",timestamp) as MONTH,extract(year from timestamp) as YEAR,extract (month from timestamp) as MONTH_NUM,

split(split(textPayload,"KeepingLogsIntoBigqueryWithRequest&Response - ")[1],"###")[0] as CONFIG,

REPLACE(JSON_EXTRACT(split(textPayload,"###")[1],"$.responseCode"),'\"',"") as RECOMMENDATION

from `{}.{}.{}`
where textPayload like '%KeepingLogsIntoBigqueryWithRequest&Response%'  

order by timestamp desc
) where RECOMMENDATION is not null
)

select YEAR,MONTH,MONTH_NUM,
COUNT(*) AS TOTAL_RECORDS,
COUNTIF(upper(RECOMMENDATION)="APPROVED") AS APPROVED,
COUNTIF(upper(RECOMMENDATION)="DENIED") AS DENIED,
concat(cast (round((COUNTIF(upper(RECOMMENDATION)="APPROVED")/COUNT(*))*100,2) as string),"%") as APPROVED_PERCENTAGE,
concat(cast (round((COUNTIF(upper(RECOMMENDATION)="DENIED")/COUNT(*))*100,2) as string),"%") as DENIED_PERCENTAGE
from processed_records_month
group by YEAR,MONTH,MONTH_NUM)

union all

(select 2023 as YEAR,"September" as MONTH,9 as MONTH_NUM,0 as TOTAL_RECORDS,0 as APPROVED,0 AS DENIED,"0%" as APPROVED_PERCENTAGE,"0%" as DENIED_PERCENTAGE)

union all

(select 2023 as YEAR,"August" as MONTH,8 as MONTH_NUM,0 as TOTAL_RECORDS,0 as APPROVED,0 AS DENIED,"0%" as APPROVED_PERCENTAGE,"0%" as DENIED_PERCENTAGE)

union all

(select 2023 as YEAR,"July" as MONTH,7 as MONTH_NUM,0 as TOTAL_RECORDS,0 as APPROVED,0 AS DENIED,"0%" as APPROVED_PERCENTAGE,"0%" as DENIED_PERCENTAGE)

order by MONTH_NUM desc



      """.format(os.environ["GCP_PROJECT_ID"],os.environ["GCP_BQ_API_LOGS_SCHEMA"],vAR_table_name)
  )

  vAR_df = vAR_client.query(vAR_sql).to_dataframe()

  return vAR_df




def GetOrderLevelStats(date):

  

  vAR_client = bigquery.Client()
  vAR_table_name = "run_googleapis_com_stdout"

  if len(date)==0:
    vAR_sql = """
with processed_individual_records_date as (

select  distinct DATE,Config,Reason,Recommendation from(
select date(timestamp) as DATE,

split(split(textPayload,"KeepingLogsIntoBigqueryWithRequest&Response - ")[1],"###")[0] as Config,

REPLACE(JSON_EXTRACT(split(textPayload,"###")[1],"$.responseCode"),'\"',"") as Recommendation,
REPLACE(JSON_EXTRACT(split(textPayload,"###")[1],"$.reasonCode"),'\"',"") as Reason

from `{}.{}.{}`
where textPayload like '%KeepingLogsIntoBigqueryWithRequest&Response%'
and extract(month from timestamp)=extract(MONTH from current_date()) and extract(year from timestamp)=extract(YEAR from current_date())

order by timestamp desc,Config
) where Recommendation is not null
)


select DATE,CONFIG,
case when Approved is null and Denied is not null then "No" else Approved end as Approved,
case when Denied is null and Approved is not null then "No" else Denied end as Denied,
Reason from(

select DATE,CONFIG,
case when upper(Recommendation) = "APPROVED" then "Yes" end as Approved,
case when upper(Recommendation) = "DENIED" then "Yes" end as Denied,
Reason
from processed_individual_records_date) where DATE=current_date()-1;

      """.format(os.environ["GCP_PROJECT_ID"],os.environ["GCP_BQ_API_LOGS_SCHEMA"],vAR_table_name)

  else:

    vAR_sql = """
with processed_individual_records_date as (

select  distinct DATE,Config,Reason,Recommendation from(
select date(timestamp) as DATE,

split(split(textPayload,"KeepingLogsIntoBigqueryWithRequest&Response - ")[1],"###")[0] as Config,

REPLACE(JSON_EXTRACT(split(textPayload,"###")[1],"$.responseCode"),'\"',"") as Recommendation,
REPLACE(JSON_EXTRACT(split(textPayload,"###")[1],"$.reasonCode"),'\"',"") as Reason

from `{}.{}.{}`
where textPayload like '%KeepingLogsIntoBigqueryWithRequest&Response%'
and extract(month from timestamp)=extract(MONTH from current_date()) and extract(year from timestamp)=extract(YEAR from current_date())

order by timestamp desc,Config
) where Recommendation is not null
)


select DATE,CONFIG,
case when Approved is null and Denied is not null then "No" else Approved end as Approved,
case when Denied is null and Approved is not null then "No" else Denied end as Denied,
Reason from(

select DATE,CONFIG,
case when upper(Recommendation) = "APPROVED" then "Yes" end as Approved,
case when upper(Recommendation) = "DENIED" then "Yes" end as Denied,
Reason
from processed_individual_records_date) where DATE='{}';

      """.format(os.environ["GCP_PROJECT_ID"],os.environ["GCP_BQ_API_LOGS_SCHEMA"],vAR_table_name,date)

  vAR_df = vAR_client.query(vAR_sql).to_dataframe()

  return vAR_df




def ProcessAPIStats(date):

  vAR_month_level_df = GetMonthLevelStats()
  vAR_mtd_df = GetMonthToDateLevelStats()
  vAR_order_level_df = GetOrderLevelStats(date)

  vAR_month_level_list = []

  
  for idx,row in vAR_month_level_df.iterrows():
    vAR_month_level_dict = {}


    vAR_month_level_dict["year"] = row["YEAR"]
    vAR_month_level_dict["month"] = row["MONTH"]
    vAR_month_level_dict["totalRecords"] = row["TOTAL_RECORDS"]
    vAR_month_level_dict["approved"] = row["APPROVED"]
    vAR_month_level_dict["denied"] = row["DENIED"]
    vAR_month_level_dict["approvedPercentage"] = row["APPROVED_PERCENTAGE"]
    vAR_month_level_dict["deniedPercentage"] = row["DENIED_PERCENTAGE"]

    vAR_month_level_list.append(vAR_month_level_dict)


  vAR_day_level_list = []

  
  for idx,row in vAR_mtd_df.iterrows():
    vAR_day_level_dict = {}


    vAR_day_level_dict["date"] = row["DATE"]
    vAR_day_level_dict["totalRecords"] = row["TOTAL_RECORDS"]
    vAR_day_level_dict["approved"] = row["APPROVED"]
    vAR_day_level_dict["denied"] = row["DENIED"]
    vAR_day_level_dict["approvedPercentage"] = row["APPROVED_PERCENTAGE"]
    vAR_day_level_dict["deniedPercentage"] = row["DENIED_PERCENTAGE"]


    vAR_day_level_list.append(vAR_day_level_dict)


  vAR_order_level_list = []

  
  for idx,row in vAR_order_level_df.iterrows():
    vAR_order_level_dict = {}


    vAR_order_level_dict["date"] = row["DATE"]
    vAR_order_level_dict["config"] = row["CONFIG"]
    vAR_order_level_dict["approved"] = row["Approved"]
    vAR_order_level_dict["denied"] = row["Denied"]
    vAR_order_level_dict["reason"] = row["Reason"]


    vAR_order_level_list.append(vAR_order_level_dict)

  return {"dayLevel(MTD)":vAR_day_level_list,"monthLevel":vAR_month_level_list,"orderLevel":vAR_order_level_list}

  

