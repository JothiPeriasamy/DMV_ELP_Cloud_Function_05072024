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

import datetime
from google.cloud import bigquery
from google.cloud import storage
import os
import json
import pandas as pd
import boto3
from io import StringIO



def GetWSIModelDenialRecord():
    vAR_client = bigquery.Client()
    vAR_table_name = "run_googleapis_com_stdout"
    
    vAR_sql = f"""
WITH processed_individual_records_date AS (
    SELECT
        DATE as UTC_Date,
        Config,
        ReasonCode,
        Reason,
        Recommendation,
        DATETIME(Timestamp,"America/Los_Angeles") as PST_Timestamp,
        DATE(Timestamp,"America/Los_Angeles") as PST_Date,
        timestamp AS UTC_Timestamp -- Include the timestamp field
    FROM (
        SELECT
            date(timestamp) AS DATE,
            split(split(textPayload, "KeepingLogsIntoBigqueryWithRequest&Response - ")[1], "###")[0] AS Config,
            REPLACE(JSON_EXTRACT(split(textPayload, "###")[1], "$.responseCode"), '\"', '') AS Recommendation,
            REPLACE(JSON_EXTRACT(split(textPayload, "###")[1], "$.reasonCode"), '\"', '') AS ReasonCode,
            REPLACE(JSON_EXTRACT(split(textPayload, "###")[1], "$.reason"), '\"', '') AS Reason,
            timestamp
        FROM `{os.environ["GCP_PROJECT_ID"]}.{os.environ["GCP_BQ_SCHEMA_NAME"]}.{vAR_table_name}`
        WHERE textPayload LIKE '%KeepingLogsIntoBigqueryWithRequest&Response%'
        ORDER BY timestamp DESC
    )
    WHERE Recommendation IS NOT NULL  
    and DATE(Timestamp,"America/Los_Angeles")=CURRENT_DATE("America/Los_Angeles")-1
),
ranked_records AS (
    SELECT
        UTC_Date,
        UTC_Timestamp,
        PST_Date,
        PST_Timestamp,
        Config,
        ReasonCode,
        Reason,
        Recommendation,
        ROW_NUMBER() OVER (PARTITION BY Config ORDER BY PST_Timestamp ASC) AS row_num
    FROM processed_individual_records_date
)
SELECT
    PST_Date,PST_Timestamp,
    Config,
    
FROM (
    SELECT
        UTC_Date,
        UTC_Timestamp,
        PST_Date,
        PST_Timestamp,
        Config,
        ReasonCode,
        Reason,
        Recommendation,
        CASE WHEN UPPER(Recommendation) = 'APPROVED' THEN 'Yes' END AS Approved,
        CASE WHEN UPPER(Recommendation) = 'DENIED' THEN 'Yes' END AS Denied,
        row_num
    FROM ranked_records
) AS subquery
WHERE row_num = 1 and subquery.ReasonCode="MODEL"
ORDER BY PST_DATE DESC,PST_Timestamp
desc, Config;



    """
    vAR_result_df = vAR_client.query(vAR_sql).to_dataframe()

    return vAR_result_df

def DataToCloudStorage():
  vAR_result = GetWSIModelDenialRecord()
  vAR_output_csv = vAR_result.to_csv()
  vAR_bucket_name = os.environ["GCS_BUCKET_NAME"]
  vAR_utc_time = datetime.datetime.utcnow()
  client = storage.Client()
  bucket = client.get_bucket(vAR_bucket_name)
  bucket.blob(os.environ["GCP_REQUEST_PATH"]+'/'+vAR_utc_time.strftime('%Y%m%d')+'/'+vAR_utc_time.strftime("%H:%M:%S")+'_request.csv').upload_from_string(vAR_output_csv, 'text/csv')
  print('WSI Request successfully saved into cloud storage')

def DataToS3():

   try:
      vAR_result = GetWSIModelDenialRecord()
      vAR_utc_time = datetime.datetime.utcnow()
      vAR_bucket_name = os.environ['S3_REQUEST_BUCKET_NAME']
      vAR_csv_buffer = StringIO()
      vAR_result.to_csv(vAR_csv_buffer)
      vAR_s3_resource = boto3.resource('s3',aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
      vAR_s3_resource.Object(vAR_bucket_name, os.environ["AWS_REQUEST_PATH"]+'/'+vAR_utc_time.strftime('%Y%m%d')+'/'+vAR_utc_time.strftime("%H:%M:%S")+'_request.csv').put(Body=vAR_csv_buffer.getvalue())
      print('WSI Request successfully saved into S3 bucket')
   
   except BaseException as exception:
      vAR_exception_message = str(exception)
      print("Error in Upload Response to S3 - ",vAR_exception_message)
      raise Exception("Response Path Error "+vAR_exception_message)

