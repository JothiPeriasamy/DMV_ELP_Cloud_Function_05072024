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


def PreProcessingReport(vAR_given_date_request_count,vAR_given_date_response_count,vAR_query_request_df_len):
    print('******************PREPROCESSING REPORT****************************')
    print('TOTAL ORDERS IN REQUEST FILE FOR GIVEN DATE (AWS) FROM METADATA TABLE - ',vAR_given_date_request_count)
    print('TOTAL ORDERS IN RESPONSE TABLE FOR GIVEN DATE - ',vAR_given_date_response_count)
    print('TOTAL ORDERS IN REQUEST FILE FOR GIVEN DATE  FROM REQUEST TABLE(NOT PROCESSED) - ',vAR_query_request_df_len)




def GetNotProcessedRecord(vAR_partial_file_date,vAR_partial_file_name):
    vAR_client = bigquery.Client()
    vAR_request_table_name = "DMV_ELP_REQUEST"
    vAR_response_table_name = "DMV_ELP_MLOPS_RESPONSE"
    
    vAR_request_sql  = "select distinct LICENSE_PLATE_CONFIG FROM `"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_request_table_name +"` where REQUEST_DATE='"+vAR_partial_file_date+ "' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"
    vAR_response_sql = "SELECT distinct LICENSE_PLATE_CONFIG from `"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_response_table_name+"` where REQUEST_DATE='"+vAR_partial_file_date+ "' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"

    print('GetNotProcessedRecordFromRequestTable QUERY - ',vAR_request_sql)
    print('GetProcessedRecordFromResponseTable QUERY2 - ',vAR_response_sql)
    vAR_query_request_df = vAR_client.query(vAR_request_sql).to_dataframe()
    vAR_query_response_df = vAR_client.query(vAR_response_sql).to_dataframe()

    vAR_query_request_df_len = len(vAR_query_request_df)
    vAR_query_response_df_len = len(vAR_query_response_df)


    print('LEN vAR_query_request_df - ',vAR_query_request_df_len)
    print('LEN vAR_query_response_df - ',vAR_query_response_df_len)

    

    return vAR_query_request_df,vAR_query_response_df,vAR_query_request_df_len,vAR_query_response_df_len



def ReadRequestFileFromGCP(vAR_partial_file_date,vAR_partial_file_name):
    

    vAR_storage_client = storage.Client(project=os.environ["GCP_PROJECT_ID"])
    vAR_bucket = vAR_storage_client.get_bucket(os.environ["GCS_BUCKET_NAME"])
    vAR_prefix = os.environ["GCP_REQUEST_PATH"]+"/"+vAR_partial_file_date.replace('-','')+"/"
    print('vAR_prefix - ',vAR_prefix)
    for blob in vAR_bucket.list_blobs(prefix=vAR_prefix):
        print('BLOB - ',blob)
        print('BLOB NAME - ',blob.name)
        if blob.name.endswith(vAR_partial_file_name):
            vAR_request_object_name = "gs://"+os.environ["GCS_BUCKET_NAME"]+"/"+blob.name
            vAR_gcs_request_file_df=  pd.read_csv(vAR_request_object_name)
        else:
            print('No Request file found in GCS bucket with given date&filename')
    return vAR_gcs_request_file_df


def GetResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name):
    vAR_response_count = 0
    vAR_run_id = 0
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_MLOPS_RESPONSE"
    vAR_query = " SELECT count(distinct LICENSE_PLATE_CONFIG) as cnt,RUN_ID FROM "+ "`"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+  " where REQUEST_DATE = '"+vAR_partial_file_date+"'"+" and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"' group by RUN_ID"
    vAR_query_job = vAR_client.query(vAR_query)
    print('Response count Query - ',vAR_query)
    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_response_count = row.get('cnt')
        vAR_run_id = row.get('RUN_ID')
    if vAR_run_id==0:
        print('Assigining run id as 0, since run id not found for given file&date')
    return vAR_response_count,vAR_run_id


def GetPartialResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name):
    vAR_response_count = 0
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_MLOPS_RESPONSE"
    vAR_query_job = vAR_client.query(
        " SELECT count(distinct LICENSE_PLATE_CONFIG) as cnt FROM "+ "`"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+  " where REQUEST_DATE = '"+vAR_partial_file_date+"'"+" and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"' and date(CREATED_DT)=current_date()"
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_response_count = row.get('cnt')
    
    return vAR_response_count




def GetMetadataTotalRecordsToProcess(vAR_partial_file_date,vAR_partial_file_name):
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_REQUEST_RESPONSE_METADATA"
    vAR_sql = " SELECT TOTAL_NUMBER_OF_ORDERS FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`" + " where date(created_dt) = '"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"
    vAR_query_job = vAR_client.query(vAR_sql)
    print('GetMetadataTotalRecordsToProcess QUERY - ',vAR_sql)
    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_total_orders = row.get('TOTAL_NUMBER_OF_ORDERS')
    return vAR_total_orders



def GetRequestCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name):
    vAR_request_count = 0
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_REQUEST"
    vAR_query_job = vAR_client.query(
        " SELECT count(1) as cnt FROM "+ "`"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+  " where REQUEST_DATE = '"+vAR_partial_file_date+"'"+" and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_request_count = row.get('cnt')
    return vAR_request_count

def GetErrorTableCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name):
    vAR_error_count = 0
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_ERROR_LOG"
    vAR_query_job = vAR_client.query(
        " SELECT count(1) as cnt FROM "+ "`"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+  " where date(created_dt) = '"+vAR_partial_file_date+"'"+" and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_error_count = row.get('cnt')
    print('Error table count = ',vAR_error_count)
    return vAR_error_count


def ReadNotProcessedRequestData(vAR_partial_file_date,vAR_partial_file_name):
    vAR_client = bigquery.Client()
    vAR_request_table_name = "DMV_ELP_REQUEST"
    vAR_sql =(
        "select * from `"+ os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_request_table_name+"`"+" where REQUEST_DATE='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name +"' order by REQUEST_ID"
    )

    vAR_df = vAR_client.query(vAR_sql).to_dataframe()
    return vAR_df

def ReadPartialResponseTable(vAR_partial_file_date,vAR_partial_file_name):
    vAR_client = bigquery.Client()
    vAR_response_table_name = "DMV_ELP_MLOPS_RESPONSE"
    # RUN_ID no need in where clause. Because,request file name will be unique for respective response records
    vAR_sql =(
        """
        SELECT * FROM `{}.{}.{}`
WHERE
      DATE(created_dt)=CURRENT_DATE() and MODEL IS NOT NULL AND REQUEST_DATE='{}' AND lower(REQUEST_FILE_NAME) like '%{}'
""".format(os.environ["GCP_PROJECT_ID"],os.environ["GCP_BQ_SCHEMA_NAME"],vAR_response_table_name,vAR_partial_file_date,vAR_partial_file_name)
    )
    vAR_df = vAR_client.query(vAR_sql).to_dataframe()
    return vAR_df


def ReadResponseTable(vAR_partial_file_date,vAR_partial_file_name):
    vAR_client = bigquery.Client()
    vAR_response_table_name = "DMV_ELP_MLOPS_RESPONSE"
    # RUN_ID no need in where clause. Because,request file name will be unique for respective response records
    vAR_sql =(
        """
        SELECT * FROM `{}.{}.{}`
WHERE
      DATE(created_dt)=CURRENT_DATE() and MODEL IS NOT NULL AND REQUEST_DATE='{}' AND lower(REQUEST_FILE_NAME) like '%{}'
""".format(os.environ["GCP_PROJECT_ID"],os.environ["GCP_BQ_SCHEMA_NAME"],vAR_response_table_name,vAR_partial_file_date,vAR_partial_file_name)
    )

    print('response file query - ',vAR_sql)

    vAR_df = vAR_client.query(vAR_sql).to_dataframe()

    #  Remove Duplicate If there's any
    vAR_newdf = vAR_df.drop_duplicates(subset = ['LICENSE_PLATE_CONFIG'],keep = 'last').reset_index(drop = True)
    return vAR_newdf


def PostProcessingReport(vAR_partial_file_date,vAR_partial_file_name):
    print("*****************POST PROCESSING REPORT**************************** ")
    vAR_total_records = GetMetadataTotalRecordsToProcess(vAR_partial_file_date,vAR_partial_file_name)
    vAR_response_count,vAR_run_id = GetResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
    vAR_request_count = GetRequestCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
    print("TOTAL ORDERS IN REQUEST FILE - ",vAR_total_records)
    print("TOTAL ORDERS IN RESPONSE TABLE - ",vAR_response_count)
    print("TOTAL ORDERS IN REQUEST TABLE - ",vAR_request_count)





def DeleteErrorTableRecords(vAR_partial_file_date,vAR_partial_file_name):
   vAR_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])

   vAR_request_table = "DMV_ELP_ERROR_LOG"

   vAR_query_delete = " delete from `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_request_table+"`" +" where  date(created_dt) = '"+vAR_partial_file_date+"'"+" and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"

   vAR_job = vAR_client.query(vAR_query_delete)
   vAR_job.result()
   print("Number of error table records deleted(after inserted into request table) - ",vAR_job.num_dml_affected_rows)



def GetRequestFilePath(vAR_partial_file_date,vAR_partial_file_name):
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_REQUEST_RESPONSE_METADATA"
    vAR_query_job = vAR_client.query(
        " SELECT REQUEST_FILE_NAME FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`" + " where date(created_dt) ='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_request_file_name = row.get('REQUEST_FILE_NAME')
    return vAR_request_file_name


def GetPreviousdayRequestCount(vAR_partial_file_date,vAR_partial_file_name):
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_REQUEST"
    vAR_query_job = vAR_client.query(
        " SELECT count(1) as cnt FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`" + " where date(created_dt) ='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"' and date(REQUEST_DATE)='"+vAR_partial_file_date+"'"
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_request_prev_count = row.get('cnt')
    return vAR_request_prev_count


def UpdateMetadataTable(vAR_partial_file_date,vAR_partial_file_name):
   vAR_num_of_updated_row =0
   vAR_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
   vAR_table_name = "DMV_ELP_REQUEST_RESPONSE_METADATA"



   vAR_query_list = ["UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN1 ='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 is NULL AND RUN2 is NULL AND RUN3 is NULL AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN1 ='COMPLETE',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='IN PROGRESS' AND RUN2 is NULL AND RUN3 is NULL AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN2='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER'  WHERE RUN1='COMPLETE' AND RUN2 is NULL AND RUN3 is NULL AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN2 ='COMPLETE',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1='COMPLETE' AND RUN2 ='IN PROGRESS' AND RUN3 is NULL AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN3='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='COMPLETE' AND RUN2 ='COMPLETE' AND RUN3 is NULL AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN3 ='COMPLETE',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='COMPLETE'  AND RUN2 ='COMPLETE' AND RUN3 ='IN PROGRESS' AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN4='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='COMPLETE' AND RUN2 ='COMPLETE' AND RUN3 ='COMPLETE' AND RUN4 is NULL AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN4 ='COMPLETE',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='COMPLETE'  AND RUN2 ='COMPLETE' AND RUN3 ='COMPLETE' AND RUN4 = 'IN PROGRESS' AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN5='IN PROGRESS',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='COMPLETE' AND RUN2 ='COMPLETE' AND RUN3 ='COMPLETE' AND RUN4= 'COMPLETE' AND RUN5 is NULL and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'",

"UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+" SET RUN5 ='COMPLETE',UPDATED_DT=CURRENT_DATETIME(),UPDATED_USER='AWS_LAMBDA_USER' WHERE RUN1 ='COMPLETE'  AND RUN2 ='COMPLETE' AND RUN3 ='COMPLETE' AND RUN4 = 'COMPLETE' AND RUN5 ='IN PROGRESS' and date(CREATED_DT)='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"]

   for query in vAR_query_list:

      vAR_job = vAR_client.query(query)
      vAR_job.result()
      vAR_num_of_updated_row = vAR_job.num_dml_affected_rows
      if vAR_num_of_updated_row==1:
         print('Metadata table update query executed - ',query)
         break
      else:
         print('Metadata table not updated')






# def GetMetadataLatestRecordTimeDiff():
#     vAR_client = bigquery.Client()
#     vAR_time_diff = 0
#     vAR_table_name = "DMV_ELP_REQUEST_RESPONSE_METADATA"
#     vAR_query_job = vAR_client.query(
#         " SELECT  TIMESTAMP_DIFF(current_timestamp(), timestamp(updated_dt), MINUTE) as minutes_different FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`  order by created_dt desc limit 1"
#     )

#     vAR_results = vAR_query_job.result()  # Waits for job to complete.
#     print('GetMetadataLatestRecordTimeDiff - ',vAR_results)
#     for row in vAR_results:
#         vAR_time_diff = row.get('minutes_different')
#     return vAR_time_diff



def GetMetadataLatestRecordTimeDiff(vAR_partial_file_date,vAR_partial_file_name):
    vAR_client = bigquery.Client()
    vAR_inprogress_cnt = 0
    vAR_table_name = "DMV_ELP_REQUEST_RESPONSE_METADATA"
    vAR_query_job = vAR_client.query(
        " SELECT  * FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`  where date(CREATED_DT)='"+vAR_partial_file_date+"' and (RUN1='IN PROGRESS' or RUN2='IN PROGRESS' or RUN3='IN PROGRESS' or RUN4='IN PROGRESS' or RUN5='IN PROGRESS')  order by created_dt desc limit 1"
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    print('GetMetadataLatestRecordTimeDiff - ',vAR_results)
    for row in vAR_results:
        vAR_inprogress_cnt +=1
        print('vAR_inprogress_cnt - ',vAR_inprogress_cnt)
    return vAR_inprogress_cnt