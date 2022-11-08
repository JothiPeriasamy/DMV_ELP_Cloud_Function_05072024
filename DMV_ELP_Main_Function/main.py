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

import pandas as pd
import numpy as np
import requests
import json
import datetime
import time
import traceback
import  multiprocessing as mp
import sys
from google.cloud import storage
import os
from datetime import date
import boto3

from DMV_ELP_Request_Upload_To_GCS import Upload_Request_GCS

from DMV_ELP_Response_To_S3 import Upload_Response_To_S3
from DMV_ELP_Response_To_Bigquery import Insert_Response_to_Bigquery,ReadResponseTable
from DMV_ELP_Request_To_Bigquery import Insert_Request_To_Bigquery
from DMV_ELP_Bigquery_Request_Validation import GetCurrentDateRequestCount,ReadNotProcessedRequestData,InsertRequesResponseMetaData,GetMetadataTotalRecordsToProcess,GetCurrentDateResponseCount,GetRequestFileName
from DMV_ELP_Request_Delete import DeleteProcessedConfigs
from DMV_ELP_Update_Metadata_Table import UpdateMetadataTable
from DMV_ELP_Update_ErrorLog import InsertErrorLog
from DMV_ELP_Process_Request import Process_ELP_Request

from DMV_ELP_Response_To_GCS import Upload_Response_GCS
from DMV_ELP_Response_To_S3 import Upload_Response_To_S3
from DMV_ELP_Request_To_AWS_Processed import Move_Request_AWS_Processed
from DMV_ELP_Audit_Log import Insert_Audit_Log


def Process_ELP_Orders(request):

   vAR_process_start_time = datetime.datetime.now().replace(microsecond=0)
   vAR_timeout_start = time.time()
   vAR_last_processed_record = ''
   
   vAR_gcs_client = storage.Client()
   vAR_bucket = vAR_gcs_client.get_bucket(os.environ['GCS_BUCKET_NAME'])
   vAR_utc_time = datetime.datetime.utcnow()
   blob = vAR_bucket.blob(os.environ['RUNTIME_STATS_PATH']+vAR_utc_time.strftime('%Y%m%d')+'/'+vAR_utc_time.strftime('%H%M%S')+'.txt')
   vAR_processed_configs = []
   
   with blob.open(mode='w') as f:
      try:
         
         vAR_request_json = request.get_json(silent=True)
         
         vAR_s3_url = "s3://"
         vAR_timeout_secs = int(os.environ['TIMEOUT_SECS'])
         pool = mp.Pool(mp.cpu_count())

         vAR_request_url = os.environ['REQUEST_URL']
         vAR_output = pd.DataFrame()
         vAR_headers = {'content-type': 'application/json','user-agent': 'Mozilla/5.0'}
         
         vAR_error_message = ""
         vAR_current_date_request_count = GetCurrentDateRequestCount()
         
         vAR_current_date_response_count = GetCurrentDateResponseCount()

         if vAR_current_date_request_count==0 and vAR_current_date_response_count==0:
            vAR_s3_request_path = RequestFileValidation()
            if len(vAR_s3_request_path)>0:
               vAR_s3_url = vAR_s3_url+os.environ["S3_REQUEST_BUCKET_NAME"]+'/'+vAR_s3_request_path
               print('VAR S3 Request URL - ',vAR_s3_url)
            else:
               vAR_err_message = "No valid csv request file found in path - "+vAR_s3_url+os.environ["S3_REQUEST_BUCKET_NAME"]+'/'+os.environ["AWS_REQUEST_PATH"]
               raise Exception(vAR_err_message)

            vAR_batch_elp_configuration_raw = pd.read_csv(vAR_s3_url,delimiter=';')
            vAR_batch_elp_configuration = PreProcessRequest(vAR_batch_elp_configuration_raw)
            vAR_number_of_configuration = len(vAR_batch_elp_configuration)
            Insert_Request_To_Bigquery(vAR_batch_elp_configuration,vAR_number_of_configuration)
            InsertRequesResponseMetaData(vAR_number_of_configuration,vAR_s3_url)

         vAR_configuration_df = ReadNotProcessedRequestData()
         vAR_configuration_df_len = len(vAR_configuration_df)

         print('There are '+str(vAR_configuration_df_len)+' configurations yet to be processed')

         if vAR_current_date_request_count==0 and vAR_configuration_df_len>0:
            Upload_Request_GCS(vAR_batch_elp_configuration)
            Move_Request_AWS_Processed(vAR_s3_request_path)
         
         UpdateMetadataTable()


         

         f.write('Start Time - {}\n\n'.format(vAR_process_start_time))
         f.write('Order No\t\t\t Start Time\t\t\t End Time\t\t\t Total Time\n')
         


         vAR_output_result_objects = [pool.apply_async(Process_ELP_Request,args=(vAR_configuration_df,elp_idx,vAR_request_url,vAR_headers)) for elp_idx in range(vAR_configuration_df_len)]

         vAR_results = []
         for vAR_result in vAR_output_result_objects:
            print('Time taking in for loop - ',time.time()-vAR_timeout_start)
            if (time.time()-vAR_timeout_start)<vAR_timeout_secs:
               f.write(vAR_result.get()["Process Time"])
               
               if len(vAR_result.get()['ERROR_MESSAGE'])>0:
                  InsertErrorLog(vAR_result.get())
                  vAR_output = vAR_output.append(vAR_result.get(),ignore_index=True)
                  vAR_last_processed_record = vAR_result.get()['LICENSE_PLATE_CONFIG']
                  
               if 'Process Time' in vAR_result.get().keys():
                  del vAR_result.get()['Process Time']
               vAR_results.append(vAR_result.get())
               Insert_Response_to_Bigquery(pd.DataFrame(vAR_result.get(),index=[0]))
               print(vAR_result.get()['LICENSE_PLATE_CONFIG']+' Inserted into response table')
               DeleteProcessedConfigs()
               print(vAR_result.get()['LICENSE_PLATE_CONFIG']+' deleted from request table')
               vAR_processed_configs.append(vAR_result.get()['LICENSE_PLATE_CONFIG'])
               vAR_output = vAR_output.append(vAR_result.get(),ignore_index=True)
               vAR_last_processed_record = vAR_result.get()['LICENSE_PLATE_CONFIG']
                  
            else:
               raise TimeoutError('Timeout Error inside result iteration')
            
            

                     
         # Close Pool and let all the processes complete
         pool.close()
         # postpones the execution of next line of code until all processes in the queue are done.
         pool.join()  

         

         vAR_records_to_process = GetMetadataTotalRecordsToProcess()
         vAR_response_count = GetCurrentDateResponseCount()
         print('records to process - ',vAR_records_to_process)
         print('response_count - ', vAR_response_count)
         if vAR_records_to_process==vAR_response_count and vAR_configuration_df_len!=0:
            vAR_output_copy = ReadResponseTable()
            vAR_output_csv = vAR_output_copy.to_csv()
            
            # Upload response to GCS bucket
            Upload_Response_GCS(vAR_output_csv)

            # Upload response to S3 bucket
            ResponsePathValidation(vAR_output_copy,vAR_records_to_process,vAR_response_count)

            
            
         


         vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
         vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time


         f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
         return 'ELP Configurations Successfully Processed'

      except TimeoutError as timeout:
         print('TIMEOUTERR - Custom Timeout Error')
         
         # Since custom timeout occurs, checking for response count in response table and copy resullts to gcs&s3
         vAR_records_to_process = GetMetadataTotalRecordsToProcess()
         vAR_response_count = GetCurrentDateResponseCount()
         print('records to process - ',vAR_records_to_process)
         print('response_count - ', vAR_response_count)
         if vAR_records_to_process==vAR_response_count:
            vAR_output_copy = ReadResponseTable()
            vAR_output_csv = vAR_output.to_csv()
            
            # Upload response to GCS bucket
            Upload_Response_GCS(vAR_output_csv)

            # Upload response to S3 bucket
            ResponsePathValidation(vAR_output_copy,vAR_records_to_process,vAR_response_count)

            
            

         vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
         vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time
         f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
         print('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
         # If we try with pool.terminate() after this return statement not executing
         # time.sleep(2)
         # pool.terminate()
         # print('Pool terminated')
         print('Number of Processed configs - ',len(vAR_processed_configs))

         

         return {'Error Message':'### Custom Timeout Error Occured'}

      
      
      except BaseException as e:
         print('BASEEXCEPTIONERR - '+str(e))
         print('Error Traceback - '+str(traceback.print_exc()))
         vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
         vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time
         f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
         print('Number of Processed configs - ',len(vAR_processed_configs))
         vAR_err_response_dict = {}
         vAR_err_response_dict['ERROR_MESSAGE'] = str(e)
         InsertErrorLog(vAR_err_response_dict)

         if "Response Path Error " in str(e):
            print("Response path error in main - ",str(e))
            # Audit trial log
      
            vAR_audit_log_df = pd.DataFrame()

            vAR_s3_url = GetRequestFileName()
            vAR_audit_log_df["AUDIT_DATE"] = 1*[date.today()]
            vAR_audit_log_df["REQUEST_DATE"] = 1*[date.today()]
            vAR_audit_log_df["TOTAL_ELP_ORDERS_IN_REQUEST"] = 1*[vAR_records_to_process]
            vAR_audit_log_df["TOTAL_ELP_ORDERS_IN_RESPONSE"] = 1*[vAR_response_count]
            vAR_audit_log_df["TOTAL_ELP_ORDER_PROCESSED"] = 1*[vAR_response_count] 
            vAR_audit_log_df["REQUEST_FILE_NAME"] = 1*[vAR_s3_url]
            vAR_audit_log_df["SYSTEM_ENVIRONMENT"] = 1*[os.environ["SYSTEM_ENVIRONMENT"]]

            vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["NO"]
            vAR_audit_log_df["ERROR_MESSAGE"] = str(e)
            Insert_Audit_Log(vAR_audit_log_df)
            print("Audit Log Table Inserted")

         return {'Error Message':'### '+str(e)}

      
def PreProcessRequest(vAR_batch_elp_configuration_raw):

   vAR_number_of_configuration = len(vAR_batch_elp_configuration_raw)
   # Drop unncessary column from request file
   if 'Unnamed: 16' in vAR_batch_elp_configuration_raw:
      vAR_batch_elp_configuration_raw = vAR_batch_elp_configuration_raw.drop(['Unnamed: 16'],axis=1)
   vAR_batch_elp_configuration_raw.insert(0,'REQUEST_ID',np.arange(1,vAR_number_of_configuration+1))
   vAR_batch_elp_configuration_raw.insert(1,'REQUEST_DATE',vAR_number_of_configuration*[date.today()])

   # Except TYPE,VIN all other column names are renamed
   vAR_batch_elp_configuration_raw.rename(columns = {'LICENSE PLATE CONFIG.':'LICENSE_PLATE_CONFIG','LICENSE PLATE DESC.':'LICENSE_PLATE_DESC','DOUBLE OR SINGLE':'DOUBLE_OR_SINGLE',
 'LICENSE PLATE':'LICENSE_PLATE','ORDER PLACED OFFICE ID':'ORDER_PLACED_OFFICE_ID','ORDER DEST. OFFICE ID':'ORDER_DEST_OFFICE_ID',
 'ORDER NUMBER & CODE':'ORDER_NUMBER_CODE','REGISTERED OWNER NAME':'REGISTERED_OWNER_NAME','REGISTERED OWNER ADDRESS':'REGISTERED_OWNER_ADDRESS',
 'REGISTERED OWNER CITY':'REGISTERED_OWNER_CITY','REGISTERED OWNER ZIP':'REGISTERED_OWNER_ZIP','ORDER PRINTED DATE':'ORDER_PRINTED_DATE',
 'TECH ID':'TECH_ID','ORDER PAYMENT DATE':'ORDER_PAYMENT_DATE'}, inplace = True)
   vAR_batch_elp_configuration_raw["ORDER_GROUP_ID"] = vAR_batch_elp_configuration_raw["ORDER_NUMBER_CODE"].str[:-1]
   vAR_batch_elp_configuration_raw['PLATE_TYPE_COUNT'] = vAR_batch_elp_configuration_raw.groupby('LICENSE_PLATE_DESC')['LICENSE_PLATE_DESC'].transform('count')
   
   return vAR_batch_elp_configuration_raw



# This method will return latest csv filename in request path folder
def RequestFileValidation():
  vAR_get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
  vAR_s3 = boto3.client('s3')
  vAR_objs = vAR_s3.list_objects_v2(Bucket=os.environ["S3_REQUEST_BUCKET_NAME"], Prefix=os.environ["AWS_REQUEST_PATH"])
  print('Return values in RequestFileValidation - ',vAR_objs)
  if vAR_objs['KeyCount']>1:
     message = "More than one file found in the request path -s3://"+os.environ["S3_REQUEST_BUCKET_NAME"]+'/'+vAR_objs['Prefix']
     raise Exception (message)
  if 'Contents' in vAR_objs.keys():
     vAR_objs = vAR_objs['Contents']
  else:
     vAR_err_message = "Request folder not found - s3://"+os.environ["S3_REQUEST_BUCKET_NAME"]+'/'+os.environ["AWS_REQUEST_PATH"]
     raise Exception(vAR_err_message)
  vAR_filelist =[]
  vAR_request_file_path = ''

    
  for obj in sorted(vAR_objs,key=vAR_get_last_modified,reverse=True):
    
    if obj['Key'].endswith('.CSV') or obj['Key'].endswith('.csv'):
      if obj['Size']==0:
        message = "Empty file found(file size 0 bytes) in the request path -s3://"+os.environ["S3_REQUEST_BUCKET_NAME"]+'/'+str(obj['Key'])
        raise Exception (message)
      else:
        vAR_filelist.append(obj['Key'])
        if len(vAR_filelist)>0:
          vAR_request_file_path = vAR_filelist[0]
          return vAR_request_file_path


  return vAR_request_file_path


def ResponsePathValidation(vAR_output_copy,vAR_records_to_process,vAR_response_count):

   
   # Audit trial log
   
   vAR_audit_log_df = pd.DataFrame()

   vAR_s3_url = GetRequestFileName()
   vAR_audit_log_df["AUDIT_DATE"] = 1*[date.today()]
   vAR_audit_log_df["REQUEST_DATE"] = 1*[date.today()]
   vAR_audit_log_df["TOTAL_ELP_ORDERS_IN_REQUEST"] = 1*[vAR_records_to_process]
   vAR_audit_log_df["TOTAL_ELP_ORDERS_IN_RESPONSE"] = 1*[vAR_response_count]
   vAR_audit_log_df["TOTAL_ELP_ORDER_PROCESSED"] = 1*[vAR_response_count] 
   vAR_audit_log_df["REQUEST_FILE_NAME"] = 1*[vAR_s3_url]
   vAR_audit_log_df["SYSTEM_ENVIRONMENT"] = 1*[os.environ["SYSTEM_ENVIRONMENT"]]

   vAR_s3 = boto3.client('s3')
   vAR_counter = 0
   vAR_objs = vAR_s3.list_objects_v2(Bucket=os.environ["S3_RESPONSE_BUCKET_NAME"], Prefix=os.environ["AWS_RESPONSE_PATH"])
   print('Return values in ResponsePathValidation - ',vAR_objs)
   if 'Contents' in vAR_objs.keys():
      vAR_objs = vAR_objs['Contents']
   else:

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy)

      vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["YES"]
      vAR_audit_log_df["RESPONSE_FILE_NAME"] = 1*[vAR_response_path]
      Insert_Audit_Log(vAR_audit_log_df)

      vAR_err_message = "Response folder not found - s3://"+os.environ["S3_RESPONSE_BUCKET_NAME"]+'/'+os.environ["AWS_RESPONSE_PATH"]+". So, created response folder"
      raise Exception(vAR_err_message)
   for obj in vAR_objs:
      print('OBJ RESPONSE KEY - ',obj['Key'])
      vAR_obj_key = obj['Key']
      vAR_obj_file = vAR_obj_key.split('/')[-1]
      vAR_obj_key =  vAR_obj_key.replace(vAR_obj_file,'')

      if os.environ["AWS_RESPONSE_PATH"] not in vAR_obj_key:
         pass
      else:
         vAR_counter = vAR_counter+1
         
   if vAR_counter>0:

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy)

      
      vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["YES"]
      vAR_audit_log_df["RESPONSE_FILE_NAME"] = 1*[vAR_response_path]
      Insert_Audit_Log(vAR_audit_log_df)


   else:

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy)
   
      vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["YES"]
      vAR_audit_log_df["RESPONSE_FILE_NAME"] = 1*[vAR_response_path]
      Insert_Audit_Log(vAR_audit_log_df)  
      
      vAR_err_message = "Response folder not found - s3://"+os.environ["S3_RESPONSE_BUCKET_NAME"]+'/'+os.environ["AWS_RESPONSE_PATH"]+". So, created response folder"
      raise Exception(vAR_err_message)
  
     


