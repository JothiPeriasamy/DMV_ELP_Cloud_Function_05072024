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
from DMV_ELP_Bigquery_Request_Validation import GetCurrentDateRequestCount,ReadNotProcessedRequestData,InsertRequesResponseMetaData,GetMetadataTotalRecordsToProcess,GetCurrentDateResponseCount,GetRequestFileName,GetMetadataLatestRecordTimeDiff
from DMV_ELP_Request_Delete import DeleteProcessedConfigs
from DMV_ELP_Update_Metadata_Table import UpdateMetadataTable
from DMV_ELP_Update_ErrorLog import InsertErrorLog
from DMV_ELP_Process_Request import Process_ELP_Request

from DMV_ELP_Response_To_GCS import Upload_Response_GCS
from DMV_ELP_Response_To_S3 import Upload_Response_To_S3
from DMV_ELP_Request_To_AWS_Processed import Move_Request_AWS_Processed
from DMV_ELP_Audit_Log import Insert_Audit_Log
from DMV_ELP_Get_Max_Plate_Count import GetMaxPlateTypeCount,GetMaxRunIdFromResponse
from DMV_ELP_Update_ChatGPT_Response import ReadRecordsToUpdate,UpdateGPTRecords
from DMV_ELP_ChatGPT_Recommendation import ELP_Recommendation

pd.set_option('display.max_colwidth', 500)




def Process_ELP_Orders(request):

   print('Value of CHATGPT_RESPONSE_UPDATE - ',os.environ["CHATGPT_RESPONSE_UPDATE"])
   if eval(os.environ["CHATGPT_RESPONSE_UPDATE"]):

      vAR_df = ReadRecordsToUpdate()
      print('Number of records to update - ',len(vAR_df))
      vAR_number_of_update = 0
      for index, row in vAR_df.iterrows():

         try:
            vAR_input = row["LICENSE_PLATE_CONFIG"].upper()
            vAR_response = ELP_Recommendation(vAR_input)
            response_json = {}

            vAR_dict1_start_index = vAR_response.index('{')
            vAR_dict1_end_index = vAR_response.index('}')


            vAR_dict2_start_index = vAR_response.rfind('{')
            vAR_dict2_end_index = vAR_response.rfind('}')

            vAR_result_dict = vAR_response[vAR_dict1_start_index:vAR_dict1_end_index+1]
            vAR_conclusion_dict = vAR_response[vAR_dict2_start_index:vAR_dict2_end_index+1]

            vAR_result_df = pd.DataFrame(json.loads(vAR_result_dict))
            vAR_conclusion_df = pd.DataFrame(json.loads(vAR_conclusion_dict))

            response_json["MODEL"] = "GPT"
            response_json["RECOMMENDATION"] = vAR_conclusion_df["Conclusion"][0]
            response_json["REASON"] = vAR_conclusion_df["Conclusion Reason"][0] 
            response_json["RECOMMENDED_CONFIGURATION"] = vAR_conclusion_df["Recommended Configuration"][0]
            response_json["RECOMMENDATION_REASON"] = vAR_conclusion_df["Recommendation Reason"][0]

            response_json["SEVERE_TOXIC_REASON"] = vAR_result_df["Reason"][0]
            response_json["OBSCENE_REASON"] = vAR_result_df["Reason"][1]
            response_json["INSULT_REASON"] = vAR_result_df["Reason"][2]
            response_json["IDENTITY_HATE_REASON"] = vAR_result_df["Reason"][3]
            response_json["TOXIC_REASON"] = vAR_result_df["Reason"][4]
            response_json["THREAT_REASON"] = vAR_result_df["Reason"][5]

            response_json["SEVERE_TOXIC"] = vAR_result_df["Probability"][0]
            response_json["OBSCENE"] = vAR_result_df["Probability"][1]
            response_json["INSULT"] = vAR_result_df["Probability"][2]
            response_json["IDENTITY_HATE"] = vAR_result_df["Probability"][3]
            response_json["TOXIC"] = vAR_result_df["Probability"][4]
            response_json["THREAT"] = vAR_result_df["Probability"][5]


            print('GPT response json - ',response_json)
            vAR_number_of_update += UpdateGPTRecords(vAR_input,response_json)

         except BaseException as e:

            print('Error while processing configuration - ',vAR_input)
            print('BASEEXCEPTIONERR IN UPDATE GPT RESPONSE - '+str(e))
            print('UPDATE GPT RESPONSE Error Traceback - '+str(traceback.print_exc()))



      print('vAR_number_of_update - ',vAR_number_of_update)


      
      return {"Message": "ChatGPT Response Successfully Updated"}





   vAR_metadata_time_diff = GetMetadataLatestRecordTimeDiff()

   if vAR_metadata_time_diff>0:
      print("Another scheduler is processing the request. So,terminating the overlap scheduler ")
      return {"Error Message":"Overlap Scheduler Terminated"}

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
         vAR_max_run_id = int(GetMaxRunIdFromResponse())

         if vAR_current_date_request_count==0:
            vAR_s3_request_path = RequestFileValidation()
            if len(vAR_s3_request_path)>0:
               vAR_s3_url = vAR_s3_url+os.environ["S3_REQUEST_BUCKET_NAME"]+'/'+vAR_s3_request_path
               print('VAR S3 Request URL - ',vAR_s3_url)
            else:
               vAR_err_message = "No valid csv request file found in path - "+vAR_s3_url+os.environ["S3_REQUEST_BUCKET_NAME"]+'/'+os.environ["AWS_REQUEST_PATH"]
               raise Exception(vAR_err_message)

            vAR_batch_elp_configuration_raw = pd.read_csv(vAR_s3_url,delimiter=';',dtype={'DOUBLE OR SINGLE': str, 'VIN': str,'REGISTERED OWNER ZIP':str})
            vAR_batch_elp_configuration = PreProcessRequest(vAR_batch_elp_configuration_raw,vAR_s3_url)
            vAR_number_of_configuration = len(vAR_batch_elp_configuration)
            Insert_Request_To_Bigquery(vAR_batch_elp_configuration,vAR_number_of_configuration)
            vAR_max_plate_desc_count = GetMaxPlateTypeCount()
            print('Max plate type desc count - ',vAR_max_plate_desc_count)
            InsertRequesResponseMetaData(vAR_number_of_configuration,vAR_max_plate_desc_count,vAR_s3_url)
            vAR_max_run_id = int(GetMaxRunIdFromResponse())+1

         vAR_configuration_df = ReadNotProcessedRequestData()
         vAR_configuration_df_len = len(vAR_configuration_df)

         print('There are '+str(vAR_configuration_df_len)+' configurations yet to be processed')

         if vAR_current_date_request_count==0 and vAR_configuration_df_len>0:
            Upload_Request_GCS(vAR_batch_elp_configuration,vAR_s3_url)
            Move_Request_AWS_Processed(vAR_s3_request_path)
            

         
         vAR_s3_url = GetRequestFileName()
         UpdateMetadataTable(vAR_s3_url)


         

         f.write('Start Time - {}\n\n'.format(vAR_process_start_time))
         f.write('Order No\t\t\t Start Time\t\t\t End Time\t\t\t Total Time\n')
         


         vAR_output_result_objects = [pool.apply_async(Process_ELP_Request,args=(vAR_configuration_df,elp_idx,vAR_request_url,vAR_headers)) for elp_idx in range(vAR_configuration_df_len)]

         time.sleep(5)
         
         for vAR_result in vAR_output_result_objects:
            
            
            if (time.time()-vAR_timeout_start)<vAR_timeout_secs:
               print('Time taking in for loop - ',time.time()-vAR_timeout_start)
            
               
               # If there is any error in insert response method exception block will log the error message for that configuration. So, trying to delete request table record in finally block
               try:      
                     vAR_result_op = vAR_result.get()

                     f.write(vAR_result_op["Process Time"])
                  
                     # vAR_output = vAR_output.append(vAR_result_op,ignore_index=True)
                     vAR_last_processed_record = vAR_result_op['LICENSE_PLATE_CONFIG']
                     vAR_payment_date  =vAR_result_op['ORDER_PAYMENT_DATE']
                     if 'Process Time' in vAR_result_op.keys():
                        del vAR_result_op['Process Time']
                     vAR_result_op["RUN_ID"] = vAR_max_run_id
                     # # Duplicate Check
                     # vAR_duplicate_count = DuplicateRecordCheck(vAR_last_processed_record,vAR_payment_date)
                     # if vAR_duplicate_count>0:
                     #    print('Duplicate Record found for configuration and order payment date - ',vAR_last_processed_record)
                     # else:
                     print('Result before bq insertion - ',vAR_result_op)
                     Insert_Response_to_Bigquery(pd.json_normalize(vAR_result_op))

                     print(vAR_result_op['LICENSE_PLATE_CONFIG']+' Inserted into response table')

                  
                     
               except BaseException as e:
                  print('BASEEXCEPTIONERR IN INSERT RESPONSE - '+str(e))
                  print('Error Traceback - '+str(traceback.print_exc()))
                  
                  vAR_err_response_dict = {}
                  vAR_err_response_dict['ERROR_MESSAGE'] = str(e)
                  vAR_err_response_dict['ERROR_CONTEXT'] = str(vAR_result_op)
                  vAR_err_response_dict['LICENSE_PLATE_CONFIG'] = vAR_result_op['LICENSE_PLATE_CONFIG']
                  vAR_err_response_dict['REQUEST_FILE_NAME'] = vAR_s3_url
                  InsertErrorLog(vAR_err_response_dict)
               finally:
                  try:
                     DeleteProcessedConfigs(vAR_result_op['LICENSE_PLATE_CONFIG'])
                     print(vAR_result_op['LICENSE_PLATE_CONFIG']+' deleted from request table')
                     vAR_processed_configs.append(vAR_result_op['LICENSE_PLATE_CONFIG'])
                     # vAR_output = vAR_output.append(vAR_result_op,ignore_index=True)
                     vAR_last_processed_record = vAR_result_op['LICENSE_PLATE_CONFIG']
                  except BaseException as e:
                     print('BASEEXCEPTIONERR IN DELETE REQUEST - ',str(e))
                     print('Error Traceback - '+str(traceback.print_exc()))
                     vAR_err_response_dict = {}
                     vAR_err_response_dict['ERROR_MESSAGE'] = str(e)
                     vAR_err_response_dict['ERROR_CONTEXT'] = str(vAR_result_op)
                     vAR_err_response_dict['LICENSE_PLATE_CONFIG'] = vAR_result_op['LICENSE_PLATE_CONFIG']
                     vAR_err_response_dict['REQUEST_FILE_NAME'] = vAR_s3_url
                     InsertErrorLog(vAR_err_response_dict)

            else:
               UpdateMetadataTable(vAR_s3_url)
               raise TimeoutError('Timeout Error inside result iteration')
                     
                        
            
            
            

                     
         # Close Pool and let all the processes complete
         pool.close()
         # postpones the execution of next line of code until all processes in the queue are done.
         pool.join()  

         

         vAR_records_to_process = GetMetadataTotalRecordsToProcess()
         vAR_response_count = GetCurrentDateResponseCount()
         vAR_current_date_request_count =  GetCurrentDateRequestCount()
         print('records to process from metadata table- ',vAR_records_to_process)
         print('response_count - ', vAR_response_count)
         print('request_table_count - ', vAR_current_date_request_count)
         if vAR_current_date_request_count==0 and vAR_configuration_df_len!=0:
            vAR_output_copy = ReadResponseTable()
            vAR_output_csv = vAR_output_copy.to_csv()
            
            vAR_s3_request_file_name = vAR_s3_url.split('/')[-1]
            vAR_s3_request_file_name = vAR_s3_request_file_name.split('.')[0]
            # Upload response to GCS bucket
            Upload_Response_GCS(vAR_output_csv,vAR_s3_request_file_name)

            # Upload response to S3 bucket
            ResponsePathValidation(vAR_output_copy,vAR_records_to_process,vAR_response_count,vAR_s3_url,vAR_s3_request_file_name)

            
            
         


         vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
         vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time


         f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
         UpdateMetadataTable(vAR_s3_url)
         return 'ELP Configurations Successfully Processed'

      except TimeoutError as timeout:
         print('TIMEOUTERR - Custom Timeout Error')
         
         # Since custom timeout occurs, checking for response count in response table and copy resullts to gcs&s3
         vAR_records_to_process = GetMetadataTotalRecordsToProcess()
         vAR_response_count = GetCurrentDateResponseCount()
         vAR_current_date_request_count =  GetCurrentDateRequestCount()
         print('records to process - ',vAR_records_to_process)
         print('response_count - ', vAR_response_count)
         print('request_table_count - ', vAR_current_date_request_count)
         if vAR_current_date_request_count==0 and vAR_configuration_df_len!=0:
            vAR_output_copy = ReadResponseTable()
            vAR_output_csv = vAR_output_copy.to_csv()
            
            vAR_s3_request_file_name = vAR_s3_url.split('/')[-1]
            vAR_s3_request_file_name = vAR_s3_request_file_name.split('.')[0]
            # Upload response to GCS bucket
            Upload_Response_GCS(vAR_output_csv,vAR_s3_request_file_name)

            # Upload response to S3 bucket
            ResponsePathValidation(vAR_output_copy,vAR_records_to_process,vAR_response_count,vAR_s3_url,vAR_s3_request_file_name)

            
            

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
         print('BASEEXCEPTIONERR IN MAIN - '+str(e))
         print('Error Traceback - '+str(traceback.print_exc()))
         vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
         vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time
         f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
         print('Number of Processed configs - ',len(vAR_processed_configs))
         UpdateMetadataTable(vAR_s3_url)
         
         # Logging only Errors in error log table during insertion of response&deletion of request tables.
         # So, commented below code to not insert into error log table. we can check the cloud funciton logs to get this exception block errors, if any.

         # vAR_err_response_dict = {}
         # vAR_err_response_dict['ERROR_MESSAGE'] = str(e)
         # vAR_err_response_dict['REQUEST_FILE_NAME'] = vAR_s3_url
         # InsertErrorLog(vAR_err_response_dict)

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

      
def PreProcessRequest(vAR_batch_elp_configuration_raw,vAR_s3_url):

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
   vAR_batch_elp_configuration_raw['REQUEST_FILE_NAME'] = vAR_number_of_configuration*[vAR_s3_url]
   return vAR_batch_elp_configuration_raw



# This method will return latest csv filename in request path folder
def RequestFileValidation():
  vAR_get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
  vAR_s3 = boto3.client('s3')
  vAR_objs = vAR_s3.list_objects_v2(Bucket=os.environ["S3_REQUEST_BUCKET_NAME"], Prefix=os.environ["AWS_REQUEST_PATH"])
  print('Return values in RequestFileValidation - ',vAR_objs)
#   if vAR_objs['KeyCount']>1:
#      message = "More than one file found in the request path -s3://"+os.environ["S3_REQUEST_BUCKET_NAME"]+'/'+vAR_objs['Prefix']
#      raise Exception (message)
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


def ResponsePathValidation(vAR_output_copy,vAR_records_to_process,vAR_response_count,vAR_s3_url,vAR_s3_request_file_name):

   
   # Audit trial log
   
   vAR_audit_log_df = pd.DataFrame()

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

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy,vAR_s3_request_file_name)

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

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy,vAR_s3_request_file_name)

      
      vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["YES"]
      vAR_audit_log_df["RESPONSE_FILE_NAME"] = 1*[vAR_response_path]
      Insert_Audit_Log(vAR_audit_log_df)


   else:

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy,vAR_s3_request_file_name)
   
      vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["YES"]
      vAR_audit_log_df["RESPONSE_FILE_NAME"] = 1*[vAR_response_path]
      Insert_Audit_Log(vAR_audit_log_df)  
      
      vAR_err_message = "Response folder not found - s3://"+os.environ["S3_RESPONSE_BUCKET_NAME"]+'/'+os.environ["AWS_RESPONSE_PATH"]+". So, created response folder"
      raise Exception(vAR_err_message)