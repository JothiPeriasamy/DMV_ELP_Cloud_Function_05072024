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


from DMV_ELP_Get_Not_Processed_Config import FilterNotProcessedRecordsFromGCSRequestFile
from DMV_ELP_Insert_Request_To_Bigquery import Insert_Request_To_Bigquery
from DMV_ELP_Bigquery_Utility_Method import GetMetadataTotalRecordsToProcess,ReadNotProcessedRequestData,GetResponseCountForGivenDateAndFile,GetRequestCountForGivenDateAndFile,ReadResponseTable,PostProcessingReport,DeleteErrorTableRecords,GetErrorTableCountForGivenDateAndFile,GetRequestFilePath,UpdateMetadataTable,GetPartialResponseCountForGivenDateAndFile,GetPreviousdayRequestCount,ReadPartialResponseTable,GetMetadataLatestRecordTimeDiff
from DMV_ELP_Process_Request import Process_ELP_Request
from DMV_ELP_Response_To_Bigquery import Insert_Response_to_Bigquery
from DMV_ELP_Insert_ErrorLog import InsertErrorLog
from DMV_ELP_Request_Delete import DeleteProcessedConfigs
from DMV_ELP_Response_To_GCS import Upload_Response_GCS
from DMV_ELP_Response_To_S3 import Upload_Response_To_S3
from DMV_ELP_Audit_Log import Insert_Audit_Log

pd.set_option('display.max_colwidth', 500)

def ProcessPartialOrders(request):

    vAR_partial_file_date = os.environ["PARTIAL_FILE_DATE"]
    vAR_partial_file_name = os.environ["PARTIAL_FILE_NAME"]

    vAR_partial_file_name = vAR_partial_file_name.lower()

    vAR_metadata_time_diff = GetMetadataLatestRecordTimeDiff(vAR_partial_file_date,vAR_partial_file_name)

    if vAR_metadata_time_diff>0:

        print("Another scheduler is processing the request. So,terminating the overlap scheduler ")
        return {"Error Message":"Overlap Scheduler Terminated"}

    vAR_gcs_client = storage.Client()
    vAR_bucket = vAR_gcs_client.get_bucket(os.environ['GCS_BUCKET_NAME'])
    vAR_utc_time = datetime.datetime.utcnow()
    blob = vAR_bucket.blob(os.environ['RUNTIME_STATS_PATH']+vAR_utc_time.strftime('%Y%m%d')+'/'+vAR_utc_time.strftime('%H%M%S')+'_partialprocess'+'.txt')
    with blob.open(mode='w') as f:
        try:
            vAR_process_start_time = datetime.datetime.now().replace(microsecond=0)
            vAR_timeout_start = time.time()

            

            UpdateMetadataTable(vAR_partial_file_date,vAR_partial_file_name)
            
            vAR_previousday_request_count = GetPreviousdayRequestCount(vAR_partial_file_date,vAR_partial_file_name)
            print('Previous day request count - ',vAR_previousday_request_count)
            vAR_records_to_insert_to_request_table = FilterNotProcessedRecordsFromGCSRequestFile(vAR_partial_file_date,vAR_partial_file_name)
            if len(vAR_records_to_insert_to_request_table)>0:
                vAR_records_to_insert_to_request_table_len = len(vAR_records_to_insert_to_request_table)
                print('Record count to insert into request table  - ',vAR_records_to_insert_to_request_table_len)
                vAR_request_table_count_before = GetRequestCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
                print('Request table count before insert  records - ',vAR_request_table_count_before)
                Insert_Request_To_Bigquery(vAR_records_to_insert_to_request_table,vAR_records_to_insert_to_request_table_len)
                vAR_request_table_count_after = GetRequestCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
                print('Request table count after records insertion- ',vAR_request_table_count_after)
            

            #  To Check Record count matches or not

            vAR_total_records_to_process = GetMetadataTotalRecordsToProcess(vAR_partial_file_date,vAR_partial_file_name)
            vAR_response_table_count,vAR_run_id = GetResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
            vAR_request_table_count = GetRequestCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
            
            vAR_processed_configs = []
            vAR_s3_url = GetRequestFilePath(vAR_partial_file_date,vAR_partial_file_name)

            if vAR_total_records_to_process == vAR_response_table_count+vAR_request_table_count:

                vAR_configuration_df = ReadNotProcessedRequestData(vAR_partial_file_date,vAR_partial_file_name)
                vAR_configuration_df_len = len(vAR_configuration_df)

                

                print('There are '+str(vAR_configuration_df_len)+' configurations yet to be processed')

                vAR_timeout_secs = int(os.environ['TIMEOUT_SECS'])
                pool = mp.Pool(mp.cpu_count())
                vAR_request_url = os.environ['REQUEST_URL']
                vAR_headers = {'content-type': 'application/json','user-agent': 'Mozilla/5.0'}

                f.write('Start Time - {}\n\n'.format(vAR_process_start_time))
                f.write('Order No\t\t\t Start Time\t\t\t End Time\t\t\t Total Time\n')

                vAR_output_result_objects = [pool.apply_async(Process_ELP_Request,args=(vAR_configuration_df,elp_idx,vAR_request_url,vAR_headers)) for elp_idx in range(vAR_configuration_df_len)]

                for vAR_result in vAR_output_result_objects:
                    vAR_result_op = vAR_result.get()
                    vAR_result_op['RUN_ID'] = vAR_run_id
                    print('Time taking in for loop - ',time.time()-vAR_timeout_start)
                    if (time.time()-vAR_timeout_start)<vAR_timeout_secs:
                        f.write(vAR_result_op["Process Time"])
                        
                        
                        # If there is any error in insert response method exception block will log the error message for that configuration. So, trying to delete request table record in finally block
                        try:      
                            
                            vAR_last_processed_record = vAR_result_op['LICENSE_PLATE_CONFIG']
                            if 'Process Time' in vAR_result_op.keys():
                                del vAR_result_op['Process Time']
                            if 'REQUEST_FILE_NAME' in vAR_result_op:
                                vAR_s3_url = vAR_result_op['REQUEST_FILE_NAME']
                            Insert_Response_to_Bigquery(pd.DataFrame(vAR_result_op,index=[0]))
                            print(vAR_result_op['LICENSE_PLATE_CONFIG']+' Inserted into response table')
                            
                        except BaseException as e:
                            print('BASEEXCEPTIONERR IN INSERT RESPONSE - '+str(e))
                            print('Error Traceback - '+str(traceback.print_exc()))
                            
                            vAR_err_response_dict = {}
                            vAR_err_response_dict['ERROR_MESSAGE'] = str(e)
                            vAR_err_response_dict['ERROR_CONTEXT'] = str(vAR_result_op)
                            vAR_err_response_dict['LICENSE_PLATE_CONFIG'] = vAR_result_op['LICENSE_PLATE_CONFIG']
                            if 'REQUEST_FILE_NAME' in vAR_result_op:
                                vAR_err_response_dict['REQUEST_FILE_NAME'] = vAR_result_op['REQUEST_FILE_NAME']
                            InsertErrorLog(vAR_err_response_dict)
                        finally:
                            try:
                                DeleteProcessedConfigs(vAR_partial_file_date,vAR_partial_file_name,vAR_result_op['LICENSE_PLATE_CONFIG'])
                                print(vAR_result_op['LICENSE_PLATE_CONFIG']+' deleted from request table')
                                vAR_processed_configs.append(vAR_result_op['LICENSE_PLATE_CONFIG'])
                                vAR_last_processed_record = vAR_result_op['LICENSE_PLATE_CONFIG']
                                print("Last processed record - ",vAR_last_processed_record)
                            except BaseException as e:
                                print('BASEEXCEPTIONERR IN DELETE REQUEST - ',str(e))
                                print('Error Traceback - '+str(traceback.print_exc()))
                                vAR_err_response_dict = {}
                                vAR_err_response_dict['ERROR_MESSAGE'] = str(e)
                                vAR_err_response_dict['ERROR_CONTEXT'] = str(vAR_result_op)
                                vAR_err_response_dict['LICENSE_PLATE_CONFIG'] = vAR_result_op['LICENSE_PLATE_CONFIG']
                                if 'REQUEST_FILE_NAME' in vAR_result_op:
                                    vAR_err_response_dict['REQUEST_FILE_NAME'] = vAR_result_op['REQUEST_FILE_NAME']
                                InsertErrorLog(vAR_err_response_dict)         
                    else:
                        UpdateMetadataTable(vAR_partial_file_date,vAR_partial_file_name)
                        raise TimeoutError('Timeout Error inside result iteration')
                
                # Close Pool and let all the processes complete
                pool.close()
                # postpones the execution of next line of code until all processes in the queue are done.
                pool.join()

                vAR_total_records_to_process = GetMetadataTotalRecordsToProcess(vAR_partial_file_date,vAR_partial_file_name)
                vAR_response_table_count,vAR_run_id = GetResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
                vAR_request_table_count = GetRequestCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)

                if vAR_previousday_request_count>0 and vAR_configuration_df_len>0 and vAR_request_table_count==0:
                    vAR_output_copy = ReadResponseTable(vAR_partial_file_date,vAR_partial_file_name)
                    vAR_output_csv = vAR_output_copy.to_csv()
                    vAR_partial_file_flag = 0
                    # Upload response to GCS bucket
                    vAR_partial_file_name_truncate = vAR_partial_file_name.replace('.csv','')
                    Upload_Response_GCS(vAR_output_csv,vAR_partial_file_name_truncate)

                    # Upload response to S3 bucket
                    ResponsePathValidation(vAR_output_copy,vAR_total_records_to_process,vAR_response_table_count,vAR_s3_url,vAR_partial_file_name_truncate,vAR_partial_file_date,vAR_partial_file_name,vAR_partial_file_flag)

                    PostProcessingReport(vAR_partial_file_date,vAR_partial_file_name)
                    UpdateMetadataTable(vAR_partial_file_date,vAR_partial_file_name)

                elif vAR_request_table_count==0 and vAR_configuration_df_len>0:
                    vAR_output_copy = ReadPartialResponseTable(vAR_partial_file_date,vAR_partial_file_name)
                    vAR_output_csv = vAR_output_copy.to_csv()
                    vAR_partial_file_flag = 1
                    # Upload response to GCS bucket
                    vAR_partial_file_name_truncate = vAR_partial_file_name.replace('.csv','')
                    Upload_Response_GCS(vAR_output_csv,vAR_partial_file_name_truncate)

                    # Upload response to S3 bucket
                    vAR_response_table_count = GetPartialResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
                    ResponsePathValidation(vAR_output_copy,vAR_total_records_to_process,vAR_response_table_count,vAR_s3_url,vAR_partial_file_name_truncate,vAR_partial_file_date,vAR_partial_file_name,vAR_partial_file_flag)

                    PostProcessingReport(vAR_partial_file_date,vAR_partial_file_name)
                    UpdateMetadataTable(vAR_partial_file_date,vAR_partial_file_name)

                vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
                vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time


                f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
                return 'ELP Configurations Successfully Processed'


            else:
                print('Metadata table, Request & Response table count doesn''t match. Kindly, investigate the issue.')

        except TimeoutError as timeout:
            print('TIMEOUTERR - Custom Timeout Error')
                
            # Since custom timeout occurs, checking for response count in response table and copy resullts to gcs&s3
            vAR_total_records_to_process = GetMetadataTotalRecordsToProcess(vAR_partial_file_date,vAR_partial_file_name)
            vAR_response_table_count,vAR_run_id = GetResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
            vAR_request_table_count = GetRequestCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)

            if vAR_previousday_request_count>0 and vAR_configuration_df_len>0 and vAR_request_table_count==0:
                vAR_output_copy = ReadResponseTable(vAR_partial_file_date,vAR_partial_file_name)
                vAR_output_csv = vAR_output_copy.to_csv()
                vAR_partial_file_flag = 0
                # Upload response to GCS bucket
                vAR_partial_file_name_truncate = vAR_partial_file_name.replace('.csv','')
                Upload_Response_GCS(vAR_output_csv,vAR_partial_file_name_truncate)

                # Upload response to S3 bucket
                ResponsePathValidation(vAR_output_copy,vAR_total_records_to_process,vAR_response_table_count,vAR_s3_url,vAR_partial_file_name_truncate,vAR_partial_file_date,vAR_partial_file_name,vAR_partial_file_flag)
                
                PostProcessingReport(vAR_partial_file_date,vAR_partial_file_name)

            elif vAR_request_table_count==0 and vAR_configuration_df_len>0:
                vAR_output_copy = ReadPartialResponseTable(vAR_partial_file_date,vAR_partial_file_name)
                vAR_output_csv = vAR_output_copy.to_csv()
                vAR_partial_file_flag=1
                # Upload response to GCS bucket
                vAR_partial_file_name_truncate = vAR_partial_file_name.replace('.csv','')
                Upload_Response_GCS(vAR_output_csv,vAR_partial_file_name_truncate)

                # Upload response to S3 bucket
                vAR_response_table_count = GetPartialResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
                ResponsePathValidation(vAR_output_copy,vAR_total_records_to_process,vAR_response_table_count,vAR_s3_url,vAR_partial_file_name_truncate,vAR_partial_file_date,vAR_partial_file_name,vAR_partial_file_flag)
                
                PostProcessingReport(vAR_partial_file_date,vAR_partial_file_name)
                    
                    

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
            print('BASE EXCEPTION ERR IN PARTIAL PROCESS - ',str(e))
            print('Error Traceback - '+str(traceback.print_exc()))
            vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
            vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time
            f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
            print('Number of Processed configs - ',len(vAR_processed_configs))
            UpdateMetadataTable(vAR_partial_file_date,vAR_partial_file_name)

            if "Response Path Error " in str(e):
                print("Response path error in main - ",str(e))
                # Audit trial log
                vAR_audit_log_df = pd.DataFrame()

                vAR_audit_log_df["AUDIT_DATE"] = 1*[date.today()]
                vAR_audit_log_df["REQUEST_DATE"] = 1*[date.today()]
                vAR_audit_log_df["TOTAL_ELP_ORDERS_IN_REQUEST"] = 1*[vAR_total_records_to_process]
                vAR_audit_log_df["TOTAL_ELP_ORDERS_IN_RESPONSE"] = 1*[vAR_response_table_count]
                vAR_audit_log_df["TOTAL_ELP_ORDER_PROCESSED"] = 1*[vAR_response_table_count] 
                vAR_audit_log_df["REQUEST_FILE_NAME"] = 1*[vAR_s3_url]
                vAR_audit_log_df["SYSTEM_ENVIRONMENT"] = 1*[os.environ["SYSTEM_ENVIRONMENT"]]

                vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["NO"]
                vAR_audit_log_df["ERROR_MESSAGE"] = str(e)
                Insert_Audit_Log(vAR_audit_log_df)
                print("Audit Log Table Inserted")

                return {'Error Message':'### '+str(e)}



def ResponsePathValidation(vAR_output_copy,vAR_records_to_process,vAR_response_count,vAR_s3_url,vAR_s3_request_file_name,vAR_partial_file_date,vAR_partial_file_name,vAR_partial_file_flag):

   
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

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy,vAR_s3_request_file_name,vAR_partial_file_flag)

      vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["YES"]
      vAR_audit_log_df["RESPONSE_FILE_NAME"] = 1*[vAR_response_path]
      Insert_Audit_Log(vAR_audit_log_df)
      PostProcessingReport(vAR_partial_file_date,vAR_partial_file_name)
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

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy,vAR_s3_request_file_name,vAR_partial_file_flag)

      
      vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["YES"]
      vAR_audit_log_df["RESPONSE_FILE_NAME"] = 1*[vAR_response_path]
      Insert_Audit_Log(vAR_audit_log_df)


   else:

      vAR_response_path = Upload_Response_To_S3(vAR_output_copy,vAR_s3_request_file_name,vAR_partial_file_flag)
   
      vAR_audit_log_df["FILE_TRANSMITTED_SUCCESSFULLY"] = 1*["YES"]
      vAR_audit_log_df["RESPONSE_FILE_NAME"] = 1*[vAR_response_path]
      Insert_Audit_Log(vAR_audit_log_df)  
      PostProcessingReport(vAR_partial_file_date,vAR_partial_file_name)
      vAR_err_message = "Response folder not found - s3://"+os.environ["S3_RESPONSE_BUCKET_NAME"]+'/'+os.environ["AWS_RESPONSE_PATH"]+". So, created response folder"
      raise Exception(vAR_err_message)
