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
import requests
import json
import datetime
import time
import traceback
import  multiprocessing as mp
import sys
from google.cloud import storage
import os

from DMV_ELP_Request_Upload_To_GCS import Upload_Request_GCS

from DMV_ELP_Response_To_S3 import Upload_Response_To_S3
from DMV_ELP_Response_To_Bigquery import Insert_Response_to_Bigquery
from DMV_ELP_Request_To_Bigquery import Insert_Request_To_Bigquery
from DMV_ELP_Bigquery_Request_Validation import GetCurrentDateRequestCount,ReadNotProcessedRequestData,InsertRequesResponseMetaData
from DMV_ELP_Request_Delete import DeleteProcessedConfigs
from DMV_ELP_Update_Metadata_Table import UpdateMetadataTable
from DMV_ELP_Update_ErrorLog import InsertErrorLog
from DMV_ELP_Process_Request import Process_ELP_Request

from DMV_ELP_Response_To_GCS import Upload_Response_GCS
from DMV_ELP_Response_To_S3 import Upload_Response_To_S3

def Process_ELP_Orders(request):

   vAR_process_start_time = datetime.datetime.now().replace(microsecond=0)
   vAR_timeout_start = time.time()
   
   vAR_gcs_client = storage.Client()
   vAR_bucket = vAR_gcs_client.get_bucket(os.environ['GCS_BUCKET_NAME'])
   vAR_utc_time = datetime.datetime.now()
   blob = vAR_bucket.blob('processing_files/'+vAR_utc_time.strftime('%Y%m%d')+'/'+vAR_utc_time.strftime('%H%M%S')+'.txt')
   vAR_processed_configs = []
   
   with blob.open(mode='w') as f:
      try:
         
         vAR_request_json = request.get_json(silent=True)
         vAR_s3_url = "s3://"+os.environ["S3_BUCKET_NAME"]+"/"+os.environ["AWS_REQUEST_PATH"]+"/"+vAR_request_json["S3_REQUEST_FILE_NAME"]         
         vAR_timeout_secs = 900
         pool = mp.Pool(mp.cpu_count())
         if vAR_s3_url.startswith('s3'):
            vAR_request_url = os.environ['REQUEST_URL']
            vAR_output = pd.DataFrame()
            vAR_batch_input = None
            vAR_headers = {'content-type': 'application/json','user-agent': 'Mozilla/5.0'}
            vAR_batch_elp_configuration = pd.read_csv(vAR_s3_url)
            vAR_number_of_configuration = len(vAR_batch_elp_configuration)
            vAR_error_message = ""
            vAR_current_date_request_count = GetCurrentDateRequestCount()
            
            if vAR_current_date_request_count==0:
               Insert_Request_To_Bigquery(vAR_batch_elp_configuration,vAR_number_of_configuration)
               InsertRequesResponseMetaData(vAR_number_of_configuration)
            
            UpdateMetadataTable()


            Upload_Request_GCS(vAR_batch_elp_configuration)

            print('Request file successfully uploaded into gcs bucket')

            

            f.write('Start Time - {}\n\n'.format(vAR_process_start_time))
            f.write('Order No\t\t\t Start Time\t\t\t End Time\t\t\t Total Time\n')
            

            vAR_configuration_df = ReadNotProcessedRequestData()
            vAR_configuration_df_len = len(vAR_configuration_df)
            print('There are '+str(vAR_configuration_df_len)+' configurations yet to be processed')

            vAR_output_result_objects = [pool.apply_async(Process_ELP_Request,args=(vAR_configuration_df,elp_idx,vAR_request_url,vAR_headers)) for elp_idx in range(vAR_configuration_df_len)]

            vAR_results = []
            for vAR_result in vAR_output_result_objects:
               print('Time taking in for loop - ',time.time()-vAR_timeout_start)
               if (time.time()-vAR_timeout_start)<vAR_timeout_secs:
                  
                  if len(vAR_result.get()['ERROR_MESSAGE'])>0:
                     InsertErrorLog(vAR_result.get())
                     vAR_output = vAR_output.append(vAR_result.get(),ignore_index=True)
                     
                  else:
                     print(vAR_result.get()["Process Time"])
                     f.write(vAR_result.get()["Process Time"])
                     del vAR_result.get()['Process Time']
                     vAR_results.append(vAR_result.get())
                     Insert_Response_to_Bigquery(pd.DataFrame(vAR_result.get(),index=[0]))
                     print(vAR_result.get()['CONFIGURATION']+' Inserted into response table')
                     DeleteProcessedConfigs()
                     print(vAR_result.get()['CONFIGURATION']+' delete from request table')
                     vAR_processed_configs.append(vAR_result.get()['CONFIGURATION'])
                     vAR_output = vAR_output.append(vAR_result.get(),ignore_index=True)
                     
               else:
                  
                  raise TimeoutError('Timeout Error inside result iteration')
               
               

                      
            # Close Pool and let all the processes complete
            pool.close()
            # postpones the execution of next line of code until all processes in the queue are done.
            pool.join()  

            vAR_output_copy = vAR_output.copy(deep=True)
            vAR_output = vAR_output.to_csv()
            
            # Upload response to GCS bucket
            Upload_Response_GCS(vAR_output)

            # Upload response to S3 bucket
            Upload_Response_To_S3(vAR_output_copy)


            vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
            vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time


            f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
            return 'ELP Configurations Successfully Processed'


         else:
            return 'Input configuration file not found in S3'
      

      except TimeoutError as timeout:
         print('TIMEOUTERR - Custom Timeout Error')
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

      except ConnectionError as connectionerror:
         print('HTTPCONNECTIONERR - Http connection error occurred')
         print('Error Traceback - '+str(traceback.print_exc()))
         print('Number of Processed configs - ',len(vAR_processed_configs))
         return {'Error Message':'### ConnectionError Occured'}
      
      except BaseException as e:
         print('BASEEXCEPTIONERR - '+str(e))
         print('Error Traceback - '+str(traceback.print_exc()))
         vAR_process_end_time = datetime.datetime.now().replace(microsecond=0)
         vAR_total_processing_time = vAR_process_end_time-vAR_process_start_time
         f.write('\n\nEnd Time - {}\nTotal Processing Time - {}'.format(vAR_process_end_time,vAR_total_processing_time))
         print('Number of Processed configs - ',len(vAR_processed_configs))
         return {'Error Message':'### '+str(e)}

      


