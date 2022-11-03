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
import json
import requests
from DMV_ELP_Mapping_Response_To_Bigquery import Process_API_Response
import os
import urllib
import google.auth.transport.requests
import google.oauth2.id_token

def Process_ELP_Request(vAR_batch_elp_configuration,elp_idx,vAR_request_url,vAR_headers):

   vAR_request_start_time = datetime.datetime.now().replace(microsecond=0)
   

   vAR_payload = vAR_batch_elp_configuration.iloc[elp_idx].to_dict()
   vAR_keys_to_remove = ['CREATED_DT','UPDATED_DT','CREATED_USER','UPDATED_USER']
   
   for key in vAR_keys_to_remove:
      if key in vAR_payload.keys():
         del vAR_payload[key]
   
   # Type conversion for datetime fields
   vAR_payload['REQUEST_DATE'] = str(vAR_payload['REQUEST_DATE'])
   vAR_payload['ORDER_PRINTED_DATE'] = str(vAR_payload['ORDER_PRINTED_DATE'])
   vAR_payload['ORDER_PAYMENT_DATE'] = str(vAR_payload['ORDER_PAYMENT_DATE'])



   print('Payload - ',vAR_payload)

   vAR_endpoint = vAR_request_url
   vAR_audience = vAR_request_url

   vAR_payload = json.dumps(vAR_payload)
   # Convert to String
   vAR_payload = str(vAR_payload)

   # Convert string to byte
   vAR_payload = vAR_payload.encode('utf-8')

   vAR_auth_request = urllib.request.Request(vAR_endpoint,data=vAR_payload)

   vAR_auth_req = google.auth.transport.requests.Request()
   vAR_id_token = google.oauth2.id_token.fetch_id_token(vAR_auth_req, vAR_audience)

   vAR_auth_request.add_header("Authorization", f"Bearer {vAR_id_token}")
   vAR_auth_request.add_header("Content-Type", "application/json; charset=utf-8")

   vAR_request = urllib.request.urlopen(vAR_auth_request)

   # vAR_request = requests.post(vAR_request_url, data=json.dumps(vAR_payload),headers=vAR_headers)
   
   # vAR_result = vAR_request.text #Getting response as str

   vAR_result = vAR_request.read()
   print('vAR_result - ',vAR_result)
   vAR_result = json.loads(vAR_result) #converting str to dict
   if len(vAR_result["ERROR_MESSAGE"])>0:
      print('Below Error in Configuration - '+str(vAR_batch_elp_configuration['LICENSE_PLATE_CONFIG'][elp_idx]))
      print(vAR_result)
      vAR_error_message = vAR_result["ERROR_MESSAGE"]
      
   vAR_response_dict = Process_API_Response(vAR_result)
   vAR_request_end_time = datetime.datetime.now().replace(microsecond=0)
   vAR_each_request_time = vAR_request_end_time-vAR_request_start_time
   print('processed - ',elp_idx)
   # Adding Process time for file object to write, since we can't directly use file object in parallel processing(later this column can be removed)
   vAR_response_dict["Process Time"] = '{}\t\t\t{}\t\t\t{}\t\t{}\n'.format(elp_idx,vAR_request_start_time,vAR_request_end_time,vAR_each_request_time)
   return vAR_response_dict
   
