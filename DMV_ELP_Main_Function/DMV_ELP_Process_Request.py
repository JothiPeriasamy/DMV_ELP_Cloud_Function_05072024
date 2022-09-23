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


def Process_ELP_Request(vAR_batch_elp_configuration,elp_idx,vAR_request_url,vAR_headers):

   vAR_request_start_time = datetime.datetime.now().replace(microsecond=0)
   configuration = vAR_batch_elp_configuration['CONFIGURATION'][elp_idx]
   vAR_model = "RNN"
   vAR_order_date = vAR_batch_elp_configuration['ORDER_DATE'][elp_idx]
   vAR_order_id = vAR_batch_elp_configuration['ORDER_ID'][elp_idx]
   vAR_sg_id = vAR_batch_elp_configuration['SG_ID'][elp_idx]
   vAR_order_group_id = vAR_batch_elp_configuration['ORDER_GROUP_ID'][elp_idx]
   vAR_request_date = vAR_batch_elp_configuration['REQUEST_DATE'][elp_idx]
   vAR_payload = {"CONFIGURATION":str(configuration),"MODEL":str(vAR_model),"ORDER_DATE":str(vAR_order_date),"ORDER_ID":str(vAR_order_id),"ORDER_GROUP_ID":str(vAR_order_group_id),"SG_ID":str(vAR_sg_id),"REQUEST_DATE":str(vAR_request_date)}
   
   print('Payload - ',vAR_payload)
   vAR_request = requests.post(vAR_request_url, data=json.dumps(vAR_payload),headers=vAR_headers)
   
   vAR_result = vAR_request.text #Getting response as str
   print('vAR_result - ',vAR_result)
   vAR_result = json.loads(vAR_result) #converting str to dict
   if len(vAR_result["Error Message"])>0:
      print('Below Error in Order Id - '+str(vAR_batch_elp_configuration['ORDER_ID'][elp_idx]))
      print(vAR_result)
      vAR_error_message = vAR_result["Error Message"]
      
   print('Order Id - '+str(vAR_batch_elp_configuration['ORDER_ID'][elp_idx])+' Successfully processed')
   vAR_response_dict = Process_API_Response(vAR_result)
   vAR_request_end_time = datetime.datetime.now().replace(microsecond=0)
   vAR_each_request_time = vAR_request_end_time-vAR_request_start_time
   print('processed - ',elp_idx)
   # Adding Process time for file object to write, since we can't directly use file object in parallel processing(later this column can be removed)
   vAR_response_dict["Process Time"] = '{}\t\t\t{}\t\t\t{}\t\t{}\n'.format(elp_idx,vAR_request_start_time,vAR_request_end_time,vAR_each_request_time)
   return vAR_response_dict
   
