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

import boto3
import os

def Move_Request_AWS_Processed():
   vAR_s3_client = boto3.client('s3')
   vAR_bucket_name = os.environ['S3_BUCKET_NAME']
   vAR_source_file_key = vAR_bucket_name+'/'+os.environ["AWS_REQUEST_PATH"]
   vAR_source_file_name = os.environ["AWS_REQUEST_PATH"].split('/')[-1]
   print('SourceFileName - ',vAR_source_file_name)
   vAR_dest_file_key = os.environ["AWS_REQUEST_PROCESSED_PATH"]+'/'+vAR_source_file_name
   vAR_response = vAR_s3_client.copy_object(
   CopySource=vAR_source_file_key,                    # /Bucket-name/path/filename
   Bucket=vAR_bucket_name,                            # Destination bucket
   Key=vAR_dest_file_key                              # Destination path/filename
)
   print('Copy file response - ',vAR_response)
   if vAR_response['ResponseMetadata']['HTTPStatusCode']==200:
      print('Request File copied into processed folder')
      vAR_delete_response = vAR_s3_client.delete_object(Bucket=vAR_bucket_name,Key=os.environ["AWS_REQUEST_PATH"])
      print('Request file successfully deleted from source location')
   else:
      raise Exception('Request File not copied to processed folder in s3.. Kindly check copy file response')
