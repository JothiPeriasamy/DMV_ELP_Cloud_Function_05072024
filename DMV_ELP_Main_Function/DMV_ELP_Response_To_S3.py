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
from io import StringIO
import datetime
import os

def Upload_Response_To_S3(vAR_result,vAR_file_name):
   vAR_bucket_name = os.environ['S3_BUCKET_NAME']
   vAR_csv_buffer = StringIO()
   vAR_result.to_csv(vAR_csv_buffer)
   vAR_s3_resource = boto3.resource('s3',aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
   vAR_s3_resource.Object(vAR_bucket_name, os.environ["AWS_RESPONSE_PATH"]+'/response_'+vAR_file_name).put(Body=vAR_csv_buffer.getvalue())
   print('bucket - ',vAR_bucket_name)
   print('path - ',os.environ["AWS_RESPONSE_PATH"]+'/response_'+vAR_file_name)
   print('API Response successfully saved into S3 bucket')
