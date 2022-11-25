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


from google.cloud import storage
import datetime
import os

def Upload_Request_GCS(vAR_request,vAR_s3_url):
    vAR_request_file_name = vAR_s3_url
    vAR_request_file_name = vAR_request_file_name.split('/')[-1]
    print('S3 request file name in upload gcs method - ',vAR_request_file_name)
    vAR_request = vAR_request.to_csv()
    vAR_bucket_name = os.environ['GCS_BUCKET_NAME']
    vAR_utc_time = datetime.datetime.utcnow()
    client = storage.Client()
    bucket = client.get_bucket(vAR_bucket_name)
    vAR_file_path = os.environ["GCP_REQUEST_PATH"]+'/'+vAR_utc_time.strftime('%Y%m%d')+'/'+vAR_utc_time.strftime('%H%M%S')+'_'+vAR_request_file_name
    vAR_file_path = vAR_file_path.lower()
    bucket.blob(vAR_file_path).upload_from_string(vAR_request, 'text/csv')
    print('ELP Configuration Request successfully saved into cloud storage')
    print('Path - ',vAR_file_path)
    return vAR_file_path