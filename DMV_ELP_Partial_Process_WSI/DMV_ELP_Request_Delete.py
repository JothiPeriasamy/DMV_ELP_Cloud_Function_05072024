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

from google.cloud import bigquery
import os

def DeleteProcessedConfigs(vAR_partial_file_date,vAR_partial_file_name,vAR_config):
   vAR_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])

   vAR_request_table = "DMV_ELP_REQUEST"

   vAR_query_delete = " delete from `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_request_table+"`" +" where  REQUEST_DATE = '"+vAR_partial_file_date+"'"+" and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"' and LICENSE_PLATE_CONFIG='"+vAR_config+"'"

   vAR_job = vAR_client.query(vAR_query_delete)
   vAR_job.result()
   print("Processed Records are deleted! - ",vAR_job.num_dml_affected_rows)