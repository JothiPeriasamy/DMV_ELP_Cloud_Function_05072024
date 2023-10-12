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
from google.cloud import bigquery
import os


def Previously_Denied_Configuration_Validation(input_text):
    vAR_response_count = 0
    vAR_input = input_text.replace('/','')
    vAR_input = input_text.replace('*','')
    vAR_denial_reason = 'PREVIOUSLY_DENIED'
    vAR_bqclient = bigquery.Client()
    vAR_table_name = "DMV_ELP_PREVIOUSLY_DENIED_CONFIGURATION"
    vAR_query_string = " SELECT count(1) as cnt FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"` where RTRIM(UPPER(PREVIOUSLY_DENIED_CONFIG))='"+vAR_input+"'"
    
    vAR_query_job = vAR_bqclient.query(vAR_query_string)
    vAR_results = vAR_query_job.result()
    for row in vAR_results:
        vAR_response_count = row.get('cnt')
    if vAR_response_count>=1:
        return True,"Denied - "+vAR_denial_reason
    else:
        return False,'Given configuration is not found in DMV Previously Denied Configuration list'
