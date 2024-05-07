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

import re
import pandas as pd
from google.cloud import bigquery
import os


def Pattern_Denial(input_text):
    
    
    # Below configurations needs to be update in bigquery table
    
    # 1.3 numbers, 2 letters 000AA-000E0 RESERVED Between AA-E0
    # 2.4 numbers 1111 PREV. ASSIGNED Check R60 4E/4S
    # 3.5 numbers 11111 PREV. ASSIGNED Check R60 4E/4S
    
    vAR_bqclient = bigquery.Client()
    vAR_result = ''
    vAR_denial_pattern = ''

    vAR_table_name = "DMV_ELP_DENIED_PATTERN"
    vAR_query_string = "SELECT REGEXP_CONTAINS('"+input_text+"',REGEX_CONFIGURATION) as result,DENIAL_PATTERN FROM "+"`"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"` where REGEXP_CONTAINS('"+input_text+"',REGEX_CONFIGURATION)=true"

    vAR_query_job = vAR_bqclient.query(vAR_query_string)
    vAR_results = vAR_query_job.result()
    for row in vAR_results:
        vAR_result = row.get('result')
        vAR_denial_pattern = row.get('DENIAL_PATTERN')
    if len(vAR_denial_pattern)>0:
        return False,vAR_denial_pattern
    else:
        return True,vAR_denial_pattern