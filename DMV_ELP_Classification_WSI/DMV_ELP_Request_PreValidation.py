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
import pandas as pd
import datetime
import os

def Pre_Request_Validation(request_json,vAR_partial_file_name,vAR_partial_file_date):
    vAR_error_message = ""
    vAR_configuration = request_json['LICENSE_PLATE_CONFIG']

    vAR_strlen = len(vAR_configuration)
    for char in vAR_configuration:
        if char=='/':
            vAR_strlen = vAR_strlen-0.5
    print('CONFIGURATION - ',vAR_configuration)
    print('LENGTH - ',vAR_strlen)
    if 'LICENSE_PLATE_CONFIG' not in request_json or len(request_json['LICENSE_PLATE_CONFIG'])==0 or request_json['LICENSE_PLATE_CONFIG']=='nan' or request_json['LICENSE_PLATE_CONFIG']=='<NA>':
        vAR_error_message =vAR_error_message+ "### Mandatory Parameter LICENSE_PLATE_CONFIG is missing"
    # if 'SG_ID' not in request_json or len(request_json["SG_ID"])==0 or request_json['SG_ID']=='nan' or request_json['SG_ID']=='<NA>':
    #     vAR_error_message =vAR_error_message+ "### Mandatory Parameter Simply Gov Id is missing"
    # if 'ORDER_GROUP_ID' not in request_json or len(request_json["ORDER_GROUP_ID"])==0 or request_json['ORDER_GROUP_ID']=='nan' or request_json['ORDER_GROUP_ID']=='<NA>':
    #     vAR_error_message =vAR_error_message+ "### Mandatory Parameter ORDER_GROUP_ID is missing"
    # if 'ORDER_ID' not in request_json or len(request_json["ORDER_ID"])==0 or request_json['ORDER_ID']=='nan' or request_json['ORDER_ID']=='<NA>':
    #     vAR_error_message = vAR_error_message+"### Mandatory Parameter ORDER_ID is missing"
    # if 'ORDER_PRINTED_DATE' not in request_json or len(request_json["ORDER_PRINTED_DATE"])==0 or request_json['ORDER_PRINTED_DATE']=='nan' or request_json['ORDER_PRINTED_DATE']=='<NA>':
    #     vAR_error_message = vAR_error_message+"### Mandatory Parameter ORDER_PRINTED_DATE is missing"

    if vAR_strlen>8:
        vAR_error_message = vAR_error_message+"### ELP Configuration can not be more than 8 characters"

    if len(request_json['LICENSE_PLATE_CONFIG'].strip())==0:
        vAR_error_message = vAR_error_message+"### ELP Configuration can not be processed for empty string"

    if len(CheckIfConfigAlreadyProcessed(request_json['LICENSE_PLATE_CONFIG'],vAR_partial_file_name,vAR_partial_file_date))>0:
        vAR_error_message = vAR_error_message+"### Configuration skipped(Already processed)"

    
    return {"Error Message":vAR_error_message}


def CheckIfConfigAlreadyProcessed(vAR_config,vAR_partial_file_name,vAR_partial_file_date):
    vAR_client = bigquery.Client()
    vAR_processed_config = ''
    vAR_table_name = "DMV_ELP_MLOPS_RESPONSE"
    
    if len(vAR_partial_file_name)>0 and len(vAR_partial_file_date)>0:
        vAR_query = "select LICENSE_PLATE_CONFIG from `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"` where LICENSE_PLATE_CONFIG='"+vAR_config+"'"+" and REQUEST_DATE='"+vAR_partial_file_date+"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"' group by LICENSE_PLATE_CONFIG having count(LICENSE_PLATE_CONFIG)>1"
        
    else:
        vAR_query = "select LICENSE_PLATE_CONFIG from `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"` where LICENSE_PLATE_CONFIG='"+vAR_config+"'"+" and date(created_dt)= current_date() group by LICENSE_PLATE_CONFIG having count(LICENSE_PLATE_CONFIG)>1"
    
    vAR_query_job = vAR_client.query(vAR_query)

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_processed_config = row.get('LICENSE_PLATE_CONFIG')
    print('vAR_processed_config_query - ',vAR_query)
    print('vAR_processed_config - ',vAR_processed_config)
    return vAR_processed_config