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


def GetPlateCode(vAR_license_plate_desc):
    vAR_error_message = ""
    vAR_client = bigquery.Client()
    vAR_plate_code = None
    vAR_table_name = "DMV_ELP_PLATE_TYPE"
    vAR_query = " SELECT PLATE_TYPE_CODE from "+ "`"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+  " where PLATE_TYPE_NAME='"+vAR_license_plate_desc+"'"
    print('License code query - ',vAR_query)
    vAR_query_job = vAR_client.query(
        vAR_query
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    vAR_result_count = vAR_results.total_rows
    if vAR_result_count==1:
        for row in vAR_results:
            vAR_plate_code = row.get('PLATE_TYPE_CODE')
    elif vAR_result_count==0:
        vAR_error_message = "PLATE CODE Not Found for given configuration"
    elif vAR_result_count>1:
        vAR_error_message = "More than one PLATE CODE Found for given configuration"
    
    return vAR_plate_code,vAR_error_message
    