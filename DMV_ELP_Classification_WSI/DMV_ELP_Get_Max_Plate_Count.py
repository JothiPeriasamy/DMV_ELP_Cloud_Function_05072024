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


def GetMaxPlateTypeCount():
    vAR_max_count = 0
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_REQUEST_RESPONSE_METADATA"
    vAR_query_job = vAR_client.query(
        " SELECT MAX_PLATE_TYPE_DESC_COUNT FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`" + " where date(created_dt) = current_date() order by created_dt desc limit 1 "
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_max_count = row.get('MAX_PLATE_TYPE_DESC_COUNT')

    if vAR_max_count==0:
        print("MAX_PLATE_TYPE_DESC_COUNT is not found in metadata table(GetMaxPlateTypeCount)")
    return vAR_max_count


def GetMaxPlateTypeCountPartial(vAR_partial_file_name,vAR_partial_file_date):
    vAR_client = bigquery.Client()
    vAR_max_count = 0
    vAR_table_name = "DMV_ELP_REQUEST_RESPONSE_METADATA"
    vAR_query_job = vAR_client.query(
        " SELECT MAX_PLATE_TYPE_DESC_COUNT FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`" + " where date(created_dt) ='"+vAR_partial_file_date +"' and lower(REQUEST_FILE_NAME) like '%"+vAR_partial_file_name+"'"+" order by created_dt desc limit 1 "
    )

    vAR_results = vAR_query_job.result()  # Waits for job to complete.
    for row in vAR_results:
        vAR_max_count = row.get('MAX_PLATE_TYPE_DESC_COUNT')

    if vAR_max_count==0:
        print("MAX_PLATE_TYPE_DESC_COUNT is not found in metadata table(GetMaxPlateTypeCountPartial)")

    return vAR_max_count