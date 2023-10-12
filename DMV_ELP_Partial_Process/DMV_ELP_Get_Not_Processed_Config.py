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
from google.cloud import bigquery
from google.cloud import storage
import os
import json

from DMV_ELP_Bigquery_Utility_Method import ReadRequestFileFromGCP,GetNotProcessedRecord,GetMetadataTotalRecordsToProcess,GetResponseCountForGivenDateAndFile,PreProcessingReport





def FilterNotProcessedRecordsFromGCSRequestFile(vAR_partial_file_date,vAR_partial_file_name):

    # - Read response table - date&filename
	# - Read total records to process from metadata table - date&filename
	# - Read request table - date&filename
	# - Read gcp request file & insert records into request table which is not in response&request
    
    vAR_query_request_df,vAR_query_response_df,vAR_query_request_df_len,vAR_query_response_df_len = GetNotProcessedRecord(vAR_partial_file_date,vAR_partial_file_name)


    vAR_gcs_request_df = ReadRequestFileFromGCP(vAR_partial_file_date,vAR_partial_file_name)

    vAR_given_date_request_count = GetMetadataTotalRecordsToProcess(vAR_partial_file_date,vAR_partial_file_name)

    vAR_given_date_response_count,vAR_run_id = GetResponseCountForGivenDateAndFile(vAR_partial_file_date,vAR_partial_file_name)
    PreProcessingReport(vAR_given_date_request_count,vAR_given_date_response_count,vAR_query_request_df_len)

    print('vAR_gcs_request_df columns - ',vAR_gcs_request_df.columns)
    print('vAR_query_response_df columns - ',vAR_query_response_df.columns)

    print('vAR_gcs_request_df len - ',len(vAR_gcs_request_df))
    print('vAR_query_response_df len - ',len(vAR_query_response_df))

    # Filter not processed records from gcp request file(request file config not in response table)
    vAR_not_processed_from_gcs_request_df = vAR_gcs_request_df[~vAR_gcs_request_df['LICENSE_PLATE_CONFIG'].isin(vAR_query_response_df['LICENSE_PLATE_CONFIG'])]
    print('vAR_not_processed_from_gcs_request_df LENGTH((total request file config not in response table) - ',len(vAR_not_processed_from_gcs_request_df))

    

    # This is to check, if not processed records already exists in request table.So, there won't be any duplicate record.
    vAR_not_processed_from_gcs_request_df = vAR_not_processed_from_gcs_request_df[~vAR_not_processed_from_gcs_request_df['LICENSE_PLATE_CONFIG'].isin(vAR_query_request_df['LICENSE_PLATE_CONFIG'])]

    if 'Unnamed: 0' in vAR_not_processed_from_gcs_request_df:
        vAR_not_processed_from_gcs_request_df = vAR_not_processed_from_gcs_request_df.drop(['Unnamed: 0'],axis=1)

    print('vAR_not_processed_from_gcs_request_df LENGTH((total request file config not in request table) - ',len(vAR_not_processed_from_gcs_request_df))

    return vAR_not_processed_from_gcs_request_df


