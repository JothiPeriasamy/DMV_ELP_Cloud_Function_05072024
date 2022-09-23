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
import os

def Insert_Response_to_Bigquery(vAR_df):
    created_at = []
    created_by = []
    updated_at = []
    updated_by = []
    df_length = len(vAR_df)
    created_at += df_length * [datetime.datetime.utcnow()]
    created_by += df_length * ['AWS_LAMBDA_USER']
    updated_by += df_length * ['AWS_LAMBDA_USER']
    updated_at += df_length * [datetime.datetime.utcnow()]
    vAR_df['CREATED_DT'] = created_at
    vAR_df['CREATED_USER'] = created_by
    vAR_df['UPDATED_DT'] = updated_at
    vAR_df['UPDATED_USER'] = updated_by

    # Load client
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])

    # Define table name, in format dataset.table_name
    table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RESPONSE'
    job_config = bigquery.LoadJobConfig(autodetect=True,schema=[
# Specify the type of columns whose type cannot be auto-detected. For
# data type is ambiguous.
    bigquery.SchemaField("ORDER_GROUP_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("ORDER_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("SG_ID", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("CREATED_DT", bigquery.enums.SqlTypeNames.DATETIME,mode="REQUIRED"),
            bigquery.SchemaField("CREATED_USER", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("UPDATED_DT", bigquery.enums.SqlTypeNames.DATETIME,mode="REQUIRED"),
            bigquery.SchemaField("UPDATED_USER", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("REQUEST_DATE", bigquery.enums.SqlTypeNames.STRING,mode="REQUIRED"),
            bigquery.SchemaField("REQUEST_ID", bigquery.enums.SqlTypeNames.INT64,mode="REQUIRED"),
],write_disposition="WRITE_APPEND",)
        # Load data to BQ
    job = client.load_table_from_dataframe(vAR_df, table,job_config=job_config)

    job.result()  # Wait for the job to complete.
    table_id = 'elp-2022-352222.DMV_ELP.DMV_ELP_MLOPS_RESPONSE'
    table_id = os.environ["GCP_PROJECT_ID"]+'.'+table
    table = client.get_table(table_id)  # Make an API request.
    print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )