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

# Commenting below code , because this insertion method throws quota exceeded error(with batch job insert approach)

# def Insert_Response_to_Bigquery(vAR_df):
#     created_at = []
#     created_by = []
#     updated_at = []
#     updated_by = []
#     df_length = len(vAR_df)
#     created_at += df_length * [datetime.datetime.utcnow()]
#     created_by += df_length * [os.environ['GCP_USER']]
#     updated_by += df_length * [os.environ['GCP_USER']]
#     updated_at += df_length * [datetime.datetime.utcnow()]
#     vAR_df['CREATED_DT'] = created_at
#     vAR_df['CREATED_USER'] = created_by
#     vAR_df['UPDATED_DT'] = updated_at
#     vAR_df['UPDATED_USER'] = updated_by

#     # Load client
#     client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])

#     # Define table name, in format dataset.table_name
#     table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RESPONSE'
#     job_config = bigquery.LoadJobConfig(autodetect=True,write_disposition="WRITE_APPEND",source_format=bigquery.SourceFormat.CSV,allow_quoted_newlines = True)
#     # Load data to BQ
#     job = client.load_table_from_dataframe(vAR_df, table,job_config=job_config)

#     job.result()  # Wait for the job to complete.
#     table_id = os.environ["GCP_PROJECT_ID"]+'.'+table
#     table = client.get_table(table_id)  # Make an API request.
#     print(
#             "Loaded {} rows and {} columns to {}".format(
#                 table.num_rows, len(table.schema), table_id
#             )
#         )




def Insert_Response_to_Bigquery(vAR_df):
    created_at = []
    created_by = []
    updated_at = []
    updated_by = []
    created_at += 1 * [datetime.datetime.utcnow()]
    created_by += 1 * [os.environ['GCP_USER']]
    updated_by += 1 * [os.environ['GCP_USER']]
    updated_at += 1 * [datetime.datetime.utcnow()]
    vAR_df['CREATED_DT'] = created_at
    vAR_df['CREATED_USER'] = created_by
    vAR_df['UPDATED_DT'] = updated_at
    vAR_df['UPDATED_USER'] = updated_by
    vAR_seperator = "','"

    # Load client
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])

   #  print('value 1 - ',vAR_df['REQUEST_ID'])
   #  print('value 2 - ',str(vAR_df['REQUEST_ID']))
   #  print('value 3 - ',vAR_df['REQUEST_ID'].to_string(index=False))
   #  print('value 4 - ',vAR_df['TOXIC'].to_string(index=False))
   #  print('value 4 type- ',type(vAR_df['TOXIC'].to_string(index=False)))
    
    # Define table name, in format dataset.table_name
    table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RESPONSE'

   #  parse_date reference link - https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements

    if vAR_df['TOXIC'].to_string(index=False) == 'None' :
       vAR_query = """Insert into `{}` (REQUEST_ID,REQUEST_DATE,SG_ID,LICENSE_PLATE_CONFIG,LICENSE_PLATE_DESC,DOUBLE_OR_SINGLE,TYPE,LICENSE_PLATE,VIN,ORDER_PLACED_OFFICE_ID,ORDER_DEST_OFFICE_ID,ORDER_NUMBER_CODE,ORDER_GROUP_ID,REGISTERED_OWNER_NAME,REGISTERED_OWNER_ADDRESS,REGISTERED_OWNER_CITY,REGISTERED_OWNER_ZIP,ORDER_PRINTED_DATE,TECH_ID,ORDER_PAYMENT_DATE,PLATE_CODE,BADWORDS_CLASSIFICATION,GUIDELINE_FWORD_CLASSIFICATION,PREVIOUSLY_DENIED_CLASSIFICATION,RULE_BASED_CLASSIFICATION,TOXIC,SEVERE_TOXIC,OBSCENE,IDENTITY_HATE,INSULT,THREAT,OVERALL_SCORE,MODEL,RECOMMENDATION,REASON,ERROR_MESSAGE,CREATED_DT,CREATED_USER,UPDATED_DT,UPDATED_USER,PLATE_TYPE_COUNT,PLATE_TYPE_MAX_COUNT_FLG,RUN_ID,REQUEST_FILE_NAME) values ({},"{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",parse_date('%m/%d/%E4Y',"{}"),"{}",parse_date('%m/%d/%E4Y',"{}"),"{}","{}","{}","{}","{}",NULL,NULL,NULL,NULL,NULL,NULL,NULL,"{}","{}","{}","{}","{}","{}","{}","{}",{},{},{},"{}")""".format(table,vAR_df['REQUEST_ID'].to_string(index=False),vAR_df['REQUEST_DATE'].to_string(index=False),vAR_df['SG_ID'].to_string(index=False),vAR_df['LICENSE_PLATE_CONFIG'].to_string(index=False),vAR_df['LICENSE_PLATE_DESC'].to_string(index=False),vAR_df['DOUBLE_OR_SINGLE'].to_string(index=False),vAR_df['TYPE'].to_string(index=False),vAR_df['LICENSE_PLATE'].to_string(index=False),vAR_df['VIN'].to_string(index=False),vAR_df['ORDER_PLACED_OFFICE_ID'].to_string(index=False),vAR_df['ORDER_DEST_OFFICE_ID'].to_string(index=False),vAR_df['ORDER_NUMBER_CODE'].to_string(index=False),vAR_df['ORDER_GROUP_ID'].to_string(index=False),vAR_df['REGISTERED_OWNER_NAME'].to_string(index=False),vAR_df['REGISTERED_OWNER_ADDRESS'].to_string(index=False),vAR_df['REGISTERED_OWNER_CITY'].to_string(index=False),vAR_df['REGISTERED_OWNER_ZIP'].to_string(index=False),vAR_df['ORDER_PRINTED_DATE'].to_string(index=False),vAR_df['TECH_ID'].to_string(index=False),vAR_df['ORDER_PAYMENT_DATE'].to_string(index=False),vAR_df['PLATE_CODE'].to_string(index=False),vAR_df['BADWORDS_CLASSIFICATION'].to_string(index=False),vAR_df['GUIDELINE_FWORD_CLASSIFICATION'].to_string(index=False),vAR_df['PREVIOUSLY_DENIED_CLASSIFICATION'].to_string(index=False),vAR_df['RULE_BASED_CLASSIFICATION'].to_string(index=False),vAR_df['MODEL'].to_string(index=False),vAR_df['RECOMMENDATION'].to_string(index=False),vAR_df['REASON'].to_string(index=False),vAR_df['ERROR_MESSAGE'].to_string(index=False),vAR_df['CREATED_DT'].to_string(index=False),vAR_df['CREATED_USER'].to_string(index=False),vAR_df['UPDATED_DT'].to_string(index=False),vAR_df['UPDATED_USER'].to_string(index=False),vAR_df['PLATE_TYPE_COUNT'].to_string(index=False),vAR_df['PLATE_TYPE_MAX_COUNT_FLG'].to_string(index=False),vAR_df['RUN_ID'].to_string(index=False),vAR_df['REQUEST_FILE_NAME'].to_string(index=False))
    else:
       vAR_query = """Insert into `{}` (REQUEST_ID,REQUEST_DATE,SG_ID,LICENSE_PLATE_CONFIG,LICENSE_PLATE_DESC,DOUBLE_OR_SINGLE,TYPE,LICENSE_PLATE,VIN,ORDER_PLACED_OFFICE_ID,ORDER_DEST_OFFICE_ID,ORDER_NUMBER_CODE,ORDER_GROUP_ID,REGISTERED_OWNER_NAME,REGISTERED_OWNER_ADDRESS,REGISTERED_OWNER_CITY,REGISTERED_OWNER_ZIP,ORDER_PRINTED_DATE,TECH_ID,ORDER_PAYMENT_DATE,PLATE_CODE,BADWORDS_CLASSIFICATION,GUIDELINE_FWORD_CLASSIFICATION,PREVIOUSLY_DENIED_CLASSIFICATION,RULE_BASED_CLASSIFICATION,TOXIC,SEVERE_TOXIC,OBSCENE,IDENTITY_HATE,INSULT,THREAT,OVERALL_SCORE,MODEL,RECOMMENDATION,REASON,ERROR_MESSAGE,CREATED_DT,CREATED_USER,UPDATED_DT,UPDATED_USER,PLATE_TYPE_COUNT,PLATE_TYPE_MAX_COUNT_FLG,RUN_ID,REQUEST_FILE_NAME) values ({},"{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",parse_date('%m/%d/%E4Y',"{}"),"{}",parse_date('%m/%d/%E4Y',"{}"),"{}","{}","{}","{}","{}",{},{},{},{},{},{},{},"{}","{}","{}","{}","{}","{}","{}","{}",{},{},{},"{}")""".format(table,vAR_df['REQUEST_ID'].to_string(index=False),vAR_df['REQUEST_DATE'].to_string(index=False),vAR_df['SG_ID'].to_string(index=False),vAR_df['LICENSE_PLATE_CONFIG'].to_string(index=False),vAR_df['LICENSE_PLATE_DESC'].to_string(index=False),vAR_df['DOUBLE_OR_SINGLE'].to_string(index=False),vAR_df['TYPE'].to_string(index=False),vAR_df['LICENSE_PLATE'].to_string(index=False),vAR_df['VIN'].to_string(index=False),vAR_df['ORDER_PLACED_OFFICE_ID'].to_string(index=False),vAR_df['ORDER_DEST_OFFICE_ID'].to_string(index=False),vAR_df['ORDER_NUMBER_CODE'].to_string(index=False),vAR_df['ORDER_GROUP_ID'].to_string(index=False),vAR_df['REGISTERED_OWNER_NAME'].to_string(index=False),vAR_df['REGISTERED_OWNER_ADDRESS'].to_string(index=False),vAR_df['REGISTERED_OWNER_CITY'].to_string(index=False),vAR_df['REGISTERED_OWNER_ZIP'].to_string(index=False),vAR_df['ORDER_PRINTED_DATE'].to_string(index=False),vAR_df['TECH_ID'].to_string(index=False),vAR_df['ORDER_PAYMENT_DATE'].to_string(index=False),vAR_df['PLATE_CODE'].to_string(index=False),vAR_df['BADWORDS_CLASSIFICATION'].to_string(index=False),vAR_df['GUIDELINE_FWORD_CLASSIFICATION'].to_string(index=False),vAR_df['PREVIOUSLY_DENIED_CLASSIFICATION'].to_string(index=False),vAR_df['RULE_BASED_CLASSIFICATION'].to_string(index=False),vAR_df['TOXIC'].to_string(index=False),vAR_df['SEVERE_TOXIC'].to_string(index=False),vAR_df['OBSCENE'].to_string(index=False),vAR_df['IDENTITY_HATE'].to_string(index=False),vAR_df['INSULT'].to_string(index=False),vAR_df['THREAT'].to_string(index=False),vAR_df['OVERALL_SCORE'].to_string(index=False),vAR_df['MODEL'].to_string(index=False),vAR_df['RECOMMENDATION'].to_string(index=False),vAR_df['REASON'].to_string(index=False),vAR_df['ERROR_MESSAGE'].to_string(index=False),vAR_df['CREATED_DT'].to_string(index=False),vAR_df['CREATED_USER'].to_string(index=False),vAR_df['UPDATED_DT'].to_string(index=False),vAR_df['UPDATED_USER'].to_string(index=False),vAR_df['PLATE_TYPE_COUNT'].to_string(index=False),vAR_df['PLATE_TYPE_MAX_COUNT_FLG'].to_string(index=False),vAR_df['RUN_ID'].to_string(index=False),vAR_df['REQUEST_FILE_NAME'].to_string(index=False))
    
    print('Insert response table query - ',vAR_query)

    vAR_job = client.query(vAR_query)
    vAR_job.result()
    vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
    print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)