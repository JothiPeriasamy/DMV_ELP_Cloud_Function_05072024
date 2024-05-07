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

   #  print('vaR_df cols - ',vAR_df.columns)
   #  # Config is accepted by all 4 table lookups
   #  print('vaR_df access - ',vAR_df["RNN"])
   #  print('vaR_df access type- ',type(vAR_df["RNN"]))
   #  print('vAR_df_access 2 - ',vAR_df["RNN"].to_string(index=False))
    
    if 'GPT.MODEL' in vAR_df.columns:
      vAR_query = """Insert into `{}` (REQUEST_ID,REQUEST_DATE,PST_DATE,PST_TIMESTAMP,LICENSE_PLATE_CONFIG,TOXIC,SEVERE_TOXIC,OBSCENE,IDENTITY_HATE,INSULT,THREAT,OVERALL_SCORE,MODEL,RECOMMENDATION,REASON,ERROR_MESSAGE,CREATED_DT,CREATED_USER,UPDATED_DT,UPDATED_USER,RUN_ID,REQUEST_FILE_NAME,TOXIC_REASON,SEVERE_TOXIC_REASON,OBSCENE_REASON,IDENTITY_HATE_REASON,INSULT_REASON,THREAT_REASON,RECOMMENDED_CONFIGURATION,RECOMMENDATION_REASON) values  ({},"{}","{}","{}","{}",{},{},{},{},{},{},NULL,"{}","{}","{}","{}","{}","{}","{}","{}",{},"{}","{}","{}","{}","{}","{}","{}","{}","{}")""".format(table,vAR_df.loc[0,'REQUEST_ID'],vAR_df.loc[0,'REQUEST_DATE'],vAR_df.loc[0,'PST_DATE'],vAR_df.loc[0,'PST_TIMESTAMP'],vAR_df.loc[0,'LICENSE_PLATE_CONFIG'],vAR_df.loc[0,'GPT.TOXIC'],vAR_df.loc[0,'GPT.SEVERE_TOXIC'],vAR_df.loc[0,'GPT.OBSCENE'],vAR_df.loc[0,'GPT.IDENTITY_HATE'],vAR_df.loc[0,'GPT.INSULT'],vAR_df.loc[0,'GPT.THREAT'],vAR_df.loc[0,'GPT.MODEL'],vAR_df.loc[0,'GPT.RECOMMENDATION'],vAR_df.loc[0,'GPT.REASON'],vAR_df.loc[0,'ERROR_MESSAGE'],vAR_df.loc[0,'CREATED_DT'],vAR_df.loc[0,'CREATED_USER'],vAR_df.loc[0,'UPDATED_DT'],vAR_df.loc[0,'UPDATED_USER'],vAR_df.loc[0,'RUN_ID'],vAR_df.loc[0,'REQUEST_FILE_NAME'],vAR_df.loc[0,'GPT.TOXIC_REASON'],vAR_df.loc[0,'GPT.SEVERE_TOXIC_REASON'],vAR_df.loc[0,'GPT.OBSCENE_REASON'],vAR_df.loc[0,'GPT.HATE_REASON'],vAR_df.loc[0,'GPT.INSULT_REASON'],vAR_df.loc[0,'GPT.THREAT_REASON'],vAR_df.loc[0,'GPT.RECOMMENDED_CONFIGURATION'],vAR_df.loc[0,'GPT.RECOMMENDATION_REASON'])
    # If RNN/GPT not a column in vAR_df, then config denied by table lookup
    else:
      pass

      # vAR_query = """Insert into `{}` (REQUEST_ID,REQUEST_DATE,SG_ID,LICENSE_PLATE_CONFIG,LICENSE_PLATE_DESC,DOUBLE_OR_SINGLE,TYPE,LICENSE_PLATE,VIN,ORDER_PLACED_OFFICE_ID,ORDER_DEST_OFFICE_ID,ORDER_NUMBER_CODE,ORDER_GROUP_ID,REGISTERED_OWNER_NAME,REGISTERED_OWNER_ADDRESS,REGISTERED_OWNER_CITY,REGISTERED_OWNER_ZIP,ORDER_PRINTED_DATE,TECH_ID,ORDER_PAYMENT_DATE,PLATE_CODE,BADWORDS_CLASSIFICATION,GUIDELINE_FWORD_CLASSIFICATION,PREVIOUSLY_DENIED_CLASSIFICATION,RULE_BASED_CLASSIFICATION,TOXIC,SEVERE_TOXIC,OBSCENE,IDENTITY_HATE,INSULT,THREAT,OVERALL_SCORE,MODEL,RECOMMENDATION,REASON,ERROR_MESSAGE,CREATED_DT,CREATED_USER,UPDATED_DT,UPDATED_USER,PLATE_TYPE_COUNT,PLATE_TYPE_MAX_COUNT_FLG,RUN_ID,REQUEST_FILE_NAME) values ({},"{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",parse_date('%m/%d/%E4Y',"{}"),"{}",parse_date('%m/%d/%E4Y',"{}"),"{}","{}","{}","{}","{}",NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,"{}","{}","{}","{}","{}","{}","{}",{},{},{},"{}")""".format(table,vAR_df.loc[0,'REQUEST_ID'],vAR_df.loc[0,'REQUEST_DATE'],vAR_df.loc[0,'SG_ID'],vAR_df.loc[0,'LICENSE_PLATE_CONFIG'],vAR_df.loc[0,'LICENSE_PLATE_DESC'],vAR_df.loc[0,'DOUBLE_OR_SINGLE'],vAR_df.loc[0,'TYPE'],vAR_df.loc[0,'LICENSE_PLATE'],vAR_df.loc[0,'VIN'],vAR_df.loc[0,'ORDER_PLACED_OFFICE_ID'],vAR_df.loc[0,'ORDER_DEST_OFFICE_ID'],vAR_df.loc[0,'ORDER_NUMBER_CODE'],vAR_df.loc[0,'ORDER_GROUP_ID'],vAR_df.loc[0,'REGISTERED_OWNER_NAME'],vAR_df.loc[0,'REGISTERED_OWNER_ADDRESS'],vAR_df.loc[0,'REGISTERED_OWNER_CITY'],vAR_df.loc[0,'REGISTERED_OWNER_ZIP'],vAR_df.loc[0,'ORDER_PRINTED_DATE'],vAR_df.loc[0,'TECH_ID'],vAR_df.loc[0,'ORDER_PAYMENT_DATE'],vAR_df.loc[0,'PLATE_CODE'],vAR_df.loc[0,'BADWORDS_CLASSIFICATION'],vAR_df.loc[0,'GUIDELINE_FWORD_CLASSIFICATION'],vAR_df.loc[0,'PREVIOUSLY_DENIED_CLASSIFICATION'],vAR_df.loc[0,'RULE_BASED_CLASSIFICATION'],vAR_df.loc[0,'RECOMMENDATION'],vAR_df.loc[0,'REASON'],vAR_df.loc[0,'ERROR_MESSAGE'],vAR_df.loc[0,'CREATED_DT'],vAR_df.loc[0,'CREATED_USER'],vAR_df.loc[0,'UPDATED_DT'],vAR_df.loc[0,'UPDATED_USER'],vAR_df.loc[0,'PLATE_TYPE_COUNT'],vAR_df.loc[0,'PLATE_TYPE_MAX_COUNT_FLG'],vAR_df.loc[0,'RUN_ID'],vAR_df.loc[0,'REQUEST_FILE_NAME'])





   #  if vAR_df['TOXIC'].to_string(index=False) == 'None' :
   #     vAR_query = """Insert into `{}` (REQUEST_ID,REQUEST_DATE,SG_ID,LICENSE_PLATE_CONFIG,LICENSE_PLATE_DESC,DOUBLE_OR_SINGLE,TYPE,LICENSE_PLATE,VIN,ORDER_PLACED_OFFICE_ID,ORDER_DEST_OFFICE_ID,ORDER_NUMBER_CODE,ORDER_GROUP_ID,REGISTERED_OWNER_NAME,REGISTERED_OWNER_ADDRESS,REGISTERED_OWNER_CITY,REGISTERED_OWNER_ZIP,ORDER_PRINTED_DATE,TECH_ID,ORDER_PAYMENT_DATE,PLATE_CODE,BADWORDS_CLASSIFICATION,GUIDELINE_FWORD_CLASSIFICATION,PREVIOUSLY_DENIED_CLASSIFICATION,RULE_BASED_CLASSIFICATION,TOXIC,SEVERE_TOXIC,OBSCENE,IDENTITY_HATE,INSULT,THREAT,OVERALL_SCORE,MODEL,RECOMMENDATION,REASON,ERROR_MESSAGE,CREATED_DT,CREATED_USER,UPDATED_DT,UPDATED_USER,PLATE_TYPE_COUNT,PLATE_TYPE_MAX_COUNT_FLG,RUN_ID,REQUEST_FILE_NAME) values ({},"{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",parse_date('%m/%d/%E4Y',"{}"),"{}",parse_date('%m/%d/%E4Y',"{}"),"{}","{}","{}","{}","{}",NULL,NULL,NULL,NULL,NULL,NULL,NULL,"{}","{}","{}","{}","{}","{}","{}","{}",{},{},{},"{}")""".format(table,vAR_df['REQUEST_ID'].to_string(index=False),vAR_df['REQUEST_DATE'].to_string(index=False),vAR_df['SG_ID'].to_string(index=False),vAR_df['LICENSE_PLATE_CONFIG'].to_string(index=False),vAR_df['LICENSE_PLATE_DESC'].to_string(index=False),vAR_df['DOUBLE_OR_SINGLE'].to_string(index=False),vAR_df['TYPE'].to_string(index=False),vAR_df['LICENSE_PLATE'].to_string(index=False),vAR_df['VIN'].to_string(index=False),vAR_df['ORDER_PLACED_OFFICE_ID'].to_string(index=False),vAR_df['ORDER_DEST_OFFICE_ID'].to_string(index=False),vAR_df['ORDER_NUMBER_CODE'].to_string(index=False),vAR_df['ORDER_GROUP_ID'].to_string(index=False),vAR_df['REGISTERED_OWNER_NAME'].to_string(index=False),vAR_df['REGISTERED_OWNER_ADDRESS'].to_string(index=False),vAR_df['REGISTERED_OWNER_CITY'].to_string(index=False),vAR_df['REGISTERED_OWNER_ZIP'].to_string(index=False),vAR_df['ORDER_PRINTED_DATE'].to_string(index=False),vAR_df['TECH_ID'].to_string(index=False),vAR_df['ORDER_PAYMENT_DATE'].to_string(index=False),vAR_df['PLATE_CODE'].to_string(index=False),vAR_df['BADWORDS_CLASSIFICATION'].to_string(index=False),vAR_df['GUIDELINE_FWORD_CLASSIFICATION'].to_string(index=False),vAR_df['PREVIOUSLY_DENIED_CLASSIFICATION'].to_string(index=False),vAR_df['RULE_BASED_CLASSIFICATION'].to_string(index=False),vAR_df['MODEL'].to_string(index=False),vAR_df['RECOMMENDATION'].to_string(index=False),vAR_df['REASON'].to_string(index=False),vAR_df['ERROR_MESSAGE'].to_string(index=False),vAR_df['CREATED_DT'].to_string(index=False),vAR_df['CREATED_USER'].to_string(index=False),vAR_df['UPDATED_DT'].to_string(index=False),vAR_df['UPDATED_USER'].to_string(index=False),vAR_df['PLATE_TYPE_COUNT'].to_string(index=False),vAR_df['PLATE_TYPE_MAX_COUNT_FLG'].to_string(index=False),vAR_df['RUN_ID'].to_string(index=False),vAR_df['REQUEST_FILE_NAME'].to_string(index=False))
   #  else:
   #     vAR_query = """Insert into `{}` (REQUEST_ID,REQUEST_DATE,SG_ID,LICENSE_PLATE_CONFIG,LICENSE_PLATE_DESC,DOUBLE_OR_SINGLE,TYPE,LICENSE_PLATE,VIN,ORDER_PLACED_OFFICE_ID,ORDER_DEST_OFFICE_ID,ORDER_NUMBER_CODE,ORDER_GROUP_ID,REGISTERED_OWNER_NAME,REGISTERED_OWNER_ADDRESS,REGISTERED_OWNER_CITY,REGISTERED_OWNER_ZIP,ORDER_PRINTED_DATE,TECH_ID,ORDER_PAYMENT_DATE,PLATE_CODE,BADWORDS_CLASSIFICATION,GUIDELINE_FWORD_CLASSIFICATION,PREVIOUSLY_DENIED_CLASSIFICATION,RULE_BASED_CLASSIFICATION,TOXIC,SEVERE_TOXIC,OBSCENE,IDENTITY_HATE,INSULT,THREAT,OVERALL_SCORE,MODEL,RECOMMENDATION,REASON,ERROR_MESSAGE,CREATED_DT,CREATED_USER,UPDATED_DT,UPDATED_USER,PLATE_TYPE_COUNT,PLATE_TYPE_MAX_COUNT_FLG,RUN_ID,REQUEST_FILE_NAME,TOXIC_REASON,SEVERE_TOXIC_REASON,OBSCENE_REASON,IDENTITY_HATE_REASON,INSULT_REASON,THREAT_REASON,RECOMMENDED_CONFIGURATION,RECOMMENDATION_REASON) values ({},"{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",parse_date('%m/%d/%E4Y',"{}"),"{}",parse_date('%m/%d/%E4Y',"{}"),"{}","{}","{}","{}","{}",{},{},{},{},{},{},NULL,"{}","{}","{}","{}","{}","{}","{}","{}",{},{},{},"{}","{}","{}","{}","{}","{}","{}","{}","{}")""".format(table,vAR_df['REQUEST_ID'].to_string(index=False),vAR_df['REQUEST_DATE'].to_string(index=False),vAR_df['SG_ID'].to_string(index=False),vAR_df['LICENSE_PLATE_CONFIG'].to_string(index=False),vAR_df['LICENSE_PLATE_DESC'].to_string(index=False),vAR_df['DOUBLE_OR_SINGLE'].to_string(index=False),vAR_df['TYPE'].to_string(index=False),vAR_df['LICENSE_PLATE'].to_string(index=False),vAR_df['VIN'].to_string(index=False),vAR_df['ORDER_PLACED_OFFICE_ID'].to_string(index=False),vAR_df['ORDER_DEST_OFFICE_ID'].to_string(index=False),vAR_df['ORDER_NUMBER_CODE'].to_string(index=False),vAR_df['ORDER_GROUP_ID'].to_string(index=False),vAR_df['REGISTERED_OWNER_NAME'].to_string(index=False),vAR_df['REGISTERED_OWNER_ADDRESS'].to_string(index=False),vAR_df['REGISTERED_OWNER_CITY'].to_string(index=False),vAR_df['REGISTERED_OWNER_ZIP'].to_string(index=False),vAR_df['ORDER_PRINTED_DATE'].to_string(index=False),vAR_df['TECH_ID'].to_string(index=False),vAR_df['ORDER_PAYMENT_DATE'].to_string(index=False),vAR_df['PLATE_CODE'].to_string(index=False),vAR_df['BADWORDS_CLASSIFICATION'].to_string(index=False),vAR_df['GUIDELINE_FWORD_CLASSIFICATION'].to_string(index=False),vAR_df['PREVIOUSLY_DENIED_CLASSIFICATION'].to_string(index=False),vAR_df['RULE_BASED_CLASSIFICATION'].to_string(index=False),vAR_df['TOXIC'].to_string(index=False),vAR_df['SEVERE_TOXIC'].to_string(index=False),vAR_df['OBSCENE'].to_string(index=False),vAR_df['IDENTITY_HATE'].to_string(index=False),vAR_df['INSULT'].to_string(index=False),vAR_df['THREAT'].to_string(index=False),vAR_df['MODEL'].to_string(index=False),vAR_df['RECOMMENDATION'].to_string(index=False),vAR_df['REASON'].to_string(index=False),vAR_df['ERROR_MESSAGE'].to_string(index=False),vAR_df['CREATED_DT'].to_string(index=False),vAR_df['CREATED_USER'].to_string(index=False),vAR_df['UPDATED_DT'].to_string(index=False),vAR_df['UPDATED_USER'].to_string(index=False),vAR_df['PLATE_TYPE_COUNT'].to_string(index=False),vAR_df['PLATE_TYPE_MAX_COUNT_FLG'].to_string(index=False),vAR_df['RUN_ID'].to_string(index=False),vAR_df['REQUEST_FILE_NAME'].to_string(index=False),vAR_df['TOXIC_REASON'].to_string(index=False),vAR_df['SEVERE_TOXIC_REASON'].to_string(index=False),vAR_df['OBSCENE_REASON'].to_string(index=False),vAR_df['HATE_REASON'].to_string(index=False),vAR_df['INSULT_REASON'].to_string(index=False),vAR_df['THREAT_REASON'].to_string(index=False),vAR_df['RECOMMENDED_CONFIGURATION'].to_string(index=False),vAR_df['RECOMMENDATION_REASON'].to_string(index=False))
    
    print('Insert response table query - ',vAR_query)

    vAR_job = client.query(vAR_query)
    vAR_job.result()
    vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
    print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)





def ReadResponseTable():
    vAR_client = bigquery.Client()
    vAR_response_table_name = "DMV_ELP_MLOPS_RESPONSE"


    vAR_sql =(
        """
        SELECT * FROM `{}.{}.{}`
WHERE
      MODEL IS NOT NULL AND RUN_ID=(select max(RUN_ID) from `{}.{}.{}` where date(created_dt)=current_date())
ORDER BY
  LICENSE_PLATE_CONFIG ASC
""".format(os.environ["GCP_PROJECT_ID"],os.environ["GCP_BQ_SCHEMA_NAME"],vAR_response_table_name,os.environ["GCP_PROJECT_ID"],os.environ["GCP_BQ_SCHEMA_NAME"],vAR_response_table_name)
    )

    vAR_df = vAR_client.query(vAR_sql).to_dataframe()
   #  Remove Duplicate If there's any
    vAR_newdf = vAR_df.drop_duplicates(subset = ['LICENSE_PLATE_CONFIG'],keep = 'last').reset_index(drop = True)


    return vAR_newdf