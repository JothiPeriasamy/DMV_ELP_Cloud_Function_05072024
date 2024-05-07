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




def ReadRecordsToUpdate():
    vAR_client = bigquery.Client()
    vAR_response_table_name = "DMV_ELP_MLOPS_RESPONSE"
    vAR_sql =(
        "select LICENSE_PLATE_CONFIG from `"+ os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_response_table_name+"`"+" where REQUEST_DATE between '"+os.environ["UPDATE_FROM_DATE"]+"' and '"+os.environ["UPDATE_TO_DATE"]+"' and MODEL!='GPT'"
    )

    vAR_df = vAR_client.query(vAR_sql).to_dataframe()
    return vAR_df



def UpdateGPTRecords(vAR_input,vAR_response):

   vAR_num_of_updated_row =0
   vAR_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
   vAR_table_name = "DMV_ELP_MLOPS_RESPONSE"

   updated_at = []
   updated_at += 1 * [datetime.datetime.utcnow()]
   vAR_response['UPDATED_DT'] = updated_at

   # query = "UPDATE "+"`"+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+"SET TOXIC ="+str(vAR_response["TOXIC"])+",SEVERE_TOXIC= "+str(vAR_response["SEVERE_TOXIC"])+",OBSCENE= "+str(vAR_response["OBSCENE"])+",INSULT= "+str(vAR_response["INSULT"])+",IDENTITY_HATE= "+str(vAR_response["IDENTITY_HATE"])+",THREAT="+str(vAR_response["THREAT"])+",SEVERE_TOXIC_REASON= "+vAR_response["SEVERE_TOXIC_REASON"]+",OBSCENE_REASON= "+vAR_response["OBSCENE_REASON"]+",INSULT_REASON= "+vAR_response["INSULT_REASON"]+",IDENTITY_HATE_REASON= "+vAR_response["IDENTITY_HATE_REASON"]+",THREAT_REASON="+vAR_response["THREAT_REASON"]+",TOXIC_REASON="+vAR_response["TOXIC_REASON"]+",MODEL= "+vAR_response["MODEL"]+",RECOMMENDATION= "+vAR_response["RECOMMENDATION"]+",REASON= "+vAR_response["REASON"]+",RECOMMENDED_CONFIGURATION="+vAR_response["RECOMMENDED_CONFIGURATION"]+",RECOMMENDATION_REASON="+vAR_response["RECOMMENDATION_REASON"]+" WHERE upper(LICENSE_PLATE_CONFIG)='"+vAR_input.upper()+"'"

   query = """UPDATE `{}.{}` SET TOXIC ={},SEVERE_TOXIC={},OBSCENE={},INSULT= {},IDENTITY_HATE= {},THREAT={},SEVERE_TOXIC_REASON= "{}",OBSCENE_REASON= "{}",INSULT_REASON= "{}",IDENTITY_HATE_REASON= "{}",THREAT_REASON="{}",TOXIC_REASON="{}",MODEL= "{}",RECOMMENDATION= "{}",REASON= "{}",RECOMMENDED_CONFIGURATION="{}",RECOMMENDATION_REASON="{}",OVERALL_SCORE=NULL,UPDATED_DT=current_datetime() WHERE upper(LICENSE_PLATE_CONFIG)="{}" """.format(os.environ["GCP_BQ_SCHEMA_NAME"],vAR_table_name,vAR_response["TOXIC"],vAR_response["SEVERE_TOXIC"],vAR_response["OBSCENE"],vAR_response["INSULT"],vAR_response["IDENTITY_HATE"],vAR_response["THREAT"],vAR_response["SEVERE_TOXIC_REASON"],vAR_response["OBSCENE_REASON"],vAR_response["INSULT_REASON"],vAR_response["IDENTITY_HATE_REASON"],vAR_response["THREAT_REASON"],vAR_response["TOXIC_REASON"],vAR_response["MODEL"],vAR_response["RECOMMENDATION"],vAR_response["REASON"],vAR_response["RECOMMENDED_CONFIGURATION"],vAR_response["RECOMMENDATION_REASON"],vAR_input.upper())
   print('GPT Update query - ',query)
   vAR_job = vAR_client.query(query)
   vAR_job.result()
   vAR_num_of_updated_row = vAR_job.num_dml_affected_rows
   

   return vAR_num_of_updated_row

   

