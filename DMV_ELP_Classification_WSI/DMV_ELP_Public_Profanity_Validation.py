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



def Profanity_Words_Check(vAR_val):
    vAR_input = vAR_val.replace('/','')
    vAR_input = vAR_val.replace('*','')
    vAR_response_count = 0
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_BADWORDS"
    vAR_result_message = ""


    vAR_reverse_input = "".join(reversed(vAR_input)).upper()
    vAR_number_replaced = Number_Replacement(vAR_input).upper()
    vAR_rev_number_replaced = Number_Replacement(vAR_reverse_input).upper()
    vAR_direct_profanity_sql = " SELECT count(1) as cnt FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"` where upper(BADWORD_DESC)='"+vAR_input+"' OR upper(BADWORD_DESC)='"+vAR_reverse_input+"' OR upper(BADWORD_DESC)='"+vAR_number_replaced+"' OR upper(BADWORD_DESC)='"+vAR_rev_number_replaced+"'"


    
    #Profanity check

    vAR_query_job = vAR_client.query(vAR_direct_profanity_sql)
    vAR_results = vAR_query_job.result()
    for row in vAR_results:
        vAR_response_count = row.get('cnt')
    if vAR_response_count>=1:
        vAR_result_message = 'Input ' +vAR_val+ ' matches with profanity words list.'
        return True,vAR_result_message

    return False,vAR_result_message




def Number_Replacement(vAR_val):
    vAR_output = vAR_val
    if "1" in vAR_val:
        vAR_output = vAR_output.replace("1","I")
    if "2" in vAR_val:
        vAR_output = vAR_output.replace("2","Z")
    if "3" in vAR_val:
        vAR_output = vAR_output.replace("3","E")
    if "4" in vAR_val:
        vAR_output = vAR_output.replace("4","A")
    if "5" in vAR_val:
        vAR_output = vAR_output.replace("5","S")
    if "8" in vAR_val:
        vAR_output = vAR_output.replace("8","B")
    if "0" in vAR_val:
        vAR_output = vAR_output.replace("0","O")
    print('number replace - ',vAR_output)
    return vAR_output




def MirrorString(vAR_input):
    vAR_input = vAR_input.replace('/','')
    vAR_input = vAR_input.replace('*','')
    vAR_mirror_chars={'Z':'S','3':'E','S':'Z','E':'3','T':'T','I':'I','H':'H'}
    try:
        return "".join(vAR_mirror_chars[char] for char in reversed(vAR_input.upper()))
    except KeyError as e:
        print('Key Error in mirror - ',str(e))
        pass