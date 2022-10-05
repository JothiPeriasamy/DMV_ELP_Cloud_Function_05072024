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
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_BADWORDS"
    vAR_sql = " SELECT * FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"` order by badword_desc "
    vAR_badwords_df = vAR_client.query(vAR_sql).to_dataframe()
    # vAR_badwords_df = pd.read_csv('gs://dmv_elp_project/data/badwords_list.csv',header=None)
    vAR_result_message = ""
    
#---------------Profanity logic implementation with O(log n) time complexity-------------------
    # Direct profanity check
    vAR_badwords_df['BADWORD_DESC'] = vAR_badwords_df['BADWORD_DESC'].str.upper()
    vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df['BADWORD_DESC'],vAR_input)
    if vAR_is_input_in_profanity_list!=-1:
        vAR_result_message = 'Input ' +vAR_val+ ' matches with direct profanity - '+vAR_badwords_df['BADWORD_DESC'][vAR_is_input_in_profanity_list]
        
        return True,vAR_result_message
    
    # Reversal profanity check
    vAR_reverse_input = "".join(reversed(vAR_input)).upper()
    vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df['BADWORD_DESC'],vAR_reverse_input)
    if vAR_is_input_in_profanity_list!=-1:
        vAR_result_message = 'Input ' +vAR_val+ ' matches with reversal profanity - '+vAR_badwords_df['BADWORD_DESC'][vAR_is_input_in_profanity_list]
        return True,vAR_result_message
    
    # Number replacement profanity check
    vAR_number_replaced = Number_Replacement(vAR_input).upper()
    vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df['BADWORD_DESC'],vAR_number_replaced)
    if vAR_is_input_in_profanity_list!=-1: 
       vAR_result_message = 'Input ' +vAR_val+ ' matches with number replacement profanity - '+vAR_badwords_df['BADWORD_DESC'][vAR_is_input_in_profanity_list]
       return True,vAR_result_message
    
    # Reversal Number replacement profanity check(5sa->as5->ass)
    vAR_number_replaced = Number_Replacement(vAR_reverse_input).upper()
    vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df['BADWORD_DESC'],vAR_number_replaced)
    if vAR_is_input_in_profanity_list!=-1:  
        vAR_result_message = 'Input ' +vAR_val+ ' matches with reversal number replacement profanity - '+vAR_badwords_df['BADWORD_DESC'][vAR_is_input_in_profanity_list]
        return True,vAR_result_message

    # Mirror word check
    # vAR_mirror_string = MirrorString(vAR_val)
    # if vAR_mirror_string is not None:
    #     vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df['BADWORD_DESC'],vAR_mirror_string)
    #     if vAR_is_input_in_profanity_list!=-1:
    #         vAR_result_message = 'Input ' +vAR_val+ ' matches with mirror word profanity - '+vAR_badwords_df['BADWORD_DESC'][vAR_is_input_in_profanity_list]
    #         return True,vAR_result_message
    # else:
    #     print('Mirror string found as none')


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



def Binary_Search(data, x):
    vAR_low = 0
    vAR_high = len(data) - 1
    vAR_mid = 0
    i =0
    while vAR_low <= vAR_high:
        i = i+1
        vAR_mid = (vAR_high + vAR_low) // 2
        
        # If x is greater, ignore left half
        if data[vAR_mid] < x:
            vAR_low = vAR_mid + 1
 
        # If x is smaller, ignore right half
        elif data[vAR_mid] > x:
            vAR_high = vAR_mid - 1
 
        # means x is present at mid
        else:
            return vAR_mid
 
    # If we reach here, then the element was not present
    return -1

def MirrorString(vAR_input):
    vAR_input = vAR_input.replace('/','')
    vAR_input = vAR_input.replace('*','')
    vAR_mirror_chars={'Z':'S','3':'E','S':'Z','E':'3','T':'T','I':'I','H':'H'}
    try:
        return "".join(vAR_mirror_chars[char] for char in reversed(vAR_input.upper()))
    except KeyError as e:
        print('Key Error in mirror - ',str(e))
        pass