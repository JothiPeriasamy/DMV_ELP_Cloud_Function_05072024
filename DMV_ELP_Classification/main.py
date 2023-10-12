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


import json
import traceback
import os
import pandas as pd

from DMV_ELP_Request_PreValidation import Pre_Request_Validation
from DMV_ELP_Public_Profanity_Validation import Profanity_Words_Check
from DMV_ELP_GuideLine_FWord_Validation import FWord_Validation
from DMV_ELP_Previously_Denied_Config_Validation import Previously_Denied_Configuration_Validation

from DMV_ELP_Pattern_Denial import Pattern_Denial

# from DMV_ELP_BERT_Test import BERT_Model_Result
from DMV_ELP_BERT_Model_Prediction import BERT_Model_Result
from DMV_ELP_LSTM_Model_Prediction import LSTM_Model_Result
from DMV_ELP_Get_License_Plate_Code import GetPlateCode
from DMV_ELP_Get_Max_Plate_Count import GetMaxPlateTypeCount,GetMaxPlateTypeCountPartial
from DMV_ChatGPT_Recommendation import ELP_Recommendation

def ELP_Validation(request):
    
    vAR_partial_file_name = ""
    vAR_partial_file_date = ""
    vAR_error_message = {}
    request_json = request.get_json()
    print('request_json - ',request_json)
    print('request_json type- ',type(request_json))
    if 'PARTIAL_FILE_NAME' in request_json.keys() and 'PARTIAL_FILE_DATE' in request_json.keys():
        vAR_partial_file_name = request_json['PARTIAL_FILE_NAME']
        vAR_partial_file_date = request_json['PARTIAL_FILE_DATE']
        vAR_max_plate_type_count = GetMaxPlateTypeCountPartial(vAR_partial_file_name,vAR_partial_file_date)
        
        del request_json['PARTIAL_FILE_NAME']
        del request_json['PARTIAL_FILE_DATE']
    else:
        vAR_max_plate_type_count = GetMaxPlateTypeCount()


    response_json = request_json.copy()
    


    try:
        # To resolve container error(TypeError: Descriptors cannot not be created directly)
        os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION']='python'

        
        vAR_input_text = request_json['LICENSE_PLATE_CONFIG'].upper()
        vAR_license_plate_desc = request_json['LICENSE_PLATE_DESC']
        
        

        
        vAR_result_message = ""
        
        vAR_error_message = Pre_Request_Validation(request_json,vAR_partial_file_name,vAR_partial_file_date)

        vAR_sg_id = 1
        vAR_platecode_error_message = ""
        vAR_plate_code,vAR_platecode_error_message = GetPlateCode(vAR_license_plate_desc)
        
        
        
        if len(vAR_error_message["Error Message"])==0:
            if len(vAR_platecode_error_message)>0:
                vAR_error_message['Error Message'] = vAR_platecode_error_message
            # It can be changed later
            # vAR_model = os.environ["MODEL"]
            response_json["SG_ID"] = vAR_sg_id

            if vAR_max_plate_type_count is not None and request_json["PLATE_TYPE_COUNT"]==vAR_max_plate_type_count :
                response_json["PLATE_TYPE_MAX_COUNT_FLG"] = 1
            else:
                response_json["PLATE_TYPE_MAX_COUNT_FLG"] = 0
                
            response_json["PLATE_CODE"] = vAR_plate_code
            response_json["ERROR_MESSAGE"] = vAR_error_message["Error Message"]

            # Profanity check
            vAR_profanity_result,vAR_result_message = Profanity_Words_Check(vAR_input_text)

            if not vAR_profanity_result:
                response_json["BADWORDS_CLASSIFICATION"] = "NOT A BADWORD"
                response_json["RECOMMENDATION"] = "Accepted"

            elif vAR_profanity_result:
                response_json["BADWORDS_CLASSIFICATION"] = vAR_result_message
                response_json["GUIDELINE_FWORD_CLASSIFICATION"] = None
                response_json["PREVIOUSLY_DENIED_CLASSIFICATION"] = None
                response_json["RULE_BASED_CLASSIFICATION"] = None
                response_json["RECOMMENDATION"] = "Denied"
                response_json["REASON"] = "BADWORD"
                response_json["MODEL"] = None
                response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":[{"IDENTITY_HATE":None,"INSULT":None,"OBSCENE":None,"SEVERE_TOXIC":None,
                "THREAT":None,"TOXIC":None}],"SUM_OF_ALL_CATEGORIES":None}
                return response_json

            # FWord Guideline check
            vAR_fword_flag,vAR_fword_validation_message = FWord_Validation(vAR_input_text)

            if (vAR_fword_flag):
                response_json["GUIDELINE_FWORD_CLASSIFICATION"] = vAR_fword_validation_message
                response_json["PREVIOUSLY_DENIED_CLASSIFICATION"] = None
                response_json["RULE_BASED_CLASSIFICATION"] = None
                response_json["RECOMMENDATION"] = "Denied"
                response_json["REASON"] = "CONFIGURATION FOUND IN FWORD GUIDELINES"
                response_json["MODEL"] = None
                response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":[{"IDENTITY_HATE":None,"INSULT":None,"OBSCENE":None,"SEVERE_TOXIC":None,
                "THREAT":None,"TOXIC":None}],"SUM_OF_ALL_CATEGORIES":None}
                return response_json
            elif not vAR_fword_flag:
                response_json["GUIDELINE_FWORD_CLASSIFICATION"] = vAR_fword_validation_message
                response_json["RECOMMENDATION"] = "Accepted"

            # Previously Denied configuration check
            vAR_pdc_flag,vAR_previously_denied_validation_message = Previously_Denied_Configuration_Validation(vAR_input_text)
            

            if (vAR_pdc_flag):
                response_json["PREVIOUSLY_DENIED_CLASSIFICATION"] = vAR_previously_denied_validation_message
                response_json["RECOMMENDATION"] = "Denied"
                response_json["RULE_BASED_CLASSIFICATION"] = None
                response_json["REASON"] = "CONFIGURATION FOUND IN PREVIOUSLY DENIED CONFIGURATIONS"
                response_json["MODEL"] = None
                response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":[{"IDENTITY_HATE":None,"INSULT":None,"OBSCENE":None,"SEVERE_TOXIC":None,
                "THREAT":None,"TOXIC":None}],"SUM_OF_ALL_CATEGORIES":None}
                return response_json
            elif not vAR_pdc_flag:
                response_json["PREVIOUSLY_DENIED_CLASSIFICATION"] = vAR_previously_denied_validation_message
                response_json["RECOMMENDATION"] = "Accepted"

            # Pattern denial check
            vAR_regex_result,vAR_pattern = Pattern_Denial(vAR_input_text)


            if not vAR_regex_result:
                response_json["RULE_BASED_CLASSIFICATION"] = " Similar to " +vAR_pattern+ " Pattern"
                response_json["RECOMMENDATION"] = "Denied"
                response_json["REASON"] = "DENIED PATTERN"
                response_json["MODEL"] = None
                response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":[{"IDENTITY_HATE":None,"INSULT":None,"OBSCENE":None,"SEVERE_TOXIC":None,
                "THREAT":None,"TOXIC":None}],"SUM_OF_ALL_CATEGORIES":None}
                return response_json

            elif vAR_regex_result:
                response_json["RULE_BASED_CLASSIFICATION"] = "NOT FOUND ANY DENIAL PATTERN"
                response_json["RECOMMENDATION"] = "Accepted"

            # RNN Model prediction for configuration
                
            vAR_result,vAR_result_data,vAR_result_target_sum = LSTM_Model_Result(vAR_input_text)
            vAR_result_data = str(vAR_result_data.to_json(orient='records'))
            if not vAR_result:
                print('Config denied by RNN Model - ',vAR_input_text)
                vAR_recommendation = "Denied"
                vAR_recommendation_reason = "Highest Profanity category probability should be below the threshold value(0.5)"
                
                response_json["RNN"] = {"PROFANITY_CLASSIFICATION":json.loads(str(vAR_result_data)),"SUM_OF_ALL_CATEGORIES":float(vAR_result_target_sum),"RECOMMENDATION":vAR_recommendation,"REASON":vAR_recommendation_reason,"MODEL":"RNN"}

                print('Response from RNN Denied - ',response_json)

                
            else:

                vAR_recommendation = "Accepted"
                vAR_recommendation_reason = "Since Highest Profanity category probability less than the threshold value(0.5), configuration accepted"

                
                response_json["RNN"] = {"PROFANITY_CLASSIFICATION":json.loads(str(vAR_result_data)),"SUM_OF_ALL_CATEGORIES":float(vAR_result_target_sum),"RECOMMENDATION":vAR_recommendation,"REASON":vAR_recommendation_reason,"MODEL":"RNN"}

                print('Response from RNN Accepted- ',response_json)
                
            # GPT Model Call

            result_json = ChatGPT_Recommendation(vAR_input_text)
            response_json["GPT"] = result_json
            print('Result json from GPT(till GPT) - ',response_json)

            print('Response after GPT- ',response_json)

            return response_json

                

            
                

                
                
            # elif vAR_model.upper()=='BERT':

            #     vAR_result,vAR_result_data,vAR_result_target_sum = BERT_Model_Result(vAR_input_text)

            #     if not vAR_result:
            #         vAR_recommendation = "Denied"
            #         vAR_recommendation_reason = "Highest Profanity category probability should be below the threshold value(0.5)"
            #         response_json["RECOMMENDATION"] = vAR_recommendation
            #         response_json["REASON"] = vAR_recommendation_reason
            #     else:
            #         vAR_recommendation = "Accepted"
            #         vAR_recommendation_reason = "Since Highest Profanity category probability less than the threshold value(0.5), configuration accepted"
            #         response_json["RECOMMENDATION"] = vAR_recommendation
            #         response_json["REASON"] = vAR_recommendation_reason
                
            #     vAR_result_data = str(vAR_result_data.to_json(orient='records'))

            
            #     response_json["MODEL"] = vAR_model
            #     response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":json.loads(str(vAR_result_data)),"SUM_OF_ALL_CATEGORIES":float(vAR_result_target_sum)}
            #     print('BERT response json - ',response_json)
            #     return response_json

            # elif vAR_model.upper()=='GPT':

            #     result_json = ChatGPT_Recommendation(vAR_input_text)



            
                
            

        else:
            response_json["ERROR_MESSAGE"] = vAR_error_message["Error Message"]
            return response_json

    except BaseException as e:
        print('In Error Block - '+str(e))
        print('Error Traceback4 - '+str(traceback.print_exc()))
        response_json["ERROR_MESSAGE"] = '### '+str(e)
        return response_json



def ChatGPT_Recommendation(vAR_input_text):
    response_json = {}

    vAR_response = ELP_Recommendation(vAR_input_text)

    vAR_dict1_start_index = vAR_response.index('{')
    vAR_dict1_end_index = vAR_response.index('}')


    vAR_dict2_start_index = vAR_response.rfind('{')
    vAR_dict2_end_index = vAR_response.rfind('}')

    vAR_result_dict = vAR_response[vAR_dict1_start_index:vAR_dict1_end_index+1]
    vAR_conclusion_dict = vAR_response[vAR_dict2_start_index:vAR_dict2_end_index+1]

    vAR_result_df = pd.DataFrame(json.loads(vAR_result_dict))
    vAR_conclusion_df = pd.DataFrame(json.loads(vAR_conclusion_dict))

    response_json["MODEL"] = "GPT"
    response_json["RECOMMENDATION"] = vAR_conclusion_df["Conclusion"][0]
    response_json["REASON"] = vAR_conclusion_df["Conclusion Reason"][0] 
    response_json["RECOMMENDED_CONFIGURATION"] = vAR_conclusion_df["Recommended Configuration"][0]
    response_json["RECOMMENDATION_REASON"] = vAR_conclusion_df["Recommendation Reason"][0]

    response_json["SEVERE_TOXIC_REASON"] = vAR_result_df["Reason"][0]
    response_json["OBSCENE_REASON"] = vAR_result_df["Reason"][1]
    response_json["INSULT_REASON"] = vAR_result_df["Reason"][2]
    response_json["HATE_REASON"] = vAR_result_df["Reason"][3]
    response_json["TOXIC_REASON"] = vAR_result_df["Reason"][4]
    response_json["THREAT_REASON"] = vAR_result_df["Reason"][5]

    response_json["SEVERE_TOXIC"] = vAR_result_df["Probability"][0]
    response_json["OBSCENE"] = vAR_result_df["Probability"][1]
    response_json["INSULT"] = vAR_result_df["Probability"][2]
    response_json["IDENTITY_HATE"] = vAR_result_df["Probability"][3]
    response_json["TOXIC"] = vAR_result_df["Probability"][4]
    response_json["THREAT"] = vAR_result_df["Probability"][5]

    response_json["OVERALL_SCORE"] = None

    print('GPT response json - ',response_json)
    return response_json
