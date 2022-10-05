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

from DMV_ELP_Request_PreValidation import Pre_Request_Validation
from DMV_ELP_Public_Profanity_Validation import Profanity_Words_Check
from DMV_ELP_GuideLine_FWord_Validation import FWord_Validation
from DMV_ELP_Previously_Denied_Config_Validation import Previously_Denied_Configuration_Validation

from DMV_ELP_Pattern_Denial import Pattern_Denial

from DMV_ELP_BERT_Model_Prediction import BERT_Model_Result
from DMV_ELP_LSTM_Model_Prediction import LSTM_Model_Result
from DMV_ELP_Get_License_Plate_Code import GetPlateCode


def ELP_Validation(request):
    
    
    vAR_error_message = {}
    request_json = request.get_json()
    response_json = request_json.copy()
    try:
        # To resolve container error(TypeError: Descriptors cannot not be created directly)
        os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION']='python'

        
        vAR_input_text = request_json['LICENSE_PLATE_CONFIG'].upper()
        vAR_license_plate_desc = request_json['LICENSE_PLATE_DESC']
        
        

        
        vAR_result_message = ""
        
        vAR_error_message = Pre_Request_Validation(request_json)

        vAR_sg_id = 1
        vAR_platecode_error_message = ""
        vAR_plate_code,vAR_platecode_error_message = GetPlateCode(vAR_license_plate_desc)
        
        
        
        if len(vAR_error_message["Error Message"])==0:
            if len(vAR_platecode_error_message)>0:
                vAR_error_message['Error Message'] = vAR_platecode_error_message
            # It can be changed later
            vAR_model = os.environ["MODEL"]
            response_json["SG_ID"] = vAR_sg_id
            response_json["PLATE_CODE"] = vAR_plate_code
            response_json["ERROR_MESSAGE"] = vAR_error_message["Error Message"]

            # Profanity check
            vAR_profanity_result,vAR_result_message = Profanity_Words_Check(vAR_input_text)

            if not vAR_profanity_result:
                response_json["BADWORDS_CLASSIFICATION"] = "NOT A BADWORD"
                response_json["RECOMMENDATION"] = "Accepted"

            elif vAR_profanity_result:
                response_json["BADWORDS_CLASSIFICATION"] = vAR_result_message
                response_json["RECOMMENDATION"] = "Denied"
                response_json["REASON"] = "BADWORD"
                return response_json

            # FWord Guideline check
            vAR_fword_flag,vAR_fword_validation_message = FWord_Validation(vAR_input_text)

            if (vAR_fword_flag):
                response_json["GUIDELINE_FWORD_CLASSIFICATION"] = vAR_fword_validation_message
                response_json["RECOMMENDATION"] = "Denied"
                response_json["REASON"] = "CONFIGURATION FOUND IN FWORD GUIDELINES"
                return response_json
            elif not vAR_fword_flag:
                response_json["GUIDELINE_FWORD_CLASSIFICATION"] = vAR_fword_validation_message
                response_json["RECOMMENDATION"] = "Accepted"

            # Previously Denied configuration check
            vAR_pdc_flag,vAR_previously_denied_validation_message = Previously_Denied_Configuration_Validation(vAR_input_text)
            

            if (vAR_pdc_flag):
                response_json["PREVIOUSLY_DENIED_CLASSIFICATION"] = vAR_previously_denied_validation_message
                response_json["RECOMMENDATION"] = "Denied"
                response_json["REASON"] = "CONFIGURATION FOUND IN PREVIOUSLY DENIED CONFIGURATIONS"
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
                return response_json

            elif vAR_regex_result:
                response_json["RULE_BASED_CLASSIFICATION"] = "NOT FOUND ANY DENIAL PATTERN"
                response_json["RECOMMENDATION"] = "Accepted"

            # Model prediction for configuration
            if vAR_model.upper()=='RNN':
                
                vAR_result,vAR_result_data,vAR_result_target_sum = LSTM_Model_Result(vAR_input_text)
                
                
            elif vAR_model.upper()=='BERT':

                vAR_result,vAR_result_data,vAR_result_target_sum = BERT_Model_Result(vAR_input_text)
                
            vAR_result_data = str(vAR_result_data.to_json(orient='records'))

            if not vAR_result:
                vAR_recommendation = "Denied"
                vAR_recommendation_reason = "Highest Profanity category probability should be below the threshold value(0.5)"
                response_json["RECOMMENDATION"] = vAR_recommendation
                response_json["REASON"] = vAR_recommendation_reason
            else:
                vAR_recommendation = "Accepted"
                vAR_recommendation_reason = "Since Highest Profanity category probability less than the threshold value(0.5), configuration accepted"
                response_json["RECOMMENDATION"] = vAR_recommendation
                response_json["REASON"] = vAR_recommendation_reason

            response_json["MODEL"] = vAR_model
            response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":json.loads(str(vAR_result_data)),"SUM_OF_ALL_CATEGORIES":float(vAR_result_target_sum)}
            return response_json

        else:
            response_json["ERROR_MESSAGE"] = vAR_error_message["Error Message"]
            return response_json

    except BaseException as e:
        print('In Error Block - '+str(e))
        print('Error Traceback4 - '+str(traceback.print_exc()))
        response_json["ERROR_MESSAGE"] = '### '+str(e)
        return response_json
