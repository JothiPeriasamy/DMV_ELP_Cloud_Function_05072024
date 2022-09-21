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
import numpy as np
import tensorflow as tf
import time
import json
import gcsfs
import h5py
import traceback
import os

from DMV_ELP_Request_PreValidation import Pre_Request_Validation
from DMV_ELP_Public_Profanity_Validation import Profanity_Words_Check
from DMV_ELP_GuideLine_FWord_Validation import FWord_Validation
from DMV_ELP_Previously_Denied_Config_Validation import Previously_Denied_Configuration_Validation

from DMV_ELP_Pattern_Denial import Pattern_Denial

from DMV_ELP_BERT_Model_Prediction import BERT_Model_Result
from DMV_ELP_LSTM_Model_Prediction import LSTM_Model_Result

from DMV_ELP_Get_RequestId import GetLastRequestId

def ELP_Validation(request):
    
    request_json = request.get_json()
    vAR_input_text = request_json['CONFIGURATION']
    vAR_request_id = GetLastRequestId()+1
    vAR_request_date = request_json['REQUEST_DATE']
    vAR_sg_id = request_json['SG_ID']
    vAR_order_group_id = request_json['ORDER_GROUP_ID']
    vAR_order_date = request_json['ORDER_DATE']
    vAR_order_id = request_json['ORDER_ID']

    vAR_error_message = {}

    try:
        # To resolve container error(TypeError: Descriptors cannot not be created directly)
        os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION']='python'
        
        start_time = time.time()
        vAR_result_message = ""
        
        vAR_error_message = Pre_Request_Validation(request_json)
        
        
        if len(vAR_error_message["Error Message"])==0:
            vAR_profanity_result,vAR_result_message = Profanity_Words_Check(vAR_input_text)
            if not vAR_profanity_result:
                vAR_message_level_1 = "Accepted"
            elif vAR_profanity_result:
                vAR_message_level_1 = "Denied - "+vAR_result_message

            vAR_pdc_flag,vAR_previously_denied_validation_message = Previously_Denied_Configuration_Validation(vAR_input_text)
            vAR_fword_flag,vAR_fword_validation_message = FWord_Validation(vAR_input_text)

            vAR_regex_result,vAR_pattern = Pattern_Denial(vAR_input_text)
            if not vAR_regex_result:
                vAR_message_level_2 = "Denied - Similar to " +vAR_pattern+ " Pattern"
            elif vAR_regex_result:
                vAR_message_level_2 = "Accepted"

            if (vAR_fword_flag):
                return {"Direct Profanity":{"Is accepted":not vAR_profanity_result,"Message":vAR_message_level_1},
                "Previously Denied":vAR_previously_denied_validation_message, "FWord Guideline Validation":vAR_fword_validation_message,
                "Denied Pattern":{"Is accepted":vAR_regex_result,"Message":vAR_message_level_2},
                'Order Id':vAR_order_id,'Configuration':vAR_input_text,
                'Request Id':vAR_request_id,'Request Date':vAR_request_date,'Simply Gov Id':vAR_sg_id,
                'Order Group Id':vAR_order_group_id,'Order Date':vAR_order_date,
                "Error Message":vAR_error_message["Error Message"],"Recommendation":"Denied","Reason":vAR_fword_validation_message}
            if (vAR_pdc_flag):
                return {"Direct Profanity":{"Is accepted":not vAR_profanity_result,"Message":vAR_message_level_1},
                "Previously Denied":vAR_previously_denied_validation_message, "FWord Guideline Validation":vAR_fword_validation_message,
                "Denied Pattern":{"Is accepted":vAR_regex_result,"Message":vAR_message_level_2},
                'Order Id':vAR_order_id,'Configuration':vAR_input_text,
                'Request Id':vAR_request_id,'Request Date':vAR_request_date,'Simply Gov Id':vAR_sg_id,
                'Order Group Id':vAR_order_group_id,'Order Date':vAR_order_date,
                "Error Message":vAR_error_message["Error Message"],"Recommendation":"Denied","Reason":vAR_previously_denied_validation_message}
            if (vAR_profanity_result):
                return {"Direct Profanity":{"Is accepted":not vAR_profanity_result,"Message":vAR_message_level_1},
                "Previously Denied":vAR_previously_denied_validation_message, "FWord Guideline Validation":vAR_fword_validation_message,
                "Denied Pattern":{"Is accepted":vAR_regex_result,"Message":vAR_message_level_2},
                'Order Id':vAR_order_id,'Configuration':vAR_input_text,
                'Request Id':vAR_request_id,'Request Date':vAR_request_date,'Simply Gov Id':vAR_sg_id,
                'Order Group Id':vAR_order_group_id,'Order Date':vAR_order_date,
                "Error Message":vAR_error_message["Error Message"],"Recommendation":"Denied","Reason":vAR_message_level_1}
            if (not vAR_regex_result):
                return {"Direct Profanity":{"Is accepted":not vAR_profanity_result,"Message":vAR_message_level_1},
                "Previously Denied":vAR_previously_denied_validation_message, "FWord Guideline Validation":vAR_fword_validation_message,
                "Denied Pattern":{"Is accepted":vAR_regex_result,"Message":vAR_message_level_2},
                'Order Id':vAR_order_id,'Configuration':vAR_input_text,
                'Request Id':vAR_request_id,'Request Date':vAR_request_date,'Simply Gov Id':vAR_sg_id,
                'Order Group Id':vAR_order_group_id,'Order Date':vAR_order_date,
                "Error Message":vAR_error_message["Error Message"],"Recommendation":"Denied","Reason":vAR_message_level_2}

            if request_json['MODEL'].upper()=='RNN':
                
                vAR_result,vAR_result_data,vAR_result_target_sum = LSTM_Model_Result(vAR_input_text)
                vAR_result_data = vAR_result_data.to_json(orient='records')
                if not vAR_result:
                    vAR_recommendation_level_3 = "Denied"
                    vAR_reason_level_3 = "Since the highest profanity category probability exceeds the threshold value(50%)"
                else:
                    vAR_recommendation_level_3 = "Accepted"
                    vAR_reason_level_3 = "Since the highest profanity category probability below the threshold value(50%)"
                vAR_response_time = round(time.time() - start_time,2)

                return {"Direct Profanity":{"Is accepted":not vAR_profanity_result,"Message":vAR_message_level_1},
                "Previously Denied":vAR_previously_denied_validation_message, "FWord Guideline Validation":vAR_fword_validation_message,
                "Denied Pattern":{"Is accepted":vAR_regex_result,"Message":vAR_message_level_2},
                "Model Prediction":{"Is accepted":vAR_result,"Recommendation":vAR_recommendation_level_3,"Reason":vAR_reason_level_3,"Profanity Classification":json.loads(vAR_result_data),
                'Sum of all Categories':vAR_result_target_sum},
                'Order Id':vAR_order_id,'Configuration':vAR_input_text,
                'Request Id':vAR_request_id,'Request Date':vAR_request_date,'Simply Gov Id':vAR_sg_id,
                'Order Group Id':vAR_order_group_id,'Order Date':vAR_order_date,
                'Response time':str(vAR_response_time)+" secs","Error Message":vAR_error_message["Error Message"]}

            elif request_json['MODEL'].upper()=='BERT':

                vAR_result,vAR_result_data,vAR_result_target_sum = BERT_Model_Result(vAR_input_text)
                vAR_result_data = vAR_result_data.to_json(orient='records')
                if vAR_result_target_sum>20:
                    vAR_recommendation_level_3 = "Denied"
                    vAR_reason_level_3 = "Since the profanity probability exceeds the threshold(sum of probability >20%)"
                else:
                    vAR_recommendation_level_3 = "Accepted"
                    vAR_reason_level_3 = "Since the profanity probability less than the threshold(sum of probability <20%)"
                vAR_response_time = round(time.time() - start_time,2)

                return {"Direct Profanity":{"Is accepted":not vAR_profanity_result,"Message":vAR_message_level_1},
                "Previously Denied":vAR_previously_denied_validation_message, "FWord Guideline Validation":vAR_fword_validation_message,
                "Denied Pattern":{"Is accepted":vAR_regex_result,"Message":vAR_message_level_2},
                "Model Prediction":{"Is accepted":vAR_result,"Recommendation":vAR_recommendation_level_3,"Reason":vAR_reason_level_3,"Profanity Classification":json.loads(vAR_result_data),
                'Sum of all Categories':vAR_result_target_sum},
                'Order Id':vAR_order_id,'Configuration':vAR_input_text,
                'Request Id':vAR_request_id,'Request Date':vAR_request_date,'Simply Gov Id':vAR_sg_id,
                'Order Group Id':vAR_order_group_id,'Order Date':vAR_order_date,
                'Response time':str(vAR_response_time)+" secs","Error Message":vAR_error_message["Error Message"]}

        else:
            vAR_error_message["Configuration"] = vAR_input_text
            vAR_error_message["Request Id"] = vAR_request_id
            vAR_error_message["Request Date"] = vAR_request_date
            vAR_error_message["Simply Gov Id"] = vAR_sg_id
            vAR_error_message["Order Group Id"] = vAR_order_group_id
            vAR_error_message["Order Date"] = vAR_order_date
            vAR_error_message["Order Id"] = vAR_order_id

            return vAR_error_message

    except BaseException as e:
        print('In Error Block - '+str(e))
        print('Error Traceback - '+str(traceback.print_exc()))
        vAR_error_message["Configuration"] = vAR_input_text
        vAR_error_message["Request Id"] = vAR_request_id
        vAR_error_message["Request Date"] = vAR_request_date
        vAR_error_message["Simply Gov Id"] = vAR_sg_id
        vAR_error_message["Order Group Id"] = vAR_order_group_id
        vAR_error_message["Order Date"] = vAR_order_date
        vAR_error_message["Order Id"] = vAR_order_id
        vAR_error_message["Error Message"] = '### '+str(e)
        return vAR_error_message
