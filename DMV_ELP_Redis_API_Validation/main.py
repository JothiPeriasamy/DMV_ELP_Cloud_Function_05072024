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

from DMV_ELP_Request_PreValidation import Pre_Request_Validation
# from DMV_ELP_BERT_Model_Prediction import BERT_Model_Result
from DMV_ELP_Read_TLS_Certificate import get_ca_cert
from DMV_ELP_Bigquery_Response import GetConfigDetails
from DMV_ChatGPT_Recommendation import ELP_Recommendation
from DMV_ELP_API_Stats_Query import ProcessAPIStats
from DMV_ELP_reCAPTCHA_Validation import create_assessment

import redis
import os
import re
import time
import traceback
import openai



vAR_ca_cert_path = get_ca_cert(os.environ["GCS_BUCKET_NAME"], os.environ["TLS_CERTIFICATE_PATH"])



def elp_redis_validation(request):

  response_json = {}

  if os.environ["CAPTCHA_ENABLED"]=="NO":
    # try:
    #   response_json = {}

    #   vAR_token = request.headers["RECAPTCHA_TOKEN"]
    #   vAR_project_id = os.environ["GCP_PROJECT_ID"]
    #   vAR_captcha_key = os.environ["RECAPTCHA_KEY"]
    #   vAR_action = os.environ["RECAPTCHA_ACTION"]

    #   print("TOKEN FROM HEADERS - ",vAR_token)

    #   vAR_risk_score,response = create_assessment(vAR_project_id,vAR_captcha_key,vAR_token,vAR_action)
    #   print('vAR_risk_score - ',vAR_risk_score)
    #   print('response - ',response)
    #   if vAR_risk_score is None:
    #     raise Exception(str(response))
    #   if vAR_risk_score<0.5:
    #     raise Exception("Risk Identified With reCaptcha Enterprise")
    # except BaseException as e:
    #   error_str = "Error in creating reCAPTCHA asssessment"
    #   print('Error Traceback in creating reCAPTCHA asssessment- '+str(traceback.print_exc()))
    #   return  {"errorCode":103 ,"errorDescription":error_str+" - "+str(e)}
    
      
  # else:

    try:
      request_json = request.get_json()
      vAR_error_message = {}
      vAR_error_message["Error Message"] = ""
      vAR_input_text = ""
      if request_json["function"]!=7:
        vAR_error_message = Pre_Request_Validation(request_json)
        vAR_input_text = request_json['licencePlateConfig'].upper()
    except KeyError as e:
      print('In Error Block - '+"###"+str(e))
      print('Error Traceback4 - '+str(traceback.print_exc()))
      response_json["errorDescription"] = '### '+str(e)
      response_json["errorCode"] = 103
      return response_json
      

    try:
      
      if len(vAR_error_message["Error Message"])==0:
        
        print("input before removing special chars - ",vAR_input_text)
        vAR_input = vAR_input_text.replace('/','').replace('*','').replace('$','')

        # redis_host = os.environ.get('REDISHOST', os.environ["REDIS_IP"])
        # redis_port = int(os.environ.get('REDISPORT', int(os.environ["REDIS_PORT"])))
        # redis_auth = os.environ.get('REDISAUTH', os.environ["REDIS_AUTH"])
        # redis_client = redis.Redis(host=os.environ["REDIS_IP"], port=int(os.environ["REDIS_PORT"]), decode_responses=True,password=os.environ["REDIS_AUTH"])

        # redis_client = redis.StrictRedis(host=os.environ["REDIS_IP"], port=int(os.environ["REDIS_PORT"]),password=os.environ["REDIS_AUTH"])

        
        redis_client = redis.Redis(host=os.environ["REDIS_IP"], port=int(os.environ["REDIS_PORT"]), decode_responses=True,password=os.environ["REDIS_AUTH"],ssl=True,ssl_ca_certs=vAR_ca_cert_path)
        if request_json["function"]==0:
          response_json = All_Function(vAR_input)
          return response_json
        elif request_json["function"]==1:
          vAR_reverse_input = "".join(reversed(vAR_input)).upper()
          vAR_number_replaced = Number_Replacement(vAR_input).upper()
          vAR_rev_number_replaced = Number_Replacement(vAR_reverse_input).upper()

          if redis_client.sismember("ELP_BADWORDS",vAR_input) or redis_client.sismember("ELP_BADWORDS",vAR_reverse_input) or redis_client.sismember("ELP_BADWORDS",vAR_number_replaced) or redis_client.sismember("ELP_BADWORDS",vAR_rev_number_replaced):
            response_json["responseCode"] = "DENIED"
            response_json["reason"] = "DENIED-BADWORD"
            response_json["reasonCode"] = "POLICY"
            return response_json

          else:
            response_json["reason"] = ""
            response_json["responseCode"] = "APPROVED"
            response_json["reasonCode"] = ""
            return response_json


        elif request_json["function"]==2:

          if redis_client.sismember("ELP_FWORD_GUIDELINES",vAR_input):
            response_json["responseCode"] = "DENIED"
            response_json["reason"] = "DENIED-FWORD-GUIDELINE"
            response_json["reasonCode"] = "PROFANITY"
            return response_json

          else:
            
            response_json["reason"] = ""
            response_json["responseCode"] = "APPROVED"
            response_json["reasonCode"] = ""
            return response_json



        elif request_json["function"]==3:
          if redis_client.sismember("ELP_PREVIOUSLY_DENIED",vAR_input):
            response_json["responseCode"] = "DENIED"
            response_json["reason"] = "DENIED-PREVIOUSLY DENIED CONFIG"
            response_json["reasonCode"] = "PREVIOUSLY"
            return response_json
          else:
            response_json["reason"] = ""
            response_json["responseCode"] = "APPROVED"
            response_json["reasonCode"] = ""
            return response_json


        elif request_json["function"]==4:
          result = redis_client.smembers('ELP_DENIED_PATTERN')
          res_list = []
          for res in result:
            result_temp = re.findall(res, vAR_input,flags=re.IGNORECASE)
            if len(result_temp)>0:
              res_list.append(result_temp[0])
          if len(res_list)>0:
            response_json["responseCode"] = "DENIED"
            response_json["reason"] = " Similar to Denied Pattern"
            response_json["reasonCode"] = "PATTERN"
            return response_json
          else:
            response_json["reason"] = ""
            response_json["responseCode"] = "APPROVED"
            response_json["reasonCode"] = ""
            return response_json

        # elif request_json["function"]==8:
        #   start_time = time.time()
          
        #   vAR_result,vAR_result_data,vAR_result_target_sum = BERT_Model_Result(vAR_input)

        #   vAR_result_data = str(vAR_result_data.to_json(orient='records'))

        #   print('input - ',vAR_input)
        #   print('model result - ',vAR_result_data)

        #   if not vAR_result:
        #     response_json["responseCode"] = "DENIED"
        #     response_json["reason"] = " Higher than Probability threshold(50%)"
        #     response_json["reasonCode"] = "MODEL"
        #     print("Bert model load and response time - ",(time.time()-start_time))
        #     return response_json
        #   else:
        #     response_json["reason"] = ""
        #     response_json["responseCode"] = "APPROVED"
        #     response_json["reasonCode"] = ""
        #     print("Bert model load and response time - ",(time.time()-start_time))
        #     return response_json


        elif request_json["function"]==5:


          start_time = time.time()

          vAR_df,vAR_result = ELP_Recommendation(vAR_input)

          if vAR_result:

            response_json["reason"] = ""
            response_json["responseCode"] = "APPROVED"
            response_json["reasonCode"] = ""
            print("GPT model response time for config "+vAR_input+" - ",(time.time()-start_time))

            return response_json

          else:

            response_json["responseCode"] = "DENIED"
            response_json["reason"] = vAR_df["Reason"]
            # response_json["reason"] = "Higher than Probability threshold(30%)"
            response_json["reasonCode"] = "MODEL"
            # response_json["recommendedConfiguration"] = vAR_df["Recommended Config"]
            print("GPT model response time for config "+vAR_input+" - ",(time.time()-start_time))
            return response_json


        elif request_json["function"]==6:
          response_json = GetConfigDetails(vAR_input_text)
          print('Response json from function 6 : ',response_json)
          return response_json

        elif request_json["function"]==7:
          if "date" in request_json:
            date = request_json["date"]
            if len(date)!=10:

              raise Exception("Invalid Date Format")
          if "date" in request_json:
            date = request_json["date"]
            response_stats = ProcessAPIStats(date)
          else:
            date = ""
            response_stats = ProcessAPIStats(date)
          print('response_stats - ',response_stats)
          return response_stats
          

          

        else:

          response_json["errorDescription"] = "Invalid Function Value"
          response_json["errorCode"] = 101
          return response_json

      else:
        response_json["errorDescription"] = vAR_error_message["Error Message"]
        response_json["errorCode"] = 102
        return response_json


    except (TypeError,ValueError) as typeerr:
      
      print('GPT Error While processing config(Response format differs) and check logs for more details - ',str(vAR_input_text)+"###"+str(typeerr))

      response_json["errorDescription"] = '### Response format differs - '+str(typeerr)
      response_json["errorCode"] = 103

      return response_json

    except Exception as e:

      print('In Error Block - '+vAR_input_text+"###"+str(e))
      print('Error Traceback4 - '+str(traceback.print_exc()))

      # if "Request timed out" in str(e):
      #   print('GPT Error While processing config and check logs for more details - ',str(vAR_input_text)+"###"+str(e))

      #   response_json["errorDescription"] = '### '+str(e)
      #   response_json["errorCode"] = 104

      #   return response_json
      
      if "ConnectionResetError" in str(e):
        print('GPT Error While processing config and check logs for more details - ',str(vAR_input_text)+"###"+str(e))

        response_json["errorDescription"] = '### '+str(e)
        response_json["errorCode"] = 105

        return response_json

      if ("timed out" in str(e)) or ("Request timed out" in str(e)):
        print("Since GPT response failed to respond in 10 seconds, the config is auto approved - ",vAR_input_text)
        response_json["reason"] = ""
        response_json["responseCode"] = "APPROVED"
        response_json["reasonCode"] = ""
        return response_json

      if "Response Filtered by content_filter by azure openai" in str(e):
        print('GPT Error While processing config and check logs for more details - ',str(vAR_input_text)+"###"+str(e))

        response_json["errorDescription"] = '### Response Filtered by content_filter by azure openai - '+str(e)
        response_json["errorCode"] = 106

        return response_json


      response_json["errorDescription"] = '### '+str(e)
      response_json["errorCode"] = 107

      print("Response json before return - ",response_json)
      return response_json

    finally:
      # To ensure logs recorded
      print("KeepingLogsIntoBigqueryWithRequest&Response - "+vAR_input_text+"###"+str(response_json))
      





def All_Function(vAR_input):

  # redis_client = redis.StrictRedis(host=os.environ["REDIS_IP"], port=int(os.environ["REDIS_PORT"]),password=os.environ["REDIS_AUTH"])

  redis_client = redis.Redis(host=os.environ["REDIS_IP"], port=int(os.environ["REDIS_PORT"]), decode_responses=True,password=os.environ["REDIS_AUTH"],ssl=True,ssl_ca_certs=vAR_ca_cert_path)


  # Pattern check in redis

  result = redis_client.smembers('ELP_DENIED_PATTERN')
  res_list = []
  for res in result:
    result_temp = re.findall(res, vAR_input,flags=re.IGNORECASE)
    if len(result_temp)>0:
      res_list.append(result_temp[0])

  # Pattern check logic ends here in redis

  response_json = {}
  # redis_host = os.environ.get('REDISHOST', os.environ["REDIS_IP"])
  # redis_port = int(os.environ.get('REDISPORT', int(os.environ["REDIS_PORT"])))
  # redis_auth = os.environ.get('REDISAUTH', os.environ["REDIS_AUTH"])
  # redis_client = redis.StrictRedis(host=redis_host, port=redis_port,password=redis_auth)


  
  # redis_client = redis.Redis(host=os.environ["REDIS_IP"], port=int(os.environ["REDIS_PORT"]), decode_responses=True,password=os.environ["REDIS_AUTH"])

  vAR_reverse_input = "".join(reversed(vAR_input)).upper()
  vAR_number_replaced = Number_Replacement(vAR_input).upper()
  vAR_rev_number_replaced = Number_Replacement(vAR_reverse_input).upper()

  if redis_client.sismember("ELP_BADWORDS",vAR_input) or redis_client.sismember("ELP_BADWORDS",vAR_reverse_input) or redis_client.sismember("ELP_BADWORDS",vAR_number_replaced) or redis_client.sismember("ELP_BADWORDS",vAR_rev_number_replaced):
    response_json["responseCode"] = "DENIED"
    response_json["reason"] = "DENIED-BADWORD"
    response_json["reasonCode"] = "POLICY"
    return response_json
  elif redis_client.sismember("ELP_FWORD_GUIDELINES",vAR_input):
    response_json["responseCode"] = "DENIED"
    response_json["reason"] = "DENIED-FWORD-GUIDELINE"
    response_json["reasonCode"] = "PROFANITY"
    return response_json
  elif redis_client.sismember("ELP_PREVIOUSLY_DENIED",vAR_input):
    response_json["responseCode"] = "DENIED"
    response_json["reason"] = "DENIED-PREVIOUSLY DENIED CONFIG"
    response_json["reasonCode"] = "PREVIOUSLY"
    return response_json

  elif len(res_list)>0:
      response_json["responseCode"] = "DENIED"
      response_json["reason"] = " Similar to Denied Pattern"
      response_json["reasonCode"] = "PATTERN"
      return response_json

  else:

      start_time = time.time()

      
      vAR_dict,vAR_result = ELP_Recommendation(vAR_input)

      

      if vAR_result:

        response_json["reason"] = ""
        response_json["responseCode"] = "APPROVED"
        response_json["reasonCode"] = ""
        print("GPT model response time for config "+vAR_input+" - ",(time.time()-start_time))

        return response_json

      else:

        response_json["responseCode"] = "DENIED"
        response_json["reason"] = vAR_dict["Reason"]
        # response_json["reason"] = "Higher than Probability threshold(30%)"
        response_json["reasonCode"] = "MODEL"
        # response_json["recommendedConfiguration"] = vAR_dict["Recommended Config"]
        print("GPT model response time for config "+vAR_input+" - ",(time.time()-start_time))
        return response_json



        # start_time = time.time()
        
        # vAR_result,vAR_result_data,vAR_result_target_sum = BERT_Model_Result(vAR_input)

        # vAR_result_data = str(vAR_result_data.to_json(orient='records'))

        # print('input - ',vAR_input)
        # print('model result - ',vAR_result_data)

        # if not vAR_result:
        #   response_json["responseCode"] = "DENIED"
        #   response_json["reason"] = " Higher than Probability threshold(50%)"
        #   response_json["reasonCode"] = "MODEL"
        #   print("Bert model load and response time - ",(time.time()-start_time))
        #   return response_json
          
        # else:
        #   response_json["reason"] = ""
        #   response_json["responseCode"] = "APPROVED"
        #   response_json["reasonCode"] = ""
        #   print("Bert model load and response time - ",(time.time()-start_time))
        #   return response_json


  




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
