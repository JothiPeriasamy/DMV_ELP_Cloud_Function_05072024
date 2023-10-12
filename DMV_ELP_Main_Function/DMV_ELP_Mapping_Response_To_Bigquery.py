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

def Process_API_Response(vAR_api_response):         

   vAR_data = vAR_api_response.copy()
   
   if 'RNN'  in vAR_api_response:
      # vAR_data['TOXIC'] = vAR_api_response["RNN"]['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['TOXIC']
      # vAR_data['SEVERE_TOXIC'] = vAR_api_response["RNN"]['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['SEVERE_TOXIC']
      # vAR_data['OBSCENE'] = vAR_api_response["RNN"]['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['OBSCENE']
      # vAR_data['IDENTITY_HATE'] = vAR_api_response["RNN"]['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['IDENTITY_HATE']
      # vAR_data['INSULT'] = vAR_api_response["RNN"]['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['INSULT']
      # vAR_data['THREAT'] = vAR_api_response["RNN"]['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['THREAT']
      # vAR_data['OVERALL_SCORE'] = vAR_api_response["RNN"]['MODEL_PREDICTION']['SUM_OF_ALL_CATEGORIES']

      # vAR_data["MODEL"] = vAR_api_response["RNN"]["MODEL"]
      # vAR_data["RECOMMENDATION"] = vAR_api_response["RNN"]["RECOMMENDATION"]
      # vAR_data["REASON"] = vAR_api_response["RNN"]["REASON"]

      vAR_data["RNN"]["RECOMMENDED_CONFIGURATION"] = None
      vAR_data["RNN"]["RECOMMENDATION_REASON"] = None

      vAR_data["RNN"]["SEVERE_TOXIC_REASON"] = None
      vAR_data["RNN"]["OBSCENE_REASON"] = None
      vAR_data["RNN"]["INSULT_REASON"] = None
      vAR_data["RNN"]["IDENTITY_HATE_REASON"] = None
      vAR_data["RNN"]["TOXIC_REASON"] = None
      vAR_data["RNN"]["THREAT_REASON"] = None

   # if "GPT" in vAR_api_response:

   #    vAR_data["MODEL"] = vAR_api_response["GPT"]["MODEL"]
   #    vAR_data["RECOMMENDATION"] = vAR_api_response["GPT"]["RECOMMENDATION"]
   #    vAR_data["REASON"] = vAR_api_response["GPT"]["REASON"]

   #    vAR_data["RECOMMENDED_CONFIGURATION"] = vAR_api_response["GPT"]["RECOMMENDED_CONFIGURATION"]
   #    vAR_data["RECOMMENDATION_REASON"] = vAR_api_response["GPT"]["RECOMMENDATION_REASON"]

   #    vAR_data["SEVERE_TOXIC_REASON"] = vAR_api_response["GPT"]["SEVERE_TOXIC_REASON"]
   #    vAR_data["OBSCENE_REASON"] = vAR_api_response["GPT"]["OBSCENE_REASON"]
   #    vAR_data["INSULT_REASON"] = vAR_api_response["GPT"]["INSULT_REASON"]
   #    vAR_data["HATE_REASON"] = vAR_api_response["GPT"]["HATE_REASON"]
   #    vAR_data["TOXIC_REASON"] = vAR_api_response["GPT"]["TOXIC_REASON"]
   #    vAR_data["THREAT_REASON"] = vAR_api_response["GPT"]["THREAT_REASON"]

   #    vAR_data['TOXIC'] = vAR_api_response["GPT"]['TOXIC']
   #    vAR_data['SEVERE_TOXIC'] = vAR_api_response["GPT"]['SEVERE_TOXIC']
   #    vAR_data['OBSCENE'] = vAR_api_response["GPT"]['OBSCENE']
   #    vAR_data['IDENTITY_HATE'] = vAR_api_response["GPT"]['IDENTITY_HATE']
   #    vAR_data['INSULT'] = vAR_api_response["GPT"]['INSULT']
   #    vAR_data['THREAT'] = vAR_api_response["GPT"]['THREAT']
   #    vAR_data['OVERALL_SCORE'] = vAR_api_response["GPT"]['OVERALL_SCORE']


   return vAR_data