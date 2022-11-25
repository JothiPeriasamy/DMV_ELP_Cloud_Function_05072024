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
   
   if 'MODEL_PREDICTION'  in vAR_api_response:
      vAR_data['TOXIC'] = vAR_api_response['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['TOXIC']
      vAR_data['SEVERE_TOXIC'] = vAR_api_response['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['SEVERE_TOXIC']
      vAR_data['OBSCENE'] = vAR_api_response['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['OBSCENE']
      vAR_data['IDENTITY_HATE'] = vAR_api_response['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['IDENTITY_HATE']
      vAR_data['INSULT'] = vAR_api_response['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['INSULT']
      vAR_data['THREAT'] = vAR_api_response['MODEL_PREDICTION']['PROFANITY_CLASSIFICATION'][0]['THREAT']
      vAR_data['OVERALL_SCORE'] = vAR_api_response['MODEL_PREDICTION']['SUM_OF_ALL_CATEGORIES']

      del vAR_data['MODEL_PREDICTION']
      
   return vAR_data 