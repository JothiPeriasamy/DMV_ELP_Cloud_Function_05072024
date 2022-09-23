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
   vAR_data = {}
   
   vAR_data['ERROR_MESSAGE'] = vAR_api_response['Error Message']

   if len(vAR_api_response['Error Message'])==0:
      vAR_data['REQUEST_ID'] = vAR_api_response['Request Id']
      vAR_data['REQUEST_DATE'] = vAR_api_response['Request Date']
      vAR_data['ORDER_DATE'] = vAR_api_response['Order Date']
      vAR_data['CONFIGURATION'] = vAR_api_response['Configuration']
      vAR_data['ORDER_ID'] = vAR_api_response['Order Id']
      vAR_data['SG_ID'] = vAR_api_response['Simply Gov Id']
      vAR_data['ORDER_GROUP_ID'] = vAR_api_response['Order Group Id']

      vAR_data['PREVIOUSLY_DENIED'] = vAR_api_response['Previously Denied']
      if vAR_api_response['Direct Profanity']['Is accepted']:
         vAR_data['DIRECT_PROFANITY'] = 'APPROVED - Not falls under any of the profanity word'
      if not vAR_api_response['Direct Profanity']['Is accepted']:
         vAR_data['DIRECT_PROFANITY'] = 'DENIED - '+vAR_api_response['Direct Profanity']['Message']

      vAR_data['GUIDELINE_FWORD'] = vAR_api_response['FWord Guideline Validation']
      
      if vAR_api_response['Denied Pattern']['Is accepted']:
         vAR_data['RULE_BASED_CLASSIFICATION'] = 'APPROVED - Not falls under any of the denied patterns'
      if not vAR_api_response['Denied Pattern']['Is accepted']:
         vAR_data['RULE_BASED_CLASSIFICATION'] = 'DENIED - '+vAR_api_response['Denied Pattern']['Message']

      if 'Model Prediction'  in vAR_api_response:
         vAR_data['MODEL'] = 'RNN'
         vAR_data['TOXIC'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['TOXIC']
         vAR_data['SEVERE_TOXIC'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['SEVERE_TOXIC']
         vAR_data['OBSCENE'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['OBSCENE']
         vAR_data['IDENTITY_HATE'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['IDENTITY_HATE']
         vAR_data['INSULT'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['INSULT']
         vAR_data['THREAT'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['THREAT']
         vAR_data['OVERALL_SCORE'] = vAR_api_response['Model Prediction']['Sum of all Categories']
         vAR_data['RECOMMENDATION'] = vAR_api_response['Model Prediction']['Recommendation']
         vAR_data['REASON'] = vAR_api_response['Model Prediction']['Reason']
      else:
         vAR_data['RECOMMENDATION'] = vAR_api_response['Recommendation']
         vAR_data['REASON'] = vAR_api_response['Reason']
   else:
      vAR_data['REQUEST_ID'] = vAR_api_response['Request Id']
      vAR_data['REQUEST_DATE'] = vAR_api_response['Request Date']
      vAR_data['ORDER_DATE'] = vAR_api_response['Order Date']
      vAR_data['CONFIGURATION'] = vAR_api_response['Configuration']
      vAR_data['ORDER_ID'] = vAR_api_response['Order Id']
      vAR_data['SG_ID'] = vAR_api_response['Simply Gov Id']
      vAR_data['ORDER_GROUP_ID'] = vAR_api_response['Order Group Id']
   return vAR_data 