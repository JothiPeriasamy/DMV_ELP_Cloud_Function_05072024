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

from openai import AzureOpenAI
import os
import pandas as pd
import re
import json
import time
from wrapt_timeout_decorator import timeout


client = AzureOpenAI(api_key = os.getenv("AZURE_API_KEY"),api_version=os.getenv("API_VERSION"),azure_endpoint=os.getenv("API_BASE"))


@timeout(10,use_signals=True)
def ELP_Recommendation(vAR_input): 
      
    try:
      vAR_input_config = vAR_input.replace('/','')
      vAR_input_config = vAR_input_config.replace('*','')
      vAR_input_config = vAR_input_config.replace('#','')
      vAR_input_config = vAR_input_config.replace('$','')
      response = client.chat.completions.create(
      model=os.environ["AZURE_GPT_ENGINE"],
      messages=[

         {"role":"system","content":f"""
Consider a california dmv customer applying new licese plate configuration. Perform below tasks for given configuration:\n1.Please Provide the probability value for each of the categories (profanity as 'P', obscene as 'O', insult as 'I', hate as 'H', toxic as 'To', threat as 'Th').\n2.Deny the configuration if any one of the above categories probability value is greater than {os.environ["PROFANITY_THRESHOLD"]}. Otherwise, accept the configuration.
"""},

{"role":"user","content":"Given configuration is : 'omfg'"},{"role":"assistant","content":f"""{{"Category":["P","O","I","H","To","Th"],"Prob":[0.9,0.8,0.7,0.5,0.6,0.3],"Conclusion": "Denied","Reason":"The configuration 'OMFG' is DENIED as the probability value of Profanity is greater than or equal to {os.environ["PROFANITY_THRESHOLD"]}"}}
  """},

  {"role":"user","content":"Given configuration is : '2ANKH'"},{"role":"assistant","content":"""{"Category":["P","O","I","H","To","Th"],"Prob":[0.0,0.0,0.1,0.0,0.0,0.0],"Conclusion": "Accepted","Reason":"N/A"}
  """},

  {"role":"user","content":"Given configuration is : 'motor'"},{"role":"assistant","content":"""{"Category":["P","O","I","H","To","Th"],"Prob":[0.0,0.0,0.0,0.0,0.0,0.0],"Conclusion": "Accepted","Reason":"N/A"}
  """},

  {"role":"user","content":"Given configuration is :'"+vAR_input_config+"'"}],


      temperature=0,
      max_tokens=100,
      seed=10,

  )

      print('raw azure gpt response - ',response)

      if response.choices[0].finish_reason=="content_filter":
        raise Exception("Response Filtered by content_filter by azure openai")
        
      print('azure gpt response - ',response.choices[0].message.content)

      


      vAR_response = response.choices[0].message.content

      vAR_dict1_end_index = vAR_response.index('}')

      vAR_response = vAR_response[:vAR_dict1_end_index+1]

      vAR_conclusion_dict = json.loads(vAR_response)

      print('azure gpt response in dict format- ',vAR_conclusion_dict)

      if vAR_conclusion_dict["Conclusion"].lower()=="accepted":
        return vAR_conclusion_dict,True

      return vAR_conclusion_dict,False

    except BaseException as e:
      print("Error in ChatGPT",str(e))
      raise Exception(str(e))


























# def ELP_Recommendation(vAR_input):   

#     try:     
    
#       vAR_input_config = vAR_input.replace('/','')
#       vAR_input_config = vAR_input_config.replace('*','')
#       vAR_input_config = vAR_input_config.replace('#','')
#       vAR_input_config = vAR_input_config.replace('$','')
#       response = openai.ChatCompletion.create(
#       engine=os.environ["AZURE_GPT_ENGINE"],
#       messages=[

#           {"role":"system","content":"""Consider a california dmv customer applying new licese plate configuration. Perform below tasks for given word:\n1.Please Provide the probability value for each of the categories (profanity, obscene, insult, hate, toxic, threat).\n2.Deny the configuration if any one of the above categories probability value is greater than 0.3. Otherwise, accept the configuration.\n3.If it's denied, recommend new configuration which must not represent/fall any of the profanity,insult,hate,threat,obscene,toxic categories and the configuration length must be less than 8 characters.If it's accepted no recommendation needed. \nNote : Strictly Follow the condition number 2. """},

#           {"role":"user","content":"Given configuration is : 'omfg'"},{"role":"assistant","content":"""{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.9,0.8,0.7,0.5,0.6,0.3]}
#   {"Conclusion": ["Denied"],"Conclusion Reason":["The configuration 'OMFG' is DENIED as the probability value of Profanity is greater than or equal to 0.3"],"Recommended Configuration":["LUVU2"]}
#   """},

#   {"role":"user","content":"Given configuration is : '2ANKH'"},{"role":"assistant","content":"""{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.0,0.0,0.1,0.0,0.0,0.0]}
#   {"Conclusion": ["Accepted"],"Conclusion Reason":["N/A"],"Recommended Configuration":["N/A"]}
#   """},

#   {"role":"user","content":"Given configuration is : 'motor'"},{"role":"assistant","content":"""{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.0,0.0,0.0,0.0,0.0,0.0]}
#   {"Conclusion": ["Accepted"],"Conclusion Reason":["N/A"],"Recommended Configuration":["N/A"]}
#   """},

#   {"role":"user","content":"Given configuration is :'"+vAR_input_config+"'"}],

#       temperature=0,
#       max_tokens=110,
#       top_p=1,
#       frequency_penalty=0,
#       presence_penalty=0.9,

#   )

#       print('raw azure gpt response - ',response)
#       print('azure gpt response - ',response['choices'][0]['message']['content'])


#       vAR_response = response['choices'][0]['message']['content']

#       vAR_dict1_start_index = vAR_response.index('{')
#       vAR_dict1_end_index = vAR_response.index('}')


#       vAR_dict2_start_index = vAR_response.rfind('{')
#       vAR_dict2_end_index = vAR_response.rfind('}')

#       vAR_result_dict = vAR_response[vAR_dict1_start_index:vAR_dict1_end_index+1]
#       vAR_conclusion_dict = vAR_response[vAR_dict2_start_index:vAR_dict2_end_index+1]

#       vAR_result_df = pd.DataFrame(json.loads(vAR_result_dict))
#       vAR_conclusion_df = pd.DataFrame(json.loads(vAR_conclusion_dict))

#       print('result df - ',vAR_result_df)

#       print('conclusion df - ',vAR_conclusion_df)

#       if vAR_conclusion_df["Conclusion"][0].lower()=="accepted":
#         return vAR_conclusion_df,True

#       return vAR_conclusion_df,False

#     except BaseException as e:
#       print('GPT Error While processing config ',str(vAR_input)+"###"+str(e))






