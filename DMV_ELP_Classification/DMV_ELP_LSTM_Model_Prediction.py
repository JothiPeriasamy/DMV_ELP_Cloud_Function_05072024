# -*- coding: utf-8 -*-
"""
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

"""


import pandas as pd
import numpy as np
from tensorflow import keras
import pickle
from google.cloud import storage
from tensorflow.keras.preprocessing.sequence import pad_sequences
import os




def LSTM_Model_Result(vAR_input_text):
    vAR_model = keras.models.load_model("gs://"+os.environ['GCS_BUCKET_NAME']+"/saved_model/LSTM/LSTM_RNN_Model_V2/LSTM_model_50k_Records")

    MAX_SEQUENCE_LENGTH=200

    vAR_gcs_client = storage.Client()
    vAR_bucket = vAR_gcs_client.get_bucket(os.environ['GCS_BUCKET_NAME'])
    blob = vAR_bucket.blob('saved_model/LSTM/LSTM_RNN_Model_V2/Tokenizer/tokenizer_50k.pickle')

    with blob.open(mode='rb') as handle:
        vAR_tokenizer = pickle.load(handle)

    vAR_seq = vAR_tokenizer.texts_to_sequences([vAR_input_text])
    vAR_padded = pad_sequences(vAR_seq, maxlen=MAX_SEQUENCE_LENGTH)
    vAR_model_result = vAR_model.predict(vAR_padded)
    vAR_target_columns = ["TOXIC","SEVERE_TOXIC","OBSCENE","THREAT","INSULT","IDENTITY_HATE"]


    print('LSTM result - ',vAR_model_result)
    print('Max value in prediction - ',vAR_target_columns[np.argmax(vAR_model_result)])


    vAR_result_data = pd.DataFrame(vAR_model_result,columns=vAR_target_columns)
    vAR_target_sum = (np.sum(vAR_model_result)*100).round(2)
    vAR_result_data.index = pd.Index(['Percentage'],name='category')
    vAR_result_data = vAR_result_data.astype(float).round(5)*100
    
    

    # If Highest probability more than 50%, then configuration is denied

    vAR_max_prob = np.array(vAR_model_result).max()
    print('arg max val - ',vAR_max_prob)

    if (vAR_max_prob*100)>50:
        return False,vAR_result_data,vAR_target_sum
    else:
        return True,vAR_result_data,vAR_target_sum


