"""
-------------------------------------------------------------------------------------------------------------------------------------------------
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

-------------------------------------------------------------------------------------------------------------------------------------------------
"""


import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
import nltk
nltk.download('stopwords')
import re
from nltk.corpus import stopwords
from sklearn.model_selection import train_test_split
from tensorflow.keras.layers import LSTM,Dense,Embedding,SpatialDropout1D
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.models import Sequential
from tensorflow.keras.preprocessing.text import one_hot
from keras.callbacks import EarlyStopping
from sklearn import metrics
import pickle
from google.cloud import bigquery,storage
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= # Json credential file path

MAX_WORDS = 50000
MAX_SEQUENCE_LENGTH = 200
EMBEDDING_DIM = 100


def data_preprocessing(vAR_data):
    vAR_corpus = []
    vAR_stop_words = set(stopwords.words('english'))
    for idx in range(0, len(vAR_data)):
        vAR_preprocessed_text = re.sub('[^a-zA-Z]', ' ', str(vAR_data[idx]))
        vAR_preprocessed_text = vAR_preprocessed_text.lower()
        vAR_preprocessed_text = vAR_preprocessed_text.split()
        vAR_preprocessed_text = [w for w in vAR_preprocessed_text if not w in vAR_stop_words]
        vAR_preprocessed_text = ' '.join(vAR_preprocessed_text)
        vAR_corpus.append(vAR_preprocessed_text)
    return vAR_corpus

def define_tokenizer(vAR_texttotokenize):
    vAR_tokenizer = Tokenizer(num_words=MAX_WORDS)
    vAR_tokenizer.fit_on_texts(vAR_texttotokenize)
    vAR_word_index = vAR_tokenizer.word_index
    print('Found %s unique tokens.' % len(vAR_word_index))
    return vAR_tokenizer

def build_model():
    vAR_model = Sequential()
    vAR_model.add(Embedding(MAX_WORDS, EMBEDDING_DIM, input_length=200))
    vAR_model.add(SpatialDropout1D(0.2))
    vAR_model.add(LSTM(100, dropout=0.2, recurrent_dropout=0.2))
    vAR_model.add(Dense(6, activation='softmax'))
    vAR_model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    print(vAR_model.summary())
    return vAR_model


def main():
    vAR_bq_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_TOXIC_COMMENTS"
    vAR_sql = "(SELECT COMMENT_TEXT,TOXIC,SEVERE_TOXIC,OBSCENE,THREAT,INSULT,IDENTITY_HATE FROM `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+ " WHERE  toxic='0' and severe_toxic='0' and obscene='0' and threat='0' and insult='0' and identity_hate='0' limit 250) union all (SELECT  COMMENT_TEXT,TOXIC,SEVERE_TOXIC,OBSCENE,THREAT,INSULT,IDENTITY_HATE FROM `"+ os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"+ " WHERE  toxic='1' or severe_toxic='1' or obscene='1' or threat='1' or insult='1' or identity_hate='1' limit 250) order by COMMENT_TEXT "
    vAR_df = vAR_bq_client.query(vAR_sql).to_dataframe()
    vAR_df = vAR_df.astype({"TOXIC": int, "SEVERE_TOXIC": int,"OBSCENE": int, "THREAT": int,"INSULT": int, "IDENTITY_HATE": int})
    print(vAR_df.head())
    # df=pd.read_csv("news_text.csv",delimiter="\t").head(500)
    vAR_final_df=vAR_df.dropna()
    vAR_preprocessed_text=data_preprocessing(vAR_final_df["COMMENT_TEXT"].values)
    vAR_final_df["PREPROCESSED_TEXT"]=vAR_preprocessed_text
    vAR_final_df=vAR_final_df.drop(["COMMENT_TEXT"],axis=1)

    vAR_tokenizer=define_tokenizer(vAR_final_df["PREPROCESSED_TEXT"].values)
    
    vAR_gcs_client = storage.Client()
    vAR_bucket = vAR_gcs_client.get_bucket() # add Project id
    blob = vAR_bucket.blob(os.environ["LSTM_MODEL_LOAD_PATH"]+"/"+os.environ["LSTM_MODEL_TOKENIZER_FILE"])
    with blob.open(mode= 'wb') as handle:
        pickle.dump(vAR_tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)

    vAR_X = tokenizer.texts_to_sequences(final_df["PREPROCESSED_TEXT"].values)
    vAR_X = pad_sequences(X, maxlen=MAX_SEQUENCE_LENGTH)
    vAR_Y = final_df.iloc[:,:6].values
    

    vAR_X_train, vAR_X_test, vAR_Y_train, vAR_Y_test = train_test_split(vAR_X,vAR_Y, test_size = 0.10, random_state = 42)

    vAR_model=build_model()

    vAR_epochs = 25
    vAR_batch_size = 64
    history = vAR_model.fit(vAR_X_train, vAR_Y_train, epochs=vAR_epochs,
                        batch_size=vAR_batch_size,validation_split=0.1,
                        callbacks=[EarlyStopping(monitor='val_loss', patience=3, min_delta=0.0001)])

    # Add cloud storage path below
    vAR_model.save("LSTM_model")

    vAR_accr = vAR_model.evaluate(vAR_X_test,vAR_Y_test)
    print('Test set\n  Loss: {:0.3f}\n  Accuracy: {:0.3f}'.format(vAR_accr[0],vAR_accr[1]))

    # Use below code for sample prediction
    
    # input_text = ['Trorist']
    # seq = tokenizer.texts_to_sequences(input_text)
    # padded = pad_sequences(seq, maxlen=MAX_SEQUENCE_LENGTH)
    # pred = model.predict(padded)
    # labels = ["TOXIC","SEVERE_TOXIC","OBSCENE","THREAT","INSULT","IDENTITY_HATE"]
    # print(pred, labels[np.argmax(pred)])

if __name__=="__main__":
    main()
