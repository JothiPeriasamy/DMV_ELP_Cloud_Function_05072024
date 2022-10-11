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




# Load Huggingface transformers
from transformers import TFBertModel,  BertConfig, BertTokenizerFast, TFAutoModel

# Then what you need from tensorflow.keras
from tensorflow.keras.layers import Input, Dropout, Dense, GlobalAveragePooling1D
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.initializers import TruncatedNormal
from tensorflow.keras.losses import CategoricalCrossentropy
from tensorflow.keras.metrics import CategoricalAccuracy
from tensorflow.keras.utils import to_categorical

# And pandas for data import + sklearn because you allways need sklearn
import pandas as pd
from google.cloud import bigquery
import tensorflow as tf
import re
import numpy as np
from sklearn.model_selection import train_test_split
import os


def read_bq_data():
    vAR_bqclient = bigquery.Client()
    vAR_query_string = "select * from `"+os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+".DMV_ELP_TOXIC_COMMENTS` limit 500"
    vAR_dataframe = (
            vAR_bqclient.query(vAR_query_string)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
    return vAR_dataframe

def clean_text(text):
    text = text.lower()
    text = re.sub(r"what's", "what is ", text)
    text = re.sub(r"\'s", " ", text)
    text = re.sub(r"\'ve", " have ", text)
    text = re.sub(r"can't", "cannot ", text)
    text = re.sub(r"n't", " not ", text)
    text = re.sub(r"i'm", "i am ", text)
    text = re.sub(r"\'re", " are ", text)
    text = re.sub(r"\'d", " would ", text)
    text = re.sub(r"\'ll", " will ", text)
    text = re.sub(r"\'scuse", " excuse ", text)
    text = re.sub('\W', ' ', text)
    text = re.sub('\s+', ' ', text)
    text = text.strip(' ')
    return text

try:
    # df=pd.read_csv('/home/jupyter/DSAI_DMV_Text_Analyzer/DSAI_Dataset/jigsaw-toxic-comment-train.csv')
    
    df = read_bq_data()
    df = df.astype({"TOXIC": int, "SEVERE_TOXIC": int,"OBSCENE": int, "THREAT": int,"INSULT": int, "IDENTITY_HATE": int})
    # df = df.head(100)
    print(df)
    df['comment_text'] = df['COMMENT_TEXT'].map(lambda x : clean_text(x))
    train_sentences = df["comment_text"].values
    list_classes = ["TOXIC", "SEVERE_TOXIC", "OBSCENE", "THREAT", "INSULT", "IDENTITY_HATE"]
    train_y = df[list_classes].values

    # Name of the BERT model to use
    model_name = 'bert-base-uncased'

    # Max length of tokens
    max_length = 128

    # Load transformers config and set output_hidden_states to False
    config = BertConfig.from_pretrained(model_name)
    #config.output_hidden_states = False

    # Load BERT tokenizer
    tokenizer = BertTokenizerFast.from_pretrained(pretrained_model_name_or_path = model_name, config = config)
    bert = TFBertModel.from_pretrained(model_name)


    input_ids = Input(shape=(max_length,), name='input_ids', dtype='int32')
    attention_mask = Input(shape=(max_length,), name='attention_mask', dtype='int32') 
    # input_ids = tf.convert_to_tensor(input_ids, dtype=tf.int32)
    # attention_mask = tf.convert_to_tensor(attention_mask, dtype=tf.int32)
    # inputs = {'input_ids': input_ids, 'attention_mask': attention_mask}
    embeddings = bert.bert(input_ids, attention_mask=attention_mask)[1]
    
    # x = bert.bert(inputs)

    #x2 =Dense(512, activation='relu')(x[1])
    
    # x2 = GlobalAveragePooling1D()(x[0])
    x = Dense(1024, activation='relu')(embeddings)
    #x3 = Dropout(0.5)(x2)
    y =Dense(len(list_classes), activation='softmax', name='outputs')(x)

    model = Model(inputs=[input_ids,attention_mask], outputs=y)
    #model.layers[2].trainable = False

    # Take a look at the model
    model.summary()


    optimizer = Adam(lr=1e-5, decay=1e-6)
    model.compile(loss='categorical_crossentropy',
    optimizer=optimizer,
    metrics=['accuracy'])


    # Tokenize the input 
    x = tokenizer(
    text=list(train_sentences),
    add_special_tokens=True,
    max_length=max_length,
    truncation=True,
    padding='max_length', 
    return_tensors='tf')
    # return_token_type_ids = False,
    # return_attention_mask = True,
    # verbose = True)

    history = model.fit(
    x={'input_ids': x['input_ids'], 'attention_mask': x['attention_mask']},
    #x={'input_ids': x['input_ids']},
    y={'outputs': train_y},
    validation_split=0.1,
    batch_size=32,
    epochs=2)
    print('Model training completed-------------------------------')
    print('Model Accuracy - ',history.history)
    # Below path should be cloud storage path
    model.save('model.h5')
    print('Model saved successfully--------------------------------')

except Exception as e:
    print('Error Block - ',str(e))
