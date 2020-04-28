"""
   classify.py

   The purpose of this script is to load cleaned tweets from the AWS account and to classify them

   Input: 
      • aws_credentials.txt: has credentials for AWS account
      • import_tweets_name: Name of .csv file (without .csv extension) of cleaned tweets, to import from AWS
      • export_tweets_name: Name to give to .csv file (without .csv extension) of classified tweets exported to AWS

   This script will scrape tweets from Twitter and store them in a "labelled_tweets/" directory in an AWS bucket titled "augmented_outrage_classifier_tweets"

"""

import boto3 # for working with AWS S3
from botocore.exceptions import NoCredentialsError
import argparse
import pandas as pd
import numpy as np
import helpers
from helpers import val_ar, nb_model, nb_vectorizer, exp_outrage_list, top_emojis, threshold_acc
import datetime

import os
import sys
import collections, os.path, emoji, gensim
import sklearn
from sklearn.preprocessing import MinMaxScaler
from sklearn import model_selection, preprocessing, linear_model, naive_bayes, metrics, svm, decomposition, ensemble
import keras
import keras.layers as layers
from keras import Sequential, optimizers
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from keras.models import load_model
from scipy.sparse import hstack
from joblib import dump, load

def extract_from_AWS(aws_access, aws_secret, bucket, s3_file, local_file):

   """
      Imports .csv file from AWS.

      Input: 
         • aws_access: AWS access key
         • aws_secret: AWS secret key
         • bucket: name of bucket in AWS S3 storage (place to store data)
         • s3_file: name of file in AWS (assumes that it is in the 'cleaned_tweets/' directory)
         • local_file: name/location of local .json file
        
   """
   # use boto3 to interface with AWS
   try:
      s3 = boto3.client('s3', 
         aws_access_key_id = aws_access,
         aws_secret_access_key= aws_secret)
      print("Connection with AWS successfully made.")
   except Exception as e:
      print("Connection with AWS unsuccessful.")
      print(e)

    # load data from AWS
   try:
      s3.download_file(Bucket = bucket, 
         Key = "cleaned_tweets/" + s3_file,
         Filename = local_file)
        
      print("Upload Successful")
      return True
   except FileNotFoundError:
      print("The file was not found")
      return False
   except NoCredentialsError:
      print("Credentials not available")
      return False
   except Exception as e:
      print("File download unsuccessful")
      print(e)

def preprocess_tweets(data):

   """ 
   
      Performs additional preprocessing steps to prepare the data to be fed into the classifier. 
      
      Input:
         • data: Pandas df of cleaned .csv file from AWS

      Output:
         • data: cleaned data

   """

   # preprocessing steps of text

   # clean data
   data['text'] = data['text'].astype('str')
   print("Preprocessing: Computing features")

   # the main features to be computed are "wn_lemmatize_hashtag", "get_arousal", "get_sentiment", "get_expanded_outrage"
   data["hashtag"] = [helpers.get_hashtag(tweet) for tweet in data["text"]]
   data["wn_lemmatize"] = [helpers.tweet_process(text) for text in data["text"]]
   data['wn_lemmatize_hashtag'] = data.apply(lambda row: ' '.join([x for x in row.wn_lemmatize.split(" ") + row.hashtag.split(" ") if x]), axis=1)

   data["psy_stemmed"], data["len_tokenize"] = zip(*data['text'].apply(helpers.psy_tweet_process))
   data["get_arousal"] = data.apply(lambda row: helpers.get_arousal(val_ar, row.psy_stemmed, row.len_tokenize), axis = 1)
   data['get_sentiment'] = data.apply(lambda row: helpers.get_sentiment(nb_model, nb_vectorizer, row.psy_stemmed), axis = 1)
   data['get_expanded_outrage'] = data.apply(lambda row: helpers.get_expanded_outrage(exp_outrage_list, row.psy_stemmed), axis = 1)
   print ("Prepocessing: Done. Start loading NLP features")

   #start getting NLP features + topic modelling is used
   data['emojis_list'] = [helpers.extract_emojis(tweet) for tweet in data['text']]
    
   # start getting NLP features
   data['raw_len'] = data['text'].str.len()
   data['has_hashtag'] = [1 if '#' in str(tweet) else 0 for tweet in data['text']]
   data['has_mention'] = [1 if '@' in str(tweet) else 0 for tweet in data['text']]
   data['has_link'] = [helpers.has_link(tweet) for tweet in data["text"]]
   data['count_emoji'] = [sum([helpers.char_is_emoji(c) for c in str(tweet)]) for tweet in data['text']]
   data['len_processed'] = data['wn_lemmatize'].str.len()
    
   # get top emojis and extract them into features
   for i in top_emojis:
      emoji_type = i[0]
      name = emoji.unicode_codes.UNICODE_EMOJI[emoji_type]
      data[name] = [1 if emoji_type in emoji_list else 0 for emoji_list in data['emojis_list']] 
   # counting the Part of Speech
   data['pos_count'] =  data.wn_lemmatize.map(lambda x: helpers.modify_pos(collections.Counter(elem[1] for elem in helpers.token_postag(x))))

   # create 7 variables for the count of specific POS
   POS = ['adj', 'verb', 'noun', 'adv', 'pronoun', 'wh', 'other']
   for pos_tag in POS:
      data[pos_tag] = data.pos_count.map(lambda x: 0 if pos_tag not in x else x[pos_tag])

   # scale + transform variables as necessary
   if scale:
      scale_var = ["raw_len", "count_emoji","len_processed"] + POS
      scaler = MinMaxScaler()
      data[scale_var] = scaler.fit_transform(data[scale_var])

   return data

def predict_values(df, gru_embedding, embedding_tokenizer):

   """

      Classifies tweets using the deep GRU model

      Input:
         • df: preprocessed df (processed using preprocess_tweets)
         • gru_embedding: word embeddings for GRU model
         • embedding_tokenizer: embedding matrix based on pre-defined tokenizer

      Output:
         • df: df with predictions

   """

   # perform GRU prediction using tweet embedding model:
   tweet_emb_processed = pad_sequences(embedding_tokenizer.texts_to_sequences(df['wn_lemmatize_hashtag']), 
                                        padding='post', maxlen=50)
   tweet_gru_predict  = gru_embedding.predict(tweet_emb_processed)
   df['gru_prob'] = tweet_gru_predict.ravel()
   df['gru_binary'] = np.where(df['gru_prob'] > .51, 1, 0)
   
   # drop unnecessary columns
   df.drop(['wn_lemmatize',\
      'wn_lemmatize_hashtag',\
      'psy_stemmed',\
      'len_tokenize',\
      'get_expanded_outrage',\
      'emojis_list',\
      'raw_len',\
      'has_hashtag',\
      'has_mention',\
      'has_link',\
      'count_emoji',\
      'len_processed',\
      ':face_with_tears_of_joy:',\
      ':rolling_on_the_floor_laughing:',\
      ':pouting_face:',\
      ':middle_finger:',\
      ':cat_face_with_tears_of_joy:',\
      ':folded_hands:',\
      ':thumbs_down:',\
      ':water_wave:',\
      ':face_with_rolling_eyes:',\
      ':thinking_face:',\
      'pos_count',\
      'adj',\
      'verb',\
      'noun',\
      'adv',\
      'pronoun',\
      'wh',\
      'other',\
      'hashtag'], axis = 1, inplace = True)

   # return the df
   return df

def store_AWS(aws_access, aws_secret, local_file, bucket, s3_file):

   """
      Takes the exported file and stores it into aws
      Input: 
         • aws_access: AWS access key
         • aws_secret: AWS secret key
         • local_file: name/location of local .csv file
         • bucket: name of bucket in AWS S3 storage (place to store data)
         • s3_file: name of file once it is stored in AWS
   """

   # use boto3 to interface with AWS
   try:
      s3 = boto3.client('s3', 
                        aws_access_key_id = aws_access,
                        aws_secret_access_key= aws_secret)
      print("Connection with AWS successfully made.")
   except Exception as e:
      print("Connection with AWS unsuccessful.")
      print(e)

   # upload data to AWS
   try:
      s3.upload_file(local_file, bucket, "labelled_tweets/" + s3_file)
      print("Upload Successful")
      return True
   except FileNotFoundError:
      print("The file was not found")
      return False
   except NoCredentialsError:
      print("Credentials not available")
      return False
   except Exception as e:
      print("Error encountered")
      print(e)
      return False

   print("Export to AWS finished.")

def main():

   # get params
   parser = argparse.ArgumentParser(description = "File for streaming tweets and storing in AWS.")
   parser.add_argument("aws_credentials", help = "Text file with AWS credentials (AWS access, AWS secret)")
   parser.add_argument("import_tweets_name", help = "Name of .csv file (without .csv extension) of cleaned tweets, to import from AWS", 
        default = "outrage_tweets_streamed_{}".format(datetime.datetime.today().strftime ('%d-%b-%Y'))) # assumes that there exists a .csv file named by default of stream.py
   parser.add_argument("export_tweets_name", help = "Name to give to .csv file (without .csv extension) of classified tweets exported to AWS", 
        default = "outrage_tweets_streamed_labeled_{}".format(datetime.datetime.today().strftime ('%d-%b-%Y'))) # named by current date, by default
   args = parser.parse_args()


   # set up access to AWS
   import_file_name = args.import_tweets_name + ".csv"
   export_file_name = args.export_tweets_name + ".csv"
   aws_access = ''
   aws_secret = ''
   bucket = 'augmented-outrage-classifier-tweets' # name of bucket in AWS

   # get AWS credentials
   with open(args.aws_credentials, 'r') as aws_creds:
      aws_access = aws_creds.readline().split(sep = "=")[1].rstrip() # separate the equal sign, eliminate \n
      aws_secret = aws_creds.readline().split(sep = "=")[1].rstrip() # separate the equal sign, eliminate \n

   # load files from AWS (extract_from_AWS)
   try: 
      # try extraction
      extract_from_AWS(aws_access, aws_secret, bucket, s3_file = import_file_name, local_file = import_file_name)
         
      # check if file was exported successfully:
      if import_file_name in os.listdir():
         print("{} file successfully imported from AWS. Proceeding with parsing...".format(import_file_name))
         print("\n")
      else:
         print("{} not found in the current directory (something may have gone wrong in the import?)".format(import_file_name))
         raise ValueError("Data could not be imported")
   except Exception as e:
      print("Extraction from AWS failed. Please see error message: ")
      print(e)

   # import data
   data = pd.read_csv(import_file_name, lineterminator = '\n', encoding = 'utf-8-sig')

   # clean (preprocess_tweets)
   try:
      cleaned_data = preprocess_tweets(data)
      print("Data successfully preprocessed. Moving to next stage: classification")
   except Exception as e:
      print("Data preprocessing unsuccessful. See error message: ")
      print(e)

   # import model files
   try:
      gru_model = load_model("model_files/GRU_20200309.h5", custom_objects={'threshold_acc': threshold_acc})
      embedding_tokenizer = load('model_files/training.joblib')
      print("Model files successfully loaded")
   except Exception as e:
      print("Error in loading model files")
      print(e)
      sys.exit()

   # predict values (using predict_values)
   try:
      preds = predict_values(data, gru_model, embedding_tokenizer)
      print("Predictions successful. Will export to AWS.")
   except Exception as e:
      print("Error in prediction step")
      print(e)

   # select rows and columns (depends on application. Hard-coded in this instance. Only select those that had outrage)
   #outrage_tweets = preds.loc[preds['gru_binary'] == 1, ['user_name', 'user_screen_name', 'user_id', 'created_at', 'text', 'tweet_id', 'gru_prob', 'gru_binary']]
   # edit: 20 April 2020 (export all predictions, not just those that have outrage)
   outrage_tweets = preds.loc[:, ['user_name', 'user_screen_name', 'user_id', 'created_at', 'text', 'tweet_id', 'gru_prob', 'gru_binary']]
   # export as csv
   outrage_tweets.to_csv(export_file_name, index = False, encoding = 'utf-8-sig')

   # re-upload to AWS (store_AWS)
   try:
      store_AWS(aws_access, aws_secret, export_file_name, bucket, export_file_name)
      print("Tweets successfully stored in AWS")
   except Exception as e:
      print("AWS storage unsuccessful. Please see error message: ")
      print(e)

   # show examples of outrage tweets
   print("{0} tweets (out of {1}) were classified as having outrage (Proportion: {2:.3f})".format(outrage_tweets.shape[0], 
                                                                              preds.shape[0], 
                                                                              (outrage_tweets.shape[0] / preds.shape[0])))

   print("Here are some examples of tweets that were labelled as having outrage:")
   for i in range(5):
      print("Example: " + outrage_tweets['text'][i])
      print("\n")

   print("Script execution finished.")

if __name__ == "__main__":
   main()



