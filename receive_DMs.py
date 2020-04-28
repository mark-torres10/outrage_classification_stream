"""
   receive_DMs.py

   The purpose of this script is to receive DMs from the Twitter account. 

   Input: 
      • twitter_credentials: has credentials for Twitter account
      • aws_credentials: has credentials for AWS account
      • export_tweets_name: Name to give to .csv file (without .csv extension) of user responses, exported to AWS in a 'user_replies/' directory

   This script will receive the DMs sent to the lab Twitter account in the past 30 days and store the information in AWS

"""
# working with Twitter API, AWS
import tweepy # using version 3.8.0
from tweepy import OAuthHandler
import boto3 # for working with AWS S3
from botocore.exceptions import NoCredentialsError

# helper functions, packages
import pandas as pd
import numpy as np
import argparse
import sys
import json
import datetime
import re
import os

def authenticate(consumer_key, consumer_secret, access_token, access_secret):
   """
      Allows authentication with Twitter API, with relevant IDs
         Input: IDs
         Output: authentication, API access
   """
   auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
   auth.set_access_token(access_token, access_secret)

   api = tweepy.API(auth)

   try:
      api.verify_credentials()
      print("Authentication OK")
   except Exception as e:
      print("Error during authentication")
      print(e)

   return auth, api

def extract_from_AWS(aws_access, aws_secret, bucket, s3_file, local_file):
   
   """
   Imports .csv file from AWS.

      Input: 
         • aws_access: AWS access key
         • aws_secret: AWS secret key
         • bucket: name of bucket in AWS S3 storage (place to store data)
         • s3_file: name of file in AWS (assumes that it is in the 'labelled_tweets/' directory)
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
         Key = "labelled_tweets/" + s3_file,
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

def get_DMs(api):
    """
        Receive the DMs that have been sent/received by the authenticated user's account (so, the lab account)
        
        Input:
            • api: authenticated Twitter API
    
    """
    
    message_list = api.list_direct_messages()
    
    return message_list

def store_AWS(aws_access, aws_secret, local_file, bucket, s3_file):

   """
   Takes the exported .csv file, and stores it into aws

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
      s3.upload_file(local_file, bucket, "user_replies/" + s3_file)
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
   parser = argparse.ArgumentParser(description = "File for sending DMs to users on Twitter, if their tweet was deemed to have outrage in it.")
   parser.add_argument("twitter_credentials", help = "Text file with Twitter developer credentials (consumer key, consumer secret, access key, access secret)")
   parser.add_argument("aws_credentials", help = "Text file with AWS credentials (AWS access, AWS secret)")
   parser.add_argument("export_tweets_name", help = "Name to give to .csv file (without .csv extension) of user replies exported to AWS")
   args = parser.parse_args()

   # get authentication
   consumer_key = ''
   consumer_secret = ''
   access_key = ''
   access_secret = ''

   with open(args.twitter_credentials, 'r') as twitter_creds:
      consumer_key = twitter_creds.readline().rstrip() # reads line, removes trailing whitespaces
      consumer_secret = twitter_creds.readline().rstrip()
      access_key = twitter_creds.readline().rstrip()
      access_secret = twitter_creds.readline().rstrip()

   try:
      auth, api = authenticate(consumer_key, consumer_secret, access_key, access_secret)
   except Exception as e:
      print("Authentication failed")
      print(e)

   # set up access to AWS
   export_file_name = args.export_tweets_name + '.csv'
   aws_access = ''
   aws_secret = ''
   bucket = 'augmented-outrage-classifier-tweets' # name of bucket in AWS

   # get AWS credentials
   with open(args.aws_credentials, 'r') as aws_creds:
      aws_access = aws_creds.readline().split(sep = "=")[1].rstrip() # separate the equal sign, eliminate \n
      aws_secret = aws_creds.readline().split(sep = "=")[1].rstrip() # separate the equal sign, eliminate \n

   # set own Twitter ID:
   own_id = int(api.me()._json['id'])

   # get most recent DMs (from the past 30 days)
   messages_list = get_DMs(api)

   # initialize array to hold responses:
   sender_id_arr = []
   message_arr = []

   # loop through all responses, keep those sent by someone else
   for message_json in messages_list:

      # initialize variables
      sender_id = 0
      message = ""

      # iterate through each json (where each JSON represents a distinct message)
      sender_id = int(message_json.message_create['sender_id'])

      # check if sender_id != your own ID (if it doesn't, then the message was from someone else)
      if sender_id != own_id:
         
         # get message
         message = message_json.message_create['message_data']['text']

         # append to arrays
         sender_id_arr.append(sender_id)
         message_arr.append(message)

   # now, package the two arrays as a dataframe
   messages_from_users = pd.DataFrame(zip(sender_id_arr, message_arr),
                                   columns = ['sender_id', 'message'])

   # export df, then export to AWS
   messages_from_users.to_csv(export_file_name, index = False)

   # re-upload to AWS (store_AWS)
   try:
      print("Storing DMs from other others (for later analysis) in AWS ")
      store_AWS(aws_access, aws_secret, export_file_name, bucket, export_file_name)
      print("Tweets successfully stored in AWS")
   except Exception as e:
      print("AWS storage unsuccessful. Please see error message: ")
      print(e)

   print("Script execution finished.")


if __name__ == "__main__":
   main()



