"""
   clean.py

   The purpose of this script is to load tweets from the AWS account, parse them, and re-upload them into the AWS account

   Input: 
      • aws_credentials.txt: has credentials for AWS account
      • import_tweets_name: Name of .json file (without .json extension) of raw tweets, to import from AWS
      • export_tweets_name: Name to give to .csv file (without .csv extension) of cleaned tweets exported to AWS

   This script will scrape tweets from Twitter and store them in a "cleaned_tweets/" directory in an AWS bucket titled "augmented_outrage_classifier_tweets"

"""

import boto3 # for working with AWS S3
from botocore.exceptions import NoCredentialsError
import argparse
import pandas as pd
import datetime
import os
import json

def extract_from_AWS(aws_access, aws_secret, bucket, s3_file, local_file):

   """
      Imports .json file from AWS.

      Input: 
         • aws_access: AWS access key
         • aws_secret: AWS secret key
         • bucket: name of bucket in AWS S3 storage (place to store data)
         • s3_file: name of file in AWS (assumes that it is in the 'raw_tweets/' directory)
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
         Key = "raw_tweets/" + s3_file,
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


def standard_parse(tweets, set_name):

    """

        Returns tweets and relevant metadata in a DataFrame
        Input:
            • tweets: raw tweets
            • set_name: name to give to set of tweets (useful if parsing multiple bunches of tweets and then concatenating)
        Output:
            • df: df with relevant tweet data

    """
    
    data = {'created_at':[],\
       'text':[],\
       'tweet_id':[],\
       'user_screen_name':[],\
       'user_name':[],\
       'user_id':[],\
       'user_followers_count':[],\
       'user_following_count':[],\
       'user_statuses_count':[],\
       'user_likes_given_count':[],\
       'user_location':[],\
       'user_verified':[],\
       'user_description':[],\
       'tweet_lat':[],\
       'tweet_long':[],\
       'tweet_retweet_count':[],\
       'tweet_favorite_count':[],\
       'tweet_reply_count': [],\
       'tweet_hashtags':[],\
       'tweet_urls':[],\
       'tweet_media':[]}
    
    for tweet in tweets:
        if 'text' in tweet:
            if 'retweeted_status' not in tweet and 'RT @' not in tweet['text'] and not tweet['user']['verified']:
                data['created_at'].append(tweet['created_at'])

                if tweet['truncated']:
                    data['text'].append(tweet['extended_tweet']['full_text'])
                elif not tweet['truncated']:
                    data['text'].append(tweet['text'])

                data['tweet_id'].append(tweet['id_str'])
                data['user_screen_name'].append(tweet['user']['screen_name'])
                data['user_name'].append(tweet['user']['name'])
                data['user_id'].append(tweet['user']['id_str'])
                data['user_followers_count'].append(tweet['user']['followers_count'])
                data['user_following_count'].append(tweet['user']['friends_count'])
                data['user_statuses_count'].append(tweet['user']['statuses_count'])
                data['user_likes_given_count'].append(tweet['user']['favourites_count'])
                data['user_location'].append(tweet['user']['location'])
                data['user_verified'].append(tweet['user']['verified'])
                data['user_description'].append(tweet['user']['description'])

                if tweet['coordinates']:
                    data['tweet_lat'].append(tweet['coordinates']['coordinates'][1])
                    data['tweet_long'].append(tweet['coordinates']['coordinates'][0])
                elif not tweet['coordinates']:
                    data['tweet_lat'].append('NaN')
                    data['tweet_long'].append('NaN')

                data['tweet_retweet_count'].append(tweet['retweet_count'])
                data['tweet_favorite_count'].append(tweet['favorite_count'])
                data['tweet_reply_count'].append(tweet['reply_count'])
                data['tweet_hashtags'].append([hashtag['text'] for hashtag in tweet['entities']['hashtags']])
                data['tweet_urls'].append(list(url['expanded_url'] for url in tweet['entities']['urls']))

                if 'media' in tweet['entities']:
                    data['tweet_media'].append(list(url['media_url'] for url in tweet['entities']['media']))
                else:
                    data['tweet_media'].append('NaN')

    df = pd.DataFrame(data)

    df.drop_duplicates(subset = 'tweet_id', inplace = True)
    df.reset_index(drop = True, inplace = True)
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['set_id'] = set_name # column lets us define the source of the data
    
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
      s3.upload_file(local_file, bucket, "cleaned_tweets/" + s3_file)
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
    parser = argparse.ArgumentParser(description = "File for cleaning tweets and storing in AWS.")
    parser.add_argument("aws_credentials", help = "Text file with AWS credentials (AWS access, AWS secret)")
    parser.add_argument("import_tweets_name", help = "Name of .json file (without .json extension) of raw tweets, to import from AWS", 
        default = "outrage_tweets_streamed_{}".format(datetime.datetime.today().strftime ('%d-%b-%Y'))) # assumes that there exists a .json file named by default of stream.py
    parser.add_argument("export_tweets_name", help = "Name to give to .csv file (without .csv extension) of cleaned tweets exported to AWS", 
        default = "outrage_tweets_streamed_cleaned_{}".format(datetime.datetime.today().strftime ('%d-%b-%Y'))) # named by current date, by default
    args = parser.parse_args()
    
    # set up access to AWS
    import_file_name = args.import_tweets_name + ".json"
    export_file_name = args.export_tweets_name + ".csv"
    aws_access = ''
    aws_secret = ''
    bucket = 'augmented-outrage-classifier-tweets' # name of bucket in AWS

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

    # load JSON tweets
    tweets = [json.loads(tweet) for tweet in open(import_file_name)]

    # clean files (standard_parse)
    try:
        print("Starting tweet parsing and cleaning....")
        df = standard_parse(tweets, args.export_tweets_name)
        df.to_csv(export_file_name, index = False, encoding = 'utf-8-sig')
        print("Finished parsing and cleaning tweets")
    except Exception as e:
        print("Error encountered with tweet parsing and cleaning. Please see error message: ")
        print(e)

    # re-upload to AWS (store_AWS)
    try:
        store_AWS(aws_access, aws_secret, export_file_name, bucket, export_file_name)
        print("Tweets successfully stored in AWS")
    except Exception as e:
        print("AWS storage unsuccessful. Please see error message: ")
        print(e)

    print("Script execution finished.")

if __name__ == "__main__":
    main()


