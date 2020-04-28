"""
   send_DMs.py

   The purpose of this script is to send DMs to Twitter accounts. It takes the tweets classified as having outrage and sends
   tweets to those users. 

   Input: 
      • twitter_credentials: has credentials for Twitter account
      • aws_credentials: has credentials for AWS account
      • import_tweets_name: Name of .csv file (without .csv extension) of cleaned tweets, to import from AWS
      • export_tweets_name: Name to give to .csv file (without .csv extension) of messaged tweets exported to AWS

   This script will send DMs to users on Twitter. It takes the users who sent tweets that had outrage in them, sends them a DM
   asking for their emotions, and also asks for a friend request. It then stores the information about which users/tweets received 
   DMs and stores it in AWS

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
import time

def authenticate(consumer_key, consumer_secret, access_token, access_secret):
   """
      Allows authentication with Twitter API, with relevant IDs
         Input: IDs
         Output: authentication, API access (authentication Tweepy API object)
   """
   auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
   auth.set_access_token(access_token, access_secret)

   api = tweepy.API(auth)

   try:
      api.verify_credentials()
      print("Authentication OK")
   except Exception as e:
      print("Error during Twitter authentication")
      print(e)

   return auth, api

def extract_from_AWS(aws_access, aws_secret, bucket, directory, s3_file, local_file):
   
   """
   Imports .csv file from AWS.

      Input: 
         • aws_access: AWS access key
         • aws_secret: AWS secret key
         • bucket: name of bucket in AWS S3 storage (place to store data)
         • directory: the directory/folder that the file is in
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
         Key = directory + s3_file,
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

def get_link(screen_name, tweet_id):

   """
      Derives tweet link from screen name and tweet id
         Input:
            • screen_name: Screen name of user
            • tweet_id: ID of tweet
         Output:
            • link: link of tweet
   """
    
   link = "https://twitter.com/" + screen_name + "/status/" + str(tweet_id)
   return link


def clean_date_tweet(date_str):
   """
   Get day name, day #, month, year, of a post. 
      Input: 
         • date_str: Date and time of tweet (str).
               Format example:
                  '2020-04-03 19:04:26+00:00'
                  (need to filter out +00:00)
                    
      Output:
         • date_obj: date/time of tweet (Datetime.datetime object)
   """

   # clean date string 
   cleaned_date_str = re.sub("\+.{5}", "", date_str)

   # turn string date/time into datetime object
   date_obj = datetime.datetime.strptime(cleaned_date_str, '%Y-%m-%d %H:%M:%S')
   return date_obj


def get_date_tweet(date_obj):

   """
      Get day name, day #, month, year, of a post. 
         Input: 
            • date_obj: date/time of tweet (Datetime.datetime object)

         Output:
            • day_name: The day of the week (Monday - Sunday)
            • day_num: The day of the month (1 - 31)
            • month: month name
            • year: year
   """

   # initialize array of days
   days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

   # initialize months
   months = ['January', 'February', 'March', 'April', 'May', 'June', 
          'July', 'August', 'September', 'October', 'November', 'December']

   # get day name
   day_name = days[date_obj.weekday()]

   # get day number
   day_num = date_obj.date().day

   # get month name
   month = months[date_obj.date().month - 1]

   # get year
   year = date_obj.date().year

   return [day_name, day_num, month, year]


def get_tweet_info(data, user_id):
   
   """
   Use user_id to get tweet info. For cases where a user has multiple tweets, use first tweet. 
      
      Input: 
         • data: pandas df
         • user_id: ID of user (int)
         • days: array of day names
         • months: array of month names
            
      Output:
         • user_id: ID of the user
         • text: text of tweet
         • tweet_link: link of tweet
         • date_time_tweet = [day, day #, month, year] of tweet
         • gru_prob, the probability of having outrage
   """

   # get tweet text
   filtered_data = data.loc[data['user_id'] == user_id, :]

   # if no rows, return
   if filtered_data.shape[0] == 0: 
      return "No entries with that user ID"

   # if the user has multiple tweets
   elif filtered_data.shape[0] > 1:
      # get dates for each of the tweets
      filtered_data['created_at'] = filtered_data['created_at'].apply(lambda x : clean_date_tweet(x))
      # get column names of filtered data
      #print(filtered_data)
      #print("The column names of the unfiltered data are : {}".format(filtered_data.columns))
      # get the earliest tweet
      min_index = np.argmin(np.array(filtered_data['created_at']))
      # get row corresponding to minimum value. This returns a series.
      min_series_arr = filtered_data.iloc[min_index, :]
      # turn series to dictionary
      min_dict = dict(min_series_arr)
      # turn dictionary into pandas df
      filtered_data = pd.DataFrame(min_dict, index = range(1))
      # change dtype of date column to string
      filtered_data['created_at'] = filtered_data['created_at'].astype('str')
      #print("The filtered dataset is : {}".format(filtered_data))
      #print("The shape of the dataset is : {}".format(filtered_data.shape))
      #print("The type of the dataset is : {}".format(type(filtered_data)))
      #print("THE columns of the data are: {}".format(filtered_data.columns))

   # reset indices
   filtered_data = filtered_data.reset_index()

   # get column names of filtered data
   #print("The column names are : {}".format(filtered_data.columns))
   
   # get tweet text
   tweet_text = filtered_data['text'][0]
   #print("THE text is: {}".format(tweet_text))
   # get tweet link
   tweet_link = get_link(filtered_data['user_screen_name'][0], filtered_data['tweet_id'][0])
   #print("THE tweet link is: {}".format(tweet_link))

   # get date and time
   date_time_arr = get_date_tweet(clean_date_tweet(filtered_data['created_at'][0]))
   #print("THE tweet date is : {}".format(date_time_arr))

   # get probability of outrage
   prob_outrage = filtered_data['gru_prob'][0]

   # return
   return [user_id, tweet_text, tweet_link, date_time_arr, prob_outrage]

def see_friend_and_DM_status(api, self_id, user_id, print_status_message):
   """
      Checks to see if we're already following a user
         Input: 
            • api: authenticated Twitter api object (tweepy)
            • self_id: your own ID
            • user_id: ID of the user to see if they're a friend
            • print_status_message: print your status with the other user? (bool, default = True)
         Output:
            • you_follow_them: do you follow them? (bool)
            • they_follow_you: do they follow you? (bool)
            • pending_request: did you sent them a pending friend request? (bool)
            • can_DM: can you DM them? (bool)

   """

   # get status of friendship between you and the other user
   try:
      friend_obj = api.show_friendship(source_id = user_id, target_id = self_id)
   # rate limit error
   except tweepy.error.TweepError:
      raise tweepy.error.TweepError("Rate limit reached")
   # general error
   except Exception as e:
      print(e)
      print("Unable to obtain a friend object from Twitter API (but not due to API)")
      return None

   """
   Note: here are the friendship possibilities that I've come up with:
      • Case 1: neither one of you follow each other (but you haven't sent a request)
      • Case 2: neither one of you follow each other (but you have sent a request)
      • Case 3: check for case where you follow them and they don't follow you
      • Case 4: check for case where you don't follow them, but they follow you
      • Case 5: check for case where you follow them and they follow you

   We can also check if we have a pending friend request with that user (this is a separate GET request)
      • Case 6: check for case where we have a pending friend request with them

   Independently from above, you can also check to see if you can DM them or not:
      • Case 7: check for case where we can't send them a DM 
         • If you're mutual friends (Case 5), you can send them DMs. For the other cases, 
         it's unclear (so you'd need to check manually)
   """

   # check friendship status
   friend_json = friend_obj[0]._json

   # check 1: see if you follow them:
   you_follow_them = bool(friend_json['followed_by'])

   # check 2: see if they follow you:
   they_follow_you = bool(friend_json['following'])

   # check 3: see if you have a pending request with them (this is a separate GET request)
   
   # this only applies if you don't follow them:
   if not you_follow_them:
      pending_follow_request = bool(dict(api.get_user('1935121784')._json)['follow_request_sent'])
   else:
      pending_follow_request = False

   # check 4: see if you can DM them. 

   # if you follow them and they follow you, you can send DMs
   if you_follow_them and they_follow_you: 
      can_DM = True
   else:
      can_DM = bool(friend_json['can_dm'])

   # print end message:
   if print_status_message:
      print("===================")
      print("Here's your status with user id: {}".format(user_id))
      print("Do you follow them? : {}".format(you_follow_them))
      print("Do they follow you? : {}".format(they_follow_you))
      print("Do you have a pending friend request with them? : {}".format(pending_follow_request))
      print("Can you DM them? : {}".format(can_DM))
      print("===================")

   return you_follow_them, they_follow_you, pending_follow_request, can_DM

def send_DM_to_user(user_id, tweet_text, link, tweet_date, script_str, api):

   """
   Sends a message to the user_id. 
   
      Input:
         • user_id: ID of user
         • tweet_text: what they tweeted
         • link: link of their tweet
         • tweet_date: date of their tweet
         • script_str: string of message
         • api: authenticated Twitter API

      Output:
         • api_DM: a tweepy DirectMessage API object
   """

   # modify string by including inputs
   script_str_subbed = re.sub("\[time\]", tweet_date + "\n\n" + tweet_text, script_str)
   script_str_subbed = re.sub("\[link to tweet\]", link, script_str_subbed)

   # send message
   try:
      # try to send message
      api_DM = api.send_direct_message(recipient_id = int(user_id), text = script_str_subbed)
      print("Message to user_id {} successfully sent!".format(user_id))
   except Exception as e:
      print("Message to user_id {} unsuccessful. Check error message: ".format(user_id))
      print(e) # couldn't find specific error in documentation. 

   return api_DM

def send_friend_request(user_id, api):
   
   """
   Sends a friend request to the user

      Input:
         • user_id: ID of the user to request as a friend
         • api: authenticated Twitter API

      Output:
         • friend: a tweepy.User object with output information
   """
   try:
      friend = api.create_friendship(user_id = user_id)
      print("Friend request to user_id {} successfully sent!".format(user_id))
      return friend
   except Exception as e:
      print("Error in sending friend request. See error: ")
      print(e)
      return false

def store_AWS(aws_access, aws_secret, local_file, bucket, directory, s3_file):

   """
   Takes the exported .csv file, and stores it into aws

   Input: 
      • aws_access: AWS access key
      • aws_secret: AWS secret key
      • local_file: name/location of local .csv file
      • bucket: name of bucket in AWS S3 storage (place to store data)
      • directory: name of directory to store the file
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
      s3.upload_file(local_file, bucket, directory + s3_file)
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

   ###### Part I: Preprocessing

   # get params
   parser = argparse.ArgumentParser(description = "File for sending DMs to users on Twitter, if their tweet was deemed to have outrage in it.")
   parser.add_argument("twitter_credentials", help = "Text file with Twitter developer credentials (consumer key, consumer secret, access key, access secret)")
   parser.add_argument("aws_credentials", help = "Text file with AWS credentials (AWS access, AWS secret)")
   parser.add_argument("import_tweets_name", help = "Name of file imported from AWS (from 'labelled_tweets/' directory, has outrage tweets labelled by classifier)", 
      default = "outrage_tweets_labelled_{}".format(datetime.datetime.today().strftime ('%d-%b-%Y'))) # named by current date, by default
   parser.add_argument("export_tweets_name", help = "Name to give to .csv file (without .csv extension) of messaged tweets exported to AWS")
   parser.add_argument("all_users_DMed_import_name", help = "Name of .csv file (without .csv extension), from AWS, that has the list of all users ever DMed")
   parser.add_argument("all_users_DMed_export_name", help = "Name of .csv file (without .csv extension), to export to AWS, that has the updated list of all users ever DMed")
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
   import_file_name = args.import_tweets_name + '.csv'
   export_file_name = args.export_tweets_name + '.csv'
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
      extract_from_AWS(aws_access, aws_secret, bucket, directory = 'labelled_tweets/', s3_file = import_file_name, local_file = import_file_name)
         
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

   
   # load file
   data = pd.read_csv(import_file_name, 
      lineterminator = '\n', 
      encoding = 'utf-8-sig')

   # check column names of data (data processed in R comes out differently, so we need to do some preprocessing to adjust for this)
   if 'status_id' in data.columns or 'user_id' in data.columns:
      # change user_id column
      data['user_id'] = data['user_id'].str.strip('x')
      data['user_id'] = data['user_id'].astype(int)
      # change status id
      data['status_id'] = data['status_id'].str.strip('x')
      data['status_id'] = data['status_id'].astype(int)
      # rename columns
      data.rename(columns = {'status_id':'tweet_id', 'screen_name':'user_screen_name'}, inplace = True)

   # loop through all user IDs, get info
   user_IDs = data.loc[:, 'user_id']

   # initalize arrays
   user_id_arr = []
   tweet_text_arr = []
   tweet_link_arr = []
   date_time_arr = []
   gru_prob_arr = []

   # loop through all IDs. If the tweet doesn't exist, skip 
   for index, user_id in enumerate(user_IDs):
      # update tweet number every 100
      if index % 100 == 0:
         print('\n')
         print("{0} IDs looped through, out of {1}".format(index, user_IDs.shape[0]))
         print('\n')

      # get relevant info
      try:
         # try to get relevant information
         user_id, tweet_text, tweet_link, date_time, gru_prob = get_tweet_info(data, user_id)

         # append IDs, text, link to arrays
         user_id_arr.append(user_id)
         tweet_text_arr.append(tweet_text)
         tweet_link_arr.append(tweet_link)
         gru_prob_arr.append(gru_prob)

         # get date, create custom format, append to array
         date_str = date_time[0] + ", " + str(date_time[2]) + " " + str(date_time[1]) + ", 2020"
         date_time_arr.append(date_str)

         #print("Information successfully appended to arrays")
      except ValueError:
         print("Error caught. User ID " + str(user_id) + " doesn't exist anymore.")
         continue

   # make df of outrage tweet and user info (for users whose tweet was marked as outrage and that tweet still exists)
   outrage_users_info = pd.DataFrame(zip(user_id_arr, tweet_text_arr, tweet_link_arr, date_time_arr, gru_prob_arr), 
      columns = ['user_id', 'tweet_text', 'tweet_link', 'tweet_date', 'gru_prob'])

   # read in the script to send
   script_str = ''
   with open('twitter_DM_script.txt', 'r') as script:
      for line in script.readlines():
         script_str += line

   # filter so that we only use those people whose gru_prob > 0.95
   outrage_users_info = outrage_users_info[outrage_users_info['gru_prob'] > 0.95]
   outrage_users_info.reset_index(inplace = True) # reset index
   
   ###### Part II: Send friend requests / DMs to users:

   users_DMed_import_file = args.all_users_DMed_import_name + '.csv'
   users_DMed_export_file = args.all_users_DMed_export_name + '.csv'

   # import list of users who have been DMed before
   try: 
      # try extraction
      extract_from_AWS(aws_access, aws_secret, bucket, directory = 'lists_users_DMed/', s3_file = users_DMed_import_file, local_file = users_DMed_import_file)
         
      # check if file was exported successfully:
      if users_DMed_import_file in os.listdir():
         print("{} file (with the list of all users previously DMed) successfully imported from AWS. Proceeding with parsing...".format(users_DMed_import_file))
         print("\n")
      else:
         print("{} not found in the current directory (something may have gone wrong in the import?)".format(import_file_name))
         raise ValueError("Data could not be imported")
   except Exception as e:
      print("Extraction from AWS failed. Please see error message: ")
      print(e)

   # get df of people who we've DMed
   df_users_DMed = pd.read_csv(users_DMed_import_file)
   print("This is the number of people that we have sent DMs to so far (prior to running this session of the code): {}".format(df_users_DMed.shape[0]))

   # get IDs of people that we've already DMed, track number of users we've previously DMed
   user_ids_previously_messaged = set(df_users_DMed['user_ids'])
   num_users_previously_DMed = 0

   # get own Twitter ID
   self_id = dict(api.me()._json)['id']

   # initialize number of new friend requests
   number_new_friend_requests = 0

   # loop through all users, send friend requests + DMs
   for i in range(outrage_users_info.shape[0]):

      # get vars
      user_id = outrage_users_info.loc[i, 'user_id']
      text = outrage_users_info.loc[i, 'tweet_text']
      link = outrage_users_info.loc[i, 'tweet_link']
      date = outrage_users_info.loc[i, 'tweet_date']

      # get your friend/DM status with the user
      try:
         # check to see if user has been DMed before. If so, don't use API call to get their information. Skip their iteration
         if int(user_id) not in user_ids_previously_messaged:
            #you_follow_them, they_follow_you, pending_follow_request, can_DM = see_friend_and_DM_status(api, self_id, user_id, print_status_message = True)
            you_follow_them = True
            they_folow_you = True
            pending_follow_request = False
            can_DM = True
         else: 
            print("You can't message user id = {} because you've already DMed them before. Moving to next user id...".format(user_id))
            num_users_previously_DMed += 1
            print("While running this program, you've encountered {} users who you've DMed before".format(num_users_previously_DMed))
            continue
      # if there is an error, it'll be because the rate limit will be exceeded. Stop the loop here
      except tweepy.error.TweepError:
         print("Rate limit reached")
         print("Limit reached after {} users processed ".format(i))
         """
         will_continue = input("Would you like to wait for the rate limit to reset? If so, type 'y' and then hit Enter. Otherwise, enter any other key : ")
         if will_continue == 'y':
            seconds_waiting = 15 * 60
            print("Program will wait for {} seconds and try again".format(seconds_waiting))
            time.sleep(seconds_waiting)
            continue
         else:
            break
         """
         seconds_waiting = 15 * 60
         print("Program will wait for {} seconds and try again".format(seconds_waiting))
         time.sleep(seconds_waiting)
         continue
      # for any other possible errors (e.g., not enough values to unpack), skip this iteration of the loop
      except Exception as e:
         print(e)
         continue

      """
      # send friend request to user if you don't follow them and if you don't have a pending follow request
      if not you_follow_them and not pending_follow_request and int(user_id) not in user_ids_previously_messaged:
         try: 
            print("The following user is one who we can send a friend request to: {}".format(user_id))
            send_friend_request(user_id, api)
            number_new_friend_requests += 1
            print("We've sent {} additional friend requests by running this script".format(number_new_friend_requests))
         except Exception as e:
            print("Friend request to user_id = {} unsuccessful".format(user_id))
            print(e)
      """

      # send DM to user, if you can DM them and if they're not in the list of people we've already DMed
      if can_DM and int(user_id) not in user_ids_previously_messaged and int(user_id) not in set(df_users_DMed['user_ids']):
         try:
            print("The following user is one who we can DM: {}".format(user_id))
            send_DM_to_user(user_id, text, link, date, script_str, api)
            # get time that the DM was sent
            time_DM_sent = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
            # gather information to add to users who received DMs (name of user, their ID, and when the DM was sent:
            values_to_add_sent_DM = {'user_names' : data.loc[np.where(data['user_id'] == user_id)[0], 'user_screen_name'].values[0], 
                                    'user_ids' : user_id, 
                                    'date_time_messaged' : time_DM_sent}
            df_users_DMed = df_users_DMed.append(values_to_add_sent_DM, sort = False, ignore_index = True)
            print("User id : {}  - successfully added to list of DMed users".format(user_id))
         except Exception as e:
            print("DM to user_id = {} unsuccessful.".format(user_id))
            print(e)
            print("Sending friend request: ")
            # if we can't send a DM, we send a friend request
            try: 
               send_friend_request(user_id, api)
               number_new_friend_requests += 1
               print("We've sent {} additional friend requests by running this script".format(number_new_friend_requests))
            except Exception as e:
               print("Friend request unsuccessful. See error message: ")
               print(e)

      else:
         if int(user_id) in user_ids_previously_messaged:
            print("You can't message user id = {} because you've already DMed them before".format(user_id))
            num_users_previously_DMed += 1
            print("You've encountered {} users who you've DMed before".format(num_users_previously_DMed))
         elif not can_DM:
            print("Due to permissions on their account / Twitter, you can't DM them (straight from Twitter status object)")
         else:
            print("You can't DM this user (but reason is unknown)")

   print("This is the new total number of users who we've sent DMs to: {}".format(df_users_DMed.shape[0]))
   print("This is the total number of users in our present dataset who we've DMed before: {}".format(num_users_previously_DMed))

   # export list of users who were supposed to received DMs (to .csv and to AWS)
   outrage_users_info.to_csv(export_file_name, index = False)

   # export list of all users who actually received DMs
   df_users_DMed = df_users_DMed.loc[:, ['user_names', 'user_ids', 'date_time_messaged']]
   df_users_DMed.to_csv(users_DMed_export_file)

   # re-upload to AWS (store_AWS)
   try:
      print("Storing tweets/IDs/date of tweets of those users who were supposed to receive DMs")
      store_AWS(aws_access, aws_secret, export_file_name, bucket, directory = "messaged_users_tweets/", s3_file = export_file_name)
      print("Tweets successfully stored in AWS (For all users who, in this session, were supposed to receive DMs - need to cross-check with list that actually received DMs)")
   except Exception as e:
      print("AWS storage unsuccessful. Please see error message: ")
      print(e)

   # re-uploaded list of all users who have received DMs, across all iterations
   try:
      print("Storing tweets/IDs/date of tweets of ALL users who have received DMs")
      store_AWS(aws_access, aws_secret, users_DMed_export_file, bucket, directory = 'lists_users_DMed/', s3_file = users_DMed_export_file)
      print("Updated list of ALL users who have received DMs: successfully stored in AWS")
   except Exception as e:
      print("AWS storage unsuccessful. Please see error message: ")
      print(e)


   print("Script execution finished.")

if __name__ == "__main__":
   main()




