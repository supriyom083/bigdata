def get_inner_dict(prefix_val,dict_obj):
    inner_dict={}
    result = {}
    inner_prefix_val=prefix_val
    for i in dict_obj.items():

        if (isinstance(i[1],dict)):

            result=get_inner_dict(prefix_val+'/'+i[0],i[1])
            for k in result.items():
                temp= str(filter(lambda x: x in printable, str(k[1])))
                inner_dict[k[0]]=temp


        else:
            temp=str(filter(lambda x: x in printable, str(i[1])))
            inner_dict[inner_prefix_val + '/' + i[0]] = temp

            #print(inner_prefix_val+'***'+i[0])
    return inner_dict

from dateutil import parser
import string
import time
import tweepy
import json
from pprint import pprint
import MySQLdb
import extract_check_sql_gen
import ftfy
import sys
import re
from  textblob import TextBlob
import string
import geocoder
list_search=sys.argv[1:]
printable = set(string.printable)
reload(sys)
sys.setdefaultencoding('utf-8')
# Authentication details. To  obtain these visit dev.twitter.com
consumer_key = 'kektIdHBwa17xg2JPw7SEhe6q'
consumer_secret = 'y397ldkvvtg8XsWLcHUKl20F1CTR0f9k8MhOwYl3Gn0JGbTEE4'
access_token_key = '156610052-tz2hLiSEGykBXlpcv3ruzjdrkmPiBEkmO8cyfMrK'
access_token_secret = 'pLep5FCXTnFFjfPwyNh9cj3zmzTDPLVIOn6xgoW91WEjs'
db = MySQLdb.connect("localhost", "root", "supriyo", "twitter_db")
cursor = db.cursor()
with open('user_attr_list.txt', 'r') as f:
    content = f.readlines()
lines = [x.strip() for x in content]
twitter_details_data = {}


# This is the listener, resposible for receiving data

class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):

        try:


            result = json.loads(data, encoding="unicode")
            
            #outer_tweet = twitter_object.get_twitter_details(result)
            inner_tweet = {}
            inner_user = {}
            t = {}
            #print(result['place'])
            k=get_inner_dict('outer',result)
            #k['outer/retweeted_status/quote_count']=0
            for j in k.items():
                pass #print(j[0]+" "+j[1])
            place = {'country_code': '',
                     'country': '',
                     'place_type': '',
                     'bounding_box': '',
                     'type': '',
                     'coordinates': '',
                     'full_name': '',
                     'id': '',
                     'name': '',
                     'url': '',
                     'long': '',
                     'lat': ''}
            try:
		print(result['place'])
                try:
                    place['country_code'] = result['place']['country_code']
                    place['country'] = result['place']['country']
                    place['place_type'] = result['place']['place_type']
                    place['full_name'] = result['place']['full_name']
                    place['id'] = result['place']['id']
                    place['name'] = result['place']['name']
                    place['url'] = result['place']['url']
                    t = result['place']['bounding_box']
                    place['long'] = (t['coordinates'][0][0][0])
                    place['lat'] = (t['coordinates'][0][0][1])
                    g = geocoder.google([place['lat'],  place['long']], method='reverse')
                    k['outer/country_code']= g.state_long       #place['country_code']
                    k['outer/place_country'] = place['country']
                    k['outer/place_type'] = place['place_type']
                    k['outer/full_name'] = place['full_name']
                    k['outer/place_id'] = place['id']
                    k['outer/place_name'] = place['name']
                    k['outer/place_url'] = place['url']
                    k['outer/long'] = place['long']
                    k['outer/lat'] = place['lat']
                except:
                    k['outer/country_code'] = ''
                    k['outer/place_country'] = ''
                    k['outer/place_type'] = ''
                    k['outer/full_name'] = ''
                    k['outer/place_id'] = ''
                    k['outer/place_name'] = ''
                    k['outer/place_url'] =''
                    k['outer/long'] = ''
                    k['outer/lat'] = ''
                    
                    pass
            except:
                k['outer/country_code'] = ''
                k['outer/place_country'] = ''
                k['outer/place_type'] = ''
                k['outer/full_name'] = ''
                k['outer/place_id'] = ''
                k['outer/place_name'] = ''
                k['outer/place_url'] =''
                k['outer/long'] = ''
                k['outer/lat'] = ''
            


            #outer_tweet=get_inner_dict('outer',result)
            if k.has_key('outer/retweeted_status/retweet_count'):
                None
            else:
                k['outer/retweeted_status/retweet_count']=0

            if k.has_key('outer/retweeted_status/user/id_str'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/id_str']=k['outer/user/id_str']
                except:
                    k['outer/retweeted_status/user/id_str'] = ''



            if k.has_key('outer/user/created_at'):

                dt = parser.parse(k['outer/user/created_at'])
                k['outer/user/created_at'] = dt.strftime("%Y-%m-%d %I:%M:%S")

            else:
                try:
                    dt = parser.parse(k['outer/user/created_at'])
                    k['outer/user/created_at'] = dt.strftime("%Y-%m-%d %I:%M:%S")
                except:
                    k['outer/user/created_at'] = ''



            if k.has_key('outer/retweeted_status/user/created_at'):
                dt=parser.parse(k['outer/retweeted_status/user/created_at'])
                k['outer/retweeted_status/user/created_at'] = dt.strftime("%Y-%m-%d %I:%M:%S")

            else:
                try:
                    dt=parser.parse(k['outer/user/created_at'])
                    k['outer/retweeted_status/user/created_at'] = dt.strftime("%Y-%m-%d %I:%M:%S")
                except:
                    k['outer/retweeted_status/user/created_at'] = ''



            if k.has_key('outer/created_at'):

                dt = parser.parse(k['outer/created_at'])

                k['outer/created_at'] = dt.strftime("%Y-%m-%d %I:%M:%S")
            else:
                try:
                    dt = parser.parse(k['outer/created_at'])
                    k['outer/created_at'] = dt.strftime("%Y-%m-%d %I:%M:%S")
                except:
                    k['outer/created_at'] = ''

            if k.has_key('outer/retweeted_status/user/listed_count'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/listed_count'] = k['outer/user/listed_count']
                except:
                    k['outer/retweeted_status/user/listed_count'] = ''


            if k.has_key('outer/retweeted_status/user/utc_offset'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/utc_offset'] = k['outer/user/utc_offset']
                except:
                    k['outer/retweeted_status/user/utc_offset'] = ''


            if k.has_key('outer/retweeted_status/user/followers_count'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/followers_count'] = k['outer/user/followers_count']
                except:
                    k['outer/retweeted_status/user/followers_count'] = ''

            if k.has_key('outer/retweeted_status/user/statuses_count'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/statuses_count'] = k['outer/user/statuses_count']
                except:
                    k['outer/retweeted_status/user/statuses_count'] = ''


            if k.has_key('outer/retweeted_status/user/description'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/description'] = k['outer/user/description']
                except:
                    k['outer/retweeted_status/user/description'] = ''


            if k.has_key('outer/retweeted_status/user/friends_count'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/friends_count'] = k['outer/user/friends_count']
                except:
                    k['outer/retweeted_status/user/friends_count'] = ''



            if k.has_key('outer/retweeted_status/user/screen_name'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/screen_name'] = k['outer/user/screen_name']
                except:
                    k['outer/retweeted_status/user/screen_name'] = ''


            if k.has_key('outer/retweeted_status/user/favourites_count'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/favourites_count'] = k['outer/user/favourites_count']
                except:
                    k['outer/retweeted_status/user/favourites_count'] = ''


            if k.has_key('outer/retweeted_status/user/location'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/location'] = k['outer/user/location']
                except:
                    k['outer/retweeted_status/user/location'] = ''


            if k.has_key('outer/retweeted_status/user/lang'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/lang'] = k['outer/user/lang']
                except:
                    k['outer/retweeted_status/user/lang'] = ''


            if k.has_key('outer/retweeted_status/user/name'):
                None
            else:
                try:
                    k['outer/retweeted_status/user/name'] = k['outer/user/name']
                except:
                    k['outer/retweeted_status/user/name'] = ''


            if k.has_key('outer/retweeted_status/filter_level'):
                None
            else:
                try:
                    k['outer/retweeted_status/filter_level'] = k['outer/filter_level']
                except:
                    k['outer/retweeted_status/filter_level'] = ''



            k['outer/polarity']=TextBlob(k['outer/text']).sentiment.polarity
            k['outer/subjectivity']=TextBlob(k['outer/text']).sentiment.subjectivity

            #print('********************************************')


            sql=extract_check_sql_gen.gen_sql_dict(k)
	    #print(sql)
            try:

                #print(sql)
                #db.commit()
		#if result['place']:
			#print(sql)
		cursor.execute(sql)
		db.commit()

            except:
                None
                print(sql)
                print("Unexpected error:", sys.exc_info()[0])
                pass

            db.commit()
        except:
	    print("Unexpected error:", sys.exc_info()[0])	
            pass
	    #cursor.close()

        return True

    def on_status(self, status):
        # Prints the text of the tweet
        # print('Tweet text: ' + status.text)

        # There are many options in the status object,
        # hashtags can be very easily accessed.
        for hashtag in status.entities['hashtags']:
            print(hashtag['text'])

        return True

    def on_error(self, status):
        time.sleep(60)
        pass
        print(status)
	#cursor.close()



if __name__ == '__main__':
    l = StdOutListener()
    #print('hi')


    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token_key, access_token_secret)

    # There are different kinds of streams: public stream, user stream, multi-user streams
    # In this example follow #programming tag
    # For more details refer to https://dev.twitter.com/docs/streaming-apis
    stream = tweepy.Stream(auth, l)
    stream.filter(track=list_search, languages=['en'])

