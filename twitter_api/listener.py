from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from abc import ABC, abstractmethod

from twitter_api.credentials import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET

from db_env.mongo_connection import DataInserter


class TwitterAuthenticator(ABC):
    @abstractmethod
    def __init__(self):
        self.auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        self.auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
        self.api = API(self.auth)


class TwitterListener(StreamListener):
    def __init__(self):
        super().__init__()
        self.inserter = DataInserter()

    def on_status(self, status):
        if (not status.retweeted) and ('RT @' not in status.text):
            document = status._json
            print(document)
            self.inserter.insert_one(document)

    def on_error(self, status_code):
        if status_code == 420:
            return False


class Streamer(TwitterAuthenticator):
    def __init__(self):
        super().__init__()

    def stream(self, filter_list: list):
        stream_listener = TwitterListener()
        stream = Stream(auth=self.api.auth, listener=stream_listener)
        stream.filter(languages=['en'], track=filter_list)


obj = Streamer()
obj.stream(['stock market'])
