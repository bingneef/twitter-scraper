import os
from elastic_transport import ConnectionTimeout
from elasticsearch import Elasticsearch as LibElasticSearch, BadRequestError
from urllib3.exceptions import InsecureRequestWarning
import numpy as np
import pandas as pd
import math
import time


mapping = {
    "properties": {
        "Datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ssZZZZZ"},
        "Search term": {"type": "keyword"},
        "Tweet Id": {"type": "long"},
        "Text": {"type": "text"},
        "TextDashboard": {"type": "keyword"},
        "Username": {"type": "keyword"},
        "UserCreatedAt": {"type": "date", "format": "yyyy-MM-dd HH:mm:ssZZZZZ"},
        "UserActiveDays": {"type": "long"},
        "UserFollowersCount": {"type": "integer"},
        "UserFriendCount": {"type": "integer"},
        "UserStatusesCount": {"type": "integer"},
        "UserVerified": {"type": "boolean"},
        "UserLocation": {"type": "keyword"},
        "LikeCount": {"type": "integer"},
        "QuoteCount": {"type": "integer"},
        "RetweetCount": {"type": "integer"},
        "Hashtags": {"type": "keyword"},
        "URL": {"type": "text"},
        "URLDashboard": {"type": "keyword"},
        "TweetContainsUrl": {"type": "boolean"},
        "TweetLength": {"type": "integer"},
        "Language": {"type": "keyword"},
        "Sentiment": {"type": "float"},
        "SentimentPositive": {"type": "boolean"},
        "SentimentNegative": {"type": "boolean"},
        "SentimentNeutral": {"type": "boolean"},
        "SentimentText": {"type": "keyword"}
    }
}


def get_readable_language(lang):
    if lang == 'nl':
        return 'Dutch'
    elif lang == 'en':
        return 'English'

    print(f"Unmapped language {lang}")
    return 'Unknown'


def get_readable_hashtags(hashtags):
    lowercase = map(str.lower, hashtags)
    return list(dict.fromkeys(lowercase))


def store():
    client = LibElasticSearch(
        os.getenv("ELASTIC_URL"),
        basic_auth=(os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD"))
    )
    docs = pd.read_pickle('data/output/aggregated_with_proxy_metrics.pkl')

    docs['UserLocation'] = docs['UserLocation'].fillna("Unknown")
    docs['ReadableDatetime'] = docs['Datetime'].astype(str)
    docs['ReadableUserCreatedAt'] = docs['UserCreatedAt'].astype(str)
    docs['ReadableLanguage'] = docs['Language'].apply(get_readable_language)
    docs['ReadableHashtags'] = docs['Hashtags'].apply(get_readable_hashtags)

    index_name = "sample_twitter"
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
    client.indices.create(index=index_name)

    client.indices.put_mapping(
        index=index_name, body=mapping,
    )

    number_of_chunks = math.ceil(len(docs) / 50)
    print(f"Processing {number_of_chunks} doc chunks")

    for index, doc_chunk in enumerate(np.array_split(docs, number_of_chunks)):
        operations = doc_chunk.apply(lambda doc: [
            {
                'index': {
                    '_index': index_name,
                    '_id': doc['Tweet Id']
                }
            },
            {
                'Datetime': doc['ReadableDatetime'],
                'SearchTerm': doc['SearchTerm'],
                'Tweet Id': doc['Tweet Id'],
                'Text': doc['Text'],
                'TextDashboard': doc['Text'],
                'Username': doc['Username'],
                'UserCreatedAt': doc['ReadableUserCreatedAt'],
                'UserActiveDays': doc['UserActiveDays'],
                'UserFollowersCount': doc['UserFollowersCount'] or 0,
                'UserFriendCount': doc['UserFriendCount'] or 0,
                'UserStatusesCount': doc['UserStatusesCount'] or 0,
                'UserVerified': doc['UserVerified'],
                'UserLocation': doc['UserLocation'],
                'LikeCount': doc['LikeCount'] or 0,
                'QuoteCount': doc['QuoteCount'] or 0,
                'RetweetCount': doc['RetweetCount'] or 0,
                'Hashtags': doc['ReadableHashtags'],
                'URL': doc['URL'],
                'URLDashboard': doc['URL'],
                'TweetContainsUrl': doc['TweetContainsUrl'],
                'TweetLength': doc['TweetLength'],
                'Language': doc['ReadableLanguage'],
                'Sentiment': doc['Sentiment'],
                'SentimentPositive': doc['SentimentPositive'],
                'SentimentNegative': doc['SentimentNegative'],
                'SentimentNeutral': doc['SentimentNeutral'],
                'SentimentText': doc['SentimentText'],
            }
        ], axis=1).explode()

        if index % 100 == 0:
            print(f"{index} of {number_of_chunks}")
        try:
            response = client.bulk(
                operations=operations
            )
            if response['errors'] is True:
                print(response)

        except BadRequestError as err:
            print(f"Unexpected {err=}, {type(err)=}")
        except ConnectionTimeout:
            print("Sleeping for 10 seconds")
            time.sleep(10)

    print(f"Done with {number_of_chunks} chunks")
