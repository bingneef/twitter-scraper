import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import pandas as pd
from textblob import TextBlob
from textblob_nl import PatternTagger, PatternAnalyzer
import re

nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()


def find_user_active_days(tweet_date, user_created_date):
    return (tweet_date - user_created_date).days


def tweet_contains_url(text):
    regex = "https?://"
    return bool(re.search(regex, text))


def find_polarity(text, lang):
    if lang == 'nl':
        blob = TextBlob(text, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())
        return blob.sentiment[0]

    return sia.polarity_scores(text)['compound']


def find_sentiment_text(sentiment):
    if sentiment > 0:
        return 'Positive'
    elif sentiment < 0:
        return 'Negative'
    else:
        return 'Neutral'


def add_proxy_metrics():
    df = pd.read_pickle("data/output/aggregated.pkl")

    df['UserActiveDays'] = df.apply(lambda x: find_user_active_days(x['Datetime'], x['UserCreatedAt']), axis=1)
    df['TweetContainsUrl'] = df['Text'].apply(tweet_contains_url)
    df['TweetLength'] = df['Text'].apply(len)

    # Sentiment
    df['Sentiment'] = df.apply(lambda x: find_polarity(x['Text'], x['Language']), axis=1)
    df['SentimentPositive'] = df['Sentiment'] > 0
    df['SentimentNegative'] = df['Sentiment'] < 0
    df['SentimentNeutral'] = df['Sentiment'] == 0
    df['SentimentText'] = df['Sentiment'].apply(find_sentiment_text)

    df.to_pickle("data/output/aggregated_with_proxy_metrics.pkl")

    print("Done with proxy metrics")
