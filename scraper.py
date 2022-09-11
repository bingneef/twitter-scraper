import snscrape.modules.twitter as sntwitter
import pandas as pd
import urllib.parse

from util import dtypes, columns


def _scrape_search_term_on_twitter(search_term, lang='en'):
    pickle_path = f"data/raw/{lang}-{urllib.parse.quote(search_term)}.pkl"

    daterange = pd.date_range(start="2010-01-01", end="2022-08-31").date.tolist()
    prev_date = None
    tweets_list = []

    for date in daterange:
        if prev_date is None:
            prev_date = date
            continue

        print(f"Running daterange: {prev_date} to {date} for {search_term}")

        query = f"'{search_term}' -filter:retweets since:{prev_date} until:{date} lang:{lang}"

        if lang == "en":
            query = f"{query} geocode:47.751569,1.675063,3000km"

        for i, tweet in enumerate(
                sntwitter.TwitterSearchScraper(query).get_items()):

            if i > 100:
                break

            clean_content = ''.join(tweet.content.splitlines())
            tweets_list.append([
                tweet.date,
                search_term,
                tweet.id,
                clean_content,
                tweet.user.username,
                tweet.user.created,
                tweet.user.followersCount,
                tweet.user.friendsCount,
                tweet.user.statusesCount,
                tweet.user.verified,
                tweet.user.location,
                tweet.likeCount,
                tweet.quoteCount,
                tweet.retweetCount,
                tweet.hashtags or [],
                tweet.url,
                lang
            ])

        prev_date = date

    # Creating a dataframe from the tweets list above
    tweets_df = pd.DataFrame(
        tweets_list,
        columns=columns
    )

    tweets_df = tweets_df.astype(
        dtypes
    )

    tweets_df.to_pickle(pickle_path)


def scrape_twitter():
    for search_term in [
        # "housing market",
        # "mortgage",
        # "housing prices",
        # "rent",
        # "property",
        # "real estate",
        "economy"
    ]:
        _scrape_search_term_on_twitter(search_term, lang='en')

    for search_term in [
        # "huizenmarkt",
        # "huizenprijzen",
        # "huurhuis",
        # "hypotheek",
        # "koophuis",
        # "overbieden",
        # "sociale huur",
        # "huurwoning",
        # "koopwoning",
        "economie",
        "jubelton"
    ]:
        _scrape_search_term_on_twitter(search_term, lang='nl')
