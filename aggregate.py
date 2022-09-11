from os import listdir
from os.path import isfile, join
import pandas as pd
from util import dtypes, columns


def aggregate_files():
    path = 'data/raw/'
    files = [f for f in listdir(path) if isfile(join(path, f))]

    tweets_df = pd.DataFrame(
        [],
        columns=columns,
    )

    tweets_df = tweets_df.astype(
        dtypes
    )

    for file in files:
        tweets_df = pd.concat([tweets_df, pd.read_pickle(path + file)], ignore_index=True)

    tweets_df.to_pickle("data/output/aggregated.pkl")
    print("Done with aggregating files")
