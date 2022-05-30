from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, Float


class DBConnection:
    def __init__(self):
        self.engine = create_engine('sqlite:///ALL_TWEETS.db')
        self.meta = MetaData()

    def create_table(self, tweets_for):
        tweets = Table(
            f'{tweets_for}_TWEETS', self.meta,
            Column('id', Integer, primary_key=True),
            Column('text', Text),
            Column('lang', String(20)),
            Column('author_id', String(100)),
            Column('retweet_count', Integer),
            Column('reply_count', Integer),
            Column('like_count', Integer),
            Column('quote_count', Integer),
            Column('tweet_id', Float),
            Column('created_at', String(100)),
            Column('referenced_tweets_id', String(100)),
            Column('referenced_tweets_type', String(100)))

        self.meta.create_all(self.engine)
        return tweets

    def insert_tables(self, table, contents):
        for tweet in contents["data"]:
            # print(contents["data"])
            try:
                reference_tweet_id = tweet["referenced_tweets"][0]["id"]
                reference_tweet_type = tweet["referenced_tweets"][0]["type"]
            except KeyError:
                reference_tweet_id = ""
                reference_tweet_type = ""
            ins = table.insert()
            ins = table.insert().values(
                text=tweet["text"],
                lang=tweet["lang"],
                author_id=tweet["author_id"],
                retweet_count=tweet["public_metrics"]["retweet_count"],
                reply_count=tweet["public_metrics"]["reply_count"],
                like_count=tweet["public_metrics"]["like_count"],
                quote_count=tweet["public_metrics"]["quote_count"],
                tweet_id=tweet["id"],
                created_at=tweet["created_at"],
                referenced_tweets_id=reference_tweet_id,
                referenced_tweets_type=reference_tweet_type,
            )
            # print (tweet["text"])
            conn = self.engine.connect()
            conn.execute(ins)
