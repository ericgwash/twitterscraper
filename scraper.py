from multiprocessing import Process
import snscrape.modules.twitter as sntwitter
import pandas as pd
import time
import os

# Setting variables to be used below
tags = [
    "#hashtag1",
    "#hashtag2",
    "#hashtag3",
    "#hashtag4",
    "#hashtag5",
    "#hashtag6",
    "#hashtag7",
    "#hashtag8",
    "#hashtag9",
]
since_time = "2023-01-01"
until_time = "2023-02-16"

start_time = time.time()

run_details = open("run_details.txt", "w")

def scrape(tag):
    tag_run_details = open(f"{tag[1:]}.txt", "w")
    t1 = time.time()
    tweets_list2 = []
    
    # Using TwitterSearchScraper to scrape data and append tweets to list
    for tweet in sntwitter.TwitterSearchScraper(
        f"{tag} since:{since_time} until:{until_time} lang:en"
    ).get_items():
        tweets_list2.append(
            [
                tweet.date,
                tweet.id,
                tweet.user.username,
                tweet.rawContent,
                tweet.user.location,
                tweet.likeCount,
                tweet.retweetCount,
                tweet.replyCount,
                tweet.quoteCount,
            ]
        )
        print(f"Scraping Tweet: {tweet.id}")

    # Creating a dataframe from the tweets list above
    df1 = pd.DataFrame(
        tweets_list2,
        columns=[
            "Datetime",
            "Id",
            "UserName",
            "Tweet",
            "Location",
            "Likes",
            "Retweets",
            "Replies",
            "Quotes",
        ],
    )

    out_file_name = f"twitter_data/{tag[1:]}.csv"
    df1.to_csv(out_file_name)

    t2 = time.time()
    t_tag = t2 - t1
    num_tweets = len(tweets_list2)
    tag_run_details.write(
        f"Scraped {num_tweets} tweets for the tag {tag} and it took {t_tag} seconds"
    )
    tag_run_details.close()


process_pool = []

for tag in tags:
    p = Process(target=scrape, args=[tag])
    p.start()
    process_pool.append(p)

for pr in process_pool:
    pr.join()

end_time = time.time()

run_details.write(f"Scraping in total took {end_time - start_time} seconds")

run_details.close()