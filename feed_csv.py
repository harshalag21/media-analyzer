import time

from kafka_producer import Kafka
import pandas as pd

data = set(
    list(
        pd
        .read_csv(
            "scraper/data/allsides_news_complete3.csv",
            delimiter="|")["heading"]
    ) +
    list(
        pd
        .read_csv(
            "scraper/data/allsides_news_complete3.csv",
            delimiter="|")["title"]
    )
)
kf = Kafka()
cnt = len(data)
ticker = 0
while cnt:
    kf.publish(data.pop(), "news")
    cnt -= 1
    ticker += 1
    if ticker % 100 == 0:
        time.sleep(30)
