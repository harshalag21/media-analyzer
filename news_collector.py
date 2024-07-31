import re
import sys

import newsapi.newsapi_exception
from newsapi import NewsApiClient
import praw
from praw import exceptions as praw_exceptions
from praw.models.reddit.subreddit import SubredditStream
from config.parsedconfig import *
from kafka_producer import Kafka


def extract_hostname(url_string):
    """
    Extract the hostname from the url string
    :param url_string:
    :return:
    """
    pattern = r'^(?:https?:\/\/)?(?:www\.)?([^\/:]+)'
    match = re.search(pattern, url_string)
    if match:
        return match.group(1)
    return "user"


class RedditScraper:
    def __init__(self):
        """
        Initializes the Reddit Scraper class
        """
        self.message_sent = 1
        try:
            # Initialise reddit API
            self.reddit = praw.Reddit(
                client_id=reddit_client_id,
                client_secret=reddit_client_secret,
                password=reddit_password,
                user_agent=reddit_user_agent,
                username=reddit_username,
            )
            print(f"Connected to Reddit: {reddit_username}")
        except praw_exceptions.PRAWException as rde:
            print(f"Failed to connect to Reddit: {rde}")

        # Initialize Kafka producer
        self.kafka_producer = Kafka()

    def _publish_submission(self, source, headline):
        self.kafka_producer.publish(
            key=source,
            text=f"{headline}",
            topic=input_topic,
        )
        sys.stdout.write(f"\rSent {self.message_sent} articles to Kafka queue")
        self.message_sent += 1

    def stream(self, subreddit="news"):
        """
        Stream subreddits to Kafka topic
        :param subreddit: name of subreddit to stream
        :return:
        """
        try:
            # Generate streamer instance
            streamer = SubredditStream(self.reddit.subreddit(subreddit))
            print(f"Connected to Subreddit: {subreddit}")
        except praw_exceptions.PRAWException as rde:
            print(f"Failed to connect to Subreddit: {rde}")
            sys.exit(1)

        # Streaming to Kafka
        print(f"Fetching top posts from: {subreddit}")
        for submission in self.reddit.subreddit(subreddit).new(limit=1000):
            if submission is None:
                break
            self._publish_submission(
                extract_hostname(submission.url),
                submission.title
            )
        print(f"\nStreaming subreddits: {subreddit}")
        for submission in streamer.submissions():
            if submission is None:
                continue
            self._publish_submission(
                extract_hostname(submission.url),
                submission.title
            )


class NewsCollector:
    def __init__(self):
        self.message_sent = 1
        try:
            # Initialise News API
            self.newsapi = NewsApiClient(api_key=news_api_key)
            print(f"Connected to NewsAPI")
        except newsapi.newsapi_exception.NewsAPIException as nwe:
            print(f"Failed to connect to NewsAPI: {nwe}")

        # Initialize Kafka producer
        self.kafka_producer = Kafka()
        self.processed_id = set()

    def _publish_submission(self, source, headline):
        self.kafka_producer.publish(
            key=source,
            text=headline,
            topic=input_topic
        )
        sys.stdout.write(f"\rSent {self.message_sent} articles to Kafka queue")
        self.message_sent += 1

    @staticmethod
    def _check_article(article):
        return (
                "title" in article and
                "url" in article and
                article["title"] is not None and
                "[Removed]" not in article["title"]
        )

    def stream_news(self):
        page = 1
        while True:
            response = self.newsapi.get_top_headlines(page=page, page_size=100, language='en')
            articles = response.get('articles', [])
            if not articles:
                break
            for article in articles:
                if self._check_article:
                    if f"{extract_hostname(article.get('url'))}_{article.get('title')}" not in self.processed_id:
                        self.processed_id.add(
                            f"{extract_hostname(article.get('url'))}_{article.get('title')}"
                        )
                        self._publish_submission(
                            source=extract_hostname(article.get('url')),
                            headline=article.get("title")
                        )
            page += 1


if __name__ == "__main__":
    try:
        news_fetcher = NewsCollector()
        news_fetcher.stream_news()
    except newsapi.newsapi_exception.NewsAPIException as e:
        print(f"\nNewsAPI limit reached, switching to Reddit source")
        reddit = RedditScraper()
        reddit.stream()
