from configparser import ConfigParser

config = ConfigParser()
config.read(["./config/config.ini"])

bootstrap_servers = config.get("KAFKA", "bootstrap_servers")

news_api_key = config.get("NEWSAPI", "news_api_key")

reddit_username = config.get("REDDIT", "username")
reddit_password = config.get("REDDIT", "password")
reddit_client_id = config.get("REDDIT", "client_id")
reddit_client_secret = config.get("REDDIT", "client_secret")
reddit_user_agent = config.get("REDDIT", "user_agent")

input_topic = config.get("TOPICS", "input_topic")
output_topic = config.get("TOPICS", "output_topic")

sentiment_analysis_model_path = config.get("MODELS", "sentiment_analysis_model_path")
category_detection_model_path = config.get("MODELS", "category_detection_model_path")
bias_detection_model_path= config.get("MODELS", "bias_detection_model_path")
