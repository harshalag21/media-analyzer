from configparser import ConfigParser

config = ConfigParser()
config.read(["./config/config.ini"])

bootstrapServers = config.get("KAFKA", "bootstrap_servers")

input_topic = config.get("TOPICS", "input_topic")
output_topic = config.get("TOPICS", "output_topic")

sentiment_analysis_model_path = config.get("MODELS", "sentiment_analysis_model")
category_detection_model_path = config.get("MODELS", "category_detection_model")
bias_detection_model_path= config.get("MODELS", "bias_detection_model")