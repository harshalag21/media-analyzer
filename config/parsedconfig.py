import configparser

bootstrap_servers = None
input_topic = None
output_topic = None
sentiment_analysis_model_path = None
category_detection_model_path = None
bias_detection_model_path = None


config = configparser.ConfigParser()
config.read("config.ini")

for section in config.sections():
    for key, value in config.items(section):
        globals()[key] = value


if __name__ == '__main__':
    print(input_topic)
