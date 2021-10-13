from logging import log
from datacleaner import clean_data
from create_trajectories import create_trajectories
from ais_loader import load_data_into_db
import configparser
from reverse_file import reverse_file

config = configparser.ConfigParser()
config.read('application.properties')

listOfFiles = ['aisdk-2021-07-27.csv', 'aisdk-2021-07-28.csv', 'aisdk-2021-07-29.csv']

for file in listOfFiles:
    reverse_file(file)
    config["Environment"]["FILE_NAME"] = "r_" + file
    print(config["Environment"]["FILE_NAME"])
    load_data_into_db(config)

# load_data_into_db(config)

# clean_data(config)

# create_trajectories(config)
