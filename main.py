from logging import log
from datacleaner import clean_data
from create_trajectories import create_trajectories
from ais_loader import load_data_into_db
import configparser

config = configparser.ConfigParser()
config.read('application.properties')

listOfFiles = ['aisdk_20211008.csv']

# for file in listOfFiles:
#     config["Environment"]["FILE_PATH"] = file
#     print(config["Environment"]["FILE_PATH"])
#     load_data_into_db(config)

# load_data_into_db(config)

# clean_data(config)

create_trajectories(config)
