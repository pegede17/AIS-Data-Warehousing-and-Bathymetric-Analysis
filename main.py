from logging import log
from datacleaner import clean_data
# from create_trajectories import create_trajectories
from ais_loader import load_data_into_db
import configparser
from reverse_file import reverse_file
from create_trajectories import create_trajectories

config = configparser.ConfigParser()
config.read('application.properties')

listOfDates = [20210727, 20210728, 20210729]

dates = [
    {
        "year": "2021",
        "month": "10",
        "date": "01"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "02"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "03"
    }
]

## Data loading

for date in dates:
    file = f'aisdk-{date["year"]}-{date["month"]}-{date["date"]}.csv'
    print("Reversing: " + file)
    reverse_file(file)
    config["Environment"]["FILE_NAME"] = "r_" + file
    print(config["Environment"]["FILE_NAME"])
    print("Loading: " + file)
    load_data_into_db(config)
    config["Database"]["initialize"] = "False"
    print("Cleaning: " + file)
    clean_data(config, date["year"] + date["month"] + date["date"])


## Trajectory Creation

# if __name__ == '__main__':
#     for date in listOfDates:
#         print("Starting " + str(date))
#         create_trajectories(date)
#         print("Finished " + str(date))