from logging import log
from datacleaner import clean_data
# from create_trajectories import create_trajectories
from ais_loader import load_data_into_db
import configparser
from reverse_file import reverse_file
from create_trajectories import create_trajectories
import resource

config = configparser.ConfigParser()
config.read('application.properties')

listOfDates = [20211001, 20211002, 20211003]

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
    },
    {
        "year": "2021",
        "month": "10",
        "date": "04"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "05"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "06"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "07"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "08"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "09"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "10"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "11"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "12"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "13"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "14"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "15"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "16"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "17"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "18"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "19"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "20"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "21"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "22"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "23"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "24"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "25"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "26"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "27"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "28"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "29"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "30"
    },
    {
        "year": "2021",
        "month": "10",
        "date": "31"
    }
]

# Data loading

# for date in dates:
#     file = f'aisdk-{date["year"]}-{date["month"]}-{date["date"]}.csv'
#     print("Reversing: " + file)
#     reverse_file(file)
#     config["Environment"]["FILE_NAME"] = "r_" + file
#     print(config["Environment"]["FILE_NAME"])
#     print("Loading: " + file)
#     load_data_into_db(config)

#     config["Database"]["initialize"] = "False"
#     print("Cleaning: " + file)
#     clean_data(config, date["year"] + date["month"] + date["date"])

#     print("Starting " + date["year"] + date["month"] + date["date"])
#     create_trajectories(date, config)
#     print("Finished " + date["year"] + date["month"] + date["date"])


# Trajectory Creation

if __name__ == '__main__':
    for date in listOfDates:
        print("Starting " + str(date))
        create_trajectories(date, config)
        print("Finished " + str(date))
