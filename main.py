from logging import log
from datacleaner import clean_data
# from create_trajectories import create_trajectories
from ais_loader import load_data_into_db
import configparser
from reverse_file import reverse_file
# from create_trajectories import create_trajectories
# import resource
import gc
from dateutil import rrule
from datetime import datetime, timedelta

config = configparser.ConfigParser()
config.read('application.properties')

# Data loading

# for date in dates:
#     file = f'aisdk-{date["year"]}-{date["month"]}-{date["date"]}.csv'
#     print("Reversing: " + file)
#     reverse_file(file)
#     config["Environment"]["FILE_NAME"] = "r_" + file
#     print(config["Environment"]["FILE_NAME"])
#     print("Loading: " + file)
#     load_data_into_db(config)

# config["Database"]["initialize"] = "False"
# print("Cleaning: " + file)
# clean_data(config, date["year"] + date["month"] + date["date"])

# print("Starting " + date["year"] + date["month"] + date["date"])
# create_trajectories(date["year"] + date["month"] + date["date"], config)
# print("Finished " + date["year"] + date["month"] + date["date"])
# gc.collect(generation=2)

# for month in range(11):
#     for day in range(31):
#         if(month == 1 and day > 27):
#             continue
#         if(month == 3 or month == 5 or month == 8 or month == 10):
#             if(day == 30):
#                 continue

#         # file = f'aisdk_2021{(month + 1):02d}{(day + 1):02d}.csv'
#         date = "2021" + f'{(month + 1):02d}' + f'{(day + 1):02d}'
#         print(date)
#         # # print("Reversing: " + file)
#         # # reverse_file(file)
#         # config["Environment"]["FILE_NAME"] = "r_" + file
#         # print(config["Environment"]["FILE_NAME"])
#         # print("2021" + f'{(month + 1):02d}' + f'{(day + 1):02d}')
#         # clean_data(config, "2021" + f'{(month + 1):02d}' + f'{(day + 1):02d}')
#         # print("Loading: " + file)
#         # load_data_into_db(config)
#         # create_trajectories(date, config)
#         print("Finished " + str(date))
#         gc.collect(generation=2)
#         # config["Database"]["initialize"] = "False"

start_date = datetime(2021,1,1)
end_date = datetime(2021,12,31)

# Creates a list based on a frequency basis. E.g. Daily = [start_date, 11-02-2022, 12-02-2022 ... end_date] (inclusive)
listOfDates = rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date)

for date in listOfDates:
    dateFormatted = int(date.strftime("%Y%m%d"))
    print(f"Currently processing date: {dateFormatted}")

    clean_data(config, dateFormatted)

    print(f"Finished processing date: {dateFormatted}")
    gc.collect(generation=2)



# Trajectory Creation

# if __name__ == '__main__':
#     for date in listOfDates:
#         print("Starting " + str(date))
