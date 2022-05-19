
import sys
import argparse
from logging import log

from pandas import date_range
from utils.cleantrajectory import clean_and_reconstruct
from utils.ais_loader import load_data_into_db
import configparser
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read('P10--backend/application.properties')


def main(argv):
    start_date = datetime.today()
    end_date = datetime.today()
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", help="Starting date in format dd/mm/yyyy")
    parser.add_argument("-ed", help="Ending date in format dd/mm/yyyy")
    parser.add_argument(
        "-l", help="Perform loading of the dates", action="store_true")
    parser.add_argument(
        "-cr", help="Perform cleaning and trajectory reconstuction of the dates", action="store_true")
    args = parser.parse_args()

    if args.sd:
        start_date = datetime.strptime(args.sd, '%d/%m/%Y')
        print("Start date: " + str(start_date))
    if args.ed:
        end_date = datetime.strptime(args.ed, '%d/%m/%Y')
        print("End date: " + str(end_date))

    if not start_date <= end_date:
        print("Start date must be before end date")
        exit(2)

    for date in date_range(start_date, end_date):
        current_date = f'{date.year:04d}{date.month:02d}{date.day:02d}'
        file = f'aisdk-{date.year:04d}-{date.month:02d}-{date.day:02d}.csv'
        config["Environment"]["FILE_NAME"] = file

        if args.l:
            print("Loading " + str(current_date))
            config["Environment"]["FILE_NAME"] = file
            # the data to load will be retrieved from the config
            load_data_into_db(config=config, date_id=current_date)

        if args.cr:
            try:
                print("Cleaning " + str(current_date))
                clean_and_reconstruct(
                    config=config, date_to_lookup=current_date)
            except Exception as e:
                print(str(e))
        print("Finished " + str(current_date))


if __name__ == '__main__':
    main(sys.argv[1:])
