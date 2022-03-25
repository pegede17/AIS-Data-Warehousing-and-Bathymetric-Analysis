
import sys
import argparse
from logging import log

from pandas import date_range
from datacleaner import clean_data
# from create_trajectories import create_trajectories
from ais_loader import load_data_into_db
# from create_trajectories import create_trajectories
import configparser
from reverse_file import reverse_file
from datetime import datetime, timedelta, date
from new_create_trajectories import create_trajectories
from trustworthines import give_trustscore_to_ships
# import resource
import gc
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
        "-c", help="Perform cleaning of the dates", action="store_true")
    parser.add_argument(
        "-r", help="Perform trajectory reconstruction of the dates", action="store_true")
    parser.add_argument(
        "-t", help="Perform updating the trust scores for ships", action="store_true")
    args = parser.parse_args()

    if args.t:
        print("updating trustwortiness")
        give_trustscore_to_ships(config=config)
        return

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
        # reverse_file(file)
        config["Environment"]["FILE_NAME"] = file

        # gc.collect(generation=2)

        if args.l:
            print("Loading " + str(current_date))
            # reverse_file(file)
            # reverse_file(file)
            # file = f'r_aisdk-{date.year:04d}-{date.month:02d}-{date.day:02d}.csv'
            config["Environment"]["FILE_NAME"] = file
            # the data to load will be retrieved from the config
            load_data_into_db(config=config)

        if args.c:
            print("Cleaning " + str(current_date))
            clean_data(config=config, date_id=current_date)

        if args.r:
            print("Reconstructing trajectories " + str(current_date))
            create_trajectories(config=config, date_to_lookup=current_date)


        

        print("Finished " + str(current_date))
        # gc.collect(generation=2)
        # config["Database"]["initialize"] = "False"


if __name__ == '__main__':
    main(sys.argv[1:])
