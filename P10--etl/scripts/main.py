
import sys
import argparse
from logging import log

from pandas import date_range
from utils.cleantrajectory import clean_and_reconstruct
from utils.ais_loader import load_data_into_db
from utils.initializer import initialize_db
from utils.fact_cell_filler import fill_fact_cell
from utils.fill_bridge_table import fill_bridge_table
import configparser
from datetime import datetime
import configparser

from time import perf_counter
from datetime import datetime, timedelta

config = configparser.ConfigParser()
config.read('../application.properties')


def main(argv):
    start_date = datetime.today()
    end_date = datetime.today()
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", help="Starting date in format dd/mm/yyyy")
    parser.add_argument("-ed", help="Ending date in format dd/mm/yyyy")
    parser.add_argument("-i", help="Initialize database", action="store_true")
    parser.add_argument(
        "-l", help="Perform loading of the dates", action="store_true")
    parser.add_argument(
        "-cr", help="Perform cleaning and trajectory reconstuction of the dates", action="store_true")
    args = parser.parse_args()

    if args.i:
        DATABASE_START_TIMER = perf_counter()
        print("Initializing database")
        initialize_db(config)
        DATABASE_END_TIMER = perf_counter()
        DATABASE_TIME_ELAPSED = timedelta(
            seconds=(DATABASE_END_TIMER - DATABASE_START_TIMER))
        print(f"Database init. duration: {DATABASE_TIME_ELAPSED}")

    if args.sd:
        start_date = datetime.strptime(args.sd, '%d/%m/%Y')
        print("Start date: " + str(start_date))
    if args.ed:
        end_date = datetime.strptime(args.ed, '%d/%m/%Y')
        print("End date: " + str(end_date))

    if not start_date <= end_date:
        print("Start date cannot be after end date")
        exit(2)

    for date in date_range(start_date, end_date):
        current_date = f'{date.year:04d}{date.month:02d}{date.day:02d}'
        file = f'aisdk-{date.year:04d}-{date.month:02d}-{date.day:02d}.csv'

        if args.l:
            print("Loading " + str(current_date))
            # the data to load will be retrieved from the config
            LOADING_START_TIMER = perf_counter()
            load_data_into_db(
                config=config, date_id=current_date, filename=file)
            LOADING_END_TIMER = perf_counter()
            LOADING_TIME_ELAPSED = timedelta(
                seconds=(LOADING_END_TIMER - LOADING_START_TIMER))
            print(f"Loading duration: {LOADING_TIME_ELAPSED}")

        if args.cr:
            try:
                CLEANING_START_TIMER = perf_counter()
                print("Cleaning " + str(current_date))
                # clean_and_reconstruct(
                #     config=config, date_to_lookup=current_date)
                fill_bridge_table(current_date)
                CLEANING_END_TIMER = perf_counter()
                CLEANING_TIME_ELAPSED = timedelta(
                    seconds=(CLEANING_END_TIMER - CLEANING_START_TIMER))
                print(f"Cleaning duration: {CLEANING_TIME_ELAPSED}")
                FACT_CELL_START_TIMER = perf_counter()
                # fill_fact_cell(current_date)
                FACT_CELL_END_TIMER = perf_counter()
                FACT_CELL_TIME_ELAPSED = timedelta(
                    seconds=(FACT_CELL_END_TIMER - FACT_CELL_START_TIMER))
                print(f"Fact cell fill duration: {FACT_CELL_TIME_ELAPSED}")
            except Exception as e:
                print(str(e))
        print("Finished " + str(current_date))


if __name__ == '__main__':
    main(sys.argv[1:])
