
import sys
import argparse
from logging import log

from pandas import date_range
from datacleaner import clean_data
# from create_trajectories import create_trajectories
from ais_loader import load_data_into_db
import configparser
from reverse_file import reverse_file
from datetime import datetime, timedelta, date
# from create_trajectories import create_trajectories
# import resource
import gc

def main(argv):
    start_date = datetime.today()
    end_date = datetime.today()
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", help="Starting date in format dd/mm/yyyy")
    parser.add_argument("-ed", help="Ending date in format dd/mm/yyyy")
    parser.add_argument("-l", help="Perform loading of the dates", action="store_true")
    parser.add_argument("-c", help="Perform cleaning of the dates", action="store_true")
    parser.add_argument("-r", help="Perform trajectory reconstruction of the dates", action="store_true")
    args = parser.parse_args()

    if args.sd:
        start_date = datetime.strptime(args.sd, '%d/%m/%Y')
        print("Start date: " + str(start_date))
    if args.ed:
        end_date = datetime.strptime(args.ed, '%d/%m/%Y')
        print("End date: " + str(end_date))

    if not start_date < end_date:
        print("Start date must be before end date")
        exit(2)

    for n in date_range(start_date, end_date):
        if args.l:
            print("Loading " + str(n))
        if args.c:
            print("Cleaning " + str(n))
        if args.r:
            print("Reconstructing trajectories " + str(n))

    


if __name__ == '__main__':
    main(sys.argv[1:])
