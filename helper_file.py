from dateutil import rrule
from datetime import datetime, timedelta

now = datetime(2021,1,1)
hundredDaysLater = datetime(2021,12,31)

listOfDates = list(rrule.rrule(rrule.WEEKLY, dtstart=now, until=hundredDaysLater))
isFirst = True

for i in range(len(listOfDates)):
    if (isFirst):
        isFirst = False
        continue
    print (f"""CREATE TABLE if not exists fact_ais_y2021w{i} PARTITION OF 
    fact_ais_clean_v4 FOR VALUES FROM ('{int(listOfDates[i-1].strftime("%Y%m%d"))}') TO ('{int(listOfDates[i].strftime("%Y%m%d"))}'); """)

    # CREATE TABLE if not exists fact_ais_y2021d01 PARTITION OF
    #  fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210101') TO ('20210102'); 
    # CREATE TABLE if not exists fact_ais_y2021d02 PARTITION OF
    #  fact_ais_clean_v{VERSION} FOR VALUES FROM ('20210102') TO ('20210103'); 