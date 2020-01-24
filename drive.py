from absl import logging, app, flags
from collections import defaultdict
import datetime
import pandas as pd
import storage
import time

DAILY_LIMIT = 990

def do_call(date, api_calls_today, wdb):
    call_date = datetime.datetime.utcnow().strftime('%Y-%m-%d')                     
    sharon = storage.PlaceTime(42.11, -71.18, date)
    if wdb.has_data(sharon):
        logging.info("found %s", date)
        return True
    else:
        if api_calls_today[call_date] < DAILY_LIMIT:
            api_calls_today[call_date] += 1
            logging.info("calling api for %s (%s calls on %s)", date, api_calls_today[call_date], call_date)
            return wdb.get(sharon)
        else:
            logging.info("want %s, but reached limit for today = %s, sleeping", date, call_date)
            time.sleep(3600)
            return False

def main(argv):
    # data exists from at least august 1942 for sharon
    dates = [x.strftime('%Y-%m-%d') for x in pd.date_range(start=pd.datetime(1942,8,1), end=pd.datetime(2020,1,1))]
    api_calls_today = defaultdict(int)
    api_calls_today['2020-01-24'] = DAILY_LIMIT
    wdb = storage.WeatherDB(base='data')

    idx = 0
    while idx < len(dates):
        dt = dates[idx]
        res = do_call(dt, api_calls_today, wdb)
        if res:
            idx += 1

if __name__ == '__main__':
    app.run(main)
