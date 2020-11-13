from collections import defaultdict
import datetime
import time

from absl import logging, app, flags
import pandas as pd

import storage

DAILY_LIMIT = 990

FLAGS = flags.FLAGS
flags.DEFINE_string("place", "san_carlos", ",".join(storage.PLACES.keys()))
flags.DEFINE_bool("skip_today", True, "Wait until tomorrow UTC")


def do_call(date, api_calls_today, wdb):
    call_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    lat, lng = storage.PLACES[FLAGS.place]
    place = storage.PlaceTime(lat, lng, date)
    if wdb.has_data(place):
        logging.debug("found %s", date)
        return True
    else:
        if api_calls_today[call_date] < DAILY_LIMIT:
            api_calls_today[call_date] += 1
            logging.info(
                "calling api for %s (%s calls on %s)",
                date,
                api_calls_today[call_date],
                call_date,
            )
            return wdb.get(place)
        else:
            logging.info(
                "want %s, but reached limit for today = %s, sleeping", date, call_date
            )
            time.sleep(3600)
            return False


def main(argv):
    # data exists from august 1942 for sharon, ~1950 generally
    dates = [
        x.strftime("%Y-%m-%d")
        for x in pd.date_range(
            start=pd.datetime(1950, 1, 1),
            end=datetime.datetime.today().date() - datetime.timedelta(1),
        )
    ]
    api_calls_today = defaultdict(int)
    if FLAGS.skip_today:
        call_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        api_calls_today[call_date] = DAILY_LIMIT
        logging.info(api_calls_today)

    wdb = storage.WeatherDB(base="data")

    idx = 0
    while idx < len(dates):
        dt = dates[idx]
        res = do_call(dt, api_calls_today, wdb)
        if res:
            idx += 1


if __name__ == "__main__":
    app.run(main)
