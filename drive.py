from collections import defaultdict
import datetime
import time
import sqlite3

from absl import logging, app, flags
import pandas as pd

import storage

DAILY_LIMIT = 990

FLAGS = flags.FLAGS
flags.DEFINE_string("place", "san_carlos", ",".join(storage.PLACES.keys()))
flags.DEFINE_bool("skip_today", True, "Wait until tomorrow UTC")
# It seems that data exists from august 1942 for sharon, ~1950 generally for
# the US, 1973 for other countries?
flags.DEFINE_integer("start_year", 1950, "Year to start collection from.")
flags.DEFINE_string("database", "darksky.sqlite", "location of database")


def do_call(wdb, location_id, date_str, api_calls_today):

    call_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    if api_calls_today[call_date] < DAILY_LIMIT:
        api_calls_today[call_date] += 1
        logging.info(
            "calling api for %s (%s calls on %s)",
            date_str,
            api_calls_today[call_date],
            call_date,
        )
        wdb.get_and_store(location_id, date_str)
        return True
    else:
        logging.info(
            "want %s, but reached limit for today = %s, sleeping", date_str, call_date
        )
        time.sleep(3600)
        return False


def main(argv):

    wdb = storage.WeatherSQL(FLAGS.database)

    start_year = (
        FLAGS.start_year
        if FLAGS.place not in {"givatayim", "foglo"}
        else max(1973, FLAGS.start_year)
    )
    start_dt = "%s-01-01" % start_year
    end_dt = (datetime.datetime.today().date() - datetime.timedelta(1)).strftime(
        "%Y-%m-%d"
    )
    location = FLAGS.place

    day_query = f"""
    WITH RECURSIVE dates(date) AS (
      VALUES('{start_dt}')
      UNION ALL
      SELECT date(date, '+1 day')
      FROM dates
      WHERE date < '{end_dt}'
    )
    ,existing AS (
        SELECT DISTINCT date_str AS dt
        FROM hourly_weather hw
        JOIN locations l ON hw.location_id = l.location_id
        WHERE location_name = '{location}'
    )
    SELECT date
    FROM dates
    WHERE date NOT IN (SELECT dt FROM existing);
    """

    dates = pd.read_sql(day_query, wdb.db)['date'].to_numpy()

    api_calls_today = defaultdict(int)
    if FLAGS.skip_today:
        # don't make any calls today in case we're over the limit
        call_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        api_calls_today[call_date] = DAILY_LIMIT
        logging.info(api_calls_today)

    location_id = storage.LOCATION_IDS[FLAGS.place]
    logging.info("%s dates to do", len(dates))

    idx = 0
    while idx < len(dates):
        dt = dates[idx]
        res = do_call(wdb, location_id, dt, api_calls_today)
        if res:
            idx += 1


if __name__ == "__main__":
    app.run(main)
