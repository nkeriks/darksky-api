import datetime
import os

from absl import logging, app, flags
import pandas as pd
import storage

FLAGS = flags.FLAGS
flags.DEFINE_string("place", "san_carlos", ",".join(storage.PLACES.keys()))
flags.DEFINE_string("datadir", "data", "location of data cache")


def main(argv):
    wdb = storage.WeatherDB(FLAGS.datadir)
    lat, lng = storage.PLACES[FLAGS.place]
    days = wdb.get_cached_days_for_location(lat, lng)  # 42.11, -71.18)
    days = sorted(days)
    logging.info("have %s days" % len(days))
    res = [wdb.get_hourly_df(storage.PlaceTime(lat, lng, dt)) for dt in days]
    logging.info("Pandas time %s, yaml time %s", wdb.PANDAS_TIME, wdb.YAML_TIME)
    df = pd.concat(res)
    logging.info(df)
    df.to_csv(
        os.path.join(
            FLAGS.datadir,
            "{}_asof_{}.csv".format(
                FLAGS.place, datetime.datetime.today().strftime("%Y%m%d")
            ),
        )
    )


if __name__ == "__main__":
    app.run(main)
