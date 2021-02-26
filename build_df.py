import datetime
import os

from absl import logging, app, flags
import pandas as pd
import storage

FLAGS = flags.FLAGS
flags.DEFINE_string("place", "san_carlos", ",".join(storage.PLACES.keys()))
flags.DEFINE_string("datadir", "data", "location of data cache")
flags.DEFINE_string("outdir", "summaries", "where to write csv")


def main(argv):
    wdb = storage.WeatherDB(FLAGS.datadir)
    lat, lng = storage.PLACES[FLAGS.place]
    days = wdb.get_cached_days_for_location(lat, lng)
    days = sorted(days)
    logging.info("have %s days" % len(days))
    res = [wdb.get_hourly_df(storage.PlaceTime(lat, lng, dt)) for dt in days]
    logging.info("Pandas time %s, yaml time %s", wdb.PANDAS_TIME, wdb.YAML_TIME)
    df = pd.concat(res)
    logging.debug(df)
    max_date = df["date"].max()
    logging.info("writing data through %s", max_date)
    df.to_csv(
        os.path.join(
            FLAGS.outdir, "{}_through_{}.csv.gz".format(FLAGS.place, max_date),
        )
    )


if __name__ == "__main__":
    app.run(main)
