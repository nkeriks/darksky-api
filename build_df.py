import datetime
import os

from absl import logging, app, flags
import pandas as pd
import storage
import sqlite3

FLAGS = flags.FLAGS
flags.DEFINE_string("place", "san_carlos", ",".join(storage.PLACES.keys()))
flags.DEFINE_string("database", "darksky.sqlite", "location of database")
flags.DEFINE_string("outdir", "summaries", "where to write csv")

my_cols = [
    "latitude",
    "longitude",
    "date_str",
    "hour",
    "icon",
    "temperature",
    "windSpeed",
    "apparentTemperature",
    "cloudCover",
    "dewPoint",
    "humidity",
    "precipIntensity",
    "precipProbability",
    "pressure",
    "summary",
]

def main(argv):
    lat, lng = storage.PLACES[FLAGS.place]
    db = sqlite3.connect(FLAGS.database)
    sel_str = ",".join(my_cols)
    df = pd.read_sql("SELECT %s from hourly_weather hw JOIN locations l ON l.location_id = hw.location_id WHERE l.location_name = '%s'" % (sel_str, FLAGS.place), db)

    logging.debug(df.head())
    max_date = df["date_str"].max()
    min_date = df["date_str"].min()
    logging.info("writing data through %s", max_date)
    df.to_csv(
        os.path.join(
            FLAGS.outdir,
            "{}_through_{}_from_{}.csv.gz".format(FLAGS.place, max_date, min_date),
        )
    )


if __name__ == "__main__":
    app.run(main)
