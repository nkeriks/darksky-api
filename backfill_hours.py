# Because I forgot to add the hour column to the database in the backfill
from absl import logging, app, flags
import sqlite3

FLAGS = flags.FLAGS
flags.DEFINE_string("database", "darksky.sqlite", "location of database")


def main(argv):
    db = sqlite3.connect(FLAGS.database)
    db.execute("UPDATE hourly_weather SET hour = cast(strftime('%H', datetime(utc_time + offset * 3600, 'unixepoch')) as integer);")
    db.commit;

if __name__ == "__main__":
    app.run(main)
