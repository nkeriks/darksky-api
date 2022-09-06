from absl import logging, app, flags
import storage

FLAGS = flags.FLAGS
flags.DEFINE_string("place", "san_carlos", ",".join(storage.PLACES.keys()))
flags.DEFINE_string("datadir", "data", "location of data cache")
flags.DEFINE_string("database", "darksky.sqlite", "location of database")


def main(argv):
    wdb = storage.WeatherDB(FLAGS.datadir)
    sql = storage.WeatherSQL(FLAGS.database)
    lat, lng = storage.PLACES[FLAGS.place]
    location_id = storage.LOCATION_IDS[FLAGS.place]

    days = wdb.get_cached_days_for_location(lat, lng)
    days = sorted(days)
    logging.info("have %s days for location %s", len(days), FLAGS.place)

    for ix, dt in enumerate(days):
        if ix % 1000 == 0: 
            logging.info("doing %s" % dt)
        res = wdb.get_from_storage(storage.PlaceTime(lat, lng, dt))
        sql.put(location_id, dt, res)


if __name__ == "__main__":
    app.run(main)
