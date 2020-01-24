from absl import logging, app
import pandas as pd
import storage

def main(argv):
    wdb = storage.WeatherDB('data')
    days = wdb.get_cached_days_for_location(42.11, -71.18)
    logging.info('have %s days' % len(days))
    res = [wdb.get_hourly_df(storage.PlaceTime(42.11, -71.18, dt)) for dt in days[:1000]]
    logging.info("Pandas time %s, yaml time %s", wdb.PANDAS_TIME, wdb.YAML_TIME)
    df = pd.concat(res)
    logging.info(df)
    df.to_csv('/tmp/sharon.csv')

if __name__ == '__main__':
    app.run(main)
