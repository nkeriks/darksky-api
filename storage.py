import pandas as pd
import numpy as np
import re
import glob
import os
import requests
import yaml
import time
from absl import logging

PLACES = dict(
    missoula=(46.83, -114.04),
    sharon=(42.11, -71.18),
    san_carlos=(37.49, -122.27),
    givatayim=(32.07, 34.81),
    minneapolis=(44.98, -93.27),
)


class PlaceTime(object):
    # date should just be a string 'yyyy-mm-hh'
    # 2 digits is more than enough
    # https://xkcd.com/2170/
    def __init__(self, latitude, longitude, date):
        self.latitude = round(latitude, 2)
        self.longitude = round(longitude, 2)
        self.date = date


class WeatherDB(object):
    def __init__(self, base):
        self.base = base
        with open("secrets.yaml") as fh:
            secrets = yaml.load(fh, Loader=yaml.SafeLoader)
            self.key = secrets["key"]
        self.YAML_TIME = 0
        self.PANDAS_TIME = 0

    def get(self, place):
        if self.has_data(place):
            return self.get_from_storage(place)
        else:
            res = self.get_from_api(place)
            self.put(res, place)
            return res

    def get_hourly_df(self, place):
        my_cols = [
            "latitide",
            "longitude",
            "date",
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
        # missing ['icon' 'precipIntensity' 'precipProbability' 'summary'] for earliest data ~ 1948

        start = time.time()
        res = self.get(place)
        self.YAML_TIME += time.time() - start
        if not "offset" in res or not "hourly" in res:
            logging.debug("missing hourly data for %s", place.date)
            return None

        offset = res["offset"]

        start = time.time()
        df = pd.DataFrame.from_records(res["hourly"]["data"]).assign(
            utc_ts=lambda d: pd.to_datetime(d["time"], unit="s"),
            local_ts=lambda d: d["utc_ts"] + np.timedelta64(offset, "h"),
            date=lambda d: d["local_ts"].dt.strftime("%Y-%m-%d"),
            hour=lambda d: d["local_ts"].dt.hour,
            latitide=place.latitude,
            longitude=place.longitude,
        )
        if not all(np.isin(my_cols, df.columns)):
            missing = np.setdiff1d(my_cols, df.columns)
            logging.debug(
                "columns missing in response %s for day %s, setting to NA",
                missing,
                place.date,
            )
            df = df.assign(**{missing_col: np.nan for missing_col in missing})
        self.PANDAS_TIME += time.time() - start

        return df[my_cols]

    def get_from_storage(self, place):
        with open(self.make_path(place)) as fh:
            res = yaml.load(fh, Loader=yaml.CLoader)
        return res

    def get_from_api(self, place):
        url_base = "https://api.darksky.net/forecast/{key}/{latitude},{longitude},{date}T00:00:00?{param}"
        param = "exclude=currently,minutely,alerts,flags,pass&units=us&lang=en"

        url = url_base.format(
            param=param,
            key=self.key,
            latitude=place.latitude,
            longitude=place.longitude,
            date=place.date,
        )

        res = requests.get(url)
        res.raise_for_status()
        return res.json()

    def get_cached_days_for_location(self, latitude, longitude):
        basedir = os.path.join(self.base, "{}_{}".format(latitude, longitude))
        existing_days = [
            re.sub("\\.yaml$", "", os.path.basename(x))
            for x in glob.glob("{}/*.yaml".format(basedir))
        ]
        return existing_days

    def put(self, result, place):
        outfile = self.make_path(place)
        outdir = os.path.dirname(outfile)
        if not os.path.isdir(outdir):
            os.makedirs(outdir)

        with open(outfile, "w") as fh:
            fh.write(yaml.dump(result))

    def has_data(self, place):
        return os.path.isfile(self.make_path(place))

    def make_path(self, place):
        return os.path.join(
            self.base,
            "{}_{}".format(place.latitude, place.longitude),
            "{}.yaml".format(place.date),
        )

    def delete(self, place):
        pass
