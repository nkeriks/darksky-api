import pandas as pd
import numpy as np
import re
import glob
import os
import requests
import yaml
import time
from absl import logging
import sqlite3

DAILY_KEYS = [
    "location_id",
    "date_str",
    "utc_time",
    "offset",
    "apparentTemperatureHigh",
    "apparentTemperatureHighTime",
    "apparentTemperatureLow",
    "apparentTemperatureLowTime",
    "apparentTemperatureMax",
    "apparentTemperatureMaxTime",
    "apparentTemperatureMin",
    "apparentTemperatureMinTime",
    "cloudCover",
    "dewPoint",
    "humidity",
    "icon",
    "moonPhase",
    "ozone",
    "precipIntensity",
    "precipIntensityMax",
    "precipIntensityMaxTime",
    "precipProbability",
    "precipType",
    "pressure",
    "summary",
    "sunriseTime",
    "sunsetTime",
    "temperatureHigh",
    "temperatureHighTime",
    "temperatureLow",
    "temperatureLowTime",
    "temperatureMax",
    "temperatureMaxTime",
    "temperatureMin",
    "temperatureMinTime",
    "uvIndex",
    "uvIndexTime",
    "visibility",
    "windBearing",
    "windGust",
    "windGustTime",
    "windSpeed",
]

HOURLY_KEYS = [
    "location_id",
    "date_str",
    "hour",
    "utc_time",
    "offset",
    "apparentTemperature",
    "cloudCover",
    "dewPoint",
    "humidity",
    "icon",
    "ozone",
    "precipIntensity",
    "precipProbability",
    "precipType",
    "pressure",
    "summary",
    "temperature",
    "uvIndex",
    "visibility",
    "windBearing",
    "windGust",
    "windSpeed",
]


class WeatherSQL:
    def __init__(self, dbname):
        self.db = sqlite3.connect(dbname)
        with open("secrets.yaml") as fh:
            secrets = yaml.load(fh, Loader=yaml.SafeLoader)
            self.key = secrets["key"]

    def get_and_store(self, location_id, date_str):
        res = self.get_from_api(location_id, date_str)
        self.put(location_id, date_str, res)

    def put(self, location_id, date_str, res):
        """
        Note: the results come as a single day in local time, but the timestamps are in utc.
        So a place that is UTC + 2 will have data from 2am to 2am UTC, midnight to midnight locally.
        """
        offset = res["offset"]

        if "daily" in res:

            daily_dict = res["daily"]["data"][0]
            daily_dict["offset"] = res["offset"]
            daily_dict["location_id"] = location_id
            daily_dict["date_str"] = date_str
            daily_dict["utc_time"] = daily_dict.pop("time")

            extra_keys = set(daily_dict.keys()) - set(DAILY_KEYS)
            if extra_keys:
                logging.debug("daily dict has extra keys: %s, dropping", extra_keys)
                for k in extra_keys:
                    _ = daily_dict.pop(k)

            daily_key_str = ",".join(daily_dict.keys())
            daily_val_str = ",".join([":%s" % ky for ky in daily_dict.keys()])
            query = f"INSERT OR IGNORE INTO daily_weather ( {daily_key_str} ) VALUES ( {daily_val_str} )"
            self.db.execute(query, daily_dict)
            self.db.commit()
        else:
            logging.info("no daily data for %s, %s", location_id, date_str)

        if "hourly" not in res:
            logging.info("no hourly data for %s, %s", location_id, date_str)
            return None

        for hour_dict in res["hourly"]["data"]:

            hour_dict["offset"] = res["offset"]
            hour_dict["location_id"] = location_id
            hour_dict["date_str"] = date_str
            hour_dict["utc_time"] = hour_dict.pop("time")

            hour_dict["hour"] = (
                pd.to_datetime(hour_dict["utc_time"], unit="s")
                + np.timedelta64(hour_dict["offset"], "h")
            ).hour

            extra_keys = set(hour_dict.keys()) - set(HOURLY_KEYS)
            if extra_keys:
                logging.debug("hourly dict has extra keys: %s, dropping", extra_keys)
                for k in extra_keys:
                    _ = hour_dict.pop(k)

            hourly_key_str = ",".join(hour_dict.keys())
            hourly_val_str = ",".join([":%s" % ky for ky in hour_dict.keys()])
            self.db.execute(
                f"INSERT OR IGNORE INTO hourly_weather ({hourly_key_str}) VALUES({hourly_val_str})",
                hour_dict,
            )
        self.db.commit()

    def location_to_long_lat(self, location_id):
        query = "select longitude, latitude from locations where location_id = ?"
        cur = self.db.execute(query, [location_id])
        res = cur.fetchall()
        return res[0]

    def get_from_api(self, location_id, date_str):
        url_base = "https://api.darksky.net/forecast/{key}/{latitude},{longitude},{date}T00:00:00?{param}"
        param = "exclude=currently,minutely,alerts,flags,pass&units=us&lang=en"

        longitude, latitude = self.location_to_long_lat(location_id)

        url = url_base.format(
            param=param,
            key=self.key,
            latitude=latitude,
            longitude=longitude,
            date=date_str,
        )

        res = requests.get(url)
        res.raise_for_status()
        return res.json()


PLACES = dict(
    missoula=(46.83, -114.04),
    sharon=(42.11, -71.18),
    san_carlos=(37.49, -122.27),
    givatayim=(32.07, 34.81),
    minneapolis=(44.98, -93.27),
    foglo=(60.01, 20.42),
)

# for backfill
LOCATION_IDS = dict(
    missoula=1,
    sharon=2,
    san_carlos=3,
    givatayim=4,
    minneapolis=5,
    foglo=6,
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
