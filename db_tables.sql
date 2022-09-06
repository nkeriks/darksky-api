CREATE TABLE locations (
    location_id INTEGER PRIMARY KEY,
    location_name TEXT,
    latitude REAL,
    longitude REAL,
    timezone TEXT
);

INSERT INTO locations VALUES 
    (1, 'missoula', 46.83, -114.04, 'America/Denver'),
    (2, 'sharon', 42.11, -71.18, 'America/New_York'),
    (3, 'san_carlos', 37.49, -122.27, 'America/Los_Angeles'),
    (4, 'givatayim', 32.07, 34.81, 'Asia/Jerusalem'),
    (5, 'minneapolis', 44.98, -93.27, 'America/Chicago'),
    (6, 'foglo', 60.01, 20.42, 'Europe/Mariehamn')
;


CREATE TABLE daily_weather (
    location_id INTEGER,
    -- date_str is LOCAL time
    date_str TEXT,
    utc_time INTEGER,
    offset REAL,
    apparentTemperatureHigh REAL,
    apparentTemperatureHighTime REAL,
    apparentTemperatureLow REAL,
    apparentTemperatureLowTime REAL,
    apparentTemperatureMax REAL,
    apparentTemperatureMaxTime REAL,
    apparentTemperatureMin REAL,
    apparentTemperatureMinTime REAL,
    cloudCover REAL,
    dewPoint REAL,
    humidity REAL,
    icon TEXT,
    moonPhase REAL,
    ozone REAL,
    precipIntensity REAL,
    precipIntensityMax REAL,
    precipIntensityMaxTime REAL,
    precipProbability REAL,
    precipType TEXT,
    pressure REAL,
    summary TEXT,
    sunriseTime REAL,
    sunsetTime REAL,
    temperatureHigh REAL,
    temperatureHighTime REAL,
    temperatureLow REAL,
    temperatureLowTime REAL,
    temperatureMax REAL,
    temperatureMaxTime REAL,
    temperatureMin REAL,
    temperatureMinTime REAL,
    uvIndex INTEGER,
    uvIndexTime REAL,
    visibility REAL,
    windBearing REAL,
    windGust REAL,
    windGustTime REAL,
    windSpeed REAL,
    PRIMARY KEY (location_id, utc_time)
);

CREATE TABLE hourly_weather (
    location_id TEXT,
    -- date_str and hour are local!
    date_str TEXT,
    hour INTEGER,
    utc_time INTEGER,
    offset REAL,
    apparentTemperature REAL,
    cloudCover REAL,
    dewPoint REAL,
    humidity REAL,
    icon TEXT,
    ozone REAL,
    precipIntensity REAL,
    precipProbability REAL,
    precipType TEXT,
    pressure REAL,
    summary TEXT,
    temperature REAL,
    uvIndex INTEGER,
    visibility REAL,
    windBearing REAL,
    windGust REAL,
    windSpeed REAL,
    PRIMARY KEY (location_id, utc_time)
);

