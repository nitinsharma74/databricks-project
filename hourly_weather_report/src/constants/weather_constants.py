import os

HOURLY_WEATHER_BASE_URL = "https://api.weather.gov/stations/{station_id}/observations/latest"
WEATHER_STATION_IDS = ["KJFK", "KSFO", "KDFW"]
REQUEST_TIMEOUT_SECONDS = 30

_DEFAULT_CATALOG = os.getenv("WEATHER_TABLE_CATALOG", "workspace")
_DEFAULT_SCHEMA = os.getenv("WEATHER_TABLE_SCHEMA", "default")
_DEFAULT_TARGET = os.getenv("BUNDLE_TARGET", "dev")
_DEFAULT_TABLE_SUFFIX = f"{_DEFAULT_TARGET}_hourly_weather_report"
WEATHER_TABLE_NAME = os.getenv(
    "WEATHER_TABLE_NAME",
    f"{_DEFAULT_CATALOG}.{_DEFAULT_SCHEMA}.{_DEFAULT_TABLE_SUFFIX}",
)
