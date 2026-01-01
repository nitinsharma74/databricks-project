import sys
from unittest.mock import MagicMock

import pandas as pd

_spark_session = MagicMock()
_spark_session.builder = MagicMock()
sys.modules.setdefault("pyspark", MagicMock())
sys.modules.setdefault("pyspark.sql", MagicMock(SparkSession=_spark_session))
sys.modules.setdefault(
    "pyspark.sql.types",
    MagicMock(
        DoubleType=MagicMock(),
        StringType=MagicMock(),
        StructField=MagicMock(),
        StructType=MagicMock(),
        TimestampType=MagicMock(),
    ),
)

from src import weather_hourly_ingestor


def test_fetch_hourly_weather_parses_rows(monkeypatch):
    response = MagicMock()
    response.json.return_value = {
        "properties": {
            "stationId": "KJFK",
            "stationName": "New York, Kennedy International Airport",
            "timestamp": "2026-01-01T00:30:00+00:00",
            "textDescription": "Partly Cloudy",
            "temperature": {"unitCode": "wmoUnit:degC", "value": -1},
            "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 25.92},
            "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 230},
            "visibility": {"unitCode": "wmoUnit:m", "value": 16093.44},
            "maxTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": 3},
            "minTemperatureLast24Hours": {"unitCode": "wmoUnit:degC", "value": -9},
        }
    }
    response.raise_for_status.return_value = None
    monkeypatch.setattr(
        weather_hourly_ingestor.requests, "get", lambda *args, **kwargs: response
    )
    monkeypatch.setattr(
        weather_hourly_ingestor.SparkSession.builder,
        "getOrCreate",
        lambda: MagicMock(),
    )

    app = weather_hourly_ingestor.WeatherHourlyIngestor()
    df = app.fetch_hourly_weather()

    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, "station_id"] == "KJFK"
    assert df.loc[0, "temperature_value"] == -1


def test_write_table_inserts_when_missing(monkeypatch):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    spark_df = MagicMock()
    spark.createDataFrame.return_value = spark_df
    monkeypatch.setattr(
        weather_hourly_ingestor.SparkSession.builder, "getOrCreate", lambda: spark
    )

    app = weather_hourly_ingestor.WeatherHourlyIngestor()
    app.write_table(pd.DataFrame([{"station_id": "KJFK"}]))

    spark_df.write.format.assert_called_with("delta")
    spark_df.write.format.return_value.mode.assert_called_with("overwrite")
