from datetime import datetime, timezone

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from src.constants.weather_constants import (
        HOURLY_WEATHER_URL,
        WEATHER_TABLE_NAME,
        REQUEST_TIMEOUT_SECONDS,
    )
except ModuleNotFoundError:
    from constants.weather_constants import (
        HOURLY_WEATHER_URL,
        WEATHER_TABLE_NAME,
        REQUEST_TIMEOUT_SECONDS,
    )


class WeatherHourlyIngestor:
    def __init__(
        self,
        url: str = HOURLY_WEATHER_URL,
        table_name: str = WEATHER_TABLE_NAME,
    ) -> None:
        self.url = url
        self.table_name = table_name
        self.spark = SparkSession.builder.getOrCreate()
        self.schema = StructType(
            [
                StructField("station_id", StringType(), True),
                StructField("station_name", StringType(), True),
                StructField("observation_timestamp", TimestampType(), True),
                StructField("observation_date", StringType(), True),
                StructField("observation_time", StringType(), True),
                StructField("text_description", StringType(), True),
                StructField("temperature_value", DoubleType(), True),
                StructField("temperature_unit", StringType(), True),
                StructField("wind_speed_value", DoubleType(), True),
                StructField("wind_speed_unit", StringType(), True),
                StructField("wind_direction_value", DoubleType(), True),
                StructField("wind_direction_unit", StringType(), True),
                StructField("visibility_value", DoubleType(), True),
                StructField("visibility_unit", StringType(), True),
                StructField("max_temperature_last_24_hours_value", DoubleType(), True),
                StructField("max_temperature_last_24_hours_unit", StringType(), True),
                StructField("min_temperature_last_24_hours_value", DoubleType(), True),
                StructField("min_temperature_last_24_hours_unit", StringType(), True),
                StructField("ingested_at", TimestampType(), True),
            ]
        )

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime:
        if not value:
            return datetime.now(timezone.utc)
        return datetime.fromisoformat(value)

    @staticmethod
    def _extract_value(metric: dict | None) -> tuple[float | None, str | None]:
        if not metric:
            return None, None
        return metric.get("value"), metric.get("unitCode")

    def fetch_hourly_weather(self) -> pd.DataFrame:
        resp = requests.get(self.url, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()

        properties = data.get("properties", {})
        timestamp = self._parse_timestamp(properties.get("timestamp"))
        temperature_value, temperature_unit = self._extract_value(
            properties.get("temperature")
        )
        wind_speed_value, wind_speed_unit = self._extract_value(
            properties.get("windSpeed")
        )
        wind_direction_value, wind_direction_unit = self._extract_value(
            properties.get("windDirection")
        )
        visibility_value, visibility_unit = self._extract_value(
            properties.get("visibility")
        )
        max_temp_value, max_temp_unit = self._extract_value(
            properties.get("maxTemperatureLast24Hours")
        )
        min_temp_value, min_temp_unit = self._extract_value(
            properties.get("minTemperatureLast24Hours")
        )

        row = {
            "station_id": properties.get("stationId"),
            "station_name": properties.get("stationName"),
            "observation_timestamp": timestamp,
            "observation_date": timestamp.date().isoformat(),
            "observation_time": timestamp.time().strftime("%H:%M:%S"),
            "text_description": properties.get("textDescription"),
            "temperature_value": temperature_value,
            "temperature_unit": temperature_unit,
            "wind_speed_value": wind_speed_value,
            "wind_speed_unit": wind_speed_unit,
            "wind_direction_value": wind_direction_value,
            "wind_direction_unit": wind_direction_unit,
            "visibility_value": visibility_value,
            "visibility_unit": visibility_unit,
            "max_temperature_last_24_hours_value": max_temp_value,
            "max_temperature_last_24_hours_unit": max_temp_unit,
            "min_temperature_last_24_hours_value": min_temp_value,
            "min_temperature_last_24_hours_unit": min_temp_unit,
            "ingested_at": datetime.now(timezone.utc),
        }

        return pd.DataFrame([row])

    def write_table(self, df: pd.DataFrame) -> None:
        spark_df = self.spark.createDataFrame(df, schema=self.schema)

        if self.spark.catalog.tableExists(self.table_name):
            row = df.iloc[0]
            station_id = row["station_id"]
            observation_timestamp = row["observation_timestamp"].isoformat()
            self.spark.sql(
                "DELETE FROM "
                f"{self.table_name} "
                f"WHERE station_id = '{station_id}' "
                f"AND observation_timestamp = '{observation_timestamp}'"
            )
            (
                spark_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(self.table_name)
            )
        else:
            spark_df.write.format("delta").mode("overwrite").saveAsTable(self.table_name)

    def run(self) -> None:
        df = self.fetch_hourly_weather()
        self.write_table(df)


if __name__ == "__main__":
    WeatherHourlyIngestor().run()
