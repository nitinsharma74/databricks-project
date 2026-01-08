from datetime import datetime, timezone
import argparse
import logging

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
        HOURLY_WEATHER_BASE_URL,
        WEATHER_STATION_IDS,
        WEATHER_TABLE_NAME,
        REQUEST_TIMEOUT_SECONDS,
    )
except ModuleNotFoundError:
    from constants.weather_constants import (
        HOURLY_WEATHER_BASE_URL,
        WEATHER_STATION_IDS,
        WEATHER_TABLE_NAME,
        REQUEST_TIMEOUT_SECONDS,
    )

logger = logging.getLogger(__name__)


class WeatherHourlyIngestor:
    METRIC_FIELDS = {
        "temperature": "temperature",
        "windSpeed": "wind_speed",
        "windDirection": "wind_direction",
        "visibility": "visibility",
        "maxTemperatureLast24Hours": "max_temperature_last_24_hours",
        "minTemperatureLast24Hours": "min_temperature_last_24_hours",
    }

    def __init__(
        self,
        station_ids: list[str] = WEATHER_STATION_IDS,
        base_url: str = HOURLY_WEATHER_BASE_URL,
        table_name: str = WEATHER_TABLE_NAME,
    ) -> None:
        self.station_ids = station_ids
        self.base_url = base_url
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
    def _parse_timestamp(value: str | None) -> datetime | None:
        if not value:
            return None
        return datetime.fromisoformat(value)

    @staticmethod
    def _extract_value(metric: dict | None) -> tuple[float | None, str | None]:
        if not metric:
            return None, None
        return metric.get("value"), metric.get("unitCode")

    def fetch_hourly_weather(self) -> pd.DataFrame:
        rows = []
        for station_id in self.station_ids:
            url = self.base_url.format(station_id=station_id)
            logger.info("Fetching weather for station_id=%s url=%s", station_id, url)
            resp = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            properties = resp.json().get("properties", {})
            now_utc = datetime.now(timezone.utc)
            timestamp = self._parse_timestamp(properties.get("timestamp")) or now_utc
            metrics = {}
            for source_key, prefix in self.METRIC_FIELDS.items():
                value, unit = self._extract_value(properties.get(source_key))
                metrics[f"{prefix}_value"] = value
                metrics[f"{prefix}_unit"] = unit

            rows.append(
                {
                    "station_id": properties.get("stationId") or station_id,
                    "station_name": properties.get("stationName"),
                    "observation_timestamp": timestamp,
                    "observation_date": timestamp.date().isoformat(),
                    "observation_time": timestamp.time().strftime("%H:%M:%S"),
                    "text_description": properties.get("textDescription"),
                    "ingested_at": now_utc,
                    **metrics,
                }
            )

        return pd.DataFrame(rows)

    def write_table(self, df: pd.DataFrame) -> None:
        logger.info("Writing %d rows to table=%s", len(df.index), self.table_name)
        ordered_df = df[self.schema.fieldNames()]
        spark_df = self.spark.createDataFrame(ordered_df, schema=self.schema)

        if self.spark.catalog.tableExists(self.table_name):
            logger.info("Table exists, performing delete+append")
            for _, row in df.iterrows():
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
            logger.info("Table missing, creating new table")
            spark_df.write.format("delta").mode("overwrite").saveAsTable(self.table_name)

    def run(self) -> None:
        logger.info(
            "Starting ingestion for stations=%s table=%s",
            ",".join(self.station_ids),
            self.table_name,
        )
        df = self.fetch_hourly_weather()
        self.write_table(df)

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest hourly weather observations.")
    parser.add_argument("--table-name", default=WEATHER_TABLE_NAME)
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args()
    WeatherHourlyIngestor(table_name=args.table_name).run()
