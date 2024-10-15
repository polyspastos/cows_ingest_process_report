import pandas as pd
import aiohttp
import asyncio
from datetime import datetime
import logging
from uuid import UUID
import random
import time
import json

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("ingestion.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

info_counter = 0
error_sensors = {}
error_cows = {}
successful_sensors = 0
failed_sensors = 0
successful_measurements = 0
failed_measurements = 0
MAX_RETRIES = 5
MAX_FAILURES_PER_SENSOR = 10
MAX_FAILURES_PER_COW = 10


async def process_sensor(session, base_url, sensor_id, unit):
    global info_counter
    endpoint = f"{base_url}/api/sensors/{sensor_id}"
    data = {"unit": unit}
    logger.info(f"Processing sensor: {sensor_id} with unit: {unit}")
    logger.debug(f"API Endpoint: {endpoint}")
    logger.debug(f"Payload for sensor: {json.dumps(data)}")
    try:
        async with session.post(endpoint, json=data) as response:
            logger.info(f"Response status for sensor {sensor_id}: {response.status}")
            if response.status == 400:
                info_counter += 1
                logger.info(
                    f"[{info_counter}] Sensor {sensor_id} already exists in the database"
                )
            elif response.status == 201:
                info_counter += 1
                logger.info(f"[{info_counter}] Successfully added sensor {sensor_id}")
            else:
                response_text = await response.text()
                logger.error(
                    f"Failed to add sensor {sensor_id}: Status {response.status}, Body: {response_text}"
                )
                response.raise_for_status()
    except aiohttp.ClientError as e:
        logger.error(f"Error processing sensor {sensor_id}: {str(e)}")


async def process_cow(session, base_url, cow_id, name, birthdate):
    global info_counter, error_cows, successful_measurements, failed_measurements

    payload = {"id": str(cow_id), "name": name, "birthdate": birthdate.isoformat()}

    endpoint = f"{base_url}/api/cows/{cow_id}"

    logger.info(f"Processing cow: {cow_id}. Poor cow.")
    logger.debug(f"API Endpoint: {endpoint}")
    logger.debug(f"Payload for cow: {json.dumps(payload)}")
    try:
        async with session.post(endpoint, json=payload) as response:
            logger.info(f"Response status for cow {cow_id}: {response.status}")
            if response.status == 400:
                info_counter += 1
                logger.info(
                    f"[{info_counter}] Cow {cow_id} already exists in the database."
                )
            elif response.status == 201:
                info_counter += 1
                logger.info(f"[{info_counter}] Successfully added cow {cow_id}")
            else:
                response_text = await response.text()
                logger.error(
                    f"Failed to add cow {cow_id}: Status {response.status}, Body: {response_text}"
                )
                response.raise_for_status()
    except aiohttp.ClientError as e:
        logger.error(f"Error processing cow {cow_id}: {str(e)}")


async def process_measurement(session, base_url, row):
    global info_counter, error_cows, successful_measurements, failed_measurements
    cow_id = UUID(row["cow_id"])
    sensor_id = row["sensor_id"]
    timestamp = row["timestamp"]

    if cow_id in error_cows and error_cows[cow_id] >= MAX_FAILURES_PER_COW:
        logger.warning(f"Skipping known problematic cow {cow_id}")
        failed_measurements += 1
        return

    for attempt in range(MAX_RETRIES):
        try:
            sensor_type = "weight" if row["value"] > 100 else "milk"

            if pd.isna(row["value"]) or row["value"] <= 0:
                logger.warning(
                    f"Invalid value for cow {cow_id}, sensor {sensor_id}, timestamp {timestamp}: {row['value']}"
                )
                failed_measurements += 1
                return

            data = {
                "date": datetime.fromtimestamp(timestamp).date().isoformat(),
                "value": float(row["value"]),
            }

            endpoint = f"{base_url}/api/cows/{cow_id}/{sensor_type}"
            logger.info(f"Calling endpoint: POST {endpoint}")
            logger.info(f"Payload: {json.dumps(data)}")

            async with session.post(endpoint, json=data) as response:
                logger.info(f"Response status for cow {cow_id}: {response.status}")
                if response.status == 422:
                    logger.warning(
                        f"Unprocessable Entity for cow {cow_id}, sensor {sensor_id}, timestamp {timestamp}, data: {data}"
                    )
                    failed_measurements += 1
                    return
                elif response.status >= 400:
                    response_text = await response.text()
                    logger.warning(
                        f"Error response for cow {cow_id}, sensor {sensor_id}, timestamp {timestamp}: Status {response.status}, Body: {response_text}"
                    )
                    raise aiohttp.ClientError(
                        f"HTTP {response.status}: {response_text}"
                    )
                response.raise_for_status()
                info_counter += 1
                logger.info(
                    f"[{info_counter}] Successfully added {sensor_type} data for cow {cow_id}, sensor {sensor_id}, timestamp {timestamp}"
                )
                successful_measurements += 1
            return
        except aiohttp.ClientError as e:
            logger.warning(
                f"Failed to add data for cow {cow_id}, sensor {sensor_id}, timestamp {timestamp} (attempt {attempt + 1}): {str(e)}"
            )
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2**attempt + random.random())
            else:
                logger.error(
                    f"Failed to add data for cow {cow_id}, sensor {sensor_id}, timestamp {timestamp} after {MAX_RETRIES} attempts"
                )
                logger.error(f"Problematic data: {row.to_dict()}")
                error_cows[cow_id] = error_cows.get(cow_id, 0) + 1
                failed_measurements += 1
        except Exception as e:
            logger.error(f"Unexpected error processing measurement: {str(e)}")
            logger.error(f"Row data: {row.to_dict()}")
            failed_measurements += 1
            return


async def process_measurement_batch(session, base_url, batch):
    tasks = [process_measurement(session, base_url, row) for _, row in batch.iterrows()]
    await asyncio.gather(*tasks)


async def ingest_data(base_url: str):
    global successful_measurements, failed_measurements
    try:
        async with aiohttp.ClientSession() as session:

            sensors_df = pd.read_parquet(
                "cow_data/sensors.parquet", engine="fastparquet"
            )
            logger.info(f"Read {len(sensors_df)} rows from cow_data/sensors.parquet")
            logger.info(f"Sensors columns: {sensors_df.columns}")

            for _, row in sensors_df.iterrows():
                await process_sensor(session, base_url, UUID(row["id"]), row["unit"])
                await asyncio.sleep(0.1)

            cows_df = pd.read_parquet("cow_data/cows.parquet", engine="fastparquet")
            logger.info(f"Read {len(cows_df)} rows from cow_data/cows.parquet")
            logger.info(f"Cows columns: {cows_df.columns}")

            for _, row in cows_df.iterrows():
                await process_cow(
                    session,
                    base_url,
                    UUID(row["id"]),
                    row["name"],
                    datetime.fromtimestamp(row["birthdate"] / 1_000_000_000),
                )

            measurements_df = pd.read_parquet(
                "cow_data/measurements.parquet", engine="fastparquet"
            )
            logger.info(
                f"Read {len(measurements_df)} rows from cow_data/measurements.parquet"
            )
            logger.info(f"Measurements columns: {measurements_df.columns}")

            batch_size = 50
            for i in range(0, len(measurements_df), batch_size):
                batch = measurements_df.iloc[i : i + batch_size]
                logger.info(f"Processing measurement batch {i//batch_size + 1}")
                await process_measurement_batch(session, base_url, batch)
                # await asyncio.sleep(0.4)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        logger.info(f"Problematic sensors and their failure counts: {error_sensors}")
        logger.info(f"Total sensors processed: {successful_sensors + failed_sensors}")
        logger.info(f"Successful sensors: {successful_sensors}")
        logger.info(f"Failed sensors: {failed_sensors}")
        logger.info(
            f"Total measurements processed: {successful_measurements + failed_measurements}"
        )
        logger.info(f"Successful measurements: {successful_measurements}")
        logger.info(f"Failed measurements: {failed_measurements}")


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(ingest_data("http://localhost:8000"))
    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time} seconds")
