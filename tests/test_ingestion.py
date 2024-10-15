import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import pandas as pd
import asyncio
import uuid
from app.ingestion import ingest_data, process_sensor, process_cow, process_measurement

@pytest.fixture
def mock_aiohttp_client():
    with patch('aiohttp.ClientSession', new_callable=AsyncMock) as mock_session:
        yield mock_session

@pytest.fixture
def mock_pandas_read_parquet():
    with patch('pandas.read_parquet') as mock_read_parquet:
        sensors_df = pd.DataFrame({
            'id': [uuid.uuid4(), uuid.uuid4()],
            'unit': ['weight', 'milk']
        })
        cows_df = pd.DataFrame({
            'id': [uuid.uuid4(), uuid.uuid4()],
            'name': ['Bessie', 'Molly'],
            'birthdate': [int(pd.Timestamp('2019-01-01').timestamp() * 1_000_000_000),
                          int(pd.Timestamp('2020-01-01').timestamp() * 1_000_000_000)]
        })
        measurements_df = pd.DataFrame({
            'cow_id': [str(cows_df.iloc[0]['id']), str(cows_df.iloc[1]['id'])],
            'sensor_id': [str(sensors_df.iloc[0]['id']), str(sensors_df.iloc[1]['id'])],
            'timestamp': [int(pd.Timestamp('2023-10-01').timestamp()), int(pd.Timestamp('2023-10-01').timestamp())],
            'value': [150.0, 50.0]

        mock_read_parquet.side_effect = [sensors_df, cows_df, measurements_df]
        yield mock_read_parquet

@pytest.mark.asyncio
async def test_ingest_data(mock_aiohttp_client, mock_pandas_read_parquet):
    mock_session = mock_aiohttp_client.return_value.__aenter__.return_value
    mock_session.post = AsyncMock(side_effect=[
        AsyncMock(status=201),
        AsyncMock(status=201)
    ])

    mock_session.post.side_effect += [
        AsyncMock(status=201),
        AsyncMock(status=201)
    ]

    mock_session.post.side_effect += [
        AsyncMock(status=201),
        AsyncMock(status=201)
    ]

    await ingest_data("http://localhost:8000")

    assert mock_session.post.call_count == 6

@pytest.mark.asyncio
async def test_process_sensor(mock_aiohttp_client):
    mock_session = mock_aiohttp_client.return_value.__aenter__.return_value
    sensor_id = uuid.uuid4()
    unit = 'weight'

    await process_sensor(mock_session, "http://localhost:8000", sensor_id, unit)

    mock_session.post.assert_called_once_with(f"http://localhost:8000/api/sensors/{sensor_id}", json={'unit': unit})

@pytest.mark.asyncio
async def test_process_cow(mock_aiohttp_client):
    mock_session = mock_aiohttp_client.return_value.__aenter__.return_value
    cow_id = uuid.uuid4()
    name = 'Bessie'
    birthdate = pd.Timestamp('2019-01-01')

    await process_cow(mock_session, "http://localhost:8000", cow_id, name, birthdate)

    mock_session.post.assert_called_once_with(f"http://localhost:8000/api/cows/{cow_id}", json={
        'id': str(cow_id),
        'name': name,
        'birthdate': birthdate.isoformat()
    })

@pytest.mark.asyncio
async def test_process_measurement(mock_aiohttp_client):
    mock_session = mock_aiohttp_client.return_value.__aenter__.return_value
    row = {
        'cow_id': uuid.uuid4(),
        'sensor_id': uuid.uuid4(),
        'timestamp': 1696128000,
        'value': 150.0
    }

    await process_measurement(mock_session, "http://localhost:8000", row)

    cow_id = row['cow_id']
    sensor_type = 'weight' if row['value'] > 100 else 'milk'
    expected_endpoint = f"http://localhost:8000/api/cows/{cow_id}/{sensor_type}"
    expected_payload = {
        "date": pd.to_datetime(row['timestamp'], unit='s').date().isoformat(),
        "value": float(row['value'])
    }

    mock_session.post.assert_called_once_with(expected_endpoint, json=expected_payload)

