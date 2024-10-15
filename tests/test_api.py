import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from uuid import uuid4
from . import models, database, api

SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
models.Base.metadata.create_all(bind=engine)

@pytest.fixture(scope="module")
def test_client():
    client = TestClient(api.app)
    yield client

@pytest.fixture(scope="module")
def db_session():
    db = TestingSessionLocal()
    yield db
    db.close()

@pytest.fixture(scope="function", autouse=True)
def clean_database(db_session):
    models.Base.metadata.drop_all(bind=engine)
    models.Base.metadata.create_all(bind=engine)

# Test Cases

def test_create_cow(test_client, db_session):
    cow_id = str(uuid4())
    response = test_client.post(f"/cows/{cow_id}", json={"name": "Bessie", "birthdate": "2020-01-01T00:00:00"})
    assert response.status_code == 201
    assert response.json() == {"message": "Cow created successfully"}

def test_create_existing_cow(test_client, db_session):
    cow_id = str(uuid4())
    test_client.post(f"/cows/{cow_id}", json={"name": "Bessie", "birthdate": "2020-01-01T00:00:00"})
    response = test_client.post(f"/cows/{cow_id}", json={"name": "Bessie", "birthdate": "2020-01-01T00:00:00"})
    assert response.status_code == 409
    assert response.json()["detail"] == "Cow already registered"

def test_add_milk_production(test_client, db_session):
    cow_id = str(uuid4())
    test_client.post(f"/cows/{cow_id}", json={"name": "Bessie", "birthdate": "2020-01-01T00:00:00"})
    response = test_client.post(f"/cows/{cow_id}/milk", json={"date": "2024-10-14", "value": 25.5})
    assert response.status_code == 201
    assert response.json() == {"message": "Milk production data added successfully"}

def test_add_weight(test_client, db_session):
    cow_id = str(uuid4())
    test_client.post(f"/cows/{cow_id}", json={"name": "Bessie", "birthdate": "2020-01-01T00:00:00"})
    response = test_client.post(f"/cows/{cow_id}/weight", json={"date": "2024-10-14", "value": 450.0})
    assert response.status_code == 201
    assert response.json() == {"message": "Weight data added successfully"}

def test_get_cow_details(test_client, db_session):
    cow_id = str(uuid4())
    test_client.post(f"/cows/{cow_id}", json={"name": "Bessie", "birthdate": "2020-01-01T00:00:00"})
    test_client.post(f"/cows/{cow_id}/milk", json={"date": "2024-10-14", "value": 25.5})
    test_client.post(f"/cows/{cow_id}/weight", json={"date": "2024-10-14", "value": 450.0})
    response = test_client.get(f"/cows/{cow_id}")
    assert response.status_code == 200
    cow_details = response.json()
    assert cow_details["latest_milk_production"] == 25.5
    assert cow_details["latest_weight"] == 450.0

def test_generate_report(test_client, db_session):
    cow_id = str(uuid4())
    test_client.post(f"/cows/{cow_id}", json={"name": "Bessie", "birthdate": "2020-01-01T00:00:00"})
    test_client.post(f"/cows/{cow_id}/milk", json={"date": "2024-10-14", "value": 25.5})
    test_client.post(f"/cows/{cow_id}/weight", json={"date": "2024-10-14", "value": 450.0})
    response = test_client.get("/cows/report?report_date=2024-10-14")
    assert response.status_code == 200
    report = response.json()
    assert len(report) > 0
    assert report[0]["cow_id"] == cow_id
    assert report[0]["total_milk"] == 25.5
    assert report[0]["latest_weight"] == 450.0

def test_create_sensor(test_client, db_session):
    sensor_id = str(uuid4())
    response = test_client.post(f"/sensors/{sensor_id}", json={"unit": "liters"})
    assert response.status_code == 201
    assert response.json() == {"message": "Sensor created successfully"}

def test_add_sensor_measurement(test_client, db_session):
    sensor_id = str(uuid4())
    test_client.post(f"/sensors/{sensor_id}", json={"unit": "liters"})
    response = test_client.post(f"/sensors/{sensor_id}/measurements", json={"date": "2024-10-14", "value": 100.0})
    assert response.status_code == 201
    assert response.json() == {"message": "Measurement data added successfully"}
