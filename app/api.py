import logging
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from . import models, database
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, timedelta, datetime
from uuid import UUID

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

app = FastAPI()


class CowCreate(BaseModel):
    name: str
    birthdate: datetime


class SensorData(BaseModel):
    date: date
    value: float = Field(..., gt=0)


class SensorCreate(BaseModel):
    unit: str


class CowDetails(BaseModel):
    id: UUID
    latest_milk_production: Optional[float] = None
    latest_weight: Optional[float] = None


class DailyMilkProduction(BaseModel):
    date: date
    total_milk: float


class CowReport(BaseModel):
    cow_id: UUID
    total_milk: Optional[float] = None
    latest_weight: Optional[float] = None
    avg_weight_last_30_days: Optional[float] = None
    potentially_ill: bool = False


@app.post("/cows/{id}", status_code=201)
def create_cow(id: UUID, cow: CowCreate, db: Session = Depends(database.get_db)):
    logger.info(
        f"Creating cow with ID: {id}, Name: {cow.name}, Birthdate: {cow.birthdate}"
    )
    db_cow = db.query(models.Cow).filter(models.Cow.id == str(id)).first()
    if db_cow:
        logger.warning(f"Cow with ID {id} already registered.")
        raise HTTPException(status_code=409, detail="Cow already registered")

    new_cow = models.Cow(id=str(id), name=cow.name, birthdate=cow.birthdate)
    db.add(new_cow)
    db.commit()
    db.refresh(new_cow)
    logger.info(f"Cow created successfully: {new_cow}")
    return {"message": "Cow created successfully"}


@app.post("/cows/{id}/milk")
def add_milk_production(
    id: UUID, data: SensorData, db: Session = Depends(database.get_db)
):
    logger.info(
        f"Adding milk production for cow ID: {id}, Date: {data.date}, Amount: {data.value}"
    )
    db_cow = db.query(models.Cow).filter(models.Cow.id == str(id)).first()
    if not db_cow:
        logger.error(f"Cow with ID {id} not found.")
        raise HTTPException(status_code=404, detail="Cow not found")

    new_production = models.MilkProduction(
        cow_id=str(id), timestamp=data.date, value=data.value
    )
    db.add(new_production)
    db.commit()
    logger.info(f"Milk production data added successfully for cow ID: {id}")
    return {"message": "Milk production data added successfully"}, 201


@app.post("/cows/{id}/weight")
def add_weight(id: UUID, data: SensorData, db: Session = Depends(database.get_db)):
    logger.info(
        f"Adding weight for cow ID: {id}, Date: {data.date}, Weight: {data.value}"
    )
    db_cow = db.query(models.Cow).filter(models.Cow.id == str(id)).first()
    if not db_cow:
        logger.error(f"Cow with ID {id} not found.")
        raise HTTPException(status_code=404, detail="Cow not found")

    new_weight = models.Weight(cow_id=str(id), timestamp=data.date, value=data.value)
    db.add(new_weight)
    db.commit()
    logger.info(f"Weight data added successfully for cow ID: {id}")
    return {"message": "Weight data added successfully"}, 201


@app.get("/cows/{id}", response_model=CowDetails)
def get_cow_details(id: UUID, db: Session = Depends(database.get_db)):
    logger.info(f"Fetching details for cow ID: {id}")
    db_cow = db.query(models.Cow).filter(models.Cow.id == str(id)).first()
    if not db_cow:
        logger.error(f"Cow with ID {id} not found.")
        raise HTTPException(status_code=404, detail="Cow not found")

    latest_milk = (
        db.query(models.MilkProduction)
        .filter(models.MilkProduction.cow_id == str(id))
        .order_by(models.MilkProduction.timestamp.desc())
        .first()
    )
    latest_weight = (
        db.query(models.Weight)
        .filter(models.Weight.cow_id == str(id))
        .order_by(models.Weight.timestamp.desc())
        .first()
    )

    return CowDetails(
        id=id,
        latest_milk_production=latest_milk.value if latest_milk else None,
        latest_weight=latest_weight.value if latest_weight else None,
    )


@app.get("/cows/report", response_model=List[CowReport])
def generate_report(
    report_date: Optional[date] = None, db: Session = Depends(database.get_db)
):
    logger.info("Generating farm report.")

    report_date = report_date or date.today()
    thirty_days_ago = report_date - timedelta(days=30)

    cows = db.query(models.Cow).all()

    report_data = []

    for cow in cows:
        total_milk = (
            db.query(func.sum(models.MilkProduction.value))
            .filter(
                models.MilkProduction.cow_id == cow.id,
                func.date(models.MilkProduction.timestamp) == report_date,
            )
            .scalar()
        )

        latest_weight = (
            db.query(models.Weight.value)
            .filter(models.Weight.cow_id == cow.id)
            .order_by(models.Weight.timestamp.desc())
            .first()
        )

        avg_weight = (
            db.query(func.avg(models.Weight.value))
            .filter(
                models.Weight.cow_id == cow.id,
                models.Weight.timestamp >= thirty_days_ago,
            )
            .scalar()
        )

        potentially_ill = False
        if latest_weight and avg_weight and latest_weight[0] < (0.9 * avg_weight):
            potentially_ill = True

        report_data.append(
            CowReport(
                cow_id=cow.id,
                total_milk=total_milk,
                latest_weight=latest_weight[0] if latest_weight else None,
                avg_weight_last_30_days=avg_weight,
                potentially_ill=potentially_ill,
            )
        )

    return report_data


@app.post("/sensors/{id}", status_code=201)
def create_sensor(
    id: UUID, sensor: SensorCreate, db: Session = Depends(database.get_db)
):
    logger.info(f"Creating sensor with ID: {id}, Unit: {sensor.unit}")
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == str(id)).first()
    if db_sensor:
        logger.warning(f"Sensor with ID {id} already registered.")
        raise HTTPException(status_code=409, detail="Sensor already registered")

    new_sensor = models.Sensor(id=str(id), unit=sensor.unit)
    db.add(new_sensor)
    db.commit()
    db.refresh(new_sensor)
    logger.info(f"Sensor created successfully: {new_sensor}")
    return {"message": "Sensor created successfully"}


@app.post("/sensors/{sensor_id}/measurements", status_code=201)
def add_measurement(
    sensor_id: UUID, data: SensorData, db: Session = Depends(database.get_db)
):
    logger.info(
        f"Adding measurement for sensor ID: {sensor_id}, Date: {data.date}, Value: {data.value}"
    )
    db_sensor = (
        db.query(models.Sensor).filter(models.Sensor.id == str(sensor_id)).first()
    )
    if not db_sensor:
        logger.error(f"Sensor with ID {sensor_id} not found.")
        raise HTTPException(status_code=404, detail="Sensor not found")

    new_measurement = models.Measurement(
        sensor_id=str(sensor_id), timestamp=data.date, value=data.value
    )
    db.add(new_measurement)
    db.commit()
    logger.info(f"Measurement data added successfully for sensor ID: {sensor_id}")
    return {"message": "Measurement data added successfully"}, 201
