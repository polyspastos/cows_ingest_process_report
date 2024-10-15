import logging
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from . import models, database
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, timedelta, datetime
from uuid import UUID

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

app = FastAPI()

logging.info('teszt')

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

@app.post("/cows/{id}", status_code=201)
def create_cow(id: UUID, cow: CowCreate, db: Session = Depends(database.get_db)):
    logger.info(f"Creating cow with ID: {id}, Name: {cow.name}, Birthdate: {cow.birthdate}")
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
def add_milk_production(id: UUID, data: SensorData, db: Session = Depends(database.get_db)):
    logger.info(f"Adding milk production for cow ID: {id}, Date: {data.date}, Amount: {data.value}")
    db_cow = db.query(models.Cow).filter(models.Cow.id == str(id)).first()
    if not db_cow:
        logger.error(f"Cow with ID {id} not found.")
        raise HTTPException(status_code=404, detail="Cow not found")
    
    new_production = models.MilkProduction(cow_id=str(id), timestamp=data.date, value=data.value)
    db.add(new_production)
    db.commit()
    logger.info(f"Milk production data added successfully for cow ID: {id}")
    return {"message": "Milk production data added successfully"}, 201

@app.post("/cows/{id}/weight")
def add_weight(id: UUID, data: SensorData, db: Session = Depends(database.get_db)):
    logger.info(f"Adding weight for cow ID: {id}, Date: {data.date}, Weight: {data.value}")
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
    
    latest_milk = db.query(models.MilkProduction).filter(models.MilkProduction.cow_id == str(id)).order_by(models.MilkProduction.date.desc()).first()
    latest_weight = db.query(models.Weight).filter(models.Weight.cow_id == str(id)).order_by(models.Weight.date.desc()).first()
    
    return CowDetails(
        id=id,
        latest_milk_production=latest_milk.amount if latest_milk else None,
        latest_weight=latest_weight.weight if latest_weight else None
    )

@app.get("/cows/{id}/milk/report", response_model=List[DailyMilkProduction])
def get_daily_milk_report(id: UUID, db: Session = Depends(database.get_db)):
    logger.info(f"Generating daily milk report for cow ID: {id}")
    db_cow = db.query(models.Cow).filter(models.Cow.id == str(id)).first()
    if not db_cow:
        logger.error(f"Cow with ID {id} not found.")
        raise HTTPException(status_code=404, detail="Cow not found")
    
    today = date.today()
    start_date = today - timedelta(days=30)
    
    milk_data = (
        db.query(models.MilkProduction.date, func.sum(models.MilkProduction.amount).label("total_milk"))
        .filter(models.MilkProduction.cow_id == str(id), models.MilkProduction.date >= start_date)
        .group_by(models.MilkProduction.date)
        .order_by(models.MilkProduction.date)
        .all()
    )
    
    return [{"date": record.date, "total_milk": record.total_milk} for record in milk_data]

@app.get("/cows/{id}/weight/average", response_model=float)
def get_average_weight(id: UUID, db: Session = Depends(database.get_db)):
    logger.info(f"Calculating average weight for cow ID: {id}")
    db_cow = db.query(models.Cow).filter(models.Cow.id == str(id)).first()
    if not db_cow:
        logger.error(f"Cow with ID {id} not found.")
        raise HTTPException(status_code=404, detail="Cow not found")
    
    today = date.today()
    start_date = today - timedelta(days=30)
    
    average_weight = (
        db.query(func.avg(models.Weight.weight))
        .filter(models.Weight.cow_id == str(id), models.Weight.date >= start_date)
        .scalar()
    )
    
    return average_weight if average_weight is not None else 0.0

@app.get("/cows/illness/report", response_model=List[CowDetails])
def get_illness_report(db: Session = Depends(database.get_db)):
    logger.info("Generating illness report for cows.")
    today = date.today()
    start_date = today - timedelta(days=30)
    
    ill_cows = []
    cows = db.query(models.Cow).all()
    
    for cow in cows:
        latest_weight = db.query(models.Weight).filter(models.Weight.cow_id == str(cow.id)).order_by(models.Weight.date.desc()).first()
        if latest_weight:
            weight_30_days_ago = db.query(models.Weight).filter(models.Weight.cow_id == str(cow.id), models.Weight.date < start_date).order_by(models.Weight.date.desc()).first()
            if weight_30_days_ago and latest_weight.weight < weight_30_days_ago.weight * 0.9:
                ill_cows.append(CowDetails(id=cow.id, latest_weight=latest_weight.weight))
    
    return ill_cows

@app.post("/sensors/{id}", status_code=201)
def create_sensor(id: UUID, sensor: SensorCreate, db: Session = Depends(database.get_db)):
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
def add_measurement(sensor_id: UUID, data: SensorData, db: Session = Depends(database.get_db)):
    logger.info(f"Adding measurement for sensor ID: {sensor_id}, Date: {data.date}, Value: {data.value}")
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == str(sensor_id)).first()
    if not db_sensor:
        logger.error(f"Sensor with ID {sensor_id} not found.")
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    new_measurement = models.Measurement(sensor_id=str(sensor_id), date=data.date, value=data.value)
    db.add(new_measurement)
    db.commit()
    logger.info(f"Measurement data added successfully for sensor ID: {sensor_id}")
    return {"message": "Measurement data added successfully"}, 201