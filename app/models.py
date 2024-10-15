from sqlalchemy import (
    Column,
    String,
    Float,
    DateTime,
    ForeignKey,
)  # Change Date to DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import uuid

Base = declarative_base()


class Sensor(Base):
    __tablename__ = "sensors"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    unit = Column(String, nullable=False)

    measurements = relationship("Measurement", back_populates="sensor")


class Cow(Base):
    __tablename__ = "cows"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    birthdate = Column(DateTime, nullable=False)

    measurements = relationship("Measurement", back_populates="cow")


class Measurement(Base):
    __tablename__ = "measurements"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    cow_id = Column(String, ForeignKey("cows.id"), nullable=False)
    sensor_id = Column(String, ForeignKey("sensors.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)

    cow = relationship("Cow", back_populates="measurements")
    sensor = relationship("Sensor", back_populates="measurements")

    def infer_measurement_type(self):
        if self.sensor.unit == "L":
            return MilkProduction(
                cow_id=self.cow_id, timestamp=self.timestamp, value=self.value
            )
        elif self.sensor.unit == "kg":
            return Weight(
                cow_id=self.cow_id, timestamp=self.timestamp, value=self.value
            )
        else:
            raise ValueError(f"Unsupported unit: {self.sensor.unit}")


class MilkProduction(Base):
    __tablename__ = "milk"

    id = Column(
        String, primary_key=True, default=lambda: str(uuid.uuid4())
    )  # Added a primary key for MilkProduction
    cow_id = Column(String, ForeignKey("cows.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)


class Weight(Base):
    __tablename__ = "weights"

    id = Column(
        String, primary_key=True, default=lambda: str(uuid.uuid4())
    )  # Added a primary key for Weight
    cow_id = Column(String, ForeignKey("cows.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)
